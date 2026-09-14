//! PostgreSQL persistence for the watchlist feed.
//!
//! This store owns only `watchlist_*` tables and its own rows in the shared
//! `esi_cache_metadata` table (one per watched alliance, keyed
//! `watchlist/alliances/{id}` -- the `watchlist/` prefix is what the
//! `watchlist_esi_progress` health check matches with `resource_key LIKE
//! 'watchlist/%'`). Like [`crate::sov_feed::store::SovStore`] it deliberately
//! does not touch `esi_collection_limiter_state`: limiter coordination goes
//! exclusively through the shared [`crate::esi_cache::EsiLimiterStore`]
//! trait object obtained at wiring time in `lib.rs` (a
//! `ContractCollectionStore`).
//!
//! `WatchlistStore::connect` deliberately does *not* run migrations: the
//! background collection loop applies them on every (re)connect attempt
//! through `contract_intelligence`'s shared `MIGRATOR` before connecting a
//! `WatchlistStore` for that cycle. Neither connect is awaited on the path
//! to starting the Discord client.

use crate::esi_cache::CacheMetadata;
use crate::watchlist_feed::model::{
    MemberDeltaDirection, MemberReference, PreparedWatchlistDelivery, WarDetail, WarObservation,
    WarParty, WarScanState, WatchedEntity, WatchlistDeliveryError, WatchlistEventKind,
    WatchlistKind, WatchlistNotificationMessage, WatchlistSubscription,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const WATCHLIST_NONCE_ENFORCEMENT_WINDOW: ChronoDuration = ChronoDuration::minutes(5);
const WATCHLIST_DELIVERY_LEASE: ChronoDuration = ChronoDuration::minutes(2);
const WATCHLIST_TRANSIENT_RETRY_MAX_BACKOFF: ChronoDuration = ChronoDuration::hours(2);

/// The `esi_cache_metadata` resource key for one watched alliance's
/// corporation-list conditional request. The `watchlist/` prefix keeps the
/// feed visible to the `watchlist_esi_progress` health check.
pub fn alliance_resource_key(alliance_id: i64) -> String {
    format!("watchlist/alliances/{alliance_id}")
}

/// The `esi_cache_metadata` resource key for one corporation's public-info
/// conditional request (`GET /corporations/{id}/`, ticket 09). The
/// `watchlist/` prefix keeps it visible to the `watchlist_esi_progress`
/// health check.
pub fn corp_resource_key(corporation_id: i64) -> String {
    format!("watchlist/corporations/{corporation_id}")
}

/// The `esi_cache_metadata` resource key for the war list head-scan
/// (`GET /wars/`, ticket 10). The `watchlist/` prefix keeps it visible to the
/// `watchlist_esi_progress` health check.
pub fn wars_list_resource_key() -> String {
    "watchlist/wars".to_string()
}

/// The `esi_cache_metadata` resource key for one war's detail conditional
/// request (`GET /wars/{id}/`, ticket 10).
pub fn war_resource_key(war_id: i64) -> String {
    format!("watchlist/wars/{war_id}")
}

/// How many days of member snapshots are retained; older rows are pruned
/// every cycle (see the migration for the rationale).
pub const MEMBER_SNAPSHOT_RETENTION: ChronoDuration = ChronoDuration::days(30);

/// How many consecutive corporation-info fetch failures exclude a member from
/// its alliance's "every member has a snapshot" aggregate requirement
/// (ticket-09 finding 3). At three, the aggregate is evaluated without the
/// stuck member rather than blocked forever.
pub const CORP_FETCH_FAILURE_EXCLUSION_THRESHOLD: i32 = 3;

/// How long a finished war is retained before [`WatchlistStore::prune_finished_wars`]
/// drops it, bounding `watchlist_wars` to the open-war population plus wars
/// finished within this window.
pub const WAR_RETENTION: ChronoDuration = ChronoDuration::days(30);

/// How many consecutive failed head-scan detail-fetch cycles quarantine a
/// non-404 war id (the mark then advances past it). A 404 is skipped
/// immediately regardless.
pub const WAR_FETCH_FAILURE_SKIP_THRESHOLD: i32 = 3;

/// The outcome of aggregating an alliance's member count from its current
/// members' latest corporation snapshots.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AllianceAggregate {
    /// The aggregate member count, or `None` when it cannot be evaluated this
    /// cycle (a required member lacks a snapshot, or there is no data at all).
    pub total: Option<i64>,
    /// Current members excluded from the snapshot requirement because their
    /// corporation-info fetch has failed for at least
    /// [`CORP_FETCH_FAILURE_EXCLUSION_THRESHOLD`] cycles.
    pub excluded_members: i64,
}

/// Mirrors [`crate::sov_feed::store::sov_transient_retry_backoff`]: 2, 4, 8,
/// 16, 32, 64 minutes, capped, non-panicking.
fn watchlist_transient_retry_backoff(attempt_count: i32) -> ChronoDuration {
    let exponent = attempt_count.clamp(1, 6) - 1;
    let minutes = 2_i64.saturating_mul(1_i64 << exponent);
    ChronoDuration::minutes(minutes).min(WATCHLIST_TRANSIENT_RETRY_MAX_BACKOFF)
}

fn json_to_sqlx(error: serde_json::Error) -> sqlx::Error {
    sqlx::Error::Protocol(error.to_string())
}

#[derive(Clone)]
pub struct WatchlistStore {
    pool: PgPool,
}

/// A late-bound handle to the watchlist store, mirroring
/// [`crate::sov_feed::store::SovStoreHandle`].
pub type WatchlistStoreHandle = Arc<RwLock<Option<Arc<WatchlistStore>>>>;

pub fn new_watchlist_store_handle() -> WatchlistStoreHandle {
    Arc::new(RwLock::new(None))
}

pub async fn available_watchlist_store(
    handle: &WatchlistStoreHandle,
) -> Option<Arc<WatchlistStore>> {
    handle.read().await.clone()
}

impl WatchlistStore {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    #[doc(hidden)]
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    // --- ESI cache metadata (own resource keys; shared table) ---

    pub async fn cache_metadata(&self, resource_key: &str) -> Result<CacheMetadata, sqlx::Error> {
        let row = sqlx::query(
            "SELECT etag, expires_at, last_modified, error_limit_remain, error_limit_reset, retry_after FROM esi_cache_metadata WHERE resource_key = $1",
        )
        .bind(resource_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            Some(row) => CacheMetadata {
                etag: row.get("etag"),
                expires_at: row.get("expires_at"),
                last_modified: row.get("last_modified"),
                expected_pages: None,
                error_limit_remain: row.get("error_limit_remain"),
                error_limit_reset: row.get("error_limit_reset"),
                rate_limit_group: None,
                rate_limit_limit: None,
                rate_limit_remaining: None,
                rate_limit_used: None,
                retry_after: row.get("retry_after"),
            },
            None => CacheMetadata {
                etag: None,
                expires_at: None,
                last_modified: None,
                expected_pages: None,
                error_limit_remain: None,
                error_limit_reset: None,
                rate_limit_group: None,
                rate_limit_limit: None,
                rate_limit_remaining: None,
                rate_limit_used: None,
                retry_after: None,
            },
        })
    }

    pub async fn persist_cache_metadata(
        &self,
        resource_key: &str,
        metadata: &CacheMetadata,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO esi_cache_metadata (resource_key, etag, expires_at, last_modified, error_limit_remain, error_limit_reset, retry_after, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,now()) ON CONFLICT (resource_key) DO UPDATE SET etag = EXCLUDED.etag, expires_at = EXCLUDED.expires_at, last_modified = EXCLUDED.last_modified, error_limit_remain = EXCLUDED.error_limit_remain, error_limit_reset = EXCLUDED.error_limit_reset, retry_after = EXCLUDED.retry_after, updated_at = now()",
        )
        .bind(resource_key)
        .bind(&metadata.etag)
        .bind(metadata.expires_at)
        .bind(metadata.last_modified)
        .bind(metadata.error_limit_remain)
        .bind(metadata.error_limit_reset)
        .bind(metadata.retry_after)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // --- Watched entities (the per-guild registry) ---

    /// Idempotent add: a duplicate `/watch add` for the same
    /// (guild, kind, entity) refreshes the stored ticker/name rather than
    /// erroring (spec: "duplicate adds are idempotent"). Returns `true` when
    /// this inserted a new row, `false` when it updated an existing one.
    pub async fn add_entity(
        &self,
        entity: &WatchedEntity,
        added_by: Option<u64>,
        added_at: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // Fix round finding 4 / ticket-09 finding 1: `remove_entity`
        // intentionally leaves an entity's persisted state in place (other
        // guilds may still watch it). But when an entity is added while NO
        // guild currently watches it, that state is stale from an earlier
        // watch that was fully removed; evaluating the next cycle against it
        // would flood the membership gap's churn (alliances) or fire a
        // `member_delta` / `corp_changed_alliance` against pre-watch history
        // and a possibly-disarmed band state (both kinds). Reset it in the
        // same transaction so the next cycle re-establishes a silent baseline.
        // Do NOT reset when another guild already watches the entity.
        let already_watched: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM watchlist_entities WHERE kind = $1 AND entity_id = $2)",
        )
        .bind(entity.kind.as_str())
        .bind(entity.entity_id)
        .fetch_one(&mut *tx)
        .await?;
        if !already_watched {
            if entity.kind.is_alliance() {
                // Ticket-08 corp join/leave state.
                sqlx::query("DELETE FROM watchlist_alliance_corps WHERE alliance_id = $1")
                    .bind(entity.entity_id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM watchlist_alliance_baseline WHERE alliance_id = $1")
                    .bind(entity.entity_id)
                    .execute(&mut *tx)
                    .await?;
            }
            // Re-review finding (both kinds): drop the entity's own
            // conditional-request metadata. Otherwise a re-add within the
            // route's cache TTL (or one answered by a 304) skips the fresh
            // response that would re-establish the silent baseline, and the
            // first real change afterwards is folded into that deferred
            // baseline and lost. A corporation's row may also be shared with
            // its role as a watched alliance's member; forcing one extra fetch
            // there is harmless.
            let resource_key = if entity.kind.is_alliance() {
                alliance_resource_key(entity.entity_id)
            } else {
                corp_resource_key(entity.entity_id)
            };
            sqlx::query("DELETE FROM esi_cache_metadata WHERE resource_key = $1")
                .bind(resource_key)
                .execute(&mut *tx)
                .await?;
            // Ticket-09 finding 1: watch-scoped member-delta state, both
            // kinds. Deleting a corporation's own snapshot rows may also drop
            // rows that served a watched alliance's aggregate; that is
            // acceptable because the aggregate requires every current member
            // to have a snapshot and the next cycle re-fetches this
            // corporation, so the aggregate is simply deferred one cycle.
            sqlx::query(
                "DELETE FROM watchlist_member_snapshots WHERE entity_kind = $1 AND entity_id = $2",
            )
            .bind(entity.kind.as_str())
            .bind(entity.entity_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM watchlist_member_band_state WHERE entity_kind = $1 AND entity_id = $2",
            )
            .bind(entity.kind.as_str())
            .bind(entity.entity_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM watchlist_member_baseline WHERE entity_kind = $1 AND entity_id = $2",
            )
            .bind(entity.kind.as_str())
            .bind(entity.entity_id)
            .execute(&mut *tx)
            .await?;
            if !entity.kind.is_alliance() {
                sqlx::query("DELETE FROM watchlist_corp_fetch_failures WHERE corporation_id = $1")
                    .bind(entity.entity_id)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        let inserted: bool = sqlx::query_scalar(
            "INSERT INTO watchlist_entities (guild_id, kind, entity_id, ticker, name, added_by, added_at) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (guild_id, kind, entity_id) DO UPDATE SET ticker = EXCLUDED.ticker, name = EXCLUDED.name RETURNING (xmax = 0) AS inserted",
        )
        .bind(entity.guild_id as i64)
        .bind(entity.kind.as_str())
        .bind(entity.entity_id)
        .bind(&entity.ticker)
        .bind(&entity.name)
        .bind(added_by.map(|id| id as i64))
        .bind(added_at)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn remove_entity(
        &self,
        guild_id: u64,
        kind: WatchlistKind,
        entity_id: i64,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "DELETE FROM watchlist_entities WHERE guild_id = $1 AND kind = $2 AND entity_id = $3",
        )
        .bind(guild_id as i64)
        .bind(kind.as_str())
        .bind(entity_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn list_entities(&self, guild_id: u64) -> Result<Vec<WatchedEntity>, sqlx::Error> {
        sqlx::query(
            "SELECT guild_id, kind, entity_id, ticker, name FROM watchlist_entities WHERE guild_id = $1 ORDER BY kind, entity_id",
        )
        .bind(guild_id as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(watched_entity_from_row)
        .collect()
    }

    /// The alliance ids one guild currently watches, used to resolve a sov
    /// `Defender { watchlist: true }` leaf against that guild's watchlist at
    /// evaluation time (ticket 08).
    pub async fn watched_alliance_ids(&self, guild_id: u64) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT entity_id FROM watchlist_entities WHERE guild_id = $1 AND kind = 'alliance' ORDER BY entity_id",
        )
        .bind(guild_id as i64)
        .fetch_all(&self.pool)
        .await
    }

    /// Every distinct alliance id watched by any guild, used by the
    /// collector to decide which alliance corporation lists to poll.
    pub async fn all_watched_alliance_ids(&self) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT DISTINCT entity_id FROM watchlist_entities WHERE kind = 'alliance' ORDER BY entity_id",
        )
        .fetch_all(&self.pool)
        .await
    }

    /// The guilds watching one alliance, used to fan an event out to only
    /// the guilds that care about it.
    pub async fn guilds_watching_alliance(
        &self,
        alliance_id: i64,
    ) -> Result<Vec<u64>, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT guild_id FROM watchlist_entities WHERE kind = 'alliance' AND entity_id = $1",
        )
        .bind(alliance_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|id| id as u64)
        .collect())
    }

    // --- Alliance corporation membership snapshot ---

    /// The persisted membership snapshot for one alliance: `corporation_id`
    /// -> `left_at` (`None` for a current member, `Some` for one recorded as
    /// having left). Empty before the alliance's first baseline.
    pub async fn alliance_corp_snapshot(
        &self,
        alliance_id: i64,
    ) -> Result<HashMap<i64, Option<DateTime<Utc>>>, sqlx::Error> {
        Ok(sqlx::query(
            "SELECT corporation_id, left_at FROM watchlist_alliance_corps WHERE alliance_id = $1",
        )
        .bind(alliance_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get::<i64, _>("corporation_id"),
                row.get::<Option<DateTime<Utc>>, _>("left_at"),
            )
        })
        .collect())
    }

    /// Records that a corporation is a current member of an alliance.
    /// `reset_first_seen` is `true` for a fresh join or rejoin (the
    /// corporation was absent or previously `left_at`), which stamps a new
    /// `first_seen_at` and clears `left_at`; `false` for a continuing member,
    /// which only bumps `last_seen_at`.
    pub async fn upsert_member(
        &self,
        alliance_id: i64,
        corporation_id: i64,
        observed_at: DateTime<Utc>,
        reset_first_seen: bool,
    ) -> Result<(), sqlx::Error> {
        if reset_first_seen {
            sqlx::query(
                "INSERT INTO watchlist_alliance_corps (alliance_id, corporation_id, first_seen_at, last_seen_at, left_at) VALUES ($1,$2,$3,$3,NULL) ON CONFLICT (alliance_id, corporation_id) DO UPDATE SET first_seen_at = EXCLUDED.first_seen_at, last_seen_at = EXCLUDED.last_seen_at, left_at = NULL",
            )
            .bind(alliance_id)
            .bind(corporation_id)
            .bind(observed_at)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                "UPDATE watchlist_alliance_corps SET last_seen_at = $3, left_at = NULL WHERE alliance_id = $1 AND corporation_id = $2",
            )
            .bind(alliance_id)
            .bind(corporation_id)
            .bind(observed_at)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Records that a member corporation has left an alliance (absent from a
    /// fresh listing): stamps `left_at`. Only affects a row currently marked
    /// as a member (`left_at IS NULL`).
    pub async fn mark_corp_left(
        &self,
        alliance_id: i64,
        corporation_id: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE watchlist_alliance_corps SET left_at = $3, last_seen_at = last_seen_at WHERE alliance_id = $1 AND corporation_id = $2 AND left_at IS NULL",
        )
        .bind(alliance_id)
        .bind(corporation_id)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn alliance_baseline_established(
        &self,
        alliance_id: i64,
    ) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM watchlist_alliance_baseline WHERE alliance_id = $1)",
        )
        .bind(alliance_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn establish_alliance_baseline(
        &self,
        alliance_id: i64,
        established_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_alliance_baseline (alliance_id, established_at) VALUES ($1,$2) ON CONFLICT (alliance_id) DO NOTHING",
        )
        .bind(alliance_id)
        .bind(established_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // --- Watched corporations & entity fan-out (ticket 09) ---

    /// Every distinct corporation id watched by any guild.
    pub async fn all_watched_corporation_ids(&self) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT DISTINCT entity_id FROM watchlist_entities WHERE kind = 'corporation' ORDER BY entity_id",
        )
        .fetch_all(&self.pool)
        .await
    }

    /// The guilds watching one entity of a given kind, used to fan a
    /// member-delta or corp-changed-alliance event out only to the guilds
    /// that care about it.
    pub async fn guilds_watching_entity(
        &self,
        kind: WatchlistKind,
        entity_id: i64,
    ) -> Result<Vec<u64>, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT guild_id FROM watchlist_entities WHERE kind = $1 AND entity_id = $2",
        )
        .bind(kind.as_str())
        .bind(entity_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|id| id as u64)
        .collect())
    }

    /// The corporation ids currently members of an alliance (present in the
    /// last snapshot with no `left_at`).
    pub async fn current_alliance_member_ids(
        &self,
        alliance_id: i64,
    ) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT corporation_id FROM watchlist_alliance_corps WHERE alliance_id = $1 AND left_at IS NULL ORDER BY corporation_id",
        )
        .bind(alliance_id)
        .fetch_all(&self.pool)
        .await
    }

    // --- Member snapshots & band state (ticket 09) ---

    /// `updated_at` of each corporation's public-info cache row, for the
    /// least-recently-fetched-first ordering the collector uses to keep the
    /// per-cycle corporation-info fetch bounded and eventually fair. A
    /// corporation with no cache row (never fetched) is absent from the map.
    pub async fn corp_cache_updated_ats(
        &self,
        corporation_ids: &[i64],
    ) -> Result<HashMap<i64, DateTime<Utc>>, sqlx::Error> {
        if corporation_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let keys: Vec<String> = corporation_ids
            .iter()
            .map(|id| corp_resource_key(*id))
            .collect();
        let rows = sqlx::query(
            "SELECT resource_key, updated_at FROM esi_cache_metadata WHERE resource_key = ANY($1)",
        )
        .bind(&keys)
        .fetch_all(&self.pool)
        .await?;
        let mut map = HashMap::new();
        for row in rows {
            let key: String = row.get("resource_key");
            if let Some(id) = key
                .strip_prefix("watchlist/corporations/")
                .and_then(|rest| rest.parse::<i64>().ok())
            {
                map.insert(id, row.get::<DateTime<Utc>, _>("updated_at"));
            }
        }
        Ok(map)
    }

    /// Records one corporation's member-count snapshot (one row per
    /// (entity, observed_at); idempotent within an hour via the primary key).
    pub async fn record_corp_snapshot(
        &self,
        corporation_id: i64,
        member_count: i64,
        alliance_id: Option<i64>,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_member_snapshots (entity_kind, entity_id, member_count, alliance_id, observed_at) VALUES ('corporation',$1,$2,$3,$4) ON CONFLICT (entity_kind, entity_id, observed_at) DO UPDATE SET member_count = EXCLUDED.member_count, alliance_id = EXCLUDED.alliance_id",
        )
        .bind(corporation_id)
        .bind(member_count)
        .bind(alliance_id)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Records one alliance's aggregate member-count snapshot.
    pub async fn record_alliance_member_snapshot(
        &self,
        alliance_id: i64,
        member_count: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_member_snapshots (entity_kind, entity_id, member_count, alliance_id, observed_at) VALUES ('alliance',$1,$2,NULL,$3) ON CONFLICT (entity_kind, entity_id, observed_at) DO UPDATE SET member_count = EXCLUDED.member_count",
        )
        .bind(alliance_id)
        .bind(member_count)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The previous (most recent) corporation snapshot's `alliance_id`, used
    /// to detect a corp-changed-alliance transition. Returns `None` when the
    /// corporation has no prior snapshot at/after `min_observed_at` (a silent
    /// baseline), or `Some(alliance_id)` where the inner option is the
    /// recorded alliance (`None` = the corporation was unaffiliated).
    ///
    /// `min_observed_at` is the watch-scoped baseline (ticket-09 finding 1):
    /// snapshots taken before the corporation became watched (e.g. while it
    /// was only a member of a watched alliance) are ignored so a fresh watch
    /// never fires against pre-watch history.
    pub async fn previous_corp_alliance(
        &self,
        corporation_id: i64,
        min_observed_at: DateTime<Utc>,
    ) -> Result<Option<Option<i64>>, sqlx::Error> {
        Ok(sqlx::query(
            "SELECT alliance_id FROM watchlist_member_snapshots WHERE entity_kind = 'corporation' AND entity_id = $1 AND observed_at >= $2 ORDER BY observed_at DESC LIMIT 1",
        )
        .bind(corporation_id)
        .bind(min_observed_at)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| row.get::<Option<i64>, _>("alliance_id")))
    }

    /// The watch-scoped member-delta baseline for an entity, i.e. when the
    /// current watch first evaluated it (ticket-09 finding 1). `None` until
    /// the member-snapshot pass has established it.
    pub async fn member_baseline(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT established_at FROM watchlist_member_baseline WHERE entity_kind = $1 AND entity_id = $2",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// Establishes the watch-scoped member-delta baseline for an entity if it
    /// has none yet (idempotent). Called by the member-snapshot pass on the
    /// first successful evaluation after the entity became watched.
    pub async fn establish_member_baseline(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
        established_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_member_baseline (entity_kind, entity_id, established_at) VALUES ($1,$2,$3) ON CONFLICT (entity_kind, entity_id) DO NOTHING",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .bind(established_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The current member count of an alliance: the sum over its current
    /// member corporations' latest snapshots.
    ///
    /// Normally the aggregate is evaluated only when *every* current member
    /// has a snapshot (otherwise `total` is `None`, meaning no evaluation this
    /// cycle). Ticket-09 finding 3: a member whose corporation-info fetch has
    /// failed continuously for at least [`CORP_FETCH_FAILURE_EXCLUSION_THRESHOLD`]
    /// cycles is excluded from that requirement -- its last known snapshot is
    /// summed if one exists, otherwise it is skipped -- so one persistently
    /// failing member cannot block the aggregate forever. `excluded_members`
    /// surfaces how many current members were excluded this way. `total` is
    /// `None` when the alliance has no current members, or when every current
    /// member is excluded and none of them has any snapshot (no data at all).
    pub async fn current_alliance_member_count(
        &self,
        alliance_id: i64,
    ) -> Result<AllianceAggregate, sqlx::Error> {
        let row = sqlx::query(
            "WITH members AS (\
                 SELECT corporation_id FROM watchlist_alliance_corps WHERE alliance_id = $1 AND left_at IS NULL\
             ), failing AS (\
                 SELECT corporation_id FROM watchlist_corp_fetch_failures WHERE consecutive_failures >= $2\
             ), required AS (\
                 SELECT corporation_id FROM members \
                 WHERE corporation_id NOT IN (SELECT corporation_id FROM failing)\
             ), latest AS (\
                 SELECT DISTINCT ON (entity_id) entity_id, member_count \
                 FROM watchlist_member_snapshots \
                 WHERE entity_kind = 'corporation' AND entity_id IN (SELECT corporation_id FROM members) \
                 ORDER BY entity_id, observed_at DESC\
             ) \
             SELECT (SELECT count(*) FROM members) AS member_count, \
                    (SELECT count(*) FROM required) AS required_count, \
                    (SELECT count(*) FROM required r \
                       WHERE EXISTS (SELECT 1 FROM latest l WHERE l.entity_id = r.corporation_id)) AS required_with_snapshot, \
                    (SELECT count(*) FROM members m \
                       JOIN failing f ON f.corporation_id = m.corporation_id) AS excluded_count, \
                    (SELECT count(*) FROM latest) AS snapshot_count, \
                    (SELECT COALESCE(sum(member_count), 0)::bigint FROM latest) AS total",
        )
        .bind(alliance_id)
        .bind(CORP_FETCH_FAILURE_EXCLUSION_THRESHOLD)
        .fetch_one(&self.pool)
        .await?;
        let member_count: i64 = row.get("member_count");
        let required_count: i64 = row.get("required_count");
        let required_with_snapshot: i64 = row.get("required_with_snapshot");
        let excluded_count: i64 = row.get("excluded_count");
        let snapshot_count: i64 = row.get("snapshot_count");
        let total: i64 = row.get("total");
        // No members, or every required (non-excluded) member lacks a
        // snapshot: cannot evaluate this cycle.
        let total =
            if member_count == 0 || required_with_snapshot != required_count || snapshot_count == 0
            {
                None
            } else {
                Some(total)
            };
        Ok(AllianceAggregate {
            total,
            excluded_members: excluded_count,
        })
    }

    /// Records one corporation-info fetch failure, incrementing its
    /// consecutive-failure counter (ticket-09 finding 3).
    pub async fn record_corp_fetch_failure(
        &self,
        corporation_id: i64,
        error: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<i32, sqlx::Error> {
        sqlx::query_scalar::<_, i32>(
            "INSERT INTO watchlist_corp_fetch_failures (corporation_id, consecutive_failures, last_error, updated_at) \
             VALUES ($1, 1, $2, $3) \
             ON CONFLICT (corporation_id) DO UPDATE SET \
                 consecutive_failures = watchlist_corp_fetch_failures.consecutive_failures + 1, \
                 last_error = EXCLUDED.last_error, \
                 updated_at = EXCLUDED.updated_at \
             RETURNING consecutive_failures",
        )
        .bind(corporation_id)
        .bind(error)
        .bind(observed_at)
        .fetch_one(&self.pool)
        .await
    }

    /// Resets a corporation's consecutive-failure counter after any success
    /// (`200`) or `304` (ticket-09 finding 3). A no-op when the corporation
    /// has no failure row.
    pub async fn reset_corp_fetch_failures(&self, corporation_id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM watchlist_corp_fetch_failures WHERE corporation_id = $1")
            .bind(corporation_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn corp_fetch_failures_for_test(
        &self,
        corporation_id: i64,
    ) -> Result<Option<i32>, sqlx::Error> {
        sqlx::query_scalar::<_, i32>(
            "SELECT consecutive_failures FROM watchlist_corp_fetch_failures WHERE corporation_id = $1",
        )
        .bind(corporation_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// The most recent member count recorded for an entity, or `None` if it
    /// has no snapshots yet. Used as the "current" count for a watched
    /// corporation's delta evaluation.
    pub async fn latest_member_count(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
    ) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT member_count FROM watchlist_member_snapshots WHERE entity_kind = $1 AND entity_id = $2 ORDER BY observed_at DESC LIMIT 1",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// The member-delta reference: the snapshot nearest `target` (usually
    /// `now - 7d`) for an entity, restricted to snapshots at/after
    /// `min_observed_at` (the watch-scoped baseline, ticket-09 finding 1), or
    /// `None` if the entity has no such snapshots. The pure
    /// `evaluate_member_delta` applies the minimum-history and zero-count
    /// rules to the returned reference.
    ///
    /// The `ORDER BY abs(...)` is a sort over the entity's own snapshot rows
    /// (bounded by the 30-day retention, so at most ~720 rows), filtered by
    /// the `(entity_kind, entity_id, observed_at)` index prefix -- it is not
    /// an index-ordered proximity seek.
    pub async fn member_reference(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
        target: DateTime<Utc>,
        min_observed_at: DateTime<Utc>,
    ) -> Result<Option<MemberReference>, sqlx::Error> {
        Ok(sqlx::query(
            "SELECT member_count, observed_at FROM watchlist_member_snapshots \
             WHERE entity_kind = $1 AND entity_id = $2 AND observed_at >= $4 \
             ORDER BY abs(EXTRACT(EPOCH FROM (observed_at - $3))) ASC, observed_at DESC LIMIT 1",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .bind(target)
        .bind(min_observed_at)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| MemberReference {
            count: row.get("member_count"),
            observed_at: row.get("observed_at"),
        }))
    }

    /// Whether an entity's member-delta band state is armed (may fire on the
    /// next crossing): `true` when there is no state row or its
    /// `last_fired_direction` is NULL.
    pub async fn member_band_armed(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
    ) -> Result<bool, sqlx::Error> {
        let last: Option<Option<String>> = sqlx::query_scalar(
            "SELECT last_fired_direction FROM watchlist_member_band_state WHERE entity_kind = $1 AND entity_id = $2",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match last {
            None => true,
            Some(direction) => direction.is_none(),
        })
    }

    /// Records a re-arm (inside-band) evaluation of an entity's band state:
    /// clears `last_fired_direction` and refreshes the reference for
    /// observability. `crossing_sequence` is deliberately left untouched (it
    /// only ever advances on a fire, in [`Self::fire_member_delta_crossing`]),
    /// so a subsequent crossing mints a fresh, never-reused evidence key.
    pub async fn rearm_member_band_state(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
        reference: Option<MemberReference>,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_member_band_state (entity_kind, entity_id, reference_count, reference_observed_at, last_fired_direction, updated_at) VALUES ($1,$2,$3,$4,NULL,$5) ON CONFLICT (entity_kind, entity_id) DO UPDATE SET reference_count = EXCLUDED.reference_count, reference_observed_at = EXCLUDED.reference_observed_at, last_fired_direction = NULL, updated_at = EXCLUDED.updated_at",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .bind(reference.map(|reference| reference.count))
        .bind(reference.map(|reference| reference.observed_at))
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically records a fired member-delta crossing (ticket-09 finding 2).
    /// In one transaction it bumps the entity's `crossing_sequence`, disarms
    /// the band (`last_fired_direction`), refreshes the reference, and inserts
    /// one `prepared` delivery row per target keyed
    /// `{kind}:{entity}:{direction}:{sequence}`. Sending happens afterward
    /// through the existing lease path, so a crash anywhere after the commit
    /// yields exactly one post per crossing (the disarm can never be lost
    /// while the delivery rows survive, and the sequence -- unlike the old
    /// sliding `reference_observed_at` key -- does not change across the
    /// crash). Passing no targets still disarms and bumps the sequence, so a
    /// crossing with no subscriber is not re-fired next cycle. Returns the
    /// evidence key and the number of rows freshly prepared.
    #[allow(clippy::too_many_arguments)]
    pub async fn fire_member_delta_crossing(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
        direction: MemberDeltaDirection,
        reference: Option<MemberReference>,
        observed_at: DateTime<Utc>,
        targets: &[WatchlistSubscription],
        message: Option<&WatchlistNotificationMessage>,
    ) -> Result<(String, usize), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let sequence: i64 = sqlx::query_scalar(
            "INSERT INTO watchlist_member_band_state (entity_kind, entity_id, reference_count, reference_observed_at, last_fired_direction, crossing_sequence, updated_at) \
             VALUES ($1,$2,$3,$4,$5,1,$6) \
             ON CONFLICT (entity_kind, entity_id) DO UPDATE SET \
                 reference_count = EXCLUDED.reference_count, \
                 reference_observed_at = EXCLUDED.reference_observed_at, \
                 last_fired_direction = EXCLUDED.last_fired_direction, \
                 crossing_sequence = watchlist_member_band_state.crossing_sequence + 1, \
                 updated_at = EXCLUDED.updated_at \
             RETURNING crossing_sequence",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .bind(reference.map(|reference| reference.count))
        .bind(reference.map(|reference| reference.observed_at))
        .bind(direction.as_str())
        .bind(observed_at)
        .fetch_one(&mut *tx)
        .await?;

        let evidence_key = format!(
            "{}:{}:{}:{}",
            entity_kind.as_str(),
            entity_id,
            direction.as_str(),
            sequence
        );

        let mut prepared = 0usize;
        if !targets.is_empty() {
            let message = message.ok_or_else(|| {
                sqlx::Error::Protocol(
                    "fire_member_delta_crossing called with targets but no message".to_string(),
                )
            })?;
            let message_json = serde_json::to_value(message).map_err(json_to_sqlx)?;
            for subscription in targets {
                let delivery_id: Option<i64> = sqlx::query_scalar(
                    "WITH new_id AS (SELECT nextval(pg_get_serial_sequence('watchlist_deliveries', 'id')) AS id) \
                     INSERT INTO watchlist_deliveries (id, guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key, message, role_id, delivery_nonce, status) \
                     SELECT new_id.id, $1,$2,$3,$4,$5,$6,$7,$8, 'wl-' || new_id.id, 'prepared' FROM new_id \
                     WHERE EXISTS (SELECT 1 FROM watchlist_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3) \
                     ON CONFLICT (guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key) DO NOTHING RETURNING id",
                )
                .bind(subscription.guild_id as i64)
                .bind(subscription.channel_id as i64)
                .bind(&subscription.name)
                .bind(WatchlistEventKind::MemberDelta.as_str())
                .bind(entity_id)
                .bind(&evidence_key)
                .bind(&message_json)
                .bind(subscription.role_id.map(|id| id as i64))
                .fetch_optional(&mut *tx)
                .await?;
                if delivery_id.is_some() {
                    prepared += 1;
                }
            }
        }
        tx.commit().await?;
        Ok((evidence_key, prepared))
    }

    /// Deletes member snapshots older than the retention window, bounding
    /// table growth. Runs once per cycle.
    pub async fn prune_member_snapshots(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, sqlx::Error> {
        let cutoff = observed_at - MEMBER_SNAPSHOT_RETENTION;
        let result = sqlx::query("DELETE FROM watchlist_member_snapshots WHERE observed_at < $1")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    // --- Wars (ticket 10) ---

    /// The head-scan cursor. Defaults to a not-yet-established baseline at
    /// mark 0 before the first scan.
    pub async fn war_scan_state(&self) -> Result<WarScanState, sqlx::Error> {
        let row = sqlx::query(
            "SELECT high_water_mark, baseline_established, baseline_head_max, baseline_cursor, baseline_floor FROM watchlist_war_scan_state WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            Some(row) => WarScanState {
                high_water_mark: row.get::<i64, _>("high_water_mark"),
                baseline_established: row.get::<bool, _>("baseline_established"),
                baseline_head_max: row.get::<Option<i64>, _>("baseline_head_max"),
                baseline_cursor: row.get::<Option<i64>, _>("baseline_cursor"),
                baseline_floor: row.get::<Option<i64>, _>("baseline_floor"),
            },
            None => WarScanState {
                high_water_mark: 0,
                baseline_established: false,
                baseline_head_max: None,
                baseline_cursor: None,
                baseline_floor: None,
            },
        })
    }

    /// Records baseline progress after a cycle inspected part of the original
    /// head page without completing it. `head_max` and `floor` are stamped once
    /// (the first baseline cycle) and preserved; `cursor` is the lowest war id
    /// inspected so far. The high-water mark and `baseline_established` are left
    /// untouched (a pause or error must never flip the baseline).
    pub async fn record_war_baseline_progress(
        &self,
        head_max: i64,
        cursor: Option<i64>,
        floor: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_war_scan_state (singleton, high_water_mark, baseline_established, baseline_head_max, baseline_cursor, baseline_floor, updated_at) \
             VALUES (TRUE, 0, FALSE, $1, $2, $3, $4) \
             ON CONFLICT (singleton) DO UPDATE SET \
                 baseline_head_max = COALESCE(watchlist_war_scan_state.baseline_head_max, EXCLUDED.baseline_head_max), \
                 baseline_cursor = COALESCE(EXCLUDED.baseline_cursor, watchlist_war_scan_state.baseline_cursor), \
                 baseline_floor = COALESCE(watchlist_war_scan_state.baseline_floor, EXCLUDED.baseline_floor), \
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(head_max)
        .bind(cursor)
        .bind(floor)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Completes the silent baseline: sets `baseline_established`, stamps its
    /// timestamp, and sets the high-water mark to the head max observed on the
    /// first baseline cycle so the next scan processes everything above it.
    pub async fn complete_war_baseline(
        &self,
        head_max: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_war_scan_state (singleton, high_water_mark, baseline_established, baseline_established_at, baseline_cursor, baseline_floor, updated_at) \
             VALUES (TRUE, $1, TRUE, $2, NULL, NULL, $2) \
             ON CONFLICT (singleton) DO UPDATE SET \
                 high_water_mark = GREATEST(watchlist_war_scan_state.high_water_mark, EXCLUDED.high_water_mark), \
                 baseline_established = TRUE, \
                 baseline_established_at = COALESCE(watchlist_war_scan_state.baseline_established_at, EXCLUDED.baseline_established_at), \
                 baseline_cursor = NULL, \
                 baseline_floor = NULL, \
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(head_max)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Advances the high-water mark after a post-baseline scan processed a
    /// contiguous safe prefix of new ids (monotone via `GREATEST`).
    pub async fn record_war_scan_mark(
        &self,
        high_water_mark: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO watchlist_war_scan_state (singleton, high_water_mark, baseline_established, baseline_established_at, updated_at) \
             VALUES (TRUE, $1, TRUE, $2, $2) \
             ON CONFLICT (singleton) DO UPDATE SET \
                 high_water_mark = GREATEST(watchlist_war_scan_state.high_water_mark, EXCLUDED.high_water_mark), \
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(high_water_mark)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The stored observation of a war (ally set + whether it was already
    /// retracted/finished), or `None` when the war is not stored. Drives the
    /// re-fetch diff.
    pub async fn stored_war_observation(
        &self,
        war_id: i64,
    ) -> Result<Option<WarObservation>, sqlx::Error> {
        let row = sqlx::query("SELECT retracted, finished FROM watchlist_wars WHERE war_id = $1")
            .bind(war_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let allies =
            sqlx::query("SELECT kind, ally_id FROM watchlist_war_allies WHERE war_id = $1")
                .bind(war_id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| {
                    let kind_text: String = row.get("kind");
                    WatchlistKind::parse(&kind_text)
                        .map(|kind| WarParty {
                            kind,
                            id: row.get::<i64, _>("ally_id"),
                        })
                        .map_err(sqlx::Error::Protocol)
                })
                .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(WarObservation {
            allies,
            retracted: row.get::<Option<DateTime<Utc>>, _>("retracted").is_some(),
            finished: row.get::<Option<DateTime<Utc>>, _>("finished").is_some(),
        }))
    }

    /// Inserts or updates one stored war and inserts any newly-observed allies
    /// (never deleting an ally, so its `war_ally_joined` dedup key is stable).
    /// `first_seen_at` on the war row is preserved across updates.
    pub async fn upsert_war(
        &self,
        war: &WarDetail,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO watchlist_wars (war_id, aggressor_kind, aggressor_id, defender_kind, defender_id, declared, started, finished, retracted, mutual, open_for_allies, first_seen_at, last_fetched_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12) \
             ON CONFLICT (war_id) DO UPDATE SET \
                 declared = EXCLUDED.declared, started = EXCLUDED.started, finished = EXCLUDED.finished, \
                 retracted = EXCLUDED.retracted, mutual = EXCLUDED.mutual, open_for_allies = EXCLUDED.open_for_allies, \
                 last_fetched_at = EXCLUDED.last_fetched_at",
        )
        .bind(war.war_id)
        .bind(war.aggressor.kind.as_str())
        .bind(war.aggressor.id)
        .bind(war.defender.kind.as_str())
        .bind(war.defender.id)
        .bind(war.declared)
        .bind(war.started)
        .bind(war.finished)
        .bind(war.retracted)
        .bind(war.mutual)
        .bind(war.open_for_allies)
        .bind(observed_at)
        .execute(&mut *tx)
        .await?;
        for ally in &war.allies {
            sqlx::query(
                "INSERT INTO watchlist_war_allies (war_id, kind, ally_id, first_seen_at) VALUES ($1,$2,$3,$4) ON CONFLICT (war_id, kind, ally_id) DO NOTHING",
            )
            .bind(war.war_id)
            .bind(ally.kind.as_str())
            .bind(ally.id)
            .bind(observed_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Open stored wars (`finished IS NULL`) that are *currently watched*
    /// (some party -- aggressor, defender, or ally -- matches a
    /// `watchlist_entities` row of the same kind in any guild) and were last
    /// fetched strictly before `before`, ordered least-recently-fetched first,
    /// capped, for the bounded per-cycle re-fetch pass.
    ///
    /// "Watched" is derived here rather than stored, so an entity added after
    /// a war was stored starts re-fetching that war (its pre-existing open
    /// wars), and an entity that is unwatched drops out of the re-fetch set.
    /// The `before` bound excludes wars stored or re-fetched earlier in the
    /// same cycle (their `last_fetched_at` equals the cycle's `observed_at`),
    /// so a war seen this cycle is not immediately re-fetched and re-diffed.
    pub async fn open_watched_war_ids_least_recently_fetched(
        &self,
        limit: i64,
        before: DateTime<Utc>,
    ) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT w.war_id FROM watchlist_wars w \
             WHERE w.finished IS NULL AND w.last_fetched_at < $2 \
               AND ( \
                 EXISTS (SELECT 1 FROM watchlist_entities e \
                         WHERE (e.kind = w.aggressor_kind AND e.entity_id = w.aggressor_id) \
                            OR (e.kind = w.defender_kind AND e.entity_id = w.defender_id)) \
                 OR EXISTS (SELECT 1 FROM watchlist_war_allies a \
                            JOIN watchlist_entities e ON e.kind = a.kind AND e.entity_id = a.ally_id \
                            WHERE a.war_id = w.war_id) \
               ) \
             ORDER BY w.last_fetched_at ASC, w.war_id ASC LIMIT $1",
        )
        .bind(limit)
        .bind(before)
        .fetch_all(&self.pool)
        .await
    }

    /// Records one failed head-scan detail fetch for a war id, returning the
    /// new consecutive-failure count. `first_failed_at` is stamped once.
    pub async fn record_war_fetch_attempt(
        &self,
        war_id: i64,
        error: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<i32, sqlx::Error> {
        let truncated: String = error.chars().take(500).collect();
        sqlx::query_scalar::<_, i32>(
            "INSERT INTO watchlist_war_fetch_attempts (war_id, attempts, last_error, first_failed_at, updated_at) \
             VALUES ($1, 1, $2, $3, $3) \
             ON CONFLICT (war_id) DO UPDATE SET \
                 attempts = watchlist_war_fetch_attempts.attempts + 1, \
                 last_error = EXCLUDED.last_error, \
                 updated_at = EXCLUDED.updated_at \
             RETURNING attempts",
        )
        .bind(war_id)
        .bind(truncated)
        .bind(observed_at)
        .fetch_one(&self.pool)
        .await
    }

    /// Clears a war id's failure ledger (called after a successful fetch, or
    /// once a war id is permanently skipped so the row does not linger).
    pub async fn clear_war_fetch_attempts(&self, war_id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM watchlist_war_fetch_attempts WHERE war_id = $1")
            .bind(war_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Prunes finished wars whose `finished` timestamp is older than the
    /// retention window and that sit below the current head-scan mark (so a
    /// war still in the newest window is never dropped). Open wars are never
    /// pruned, keeping the table bounded to the open-war population plus wars
    /// finished within the window. Allies cascade.
    pub async fn prune_finished_wars(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<u64, sqlx::Error> {
        let cutoff = observed_at - WAR_RETENTION;
        let result = sqlx::query(
            "DELETE FROM watchlist_wars \
             WHERE finished IS NOT NULL AND finished < $1 \
               AND war_id < COALESCE((SELECT high_water_mark FROM watchlist_war_scan_state WHERE singleton = TRUE), 0)",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Bumps a war's `last_fetched_at` without otherwise changing it (used on a
    /// conditional `304`, so the least-recently-fetched ordering rotates).
    pub async fn mark_war_fetched(
        &self,
        war_id: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE watchlist_wars SET last_fetched_at = $2 WHERE war_id = $1")
            .bind(war_id)
            .bind(observed_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Marks a stored open war as gone after its detail re-fetch returned a
    /// `404` (CCP deleted it): stamps `finished`/`last_fetched_at` to
    /// `observed_at` so it drops out of the open-war re-fetch query
    /// (`finished IS NULL`) and becomes eligible for [`Self::prune_finished_wars`]
    /// once past retention. Reuses the `finished` column rather than adding a
    /// dedicated `removed_at`: it needs no schema change, the war is genuinely
    /// no longer open, and the collector emits no event for the removal so no
    /// spurious `war_finished` fires. Only touches rows still open so a real
    /// finish observed earlier is never overwritten.
    pub async fn mark_war_removed(
        &self,
        war_id: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE watchlist_wars SET finished = $2, last_fetched_at = $2 WHERE war_id = $1 AND finished IS NULL",
        )
        .bind(war_id)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn war_is_stored_for_test(&self, war_id: i64) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM watchlist_wars WHERE war_id = $1)",
        )
        .bind(war_id)
        .fetch_one(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn war_last_fetched_at_for_test(
        &self,
        war_id: i64,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT last_fetched_at FROM watchlist_wars WHERE war_id = $1",
        )
        .bind(war_id)
        .fetch_optional(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn war_is_open_for_test(&self, war_id: i64) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar::<_, bool>(
            "SELECT finished IS NULL FROM watchlist_wars WHERE war_id = $1",
        )
        .bind(war_id)
        .fetch_one(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn war_high_water_mark_for_test(&self) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT high_water_mark FROM watchlist_war_scan_state WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn war_baseline_established_for_test(&self) -> Result<bool, sqlx::Error> {
        Ok(self.war_scan_state().await?.baseline_established)
    }

    // --- Subscriptions ---

    pub async fn upsert_subscription(
        &self,
        subscription: &WatchlistSubscription,
    ) -> Result<(), sqlx::Error> {
        subscription.validate().map_err(sqlx::Error::Protocol)?;
        let event_kinds: Vec<String> = subscription
            .event_kinds
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect();
        sqlx::query(
            "INSERT INTO watchlist_subscriptions (guild_id, channel_id, name, event_kinds, role_id, ping_user_id, options) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (guild_id, channel_id, name) DO UPDATE SET event_kinds = EXCLUDED.event_kinds, role_id = EXCLUDED.role_id, ping_user_id = EXCLUDED.ping_user_id, options = EXCLUDED.options, updated_at = now()",
        )
        .bind(subscription.guild_id as i64)
        .bind(subscription.channel_id as i64)
        .bind(&subscription.name)
        .bind(&event_kinds)
        .bind(subscription.role_id.map(|id| id as i64))
        .bind(subscription.ping_user_id.map(|id| id as i64))
        .bind(&subscription.options)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn remove_subscription(
        &self,
        guild_id: u64,
        channel_id: u64,
        name: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "DELETE FROM watchlist_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// All subscriptions configured in one guild, used by the collector to
    /// fan an alliance event out to that guild's channels.
    pub async fn subscriptions_for_guild(
        &self,
        guild_id: u64,
    ) -> Result<Vec<WatchlistSubscription>, sqlx::Error> {
        sqlx::query(
            "SELECT guild_id, channel_id, name, event_kinds, role_id, ping_user_id, options FROM watchlist_subscriptions WHERE guild_id = $1 ORDER BY channel_id, name",
        )
        .bind(guild_id as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(watchlist_subscription_from_row)
        .collect()
    }

    // --- Deliveries ---

    pub async fn delivery_exists(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        event_kind: WatchlistEventKind,
        entity_id: i64,
        evidence_key: &str,
    ) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM watchlist_deliveries WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND event_kind = $4 AND entity_id = $5 AND evidence_key = $6)",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(event_kind.as_str())
        .bind(entity_id)
        .bind(evidence_key)
        .fetch_one(&self.pool)
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_delivery(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        event_kind: WatchlistEventKind,
        entity_id: i64,
        evidence_key: &str,
        role_id: Option<u64>,
        message: &WatchlistNotificationMessage,
    ) -> Result<bool, sqlx::Error> {
        // Row and `delivery_nonce` written atomically in a single INSERT so
        // no prepared row is ever nonce-less (mirrors the sov feed): a
        // concurrent claim could otherwise send a half-prepared row with an
        // empty nonce. The pre-allocated `id` feeds both the primary key and
        // the derived `'wl-' || id` nonce.
        let delivery_id: Option<i64> = sqlx::query_scalar(
            "WITH new_id AS (SELECT nextval(pg_get_serial_sequence('watchlist_deliveries', 'id')) AS id) \
             INSERT INTO watchlist_deliveries (id, guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key, message, role_id, delivery_nonce, status) \
             SELECT new_id.id, $1,$2,$3,$4,$5,$6,$7,$8, 'wl-' || new_id.id, 'prepared' FROM new_id \
             WHERE EXISTS (SELECT 1 FROM watchlist_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3) \
             ON CONFLICT (guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key) DO NOTHING RETURNING id",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(event_kind.as_str())
        .bind(entity_id)
        .bind(evidence_key)
        .bind(serde_json::to_value(message).map_err(json_to_sqlx)?)
        .bind(role_id.map(|id| id as i64))
        .fetch_optional(&self.pool)
        .await?;
        Ok(delivery_id.is_some())
    }

    pub async fn prepared_delivery_ids(&self) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT id FROM watchlist_deliveries WHERE status = 'prepared' ORDER BY prepared_at, id",
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn begin_delivery_attempt(
        &self,
        delivery_id: i64,
        attempted_at: DateTime<Utc>,
    ) -> Result<Option<PreparedWatchlistDelivery>, sqlx::Error> {
        let nonce_window_until = attempted_at + WATCHLIST_NONCE_ENFORCEMENT_WINDOW;
        let claim_token = format!("wl-delivery-{:032x}", rand::thread_rng().gen::<u128>());
        let lease_until = attempted_at + WATCHLIST_DELIVERY_LEASE;
        let row = sqlx::query(
            "UPDATE watchlist_deliveries SET attempt_count = attempt_count + 1, nonce_window_until = COALESCE(nonce_window_until, $2), delivery_nonce = COALESCE(delivery_nonce, 'wl-' || id), delivery_claim_token = $3, delivery_claimed_at = $4, delivery_lease_until = $5 WHERE id = $1 AND status = 'prepared' AND (delivery_lease_until IS NULL OR delivery_lease_until <= $4) RETURNING id, guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key, message, role_id, (SELECT s.ping_user_id FROM watchlist_subscriptions s WHERE s.guild_id = watchlist_deliveries.guild_id AND s.channel_id = watchlist_deliveries.channel_id AND s.name = watchlist_deliveries.subscription_name) AS ping_user_id, delivery_nonce, nonce_window_until, delivery_claim_token, attempt_count",
        )
        .bind(delivery_id)
        .bind(nonce_window_until)
        .bind(&claim_token)
        .bind(attempted_at)
        .bind(lease_until)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let nonce_window_until: Option<DateTime<Utc>> = row.get("nonce_window_until");
            let mut delivery = prepared_watchlist_delivery_from_row(row)?;
            delivery.enforce_nonce = nonce_window_until.is_some_and(|until| until > attempted_at);
            Ok(delivery)
        })
        .transpose()
    }

    pub async fn mark_delivery_sent(
        &self,
        delivery: &PreparedWatchlistDelivery,
        discord_message_id: &str,
    ) -> Result<(), sqlx::Error> {
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol(
                "watchlist delivery completion is missing its claim token".to_string(),
            )
        })?;
        let result = sqlx::query(
            "UPDATE watchlist_deliveries SET status = 'sent', discord_message_id = $2, sent_at = now(), delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $3",
        )
        .bind(delivery.delivery_id)
        .bind(discord_message_id)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "watchlist delivery {} was not claimed when Discord returned success",
                delivery.delivery_id
            )));
        }
        Ok(())
    }

    pub async fn record_delivery_failure(
        &self,
        delivery: &PreparedWatchlistDelivery,
        error: &WatchlistDeliveryError,
        failed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol(
                "watchlist delivery failure is missing its claim token".to_string(),
            )
        })?;
        if error.is_permanent() {
            sqlx::query(
                "UPDATE watchlist_deliveries SET status = 'failed', failure_kind = 'permanent', last_error = $2, failed_at = $3, delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $4",
            )
            .bind(delivery.delivery_id)
            .bind(&error.message)
            .bind(failed_at)
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        } else {
            let lease_until = failed_at + watchlist_transient_retry_backoff(delivery.attempt_count);
            sqlx::query(
                "UPDATE watchlist_deliveries SET failure_kind = 'transient', last_error = $2, failed_at = $3, delivery_lease_until = $4 WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $5",
            )
            .bind(delivery.delivery_id)
            .bind(&error.message)
            .bind(failed_at)
            .bind(lease_until)
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    // --- Test helpers ---

    #[doc(hidden)]
    pub async fn delivery_count(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT count(*) FROM watchlist_deliveries")
            .fetch_one(&self.pool)
            .await
    }

    #[doc(hidden)]
    pub async fn delivery_status_by_evidence(
        &self,
        event_kind: WatchlistEventKind,
        entity_id: i64,
        evidence_key: &str,
    ) -> Result<Option<String>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT status FROM watchlist_deliveries WHERE event_kind = $1 AND entity_id = $2 AND evidence_key = $3 LIMIT 1",
        )
        .bind(event_kind.as_str())
        .bind(entity_id)
        .bind(evidence_key)
        .fetch_optional(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn deliveries_for_entity(
        &self,
        event_kind: WatchlistEventKind,
        entity_id: i64,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT count(*) FROM watchlist_deliveries WHERE event_kind = $1 AND entity_id = $2",
        )
        .bind(event_kind.as_str())
        .bind(entity_id)
        .fetch_one(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn band_crossing_sequence_for_test(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
    ) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar::<_, i64>(
            "SELECT crossing_sequence FROM watchlist_member_band_state WHERE entity_kind = $1 AND entity_id = $2",
        )
        .bind(entity_kind.as_str())
        .bind(entity_id)
        .fetch_optional(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn count_deliveries_for_evidence(
        &self,
        event_kind: WatchlistEventKind,
        entity_id: i64,
        evidence_key: &str,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT count(*) FROM watchlist_deliveries WHERE event_kind = $1 AND entity_id = $2 AND evidence_key = $3",
        )
        .bind(event_kind.as_str())
        .bind(entity_id)
        .bind(evidence_key)
        .fetch_one(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn clear_delivery_nonce_for_test(&self, delivery_id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE watchlist_deliveries SET delivery_nonce = NULL WHERE id = $1")
            .bind(delivery_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn delivery_nonce_for_test(
        &self,
        delivery_id: i64,
    ) -> Result<Option<String>, sqlx::Error> {
        sqlx::query_scalar("SELECT delivery_nonce FROM watchlist_deliveries WHERE id = $1")
            .bind(delivery_id)
            .fetch_one(&self.pool)
            .await
    }
}

fn watched_entity_from_row(row: PgRow) -> Result<WatchedEntity, sqlx::Error> {
    let kind_text: String = row.get("kind");
    let kind = WatchlistKind::parse(&kind_text).map_err(sqlx::Error::Protocol)?;
    Ok(WatchedEntity {
        guild_id: row.get::<i64, _>("guild_id") as u64,
        kind,
        entity_id: row.get("entity_id"),
        ticker: row.get("ticker"),
        name: row.get("name"),
    })
}

fn watchlist_subscription_from_row(row: PgRow) -> Result<WatchlistSubscription, sqlx::Error> {
    let event_kinds_text: Vec<String> = row.get("event_kinds");
    let event_kinds: Vec<WatchlistEventKind> = event_kinds_text
        .iter()
        .filter_map(|token| WatchlistEventKind::parse(token))
        .collect();
    Ok(WatchlistSubscription {
        guild_id: row.get::<i64, _>("guild_id") as u64,
        channel_id: row.get::<i64, _>("channel_id") as u64,
        name: row.get("name"),
        event_kinds,
        role_id: row.get::<Option<i64>, _>("role_id").map(|id| id as u64),
        ping_user_id: row
            .get::<Option<i64>, _>("ping_user_id")
            .map(|id| id as u64),
        options: row.get("options"),
    })
}

fn prepared_watchlist_delivery_from_row(
    row: PgRow,
) -> Result<PreparedWatchlistDelivery, sqlx::Error> {
    Ok(PreparedWatchlistDelivery {
        delivery_id: row.get("id"),
        guild_id: row.get::<i64, _>("guild_id") as u64,
        channel_id: row.get::<i64, _>("channel_id") as u64,
        subscription_name: row.get("subscription_name"),
        event_kind: row.get("event_kind"),
        entity_id: row.get("entity_id"),
        evidence_key: row.get("evidence_key"),
        role_id: row.get::<Option<i64>, _>("role_id").map(|id| id as u64),
        ping_user_id: row
            .get::<Option<i64>, _>("ping_user_id")
            .map(|id| id as u64),
        message: serde_json::from_value(row.get("message")).map_err(json_to_sqlx)?,
        nonce: row
            .get::<Option<String>, _>("delivery_nonce")
            .unwrap_or_default(),
        enforce_nonce: false,
        attempt_count: row.get("attempt_count"),
        delivery_claim_token: row.get("delivery_claim_token"),
    })
}
