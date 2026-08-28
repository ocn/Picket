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
    PreparedWatchlistDelivery, WatchedEntity, WatchlistDeliveryError, WatchlistEventKind,
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

        // Fix round finding 4: `remove_entity` intentionally leaves the
        // alliance's membership snapshot and baseline marker in place (other
        // guilds may still watch it). But when an alliance is added while NO
        // guild currently watches it, those rows are stale from an earlier
        // watch that was fully removed; diffing the next listing against them
        // would flood the gap's churn. Reset the snapshot and baseline in the
        // same transaction so the next cycle re-establishes a silent baseline.
        // Do NOT reset when another guild already watches the alliance.
        if entity.kind.is_alliance() {
            let already_watched: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM watchlist_entities WHERE kind = 'alliance' AND entity_id = $1)",
            )
            .bind(entity.entity_id)
            .fetch_one(&mut *tx)
            .await?;
            if !already_watched {
                sqlx::query("DELETE FROM watchlist_alliance_corps WHERE alliance_id = $1")
                    .bind(entity.entity_id)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM watchlist_alliance_baseline WHERE alliance_id = $1")
                    .bind(entity.entity_id)
                    .execute(&mut *tx)
                    .await?;
                // Re-review finding: also drop the alliance's conditional-request
                // metadata. Otherwise a re-add within the listing's cache TTL
                // (or one answered by a 304) skips the fresh listing that would
                // re-establish the silent baseline, and the first real
                // membership change afterwards is folded into that deferred
                // baseline and lost.
                sqlx::query("DELETE FROM esi_cache_metadata WHERE resource_key = $1")
                    .bind(alliance_resource_key(entity.entity_id))
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
            "INSERT INTO watchlist_subscriptions (guild_id, channel_id, name, event_kinds, role_id, options) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (guild_id, channel_id, name) DO UPDATE SET event_kinds = EXCLUDED.event_kinds, role_id = EXCLUDED.role_id, options = EXCLUDED.options, updated_at = now()",
        )
        .bind(subscription.guild_id as i64)
        .bind(subscription.channel_id as i64)
        .bind(&subscription.name)
        .bind(&event_kinds)
        .bind(subscription.role_id.map(|id| id as i64))
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
            "SELECT guild_id, channel_id, name, event_kinds, role_id, options FROM watchlist_subscriptions WHERE guild_id = $1 ORDER BY channel_id, name",
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
            "UPDATE watchlist_deliveries SET attempt_count = attempt_count + 1, nonce_window_until = COALESCE(nonce_window_until, $2), delivery_nonce = COALESCE(delivery_nonce, 'wl-' || id), delivery_claim_token = $3, delivery_claimed_at = $4, delivery_lease_until = $5 WHERE id = $1 AND status = 'prepared' AND (delivery_lease_until IS NULL OR delivery_lease_until <= $4) RETURNING id, guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key, message, role_id, delivery_nonce, nonce_window_until, delivery_claim_token, attempt_count",
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
        message: serde_json::from_value(row.get("message")).map_err(json_to_sqlx)?,
        nonce: row
            .get::<Option<String>, _>("delivery_nonce")
            .unwrap_or_default(),
        enforce_nonce: false,
        attempt_count: row.get("attempt_count"),
        delivery_claim_token: row.get("delivery_claim_token"),
    })
}
