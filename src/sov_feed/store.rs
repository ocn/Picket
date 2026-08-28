//! PostgreSQL persistence for the sovereignty campaign feed.
//!
//! This store owns only `sov_*` tables and its own row in the shared
//! `esi_cache_metadata` table (keyed [`SOV_CAMPAIGNS_RESOURCE_KEY`]). It
//! deliberately does not touch `esi_collection_limiter_state`: limiter
//! coordination goes exclusively through the shared
//! [`crate::esi_cache::EsiLimiterStore`] trait object obtained at wiring
//! time in `lib.rs` (a `ContractCollectionStore`), per
//! `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`.
//!
//! `SovStore::connect` deliberately does *not* run migrations: the
//! background collection loop (`spawn_sov_collection_loop` in
//! `src/lib.rs`) applies them on every (re)connect attempt through
//! `contract_intelligence`'s shared `MIGRATOR` (which covers every file
//! under `migrations/`, including this feed's) before connecting a
//! `SovStore` for that cycle. Neither connect is awaited on the path to
//! starting the Discord client: Postgres being briefly unavailable at
//! boot must never delay the gateway connection or the killmail pipeline,
//! and the loop keeps retrying with backoff and reconnects automatically
//! once Postgres recovers (review finding 1 on
//! `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).

use crate::esi_cache::CacheMetadata;
use crate::sov_feed::chain::ChainSnapshot;
use crate::sov_feed::model::{
    PreparedSovDelivery, SovAlertStage, SovCampaign, SovDeliveryError, SovFilter, SovMapEntry,
    SovNotificationMessage, SovStructure, SovSubscription,
};
use crate::sov_feed::wanderer::{WandererConnection, WandererSystem};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The `esi_cache_metadata` resource key this feed's collector uses for
/// conditional requests against `/sovereignty/campaigns/`.
pub const SOV_CAMPAIGNS_RESOURCE_KEY: &str = "sovereignty/campaigns";

/// The `esi_cache_metadata` resource key for `/sovereignty/structures/`
/// (ticket 07). Sharing the `sovereignty/` prefix with
/// [`SOV_CAMPAIGNS_RESOURCE_KEY`] keeps this feed visible to the existing
/// `sov_esi_progress` health check, which matches `resource_key LIKE
/// 'sovereignty/%'` (`src/contract_intelligence.rs`).
pub const SOV_STRUCTURES_RESOURCE_KEY: &str = "sovereignty/structures";

/// The `esi_cache_metadata` resource key for `/sovereignty/map/` (ticket
/// 07). Same `sovereignty/` prefix rationale as
/// [`SOV_STRUCTURES_RESOURCE_KEY`].
pub const SOV_MAP_RESOURCE_KEY: &str = "sovereignty/map";

const SOV_NONCE_ENFORCEMENT_WINDOW: ChronoDuration = ChronoDuration::minutes(5);
const SOV_DELIVERY_LEASE: ChronoDuration = ChronoDuration::minutes(2);
/// Cap on the exponential backoff applied to a transient delivery
/// failure's retry lease, so `attempt_count` growth cannot push a stuck
/// delivery's next retry arbitrarily far into the future (review finding
/// 3).
const SOV_TRANSIENT_RETRY_MAX_BACKOFF: ChronoDuration = ChronoDuration::hours(2);

/// The lease-extension backoff for a transient delivery failure, scaled by
/// the claim's post-increment `attempt_count`: 2, 4, 8, 16, 32, 64 minutes.
/// The exponent clamp makes 64 minutes the effective ceiling;
/// `SOV_TRANSIENT_RETRY_MAX_BACKOFF` is only a defensive upper bound that the
/// ladder never reaches. `attempt_count` is always at least 1 by the time a
/// failure is recorded (`begin_delivery_attempt` increments it before the
/// send is attempted).
fn sov_transient_retry_backoff(attempt_count: i32) -> ChronoDuration {
    let exponent = attempt_count.clamp(1, 6) - 1;
    let minutes = 2_i64.saturating_mul(1_i64 << exponent);
    ChronoDuration::minutes(minutes).min(SOV_TRANSIENT_RETRY_MAX_BACKOFF)
}

fn json_to_sqlx(error: serde_json::Error) -> sqlx::Error {
    sqlx::Error::Protocol(error.to_string())
}

/// The outcome of one [`SovStore::record_reachability_observation`] call
/// (ticket 06; `pending_announcement` added in the fix round, finding 2).
/// Only [`Self::Observed`] with `reachable && pending_announcement` is
/// ever eligible to fire the `reachable` Alert Stage -- and the caller
/// must check that on *every* pass while it holds, not only the pass that
/// produced a fresh transition: a subscription like
/// `And(Reachable, VulnerableWithin(12h))` may not have its full filter
/// match until several passes after the `Reachable` leaf itself flipped,
/// and the announcement must not be lost in the meantime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SovReachabilityTransition {
    /// No prior row existed for this (subscription, campaign) pair: state
    /// is now established at `reachable`, already marked announced (spec:
    /// "reachable at first observation never fires", regardless of when
    /// the full filter might later match).
    Baseline { reachable: bool },
    /// A prior row existed. `reachable`/`transition_sequence` are the
    /// current persisted values after this observation.
    /// `pending_announcement` is `true` exactly when `reachable` is
    /// `true` and the `reachable:<transition_sequence>` alert for the
    /// current true-streak has not yet been marked announced (see
    /// [`SovStore::mark_reachability_announced`]) -- it stays `true`
    /// across as many `Unchanged`-shaped observations as it takes for the
    /// caller's full filter to match, not only on the observation that
    /// caused the flip.
    Observed {
        reachable: bool,
        transition_sequence: i64,
        pending_announcement: bool,
    },
}

/// The outcome of one [`SovStore::record_tz_window_observation`] call
/// (ticket 07). Mirrors [`SovReachabilityTransition`] exactly, substituting
/// "in window" for "reachable" -- see that type's doc comment for the full
/// rationale, which applies unchanged here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SovTzWindowTransition {
    /// No prior row existed for this (subscription, structure) pair
    /// (Sov Baseline: "hubs present in the first successful cycle
    /// establish state silently").
    Baseline { in_window: bool },
    /// A prior row existed; `pending_announcement` is `true` exactly when
    /// `in_window` is `true` and the `tz_window_entered:<sequence>` alert
    /// for the current in-window streak has not yet been marked announced.
    Observed {
        in_window: bool,
        transition_sequence: i64,
        pending_announcement: bool,
    },
}

#[derive(Clone)]
pub struct SovStore {
    pool: PgPool,
}

/// A late-bound handle to the sov store, mirroring
/// `contract_intelligence::ContractStoreHandle`: the composition root
/// inserts an empty handle into the Discord command TypeMap immediately
/// (before any database connect), and the background collection loop
/// (`spawn_sov_collection_loop` in `src/lib.rs`) populates it once it
/// successfully connects, clearing it again if the database becomes
/// unavailable. Commands read through [`available_sov_store`] and answer
/// "not connected" rather than panicking when it is empty (review finding
/// 1 on `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
pub type SovStoreHandle = Arc<RwLock<Option<Arc<SovStore>>>>;

pub fn new_sov_store_handle() -> SovStoreHandle {
    Arc::new(RwLock::new(None))
}

pub async fn available_sov_store(handle: &SovStoreHandle) -> Option<Arc<SovStore>> {
    handle.read().await.clone()
}

impl SovStore {
    /// Connects a pool without running migrations. Callers must have
    /// already applied migrations (e.g. via
    /// `ContractCollectionStore::connect`) against the same database.
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

    // --- ESI cache metadata (own resource key; shared table) ---

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

    // --- Chain snapshots (ticket 05) ---

    /// Persists a freshly-fetched Wanderer chain observation. Returns its
    /// row ID (unused by any caller today, but a natural companion to
    /// `upsert_campaign`'s return shape and useful for future debugging
    /// queries).
    pub async fn insert_chain_snapshot(
        &self,
        snapshot: &ChainSnapshot,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "INSERT INTO sov_chain_snapshots (fetched_at, systems, connections) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(snapshot.fetched_at)
        .bind(serde_json::to_value(&snapshot.systems).map_err(json_to_sqlx)?)
        .bind(serde_json::to_value(&snapshot.connections).map_err(json_to_sqlx)?)
        .fetch_one(&self.pool)
        .await
    }

    /// The most recently fetched chain snapshot, or `None` before the
    /// first successful fetch. Used by the poll loop to diff the newly
    /// fetched connection list against what was persisted last cycle
    /// (`connection_topology_changed`) before inserting the new snapshot.
    pub async fn latest_chain_snapshot(&self) -> Result<Option<ChainSnapshot>, sqlx::Error> {
        sqlx::query(
            "SELECT fetched_at, systems, connections FROM sov_chain_snapshots ORDER BY fetched_at DESC, id DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(chain_snapshot_from_row)
        .transpose()
    }

    /// `max(fetched_at)` across every persisted snapshot -- the sov chain
    /// feed's health-check and `/sov_timers` staleness signal doubles as
    /// "last successful fetch" because a row only ever exists after a
    /// successful fetch (see the migration's doc comment).
    pub async fn chain_progress_at(&self) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar("SELECT max(fetched_at) FROM sov_chain_snapshots")
            .fetch_one(&self.pool)
            .await
    }

    /// Bounded retention (spec: "Retain a bounded history ... decide and
    /// document" -- this feed keeps the last 24 hours, always preserving
    /// at least the single newest row regardless of its age, so a long
    /// Wanderer outage followed by recovery never leaves the table
    /// briefly empty). Called by the poll loop after every successful
    /// insert.
    pub async fn prune_chain_snapshots(
        &self,
        older_than: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "DELETE FROM sov_chain_snapshots WHERE fetched_at < $1 AND id <> (SELECT id FROM sov_chain_snapshots ORDER BY fetched_at DESC, id DESC LIMIT 1)",
        )
        .bind(older_than)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // --- Campaigns and Sov Baseline ---

    pub async fn open_campaign_ids(&self) -> Result<HashSet<i64>, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT campaign_id FROM sov_campaigns WHERE ended_at IS NULL",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect())
    }

    /// All non-ended campaigns with their full facts (baseline and
    /// non-baseline alike), used by the stage-evaluation pass
    /// (`SovCollector::evaluate_stage_cycle`) to check T-minus marks
    /// purely against persisted state and the controlled clock, with no
    /// ESI request (ticket 03: "Baseline campaigns get marks even though
    /// they never got `appeared`").
    pub async fn open_campaigns(&self) -> Result<Vec<SovCampaign>, sqlx::Error> {
        sqlx::query(
            "SELECT campaign_id, event_type, structure_id, solar_system_id, constellation_id, defender_id, defender_score, attackers_score, start_time FROM sov_campaigns WHERE ended_at IS NULL",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(sov_campaign_from_row)
        .collect()
    }

    pub async fn baseline_established(&self) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sov_collector_baseline)")
            .fetch_one(&self.pool)
            .await
    }

    pub async fn establish_baseline(&self, observed_at: DateTime<Utc>) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO sov_collector_baseline (singleton, established_at) VALUES (TRUE, $1) ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Upserts one campaign's facts and bumps `last_seen_at`. Returns
    /// `(inserted, is_baseline)`: `inserted` is `true` only when this
    /// campaign was not previously known; `is_baseline` is the row's
    /// *persisted* baseline flag after this upsert (preserved across
    /// updates, set only at insert time). Callers must gate `appeared` on
    /// `is_baseline` alone, re-evaluated every cycle — not on `inserted` —
    /// so a campaign whose start time is initially outside the alert
    /// window still alerts once it later falls inside it (review finding
    /// 2 on `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
    pub async fn upsert_campaign(
        &self,
        campaign: &SovCampaign,
        is_baseline_for_new_rows: bool,
        observed_at: DateTime<Utc>,
    ) -> Result<(bool, bool), sqlx::Error> {
        sqlx::query_as(
            "INSERT INTO sov_campaigns (campaign_id, event_type, structure_id, solar_system_id, constellation_id, defender_id, defender_score, attackers_score, start_time, is_baseline, first_seen_at, last_seen_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11) ON CONFLICT (campaign_id) DO UPDATE SET event_type = EXCLUDED.event_type, structure_id = EXCLUDED.structure_id, solar_system_id = EXCLUDED.solar_system_id, constellation_id = EXCLUDED.constellation_id, defender_id = EXCLUDED.defender_id, defender_score = EXCLUDED.defender_score, attackers_score = EXCLUDED.attackers_score, start_time = EXCLUDED.start_time, last_seen_at = EXCLUDED.last_seen_at, ended_at = NULL RETURNING (xmax = 0) AS inserted, is_baseline",
        )
        .bind(campaign.campaign_id)
        .bind(&campaign.event_type)
        .bind(campaign.structure_id)
        .bind(campaign.solar_system_id)
        .bind(campaign.constellation_id)
        .bind(campaign.defender_id)
        .bind(campaign.defender_score)
        .bind(campaign.attackers_score)
        .bind(campaign.start_time)
        .bind(is_baseline_for_new_rows)
        .bind(observed_at)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn mark_ended(
        &self,
        campaign_ids: &[i64],
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        if campaign_ids.is_empty() {
            return Ok(());
        }
        // One transaction (fix round finding 4): the `ended_at` update and
        // the reachability-state prune are two facts about the same event
        // ("this campaign just ended") and must become visible together,
        // not as two independently-committed writes a concurrent reader
        // could observe half-applied.
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE sov_campaigns SET ended_at = $1 WHERE campaign_id = ANY($2) AND ended_at IS NULL",
        )
        .bind(observed_at)
        .bind(campaign_ids)
        .execute(&mut *tx)
        .await?;
        // Reachability state may be pruned along with its campaign (ticket
        // 06: "Ended campaigns are skipped and their state may be pruned
        // with the campaign") -- `evaluate_stage_cycle` only ever reads
        // `open_campaigns()`, so a state row surviving past its campaign's
        // end would just be dead weight, never a correctness problem; this
        // keeps the table from growing unbounded regardless.
        sqlx::query("DELETE FROM sov_reachability_state WHERE campaign_id = ANY($1)")
            .bind(campaign_ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn campaign_ended_at(
        &self,
        campaign_id: i64,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar("SELECT ended_at FROM sov_campaigns WHERE campaign_id = $1")
            .bind(campaign_id)
            .fetch_one(&self.pool)
            .await
    }

    #[doc(hidden)]
    pub async fn campaign_is_baseline(&self, campaign_id: i64) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT is_baseline FROM sov_campaigns WHERE campaign_id = $1")
            .bind(campaign_id)
            .fetch_one(&self.pool)
            .await
    }

    // --- Subscriptions ---

    pub async fn upsert_subscription(
        &self,
        subscription: &SovSubscription,
    ) -> Result<(), sqlx::Error> {
        subscription.validate().map_err(sqlx::Error::Protocol)?;
        sqlx::query(
            "INSERT INTO sov_subscriptions (guild_id, channel_id, name, filter, options, role_id) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (guild_id, channel_id, name) DO UPDATE SET filter = EXCLUDED.filter, options = EXCLUDED.options, role_id = EXCLUDED.role_id, updated_at = now()",
        )
        .bind(subscription.guild_id as i64)
        .bind(subscription.channel_id as i64)
        .bind(&subscription.name)
        .bind(serde_json::to_value(&subscription.filter).map_err(json_to_sqlx)?)
        .bind(&subscription.options)
        .bind(subscription.role_id.map(|id| id as i64))
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
            "DELETE FROM sov_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn subscriptions(&self) -> Result<Vec<SovSubscription>, sqlx::Error> {
        sqlx::query(
            "SELECT guild_id, channel_id, name, filter, options, role_id FROM sov_subscriptions ORDER BY guild_id, channel_id, name",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(sov_subscription_from_row)
        .collect()
    }

    /// All subscriptions configured for one channel, used by `/sov_timers`
    /// to evaluate the live board against exactly this channel's filters
    /// rather than every guild's.
    pub async fn subscriptions_for_channel(
        &self,
        guild_id: u64,
        channel_id: u64,
    ) -> Result<Vec<SovSubscription>, sqlx::Error> {
        sqlx::query(
            "SELECT guild_id, channel_id, name, filter, options, role_id FROM sov_subscriptions WHERE guild_id = $1 AND channel_id = $2 ORDER BY name",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(sov_subscription_from_row)
        .collect()
    }

    /// Looks up one subscription by its primary key, or `None` when it
    /// does not exist yet. Used by `/sov_subscribe` to read a
    /// previously-stored `options` document before merging in this
    /// invocation's changes, so re-running the command without
    /// `tminus_marks` preserves custom marks instead of resetting them to
    /// the default (review finding 2 on ticket 03).
    pub async fn subscription(
        &self,
        guild_id: u64,
        channel_id: u64,
        name: &str,
    ) -> Result<Option<SovSubscription>, sqlx::Error> {
        sqlx::query(
            "SELECT guild_id, channel_id, name, filter, options, role_id FROM sov_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .map(sov_subscription_from_row)
        .transpose()
    }

    // --- Reachability transition state (ticket 06) ---

    /// Records one subscription's observation of whether a campaign is
    /// currently reachable (per that subscription's own `Reachable` leaf,
    /// via `SovFilterNode::reachable_leaf_state` -- the caller must have
    /// already checked `has_reachable_leaf` before calling this at all)
    /// and reports what kind of transition, if any, this observation is.
    ///
    /// A `SELECT ... FOR UPDATE` inside an explicit transaction reads the
    /// prior row (if any) before deciding how to write the new one, so the
    /// "was this a false->true flip" decision and the persisted write are
    /// atomic with respect to each other -- there is exactly one caller
    /// today (`SovCollector::evaluate_stage_cycle`'s single loop), but this
    /// keeps the state machine correct if that ever changes.
    pub async fn record_reachability_observation(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        campaign_id: i64,
        reachable_now: bool,
        observed_at: DateTime<Utc>,
    ) -> Result<SovReachabilityTransition, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT reachable, transition_sequence, announced FROM sov_reachability_state WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND campaign_id = $4 FOR UPDATE",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(campaign_id)
        .fetch_optional(&mut *tx)
        .await?;

        let transition = match existing {
            None => {
                // First observation: establish the row, already
                // `announced` -- "reachable at first observation never
                // fires" (spec), regardless of when the full filter might
                // later match.
                sqlx::query(
                    "INSERT INTO sov_reachability_state (guild_id, channel_id, subscription_name, campaign_id, reachable, since, transition_sequence, announced, updated_at) VALUES ($1,$2,$3,$4,$5,$6,0,TRUE,now())",
                )
                .bind(guild_id as i64)
                .bind(channel_id as i64)
                .bind(subscription_name)
                .bind(campaign_id)
                .bind(reachable_now)
                .bind(observed_at)
                .execute(&mut *tx)
                .await?;
                SovReachabilityTransition::Baseline {
                    reachable: reachable_now,
                }
            }
            Some(row) => {
                let previous_reachable: bool = row.get("reachable");
                let previous_sequence: i64 = row.get("transition_sequence");
                let previous_announced: bool = row.get("announced");
                if previous_reachable == reachable_now {
                    // Unchanged: no write needed. `pending_announcement`
                    // still reflects the stored `announced` flag, so a
                    // caller whose full filter did not match on the pass
                    // that produced the original transition keeps getting
                    // a chance to fire on every later pass while it stays
                    // reachable (fix round finding 2).
                    SovReachabilityTransition::Observed {
                        reachable: reachable_now,
                        transition_sequence: previous_sequence,
                        pending_announcement: reachable_now && !previous_announced,
                    }
                } else if reachable_now {
                    // false -> true: a genuine transition. Saturating so a
                    // pathologically long-lived campaign flapping forever
                    // can never overflow (bounded, non-panicking
                    // arithmetic, mirroring every other counter in this
                    // feed). `announced` resets to FALSE: this fresh
                    // true-streak has not been announced yet.
                    let new_sequence = previous_sequence.saturating_add(1);
                    sqlx::query(
                        "UPDATE sov_reachability_state SET reachable = TRUE, since = $5, transition_sequence = $6, announced = FALSE, updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND campaign_id = $4",
                    )
                    .bind(guild_id as i64)
                    .bind(channel_id as i64)
                    .bind(subscription_name)
                    .bind(campaign_id)
                    .bind(observed_at)
                    .bind(new_sequence)
                    .execute(&mut *tx)
                    .await?;
                    SovReachabilityTransition::Observed {
                        reachable: true,
                        transition_sequence: new_sequence,
                        pending_announcement: true,
                    }
                } else {
                    // true -> false: never fires an alert (no retraction
                    // events), but re-arms the next false->true flip since
                    // the sequence is left unchanged for now and only
                    // bumped on the next reachable transition. `announced`
                    // resets to TRUE: nothing is pending while unreachable
                    // (whether or not the prior true-streak ever actually
                    // fired), and the next false->true flip starts its own
                    // fresh `announced = FALSE`.
                    sqlx::query(
                        "UPDATE sov_reachability_state SET reachable = FALSE, since = $5, announced = TRUE, updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND campaign_id = $4",
                    )
                    .bind(guild_id as i64)
                    .bind(channel_id as i64)
                    .bind(subscription_name)
                    .bind(campaign_id)
                    .bind(observed_at)
                    .execute(&mut *tx)
                    .await?;
                    SovReachabilityTransition::Observed {
                        reachable: false,
                        transition_sequence: previous_sequence,
                        pending_announcement: false,
                    }
                }
            }
        };
        tx.commit().await?;
        Ok(transition)
    }

    /// Marks the current `reachable:<transition_sequence>` announcement as
    /// done, so future passes stop re-attempting the full-filter check for
    /// it (fix round finding 2). Keyed by `transition_sequence` as well as
    /// the row's primary key so a call racing behind a newer transition
    /// (already bumped the sequence again) can never mark the *new*
    /// transition announced by mistake -- it simply matches zero rows.
    /// Called once the caller has confirmed (via `prepare_delivery`,
    /// whether freshly inserted or already existing from a prior attempt)
    /// that a durable delivery row for this exact stage exists; the
    /// delivery's own dedup/lease machinery takes it from there, so this
    /// flag only needs to be "eventually true", not part of the delivery
    /// dedup authority itself.
    pub async fn mark_reachability_announced(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        campaign_id: i64,
        transition_sequence: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE sov_reachability_state SET announced = TRUE, updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND campaign_id = $4 AND transition_sequence = $5",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(campaign_id)
        .bind(transition_sequence)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn reachability_state_for_test(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        campaign_id: i64,
    ) -> Result<Option<(bool, DateTime<Utc>, i64, bool)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT reachable, since, transition_sequence, announced FROM sov_reachability_state WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND campaign_id = $4",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(campaign_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| {
            (
                row.get("reachable"),
                row.get("since"),
                row.get("transition_sequence"),
                row.get("announced"),
            )
        }))
    }

    // --- Sovereignty Hubs and Sov Baseline (ticket 07) ---

    pub async fn structures_baseline_established(&self) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sov_structures_baseline)")
            .fetch_one(&self.pool)
            .await
    }

    pub async fn establish_structures_baseline(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO sov_structures_baseline (singleton, established_at) VALUES (TRUE, $1) ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn known_structure_ids(&self) -> Result<HashSet<i64>, sqlx::Error> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT structure_id FROM sov_structures")
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .collect(),
        )
    }

    /// Upserts one Sovereignty Hub's facts and bumps `last_seen_at`.
    /// Returns `(inserted, is_baseline)`, mirroring [`Self::upsert_campaign`]
    /// exactly (`is_baseline` is the row's persisted flag, preserved across
    /// updates, set only at insert time).
    pub async fn upsert_structure(
        &self,
        structure: &SovStructure,
        is_baseline_for_new_rows: bool,
        observed_at: DateTime<Utc>,
    ) -> Result<(bool, bool), sqlx::Error> {
        sqlx::query_as(
            "INSERT INTO sov_structures (structure_id, structure_type_id, alliance_id, solar_system_id, vulnerability_occupancy_level, vulnerable_start_time, vulnerable_end_time, is_baseline, first_seen_at, last_seen_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9) ON CONFLICT (structure_id) DO UPDATE SET structure_type_id = EXCLUDED.structure_type_id, alliance_id = EXCLUDED.alliance_id, solar_system_id = EXCLUDED.solar_system_id, vulnerability_occupancy_level = EXCLUDED.vulnerability_occupancy_level, vulnerable_start_time = EXCLUDED.vulnerable_start_time, vulnerable_end_time = EXCLUDED.vulnerable_end_time, last_seen_at = EXCLUDED.last_seen_at RETURNING (xmax = 0) AS inserted, is_baseline",
        )
        .bind(structure.structure_id)
        .bind(structure.structure_type_id)
        .bind(structure.alliance_id)
        .bind(structure.solar_system_id)
        .bind(structure.vulnerability_occupancy_level)
        .bind(structure.vulnerable_start_time)
        .bind(structure.vulnerable_end_time)
        .bind(is_baseline_for_new_rows)
        .bind(observed_at)
        .fetch_one(&self.pool)
        .await
    }

    /// Removes hubs no longer present in a fresh listing (destroyed, or
    /// sov lost -- there is no "ended" fact worth keeping for a hub, only
    /// "currently exists"). Cascades into `sov_tz_window_state` via that
    /// table's `structure_id` foreign key.
    pub async fn remove_structures(&self, structure_ids: &[i64]) -> Result<(), sqlx::Error> {
        if structure_ids.is_empty() {
            return Ok(());
        }
        sqlx::query("DELETE FROM sov_structures WHERE structure_id = ANY($1)")
            .bind(structure_ids)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every currently known Sovereignty Hub (baseline and non-baseline
    /// alike), used by the `tz_window_entered` stage-evaluation pass.
    pub async fn open_structures(&self) -> Result<Vec<SovStructure>, sqlx::Error> {
        sqlx::query(
            "SELECT structure_id, structure_type_id, alliance_id, solar_system_id, vulnerability_occupancy_level, vulnerable_start_time, vulnerable_end_time FROM sov_structures",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(sov_structure_from_row)
        .collect()
    }

    // --- Sovereignty map (ticket 07) ---

    /// Replaces `sov_map` wholesale with a fresh listing: upserts every
    /// entry, then deletes any system absent from `entries` -- the feed
    /// only needs "current owner", never map history (spec "Embed":
    /// "current owner from the sovereignty map").
    pub async fn replace_map(
        &self,
        entries: &[SovMapEntry],
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        for entry in entries {
            sqlx::query(
                "INSERT INTO sov_map (solar_system_id, alliance_id, corporation_id, faction_id, updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (solar_system_id) DO UPDATE SET alliance_id = EXCLUDED.alliance_id, corporation_id = EXCLUDED.corporation_id, faction_id = EXCLUDED.faction_id, updated_at = EXCLUDED.updated_at",
            )
            .bind(entry.solar_system_id)
            .bind(entry.alliance_id)
            .bind(entry.corporation_id)
            .bind(entry.faction_id)
            .bind(observed_at)
            .execute(&mut *tx)
            .await?;
        }
        let current_ids: Vec<i64> = entries.iter().map(|entry| entry.solar_system_id).collect();
        sqlx::query("DELETE FROM sov_map WHERE solar_system_id <> ALL($1)")
            .bind(&current_ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The current sovereignty owner of one system, or `None` when the
    /// system carries no sovereignty (never in `sov_map`) or the map has
    /// not been fetched yet.
    pub async fn map_owner(
        &self,
        solar_system_id: i64,
    ) -> Result<Option<SovMapEntry>, sqlx::Error> {
        sqlx::query(
            "SELECT solar_system_id, alliance_id, corporation_id, faction_id FROM sov_map WHERE solar_system_id = $1",
        )
        .bind(solar_system_id)
        .fetch_optional(&self.pool)
        .await?
        .map(sov_map_entry_from_row)
        .transpose()
    }

    // --- Timezone window transition state (ticket 07) ---

    /// Records one subscription's observation of whether one Sovereignty
    /// Hub's vulnerability window currently overlaps that subscription's
    /// timezone window by at least [`crate::sov_feed::model::SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES`]
    /// and reports what kind of transition, if any, this observation is.
    /// Mirrors [`Self::record_reachability_observation`] exactly --
    /// substitute "in window" for "reachable" throughout its doc comment.
    pub async fn record_tz_window_observation(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        structure_id: i64,
        in_window_now: bool,
        observed_at: DateTime<Utc>,
    ) -> Result<SovTzWindowTransition, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT in_window, transition_sequence, announced FROM sov_tz_window_state WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND structure_id = $4 FOR UPDATE",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(structure_id)
        .fetch_optional(&mut *tx)
        .await?;

        let transition = match existing {
            None => {
                sqlx::query(
                    "INSERT INTO sov_tz_window_state (guild_id, channel_id, subscription_name, structure_id, in_window, since, transition_sequence, announced, updated_at) VALUES ($1,$2,$3,$4,$5,$6,0,TRUE,now())",
                )
                .bind(guild_id as i64)
                .bind(channel_id as i64)
                .bind(subscription_name)
                .bind(structure_id)
                .bind(in_window_now)
                .bind(observed_at)
                .execute(&mut *tx)
                .await?;
                SovTzWindowTransition::Baseline {
                    in_window: in_window_now,
                }
            }
            Some(row) => {
                let previous_in_window: bool = row.get("in_window");
                let previous_sequence: i64 = row.get("transition_sequence");
                let previous_announced: bool = row.get("announced");
                if previous_in_window == in_window_now {
                    SovTzWindowTransition::Observed {
                        in_window: in_window_now,
                        transition_sequence: previous_sequence,
                        pending_announcement: in_window_now && !previous_announced,
                    }
                } else if in_window_now {
                    let new_sequence = previous_sequence.saturating_add(1);
                    sqlx::query(
                        "UPDATE sov_tz_window_state SET in_window = TRUE, since = $5, transition_sequence = $6, announced = FALSE, updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND structure_id = $4",
                    )
                    .bind(guild_id as i64)
                    .bind(channel_id as i64)
                    .bind(subscription_name)
                    .bind(structure_id)
                    .bind(observed_at)
                    .bind(new_sequence)
                    .execute(&mut *tx)
                    .await?;
                    SovTzWindowTransition::Observed {
                        in_window: true,
                        transition_sequence: new_sequence,
                        pending_announcement: true,
                    }
                } else {
                    sqlx::query(
                        "UPDATE sov_tz_window_state SET in_window = FALSE, since = $5, announced = TRUE, updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND structure_id = $4",
                    )
                    .bind(guild_id as i64)
                    .bind(channel_id as i64)
                    .bind(subscription_name)
                    .bind(structure_id)
                    .bind(observed_at)
                    .execute(&mut *tx)
                    .await?;
                    SovTzWindowTransition::Observed {
                        in_window: false,
                        transition_sequence: previous_sequence,
                        pending_announcement: false,
                    }
                }
            }
        };
        tx.commit().await?;
        Ok(transition)
    }

    /// Marks the current `tz_window_entered:<transition_sequence>`
    /// announcement as done. Mirrors [`Self::mark_reachability_announced`]
    /// exactly, including the "keyed by `transition_sequence` too, so a
    /// call racing behind a newer transition matches zero rows" guard.
    pub async fn mark_tz_window_announced(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        structure_id: i64,
        transition_sequence: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE sov_tz_window_state SET announced = TRUE, updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND structure_id = $4 AND transition_sequence = $5",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(structure_id)
        .bind(transition_sequence)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn tz_window_state_for_test(
        &self,
        guild_id: u64,
        channel_id: u64,
        subscription_name: &str,
        structure_id: i64,
    ) -> Result<Option<(bool, DateTime<Utc>, i64, bool)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT in_window, since, transition_sequence, announced FROM sov_tz_window_state WHERE guild_id = $1 AND channel_id = $2 AND subscription_name = $3 AND structure_id = $4",
        )
        .bind(guild_id as i64)
        .bind(channel_id as i64)
        .bind(subscription_name)
        .bind(structure_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| {
            (
                row.get("in_window"),
                row.get("since"),
                row.get("transition_sequence"),
                row.get("announced"),
            )
        }))
    }

    // --- Deliveries ---

    /// Inserts a prepared delivery if none exists yet for
    /// `(subscription, subject_kind, subject_id, stage)`. Idempotent: a
    /// duplicate call for the same identity is a no-op, which is the dedup
    /// authority for "never post the same stage twice" (the collector may
    /// call this every cycle for a still-eligible campaign or structure;
    /// only a genuinely fresh row returns `true`). `subject_kind` is
    /// `"campaign"` for every campaign-shaped Alert Stage
    /// (`appeared`/`tminus`/`reachable`) and `"structure"` for
    /// `tz_window_entered` (ticket 07, spec "Alert stages and dedup": "the
    /// delivery table already has `subject_kind`; use
    /// `tz_window_entered:<seq>`").
    pub async fn prepare_delivery(
        &self,
        subscription: &SovSubscription,
        subject_kind: &str,
        subject_id: i64,
        stage: SovAlertStage,
        message: &SovNotificationMessage,
    ) -> Result<bool, sqlx::Error> {
        let delivery_id: Option<i64> = sqlx::query_scalar(
            "INSERT INTO sov_alert_deliveries (guild_id, channel_id, subscription_name, subject_kind, subject_id, stage, message, role_id, status) SELECT $1,$2,$3,$4,$5,$6,$7,$8,'prepared' WHERE EXISTS (SELECT 1 FROM sov_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3) ON CONFLICT (guild_id, channel_id, subscription_name, subject_kind, subject_id, stage) DO NOTHING RETURNING id",
        )
        .bind(subscription.guild_id as i64)
        .bind(subscription.channel_id as i64)
        .bind(&subscription.name)
        .bind(subject_kind)
        .bind(subject_id)
        .bind(stage.as_str())
        .bind(serde_json::to_value(message).map_err(json_to_sqlx)?)
        .bind(subscription.role_id.map(|id| id as i64))
        .fetch_optional(&self.pool)
        .await?;
        if let Some(delivery_id) = delivery_id {
            sqlx::query("UPDATE sov_alert_deliveries SET delivery_nonce = $2 WHERE id = $1")
                .bind(delivery_id)
                .bind(format!("sov-{delivery_id}"))
                .execute(&self.pool)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn prepared_delivery_ids(&self) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT id FROM sov_alert_deliveries WHERE status = 'prepared' ORDER BY prepared_at, id",
        )
        .fetch_all(&self.pool)
        .await
    }

    /// Claims a prepared delivery with a fresh lease, or returns `None`
    /// when it is already sent or its lease is still held by another
    /// attempt. Unexpired leases are left alone across restarts;
    /// expired-lease prepared rows are re-claimed by the next caller.
    pub async fn begin_delivery_attempt(
        &self,
        delivery_id: i64,
        attempted_at: DateTime<Utc>,
    ) -> Result<Option<PreparedSovDelivery>, sqlx::Error> {
        let nonce_window_until = attempted_at + SOV_NONCE_ENFORCEMENT_WINDOW;
        let claim_token = format!("sov-delivery-{:032x}", rand::thread_rng().gen::<u128>());
        let lease_until = attempted_at + SOV_DELIVERY_LEASE;
        let row = sqlx::query(
            "UPDATE sov_alert_deliveries SET attempt_count = attempt_count + 1, nonce_window_until = COALESCE(nonce_window_until, $2), delivery_claim_token = $3, delivery_claimed_at = $4, delivery_lease_until = $5 WHERE id = $1 AND status = 'prepared' AND (delivery_lease_until IS NULL OR delivery_lease_until <= $4) RETURNING id, guild_id, channel_id, subscription_name, subject_kind, subject_id, stage, message, role_id, delivery_nonce, nonce_window_until, delivery_claim_token, attempt_count",
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
            let mut delivery = prepared_sov_delivery_from_row(row)?;
            delivery.enforce_nonce = nonce_window_until.is_some_and(|until| until > attempted_at);
            Ok(delivery)
        })
        .transpose()
    }

    pub async fn mark_delivery_sent(
        &self,
        delivery: &PreparedSovDelivery,
        discord_message_id: &str,
    ) -> Result<(), sqlx::Error> {
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("sov delivery completion is missing its claim token".to_string())
        })?;
        let result = sqlx::query(
            "UPDATE sov_alert_deliveries SET status = 'sent', discord_message_id = $2, sent_at = now(), delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $3",
        )
        .bind(delivery.delivery_id)
        .bind(discord_message_id)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "sov delivery {} was not claimed when Discord returned success",
                delivery.delivery_id
            )));
        }
        Ok(())
    }

    /// Records a delivery attempt's failure. Permanent failures leave the
    /// `prepared` state for good (`status = 'failed'`, claim cleared, never
    /// re-claimed). Transient failures stay `prepared` but their lease is
    /// pushed out by an attempt-count-scaled backoff so the next reclaim
    /// waits rather than retrying every cycle at the same short cadence
    /// (review finding 3).
    pub async fn record_delivery_failure(
        &self,
        delivery: &PreparedSovDelivery,
        error: &SovDeliveryError,
        failed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("sov delivery failure is missing its claim token".to_string())
        })?;
        if error.is_permanent() {
            sqlx::query(
                "UPDATE sov_alert_deliveries SET status = 'failed', failure_kind = 'permanent', last_error = $2, failed_at = $3, delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $4",
            )
            .bind(delivery.delivery_id)
            .bind(&error.message)
            .bind(failed_at)
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        } else {
            let lease_until = failed_at + sov_transient_retry_backoff(delivery.attempt_count);
            sqlx::query(
                "UPDATE sov_alert_deliveries SET failure_kind = 'transient', last_error = $2, failed_at = $3, delivery_lease_until = $4 WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $5",
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

    #[doc(hidden)]
    pub async fn delivery_status(&self, delivery_id: i64) -> Result<String, sqlx::Error> {
        sqlx::query_scalar("SELECT status FROM sov_alert_deliveries WHERE id = $1")
            .bind(delivery_id)
            .fetch_one(&self.pool)
            .await
    }

    #[doc(hidden)]
    pub async fn delivery_status_by_subject(
        &self,
        subject_id: i64,
        stage: SovAlertStage,
    ) -> Result<String, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT status FROM sov_alert_deliveries WHERE subject_kind = 'campaign' AND subject_id = $1 AND stage = $2",
        )
        .bind(subject_id)
        .bind(stage.as_str())
        .fetch_one(&self.pool)
        .await
    }

    #[doc(hidden)]
    pub async fn count_deliveries_for(
        &self,
        subject_id: i64,
        stage: SovAlertStage,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT count(*) FROM sov_alert_deliveries WHERE subject_kind = 'campaign' AND subject_id = $1 AND stage = $2",
        )
        .bind(subject_id)
        .bind(stage.as_str())
        .fetch_one(&self.pool)
        .await
    }

    /// `subject_kind = 'structure'` counterpart of
    /// [`Self::delivery_status_by_subject`], used by `tz_window_entered`
    /// tests (ticket 07).
    #[doc(hidden)]
    pub async fn structure_delivery_status(
        &self,
        subject_id: i64,
        stage: SovAlertStage,
    ) -> Result<String, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT status FROM sov_alert_deliveries WHERE subject_kind = 'structure' AND subject_id = $1 AND stage = $2",
        )
        .bind(subject_id)
        .bind(stage.as_str())
        .fetch_one(&self.pool)
        .await
    }

    /// `subject_kind = 'structure'` counterpart of
    /// [`Self::count_deliveries_for`].
    #[doc(hidden)]
    pub async fn count_structure_deliveries_for(
        &self,
        subject_id: i64,
        stage: SovAlertStage,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT count(*) FROM sov_alert_deliveries WHERE subject_kind = 'structure' AND subject_id = $1 AND stage = $2",
        )
        .bind(subject_id)
        .bind(stage.as_str())
        .fetch_one(&self.pool)
        .await
    }
}

fn chain_snapshot_from_row(row: PgRow) -> Result<ChainSnapshot, sqlx::Error> {
    Ok(ChainSnapshot {
        systems: serde_json::from_value::<Vec<WandererSystem>>(row.get("systems"))
            .map_err(json_to_sqlx)?,
        connections: serde_json::from_value::<Vec<WandererConnection>>(row.get("connections"))
            .map_err(json_to_sqlx)?,
        fetched_at: row.get("fetched_at"),
    })
}

fn sov_campaign_from_row(row: PgRow) -> Result<SovCampaign, sqlx::Error> {
    Ok(SovCampaign {
        campaign_id: row.get("campaign_id"),
        event_type: row.get("event_type"),
        structure_id: row.get("structure_id"),
        solar_system_id: row.get("solar_system_id"),
        constellation_id: row.get("constellation_id"),
        defender_id: row.get("defender_id"),
        defender_score: row.get("defender_score"),
        attackers_score: row.get("attackers_score"),
        start_time: row.get("start_time"),
    })
}

fn sov_structure_from_row(row: PgRow) -> Result<SovStructure, sqlx::Error> {
    Ok(SovStructure {
        structure_id: row.get("structure_id"),
        structure_type_id: row.get("structure_type_id"),
        alliance_id: row.get("alliance_id"),
        solar_system_id: row.get("solar_system_id"),
        vulnerability_occupancy_level: row.get("vulnerability_occupancy_level"),
        vulnerable_start_time: row.get("vulnerable_start_time"),
        vulnerable_end_time: row.get("vulnerable_end_time"),
    })
}

fn sov_map_entry_from_row(row: PgRow) -> Result<SovMapEntry, sqlx::Error> {
    Ok(SovMapEntry {
        solar_system_id: row.get("solar_system_id"),
        alliance_id: row.get("alliance_id"),
        corporation_id: row.get("corporation_id"),
        faction_id: row.get("faction_id"),
    })
}

fn sov_subscription_from_row(row: PgRow) -> Result<SovSubscription, sqlx::Error> {
    Ok(SovSubscription {
        guild_id: row.get::<i64, _>("guild_id") as u64,
        channel_id: row.get::<i64, _>("channel_id") as u64,
        name: row.get("name"),
        filter: serde_json::from_value::<SovFilter>(row.get("filter")).map_err(json_to_sqlx)?,
        options: row.get("options"),
        role_id: row.get::<Option<i64>, _>("role_id").map(|id| id as u64),
    })
}

fn prepared_sov_delivery_from_row(row: PgRow) -> Result<PreparedSovDelivery, sqlx::Error> {
    Ok(PreparedSovDelivery {
        delivery_id: row.get("id"),
        guild_id: row.get::<i64, _>("guild_id") as u64,
        channel_id: row.get::<i64, _>("channel_id") as u64,
        subscription_name: row.get("subscription_name"),
        subject_kind: row.get("subject_kind"),
        subject_id: row.get("subject_id"),
        stage: row.get("stage"),
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
