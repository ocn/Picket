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
use crate::sov_feed::model::{
    PreparedSovDelivery, SovAlertStage, SovCampaign, SovDeliveryError, SovFilter,
    SovNotificationMessage, SovSubscription,
};
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
        sqlx::query(
            "UPDATE sov_campaigns SET ended_at = $1 WHERE campaign_id = ANY($2) AND ended_at IS NULL",
        )
        .bind(observed_at)
        .bind(campaign_ids)
        .execute(&self.pool)
        .await?;
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

    // --- Deliveries ---

    /// Inserts a prepared delivery if none exists yet for
    /// `(subscription, subject_kind='campaign', subject_id, stage)`.
    /// Idempotent: a duplicate call for the same identity is a no-op, which
    /// is the dedup authority for "never post the same stage twice" (the
    /// collector may call this every cycle for a still-eligible campaign;
    /// only a genuinely fresh row returns `true`).
    pub async fn prepare_delivery(
        &self,
        subscription: &SovSubscription,
        campaign_id: i64,
        stage: SovAlertStage,
        message: &SovNotificationMessage,
    ) -> Result<bool, sqlx::Error> {
        let delivery_id: Option<i64> = sqlx::query_scalar(
            "INSERT INTO sov_alert_deliveries (guild_id, channel_id, subscription_name, subject_kind, subject_id, stage, message, role_id, status) SELECT $1,$2,$3,'campaign',$4,$5,$6,$7,'prepared' WHERE EXISTS (SELECT 1 FROM sov_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND name = $3) ON CONFLICT (guild_id, channel_id, subscription_name, subject_kind, subject_id, stage) DO NOTHING RETURNING id",
        )
        .bind(subscription.guild_id as i64)
        .bind(subscription.channel_id as i64)
        .bind(&subscription.name)
        .bind(campaign_id)
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
