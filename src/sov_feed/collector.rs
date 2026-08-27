//! The sov campaign collector: `collect_cycle` polls
//! `/sovereignty/campaigns/`, updates Sov Baseline / ended-campaign state,
//! evaluates subscriptions against newly appeared campaigns, and drives
//! the durable delivery path. `evaluate_stage_cycle` is a second,
//! ESI-free entry point that checks T-minus marks against persisted
//! campaigns and the clock alone, so it can run at a cheaper cadence than
//! the sixty-second ESI poll (ticket 03).
//!
//! Mirrors `ContractCollector::collect_cycle` in `contract_intelligence.rs`
//! at a much smaller scale: one global resource instead of per-region
//! collection, and two Alert Stages (`appeared`, `tminus:<minutes>`)
//! instead of the full contract lifecycle.

use crate::esi_cache::{merge_cache_metadata, EsiError, EsiLimiterStore};
use crate::sov_feed::model::{
    effective_tminus_marks_minutes, PreparedSovDelivery, SovAlertStage, SovCampaign, SovDelivery,
    SovEmbedField, SovNotificationMessage,
};
use crate::sov_feed::store::{SovStore, SOV_CAMPAIGNS_RESOURCE_KEY};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::sync::Arc;
use tracing::warn;

/// How far ahead of `observed_at` a non-baseline campaign's `start_time`
/// may be for the `appeared` stage to fire (spec: "start within twelve
/// hours").
const SOV_APPEARED_WINDOW: ChronoDuration = ChronoDuration::hours(12);

pub trait SovClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemSovClock;

impl SovClock for SystemSovClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A resolved solar system, used for embed rendering and the `Region`
/// filter leaf.
#[derive(Clone, Debug, PartialEq)]
pub struct SovSystemInfo {
    pub name: String,
    pub region_name: String,
    pub region_id: i64,
}

/// Synchronous system/region lookups, backed by the killfeed's systems
/// cache (`config/systems.json`); no ESI fallback is needed because sov
/// campaigns only ever reference known k-space systems already present in
/// that cache.
pub trait SovSystemDirectory: Send + Sync {
    fn resolve(&self, solar_system_id: i64) -> Option<SovSystemInfo>;
}

/// Alliance ticker lookup, with an ESI fallback for tickers not yet cached
/// (mirrors the killfeed embed's ticker resolution).
#[async_trait]
pub trait SovTickerResolver: Send + Sync {
    async fn alliance_ticker(&self, alliance_id: i64) -> Option<String>;
}

#[derive(Debug)]
pub enum SovCollectionError {
    Store(sqlx::Error),
    Esi(EsiError),
}

impl std::fmt::Display for SovCollectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "sov store error: {error}"),
            Self::Esi(error) => write!(formatter, "sov ESI error: {error}"),
        }
    }
}

impl std::error::Error for SovCollectionError {}

impl From<EsiError> for SovCollectionError {
    fn from(error: EsiError) -> Self {
        Self::Esi(error)
    }
}

fn store_error(error: sqlx::Error) -> SovCollectionError {
    SovCollectionError::Store(error)
}

/// What one `collect_cycle()` did, for tests and logging.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SovCollectionReport {
    pub paused_until: Option<DateTime<Utc>>,
    pub not_yet_expired: bool,
    pub not_modified: bool,
    pub baseline_established_this_cycle: bool,
    pub campaigns_observed: usize,
    pub newly_appeared: usize,
    pub ended: usize,
    pub alerts_prepared: usize,
}

/// What one `evaluate_stage_cycle()` did, for tests and logging. Runs
/// independently of `collect_cycle()`'s sixty-second ESI poll (spec:
/// "A stage-evaluation pass runs at least every thirty seconds"; ticket
/// 03), evaluating T-minus marks purely against persisted campaigns and
/// the controlled clock, with no ESI request.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SovStageEvaluationReport {
    pub campaigns_evaluated: usize,
    pub tminus_prepared: usize,
}

impl SovCollectionReport {
    fn paused(deadline: DateTime<Utc>) -> Self {
        Self {
            paused_until: Some(deadline),
            ..Default::default()
        }
    }

    fn not_yet_expired() -> Self {
        Self {
            not_yet_expired: true,
            ..Default::default()
        }
    }

    fn not_modified() -> Self {
        Self {
            not_modified: true,
            ..Default::default()
        }
    }
}

pub struct SovCollector {
    store: SovStore,
    esi: Arc<dyn crate::sov_feed::esi::SovereigntyEsi>,
    limiter: Arc<dyn EsiLimiterStore>,
    delivery: Arc<dyn SovDelivery>,
    directory: Arc<dyn SovSystemDirectory>,
    tickers: Arc<dyn SovTickerResolver>,
    clock: Arc<dyn SovClock>,
}

impl SovCollector {
    pub fn new(
        store: SovStore,
        esi: Arc<dyn crate::sov_feed::esi::SovereigntyEsi>,
        limiter: Arc<dyn EsiLimiterStore>,
        delivery: Arc<dyn SovDelivery>,
        directory: Arc<dyn SovSystemDirectory>,
        tickers: Arc<dyn SovTickerResolver>,
    ) -> Self {
        Self {
            store,
            esi,
            limiter,
            delivery,
            directory,
            tickers,
            clock: Arc::new(SystemSovClock),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn SovClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Exercises exactly the restart-recovery claim step (no ESI request,
    /// no diff) so integration tests can assert unexpired leases are left
    /// alone while expired ones are reclaimed, independent of a full cycle.
    #[doc(hidden)]
    pub async fn deliver_claimable_for_test(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(), SovCollectionError> {
        self.deliver_claimable(observed_at).await
    }

    pub async fn collect_cycle(&self) -> Result<SovCollectionReport, SovCollectionError> {
        let observed_at = self.clock.now();
        self.deliver_claimable(observed_at).await?;

        if let Some(deadline) = self
            .limiter
            .active_esi_limiter_deadline()
            .await
            .map_err(store_error)?
        {
            return Ok(SovCollectionReport::paused(deadline));
        }

        let cached = self
            .store
            .cache_metadata(SOV_CAMPAIGNS_RESOURCE_KEY)
            .await
            .map_err(store_error)?;
        if cached.is_fresh() {
            return Ok(SovCollectionReport::not_yet_expired());
        }

        let response = match self.esi.fetch_campaigns(cached.etag.clone()).await {
            Ok(response) => response,
            Err(error) => {
                let merged = merge_cache_metadata(&cached, error.metadata.clone());
                self.store
                    .persist_cache_metadata(SOV_CAMPAIGNS_RESOURCE_KEY, &merged)
                    .await
                    .map_err(store_error)?;
                self.limiter
                    .record_esi_limiter_at(&error.metadata, observed_at)
                    .await
                    .map_err(store_error)?;
                return Err(error.into());
            }
        };
        let merged = merge_cache_metadata(&cached, response.metadata.clone());
        self.store
            .persist_cache_metadata(SOV_CAMPAIGNS_RESOURCE_KEY, &merged)
            .await
            .map_err(store_error)?;
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;

        if response.not_modified {
            return Ok(SovCollectionReport::not_modified());
        }
        let campaigns = response.value.unwrap_or_default();
        let report = self.diff_and_alert(campaigns, observed_at).await?;
        self.deliver_claimable(observed_at).await?;
        Ok(report)
    }

    /// Evaluates T-minus marks for every open (non-ended) campaign against
    /// every subscription's filter and configured marks, purely from
    /// persisted state and the controlled clock: no ESI request is made,
    /// so this can run far more often than the sixty-second campaigns
    /// poll (spec "Alert stages and dedup"; ticket 03). Baseline campaigns
    /// are included -- `open_campaigns` does not filter on the baseline
    /// flag -- because they never received `appeared` but must still
    /// alert on T-minus marks. `deliver_claimable` runs before and after,
    /// mirroring `collect_cycle`, so a mark prepared in this pass is
    /// attempted immediately rather than waiting for the next poll.
    pub async fn evaluate_stage_cycle(
        &self,
    ) -> Result<SovStageEvaluationReport, SovCollectionError> {
        let observed_at = self.clock.now();
        self.deliver_claimable(observed_at).await?;

        let campaigns = self.store.open_campaigns().await.map_err(store_error)?;
        let mut tminus_prepared = 0usize;
        if !campaigns.is_empty() {
            let subscriptions = self.store.subscriptions().await.map_err(store_error)?;
            let region_of =
                |system_id: i64| self.directory.resolve(system_id).map(|info| info.region_id);
            for campaign in &campaigns {
                // "start_time > now" (spec): a campaign that has already
                // started never gets a T-minus mark.
                if campaign.start_time <= observed_at {
                    continue;
                }
                let remaining = campaign.start_time - observed_at;
                for subscription in &subscriptions {
                    if !subscription
                        .filter
                        .root
                        .matches(campaign, observed_at, &region_of)
                    {
                        continue;
                    }
                    // All due marks fire in this same pass (e.g. after
                    // downtime skipped an earlier evaluation), each once
                    // via `prepare_delivery`'s unique-constraint dedup.
                    for minutes in effective_tminus_marks_minutes(&subscription.options) {
                        // Non-panicking construction (review finding 1b):
                        // `/sov_subscribe` already rejects marks above
                        // `SOV_TMINUS_MARK_MAX_MINUTES`, but a value
                        // stored some other way (direct `options` write,
                        // a future migration) must never reach the
                        // panicking `ChronoDuration::minutes`. Treat an
                        // out-of-range mark as permanently not-due rather
                        // than always-due.
                        let Some(mark_duration) = ChronoDuration::try_minutes(minutes) else {
                            continue;
                        };
                        if remaining > mark_duration {
                            continue;
                        }
                        let message = self
                            .render_stage_message(campaign, SovAlertStage::TMinus(minutes))
                            .await;
                        let freshly_prepared = self
                            .store
                            .prepare_delivery(
                                subscription,
                                campaign.campaign_id,
                                SovAlertStage::TMinus(minutes),
                                &message,
                            )
                            .await
                            .map_err(store_error)?;
                        if freshly_prepared {
                            tminus_prepared += 1;
                        }
                    }
                }
            }
        }
        self.deliver_claimable(observed_at).await?;
        Ok(SovStageEvaluationReport {
            campaigns_evaluated: campaigns.len(),
            tminus_prepared,
        })
    }

    async fn diff_and_alert(
        &self,
        campaigns: Vec<SovCampaign>,
        observed_at: DateTime<Utc>,
    ) -> Result<SovCollectionReport, SovCollectionError> {
        let baseline_established = self
            .store
            .baseline_established()
            .await
            .map_err(store_error)?;
        let existing_open_ids = self.store.open_campaign_ids().await.map_err(store_error)?;

        // Re-evaluated every cycle for every observed campaign, not only
        // when the row is freshly inserted: a campaign whose start time is
        // initially outside the twelve-hour window must still alert once
        // it later falls inside it. Dedup is `prepare_delivery`'s unique
        // constraint below, not "did we just insert this campaign row"
        // (review finding 2).
        let mut alert_candidates = Vec::new();
        for campaign in &campaigns {
            let (_inserted, is_baseline) = self
                .store
                .upsert_campaign(campaign, !baseline_established, observed_at)
                .await
                .map_err(store_error)?;
            if !is_baseline && campaign.start_time <= observed_at + SOV_APPEARED_WINDOW {
                alert_candidates.push(campaign.clone());
            }
        }

        let current_ids: std::collections::HashSet<i64> = campaigns
            .iter()
            .map(|campaign| campaign.campaign_id)
            .collect();
        let ended_ids: Vec<i64> = existing_open_ids
            .difference(&current_ids)
            .copied()
            .collect();
        self.store
            .mark_ended(&ended_ids, observed_at)
            .await
            .map_err(store_error)?;

        if !baseline_established {
            self.store
                .establish_baseline(observed_at)
                .await
                .map_err(store_error)?;
        }

        let mut alerts_prepared = 0usize;
        if !alert_candidates.is_empty() {
            let subscriptions = self.store.subscriptions().await.map_err(store_error)?;
            let region_of =
                |system_id: i64| self.directory.resolve(system_id).map(|info| info.region_id);
            for campaign in &alert_candidates {
                for subscription in &subscriptions {
                    if subscription
                        .filter
                        .root
                        .matches(campaign, observed_at, &region_of)
                    {
                        let message = self
                            .render_stage_message(campaign, SovAlertStage::Appeared)
                            .await;
                        let freshly_prepared = self
                            .store
                            .prepare_delivery(
                                subscription,
                                campaign.campaign_id,
                                SovAlertStage::Appeared,
                                &message,
                            )
                            .await
                            .map_err(store_error)?;
                        if freshly_prepared {
                            alerts_prepared += 1;
                        }
                    }
                }
            }
        }

        Ok(SovCollectionReport {
            baseline_established_this_cycle: !baseline_established,
            campaigns_observed: campaigns.len(),
            newly_appeared: alert_candidates.len(),
            ended: ended_ids.len(),
            alerts_prepared,
            ..Default::default()
        })
    }

    /// Renders one campaign's notification for the given stage. Assembled
    /// once and stored verbatim (spec, `SovNotificationMessage` doc
    /// comment) so a restart replays identical content; the footer is the
    /// only part that varies by stage (spec "Embed": "Footer names the
    /// stage"; e.g. `Appeared` or `T-120m`), so this single builder is
    /// reused for every stage rather than duplicated per stage.
    async fn render_stage_message(
        &self,
        campaign: &SovCampaign,
        stage: SovAlertStage,
    ) -> SovNotificationMessage {
        let system_info = self.directory.resolve(campaign.solar_system_id);
        let (system_name, region_name) = system_info
            .map(|info| (info.name, info.region_name))
            .unwrap_or_else(|| {
                (
                    campaign.solar_system_id.to_string(),
                    "Unknown Region".to_string(),
                )
            });
        let defender_ticker = match campaign.defender_id {
            Some(alliance_id) => self
                .tickers
                .alliance_ticker(alliance_id)
                .await
                .unwrap_or_else(|| "Unknown".to_string()),
            None => "Unknown".to_string(),
        };
        let title = format!(
            "{system_name} ({region_name}) \u{2014} {} \u{2014} {defender_ticker}",
            event_type_label(&campaign.event_type)
        );

        let unix = campaign.start_time.timestamp();
        let start_value = format!(
            "{} EVE\n<t:{unix}:R>",
            campaign.start_time.format("%Y-%m-%d %H:%M:%S")
        );
        let mut fields = vec![SovEmbedField {
            name: "Start Time".to_string(),
            value: start_value,
            inline: false,
        }];

        if let (Some(defender_score), Some(attackers_score)) =
            (campaign.defender_score, campaign.attackers_score)
        {
            if defender_score != 0.0 || attackers_score != 0.0 {
                fields.push(SovEmbedField {
                    name: "Scores".to_string(),
                    value: format!(
                        "Defender {:.0}% / Attackers {:.0}%",
                        defender_score * 100.0,
                        attackers_score * 100.0
                    ),
                    inline: true,
                });
            }
        }

        fields.push(SovEmbedField {
            name: "Links".to_string(),
            value: format!(
                "[zKillboard](https://zkillboard.com/system/{}/) | [Dotlan](https://evemaps.dotlan.net/system/{})",
                campaign.solar_system_id, campaign.solar_system_id
            ),
            inline: true,
        });

        SovNotificationMessage {
            title,
            fields,
            footer: stage.footer_label(),
        }
    }

    async fn deliver_claimable(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(), SovCollectionError> {
        let prepared = self
            .store
            .prepared_delivery_ids()
            .await
            .map_err(store_error)?;
        for delivery_id in prepared {
            let Some(delivery) = self
                .store
                .begin_delivery_attempt(delivery_id, observed_at)
                .await
                .map_err(store_error)?
            else {
                continue;
            };
            self.send_and_mark(delivery, observed_at).await?;
        }
        Ok(())
    }

    async fn send_and_mark(
        &self,
        delivery: PreparedSovDelivery,
        observed_at: DateTime<Utc>,
    ) -> Result<(), SovCollectionError> {
        let delivery_id = delivery.delivery_id;
        match self.delivery.send(delivery.clone()).await {
            Ok(discord_message_id) => {
                if let Err(error) = self
                    .store
                    .mark_delivery_sent(&delivery, &discord_message_id)
                    .await
                {
                    warn!("sov delivery {delivery_id} sent but could not be marked sent: {error}");
                }
            }
            Err(error) => {
                let permanent = error.is_permanent();
                if let Err(record_error) = self
                    .store
                    .record_delivery_failure(&delivery, &error, observed_at)
                    .await
                {
                    warn!(
                        "sov delivery {delivery_id} failed ({error}) and its failure could not be recorded: {record_error}"
                    );
                } else if permanent {
                    warn!("sov delivery {delivery_id} permanently failed and will not be retried: {error}");
                } else {
                    warn!(
                        "sov delivery {delivery_id} failed and will be retried after its backed-off lease expires: {error}"
                    );
                }
            }
        }
        Ok(())
    }
}

fn event_type_label(event_type: &str) -> String {
    match event_type {
        "tcu_defense" => "TCU Defense".to_string(),
        "ihub_defense" => "IHub Defense".to_string(),
        "station_defense" => "Station Defense".to_string(),
        "station_freeport" => "Station Freeport".to_string(),
        other => other.to_string(),
    }
}
