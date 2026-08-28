//! The watchlist collector: `collect_cycle` polls each watched alliance's
//! `GET /alliances/{id}/corporations/` list with a conditional request and
//! the shared limiter, diffs it against the persisted membership snapshot
//! into `corp_joined` / `corp_left` events, fans each event out to the
//! guilds watching that alliance, and drives the durable, lease-claimed
//! delivery path.
//!
//! Mirrors [`crate::sov_feed::collector::SovCollector`] at a smaller scale:
//! one conditional request per watched alliance, a per-alliance silent
//! baseline, and two event kinds. An alliance whose request errors keeps its
//! last snapshot and posts nothing; the rest of the cycle continues.

use crate::esi_cache::{merge_cache_metadata, EsiError, EsiLimiterStore};
use crate::watchlist_feed::esi::WatchlistEsi;
use crate::watchlist_feed::model::{
    PreparedWatchlistDelivery, WatchlistDelivery, WatchlistEmbedField, WatchlistEntityResolver,
    WatchlistEvent, WatchlistEventKind, WatchlistNotificationMessage,
};
use crate::watchlist_feed::store::{alliance_resource_key, WatchlistStore};
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::warn;

pub trait WatchlistClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemWatchlistClock;

impl WatchlistClock for SystemWatchlistClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug)]
pub enum WatchlistCollectionError {
    Store(sqlx::Error),
    Esi(EsiError),
}

impl std::fmt::Display for WatchlistCollectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "watchlist store error: {error}"),
            Self::Esi(error) => write!(formatter, "watchlist ESI error: {error}"),
        }
    }
}

impl std::error::Error for WatchlistCollectionError {}

fn store_error(error: sqlx::Error) -> WatchlistCollectionError {
    WatchlistCollectionError::Store(error)
}

/// What one `collect_cycle()` did, for tests and logging.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct WatchlistCollectionReport {
    pub paused_until: Option<DateTime<Utc>>,
    pub alliances_polled: usize,
    pub not_modified: usize,
    pub baselines_established: usize,
    /// Fresh `200`s whose corporation array was empty and were treated as a
    /// no-op (fix round finding 1). A legitimately empty alliance
    /// corporation list is implausible, so diffing it against an established
    /// baseline would mark every member left and flood `corp_left`, then
    /// flood `corp_joined` on the next real listing. Counted here so an
    /// empty upstream body is observable in the cycle summary.
    pub empty_listings: usize,
    pub corp_joined: usize,
    pub corp_left: usize,
    pub deliveries_prepared: usize,
}

pub struct WatchlistCollector {
    store: WatchlistStore,
    esi: Arc<dyn WatchlistEsi>,
    limiter: Arc<dyn EsiLimiterStore>,
    delivery: Arc<dyn WatchlistDelivery>,
    resolver: Arc<dyn WatchlistEntityResolver>,
    clock: Arc<dyn WatchlistClock>,
}

impl WatchlistCollector {
    pub fn new(
        store: WatchlistStore,
        esi: Arc<dyn WatchlistEsi>,
        limiter: Arc<dyn EsiLimiterStore>,
        delivery: Arc<dyn WatchlistDelivery>,
        resolver: Arc<dyn WatchlistEntityResolver>,
    ) -> Self {
        Self {
            store,
            esi,
            limiter,
            delivery,
            resolver,
            clock: Arc::new(SystemWatchlistClock),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn WatchlistClock>) -> Self {
        self.clock = clock;
        self
    }

    #[doc(hidden)]
    pub async fn deliver_claimable_for_test(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(), WatchlistCollectionError> {
        self.deliver_claimable(observed_at).await
    }

    pub async fn collect_cycle(
        &self,
    ) -> Result<WatchlistCollectionReport, WatchlistCollectionError> {
        let observed_at = self.clock.now();
        self.deliver_claimable(observed_at).await?;

        let alliance_ids = self
            .store
            .all_watched_alliance_ids()
            .await
            .map_err(store_error)?;

        let mut report = WatchlistCollectionReport::default();
        for alliance_id in alliance_ids {
            // The legacy ESI error-limit allowance is global per application
            // (ADR 0004); this route carries no per-route bucket of its own.
            // Read the shared deadline *before* every request and pause the
            // rest of the cycle if it is active, rather than hammering the
            // shared budget across many alliances.
            if let Some(deadline) = self
                .limiter
                .active_esi_limiter_deadline()
                .await
                .map_err(store_error)?
            {
                report.paused_until = Some(deadline);
                break;
            }
            match self
                .collect_alliance(alliance_id, observed_at, &mut report)
                .await
            {
                Ok(()) => {}
                Err(WatchlistCollectionError::Esi(error)) => {
                    // Keep the last snapshot and post nothing for this
                    // alliance; the cache/limiter metadata was already
                    // persisted by `collect_alliance` before it returned.
                    warn!(
                        "watchlist corporation list for alliance {alliance_id} failed; keeping last snapshot: {error}"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        self.deliver_claimable(observed_at).await?;
        Ok(report)
    }

    async fn collect_alliance(
        &self,
        alliance_id: i64,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let resource_key = alliance_resource_key(alliance_id);
        let cached = self
            .store
            .cache_metadata(&resource_key)
            .await
            .map_err(store_error)?;
        if cached.is_fresh() {
            return Ok(());
        }

        report.alliances_polled += 1;
        let response = match self
            .esi
            .fetch_alliance_corporations(alliance_id, cached.etag.clone())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let merged = merge_cache_metadata(&cached, error.metadata.clone());
                self.store
                    .persist_cache_metadata(&resource_key, &merged)
                    .await
                    .map_err(store_error)?;
                self.limiter
                    .record_esi_limiter_at(&error.metadata, observed_at)
                    .await
                    .map_err(store_error)?;
                return Err(WatchlistCollectionError::Esi(error));
            }
        };
        let merged = merge_cache_metadata(&cached, response.metadata.clone());
        self.store
            .persist_cache_metadata(&resource_key, &merged)
            .await
            .map_err(store_error)?;
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;

        if response.not_modified {
            report.not_modified += 1;
            return Ok(());
        }
        let corporation_ids = response.value.unwrap_or_default();
        if corporation_ids.is_empty() {
            // A legitimately empty alliance corporation list does not occur
            // in practice (fix round finding 1); mirror the sov collector's
            // empty-listing guard. Diffing an empty body against an
            // established baseline would mark every member left (flooding
            // `corp_left`) and then flood `corp_joined` on the next real
            // listing. Keep the cache metadata already persisted above and
            // leave the snapshot untouched. A 404/closed alliance takes the
            // Err path above instead, which also keeps the snapshot.
            report.empty_listings += 1;
            warn!(
                "watchlist corporation list for alliance {alliance_id} returned an empty 200 body; \
                 treating as a no-op to avoid flooding corp_left/corp_joined"
            );
            return Ok(());
        }
        let events = self
            .diff_alliance(alliance_id, &corporation_ids, observed_at, report)
            .await?;
        for event in events {
            self.prepare_event(&event, observed_at, report).await?;
        }
        Ok(())
    }

    /// Diffs a fresh corporation listing against the persisted snapshot,
    /// updating the snapshot and returning the classified events. The first
    /// successful listing per alliance is a silent baseline (no events).
    async fn diff_alliance(
        &self,
        alliance_id: i64,
        corporation_ids: &[i64],
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<Vec<WatchlistEvent>, WatchlistCollectionError> {
        let fresh: HashSet<i64> = corporation_ids.iter().copied().collect();
        let baseline_established = self
            .store
            .alliance_baseline_established(alliance_id)
            .await
            .map_err(store_error)?;

        if !baseline_established {
            for corporation_id in &fresh {
                self.store
                    .upsert_member(alliance_id, *corporation_id, observed_at, true)
                    .await
                    .map_err(store_error)?;
            }
            self.store
                .establish_alliance_baseline(alliance_id, observed_at)
                .await
                .map_err(store_error)?;
            report.baselines_established += 1;
            return Ok(Vec::new());
        }

        let snapshot = self
            .store
            .alliance_corp_snapshot(alliance_id)
            .await
            .map_err(store_error)?;
        let evidence_date = observed_at.date_naive();
        let mut events = Vec::new();
        for corporation_id in &fresh {
            match snapshot.get(corporation_id) {
                None => {
                    // A corporation never before seen in this alliance.
                    self.store
                        .upsert_member(alliance_id, *corporation_id, observed_at, true)
                        .await
                        .map_err(store_error)?;
                    events.push(WatchlistEvent {
                        alliance_id,
                        corporation_id: *corporation_id,
                        kind: WatchlistEventKind::CorpJoined,
                        evidence_date,
                    });
                }
                Some(Some(_left_at)) => {
                    // A corporation that previously left and has now rejoined.
                    self.store
                        .upsert_member(alliance_id, *corporation_id, observed_at, true)
                        .await
                        .map_err(store_error)?;
                    events.push(WatchlistEvent {
                        alliance_id,
                        corporation_id: *corporation_id,
                        kind: WatchlistEventKind::CorpJoined,
                        evidence_date,
                    });
                }
                Some(None) => {
                    // Still a member: bump last_seen only.
                    self.store
                        .upsert_member(alliance_id, *corporation_id, observed_at, false)
                        .await
                        .map_err(store_error)?;
                }
            }
        }
        for (corporation_id, left_at) in &snapshot {
            if left_at.is_none() && !fresh.contains(corporation_id) {
                self.store
                    .mark_corp_left(alliance_id, *corporation_id, observed_at)
                    .await
                    .map_err(store_error)?;
                events.push(WatchlistEvent {
                    alliance_id,
                    corporation_id: *corporation_id,
                    kind: WatchlistEventKind::CorpLeft,
                    evidence_date,
                });
            }
        }
        Ok(events)
    }

    /// Fans one event out to every subscription that wants it across the
    /// guilds watching the alliance, rendering the embed once (only when at
    /// least one subscription still needs a fresh delivery) and storing it
    /// verbatim through the durable delivery path.
    async fn prepare_event(
        &self,
        event: &WatchlistEvent,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let evidence_key = event.evidence_key();
        let guilds = self
            .store
            .guilds_watching_alliance(event.alliance_id)
            .await
            .map_err(store_error)?;

        // Collect the (guild, subscription) targets that want this event and
        // do not already have a delivery row, so the expensive
        // corporation/alliance resolution and render happen only when a row
        // will actually be inserted.
        let mut targets = Vec::new();
        for guild_id in guilds {
            let subscriptions = self
                .store
                .subscriptions_for_guild(guild_id)
                .await
                .map_err(store_error)?;
            for subscription in subscriptions {
                if !subscription.wants(event.kind) {
                    continue;
                }
                if self
                    .store
                    .delivery_exists(
                        subscription.guild_id,
                        subscription.channel_id,
                        &subscription.name,
                        event.kind,
                        event.alliance_id,
                        &evidence_key,
                    )
                    .await
                    .map_err(store_error)?
                {
                    continue;
                }
                targets.push(subscription);
            }
        }
        if targets.is_empty() {
            return Ok(());
        }

        let message = self.render_event(event).await;
        for subscription in targets {
            let freshly_prepared = self
                .store
                .prepare_delivery(
                    subscription.guild_id,
                    subscription.channel_id,
                    &subscription.name,
                    event.kind,
                    event.alliance_id,
                    &evidence_key,
                    subscription.role_id,
                    &message,
                )
                .await
                .map_err(store_error)?;
            if freshly_prepared {
                report.deliveries_prepared += 1;
                match event.kind {
                    WatchlistEventKind::CorpJoined => report.corp_joined += 1,
                    WatchlistEventKind::CorpLeft => report.corp_left += 1,
                }
            }
        }
        // Attempt sends immediately, mirroring the sov feed's post-prepare
        // drain (restart-safe regardless via the lease/dedup path).
        self.deliver_claimable(observed_at).await?;
        Ok(())
    }

    /// Renders one event's notification, assembled once and stored verbatim
    /// (spec: content derived at prepare time). Resolves the corporation and
    /// alliance identity (bounded: one of each per event) at prepare time.
    async fn render_event(&self, event: &WatchlistEvent) -> WatchlistNotificationMessage {
        let corporation = self.resolver.corporation(event.corporation_id).await;
        let alliance = self.resolver.alliance(event.alliance_id).await;

        let corp_label = entity_label(
            &corporation.name,
            &corporation.ticker,
            "Corporation",
            event.corporation_id,
        );
        let alliance_label = entity_label(
            &alliance.name,
            &alliance.ticker,
            "Alliance",
            event.alliance_id,
        );
        let verb = match event.kind {
            WatchlistEventKind::CorpJoined => "joined",
            WatchlistEventKind::CorpLeft => "left",
        };
        let title = format!("{corp_label} {verb} {alliance_label}");

        let fields = vec![
            WatchlistEmbedField {
                name: "Alliance".to_string(),
                value: format!(
                    "{alliance_label}\n[zKillboard](https://zkillboard.com/alliance/{0}/) | [Dotlan](https://evemaps.dotlan.net/alliance/{0})",
                    event.alliance_id
                ),
                inline: false,
            },
            WatchlistEmbedField {
                name: "Corporation".to_string(),
                value: format!(
                    "{corp_label}\n[zKillboard](https://zkillboard.com/corporation/{0}/) | [Dotlan](https://evemaps.dotlan.net/corp/{0})",
                    event.corporation_id
                ),
                inline: false,
            },
            WatchlistEmbedField {
                name: "Event".to_string(),
                value: event.kind.footer_label().to_string(),
                inline: true,
            },
        ];

        WatchlistNotificationMessage {
            title,
            fields,
            footer: event.kind.footer_label().to_string(),
        }
    }

    async fn deliver_claimable(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(), WatchlistCollectionError> {
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
        delivery: PreparedWatchlistDelivery,
        observed_at: DateTime<Utc>,
    ) -> Result<(), WatchlistCollectionError> {
        let delivery_id = delivery.delivery_id;
        match self.delivery.send(delivery.clone()).await {
            Ok(discord_message_id) => {
                if let Err(error) = self
                    .store
                    .mark_delivery_sent(&delivery, &discord_message_id)
                    .await
                {
                    warn!(
                        "watchlist delivery {delivery_id} sent but could not be marked sent: {error}"
                    );
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
                        "watchlist delivery {delivery_id} failed ({error}) and its failure could not be recorded: {record_error}"
                    );
                } else if permanent {
                    warn!(
                        "watchlist delivery {delivery_id} permanently failed and will not be retried: {error}"
                    );
                } else {
                    warn!(
                        "watchlist delivery {delivery_id} failed and will be retried after its backed-off lease expires: {error}"
                    );
                }
            }
        }
        Ok(())
    }
}

/// Renders an entity as `Name [TICKER]`, falling back through
/// `Name`, `[TICKER]`, or `<Kind> <id>` when parts are unresolved.
fn entity_label(
    name: &Option<String>,
    ticker: &Option<String>,
    kind_label: &str,
    id: i64,
) -> String {
    match (name.as_deref(), ticker.as_deref()) {
        (Some(name), Some(ticker)) => format!("{name} [{ticker}]"),
        (Some(name), None) => name.to_string(),
        (None, Some(ticker)) => format!("[{ticker}]"),
        (None, None) => format!("{kind_label} {id}"),
    }
}
