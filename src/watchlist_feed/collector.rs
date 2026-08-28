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
    evaluate_member_delta, MemberDeltaAlert, MemberDeltaDirection, MemberDeltaOutcome,
    PreparedWatchlistDelivery, WatchlistDelivery, WatchlistEmbedField, WatchlistEntityResolver,
    WatchlistEvent, WatchlistEventKind, WatchlistKind, WatchlistNotificationMessage,
    WatchlistSubscription, MEMBER_DELTA_REFERENCE_WINDOW,
};
use crate::watchlist_feed::store::{alliance_resource_key, corp_resource_key, WatchlistStore};
use chrono::{DateTime, Utc};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use tracing::warn;

/// The maximum number of corporation-info conditional GETs the collector
/// issues in one cycle. An alliance with 150 member corporations means 150
/// conditional requests per hour; capping the fetches keeps a large
/// watchlist bounded. Corporations over the cap are deferred: the next cycle
/// orders least-recently-fetched (or never-fetched) corporations first, so
/// coverage rotates rather than starving the tail. Cache freshness (the
/// route's one-hour TTL) already skips most re-fetches, so this cap only
/// bites on the first cycles after a large watchlist is configured.
const MAX_CORP_INFO_FETCHES_PER_CYCLE: usize = 200;

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
    /// Corporations whose public info was fetched fresh (a `200`) this cycle.
    pub corp_info_fetched: usize,
    /// Corporation-info conditional requests that returned `304`.
    pub corp_info_not_modified: usize,
    /// Corporation-info requests skipped this cycle because the per-cycle
    /// fetch cap was reached; deferred to a later cycle.
    pub corp_info_deferred: usize,
    /// Corporations skipped this cycle because their cached public info was
    /// still fresh (no ESI call needed); counted before the fetch cap so a
    /// full cap never defers a corporation that would have cost nothing
    /// (finding 4).
    pub corp_info_fresh: usize,
    /// Corporation-info requests that errored (isolated per corporation).
    pub corp_info_errors: usize,
    /// Alliance aggregate member-count snapshots recorded this cycle.
    pub alliance_member_counts: usize,
    /// Current alliance members excluded from the aggregate this cycle because
    /// their corporation-info fetch has failed for at least
    /// [`crate::watchlist_feed::store::CORP_FETCH_FAILURE_EXCLUSION_THRESHOLD`]
    /// cycles (finding 3).
    pub excluded_members: usize,
    /// `member_delta` deliveries prepared (fresh crossings).
    pub member_delta: usize,
    /// `corp_changed_alliance` deliveries prepared.
    pub corp_changed_alliance: usize,
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
        for &alliance_id in &alliance_ids {
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

        // Member-count snapshots and the delta / alliance-change events
        // (ticket 09). Skipped when the alliance pass already hit the shared
        // limiter deadline this cycle; it resumes next cycle.
        if report.paused_until.is_none() {
            self.collect_member_snapshots(&alliance_ids, observed_at, &mut report)
                .await?;
        }

        // Best-effort retention prune; a failure here must not fail the cycle.
        if let Err(error) = self.store.prune_member_snapshots(observed_at).await {
            warn!("watchlist member snapshot prune failed: {error}");
        }

        self.deliver_claimable(observed_at).await?;
        Ok(report)
    }

    /// The member-snapshot pass: fetch corporation public info for every
    /// watched corporation and every current member corporation of each
    /// watched alliance, persist member snapshots, and derive the
    /// `member_delta` and `corp_changed_alliance` events. Bounded to
    /// [`MAX_CORP_INFO_FETCHES_PER_CYCLE`] fetches per cycle, ordered
    /// least-recently-fetched first, and paused when the shared limiter
    /// deadline is active.
    async fn collect_member_snapshots(
        &self,
        alliance_ids: &[i64],
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let watched_corp_ids: BTreeSet<i64> = self
            .store
            .all_watched_corporation_ids()
            .await
            .map_err(store_error)?
            .into_iter()
            .collect();

        // Target corporations: every watched corporation plus every current
        // member of each watched alliance, deduped.
        let mut target_ids: BTreeSet<i64> = watched_corp_ids.clone();
        for &alliance_id in alliance_ids {
            for corporation_id in self
                .store
                .current_alliance_member_ids(alliance_id)
                .await
                .map_err(store_error)?
            {
                target_ids.insert(corporation_id);
            }
        }

        // Order least-recently-fetched (or never-fetched) first so a
        // watchlist larger than the per-cycle cap rotates coverage.
        let target_ids: Vec<i64> = target_ids.into_iter().collect();
        let updated_ats = self
            .store
            .corp_cache_updated_ats(&target_ids)
            .await
            .map_err(store_error)?;
        let mut ordered = target_ids.clone();
        ordered.sort_by(|left, right| {
            let left_key = updated_ats.get(left);
            let right_key = updated_ats.get(right);
            // Never-fetched (None) sorts first, then oldest updated_at first.
            match (left_key, right_key) {
                (None, None) => left.cmp(right),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(a), Some(b)) => a.cmp(b).then(left.cmp(right)),
            }
        });

        let mut fetches = 0usize;
        for corporation_id in ordered {
            if let Some(deadline) = self
                .limiter
                .active_esi_limiter_deadline()
                .await
                .map_err(store_error)?
            {
                report.paused_until = Some(deadline);
                break;
            }
            // Freshness is checked before the per-cycle fetch cap (finding 4):
            // a corporation whose cached info is still fresh costs no ESI call,
            // so it must be skipped for free rather than counted against the
            // cap and deferred.
            let resource_key = corp_resource_key(corporation_id);
            let cached = self
                .store
                .cache_metadata(&resource_key)
                .await
                .map_err(store_error)?;
            if cached.is_fresh() {
                report.corp_info_fresh += 1;
                continue;
            }
            if fetches >= MAX_CORP_INFO_FETCHES_PER_CYCLE {
                report.corp_info_deferred += 1;
                continue;
            }
            fetches += 1;
            let is_watched = watched_corp_ids.contains(&corporation_id);
            match self
                .collect_corporation_info(corporation_id, is_watched, &cached, observed_at, report)
                .await
            {
                Ok(()) => {}
                Err(WatchlistCollectionError::Esi(error)) => {
                    report.corp_info_errors += 1;
                    warn!(
                        "watchlist corporation info for {corporation_id} failed; other corporations still snapshot: {error}"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        // Alliance aggregate member counts and their deltas.
        if report.paused_until.is_none() {
            for &alliance_id in alliance_ids {
                let aggregate = self
                    .store
                    .current_alliance_member_count(alliance_id)
                    .await
                    .map_err(store_error)?;
                report.excluded_members += aggregate.excluded_members as usize;
                if let Some(total) = aggregate.total {
                    self.store
                        .record_alliance_member_snapshot(alliance_id, total, observed_at)
                        .await
                        .map_err(store_error)?;
                    report.alliance_member_counts += 1;
                    // Watch-scoped baseline (finding 1): established on the
                    // first successful aggregate evaluation after the alliance
                    // became watched, so a delta measures only post-watch
                    // history.
                    self.store
                        .establish_member_baseline(
                            WatchlistKind::Alliance,
                            alliance_id,
                            observed_at,
                        )
                        .await
                        .map_err(store_error)?;
                    self.evaluate_member_delta_event(
                        WatchlistKind::Alliance,
                        alliance_id,
                        total,
                        observed_at,
                        report,
                    )
                    .await?;
                }
            }

            // Watched-corporation deltas (direct member count).
            for corporation_id in &watched_corp_ids {
                if let Some(current) = self
                    .store
                    .latest_member_count(WatchlistKind::Corporation, *corporation_id)
                    .await
                    .map_err(store_error)?
                {
                    self.evaluate_member_delta_event(
                        WatchlistKind::Corporation,
                        *corporation_id,
                        current,
                        observed_at,
                        report,
                    )
                    .await?;
                }
            }
        }

        // Persist any identity-cache updates accumulated during the fetch loop
        // a single time (finding 5), rather than once per fresh corporation.
        self.resolver.flush_noted_corporations().await;
        Ok(())
    }

    /// Fetches one corporation's public info, persists a member snapshot,
    /// feeds the identity caches, and (for a watched corporation) emits a
    /// `corp_changed_alliance` event when its alliance differs from the
    /// previous snapshot. Cache and limiter metadata are recorded after
    /// every response including 304s and errors.
    async fn collect_corporation_info(
        &self,
        corporation_id: i64,
        is_watched: bool,
        cached: &crate::esi_cache::CacheMetadata,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let resource_key = corp_resource_key(corporation_id);
        let response = match self
            .esi
            .fetch_corporation_info(corporation_id, cached.etag.clone())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let merged = merge_cache_metadata(cached, error.metadata.clone());
                self.store
                    .persist_cache_metadata(&resource_key, &merged)
                    .await
                    .map_err(store_error)?;
                self.limiter
                    .record_esi_limiter_at(&error.metadata, observed_at)
                    .await
                    .map_err(store_error)?;
                // Finding 3: track consecutive fetch failures so a
                // persistently failing member can eventually be excluded from
                // its alliance's aggregate rather than blocking it forever.
                self.store
                    .record_corp_fetch_failure(corporation_id, &error.to_string(), observed_at)
                    .await
                    .map_err(store_error)?;
                return Err(WatchlistCollectionError::Esi(error));
            }
        };
        let merged = merge_cache_metadata(cached, response.metadata.clone());
        self.store
            .persist_cache_metadata(&resource_key, &merged)
            .await
            .map_err(store_error)?;
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;
        // Any answered request (200 or 304) clears the failure streak (finding
        // 3).
        self.store
            .reset_corp_fetch_failures(corporation_id)
            .await
            .map_err(store_error)?;

        if response.not_modified {
            report.corp_info_not_modified += 1;
            return Ok(());
        }
        let Some(info) = response.value else {
            return Ok(());
        };
        report.corp_info_fetched += 1;

        // corp_changed_alliance for a watched corporation, detected before the
        // new snapshot is written and only against snapshots at/after the
        // watch-scoped baseline (finding 1): a corporation snapshotted before
        // it became watched (e.g. while only an alliance member) must not fire
        // a change on the first post-watch cycle. No baseline yet -> the first
        // post-watch snapshot is a silent baseline.
        if is_watched {
            if let Some(baseline) = self
                .store
                .member_baseline(WatchlistKind::Corporation, corporation_id)
                .await
                .map_err(store_error)?
            {
                if let Some(previous_alliance) = self
                    .store
                    .previous_corp_alliance(corporation_id, baseline)
                    .await
                    .map_err(store_error)?
                {
                    if previous_alliance != info.alliance_id {
                        self.prepare_corp_changed_alliance(
                            corporation_id,
                            previous_alliance,
                            info.alliance_id,
                            observed_at,
                            report,
                        )
                        .await?;
                    }
                }
            }
        }

        self.store
            .record_corp_snapshot(
                corporation_id,
                info.member_count,
                info.alliance_id,
                observed_at,
            )
            .await
            .map_err(store_error)?;
        // Establish the watch-scoped baseline for a watched corporation on its
        // first post-watch snapshot (finding 1); idempotent thereafter.
        if is_watched {
            self.store
                .establish_member_baseline(WatchlistKind::Corporation, corporation_id, observed_at)
                .await
                .map_err(store_error)?;
        }
        // Feed the shared identity caches so the embed needs no extra lookup.
        self.resolver
            .note_corporation(corporation_id, info.name.clone(), info.ticker.clone())
            .await;
        Ok(())
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
        let targets = self
            .delivery_targets(event.kind, event.alliance_id, &evidence_key, guilds)
            .await?;
        if targets.is_empty() {
            return Ok(());
        }
        let message = self.render_event(event).await;
        let prepared = self
            .insert_and_send(
                targets,
                event.kind,
                event.alliance_id,
                &evidence_key,
                &message,
                observed_at,
            )
            .await?;
        report.deliveries_prepared += prepared;
        match event.kind {
            WatchlistEventKind::CorpJoined => report.corp_joined += prepared,
            WatchlistEventKind::CorpLeft => report.corp_left += prepared,
            _ => {}
        }
        Ok(())
    }

    /// The subscriptions across `guilds` that want `event_kind` and do not
    /// already have a delivery row for this evidence key. Computed before any
    /// render so the expensive resolution/render happens only when a delivery
    /// row will actually be inserted (spec: content derived at prepare time,
    /// render only when a row will be inserted).
    async fn delivery_targets(
        &self,
        event_kind: WatchlistEventKind,
        entity_id: i64,
        evidence_key: &str,
        guilds: Vec<u64>,
    ) -> Result<Vec<WatchlistSubscription>, WatchlistCollectionError> {
        let mut targets = Vec::new();
        for guild_id in guilds {
            let subscriptions = self
                .store
                .subscriptions_for_guild(guild_id)
                .await
                .map_err(store_error)?;
            for subscription in subscriptions {
                if !subscription.wants(event_kind) {
                    continue;
                }
                if self
                    .store
                    .delivery_exists(
                        subscription.guild_id,
                        subscription.channel_id,
                        &subscription.name,
                        event_kind,
                        entity_id,
                        evidence_key,
                    )
                    .await
                    .map_err(store_error)?
                {
                    continue;
                }
                targets.push(subscription);
            }
        }
        Ok(targets)
    }

    /// Inserts one delivery row per target (deduped by the unique
    /// constraint), then drains claimable deliveries. Returns the number of
    /// rows freshly prepared.
    async fn insert_and_send(
        &self,
        targets: Vec<WatchlistSubscription>,
        event_kind: WatchlistEventKind,
        entity_id: i64,
        evidence_key: &str,
        message: &WatchlistNotificationMessage,
        observed_at: DateTime<Utc>,
    ) -> Result<usize, WatchlistCollectionError> {
        let mut prepared = 0usize;
        for subscription in targets {
            let freshly_prepared = self
                .store
                .prepare_delivery(
                    subscription.guild_id,
                    subscription.channel_id,
                    &subscription.name,
                    event_kind,
                    entity_id,
                    evidence_key,
                    subscription.role_id,
                    message,
                )
                .await
                .map_err(store_error)?;
            if freshly_prepared {
                prepared += 1;
            }
        }
        // Attempt sends immediately, mirroring the sov feed's post-prepare
        // drain (restart-safe regardless via the lease/dedup path).
        self.deliver_claimable(observed_at).await?;
        Ok(prepared)
    }

    /// Evaluates one entity's member-count delta (spec user stories 45-46)
    /// and, on a fresh band crossing, fans a `member_delta` delivery out to
    /// the guilds watching that entity. Persists the band state so the
    /// crossing fires once and re-arms only inside the band, restart-safe.
    async fn evaluate_member_delta_event(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
        current_count: i64,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        // Watch-scoped baseline (finding 1): a delta measures only snapshots
        // observed at/after the current watch's baseline. Without a baseline
        // yet, there is nothing post-watch to measure -- stay silent.
        let Some(baseline) = self
            .store
            .member_baseline(entity_kind, entity_id)
            .await
            .map_err(store_error)?
        else {
            return Ok(());
        };
        let target = observed_at - MEMBER_DELTA_REFERENCE_WINDOW;
        let reference = self
            .store
            .member_reference(entity_kind, entity_id, target, baseline)
            .await
            .map_err(store_error)?;
        let armed = self
            .store
            .member_band_armed(entity_kind, entity_id)
            .await
            .map_err(store_error)?;
        match evaluate_member_delta(observed_at, current_count, reference, armed) {
            MemberDeltaOutcome::InsufficientHistory => {}
            MemberDeltaOutcome::AlreadyFired => {}
            MemberDeltaOutcome::InsideBand => {
                self.store
                    .rearm_member_band_state(entity_kind, entity_id, reference, observed_at)
                    .await
                    .map_err(store_error)?;
            }
            MemberDeltaOutcome::Fires(alert) => {
                // The disarm (sequence bump + direction) and the delivery-row
                // inserts commit in one transaction inside
                // `fire_member_delta_crossing`; sending happens afterward
                // through the lease path, so a crash yields exactly one post
                // (finding 2). The evidence key derives from the bumped
                // sequence, so it is stable across a crash and unique per
                // crossing. Disarm still happens when no subscription targets
                // the event.
                let guilds = self
                    .store
                    .guilds_watching_entity(entity_kind, entity_id)
                    .await
                    .map_err(store_error)?;
                let targets = self
                    .subscriptions_wanting(WatchlistEventKind::MemberDelta, guilds)
                    .await?;
                let message = if targets.is_empty() {
                    None
                } else {
                    Some(
                        self.render_member_delta(entity_kind, entity_id, &alert, observed_at)
                            .await,
                    )
                };
                let (_evidence_key, prepared) = self
                    .store
                    .fire_member_delta_crossing(
                        entity_kind,
                        entity_id,
                        alert.direction,
                        reference,
                        observed_at,
                        &targets,
                        message.as_ref(),
                    )
                    .await
                    .map_err(store_error)?;
                report.member_delta += prepared;
                report.deliveries_prepared += prepared;
                if prepared > 0 {
                    self.deliver_claimable(observed_at).await?;
                }
            }
        }
        Ok(())
    }

    /// The subscriptions across `guilds` that want `event_kind`, with no
    /// evidence-key dedup filter (the caller inserts through a path whose
    /// unique constraint dedups idempotently). Used by the member-delta
    /// crossing, whose evidence key is only known inside its transaction.
    async fn subscriptions_wanting(
        &self,
        event_kind: WatchlistEventKind,
        guilds: Vec<u64>,
    ) -> Result<Vec<WatchlistSubscription>, WatchlistCollectionError> {
        let mut targets = Vec::new();
        for guild_id in guilds {
            let subscriptions = self
                .store
                .subscriptions_for_guild(guild_id)
                .await
                .map_err(store_error)?;
            for subscription in subscriptions {
                if subscription.wants(event_kind) {
                    targets.push(subscription);
                }
            }
        }
        Ok(targets)
    }

    /// Fans a `corp_changed_alliance` event out to the guilds watching a
    /// watched corporation. Evidence key is corporation + old + new alliance
    /// + observed date (spec: fires once per change).
    async fn prepare_corp_changed_alliance(
        &self,
        corporation_id: i64,
        old_alliance: Option<i64>,
        new_alliance: Option<i64>,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let evidence_key = format!(
            "{}:{}:{}:{}",
            corporation_id,
            old_alliance.map(|id| id.to_string()).unwrap_or_default(),
            new_alliance.map(|id| id.to_string()).unwrap_or_default(),
            observed_at.date_naive()
        );
        let guilds = self
            .store
            .guilds_watching_entity(WatchlistKind::Corporation, corporation_id)
            .await
            .map_err(store_error)?;
        let targets = self
            .delivery_targets(
                WatchlistEventKind::CorpChangedAlliance,
                corporation_id,
                &evidence_key,
                guilds,
            )
            .await?;
        if targets.is_empty() {
            return Ok(());
        }
        let message = self
            .render_corp_changed_alliance(corporation_id, old_alliance, new_alliance)
            .await;
        let prepared = self
            .insert_and_send(
                targets,
                WatchlistEventKind::CorpChangedAlliance,
                corporation_id,
                &evidence_key,
                &message,
                observed_at,
            )
            .await?;
        report.corp_changed_alliance += prepared;
        report.deliveries_prepared += prepared;
        Ok(())
    }

    /// Renders a `member_delta` notification: before/after counts, the
    /// percentage, direction, and the reference window.
    async fn render_member_delta(
        &self,
        entity_kind: WatchlistKind,
        entity_id: i64,
        alert: &MemberDeltaAlert,
        observed_at: DateTime<Utc>,
    ) -> WatchlistNotificationMessage {
        let (label, links) = match entity_kind {
            WatchlistKind::Alliance => {
                let alliance = self.resolver.alliance(entity_id).await;
                (
                    entity_label(&alliance.name, &alliance.ticker, "Alliance", entity_id),
                    format!(
                        "[zKillboard](https://zkillboard.com/alliance/{entity_id}/) | [Dotlan](https://evemaps.dotlan.net/alliance/{entity_id})"
                    ),
                )
            }
            WatchlistKind::Corporation => {
                let corporation = self.resolver.corporation(entity_id).await;
                (
                    entity_label(
                        &corporation.name,
                        &corporation.ticker,
                        "Corporation",
                        entity_id,
                    ),
                    format!(
                        "[zKillboard](https://zkillboard.com/corporation/{entity_id}/) | [Dotlan](https://evemaps.dotlan.net/corp/{entity_id})"
                    ),
                )
            }
        };
        let direction_word = match alert.direction {
            MemberDeltaDirection::Up => "up",
            MemberDeltaDirection::Down => "down",
        };
        let title = format!(
            "{label} member count {direction_word} {}% ({} → {})",
            alert.percentage(),
            alert.reference_count,
            alert.current_count
        );
        let fields = vec![
            WatchlistEmbedField {
                name: "Members".to_string(),
                value: format!(
                    "{} → {} ({direction_word} {}%)",
                    alert.reference_count,
                    alert.current_count,
                    alert.percentage()
                ),
                inline: false,
            },
            WatchlistEmbedField {
                name: "Window".to_string(),
                value: format!(
                    "{} → {}",
                    alert.reference_observed_at.format("%Y-%m-%d %H:%M UTC"),
                    observed_at.format("%Y-%m-%d %H:%M UTC")
                ),
                inline: false,
            },
            WatchlistEmbedField {
                name: "Entity".to_string(),
                value: format!("{label}\n{links}"),
                inline: false,
            },
        ];
        WatchlistNotificationMessage {
            title,
            fields,
            footer: WatchlistEventKind::MemberDelta.footer_label().to_string(),
        }
    }

    /// Renders a `corp_changed_alliance` notification: the corporation and
    /// old → new alliance names/tickers ("none" when unaffiliated).
    async fn render_corp_changed_alliance(
        &self,
        corporation_id: i64,
        old_alliance: Option<i64>,
        new_alliance: Option<i64>,
    ) -> WatchlistNotificationMessage {
        let corporation = self.resolver.corporation(corporation_id).await;
        let corp_label = entity_label(
            &corporation.name,
            &corporation.ticker,
            "Corporation",
            corporation_id,
        );
        let old_label = self.alliance_label_or_none(old_alliance).await;
        let new_label = self.alliance_label_or_none(new_alliance).await;
        let title = format!("{corp_label} changed alliance: {old_label} → {new_label}");
        let fields = vec![
            WatchlistEmbedField {
                name: "Corporation".to_string(),
                value: format!(
                    "{corp_label}\n[zKillboard](https://zkillboard.com/corporation/{corporation_id}/) | [Dotlan](https://evemaps.dotlan.net/corp/{corporation_id})"
                ),
                inline: false,
            },
            WatchlistEmbedField {
                name: "From".to_string(),
                value: old_label,
                inline: true,
            },
            WatchlistEmbedField {
                name: "To".to_string(),
                value: new_label,
                inline: true,
            },
        ];
        WatchlistNotificationMessage {
            title,
            fields,
            footer: WatchlistEventKind::CorpChangedAlliance
                .footer_label()
                .to_string(),
        }
    }

    async fn alliance_label_or_none(&self, alliance_id: Option<i64>) -> String {
        match alliance_id {
            None => "none".to_string(),
            Some(id) => {
                let alliance = self.resolver.alliance(id).await;
                entity_label(&alliance.name, &alliance.ticker, "Alliance", id)
            }
        }
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
            // `render_event` is only ever called for corp join/left events;
            // the other kinds have their own renderers.
            _ => "changed",
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
