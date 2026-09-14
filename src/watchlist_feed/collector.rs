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
    derive_war_events, evaluate_member_delta, scan_war_page, MemberDeltaAlert,
    MemberDeltaDirection, MemberDeltaOutcome, PreparedWatchlistDelivery, WarDetail, WarEvent,
    WarParty, WarScanState, WatchlistDelivery, WatchlistEmbedField, WatchlistEntityResolver,
    WatchlistEvent, WatchlistEventKind, WatchlistKind, WatchlistNotificationMessage,
    WatchlistSubscription, MEMBER_DELTA_REFERENCE_WINDOW,
};
use crate::watchlist_feed::store::{
    alliance_resource_key, corp_resource_key, war_resource_key, wars_list_resource_key,
    WatchlistStore, WAR_FETCH_FAILURE_SKIP_THRESHOLD,
};
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

/// The maximum number of war-list pages (`GET /wars/`, 2000 ids each) the
/// head-scan reads above the high-water mark in one cycle. New wars accumulate
/// at the head at a rate far below 2000/hour, so a subsequent scan almost
/// always finishes on page 1; the cap only bounds a pathological backlog and
/// defers the rest to the next cycle (the mark is not advanced past unread
/// ids). The first (baseline) scan reads a single page.
const MAX_WAR_LIST_PAGES_PER_CYCLE: usize = 5;

/// The maximum number of new-war detail fetches (`GET /wars/{id}/`) the
/// head-scan issues per cycle. New watched wars are rare, so this only bounds
/// the first baseline scan of a fresh head page; any overflow is deferred (the
/// mark is not advanced past an unfetched id).
const MAX_NEW_WAR_DETAIL_FETCHES_PER_CYCLE: usize = 200;

/// The maximum number of stored OPEN wars re-fetched per cycle, ordered
/// least-recently-fetched first so coverage rotates like the corp-info cap.
/// Cache freshness (the route's one-hour TTL) skips most re-fetches anyway, so
/// this only bites with a very large set of concurrently-open watched wars.
const MAX_OPEN_WAR_REFETCHES_PER_CYCLE: i64 = 200;

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
    /// Whether the war head-scan completed (established) its silent baseline
    /// this cycle. The baseline may span several cycles; this is true only on
    /// the cycle it finally flips.
    pub war_baseline_established: bool,
    /// Whether the silent baseline is still in progress after this cycle
    /// (some head-page ids remain to inspect). Mutually exclusive with the
    /// established flip.
    pub war_baseline_pending: bool,
    /// War ids inspected by the baseline this cycle (a subset of the head page).
    pub war_baseline_scanned: usize,
    /// New war ids scanned above the high-water mark this cycle.
    pub wars_scanned: usize,
    /// War ids skipped this cycle after the failure ledger quarantined them
    /// (a 404, or a non-404 error persisting past the threshold). The mark
    /// advances past a skipped id.
    pub wars_skipped: usize,
    /// Wars stored this cycle (every inspected war, watched or not, including
    /// baseline seeding).
    pub wars_stored: usize,
    /// Stored open wars re-fetched (a fresh `200`) this cycle.
    pub wars_refetched: usize,
    /// Open watched wars whose re-fetch errored (non-404); counted and rotated
    /// to the back of the least-recently-fetched order so they retry later.
    pub wars_refetch_errors: usize,
    /// Open watched wars removed this cycle because their re-fetch returned a
    /// `404` (CCP deleted the war); marked finished, no event emitted.
    pub wars_removed: usize,
    /// War detail / list conditional requests that returned `304`.
    pub war_not_modified: usize,
    /// War detail fetches that errored (isolated per war).
    pub war_errors: usize,
    /// `war_declared` deliveries prepared.
    pub war_declared: usize,
    /// `war_ally_joined` deliveries prepared.
    pub war_ally_joined: usize,
    /// `war_retracted` deliveries prepared.
    pub war_retracted: usize,
    /// `war_finished` deliveries prepared.
    pub war_finished: usize,
    pub deliveries_prepared: usize,
}

/// The outcome of one war detail fetch (cache/limiter metadata already
/// persisted). `Failed` carries whether the error was a `404` so the caller
/// can distinguish a permanent absence from a transient failure.
enum WarFetch {
    Body(WarDetail),
    NotModified,
    Failed { not_found: bool, message: String },
}

/// How a post-baseline scanned war affects the contiguous safe prefix that
/// advances the high-water mark.
enum WarScanOutcome {
    /// Fetched and processed (the mark may advance across it).
    Processed,
    /// Quarantined and skipped (404, or a non-404 past the threshold); the
    /// mark advances past it as if it were processed.
    Skipped,
    /// A transient failure below the threshold; the mark must freeze below it.
    Frozen,
}

pub struct WatchlistCollector {
    store: WatchlistStore,
    esi: Arc<dyn WatchlistEsi>,
    limiter: Arc<dyn EsiLimiterStore>,
    delivery: Arc<dyn WatchlistDelivery>,
    resolver: Arc<dyn WatchlistEntityResolver>,
    clock: Arc<dyn WatchlistClock>,
    /// The per-cycle cap on new-war detail fetches (head-scan and baseline).
    /// Injectable so tests can force the baseline to span multiple cycles;
    /// defaults to [`MAX_NEW_WAR_DETAIL_FETCHES_PER_CYCLE`].
    war_detail_cap: usize,
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
            war_detail_cap: MAX_NEW_WAR_DETAIL_FETCHES_PER_CYCLE,
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn WatchlistClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Overrides the per-cycle new-war detail-fetch cap (tests only).
    #[doc(hidden)]
    pub fn with_war_detail_cap(mut self, cap: usize) -> Self {
        self.war_detail_cap = cap;
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

        // War head-scan and open-war re-fetch pass (ticket 10). Skipped when an
        // earlier pass already hit the shared limiter deadline; it resumes next
        // cycle. Its per-war errors are isolated inside the pass.
        if report.paused_until.is_none() {
            self.collect_wars(observed_at, &mut report).await?;
        }

        // Best-effort retention prune; a failure here must not fail the cycle.
        if let Err(error) = self.store.prune_member_snapshots(observed_at).await {
            warn!("watchlist member snapshot prune failed: {error}");
        }
        if let Err(error) = self.store.prune_finished_wars(observed_at).await {
            warn!("watchlist finished-war prune failed: {error}");
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

    /// The war pass (ticket 10): head-scan new war ids above the persisted
    /// high-water mark and re-fetch stored open wars, both bounded and
    /// limiter-aware, with per-war error isolation.
    async fn collect_wars(
        &self,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        self.head_scan_wars(observed_at, report).await?;
        if report.paused_until.is_some() {
            return Ok(());
        }
        self.refetch_open_wars(observed_at, report).await?;
        Ok(())
    }

    /// Head-scans the id-descending war list. Before the baseline is
    /// established this runs the multi-cycle silent baseline; afterwards it
    /// processes new ids above the high-water mark, storing every inspected
    /// war and emitting events for the watched ones.
    async fn head_scan_wars(
        &self,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let state = self.store.war_scan_state().await.map_err(store_error)?;
        if state.baseline_established {
            self.head_scan_new_wars(&state, observed_at, report).await
        } else {
            self.run_war_baseline(&state, observed_at, report).await
        }
    }

    /// The silent baseline, spanning as many cycles as its detail-fetch cap
    /// requires to inspect the ids that existed on the head page when it began.
    ///
    /// The very first baseline cycle reads the head page fresh to pin
    /// `baseline_head_max` (the mark set on completion) and `baseline_floor`
    /// (the bottom id of that original page). Every resume cycle then pages
    /// strictly DOWNWARD from the persisted cursor (`fetch_wars(Some(cursor))`)
    /// rather than re-reading the head: new wars enter at the head each hour
    /// and push the oldest ids out of the ~2000-id window, so an id that was on
    /// the original page but rolls off the head before the cursor reaches it is
    /// still reachable below the cursor. Each cycle inspects the next
    /// [`Self::war_detail_cap`] in-scope ids (`>= floor`, `< cursor`, newest
    /// first), stores every inspected war silently, and advances the cursor.
    ///
    /// The flag flips true (and the mark is set to the head max) only once the
    /// cursor reaches the floor -- i.e. a page's in-scope ids are exhausted
    /// without hitting the cap -- meaning every id that existed on the original
    /// page has been inspected. Ids strictly below the original floor are out
    /// of scope by design (never on the starting window). A pause or error
    /// never flips it. New wars declared during the baseline (ids above the
    /// head max) are ignored here and picked up by the first post-baseline scan.
    ///
    /// Cursor pages are plain GETs without `If-None-Match`: a per-cursor ETag is
    /// useless because the cursor moves every cycle, and storing it under the
    /// head's list resource key would corrupt the head-scan freshness gate. So
    /// only the first (head) cycle persists cache metadata under the list key;
    /// every page still records the limiter/rate-limit metadata after its
    /// response.
    #[allow(clippy::explicit_counter_loop)]
    async fn run_war_baseline(
        &self,
        state: &WarScanState,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        if let Some(deadline) = self
            .limiter
            .active_esi_limiter_deadline()
            .await
            .map_err(store_error)?
        {
            report.paused_until = Some(deadline);
            return Ok(());
        }
        let list_key = wars_list_resource_key();
        let cached = self
            .store
            .cache_metadata(&list_key)
            .await
            .map_err(store_error)?;
        // The first cycle reads the head page (no cursor persisted yet) to pin
        // the head max and floor; later cycles page downward from the cursor.
        let cursor = state.baseline_cursor;
        let is_head_cycle = cursor.is_none();
        // The head cycle forces a fresh fetch (no ETag) so it always sees the
        // ids; cursor pages are plain GETs (a moving cursor makes an ETag
        // useless).
        let response = match self.esi.fetch_wars(cursor, None).await {
            Ok(response) => response,
            Err(error) => {
                if is_head_cycle {
                    let merged = merge_cache_metadata(&cached, error.metadata.clone());
                    self.store
                        .persist_cache_metadata(&list_key, &merged)
                        .await
                        .map_err(store_error)?;
                }
                self.limiter
                    .record_esi_limiter_at(&error.metadata, observed_at)
                    .await
                    .map_err(store_error)?;
                report.war_errors += 1;
                warn!(
                    "watchlist war baseline list fetch failed; not flipping the baseline: {error}"
                );
                return Ok(());
            }
        };
        if is_head_cycle {
            let merged = merge_cache_metadata(&cached, response.metadata.clone());
            self.store
                .persist_cache_metadata(&list_key, &merged)
                .await
                .map_err(store_error)?;
        }
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;
        if response.not_modified {
            // A 304 carries no ids; do not flip or advance (the head cycle
            // needs the ids, and a no-ETag cursor page should not 304).
            report.war_not_modified += 1;
            return Ok(());
        }
        let ids = response.value.unwrap_or_default();
        // Pin the head max and floor on the head cycle; preserve them after.
        let head_max = state
            .baseline_head_max
            .unwrap_or_else(|| ids.iter().copied().max().unwrap_or(0));
        let floor = state
            .baseline_floor
            .unwrap_or_else(|| ids.iter().copied().min().unwrap_or(0));
        // In scope: ids that existed on the original page ([floor, head_max])
        // and still below the cursor. Ids below the floor are out of scope
        // (they were never on the starting window).
        let mut eligible: Vec<i64> = ids
            .into_iter()
            .filter(|id| *id <= head_max && *id >= floor && cursor.is_none_or(|c| *id < c))
            .collect();
        eligible.sort_unstable_by(|a, b| b.cmp(a)); // newest first

        let mut new_cursor = cursor;
        let mut fetches = 0usize;
        let mut incomplete = false; // stopped by cap or pause, not exhaustion
        for war_id in eligible {
            if fetches >= self.war_detail_cap {
                incomplete = true;
                break;
            }
            if let Some(deadline) = self
                .limiter
                .active_esi_limiter_deadline()
                .await
                .map_err(store_error)?
            {
                report.paused_until = Some(deadline);
                incomplete = true;
                break;
            }
            fetches += 1;
            self.baseline_store_war(war_id, observed_at, report).await?;
            new_cursor = Some(war_id); // descending, so this is the lowest seen
        }
        report.war_baseline_scanned += fetches;

        if incomplete {
            // Persist progress (pinning the head max and floor) but never flip.
            self.store
                .record_war_baseline_progress(head_max, new_cursor, floor, observed_at)
                .await
                .map_err(store_error)?;
            report.war_baseline_pending = true;
        } else {
            // The page's in-scope ids were exhausted without hitting the cap:
            // the cursor has reached the floor (a downward page always extends
            // to it, so exhausting it means every original-page id was
            // inspected). Complete the baseline and set the mark to the head
            // max so the next scan processes everything above.
            self.store
                .complete_war_baseline(head_max, observed_at)
                .await
                .map_err(store_error)?;
            report.war_baseline_established = true;
        }
        Ok(())
    }

    /// Stores one baseline-inspected war silently (no events). A failed or
    /// unchanged fetch is skipped best-effort; the cursor still advances past
    /// it so the one-time baseline cannot get stuck on a bad id.
    async fn baseline_store_war(
        &self,
        war_id: i64,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        match self
            .fetch_and_persist_war(war_id, observed_at, report)
            .await?
        {
            WarFetch::Body(detail) => {
                self.store
                    .upsert_war(&detail, observed_at)
                    .await
                    .map_err(store_error)?;
                self.store
                    .clear_war_fetch_attempts(war_id)
                    .await
                    .map_err(store_error)?;
                report.wars_stored += 1;
            }
            WarFetch::NotModified | WarFetch::Failed { .. } => {}
        }
        Ok(())
    }

    /// The post-baseline scan: pages the head above the mark, then processes
    /// new ids ascending. Every inspected war is stored; the mark advances
    /// across the contiguous safe prefix (successes and quarantined skips),
    /// while a transient failure breaks contiguity yet still lets higher ids
    /// this cycle be processed and posted.
    #[allow(clippy::explicit_counter_loop)]
    async fn head_scan_new_wars(
        &self,
        state: &WarScanState,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let mark = state.high_water_mark;
        let list_key = wars_list_resource_key();
        let cached = self
            .store
            .cache_metadata(&list_key)
            .await
            .map_err(store_error)?;
        // Within the list's one-hour freshness the head cannot have changed.
        if cached.is_fresh() {
            return Ok(());
        }

        let mut new_ids: Vec<i64> = Vec::new();
        let mut highest_seen: Option<i64> = None;
        let mut max_war_id: Option<i64> = None;
        let mut pages = 0usize;
        loop {
            if let Some(deadline) = self
                .limiter
                .active_esi_limiter_deadline()
                .await
                .map_err(store_error)?
            {
                report.paused_until = Some(deadline);
                return Ok(());
            }
            // Only page 1 (the head) carries the stored ETag; deeper pages are
            // distinct URLs fetched fresh.
            let etag = if max_war_id.is_none() {
                cached.etag.clone()
            } else {
                None
            };
            let response = match self.esi.fetch_wars(max_war_id, etag).await {
                Ok(response) => response,
                Err(error) => {
                    if max_war_id.is_none() {
                        let merged = merge_cache_metadata(&cached, error.metadata.clone());
                        self.store
                            .persist_cache_metadata(&list_key, &merged)
                            .await
                            .map_err(store_error)?;
                    }
                    self.limiter
                        .record_esi_limiter_at(&error.metadata, observed_at)
                        .await
                        .map_err(store_error)?;
                    report.war_errors += 1;
                    warn!("watchlist war list scan failed; keeping the high-water mark: {error}");
                    return Ok(());
                }
            };
            if max_war_id.is_none() {
                let merged = merge_cache_metadata(&cached, response.metadata.clone());
                self.store
                    .persist_cache_metadata(&list_key, &merged)
                    .await
                    .map_err(store_error)?;
            }
            self.limiter
                .record_esi_limiter_at(&response.metadata, observed_at)
                .await
                .map_err(store_error)?;

            if response.not_modified {
                report.war_not_modified += 1;
                break;
            }
            let ids = response.value.unwrap_or_default();
            let scan = scan_war_page(&ids, mark);
            if highest_seen.is_none() {
                highest_seen = scan.highest_seen;
            }
            new_ids.extend(scan.new_ids);
            pages += 1;
            match scan.next_max_war_id {
                Some(next) if pages < MAX_WAR_LIST_PAGES_PER_CYCLE => {
                    max_war_id = Some(next);
                    continue;
                }
                _ => break,
            }
        }

        report.wars_scanned += new_ids.len();
        let mark_target = highest_seen.unwrap_or(mark).max(mark);

        new_ids.sort_unstable();
        let mut contiguous_safe = mark;
        let mut broke = false; // contiguity broken by a transient (non-quarantined) failure
        let mut incomplete = false; // stopped early by cap or pause
        let mut fetches = 0usize;
        for war_id in new_ids {
            if fetches >= self.war_detail_cap {
                incomplete = true;
                break;
            }
            if let Some(deadline) = self
                .limiter
                .active_esi_limiter_deadline()
                .await
                .map_err(store_error)?
            {
                report.paused_until = Some(deadline);
                incomplete = true;
                break;
            }
            fetches += 1;
            match self.scan_new_war(war_id, observed_at, report).await? {
                WarScanOutcome::Processed | WarScanOutcome::Skipped => {
                    // Successes and quarantined skips extend the safe prefix
                    // (a skipped id is treated as safe: the mark moves past it).
                    if !broke {
                        contiguous_safe = contiguous_safe.max(war_id);
                    }
                }
                WarScanOutcome::Frozen => {
                    // Transient failure below the skip threshold: freeze the
                    // mark below it (retried next cycle) but keep processing
                    // higher ids this cycle.
                    broke = true;
                }
            }
        }
        let final_mark = if broke || incomplete {
            contiguous_safe
        } else {
            mark_target
        };
        self.store
            .record_war_scan_mark(final_mark, observed_at)
            .await
            .map_err(store_error)?;
        Ok(())
    }

    /// Fetches and processes one new (above-the-mark) war: stores it and emits
    /// its events. On failure, consults the bounded skip ledger and returns
    /// whether the id is safe to advance the mark past, must be quarantined, or
    /// must freeze the mark below it.
    async fn scan_new_war(
        &self,
        war_id: i64,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<WarScanOutcome, WatchlistCollectionError> {
        match self
            .fetch_and_persist_war(war_id, observed_at, report)
            .await?
        {
            WarFetch::Body(detail) => {
                self.store
                    .clear_war_fetch_attempts(war_id)
                    .await
                    .map_err(store_error)?;
                self.store_and_emit_war(&detail, observed_at, report)
                    .await?;
                Ok(WarScanOutcome::Processed)
            }
            WarFetch::NotModified => {
                // A new war carries no stored ETag, so a 304 is unexpected;
                // treat it as handled and clear any ledger row.
                self.store
                    .clear_war_fetch_attempts(war_id)
                    .await
                    .map_err(store_error)?;
                Ok(WarScanOutcome::Processed)
            }
            WarFetch::Failed { not_found, message } => {
                if not_found {
                    // A permanent absence: skip immediately, mark advances past.
                    self.store
                        .clear_war_fetch_attempts(war_id)
                        .await
                        .map_err(store_error)?;
                    report.wars_skipped += 1;
                    warn!(
                        "watchlist war {war_id} not found (404); skipping and advancing the mark"
                    );
                    return Ok(WarScanOutcome::Skipped);
                }
                let attempts = self
                    .store
                    .record_war_fetch_attempt(war_id, &message, observed_at)
                    .await
                    .map_err(store_error)?;
                if attempts >= WAR_FETCH_FAILURE_SKIP_THRESHOLD {
                    self.store
                        .clear_war_fetch_attempts(war_id)
                        .await
                        .map_err(store_error)?;
                    report.wars_skipped += 1;
                    warn!("watchlist war {war_id} failed {attempts} cycles; skipping and advancing the mark: {message}");
                    Ok(WarScanOutcome::Skipped)
                } else {
                    // Not yet at the threshold: freeze the mark below it.
                    Ok(WarScanOutcome::Frozen)
                }
            }
        }
    }

    /// Stores one inspected war and emits its derived events. Every inspected
    /// war is stored (watched or not); events for unwatched wars fan out to no
    /// guilds and produce no deliveries.
    async fn store_and_emit_war(
        &self,
        detail: &WarDetail,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let previous = self
            .store
            .stored_war_observation(detail.war_id)
            .await
            .map_err(store_error)?;
        let events = derive_war_events(previous.as_ref(), detail);
        self.store
            .upsert_war(detail, observed_at)
            .await
            .map_err(store_error)?;
        if previous.is_none() {
            report.wars_stored += 1;
        }
        for event in events {
            self.prepare_war_event(&event, detail, observed_at, report)
                .await?;
        }
        Ok(())
    }

    /// Re-fetches currently-watched stored open wars (bounded,
    /// least-recently-fetched) and emits ally-joined / retracted / finished
    /// events. "Watched" is derived at query time, so a war whose only
    /// watching entity was removed drops out (no more re-fetches) and a war
    /// stored before its party was watched is re-fetched once that party is
    /// added. Per-war errors are isolated.
    async fn refetch_open_wars(
        &self,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let war_ids = self
            .store
            .open_watched_war_ids_least_recently_fetched(
                MAX_OPEN_WAR_REFETCHES_PER_CYCLE,
                observed_at,
            )
            .await
            .map_err(store_error)?;
        for war_id in war_ids {
            if let Some(deadline) = self
                .limiter
                .active_esi_limiter_deadline()
                .await
                .map_err(store_error)?
            {
                report.paused_until = Some(deadline);
                break;
            }
            // Per-war ESI errors are isolated inside `refetch_open_war`
            // (counted + warned, returning `Ok`); only a store error
            // propagates and fails the cycle.
            self.refetch_open_war(war_id, observed_at, report).await?;
        }
        Ok(())
    }

    async fn refetch_open_war(
        &self,
        war_id: i64,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let key = war_resource_key(war_id);
        let cached = self.store.cache_metadata(&key).await.map_err(store_error)?;
        if cached.is_fresh() {
            return Ok(());
        }
        let detail = match self
            .fetch_and_persist_war(war_id, observed_at, report)
            .await?
        {
            WarFetch::Failed {
                not_found: true, ..
            } => {
                // The war was deleted by CCP: mark it gone so it drops out of
                // the open-war re-fetch query (otherwise its stale
                // `last_fetched_at` re-selects it first every cycle forever)
                // and is pruned later. No event is emitted for the removal.
                self.store
                    .mark_war_removed(war_id, observed_at)
                    .await
                    .map_err(store_error)?;
                report.wars_removed += 1;
                warn!("watchlist war {war_id} not found (404) on re-fetch; marking it removed");
                return Ok(());
            }
            WarFetch::Failed {
                not_found: false, ..
            } => {
                // A transient failure (already warned + counted in
                // `war_errors`): bump `last_fetched_at` so this war rotates to
                // the back of the least-recently-fetched order and other open
                // wars get a turn before it is retried.
                self.store
                    .mark_war_fetched(war_id, observed_at)
                    .await
                    .map_err(store_error)?;
                report.wars_refetch_errors += 1;
                return Ok(());
            }
            WarFetch::NotModified => {
                // 304: content unchanged; rotate the least-recently-fetched
                // order so other open wars get a turn.
                self.store
                    .mark_war_fetched(war_id, observed_at)
                    .await
                    .map_err(store_error)?;
                return Ok(());
            }
            WarFetch::Body(detail) => detail,
        };
        let previous = self
            .store
            .stored_war_observation(war_id)
            .await
            .map_err(store_error)?;
        let events = derive_war_events(previous.as_ref(), &detail);
        self.store
            .upsert_war(&detail, observed_at)
            .await
            .map_err(store_error)?;
        report.wars_refetched += 1;
        for event in events {
            self.prepare_war_event(&event, &detail, observed_at, report)
                .await?;
        }
        Ok(())
    }

    /// Fetches one war's detail, persisting cache and limiter metadata after
    /// every response (feeding the shared `killmail` bucket the health snapshot
    /// surfaces). Returns [`WarFetch::Failed`] on an isolated ESI error
    /// (counted + warned, carrying whether it was a 404), [`WarFetch::NotModified`]
    /// on a `304`, and [`WarFetch::Body`] on a fresh body.
    async fn fetch_and_persist_war(
        &self,
        war_id: i64,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<WarFetch, WatchlistCollectionError> {
        let key = war_resource_key(war_id);
        let cached = self.store.cache_metadata(&key).await.map_err(store_error)?;
        let response = match self.esi.fetch_war(war_id, cached.etag.clone()).await {
            Ok(response) => response,
            Err(error) => {
                let merged = merge_cache_metadata(&cached, error.metadata.clone());
                self.store
                    .persist_cache_metadata(&key, &merged)
                    .await
                    .map_err(store_error)?;
                self.limiter
                    .record_esi_limiter_at(&error.metadata, observed_at)
                    .await
                    .map_err(store_error)?;
                report.war_errors += 1;
                warn!("watchlist war {war_id} detail fetch failed: {error}");
                return Ok(WarFetch::Failed {
                    not_found: error.is_not_found(),
                    message: error.to_string(),
                });
            }
        };
        let merged = merge_cache_metadata(&cached, response.metadata.clone());
        self.store
            .persist_cache_metadata(&key, &merged)
            .await
            .map_err(store_error)?;
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;
        if response.not_modified {
            report.war_not_modified += 1;
            return Ok(WarFetch::NotModified);
        }
        match response.value {
            Some(detail) => Ok(WarFetch::Body(detail)),
            None => Ok(WarFetch::NotModified),
        }
    }

    /// Fans one war event out to every subscription that wants it across the
    /// guilds watching any involved party, rendering the embed once (only when
    /// a delivery row will be inserted) and storing it verbatim.
    async fn prepare_war_event(
        &self,
        event: &WarEvent,
        detail: &WarDetail,
        observed_at: DateTime<Utc>,
        report: &mut WatchlistCollectionReport,
    ) -> Result<(), WatchlistCollectionError> {
        let evidence_key = event.evidence_key();
        let mut guild_set = BTreeSet::new();
        for party in detail.involved_parties() {
            for guild_id in self
                .store
                .guilds_watching_entity(party.kind, party.id)
                .await
                .map_err(store_error)?
            {
                guild_set.insert(guild_id);
            }
        }
        let guilds: Vec<u64> = guild_set.into_iter().collect();
        let targets = self
            .delivery_targets(event.kind, event.war_id, &evidence_key, guilds)
            .await?;
        if targets.is_empty() {
            return Ok(());
        }
        let message = self
            .render_war_event(detail, event.kind, event.ally, observed_at)
            .await;
        let prepared = self
            .insert_and_send(
                targets,
                event.kind,
                event.war_id,
                &evidence_key,
                &message,
                observed_at,
            )
            .await?;
        report.deliveries_prepared += prepared;
        match event.kind {
            WatchlistEventKind::WarDeclared => report.war_declared += prepared,
            WatchlistEventKind::WarAllyJoined => report.war_ally_joined += prepared,
            WatchlistEventKind::WarRetracted => report.war_retracted += prepared,
            WatchlistEventKind::WarFinished => report.war_finished += prepared,
            _ => {}
        }
        Ok(())
    }

    async fn party_label(&self, party: WarParty) -> String {
        match party.kind {
            WatchlistKind::Alliance => {
                let alliance = self.resolver.alliance(party.id).await;
                entity_label(&alliance.name, &alliance.ticker, "Alliance", party.id)
            }
            WatchlistKind::Corporation => {
                let corporation = self.resolver.corporation(party.id).await;
                entity_label(
                    &corporation.name,
                    &corporation.ticker,
                    "Corporation",
                    party.id,
                )
            }
        }
    }

    /// Renders one war event, resolving aggressor/defender (and the ally, for
    /// an ally-join) identities at prepare time and storing the content
    /// verbatim (spec "Embed": war id, parties + tickers, mutual/open-for-allies
    /// flags, relevant timestamps, zKillboard war link; footer names the kind).
    async fn render_war_event(
        &self,
        detail: &WarDetail,
        kind: WatchlistEventKind,
        ally: Option<WarParty>,
        observed_at: DateTime<Utc>,
    ) -> WatchlistNotificationMessage {
        let aggressor_label = self.party_label(detail.aggressor).await;
        let defender_label = self.party_label(detail.defender).await;
        let title = match kind {
            WatchlistEventKind::WarDeclared => {
                format!("War declared: {aggressor_label} vs {defender_label}")
            }
            WatchlistEventKind::WarAllyJoined => {
                format!("Ally joined war {}", detail.war_id)
            }
            WatchlistEventKind::WarRetracted => {
                format!("War retracted: {aggressor_label} vs {defender_label}")
            }
            WatchlistEventKind::WarFinished => {
                format!("War finished: {aggressor_label} vs {defender_label}")
            }
            _ => format!("War {}", detail.war_id),
        };
        let mut fields = vec![
            WatchlistEmbedField {
                name: "War".to_string(),
                value: format!("[#{0}](https://zkillboard.com/war/{0}/)", detail.war_id),
                inline: true,
            },
            WatchlistEmbedField {
                name: "Aggressor".to_string(),
                value: aggressor_label.clone(),
                inline: true,
            },
            WatchlistEmbedField {
                name: "Defender".to_string(),
                value: defender_label.clone(),
                inline: true,
            },
        ];
        if kind == WatchlistEventKind::WarAllyJoined {
            if let Some(ally) = ally {
                fields.push(WatchlistEmbedField {
                    name: "Ally".to_string(),
                    value: self.party_label(ally).await,
                    inline: true,
                });
            }
        }
        fields.push(WatchlistEmbedField {
            name: "Flags".to_string(),
            value: format!(
                "Mutual: {} | Open for allies: {}",
                yes_no(detail.mutual),
                yes_no(detail.open_for_allies)
            ),
            inline: false,
        });
        let mut timeline = Vec::new();
        if let Some(declared) = detail.declared {
            timeline.push(format!(
                "Declared: {}",
                declared.format("%Y-%m-%d %H:%M UTC")
            ));
        }
        if let Some(started) = detail.started {
            timeline.push(format!("Started: {}", started.format("%Y-%m-%d %H:%M UTC")));
        }
        if kind == WatchlistEventKind::WarRetracted {
            if let Some(retracted) = detail.retracted {
                timeline.push(format!(
                    "Retracted: {}",
                    retracted.format("%Y-%m-%d %H:%M UTC")
                ));
            }
        }
        if kind == WatchlistEventKind::WarFinished {
            if let Some(finished) = detail.finished {
                timeline.push(format!(
                    "Finished: {}",
                    finished.format("%Y-%m-%d %H:%M UTC")
                ));
            }
        }
        if !timeline.is_empty() {
            fields.push(WatchlistEmbedField {
                name: "Timeline".to_string(),
                value: timeline.join("\n"),
                inline: false,
            });
        }
        let content = crate::contract_intelligence::neutralize_summary_mentions(&title);
        let war_color = match kind {
            WatchlistEventKind::WarDeclared => 0xC0_39_2B,
            WatchlistEventKind::WarAllyJoined => 0xE6_7E_22,
            WatchlistEventKind::WarRetracted | WatchlistEventKind::WarFinished => 0x2E_CC_71,
            _ => 0x95_A5_A6,
        };
        let war_url = format!("https://zkillboard.com/war/{}/", detail.war_id);
        WatchlistNotificationMessage {
            title,
            fields,
            footer: format!(
                "{} \u{2022} EVETime {}",
                kind.footer_label(),
                observed_at.format("%d/%m/%Y, %H:%M")
            ),
            content: Some(content),
            author: Some(format!("War #{}", detail.war_id)),
            author_url: Some(war_url.clone()),
            author_icon: None,
            description: Some(format!(
                "{aggressor_label} vs {defender_label}\nMutual: {} \u{b7} Open for allies: {}",
                yes_no(detail.mutual),
                yes_no(detail.open_for_allies)
            )),
            thumbnail_url: None,
            url: Some(war_url),
            color: Some(war_color),
            footer_icon: None,
            timestamp: Some(observed_at),
        }
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
        let message = self.render_event(event, observed_at).await;
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
            .render_corp_changed_alliance(corporation_id, old_alliance, new_alliance, observed_at)
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
        // Plain-language swing verb (ticket 20: "lost 12% of members" /
        // "gained 12% of members").
        let swing_verb = match alert.direction {
            MemberDeltaDirection::Up => "gained",
            MemberDeltaDirection::Down => "lost",
        };
        let title = format!(
            "{label} {swing_verb} {}% of members ({} → {})",
            alert.percentage(),
            thousands(alert.reference_count),
            thousands(alert.current_count)
        );
        let content = crate::contract_intelligence::neutralize_summary_mentions(&title);
        let ref_unix = alert.reference_observed_at.timestamp();
        let fields = vec![
            WatchlistEmbedField {
                name: "Members".to_string(),
                value: format!(
                    "{} → {} ({direction_word} {}%)",
                    thousands(alert.reference_count),
                    thousands(alert.current_count),
                    alert.percentage()
                ),
                inline: true,
            },
            WatchlistEmbedField {
                name: "Since".to_string(),
                value: format!("<t:{ref_unix}:f>"),
                inline: true,
            },
        ];
        let logo = match entity_kind {
            WatchlistKind::Alliance => wl_alliance_logo(entity_id),
            WatchlistKind::Corporation => wl_corp_logo(entity_id),
        };
        let (author_url, url) = match entity_kind {
            WatchlistKind::Alliance => (
                format!("https://zkillboard.com/alliance/{entity_id}/"),
                format!("https://zkillboard.com/alliance/{entity_id}/"),
            ),
            WatchlistKind::Corporation => (
                format!("https://zkillboard.com/corporation/{entity_id}/"),
                format!("https://zkillboard.com/corporation/{entity_id}/"),
            ),
        };
        WatchlistNotificationMessage {
            title,
            fields,
            footer: format!(
                "{} \u{2022} EVETime {}",
                WatchlistEventKind::MemberDelta.footer_label(),
                observed_at.format("%d/%m/%Y, %H:%M")
            ),
            content: Some(content),
            author: Some(format!("Watching {label}")),
            author_url: Some(author_url),
            author_icon: Some(logo.clone()),
            description: Some(format!(
                "since <t:{ref_unix}:R> (<t:{ref_unix}:f>)\n{links}"
            )),
            thumbnail_url: Some(logo),
            url: Some(url),
            // blue: a membership swing (ticket 20 colour rule).
            color: Some(0x34_98_DB),
            footer_icon: None,
            timestamp: Some(observed_at),
        }
    }

    /// Renders a `corp_changed_alliance` notification: the corporation and
    /// old → new alliance names/tickers ("none" when unaffiliated).
    async fn render_corp_changed_alliance(
        &self,
        corporation_id: i64,
        old_alliance: Option<i64>,
        new_alliance: Option<i64>,
        observed_at: DateTime<Utc>,
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
        // Plain language: "Corp [T] left A [A], joined B [B]", with the
        // half dropped when the corporation was previously unaffiliated
        // (join only) or is now unaffiliated (leave only).
        let title = match (old_alliance, new_alliance) {
            (Some(_), Some(_)) => format!("{corp_label} left {old_label}, joined {new_label}"),
            (Some(_), None) => format!("{corp_label} left {old_label}"),
            (None, Some(_)) => format!("{corp_label} joined {new_label}"),
            (None, None) => format!("{corp_label} changed alliance"),
        };
        let content = crate::contract_intelligence::neutralize_summary_mentions(&title);
        let fields = vec![
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
            footer: format!(
                "{} \u{2022} EVETime {}",
                WatchlistEventKind::CorpChangedAlliance.footer_label(),
                observed_at.format("%d/%m/%Y, %H:%M")
            ),
            content: Some(content),
            author: Some(format!("Watching {corp_label}")),
            author_url: Some(format!(
                "https://zkillboard.com/corporation/{corporation_id}/"
            )),
            author_icon: Some(wl_corp_logo(corporation_id)),
            description: Some(format!(
                "[Dotlan](https://evemaps.dotlan.net/corp/{corporation_id}) \u{b7} [zKillboard](https://zkillboard.com/corporation/{corporation_id}/)"
            )),
            thumbnail_url: Some(wl_corp_logo(corporation_id)),
            url: Some(format!(
                "https://zkillboard.com/corporation/{corporation_id}/"
            )),
            // orange: a corporation changed alliance (ticket 20 colour rule).
            color: Some(0xE6_7E_22),
            footer_icon: None,
            timestamp: Some(observed_at),
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
    async fn render_event(
        &self,
        event: &WatchlistEvent,
        observed_at: DateTime<Utc>,
    ) -> WatchlistNotificationMessage {
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
        let alliance_name = entity_name_only(
            &alliance.name,
            &alliance.ticker,
            "Alliance",
            event.alliance_id,
        );
        let joined = matches!(event.kind, WatchlistEventKind::CorpJoined);
        let verb = if joined { "joined" } else { "left" };

        // Alliance aggregate for the author line and the description
        // "is now N corps, M members" clause (ticket 20: corp count and
        // member total from the store).
        let corp_count = self
            .store
            .current_alliance_member_ids(event.alliance_id)
            .await
            .map(|ids| ids.len() as i64)
            .unwrap_or_default();
        let member_total = self
            .store
            .current_alliance_member_count(event.alliance_id)
            .await
            .ok()
            .and_then(|aggregate| aggregate.total);

        // --- content (phone banner) ---
        let members_clause = corporation
            .member_count
            .map(|count| format!(" ({} members)", thousands(count)))
            .unwrap_or_default();
        let content = crate::contract_intelligence::neutralize_summary_mentions(&format!(
            "{corp_label}{members_clause} {verb} {alliance_label}"
        ));

        // --- title ---
        let title = format!("{corp_label} {verb} {alliance_name}");

        // --- description ---
        let mut corp_facts = Vec::new();
        if let Some(count) = corporation.member_count {
            corp_facts.push(format!("{} members", thousands(count)));
        }
        if let Some(founded) = corporation.date_founded {
            corp_facts.push(format!("founded {}", founded.format("%Y")));
        }
        corp_facts.push(format!(
            "[Dotlan](https://evemaps.dotlan.net/corp/{0}) \u{b7} [zKillboard](https://zkillboard.com/corporation/{0}/)",
            event.corporation_id
        ));
        let mut description_lines = vec![corp_facts.join(" \u{b7} ")];
        let this_cycle = corporation
            .member_count
            .map(|count| if joined { count } else { -count });
        let mut aggregate_clause =
            format!("{alliance_name} is now {} corps", thousands(corp_count));
        if let Some(total) = member_total {
            aggregate_clause.push_str(&format!(", {} members", thousands(total)));
        }
        if let Some(delta) = this_cycle {
            aggregate_clause.push_str(&format!(" ({} this cycle)", signed_thousands(delta)));
        }
        description_lines.push(aggregate_clause);
        if !joined {
            // Where the corporation went (ticket 20 corp_left).
            let destination = match corporation.alliance_id {
                Some(new_alliance) => {
                    let dest = self.resolver.alliance(new_alliance).await;
                    format!(
                        "now in {}",
                        entity_label(&dest.name, &dest.ticker, "Alliance", new_alliance)
                    )
                }
                None => "now unaffiliated".to_string(),
            };
            description_lines.push(destination);
        }

        // --- author ---
        let mut author = format!(
            "Watching {alliance_name} \u{b7} {} corps",
            thousands(corp_count)
        );
        if let Some(total) = member_total {
            author.push_str(&format!(" \u{b7} {} members", thousands(total)));
        }

        let fields = vec![
            WatchlistEmbedField {
                name: "Corporation".to_string(),
                value: corp_label.clone(),
                inline: true,
            },
            WatchlistEmbedField {
                name: "Alliance".to_string(),
                value: alliance_label.clone(),
                inline: true,
            },
        ];

        WatchlistNotificationMessage {
            title,
            fields,
            footer: format!(
                "{} \u{2022} EVETime {}",
                event.kind.footer_label(),
                observed_at.format("%d/%m/%Y, %H:%M")
            ),
            content: Some(content),
            author: Some(author),
            author_url: Some(format!(
                "https://zkillboard.com/alliance/{}/",
                event.alliance_id
            )),
            author_icon: Some(wl_alliance_logo(event.alliance_id)),
            description: Some(description_lines.join("\n")),
            thumbnail_url: Some(wl_corp_logo(event.corporation_id)),
            url: Some(format!(
                "https://zkillboard.com/corporation/{}/",
                event.corporation_id
            )),
            color: Some(if joined { 0x2E_CC_71 } else { 0xE7_4C_3C }),
            footer_icon: Some(wl_alliance_logo(event.alliance_id)),
            timestamp: Some(observed_at),
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

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

/// Groups a positive integer with thousands separators (ticket 20: member
/// counts are never abbreviated, e.g. `1,540`). Negative values keep their
/// sign.
fn thousands(value: i64) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let mut grouped = String::new();
    let len = digits.len();
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (len - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    if negative {
        format!("-{grouped}")
    } else {
        grouped
    }
}

/// A signed member-count delta with an explicit sign for the "this cycle"
/// clause (ticket 20: `(+124 this cycle)` / `(-12 this cycle)`).
fn signed_thousands(value: i64) -> String {
    if value >= 0 {
        format!("+{}", thousands(value))
    } else {
        thousands(value)
    }
}

fn wl_alliance_logo(alliance_id: i64) -> String {
    format!("https://images.evetech.net/alliances/{alliance_id}/logo?size=64")
}

fn wl_corp_logo(corporation_id: i64) -> String {
    format!("https://images.evetech.net/corporations/{corporation_id}/logo?size=64")
}

/// The `Name` part of an entity label (no ticker), for the plain-language
/// titles that read "... joined Snuffed Out" rather than repeating the
/// ticker already carried in the corporation label.
fn entity_name_only(
    name: &Option<String>,
    ticker: &Option<String>,
    kind_label: &str,
    id: i64,
) -> String {
    match (name.as_deref(), ticker.as_deref()) {
        (Some(name), _) => name.to_string(),
        (None, Some(ticker)) => format!("[{ticker}]"),
        (None, None) => format!("{kind_label} {id}"),
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

#[cfg(test)]
mod dense_render_tests {
    use super::*;

    #[test]
    fn thousands_groups_without_abbreviating() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(124), "124");
        assert_eq!(thousands(1_540), "1,540");
        assert_eq!(thousands(1_355), "1,355");
        assert_eq!(thousands(1_000_000), "1,000,000");
        assert_eq!(thousands(-12), "-12");
    }

    #[test]
    fn signed_thousands_carries_an_explicit_sign() {
        assert_eq!(signed_thousands(124), "+124");
        assert_eq!(signed_thousands(-124), "-124");
        assert_eq!(signed_thousands(1_540), "+1,540");
    }

    #[test]
    fn entity_name_only_drops_the_ticker() {
        assert_eq!(
            entity_name_only(
                &Some("Snuffed Out".to_string()),
                &Some("B B C".to_string()),
                "Alliance",
                1
            ),
            "Snuffed Out"
        );
        assert_eq!(
            entity_name_only(&None, &Some("B B C".to_string()), "Alliance", 1),
            "[B B C]"
        );
        assert_eq!(entity_name_only(&None, &None, "Alliance", 7), "Alliance 7");
    }
}
