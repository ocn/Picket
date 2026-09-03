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
    effective_tminus_marks_minutes, effective_tz_shift_enabled, effective_tz_window,
    tz_window_overlap_minutes, PreparedSovDelivery, SovAlertStage, SovCampaign, SovDelivery,
    SovEmbedField, SovNotificationMessage, SovStructure, SOV_HUB_STRUCTURE_TYPE_IDS,
    SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES,
};
use crate::sov_feed::store::{
    SovReachabilityTransition, SovStore, SovTzWindowTransition, SOV_CAMPAIGNS_RESOURCE_KEY,
    SOV_MAP_RESOURCE_KEY, SOV_RETENTION, SOV_STRUCTURES_RESOURCE_KEY,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Supplies a guild's currently-watched alliance ids so a sov
/// `Defender { watchlist: true }` leaf can be resolved against that guild's
/// watchlist at evaluation time (ticket 08). The sov collector holds this
/// as a trait object obtained at composition-root wiring time
/// (`src/lib.rs`), so `sov_feed` does not depend on `watchlist_feed`
/// directly. The default [`NoSovWatchlist`] returns an empty list for every
/// guild, which leaves an unresolved watchlist leaf never matching -- the
/// correct "no watchlist context" fallback for every existing collector.
#[async_trait]
pub trait SovWatchlistSource: Send + Sync {
    async fn watched_alliance_ids(&self, guild_id: u64) -> Result<Vec<i64>, sqlx::Error>;
}

/// The default watchlist source: no guild watches anything, so a
/// `Defender { watchlist: true }` leaf resolves to an empty `alliance_ids`
/// and never matches. Used by every collector that predates ticket 08 and
/// by the structures/map loops, which never evaluate subscriptions.
pub struct NoSovWatchlist;

#[async_trait]
impl SovWatchlistSource for NoSovWatchlist {
    async fn watched_alliance_ids(&self, _guild_id: u64) -> Result<Vec<i64>, sqlx::Error> {
        Ok(Vec::new())
    }
}

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

/// Alliance/corporation ticker and faction name lookup, with an ESI
/// fallback for entries not yet cached (mirrors the killfeed embed's
/// ticker resolution). `corporation_ticker` and `faction_name` default to
/// `None` so every fake implementing only `alliance_ticker` from before
/// ticket 07 keeps compiling unchanged; the sovereignty map's "current
/// owner" field (ticket 07, spec "Embed": "current owner from the
/// sovereignty map") is the only caller of the two new methods.
#[async_trait]
pub trait SovTickerResolver: Send + Sync {
    async fn alliance_ticker(&self, alliance_id: i64) -> Option<String>;

    async fn corporation_ticker(&self, _corporation_id: i64) -> Option<String> {
        None
    }

    async fn faction_name(&self, _faction_id: i64) -> Option<String> {
        None
    }
}

/// Per-campaign reachability facts for the `Reachable` filter leaf and
/// embed rendering (ticket 04, spec "Reachability" and "Embed"): jumps
/// from the configured home system and the system-ID route, home first.
/// `via_chain` and `path_risk` are ticket 05 additions: `via_chain` is
/// `true` when the chosen route crosses at least one Wanderer chain
/// connection (wormhole, gate, or bridge) rather than pure static
/// stargates; `path_risk` is `Some` exactly when the route crosses at
/// least one *wormhole* connection, since gate/bridge jumps carry no risk
/// (spec "Embed": the Path Risk line is "omitted for gate-only routes").
#[derive(Clone, Debug, PartialEq)]
pub struct SovReachabilityInfo {
    pub jumps: i64,
    pub route: Vec<i64>,
    pub via_chain: bool,
    pub path_risk: Option<crate::sov_feed::chain::PathRisk>,
}

/// Reachability lookup for the `Reachable` filter leaf and embed
/// rendering. `reachable` returns `None` both when the system is
/// unreachable within the loaded graph and when no graph is loaded at all
/// (spec: a missing/malformed stargate file degrades the feed to
/// "Reachable never matches", never a startup failure);
/// [`SovReachabilitySource::graph_available`] distinguishes the two so
/// `/sov_timers` can say "unreachable" vs "graph unavailable".
/// `allow_frigate_holes` selects which of the two cached BFS results
/// (spec "Reachability": "Two BFS results are cached per chain snapshot")
/// a lookup uses; implementations that have no notion of frigate holes
/// (stargates only) ignore it.
pub trait SovReachabilitySource: Send + Sync {
    fn reachable(
        &self,
        solar_system_id: i64,
        allow_frigate_holes: bool,
    ) -> Option<SovReachabilityInfo>;
    fn graph_available(&self) -> bool;

    /// Whether the reachability source's graph reflects a real observation
    /// yet -- the readiness signal that gates recording reachability
    /// observations (ticket 06). Defaults to `true` for every source with a
    /// static or immediately-available graph (`StaticSovReachability`, every
    /// test fake): those are ready the moment they are constructed. Only
    /// [`crate::sov_feed::chain::DynamicChainSovReachability`] overrides it,
    /// returning `false` until the Wanderer chain has been computed from a
    /// live fetch or a persisted-snapshot seed at least once this process, so
    /// a restart across a Wanderer outage records no observation (and thus
    /// produces no burst of spurious "now reachable" alerts) until the chain
    /// is actually loaded.
    fn reachability_ready(&self) -> bool {
        true
    }

    /// Whether a Wanderer chain feed is configured and, if so, how fresh
    /// its most recent successful fetch is (ticket 05, spec "On Wanderer
    /// errors ... the last good snapshot is kept and marked stale after
    /// ten minutes; stale state is visible ... in `/sov_timers`"). Default
    /// implementation covers every reachability source with no chain
    /// concept at all (`StaticSovReachability`, every test fake that
    /// predates this ticket) without requiring any change to them.
    fn chain_status(
        &self,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> crate::sov_feed::chain::SovChainStatus {
        crate::sov_feed::chain::SovChainStatus::NotConfigured
    }
}

/// Adapts a computed [`crate::sov_feed::graph::Reachability`] (or its
/// absence) into [`SovReachabilitySource`]. `None` is the "graph
/// unavailable" state the composition root (`src/lib.rs`) falls back to
/// when `config/stargates.json` is missing or malformed at start-up, or
/// when the sov feed is disabled entirely; it is also the default used by
/// every existing test that does not care about reachability. Stargates
/// only -- no chain, so `via_chain` is always `false` and `path_risk`
/// always `None`; `allow_frigate_holes` has no effect here.
pub struct StaticSovReachability(pub Option<crate::sov_feed::graph::Reachability>);

impl SovReachabilitySource for StaticSovReachability {
    fn reachable(
        &self,
        solar_system_id: i64,
        _allow_frigate_holes: bool,
    ) -> Option<SovReachabilityInfo> {
        let reachability = self.0.as_ref()?;
        let jumps = reachability.jumps(solar_system_id)?;
        let route = reachability.path(solar_system_id)?;
        Some(SovReachabilityInfo {
            jumps,
            route,
            via_chain: false,
            path_risk: None,
        })
    }

    fn graph_available(&self) -> bool {
        self.0.is_some()
    }
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
    /// Freshly prepared `reachable:<seq>` deliveries this cycle (ticket
    /// 06): an observed false->true transition for a subscription's
    /// `Reachable` leaf whose full filter also matched at the moment of
    /// the transition.
    pub reachable_prepared: usize,
    /// Freshly prepared `tz_window_entered:<seq>` deliveries this cycle
    /// (ticket 07): an observed not-in-window-to-in-window transition for
    /// a `tz_shift_enabled` subscription whose Defender/Region/System
    /// leaves also matched at the moment of the transition.
    pub tz_window_prepared: usize,
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

/// What one `collect_structures_cycle()` did, for tests and logging.
/// Mirrors [`SovCollectionReport`], scoped to Sovereignty Hubs.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SovStructuresCollectionReport {
    pub paused_until: Option<DateTime<Utc>>,
    pub not_yet_expired: bool,
    pub not_modified: bool,
    /// A successful `200` whose body was an empty listing. Treated as a
    /// no-op (fix round finding 4): a legitimately empty ESI sovereignty
    /// structures listing does not exist, so an empty body is almost
    /// certainly a transient upstream glitch and must not remove every
    /// hub (which would cascade `sov_tz_window_state`).
    pub empty_listing: bool,
    pub baseline_established_this_cycle: bool,
    pub hubs_observed: usize,
    pub removed: usize,
}

impl SovStructuresCollectionReport {
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

    fn empty_listing() -> Self {
        Self {
            empty_listing: true,
            ..Default::default()
        }
    }
}

/// What one `collect_map_cycle()` did, for tests and logging.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SovMapCollectionReport {
    pub paused_until: Option<DateTime<Utc>>,
    pub not_yet_expired: bool,
    pub not_modified: bool,
    /// A successful `200` whose body was an empty listing. Treated as a
    /// no-op (fix round finding 4): a legitimately empty ESI sovereignty
    /// map does not exist, so an empty body must not `replace_map` every
    /// current owner away.
    pub empty_listing: bool,
    pub entries_observed: usize,
}

impl SovMapCollectionReport {
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

    fn empty_listing() -> Self {
        Self {
            empty_listing: true,
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
    reachability: Arc<dyn SovReachabilitySource>,
    clock: Arc<dyn SovClock>,
    watchlist: Arc<dyn SovWatchlistSource>,
}

impl SovCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: SovStore,
        esi: Arc<dyn crate::sov_feed::esi::SovereigntyEsi>,
        limiter: Arc<dyn EsiLimiterStore>,
        delivery: Arc<dyn SovDelivery>,
        directory: Arc<dyn SovSystemDirectory>,
        tickers: Arc<dyn SovTickerResolver>,
        reachability: Arc<dyn SovReachabilitySource>,
    ) -> Self {
        Self {
            store,
            esi,
            limiter,
            delivery,
            directory,
            tickers,
            reachability,
            clock: Arc::new(SystemSovClock),
            watchlist: Arc::new(NoSovWatchlist),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn SovClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Injects the watchlist source used to resolve
    /// `Defender { watchlist: true }` leaves against a guild's current
    /// watchlist (ticket 08). Defaults to [`NoSovWatchlist`]; the collection
    /// and stage loops (`src/lib.rs`) wire the real
    /// `watchlist_feed`-backed source.
    pub fn with_watchlist_source(mut self, watchlist: Arc<dyn SovWatchlistSource>) -> Self {
        self.watchlist = watchlist;
        self
    }

    /// Fetches every subscription and replaces each watchlist-backed
    /// `Defender` leaf with its guild's currently-watched alliance ids
    /// unioned in (ticket 08). Watched ids are loaded once per distinct
    /// guild that actually references the watchlist, so a filter with no
    /// watchlist leaf pays nothing. After this every `Defender` leaf matches
    /// purely on `alliance_ids`, so all downstream `matches`/
    /// `matches_structure` calls need no watchlist parameter.
    async fn subscriptions_with_watchlist_resolved(
        &self,
    ) -> Result<Vec<crate::sov_feed::model::SovSubscription>, SovCollectionError> {
        let mut subscriptions = self.store.subscriptions().await.map_err(store_error)?;
        let mut watched_by_guild: HashMap<u64, Vec<i64>> = HashMap::new();
        for subscription in subscriptions.iter() {
            if subscription.filter.root.references_watchlist()
                && !watched_by_guild.contains_key(&subscription.guild_id)
            {
                let ids = self
                    .watchlist
                    .watched_alliance_ids(subscription.guild_id)
                    .await
                    .map_err(store_error)?;
                watched_by_guild.insert(subscription.guild_id, ids);
            }
        }
        for subscription in subscriptions.iter_mut() {
            if subscription.filter.root.references_watchlist() {
                let ids = watched_by_guild
                    .get(&subscription.guild_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                subscription.filter.root = subscription.filter.root.with_watchlist_resolved(ids);
            }
        }
        Ok(subscriptions)
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
        // Best-effort retention prune (ticket 16), mirroring where the
        // watchlist collector prunes: a failure here must never fail the
        // collection cycle.
        self.prune_retention(observed_at).await;
        Ok(report)
    }

    /// Runs one [`SovStore::prune_expired_sov_data`] pass over the fixed
    /// [`SOV_RETENTION`] window and logs a single summary line when anything
    /// was removed (ticket 16). Best-effort: a prune failure is logged and
    /// swallowed so it can never abort the collection cycle.
    async fn prune_retention(&self, observed_at: DateTime<Utc>) {
        match self
            .store
            .prune_expired_sov_data(observed_at, SOV_RETENTION)
            .await
        {
            Ok(counts) => {
                if !counts.is_empty() {
                    info!(
                        "sov retention prune: {} sent deliveries, {} campaigns, {} reachability-state rows removed",
                        counts.deliveries_deleted,
                        counts.campaigns_deleted,
                        counts.reachability_state_deleted
                    );
                }
            }
            Err(error) => warn!("sov retention prune failed: {error}"),
        }
    }

    /// Polls `GET /sovereignty/structures/` at its own `max-age=300`
    /// conditional cadence (ticket 07, spec "Sources and cadence") and
    /// persists Sovereignty Hubs only (spec "sov hubs only" --
    /// [`SOV_HUB_STRUCTURE_TYPE_IDS`] filters the raw listing before
    /// anything reaches the store, so a legacy TCU or any other structure
    /// type is never inserted at all). Sov Baseline and hub removal mirror
    /// `diff_and_alert`'s campaign handling, minus any alerting -- this
    /// stage produces no Alert Stage of its own; `evaluate_stage_cycle`
    /// reads the persisted hubs for `tz_window_entered`. Prepares no
    /// delivery, so this never calls `deliver_claimable`.
    pub async fn collect_structures_cycle(
        &self,
    ) -> Result<SovStructuresCollectionReport, SovCollectionError> {
        let observed_at = self.clock.now();

        if let Some(deadline) = self
            .limiter
            .active_esi_limiter_deadline()
            .await
            .map_err(store_error)?
        {
            return Ok(SovStructuresCollectionReport::paused(deadline));
        }

        let cached = self
            .store
            .cache_metadata(SOV_STRUCTURES_RESOURCE_KEY)
            .await
            .map_err(store_error)?;
        if cached.is_fresh() {
            return Ok(SovStructuresCollectionReport::not_yet_expired());
        }

        let response = match self.esi.fetch_structures(cached.etag.clone()).await {
            Ok(response) => response,
            Err(error) => {
                let merged = merge_cache_metadata(&cached, error.metadata.clone());
                self.store
                    .persist_cache_metadata(SOV_STRUCTURES_RESOURCE_KEY, &merged)
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
            .persist_cache_metadata(SOV_STRUCTURES_RESOURCE_KEY, &merged)
            .await
            .map_err(store_error)?;
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;

        if response.not_modified {
            return Ok(SovStructuresCollectionReport::not_modified());
        }
        let listing = response.value.unwrap_or_default();
        if listing.is_empty() {
            // A legitimately empty sovereignty structures listing does not
            // exist (fix round finding 4): removing every hub here would
            // cascade `sov_tz_window_state`. Keep the persisted cache
            // metadata (already written above) and leave hubs untouched.
            warn!(
                "sov structures listing returned an empty 200 body; \
                 treating as a no-op to avoid removing every hub"
            );
            return Ok(SovStructuresCollectionReport::empty_listing());
        }
        let hubs: Vec<SovStructure> = listing
            .into_iter()
            .filter(|structure| SOV_HUB_STRUCTURE_TYPE_IDS.contains(&structure.structure_type_id))
            .collect();

        let baseline_established = self
            .store
            .structures_baseline_established()
            .await
            .map_err(store_error)?;
        let existing_ids = self
            .store
            .known_structure_ids()
            .await
            .map_err(store_error)?;
        for hub in &hubs {
            self.store
                .upsert_structure(hub, !baseline_established, observed_at)
                .await
                .map_err(store_error)?;
        }
        let current_ids: std::collections::HashSet<i64> =
            hubs.iter().map(|hub| hub.structure_id).collect();
        let removed_ids: Vec<i64> = existing_ids.difference(&current_ids).copied().collect();
        self.store
            .remove_structures(&removed_ids)
            .await
            .map_err(store_error)?;
        if !baseline_established {
            self.store
                .establish_structures_baseline(observed_at)
                .await
                .map_err(store_error)?;
        }

        Ok(SovStructuresCollectionReport {
            baseline_established_this_cycle: !baseline_established,
            hubs_observed: hubs.len(),
            removed: removed_ids.len(),
            ..Default::default()
        })
    }

    /// Polls `GET /sovereignty/map/` at its own `max-age=3600` conditional
    /// cadence (ticket 07, spec "Sources and cadence") and replaces
    /// `sov_map` wholesale (`SovStore::replace_map`) -- only "current
    /// owner" is needed, never map history.
    pub async fn collect_map_cycle(&self) -> Result<SovMapCollectionReport, SovCollectionError> {
        let observed_at = self.clock.now();

        if let Some(deadline) = self
            .limiter
            .active_esi_limiter_deadline()
            .await
            .map_err(store_error)?
        {
            return Ok(SovMapCollectionReport::paused(deadline));
        }

        let cached = self
            .store
            .cache_metadata(SOV_MAP_RESOURCE_KEY)
            .await
            .map_err(store_error)?;
        if cached.is_fresh() {
            return Ok(SovMapCollectionReport::not_yet_expired());
        }

        let response = match self.esi.fetch_map(cached.etag.clone()).await {
            Ok(response) => response,
            Err(error) => {
                let merged = merge_cache_metadata(&cached, error.metadata.clone());
                self.store
                    .persist_cache_metadata(SOV_MAP_RESOURCE_KEY, &merged)
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
            .persist_cache_metadata(SOV_MAP_RESOURCE_KEY, &merged)
            .await
            .map_err(store_error)?;
        self.limiter
            .record_esi_limiter_at(&response.metadata, observed_at)
            .await
            .map_err(store_error)?;

        if response.not_modified {
            return Ok(SovMapCollectionReport::not_modified());
        }
        let entries = response.value.unwrap_or_default();
        if entries.is_empty() {
            // A legitimately empty sovereignty map does not exist (fix
            // round finding 4): `replace_map` on an empty listing would
            // wipe every current owner. Keep the persisted cache metadata
            // (already written above) and leave owners untouched.
            warn!(
                "sov map listing returned an empty 200 body; \
                 treating as a no-op to avoid wiping every current owner"
            );
            return Ok(SovMapCollectionReport::empty_listing());
        }
        self.store
            .replace_map(&entries, observed_at)
            .await
            .map_err(store_error)?;

        Ok(SovMapCollectionReport {
            entries_observed: entries.len(),
            ..Default::default()
        })
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
        let mut reachable_prepared = 0usize;
        let mut tz_window_prepared = 0usize;
        // Fetched once, unconditionally: both the campaign-shaped passes
        // below (gated on `!campaigns.is_empty()`) and the `tz_window_entered`
        // pass (gated on `!structures.is_empty()`) need the full
        // subscription list, and it is small (one row per channel
        // subscription).
        let subscriptions = self.subscriptions_with_watchlist_resolved().await?;
        let region_of =
            |system_id: i64| self.directory.resolve(system_id).map(|info| info.region_id);
        if !campaigns.is_empty() {
            let reachable_jumps = |system_id: i64, allow_frigate_holes: bool| {
                self.reachability
                    .reachable(system_id, allow_frigate_holes)
                    .map(|info| info.jumps)
            };
            // Fix round finding 1: when the reachability source itself is
            // unavailable (missing/malformed stargate file, ticket 04's
            // degrade path), `reachable_jumps` above returns `None` for
            // every system -- indistinguishable, at that closure's level,
            // from "loaded graph, genuinely unreachable". Recording that
            // as an observed `false` would be wrong: if the graph then
            // comes back, every campaign that was actually reachable all
            // along would look like a fresh false->true transition and
            // the whole feed would burst-fire "now reachable" for
            // everything at once. Checked once per cycle (a property of
            // the reachability source as a whole, not per system) and used
            // to skip the entire reachability pass below when true: no
            // observation is recorded at all, so state -- including
            // whether a campaign first seen during the outage has ever
            // gotten its baseline row -- simply waits for the graph to
            // come back.
            //
            // `reachability_ready()` is the second half of the same guard
            // (ticket 05 blocker / ticket 06 readiness): a dynamic Wanderer
            // chain reports `graph_available() == true` (its stargate graph
            // is always loaded) but is not yet a faithful picture of the live
            // chain until it has been fetched or seeded once this process.
            // Until then it answers stargate-only, so recording observations
            // would manufacture false->true transitions for chain-only
            // campaigns after a restart. Static/immediately-available sources
            // default `reachability_ready()` to true and are unaffected.
            let record_reachability =
                self.reachability.graph_available() && self.reachability.reachability_ready();
            for campaign in &campaigns {
                // T-minus marks: "start_time > now" (spec) -- a campaign
                // that has already started never gets a T-minus mark. This
                // gate is specific to T-minus; reachability transitions
                // below run for every open campaign regardless of
                // start_time (ticket 06 sets no such restriction -- a
                // chain-opens-a-door moment matters for as long as the
                // campaign stays open).
                if campaign.start_time > observed_at {
                    let remaining = campaign.start_time - observed_at;
                    for subscription in &subscriptions {
                        if !subscription.filter.root.matches(
                            campaign,
                            observed_at,
                            &region_of,
                            &reachable_jumps,
                        ) {
                            continue;
                        }
                        // All due marks fire in this same pass (e.g. after
                        // downtime skipped an earlier evaluation), each
                        // once via `prepare_delivery`'s unique-constraint
                        // dedup.
                        for minutes in effective_tminus_marks_minutes(&subscription.options) {
                            // Non-panicking construction (review finding
                            // 1b): `/sov_subscribe` already rejects marks
                            // above `SOV_TMINUS_MARK_MAX_MINUTES`, but a
                            // value stored some other way (direct
                            // `options` write, a future migration) must
                            // never reach the panicking
                            // `ChronoDuration::minutes`. Treat an
                            // out-of-range mark as permanently not-due
                            // rather than always-due.
                            let Some(mark_duration) = ChronoDuration::try_minutes(minutes) else {
                                continue;
                            };
                            if remaining > mark_duration {
                                continue;
                            }
                            let stage = SovAlertStage::TMinus(minutes);
                            // Skip the expensive render (directory/ticker/
                            // owner lookups + a `map_owner` query) when a
                            // delivery for this mark already exists: the mark
                            // stays "due" for its whole T-120..start window,
                            // so every 30 s pass would otherwise re-render and
                            // throw the work away once `prepare_delivery`
                            // no-ops (tickets 03/07). `prepare_delivery`'s
                            // ON CONFLICT still guards correctness on the rare
                            // race between this check and the insert.
                            if self
                                .store
                                .delivery_exists(
                                    subscription,
                                    "campaign",
                                    campaign.campaign_id,
                                    stage,
                                )
                                .await
                                .map_err(store_error)?
                            {
                                continue;
                            }
                            let message = self
                                .render_stage_message(
                                    campaign,
                                    stage,
                                    subscription.filter.root.allows_frigate_holes_anywhere(),
                                )
                                .await;
                            let freshly_prepared = self
                                .store
                                .prepare_delivery(
                                    subscription,
                                    "campaign",
                                    campaign.campaign_id,
                                    stage,
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

                // Reachability transitions (ticket 06): state is tracked
                // per (subscription, campaign) for every subscription
                // whose filter contains a `Reachable` leaf, following that
                // leaf's own boolean alone -- independent of whether the
                // subscription's other leaves (Region, Defender, ...)
                // match this campaign. Only firing the `reachable` alert
                // itself additionally requires the *full* filter to match
                // (recommended in the ticket: otherwise a campaign in the
                // wrong region would announce "now reachable") -- and that
                // check runs on *every* pass while an announcement is
                // pending, not only the pass that produced the transition
                // (fix round finding 2), so `And(Reachable,
                // VulnerableWithin(12h))` still fires once the campaign
                // enters the window, even many passes after the leaf
                // itself flipped true.
                if record_reachability {
                    for subscription in &subscriptions {
                        if !subscription.filter.root.has_reachable_leaf() {
                            continue;
                        }
                        let reachable_now = subscription
                            .filter
                            .root
                            .reachable_leaf_state(campaign, &reachable_jumps)
                            .unwrap_or(false);
                        let transition = self
                            .store
                            .record_reachability_observation(
                                subscription.guild_id,
                                subscription.channel_id,
                                &subscription.name,
                                campaign.campaign_id,
                                reachable_now,
                                observed_at,
                            )
                            .await
                            .map_err(store_error)?;
                        let SovReachabilityTransition::Observed {
                            reachable,
                            transition_sequence,
                            pending_announcement,
                        } = transition
                        else {
                            // Baseline: never fires, regardless of value.
                            continue;
                        };
                        if !reachable || !pending_announcement {
                            continue;
                        }
                        if !subscription.filter.root.matches(
                            campaign,
                            observed_at,
                            &region_of,
                            &reachable_jumps,
                        ) {
                            // Still pending: the leaf flipped true but the
                            // full filter does not match yet. Try again
                            // next pass rather than losing the
                            // announcement.
                            continue;
                        }
                        let stage = SovAlertStage::Reachable(transition_sequence);
                        // Skip the render when a delivery for this exact
                        // transition already exists (e.g. a prior process
                        // prepared it and crashed before marking it announced,
                        // so this pass re-attempts): the row is durable, so
                        // just re-mark it announced below without paying for
                        // another render (tickets 03/07).
                        let freshly_prepared = if self
                            .store
                            .delivery_exists(subscription, "campaign", campaign.campaign_id, stage)
                            .await
                            .map_err(store_error)?
                        {
                            false
                        } else {
                            let message = self
                                .render_stage_message(
                                    campaign,
                                    stage,
                                    subscription.filter.root.allows_frigate_holes_anywhere(),
                                )
                                .await;
                            self.store
                                .prepare_delivery(
                                    subscription,
                                    "campaign",
                                    campaign.campaign_id,
                                    stage,
                                    &message,
                                )
                                .await
                                .map_err(store_error)?
                        };
                        // Marked announced once a durable delivery row is
                        // confirmed to exist, whether freshly prepared
                        // here or already prepared by an earlier attempt
                        // (restart-safe: the delivery's own lease/dedup
                        // path owns exactly-once send from here).
                        self.store
                            .mark_reachability_announced(
                                subscription.guild_id,
                                subscription.channel_id,
                                &subscription.name,
                                campaign.campaign_id,
                                transition_sequence,
                            )
                            .await
                            .map_err(store_error)?;
                        if freshly_prepared {
                            reachable_prepared += 1;
                        }
                    }
                }
            }
        }

        // `tz_window_entered` (ticket 07): state is tracked per
        // (subscription, structure) for every `tz_shift_enabled`
        // subscription, following the overlap fact alone -- independent of
        // whether the subscription's Defender/Region/System leaves also
        // match this hub (mirrors the `Reachable` leaf's "track the leaf,
        // gate the alert on the full filter" split immediately above).
        // Only firing the alert itself additionally requires those leaves
        // to match (`matches_structure`, which ignores `Reachable`/
        // `VulnerableWithin`/`EventType`), re-checked on every pass while
        // an announcement is pending, exactly like the reachability pass.
        // Gate the whole structures pass on at least one subscription
        // opting in (fix round finding 3): with ~2700 hubs and a
        // FOR UPDATE observation transaction per (hub x tz-enabled
        // subscription), reading `open_structures()` every 30 s stage
        // cycle is pure waste when nobody enabled `tz_shift_enabled`. The
        // per-subscription guard inside the loop still applies so a mix of
        // enabled and disabled subscriptions only observes for the enabled
        // ones.
        let any_tz_enabled = subscriptions
            .iter()
            .any(|subscription| effective_tz_shift_enabled(&subscription.options));
        let structures = if any_tz_enabled {
            self.store.open_structures().await.map_err(store_error)?
        } else {
            Vec::new()
        };
        if !structures.is_empty() {
            for structure in &structures {
                for subscription in &subscriptions {
                    if !effective_tz_shift_enabled(&subscription.options) {
                        continue;
                    }
                    let Some(window) = effective_tz_window(&subscription.options) else {
                        // Defensive: a malformed stored `tz_window` (see
                        // `effective_tz_window`'s doc comment) never
                        // participates rather than panicking or guessing a
                        // default.
                        continue;
                    };
                    let in_window_now = match (
                        structure.vulnerable_start_time,
                        structure.vulnerable_end_time,
                    ) {
                        (Some(start), Some(end)) => {
                            tz_window_overlap_minutes(start, end, window)
                                >= SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES
                        }
                        // Spec: "A hub whose vulnerability fields are
                        // null/absent is not-in-window".
                        _ => false,
                    };
                    let transition = self
                        .store
                        .record_tz_window_observation(
                            subscription.guild_id,
                            subscription.channel_id,
                            &subscription.name,
                            structure.structure_id,
                            in_window_now,
                            observed_at,
                        )
                        .await
                        .map_err(store_error)?;
                    let SovTzWindowTransition::Observed {
                        in_window,
                        transition_sequence,
                        pending_announcement,
                    } = transition
                    else {
                        // Baseline: never fires (Sov Baseline), regardless
                        // of value.
                        continue;
                    };
                    if !in_window || !pending_announcement {
                        continue;
                    }
                    if !subscription.filter.root.matches_structure(
                        structure.alliance_id,
                        structure.solar_system_id,
                        &region_of,
                    ) {
                        // Still pending: the window overlap flipped true
                        // but Defender/Region/System does not match yet.
                        // Try again next pass rather than losing the
                        // announcement.
                        continue;
                    }
                    let stage = SovAlertStage::TzWindowEntered(transition_sequence);
                    // Skip the render when a delivery for this exact
                    // transition already exists (restart-replay): the row is
                    // durable, so just re-mark it announced below without
                    // paying for another render (tickets 03/07).
                    let freshly_prepared = if self
                        .store
                        .delivery_exists(subscription, "structure", structure.structure_id, stage)
                        .await
                        .map_err(store_error)?
                    {
                        false
                    } else {
                        let message = self
                            .render_tz_window_message(structure, window, stage)
                            .await;
                        self.store
                            .prepare_delivery(
                                subscription,
                                "structure",
                                structure.structure_id,
                                stage,
                                &message,
                            )
                            .await
                            .map_err(store_error)?
                    };
                    self.store
                        .mark_tz_window_announced(
                            subscription.guild_id,
                            subscription.channel_id,
                            &subscription.name,
                            structure.structure_id,
                            transition_sequence,
                        )
                        .await
                        .map_err(store_error)?;
                    if freshly_prepared {
                        tz_window_prepared += 1;
                    }
                }
            }
        }

        self.deliver_claimable(observed_at).await?;
        Ok(SovStageEvaluationReport {
            campaigns_evaluated: campaigns.len(),
            tminus_prepared,
            reachable_prepared,
            tz_window_prepared,
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
            let subscriptions = self.subscriptions_with_watchlist_resolved().await?;
            let region_of =
                |system_id: i64| self.directory.resolve(system_id).map(|info| info.region_id);
            let reachable_jumps = |system_id: i64, allow_frigate_holes: bool| {
                self.reachability
                    .reachable(system_id, allow_frigate_holes)
                    .map(|info| info.jumps)
            };
            for campaign in &alert_candidates {
                for subscription in &subscriptions {
                    if subscription.filter.root.matches(
                        campaign,
                        observed_at,
                        &region_of,
                        &reachable_jumps,
                    ) {
                        // Skip the render once the `appeared` delivery exists:
                        // the campaign re-matches on every 60 s poll for its
                        // whole twelve-hour window, and re-rendering just to
                        // have `prepare_delivery` no-op is wasted work
                        // (tickets 03/07). ON CONFLICT still guards the rare
                        // race between this check and the insert.
                        if self
                            .store
                            .delivery_exists(
                                subscription,
                                "campaign",
                                campaign.campaign_id,
                                SovAlertStage::Appeared,
                            )
                            .await
                            .map_err(store_error)?
                        {
                            continue;
                        }
                        let message = self
                            .render_stage_message(
                                campaign,
                                SovAlertStage::Appeared,
                                subscription.filter.root.allows_frigate_holes_anywhere(),
                            )
                            .await;
                        let freshly_prepared = self
                            .store
                            .prepare_delivery(
                                subscription,
                                "campaign",
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
        allow_frigate_holes: bool,
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

        // Only present when the system is reachable (spec "Embed": "jumps
        // and route summary ... omitted for gate-only routes" -- and, by
        // the same "presentation, not a filter" spirit, omitted entirely
        // rather than shown as a placeholder for an unreachable system or
        // an unavailable graph, ticket 04). `allow_frigate_holes` picks
        // which of the subscription's two cached routes to show (ticket
        // 05); see `SovFilterNode::allows_frigate_holes_anywhere`.
        if let Some(info) = self
            .reachability
            .reachable(campaign.solar_system_id, allow_frigate_holes)
        {
            fields.push(SovEmbedField {
                name: "Reachability".to_string(),
                value: format!(
                    "{} jump{} from home\n{}",
                    info.jumps,
                    if info.jumps == 1 { "" } else { "s" },
                    self.route_summary(&info.route)
                ),
                inline: false,
            });
            // Path Risk (ticket 05, spec "Embed"): one line, only when the
            // chosen route crosses at least one wormhole -- `path_risk` is
            // `None` by construction for a gate-only route (see
            // `SovReachabilityInfo`'s doc comment), so this needs no
            // separate "is it gate-only" check.
            if let Some(risk) = &info.path_risk {
                fields.push(SovEmbedField {
                    name: "Path Risk".to_string(),
                    value: format!("worst hole: {}", risk.describe()),
                    inline: true,
                });
            }
        }

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

        // Current system owner from the sovereignty map (ticket 07, spec
        // "Embed": "current owner from the sovereignty map"), applied to
        // every stage from this ticket on -- omitted entirely when the map
        // has not loaded this system yet, rather than shown as a
        // placeholder (mirrors the Reachability field's "omit, don't
        // placeholder" convention above).
        if let Some(owner) = self.owner_field_value(campaign.solar_system_id).await {
            fields.push(SovEmbedField {
                name: "System Owner".to_string(),
                value: owner,
                inline: true,
            });
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

    /// Renders one Sovereignty Hub's `tz_window_entered` notification
    /// (ticket 07, spec "Embed"): the hub's system and region, its own
    /// owner alliance ticker in the title (mirrors the campaign embed's
    /// defender ticker), the vulnerability window as EVE times with a
    /// Discord relative timestamp for the start, the current system owner
    /// from the sovereignty map, and zKillboard/Dotlan links. Footer:
    /// `"vuln window entered <window>"` (the subscription's own window,
    /// not the hub's vulnerability window).
    async fn render_tz_window_message(
        &self,
        structure: &SovStructure,
        window: crate::sov_feed::model::TzWindow,
        stage: SovAlertStage,
    ) -> SovNotificationMessage {
        let system_info = self.directory.resolve(structure.solar_system_id);
        let (system_name, region_name) = system_info
            .map(|info| (info.name, info.region_name))
            .unwrap_or_else(|| {
                (
                    structure.solar_system_id.to_string(),
                    "Unknown Region".to_string(),
                )
            });
        let hub_owner_ticker = match structure.alliance_id {
            Some(alliance_id) => self
                .tickers
                .alliance_ticker(alliance_id)
                .await
                .unwrap_or_else(|| "Unknown".to_string()),
            None => "Unknown".to_string(),
        };
        let title = format!(
            "{system_name} ({region_name}) \u{2014} Vulnerability Window Shift \u{2014} {hub_owner_ticker}"
        );

        let mut fields = Vec::new();
        if let (Some(start), Some(end)) = (
            structure.vulnerable_start_time,
            structure.vulnerable_end_time,
        ) {
            let unix = start.timestamp();
            fields.push(SovEmbedField {
                name: "Vulnerability Window".to_string(),
                value: format!(
                    "{} \u{2013} {} EVE\n<t:{unix}:R>",
                    start.format("%Y-%m-%d %H:%M:%S"),
                    end.format("%H:%M:%S")
                ),
                inline: false,
            });
        }

        if let Some(owner) = self.owner_field_value(structure.solar_system_id).await {
            fields.push(SovEmbedField {
                name: "System Owner".to_string(),
                value: owner,
                inline: true,
            });
        }

        fields.push(SovEmbedField {
            name: "Links".to_string(),
            value: format!(
                "[zKillboard](https://zkillboard.com/system/{}/) | [Dotlan](https://evemaps.dotlan.net/system/{})",
                structure.solar_system_id, structure.solar_system_id
            ),
            inline: true,
        });

        SovNotificationMessage {
            title,
            fields,
            footer: format!("{} {}", stage.footer_label(), window.display()),
        }
    }

    /// Resolves one system's current sovereignty owner into display text
    /// (spec "Embed": "current owner from the sovereignty map"; task:
    /// "alliance ticker/name via the existing ticker resolver; corporation
    /// or faction fallback"). `None` when the map has no row for this
    /// system yet (not fetched, or the system carries no sovereignty) --
    /// callers omit the field entirely rather than showing a placeholder.
    /// A resolvable ID whose ticker/name lookup itself comes back empty
    /// (ESI miss) still shows the numeric ID rather than "Unknown", since
    /// unlike a defender ticker (which the campaign/hub embed already
    /// shows elsewhere), this is the *only* place the map's owner fact
    /// appears at all.
    async fn owner_field_value(&self, solar_system_id: i64) -> Option<String> {
        let entry = match self.store.map_owner(solar_system_id).await {
            Ok(entry) => entry?,
            Err(error) => {
                warn!("cannot resolve sov map owner for system {solar_system_id}: {error}");
                return None;
            }
        };
        if let Some(alliance_id) = entry.alliance_id {
            let ticker = self.tickers.alliance_ticker(alliance_id).await;
            return Some(ticker.unwrap_or_else(|| format!("Alliance {alliance_id}")));
        }
        if let Some(corporation_id) = entry.corporation_id {
            let ticker = self.tickers.corporation_ticker(corporation_id).await;
            return Some(ticker.unwrap_or_else(|| format!("Corporation {corporation_id}")));
        }
        if let Some(faction_id) = entry.faction_id {
            let name = self.tickers.faction_name(faction_id).await;
            return Some(name.unwrap_or_else(|| format!("Faction {faction_id}")));
        }
        None
    }

    /// Renders a route (home-to-target system IDs, in travel order) as a
    /// display string, resolving each ID to its system name and truncating
    /// to eight systems with `…` (spec "Embed": "route summary truncated
    /// to eight systems"). A system this directory cannot resolve falls
    /// back to its raw ID rather than dropping it from the route.
    const SOV_ROUTE_SUMMARY_MAX_SYSTEMS: usize = 8;

    fn route_summary(&self, route: &[i64]) -> String {
        let names: Vec<String> = route
            .iter()
            .map(|system_id| {
                self.directory
                    .resolve(*system_id)
                    .map(|info| info.name)
                    .unwrap_or_else(|| system_id.to_string())
            })
            .collect();
        if names.len() > Self::SOV_ROUTE_SUMMARY_MAX_SYSTEMS {
            format!(
                "{} \u{2192} \u{2026}",
                names[..Self::SOV_ROUTE_SUMMARY_MAX_SYSTEMS].join(" \u{2192} ")
            )
        } else {
            names.join(" \u{2192} ")
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
