//! Chain Snapshot persistence shape, traversability rules, and the dynamic
//! (Wanderer-chain-aware) [`crate::sov_feed::SovReachabilitySource`]
//! implementation (spec `.scratch/esi-intel-feeds/spec.md`, "Reachability";
//! ticket `.scratch/esi-intel-feeds/issues/05-wanderer-chain-reachability-and-path-risk.md`).
//!
//! Nothing in this module touches the network or a database: [`ChainSnapshot`]
//! is a pure value persisted by `src/sov_feed/store.rs`, and
//! [`DynamicChainSovReachability`] is pure CPU/memory (the same BFS engine
//! `src/sov_feed/graph.rs` already uses for stargates alone). The poll
//! loop that owns the network/database IO lives in `src/lib.rs`, mirroring
//! every other sov feed loop.

use crate::sov_feed::collector::{SovClock, SystemSovClock};
use crate::sov_feed::graph::{Reachability, StargateGraph};
use crate::sov_feed::store::SovStore;
use crate::sov_feed::wanderer::{
    WandererChainSource, WandererConnection, WandererConnectionType, WandererError,
    WandererMassStatus, WandererShipSizeType, WandererSystem, WandererTimeStatus,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tracing::warn;

/// A snapshot goes stale ten minutes after its `fetched_at` with no
/// successful re-fetch (spec: "the last good snapshot is kept and marked
/// stale after ten minutes"). Shared with the health check in
/// `src/contract_intelligence.rs` (`sov_chain_progress`) so both surfaces
/// agree on the exact same bound.
pub const SOV_CHAIN_STALE_AFTER: ChronoDuration = ChronoDuration::minutes(10);

/// One observation of the Wanderer map's systems and connections, as
/// persisted in `sov_chain_snapshots` (spec: "Chain Snapshot: one
/// observation of the Wanderer map's systems and connections").
#[derive(Clone, Debug, PartialEq)]
pub struct ChainSnapshot {
    pub systems: Vec<WandererSystem>,
    pub connections: Vec<WandererConnection>,
    pub fetched_at: DateTime<Utc>,
}

/// Normalizes an undirected edge's endpoints into a stable `(min, max)`
/// key so `(a, b)` and `(b, a)` compare equal everywhere this module
/// tracks edges by system-ID pair.
fn normalize_edge(a: i64, b: i64) -> (i64, i64) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// One connection's full traversability-relevant signature: normalized
/// endpoints plus every field that traversability or Path Risk depends on
/// (`connection_type`, `mass_status`, `time_status`, `ship_size_type`).
/// Two connections with the same endpoints but different attributes
/// compare unequal, so [`connection_topology_changed`] treats e.g. a hole
/// ticking to critical mass, or into a worse EOL bucket, as a genuine
/// diff -- not only an edge being added or removed.
type EdgeSignature = (
    i64,
    i64,
    WandererConnectionType,
    WandererMassStatus,
    WandererTimeStatus,
    WandererShipSizeType,
);

fn edge_signature(connection: &WandererConnection) -> EdgeSignature {
    let (source, target) = normalize_edge(
        connection.solar_system_source,
        connection.solar_system_target,
    );
    (
        source,
        target,
        connection.connection_type,
        connection.mass_status,
        connection.time_status,
        connection.ship_size_type,
    )
}

/// True when the traversability-relevant facts differ between two
/// connection lists: an edge was added or removed, *or* an edge present
/// in both changed `connection_type`, `mass_status`, `time_status`, or
/// `ship_size_type` (spec: "consecutive snapshots are diffed; a
/// non-empty diff invalidates cached reachability"). Reviewer finding on
/// this ticket: an earlier version diffed by `(source, target)` alone, so
/// a hole ticking to critical mass or into a worse EOL bucket never
/// triggered a recompute until some unrelated edge changed -- since a
/// recompute is a cheap bounded BFS (`build_chain_graph_variant`, a few
/// thousand systems), there is no reason to under-diff here.
pub fn connection_topology_changed(
    previous: &[WandererConnection],
    current: &[WandererConnection],
) -> bool {
    let previous_edges: HashSet<EdgeSignature> = previous.iter().map(edge_signature).collect();
    let current_edges: HashSet<EdgeSignature> = current.iter().map(edge_signature).collect();
    previous_edges != current_edges
}

/// The worst time-status and mass-status among the wormhole edges on one
/// chosen route (spec "Path Risk": "the worst wormhole state on a route").
/// Presentation only, never a filter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PathRisk {
    pub worst_time_status: WandererTimeStatus,
    pub worst_mass_status: WandererMassStatus,
}

impl PathRisk {
    /// Renders e.g. `"EOL <4h, mass <50%"` (spec "Embed" example: "worst
    /// hole: EOL <4h, mass <50%" -- this is the part after "worst hole:
    /// ", which the caller prefixes).
    pub fn describe(&self) -> String {
        format!(
            "{}, {}",
            describe_time_status(self.worst_time_status),
            describe_mass_status(self.worst_mass_status)
        )
    }
}

fn describe_time_status(status: WandererTimeStatus) -> String {
    match status {
        WandererTimeStatus::Normal => "not yet EOL".to_string(),
        WandererTimeStatus::Eol1Hour => "EOL <1h".to_string(),
        WandererTimeStatus::Eol4Hours => "EOL <4h".to_string(),
        WandererTimeStatus::Eol4Point5Hours => "EOL <4h30m".to_string(),
        WandererTimeStatus::Eol16Hours => "EOL <16h".to_string(),
        WandererTimeStatus::Eol24Hours => "EOL <24h".to_string(),
        WandererTimeStatus::Eol48Hours => "EOL <48h".to_string(),
        WandererTimeStatus::Unknown(other) => format!("EOL status unknown ({other})"),
    }
}

fn describe_mass_status(status: WandererMassStatus) -> String {
    match status {
        WandererMassStatus::Normal => "mass >50%".to_string(),
        WandererMassStatus::Depleted => "mass <50%".to_string(),
        // Critical-mass connections are never traversable (excluded from
        // every graph variant below), so this arm is unreachable through
        // this module's own construction -- kept for exhaustiveness and
        // in case a future caller constructs `PathRisk` some other way.
        WandererMassStatus::Critical => "mass critical".to_string(),
        WandererMassStatus::Unknown(other) => format!("mass status unknown ({other})"),
    }
}

/// Severity ranking for [`WandererTimeStatus`], most urgent first: an
/// active EOL timer close to collapse ranks worse than one further out,
/// and a genuinely unrecognized future status ranks as the *worst* of all
/// -- the conservative choice when this code cannot know what the new
/// value means (mirrors the "treat as never-due" conservatism used
/// throughout `src/sov_feed/model.rs` for out-of-range persisted values).
fn time_status_severity(status: WandererTimeStatus) -> u8 {
    match status {
        WandererTimeStatus::Unknown(_) => 7,
        WandererTimeStatus::Eol1Hour => 6,
        WandererTimeStatus::Eol4Hours => 5,
        WandererTimeStatus::Eol4Point5Hours => 4,
        WandererTimeStatus::Eol16Hours => 3,
        WandererTimeStatus::Eol24Hours => 2,
        WandererTimeStatus::Eol48Hours => 1,
        WandererTimeStatus::Normal => 0,
    }
}

/// Severity ranking for [`WandererMassStatus`] among *traversable* edges
/// only (`Critical` never reaches this function -- see
/// [`build_chain_graph_variant`]); `Unknown` again ranks worst,
/// conservatively.
fn mass_status_severity(status: WandererMassStatus) -> u8 {
    match status {
        WandererMassStatus::Unknown(_) => 3,
        WandererMassStatus::Critical => 2,
        WandererMassStatus::Depleted => 1,
        WandererMassStatus::Normal => 0,
    }
}

/// A wormhole edge's risk-relevant facts, keyed by its normalized
/// `(source, target)` pair in [`ChainGraphVariant::wormhole_edges`].
#[derive(Clone, Copy, Debug)]
struct WormholeEdgeInfo {
    time_status: WandererTimeStatus,
    mass_status: WandererMassStatus,
}

/// One of the two cached BFS results per chain snapshot (spec: "Two BFS
/// results are cached per chain snapshot (with and without frigate
/// holes)"), plus the edge provenance [`DynamicChainSovReachability::reachable`]
/// needs to compute `via_chain` and `path_risk` for whichever route the
/// BFS returns.
struct ChainGraphVariant {
    reachability: Reachability,
    /// Every traversable *wormhole* edge included in this variant's BFS,
    /// keyed by normalized endpoints -- used to compute Path Risk over
    /// the chosen route.
    wormhole_edges: HashMap<(i64, i64), WormholeEdgeInfo>,
    /// Every traversable Wanderer connection (wormhole, gate, or bridge)
    /// included in this variant's BFS, keyed by normalized endpoints --
    /// used to compute `via_chain` (whether the chosen route used any of
    /// them, as opposed to being pure static stargates).
    chain_edges: HashSet<(i64, i64)>,
}

/// Builds one [`ChainGraphVariant`] from a Wanderer connection list,
/// applying the traversability rules (spec "Traversability rules"):
/// - Wormhole connections traverse unless `mass_status` is `Critical`,
///   and (unless `allow_frigate_holes`) unless `ship_size_type` is
///   `Frigate`.
/// - Gate and bridge connections always traverse as one jump; Wanderer's
///   ship-size scanning signature is a wormhole-only concept, so it does
///   not restrict these.
/// - Every `time_status` traverses (EOL is a Path Risk fact, not a
///   traversability rule).
/// - A connection of a genuinely unrecognized future `type` never
///   traverses: the conservative choice when this code cannot know what
///   travel through it would mean.
///
/// A traversable connection whose endpoints [`StargateGraph::has_edge`]
/// already reports as a real stargate is *not* added as a chain edge at
/// all (reviewer finding on this ticket): the static graph already
/// provides that jump, so adding a parallel chain edge would make
/// `via_chain`/Path Risk depend on which of the two identical-cost edges
/// the BFS happened to record a predecessor through -- an implementation
/// detail, not a fact about the route. This is the map-operator-tracked
/// case in practice (a `Gate`-type Wanderer connection mirroring a real
/// stargate); it is applied to every connection type uniformly rather
/// than special-cased to `Gate`, since the same ambiguity would apply to
/// any type that happened to duplicate a stargate edge.
fn build_chain_graph_variant(
    graph: &StargateGraph,
    home_system_id: i64,
    connections: &[WandererConnection],
    allow_frigate_holes: bool,
) -> ChainGraphVariant {
    let mut extra_edges = Vec::new();
    let mut wormhole_edges = HashMap::new();
    let mut chain_edges = HashSet::new();

    for connection in connections {
        let traversable = match connection.connection_type {
            WandererConnectionType::Wormhole => {
                !connection.mass_status.is_critical()
                    && (allow_frigate_holes || !connection.ship_size_type.is_frigate())
            }
            WandererConnectionType::Gate | WandererConnectionType::Bridge => true,
            WandererConnectionType::Unknown(_) => false,
        };
        if !traversable {
            continue;
        }
        if graph.has_edge(
            connection.solar_system_source,
            connection.solar_system_target,
        ) {
            // Already real k-space connectivity; the static graph
            // provides this jump on its own, so this connection
            // contributes no chain-specific fact (see the doc comment
            // above).
            continue;
        }
        let edge = normalize_edge(
            connection.solar_system_source,
            connection.solar_system_target,
        );
        extra_edges.push((
            connection.solar_system_source,
            connection.solar_system_target,
        ));
        chain_edges.insert(edge);
        if connection.connection_type == WandererConnectionType::Wormhole {
            wormhole_edges.insert(
                edge,
                WormholeEdgeInfo {
                    time_status: connection.time_status,
                    mass_status: connection.mass_status,
                },
            );
        }
    }

    let reachability = Reachability::compute(graph, home_system_id, &extra_edges);
    ChainGraphVariant {
        reachability,
        wormhole_edges,
        chain_edges,
    }
}

struct ChainReachabilityState {
    with_frigate: ChainGraphVariant,
    without_frigate: ChainGraphVariant,
}

/// Whether a Wanderer chain feed is configured and how fresh its evidence
/// is (spec: "stale state is visible in the health snapshot ... and in
/// `/sov_timers`"). Rendered by `/sov_timers` (`src/commands/sov_timers.rs`)
/// and folded into the Runtime Health Snapshot as `sov_chain_progress`
/// (`src/contract_intelligence.rs`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SovChainStatus {
    /// `WANDERER_BASE_URL`/`WANDERER_MAP`/`WANDERER_MAP_API_KEY` are not
    /// all set, or the sov feed itself is disabled: reachability is
    /// stargate-only by construction (`StaticSovReachability`), which
    /// always reports this via its default `chain_status` implementation.
    NotConfigured,
    /// Configured, but no successful fetch has completed yet (e.g. right
    /// after start-up, or Wanderer has been down since before the first
    /// successful poll).
    Pending,
    /// A successful fetch completed within [`SOV_CHAIN_STALE_AFTER`].
    Fresh,
    /// No successful fetch within [`SOV_CHAIN_STALE_AFTER`]; `since` is
    /// the last successful fetch's timestamp. Reachability keeps serving
    /// the last good snapshot regardless (spec: "the last good snapshot
    /// is kept").
    Stale { since: DateTime<Utc> },
}

/// A [`crate::sov_feed::SovReachabilitySource`] over the unified stargate +
/// Wanderer chain graph (spec "Reachability": "Unified undirected graph of
/// stargate edges plus traversable Wanderer connections"). Constructed
/// once at start-up with the static stargate graph (so it behaves exactly
/// like ticket 04's `StaticSovReachability` -- stargate-only -- until the
/// first successful chain fetch); the chain poll loop in `src/lib.rs`
/// calls [`Self::recompute`] whenever [`connection_topology_changed`]
/// reports a non-empty diff (or on the very first successful fetch, which
/// has no previous snapshot to diff against) and [`Self::mark_fetch_success`]
/// on every successful fetch regardless, so staleness tracks "last
/// successful fetch", not "last time the graph actually changed".
///
/// # Locking
///
/// `state` and `last_success_at` are plain `std::sync::RwLock`, not
/// `tokio::sync::RwLock`: every access is a synchronous, in-memory
/// read/BFS-lookup or a synchronous swap, so a blocking read/write lock is
/// held only for the duration of that synchronous work and is provably
/// never held across an `.await` -- there is no `.await` anywhere between
/// acquiring and releasing either lock, in either direction. This also
/// lets [`SovReachabilitySource::reachable`] stay a plain (non-async)
/// trait method, unchanged from `StaticSovReachability`.
pub struct DynamicChainSovReachability {
    stargate_graph: StargateGraph,
    home_system_id: i64,
    state: RwLock<ChainReachabilityState>,
    last_success_at: RwLock<Option<DateTime<Utc>>>,
    /// Whether the in-memory chain variants have been (re)computed from a
    /// real chain observation at least once this process -- either a
    /// successful live fetch or a seed from the persisted snapshot at
    /// start-up. Until this is `true`, `state` holds the stargate-only
    /// bootstrap built by [`Self::new`], which is *not* the live chain: a
    /// process restart during a Wanderer outage would otherwise report the
    /// chain silently absent while claiming to be available, and every
    /// chain-only-reachable campaign would look unreachable and then fire a
    /// spurious "now reachable" once the chain finally recomputed (ticket 05
    /// blocker / ticket 06 readiness). Exposed as [`Self::has_recomputed`]
    /// and surfaced through [`SovReachabilitySource::reachability_ready`].
    recomputed_once: AtomicBool,
}

impl DynamicChainSovReachability {
    pub fn new(stargate_graph: StargateGraph, home_system_id: i64) -> Self {
        let with_frigate = build_chain_graph_variant(&stargate_graph, home_system_id, &[], true);
        let without_frigate =
            build_chain_graph_variant(&stargate_graph, home_system_id, &[], false);
        Self {
            stargate_graph,
            home_system_id,
            state: RwLock::new(ChainReachabilityState {
                with_frigate,
                without_frigate,
            }),
            last_success_at: RwLock::new(None),
            recomputed_once: AtomicBool::new(false),
        }
    }

    /// Whether the chain variants reflect a real chain observation (live
    /// fetch or persisted-snapshot seed) at least once this process. Used
    /// both to force a recompute on the first successful fetch after a
    /// restart (regardless of whether the topology changed against the
    /// persisted snapshot) and as the readiness signal that gates recording
    /// reachability observations at all.
    pub fn has_recomputed(&self) -> bool {
        self.recomputed_once.load(Ordering::Acquire)
    }

    /// Seeds the in-memory chain from the last persisted [`ChainSnapshot`]
    /// before the poll loop starts, so a restart during a Wanderer outage
    /// still serves the last known chain (spec story 18) instead of dropping
    /// to stargate-only. Marks the reachability ready and dates staleness
    /// from the snapshot's own `fetched_at`, so `chain_status` correctly
    /// reports the seeded chain as stale if that snapshot is already old.
    pub fn seed_from_snapshot(
        &self,
        connections: &[WandererConnection],
        fetched_at: DateTime<Utc>,
    ) {
        self.recompute(connections);
        self.mark_fetch_success(fetched_at);
    }

    /// Recomputes both cached BFS results from a freshly-fetched
    /// connection list and atomically swaps them in. Pure CPU/memory work
    /// (the same BFS engine `src/sov_feed/graph.rs` uses for a few
    /// thousand systems); the write lock is held only for the final
    /// assignment.
    pub fn recompute(&self, connections: &[WandererConnection]) {
        let with_frigate =
            build_chain_graph_variant(&self.stargate_graph, self.home_system_id, connections, true);
        let without_frigate = build_chain_graph_variant(
            &self.stargate_graph,
            self.home_system_id,
            connections,
            false,
        );
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = ChainReachabilityState {
            with_frigate,
            without_frigate,
        };
        // Released after the swap: readiness must never be observable before
        // the variants it describes are actually in place.
        self.recomputed_once.store(true, Ordering::Release);
    }

    /// Records a successful fetch's timestamp, independent of whether it
    /// triggered [`Self::recompute`] (spec: staleness tracks "last
    /// successful fetch", not "last topology change").
    pub fn mark_fetch_success(&self, fetched_at: DateTime<Utc>) {
        let mut last_success_at = self
            .last_success_at
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *last_success_at = Some(fetched_at);
    }
}

impl super::SovReachabilitySource for DynamicChainSovReachability {
    fn reachable(
        &self,
        solar_system_id: i64,
        allow_frigate_holes: bool,
    ) -> Option<super::SovReachabilityInfo> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let variant = if allow_frigate_holes {
            &state.with_frigate
        } else {
            &state.without_frigate
        };
        let jumps = variant.reachability.jumps(solar_system_id)?;
        let route = variant.reachability.path(solar_system_id)?;

        let mut via_chain = false;
        let mut worst: Option<(WandererTimeStatus, WandererMassStatus)> = None;
        for pair in route.windows(2) {
            let edge = normalize_edge(pair[0], pair[1]);
            if variant.chain_edges.contains(&edge) {
                via_chain = true;
            }
            if let Some(info) = variant.wormhole_edges.get(&edge) {
                worst = Some(match worst {
                    None => (info.time_status, info.mass_status),
                    Some((worst_time, worst_mass)) => (
                        if time_status_severity(info.time_status) > time_status_severity(worst_time)
                        {
                            info.time_status
                        } else {
                            worst_time
                        },
                        if mass_status_severity(info.mass_status) > mass_status_severity(worst_mass)
                        {
                            info.mass_status
                        } else {
                            worst_mass
                        },
                    ),
                });
            }
        }

        Some(super::SovReachabilityInfo {
            jumps,
            route,
            via_chain,
            path_risk: worst.map(|(worst_time_status, worst_mass_status)| PathRisk {
                worst_time_status,
                worst_mass_status,
            }),
        })
    }

    fn graph_available(&self) -> bool {
        // The static stargate graph backing this type always loaded
        // successfully -- `DynamicChainSovReachability` is only ever
        // constructed after that succeeded (see `src/lib.rs`'s
        // reachability wiring); a load failure falls back to
        // `StaticSovReachability(None)` instead, same as ticket 04.
        true
    }

    fn reachability_ready(&self) -> bool {
        // Distinct from `graph_available`: the stargate graph is always
        // loaded, but until the *chain* has been computed from a real
        // observation once this process (seed or first successful fetch),
        // `reachable()` answers stargate-only and does not reflect the live
        // chain. The collector must not record reachability observations
        // during that window, or a restart across a Wanderer outage would
        // manufacture false->true transitions and burst-fire "now reachable"
        // (ticket 05 blocker / ticket 06 readiness).
        self.has_recomputed()
    }

    fn chain_status(&self, now: DateTime<Utc>) -> SovChainStatus {
        let last_success_at = *self
            .last_success_at
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match last_success_at {
            None => SovChainStatus::Pending,
            Some(since) => {
                let age = now.signed_duration_since(since);
                if age >= SOV_CHAIN_STALE_AFTER {
                    SovChainStatus::Stale { since }
                } else {
                    SovChainStatus::Fresh
                }
            }
        }
    }
}

/// Bounded retention window for `sov_chain_snapshots` (spec: "Retain a
/// bounded history (e.g. latest N or 24 h) -- decide and document"; see
/// the migration's doc comment for the full rationale).
const SOV_CHAIN_SNAPSHOT_RETENTION: ChronoDuration = ChronoDuration::hours(24);

/// Why one [`SovChainCollector::collect_cycle`] could not complete. Every
/// variant is a normal `Result::Err` the poll loop degrades from --
/// mirrors `SovCollectionError` in `src/sov_feed/collector.rs` (spec: "the
/// feed never panics on upstream failure").
#[derive(Debug)]
pub enum SovChainCollectionError {
    Wanderer(WandererError),
    Store(sqlx::Error),
}

impl std::fmt::Display for SovChainCollectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wanderer(error) => write!(formatter, "{error}"),
            Self::Store(error) => write!(formatter, "sov chain store error: {error}"),
        }
    }
}

impl std::error::Error for SovChainCollectionError {}

impl From<sqlx::Error> for SovChainCollectionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(error)
    }
}

/// What one `collect_cycle()` did, for tests and logging.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SovChainCollectionReport {
    /// The freshly-fetched connection list's `(source, target)` edge set
    /// differed from the previously persisted snapshot's (or there was no
    /// previous snapshot at all -- the first-ever fetch always counts as
    /// changed, since there is nothing to compare it against).
    pub topology_changed: bool,
    /// Whether [`DynamicChainSovReachability::recompute`] ran this cycle
    /// (exactly when `topology_changed`).
    pub recomputed: bool,
}

/// Polls the Wanderer map, persists a [`ChainSnapshot`], diffs it against
/// the previous one, and recomputes [`DynamicChainSovReachability`] on a
/// non-empty diff (spec "Reachability", "Sources and cadence"). Mirrors
/// `SovCollector` in `src/sov_feed/collector.rs`: injectable clock for
/// deterministic tests (no wall-clock), a `source` trait object so tests
/// inject a fake `WandererChainSource` instead of a real HTTP client.
pub struct SovChainCollector {
    store: SovStore,
    source: Arc<dyn WandererChainSource>,
    reachability: Arc<DynamicChainSovReachability>,
    clock: Arc<dyn SovClock>,
}

impl SovChainCollector {
    pub fn new(
        store: SovStore,
        source: Arc<dyn WandererChainSource>,
        reachability: Arc<DynamicChainSovReachability>,
    ) -> Self {
        Self {
            store,
            source,
            reachability,
            clock: Arc::new(SystemSovClock),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn SovClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Seeds the in-memory reachability from the last persisted chain
    /// snapshot before the poll loop starts (ticket 05 blocker). Called once
    /// at start-up (`src/lib.rs`) so a restart during a Wanderer outage still
    /// serves the last known chain instead of dropping to stargate-only until
    /// the map topology next changes. Returns whether a snapshot was found
    /// and used to seed.
    pub async fn seed_reachability_from_persisted_snapshot(
        &self,
    ) -> Result<bool, SovChainCollectionError> {
        match self.store.latest_chain_snapshot().await? {
            Some(snapshot) => {
                self.reachability
                    .seed_from_snapshot(&snapshot.connections, snapshot.fetched_at);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// One poll cycle. On any Wanderer error (401/403/503/timeout/parse
    /// error) this returns `Err` *without* touching the store or the
    /// reachability state at all -- the previously persisted snapshot and
    /// the previously computed routes are left exactly as they were,
    /// which is what "the last good snapshot is kept" means in practice:
    /// there is nothing here that actively "keeps" it, because nothing
    /// ever overwrote it. The poll loop (`src/lib.rs`) logs the error;
    /// staleness is then a pure function of time against the last
    /// successful [`SovStore::insert_chain_snapshot`] (see
    /// [`DynamicChainSovReachability::chain_status`]).
    pub async fn collect_cycle(&self) -> Result<SovChainCollectionReport, SovChainCollectionError> {
        let observed_at = self.clock.now();
        let systems = self
            .source
            .fetch_systems()
            .await
            .map_err(SovChainCollectionError::Wanderer)?;
        let connections = self
            .source
            .fetch_connections()
            .await
            .map_err(SovChainCollectionError::Wanderer)?;

        let previous = self.store.latest_chain_snapshot().await?;
        let topology_changed = previous
            .as_ref()
            .map(|snapshot| connection_topology_changed(&snapshot.connections, &connections))
            .unwrap_or(true);

        let snapshot = ChainSnapshot {
            systems,
            connections: connections.clone(),
            fetched_at: observed_at,
        };
        self.store.insert_chain_snapshot(&snapshot).await?;
        self.store
            .prune_chain_snapshots(observed_at - SOV_CHAIN_SNAPSHOT_RETENTION)
            .await?;

        // Recorded on every successful fetch regardless of `topology_changed`
        // (staleness tracks "last successful fetch", not "last time the
        // graph actually changed" -- see `DynamicChainSovReachability`'s
        // doc comment).
        self.reachability.mark_fetch_success(observed_at);

        // Force a recompute on the first successful fetch of this process,
        // even when the fetched topology is byte-identical to the persisted
        // snapshot (ticket 05 blocker). After a restart the in-memory
        // variants start stargate-only (see `DynamicChainSovReachability::new`)
        // while `topology_changed` diffs against the snapshot persisted a
        // poll ago -- almost always unchanged -- so without this the live
        // chain would stay absent from reachability until the map topology
        // next changed. `has_recomputed()` is true after a start-up seed, so
        // a seeded process does not redundantly recompute here.
        let recomputed = topology_changed || !self.reachability.has_recomputed();
        if recomputed {
            self.reachability.recompute(&connections);
        }

        Ok(SovChainCollectionReport {
            topology_changed,
            recomputed,
        })
    }
}

/// Probes the Wanderer host's SSE stream once and logs the result (spec:
/// "the client probes the SSE stream once and logs whether it is enabled,
/// without depending on it"). A thin, deliberately untested (no network,
/// no branching logic beyond formatting) wrapper around
/// `WandererClient::probe_sse` kept here so `src/lib.rs` has one call to
/// make at start-up.
pub async fn log_sse_probe_result(
    client: &crate::sov_feed::WandererClient,
    timeout: std::time::Duration,
) {
    let result = client.probe_sse(timeout).await;
    match result {
        crate::sov_feed::WandererSseProbeResult::Enabled => {
            tracing::info!("Wanderer SSE stream is enabled on this host (still polling; no SSE consumer implemented yet)");
        }
        crate::sov_feed::WandererSseProbeResult::Disabled => {
            tracing::info!(
                "Wanderer SSE stream is disabled on this host; the chain feed continues polling"
            );
        }
        crate::sov_feed::WandererSseProbeResult::AuthRejected => {
            warn!(
                "Wanderer SSE probe was rejected with an auth error (401/403); check the map API key -- polling continues regardless"
            );
        }
        crate::sov_feed::WandererSseProbeResult::Unknown => {
            warn!(
                "Wanderer SSE probe did not return a usable result; polling continues regardless"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sov_feed::SovReachabilitySource;
    use chrono::TimeZone;

    fn connection(
        source: i64,
        target: i64,
        connection_type: WandererConnectionType,
        mass_status: WandererMassStatus,
        time_status: WandererTimeStatus,
        ship_size_type: crate::sov_feed::wanderer::WandererShipSizeType,
    ) -> WandererConnection {
        WandererConnection {
            solar_system_source: source,
            solar_system_target: target,
            id: None,
            map_id: None,
            connection_type,
            mass_status,
            time_status,
            ship_size_type,
            wormhole_type: None,
            locked: false,
        }
    }

    fn wh(
        source: i64,
        target: i64,
        mass_status: WandererMassStatus,
        time_status: WandererTimeStatus,
    ) -> WandererConnection {
        connection(
            source,
            target,
            WandererConnectionType::Wormhole,
            mass_status,
            time_status,
            crate::sov_feed::wanderer::WandererShipSizeType::Medium,
        )
    }

    #[test]
    fn topology_diff_detects_attribute_changes_on_an_existing_edge_as_well_as_added_or_removed_edges(
    ) {
        // Reviewer finding: attribute-only changes (mass ticking to
        // critical, EOL bucket worsening) must count as a diff, not only
        // an edge appearing or disappearing.
        let a = wh(1, 2, WandererMassStatus::Normal, WandererTimeStatus::Normal);
        let a_unchanged = wh(1, 2, WandererMassStatus::Normal, WandererTimeStatus::Normal);
        assert!(!connection_topology_changed(&[a.clone()], &[a_unchanged]));

        let a_degraded = wh(
            1,
            2,
            WandererMassStatus::Critical,
            WandererTimeStatus::Eol1Hour,
        );
        assert!(connection_topology_changed(&[a.clone()], &[a_degraded]));

        let b = wh(2, 3, WandererMassStatus::Normal, WandererTimeStatus::Normal);
        assert!(connection_topology_changed(
            &[a.clone()],
            &[a.clone(), b.clone()]
        ));
        assert!(connection_topology_changed(&[a.clone(), b], &[a]));
    }

    #[test]
    fn topology_diff_treats_reversed_endpoints_as_the_same_edge() {
        let forward = wh(1, 2, WandererMassStatus::Normal, WandererTimeStatus::Normal);
        let reversed = wh(2, 1, WandererMassStatus::Normal, WandererTimeStatus::Normal);
        assert!(!connection_topology_changed(&[forward], &[reversed]));
    }

    fn linear_stargate_graph() -> StargateGraph {
        // 1 -- 2 -- 3 -- 4 -- 5 -- 6 (5 gate jumps end to end).
        StargateGraph::from_edges("test", &[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)])
    }

    #[test]
    fn a_chain_shortcut_beats_a_long_gate_route() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        // Before any chain data, this is exactly ticket 04's stargate-only
        // reachability: 5 jumps to system 6.
        assert_eq!(dynamic.reachable(6, false).unwrap().jumps, 5);

        // A single wormhole from home straight to system 6 shortcuts the
        // whole gate chain.
        dynamic.recompute(&[wh(
            1,
            6,
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
        )]);
        let info = dynamic.reachable(6, false).expect("reachable via chain");
        assert_eq!(info.jumps, 1);
        assert!(info.via_chain);
        assert!(info.path_risk.is_some());
    }

    #[test]
    fn a_critical_mass_hole_is_excluded_from_every_route() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        dynamic.recompute(&[wh(
            1,
            6,
            WandererMassStatus::Critical,
            WandererTimeStatus::Normal,
        )]);
        let info = dynamic
            .reachable(6, false)
            .expect("still reachable by gate");
        assert_eq!(info.jumps, 5);
        assert!(!info.via_chain);
        assert!(info.path_risk.is_none());
    }

    #[test]
    fn a_frigate_hole_is_excluded_by_default_and_included_when_allowed() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let frigate_hole = connection(
            1,
            6,
            WandererConnectionType::Wormhole,
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
            crate::sov_feed::wanderer::WandererShipSizeType::Frigate,
        );
        dynamic.recompute(&[frigate_hole]);

        let without_frigate = dynamic
            .reachable(6, false)
            .expect("gate route still exists");
        assert_eq!(without_frigate.jumps, 5);
        assert!(!without_frigate.via_chain);

        let with_frigate = dynamic
            .reachable(6, true)
            .expect("chain route counted when frigate holes are allowed");
        assert_eq!(with_frigate.jumps, 1);
        assert!(with_frigate.via_chain);
    }

    #[test]
    fn an_eol_hole_is_included_with_correct_path_risk() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        dynamic.recompute(&[wh(
            1,
            6,
            WandererMassStatus::Depleted,
            WandererTimeStatus::Eol4Hours,
        )]);
        let info = dynamic
            .reachable(6, false)
            .expect("reachable via the EOL hole");
        assert_eq!(info.jumps, 1);
        let risk = info
            .path_risk
            .expect("path risk present for a wormhole route");
        assert_eq!(risk.worst_time_status, WandererTimeStatus::Eol4Hours);
        assert_eq!(risk.worst_mass_status, WandererMassStatus::Depleted);
        assert_eq!(risk.describe(), "EOL <4h, mass <50%");
    }

    #[test]
    fn a_gate_type_connection_traverses_as_one_jump_with_no_path_risk() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let gate = connection(
            1,
            6,
            WandererConnectionType::Gate,
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
            crate::sov_feed::wanderer::WandererShipSizeType::Unknown(-1),
        );
        dynamic.recompute(&[gate]);
        let info = dynamic
            .reachable(6, false)
            .expect("reachable via the tracked gate");
        assert_eq!(info.jumps, 1);
        assert!(info.via_chain);
        assert!(info.path_risk.is_none());
    }

    #[test]
    fn a_wanderer_connection_duplicating_an_existing_stargate_edge_does_not_count_as_via_chain() {
        // Reviewer finding: a `Gate`-type Wanderer connection that merely
        // mirrors a real stargate (the common case -- a map operator
        // tracking a normal gate route) must not report `via_chain: true`
        // just because the BFS happened to record its predecessor through
        // the duplicate edge rather than the identical-cost stargate one.
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let duplicate_gate = connection(
            1,
            2,
            WandererConnectionType::Gate,
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
            crate::sov_feed::wanderer::WandererShipSizeType::Unknown(-1),
        );
        dynamic.recompute(&[duplicate_gate]);
        let info = dynamic
            .reachable(2, false)
            .expect("reachable via the real stargate");
        assert_eq!(info.jumps, 1);
        assert!(
            !info.via_chain,
            "a chain connection duplicating a real stargate edge must not count as via_chain"
        );

        // A genuine wormhole shortcut on a *different* pair of systems is
        // unaffected -- only edges that already exist in the static graph
        // are excluded from chain provenance.
        let shortcut = wh(1, 6, WandererMassStatus::Normal, WandererTimeStatus::Normal);
        dynamic.recompute(&[shortcut]);
        let shortcut_info = dynamic
            .reachable(6, false)
            .expect("reachable via the genuine wormhole shortcut");
        assert_eq!(shortcut_info.jumps, 1);
        assert!(shortcut_info.via_chain);
    }

    #[test]
    fn a_gate_only_route_has_no_path_risk_and_stargate_only_reachability_is_unaffected() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let info = dynamic.reachable(6, false).expect("plain stargate route");
        assert_eq!(info.jumps, 5);
        assert!(!info.via_chain);
        assert!(info.path_risk.is_none());
    }

    #[test]
    fn chain_status_starts_pending_and_becomes_fresh_after_a_recorded_success() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let now = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        assert_eq!(dynamic.chain_status(now), SovChainStatus::Pending);
        dynamic.mark_fetch_success(now);
        assert_eq!(dynamic.chain_status(now), SovChainStatus::Fresh);
    }

    #[test]
    fn chain_status_reports_stale_after_ten_minutes_with_no_successful_fetch() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let now = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let long_ago = now - ChronoDuration::minutes(11);
        dynamic.mark_fetch_success(long_ago);
        assert_eq!(
            dynamic.chain_status(now),
            SovChainStatus::Stale { since: long_ago }
        );
    }

    #[test]
    fn an_unrecognized_connection_type_never_traverses() {
        let graph = linear_stargate_graph();
        let dynamic = DynamicChainSovReachability::new(graph, 1);
        let unknown = connection(
            1,
            6,
            WandererConnectionType::Unknown(99),
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
            crate::sov_feed::wanderer::WandererShipSizeType::Medium,
        );
        dynamic.recompute(&[unknown]);
        let info = dynamic
            .reachable(6, false)
            .expect("falls back to the gate route");
        assert_eq!(info.jumps, 5);
        assert!(!info.via_chain);
    }
}
