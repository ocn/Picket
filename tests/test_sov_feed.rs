//! Collector-cycle tests for the sovereignty campaign feed
//! (`.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
//!
//! Follows the contract feed's prior art (`tests/test_contract_intelligence.rs`):
//! fake `SovereigntyEsi`, fake `SovDelivery`, a controlled clock, and a
//! temporary PostgreSQL database. Pure filter-leaf matching is exercised
//! without a database in `src/sov_feed/model.rs`'s unit tests; these tests
//! exercise the collector cycle end to end: Sov Baseline suppression, the
//! `appeared` stage firing exactly once (including across a simulated
//! restart), the twelve-hour window, 304 handling, limiter coordination
//! through the shared `esi_collection_limiter_state` row, and ended
//! campaigns.

mod common;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use killbot_rust::contract_intelligence::ContractCollectionStore;
use killbot_rust::esi_cache::{CacheMetadata, EsiError, EsiResponse};
use killbot_rust::sov_feed::{
    DynamicChainSovReachability, PathRisk, PreparedSovDelivery, SovAlertStage, SovCampaign,
    SovChainCollector, SovChainStatus, SovClock, SovCollector, SovDelivery, SovDeliveryError,
    SovFilter, SovFilterCondition, SovFilterNode, SovMapEntry, SovNotificationMessage,
    SovReachabilityInfo, SovReachabilitySource, SovStore, SovStructure, SovSubscription,
    SovSystemDirectory, SovSystemInfo, SovTickerResolver, SovWatchlistSource, SovereigntyEsi,
    StargateGraph, StargateGraphFile, StaticSovReachability, WandererChainSource,
    WandererConnection, WandererConnectionType, WandererError, WandererMassStatus,
    WandererShipSizeType, WandererSystem, WandererTimeStatus, SOV_HUB_STRUCTURE_TYPE_IDS,
};
use killbot_rust::spawn_sov_collection_loop;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use common::TemporaryDatabase;

// --- Fakes ---

struct FakeSovereigntyEsi {
    responses: Vec<Result<EsiResponse<Vec<SovCampaign>>, EsiError>>,
    cursor: Mutex<usize>,
    /// Scripted `fetch_structures`/`fetch_map` responses (ticket 07).
    /// Empty by default: the 45 pre-ticket-07 call sites never invoke
    /// either method (`collect_structures_cycle`/`collect_map_cycle` are
    /// only exercised by this ticket's own tests), so the "no scripted
    /// responses" assertion mirrors `fetch_campaigns`'s without requiring
    /// every existing `FakeSovereigntyEsi::new(...)` call site to change.
    structures_responses: Mutex<Vec<Result<EsiResponse<Vec<SovStructure>>, EsiError>>>,
    structures_cursor: Mutex<usize>,
    map_responses: Mutex<Vec<Result<EsiResponse<Vec<SovMapEntry>>, EsiError>>>,
    map_cursor: Mutex<usize>,
}

impl FakeSovereigntyEsi {
    fn new(responses: Vec<Result<EsiResponse<Vec<SovCampaign>>, EsiError>>) -> Self {
        Self {
            responses,
            cursor: Mutex::new(0),
            structures_responses: Mutex::new(Vec::new()),
            structures_cursor: Mutex::new(0),
            map_responses: Mutex::new(Vec::new()),
            map_cursor: Mutex::new(0),
        }
    }

    fn with_structures(
        self,
        responses: Vec<Result<EsiResponse<Vec<SovStructure>>, EsiError>>,
    ) -> Self {
        *self.structures_responses.lock().unwrap() = responses;
        self
    }

    fn with_map(self, responses: Vec<Result<EsiResponse<Vec<SovMapEntry>>, EsiError>>) -> Self {
        *self.map_responses.lock().unwrap() = responses;
        self
    }
}

#[async_trait]
impl SovereigntyEsi for FakeSovereigntyEsi {
    async fn fetch_campaigns(
        &self,
        _etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovCampaign>>, EsiError> {
        assert!(
            !self.responses.is_empty(),
            "fake sovereignty ESI was called but has no scripted responses"
        );
        // Once the scripted queue is exhausted, keep repeating the last
        // response rather than panicking: a background retry loop (e.g.
        // `spawn_sov_collection_loop`) may run more cycles than a test
        // explicitly scripts for.
        let mut cursor = self.cursor.lock().unwrap();
        let index = (*cursor).min(self.responses.len() - 1);
        *cursor += 1;
        self.responses[index].clone()
    }

    async fn fetch_structures(
        &self,
        _etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovStructure>>, EsiError> {
        let responses = self.structures_responses.lock().unwrap();
        assert!(
            !responses.is_empty(),
            "fake sovereignty ESI was called for structures but has no scripted responses"
        );
        let mut cursor = self.structures_cursor.lock().unwrap();
        let index = (*cursor).min(responses.len() - 1);
        *cursor += 1;
        responses[index].clone()
    }

    async fn fetch_map(
        &self,
        _etag: Option<String>,
    ) -> Result<EsiResponse<Vec<SovMapEntry>>, EsiError> {
        let responses = self.map_responses.lock().unwrap();
        assert!(
            !responses.is_empty(),
            "fake sovereignty ESI was called for the map but has no scripted responses"
        );
        let mut cursor = self.map_cursor.lock().unwrap();
        let index = (*cursor).min(responses.len() - 1);
        *cursor += 1;
        responses[index].clone()
    }
}

#[derive(Clone, Copy)]
enum FakeSovDeliveryBehavior {
    Succeed,
    FailTransient,
    FailPermanent,
}

struct FakeSovDelivery {
    behavior: Mutex<FakeSovDeliveryBehavior>,
    sent: Mutex<Vec<PreparedSovDelivery>>,
    attempts: Mutex<usize>,
}

impl FakeSovDelivery {
    fn new(fail: bool) -> Self {
        let behavior = if fail {
            FakeSovDeliveryBehavior::FailTransient
        } else {
            FakeSovDeliveryBehavior::Succeed
        };
        Self {
            behavior: Mutex::new(behavior),
            sent: Mutex::new(Vec::new()),
            attempts: Mutex::new(0),
        }
    }

    fn failing_permanently() -> Self {
        Self {
            behavior: Mutex::new(FakeSovDeliveryBehavior::FailPermanent),
            sent: Mutex::new(Vec::new()),
            attempts: Mutex::new(0),
        }
    }

    fn set_behavior(&self, behavior: FakeSovDeliveryBehavior) {
        *self.behavior.lock().unwrap() = behavior;
    }

    fn sent_count(&self) -> usize {
        self.sent.lock().unwrap().len()
    }

    fn sent(&self) -> Vec<PreparedSovDelivery> {
        self.sent.lock().unwrap().clone()
    }

    fn attempt_count(&self) -> usize {
        *self.attempts.lock().unwrap()
    }
}

#[async_trait]
impl SovDelivery for FakeSovDelivery {
    async fn send(&self, delivery: PreparedSovDelivery) -> Result<String, SovDeliveryError> {
        *self.attempts.lock().unwrap() += 1;
        match *self.behavior.lock().unwrap() {
            FakeSovDeliveryBehavior::Succeed => {
                let message_id = format!("msg-{}", delivery.delivery_id);
                self.sent.lock().unwrap().push(delivery);
                Ok(message_id)
            }
            FakeSovDeliveryBehavior::FailTransient => Err(SovDeliveryError::transient(
                "simulated transient delivery failure",
            )),
            FakeSovDeliveryBehavior::FailPermanent => Err(SovDeliveryError::permanent(
                "simulated permanent delivery failure",
            )),
        }
    }
}

struct FakeSovSystemDirectory {
    systems: HashMap<i64, SovSystemInfo>,
    resolve_calls: AtomicUsize,
}

impl FakeSovSystemDirectory {
    fn new() -> Self {
        let mut systems = HashMap::new();
        systems.insert(
            30_005_174,
            SovSystemInfo {
                name: "Turnur".to_string(),
                region_name: "Tenal".to_string(),
                region_id: 10_000_037,
            },
        );
        systems.insert(
            30_004_737,
            SovSystemInfo {
                name: "Jita".to_string(),
                region_name: "The Forge".to_string(),
                region_id: 10_000_002,
            },
        );
        Self {
            systems,
            resolve_calls: AtomicUsize::new(0),
        }
    }

    /// Number of `resolve` calls, used by the lazy-render test (ticket 03) as
    /// an observable proxy for "the notification was rendered": every
    /// `render_stage_message` resolves the system exactly once.
    fn resolve_calls(&self) -> usize {
        self.resolve_calls.load(Ordering::SeqCst)
    }
}

impl SovSystemDirectory for FakeSovSystemDirectory {
    fn resolve(&self, solar_system_id: i64) -> Option<SovSystemInfo> {
        self.resolve_calls.fetch_add(1, Ordering::SeqCst);
        self.systems.get(&solar_system_id).cloned()
    }
}

/// A small in-test reachability source (ticket 04): maps specific system
/// IDs to jumps/route, or reports "graph unavailable" entirely. Mirrors
/// `FakeSovSystemDirectory`'s shape.
struct FakeSovReachability {
    info: HashMap<i64, SovReachabilityInfo>,
    available: bool,
}

impl FakeSovReachability {
    /// No graph loaded at all: `Reachable` never matches, and `/sov_timers`
    /// distinguishes this from "unreachable".
    fn unavailable() -> Self {
        Self {
            info: HashMap::new(),
            available: false,
        }
    }

    /// A graph is available; only the given systems are reachable, purely
    /// over stargates (`via_chain: false`, `path_risk: None`).
    fn with(pairs: Vec<(i64, i64, Vec<i64>)>) -> Self {
        let info = pairs
            .into_iter()
            .map(|(system_id, jumps, route)| {
                (
                    system_id,
                    SovReachabilityInfo {
                        jumps,
                        route,
                        via_chain: false,
                        path_risk: None,
                    },
                )
            })
            .collect();
        Self {
            info,
            available: true,
        }
    }
}

impl SovReachabilitySource for FakeSovReachability {
    fn reachable(
        &self,
        solar_system_id: i64,
        _allow_frigate_holes: bool,
    ) -> Option<SovReachabilityInfo> {
        self.info.get(&solar_system_id).cloned()
    }

    fn graph_available(&self) -> bool {
        self.available
    }
}

/// A reachability source whose jump count (and, if set, Path Risk) can be
/// changed between collector cycles (ticket 06): unlike `FakeSovReachability`,
/// which is fixed at construction, this simulates a wormhole chain
/// opening, closing, and re-opening -- one system's reachability
/// transitioning cycle to cycle the way `DynamicChainSovReachability`
/// does after a real chain poll.
struct VariableSovReachability {
    system_id: i64,
    jumps: Mutex<Option<i64>>,
    path_risk: Mutex<Option<PathRisk>>,
    available: Mutex<bool>,
}

impl VariableSovReachability {
    fn new(system_id: i64) -> Self {
        Self {
            system_id,
            jumps: Mutex::new(None),
            path_risk: Mutex::new(None),
            available: Mutex::new(true),
        }
    }

    /// Sets the current jump count; `None` means unreachable.
    fn set_jumps(&self, jumps: Option<i64>) {
        *self.jumps.lock().unwrap() = jumps;
    }

    fn set_path_risk(&self, risk: Option<PathRisk>) {
        *self.path_risk.lock().unwrap() = risk;
    }

    /// Simulates the graph itself becoming unavailable/available (fix
    /// round finding 1), independent of `jumps`: `reachable()` still
    /// answers per `jumps` regardless of this flag (mirroring
    /// `StaticSovReachability`/`DynamicChainSovReachability`, whose
    /// `reachable()` does not consult their own availability either) --
    /// it is the collector's job to consult `graph_available()` and skip
    /// the reachability pass entirely when it is false.
    fn set_graph_available(&self, available: bool) {
        *self.available.lock().unwrap() = available;
    }
}

impl SovReachabilitySource for VariableSovReachability {
    fn reachable(
        &self,
        solar_system_id: i64,
        _allow_frigate_holes: bool,
    ) -> Option<SovReachabilityInfo> {
        if solar_system_id != self.system_id {
            return None;
        }
        let jumps = (*self.jumps.lock().unwrap())?;
        Some(SovReachabilityInfo {
            jumps,
            route: vec![30_005_174, solar_system_id],
            via_chain: false,
            path_risk: *self.path_risk.lock().unwrap(),
        })
    }

    fn graph_available(&self) -> bool {
        *self.available.lock().unwrap()
    }
}

struct FakeSovTickerResolver;

#[async_trait]
impl SovTickerResolver for FakeSovTickerResolver {
    async fn alliance_ticker(&self, _alliance_id: i64) -> Option<String> {
        Some("TEST".to_string())
    }
}

struct VirtualSovClock {
    now: Mutex<DateTime<Utc>>,
}

impl VirtualSovClock {
    fn new(now: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new(now),
        })
    }

    fn advance(&self, duration: ChronoDuration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

impl SovClock for VirtualSovClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

// --- Helpers ---

fn campaign(
    campaign_id: i64,
    solar_system_id: i64,
    defender_id: i64,
    start_time: DateTime<Utc>,
) -> SovCampaign {
    SovCampaign {
        campaign_id,
        event_type: "ihub_defense".to_string(),
        structure_id: 1_053_511_343_965,
        solar_system_id,
        constellation_id: 20_000_757,
        defender_id: Some(defender_id),
        defender_score: Some(0.6),
        attackers_score: Some(0.4),
        start_time,
    }
}

fn fresh_metadata(observed_at: DateTime<Utc>, etag: &str) -> CacheMetadata {
    CacheMetadata {
        etag: Some(etag.to_string()),
        expires_at: Some(observed_at + ChronoDuration::seconds(5)),
        last_modified: Some(observed_at),
        expected_pages: None,
        error_limit_remain: Some(100),
        error_limit_reset: Some(60),
        rate_limit_group: Some("sovereignty".to_string()),
        rate_limit_limit: Some("600/15m".to_string()),
        rate_limit_remaining: Some(598),
        rate_limit_used: Some(2),
        retry_after: None,
    }
}

fn all_campaigns_filter() -> SovFilter {
    SovFilter {
        root: SovFilterNode::Condition(SovFilterCondition::EventType(vec![
            "ihub_defense".to_string(),
            "tcu_defense".to_string(),
            "station_defense".to_string(),
            "station_freeport".to_string(),
        ])),
    }
}

async fn subscribe(store: &SovStore, name: &str, filter: SovFilter, role_id: Option<u64>) {
    subscribe_with_options(store, name, filter, role_id, serde_json::json!({})).await
}

/// Like [`subscribe`] but with an explicit `options` document, used by the
/// T-minus mark tests to configure custom or disabled marks
/// (`tminus_marks_minutes`). Plain `subscribe` always persists `{}` --
/// exactly what every subscription stored before ticket 03 landed has --
/// so it doubles as the "no stored marks" fixture for the default-marks
/// tests.
async fn subscribe_with_options(
    store: &SovStore,
    name: &str,
    filter: SovFilter,
    role_id: Option<u64>,
    options: serde_json::Value,
) {
    let subscription = SovSubscription {
        guild_id: 1,
        channel_id: 2,
        name: name.to_string(),
        filter,
        options,
        role_id,
    };
    store
        .upsert_subscription(&subscription)
        .await
        .expect("persist sov subscription");
}

/// A Sovereignty Hub fixture (ticket 07): `structure_type_id` defaults to
/// the real observed hub type ([`SOV_HUB_STRUCTURE_TYPE_IDS`]) unless
/// overridden by [`non_hub_structure`].
fn hub(
    structure_id: i64,
    solar_system_id: i64,
    alliance_id: Option<i64>,
    vulnerable_start_time: Option<DateTime<Utc>>,
    vulnerable_end_time: Option<DateTime<Utc>>,
) -> SovStructure {
    SovStructure {
        structure_id,
        structure_type_id: SOV_HUB_STRUCTURE_TYPE_IDS[0],
        alliance_id,
        solar_system_id,
        vulnerability_occupancy_level: Some(6.0),
        vulnerable_start_time,
        vulnerable_end_time,
    }
}

/// A structure with a `structure_type_id` this feed does not treat as a
/// Sovereignty Hub (ticket 07: "a non-hub structure type is ignored").
fn non_hub_structure(structure_id: i64, solar_system_id: i64) -> SovStructure {
    SovStructure {
        structure_id,
        structure_type_id: 99_999, // not in SOV_HUB_STRUCTURE_TYPE_IDS
        alliance_id: Some(99_000_001),
        solar_system_id,
        vulnerability_occupancy_level: Some(6.0),
        vulnerable_start_time: None,
        vulnerable_end_time: None,
    }
}

/// Like [`subscribe_with_options`] but sets `tz_window`/`tz_shift_enabled`
/// directly (ticket 07).
async fn subscribe_with_tz(
    store: &SovStore,
    name: &str,
    filter: SovFilter,
    tz_window: &str,
    tz_shift_enabled: bool,
) {
    subscribe_with_options(
        store,
        name,
        filter,
        None,
        serde_json::json!({"tz_window": tz_window, "tz_shift_enabled": tz_shift_enabled}),
    )
    .await
}

async fn provisioned_stores(
    database: &TemporaryDatabase,
) -> (SovStore, Arc<ContractCollectionStore>) {
    // `ContractCollectionStore::connect` runs the shared MIGRATOR, which
    // covers every file under `migrations/`, including the sov feed's.
    let limiter_store = ContractCollectionStore::connect(&database.url)
        .await
        .expect("connect contract store to apply migrations");
    let sov_store = SovStore::connect(&database.url)
        .await
        .expect("connect sov store");
    (sov_store, Arc::new(limiter_store))
}

fn collector(
    store: SovStore,
    limiter: Arc<ContractCollectionStore>,
    esi: FakeSovereigntyEsi,
    delivery: Arc<FakeSovDelivery>,
    clock: Arc<VirtualSovClock>,
) -> SovCollector {
    collector_with_reachability(
        store,
        limiter,
        esi,
        delivery,
        clock,
        Arc::new(StaticSovReachability(None)),
    )
}

/// Like [`collector`] but with an injected reachability source, used by
/// the `Reachable` filter leaf tests (ticket 04) to exercise the collector
/// against a small in-test graph rather than the real
/// `config/stargates.json`.
fn collector_with_reachability(
    store: SovStore,
    limiter: Arc<ContractCollectionStore>,
    esi: FakeSovereigntyEsi,
    delivery: Arc<FakeSovDelivery>,
    clock: Arc<VirtualSovClock>,
    reachability: Arc<dyn SovReachabilitySource>,
) -> SovCollector {
    SovCollector::new(
        store,
        Arc::new(esi),
        limiter,
        delivery,
        Arc::new(FakeSovSystemDirectory::new()),
        Arc::new(FakeSovTickerResolver),
        reachability,
    )
    .with_clock(clock)
}

// --- Tests ---

#[tokio::test]
async fn baseline_cycle_stores_campaigns_and_posts_nothing() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        112_007,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(1),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    let report = collector.collect_cycle().await.expect("baseline cycle");
    assert!(report.baseline_established_this_cycle);
    assert_eq!(report.newly_appeared, 0);
    assert_eq!(report.alerts_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert!(store
        .campaign_is_baseline(baseline_campaign.campaign_id)
        .await
        .expect("read baseline flag"));
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count deliveries"),
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_new_non_baseline_campaign_within_the_window_posts_exactly_once() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), Some(999)).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let new_campaign = campaign(
        2,
        30_004_737,
        99_012_982,
        observed_at + ChronoDuration::hours(3),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![baseline_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![baseline_campaign.clone(), new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    assert!(!report.baseline_established_this_cycle);
    assert_eq!(report.newly_appeared, 1);
    assert_eq!(report.alerts_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    let sent = delivery.sent();
    assert_eq!(sent[0].subject_id, new_campaign.campaign_id);
    assert_eq!(sent[0].role_id, Some(999));
    assert_eq!(sent[0].message.footer, "Appeared");

    // A third cycle with no ESI change and no new campaigns must not
    // double-post: the unique constraint on (subscription, subject_kind,
    // subject_id, stage) is the dedup authority.
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count deliveries"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_new_campaign_outside_twelve_hours_does_not_post() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let far_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(30),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![far_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    assert_eq!(report.newly_appeared, 0);
    assert_eq!(report.alerts_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_campaign_initially_outside_twelve_hours_posts_once_it_enters_the_window() {
    // Regression for review finding 2: `appeared` must be re-evaluated
    // every cycle for a non-baseline campaign, not only at the cycle it
    // was first observed. A campaign scheduled 30h out is stored without
    // alerting; once its start time falls inside 12h, the very next cycle
    // must alert exactly once (dedup is the `prepare_delivery` unique
    // constraint, not "did we just insert this row").
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let far_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(20),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        // Cycle 2: campaign appears, still outside the 12h window.
        Ok(EsiResponse::fresh(
            vec![far_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
        // Cycle 3: same campaign, ESI facts unchanged, but now inside the
        // window purely because time passed.
        Ok(EsiResponse::fresh(
            vec![far_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::hours(9), "etag-2"),
        )),
        // Cycle 4: nothing changes; must not post a second time.
        Ok(EsiResponse::fresh(
            vec![far_campaign.clone()],
            fresh_metadata(
                observed_at + ChronoDuration::hours(9) + ChronoDuration::seconds(10),
                "etag-2",
            ),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");

    clock.advance(ChronoDuration::seconds(10));
    let outside_report = collector
        .collect_cycle()
        .await
        .expect("campaign appears, still far out");
    assert_eq!(outside_report.alerts_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    // Advance the clock so the campaign's start time is now inside 12h,
    // without any new ESI fact about the campaign itself.
    clock.advance(ChronoDuration::hours(9) - ChronoDuration::seconds(10));
    let entered_report = collector
        .collect_cycle()
        .await
        .expect("campaign now falls inside the twelve-hour window");
    assert_eq!(entered_report.alerts_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);

    // A further cycle with the campaign still in the window must not
    // double-post.
    clock.advance(ChronoDuration::seconds(10));
    let repeat_report = collector.collect_cycle().await.expect("fourth cycle");
    assert_eq!(repeat_report.alerts_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(
        store
            .count_deliveries_for(far_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count deliveries"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_campaign_not_matching_the_subscription_filter_does_not_post() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "only-jita",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::System(vec![30_004_737])),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let elsewhere = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![elsewhere.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    assert_eq!(report.newly_appeared, 1);
    assert_eq!(report.alerts_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn restart_between_prepare_and_send_does_not_double_post() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let failing_delivery = Arc::new(FakeSovDelivery::new(true));
    let clock = VirtualSovClock::new(observed_at);
    let first_collector = collector(
        store.clone(),
        limiter.clone(),
        esi,
        failing_delivery.clone(),
        clock.clone(),
    );

    first_collector
        .collect_cycle()
        .await
        .expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    first_collector
        .collect_cycle()
        .await
        .expect("prepare, claim, and fail to send");

    assert_eq!(failing_delivery.attempt_count(), 1);
    assert_eq!(failing_delivery.sent_count(), 0);
    assert_eq!(
        store
            .delivery_status_by_subject(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("read delivery status"),
        "prepared"
    );

    // Before the lease expires, a fresh collector (simulating a restarted
    // process) must not re-claim the still-leased delivery.
    let still_leased_esi = FakeSovereigntyEsi::new(vec![]);
    let succeeding_delivery = Arc::new(FakeSovDelivery::new(false));
    let second_collector = collector(
        store.clone(),
        limiter.clone(),
        still_leased_esi,
        succeeding_delivery.clone(),
        clock.clone(),
    );
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("unexpired lease is left alone");
    assert_eq!(succeeding_delivery.attempt_count(), 0);

    // After the lease expires (restart-safe recovery window), the next
    // collector reclaims the prepared row and sends it exactly once.
    clock.advance(ChronoDuration::minutes(3));
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("expired lease is reclaimed");
    assert_eq!(succeeding_delivery.sent_count(), 1);
    assert_eq!(
        store
            .delivery_status_by_subject(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("read delivery status"),
        "sent"
    );

    database.destroy().await;
}

#[tokio::test]
async fn not_modified_response_does_not_reprocess_campaigns() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(1),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![baseline_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::not_modified(fresh_metadata(
            observed_at + ChronoDuration::seconds(10),
            "etag-1",
        ))),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("304 cycle");

    assert!(report.not_modified);
    assert_eq!(report.campaigns_observed, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn limiter_coordination_is_persisted_through_the_shared_row() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store, limiter, esi, delivery, clock);

    collector
        .collect_cycle()
        .await
        .expect("cycle records limiter state");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect the shared limiter row");
    let row = sqlx::query(
        "SELECT rate_limit_group, rate_limit_limit, rate_limit_remaining, rate_limit_used FROM esi_collection_limiter_state WHERE limiter_scope = TRUE",
    )
    .fetch_one(&pool)
    .await
    .expect("read shared limiter row");
    assert_eq!(
        row.get::<Option<String>, _>("rate_limit_group"),
        Some("sovereignty".to_string())
    );
    assert_eq!(
        row.get::<Option<String>, _>("rate_limit_limit"),
        Some("600/15m".to_string())
    );
    assert_eq!(row.get::<Option<i64>, _>("rate_limit_remaining"), Some(598));
    assert_eq!(row.get::<Option<i64>, _>("rate_limit_used"), Some(2));
    pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn ended_campaigns_are_marked_when_missing_from_a_later_listing() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let ending_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(1),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![ending_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery, clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    assert!(store
        .campaign_ended_at(ending_campaign.campaign_id)
        .await
        .expect("read ended_at")
        .is_none());

    clock.advance(ChronoDuration::seconds(10));
    let report = collector
        .collect_cycle()
        .await
        .expect("second cycle marks the campaign ended");
    assert_eq!(report.ended, 1);
    assert!(store
        .campaign_ended_at(ending_campaign.campaign_id)
        .await
        .expect("read ended_at")
        .is_some());

    database.destroy().await;
}

#[tokio::test]
async fn fixture_shaped_campaigns_deserialize_and_flow_through_a_baseline_cycle() {
    let fixture = std::fs::read_to_string("resources/sov_campaigns_fixture.json")
        .expect("read live-shape sov campaigns fixture");
    let campaigns: Vec<SovCampaign> =
        serde_json::from_str(&fixture).expect("fixture matches the ESI wire shape");
    assert!(!campaigns.is_empty());
    assert!(campaigns
        .iter()
        .all(|campaign| campaign.event_type == "ihub_defense"));
    assert!(campaigns
        .iter()
        .all(|campaign| campaign.defender_id.is_some()));

    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        campaigns.clone(),
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store, limiter, esi, delivery.clone(), clock);

    let report = collector
        .collect_cycle()
        .await
        .expect("baseline cycle over live-shape fixture");
    assert!(report.baseline_established_this_cycle);
    assert_eq!(report.campaigns_observed, campaigns.len());
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_permanent_delivery_failure_marks_the_row_failed_and_is_never_resent() {
    // Regression for review finding 3: a permanent Discord failure
    // (channel deleted, missing permissions, ...) must leave the delivery
    // durably `failed` rather than retried forever with an unbounded
    // `attempt_count`.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::failing_permanently());
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("prepare, claim, and permanently fail to send");

    assert_eq!(delivery.attempt_count(), 1);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(
        store
            .delivery_status_by_subject(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("read delivery status"),
        "failed"
    );

    // Even long after any lease would have expired, a permanently-failed
    // delivery is never re-claimed or re-sent.
    clock.advance(ChronoDuration::hours(6));
    collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("permanently failed deliveries are not reclaimed");
    assert_eq!(delivery.attempt_count(), 1);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(
        store
            .delivery_status_by_subject(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("read delivery status"),
        "failed"
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_transient_delivery_failure_is_retried_after_its_backoff_lease_expires() {
    // Regression for review finding 3: a transient Discord failure keeps
    // the delivery `prepared` and retryable, but the retry must wait for
    // an attempt-count-scaled backoff rather than the flat short lease
    // used for crash recovery.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(true));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("prepare, claim, and transiently fail to send");
    assert_eq!(delivery.attempt_count(), 1);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(
        store
            .delivery_status_by_subject(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("read delivery status"),
        "prepared"
    );

    delivery.set_behavior(FakeSovDeliveryBehavior::Succeed);

    // Before the first-attempt backoff (2 minutes) elapses, the delivery
    // must not be reclaimed.
    clock.advance(ChronoDuration::seconds(90));
    collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("backoff has not elapsed yet");
    assert_eq!(delivery.attempt_count(), 1);
    assert_eq!(delivery.sent_count(), 0);

    // Once the backoff elapses, the next cycle reclaims and sends it
    // exactly once.
    clock.advance(ChronoDuration::seconds(60));
    collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("backoff elapsed; delivery is reclaimed");
    assert_eq!(delivery.attempt_count(), 2);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(
        store
            .delivery_status_by_subject(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("read delivery status"),
        "sent"
    );

    database.destroy().await;
}

#[tokio::test]
async fn sov_runtime_recovers_the_shared_store_after_postgres_becomes_available() {
    // Regression for review finding 1 (BLOCKER): the sov runtime must
    // start (and let independent tasks run) even while Postgres is
    // unavailable, and must self-heal once it becomes available, without
    // ever blocking on a synchronous connect. Mirrors
    // `contract_runtime_recovers_the_shared_store_after_postgres_becomes_available`
    // in `tests/test_contract_intelligence.rs`.
    let database = TemporaryDatabase::unavailable();
    let store_handle = killbot_rust::sov_feed::new_sov_store_handle();
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi: Arc<dyn SovereigntyEsi> = Arc::new(FakeSovereigntyEsi::new(vec![Ok(
        EsiResponse::fresh(vec![], fresh_metadata(observed_at, "etag-1")),
    )]));
    let delivery: Arc<dyn SovDelivery> = Arc::new(FakeSovDelivery::new(false));
    let directory: Arc<dyn SovSystemDirectory> = Arc::new(FakeSovSystemDirectory::new());
    let tickers: Arc<dyn SovTickerResolver> = Arc::new(FakeSovTickerResolver);

    let reachability: Arc<dyn SovReachabilitySource> = Arc::new(StaticSovReachability(None));
    let collection_task = spawn_sov_collection_loop(
        database.url.clone(),
        store_handle.clone(),
        StdDuration::from_millis(10),
        esi,
        delivery,
        directory,
        tickers,
        reachability,
        killbot_rust::SovFeedStartupSummary {
            home_system_id: 30_002_086,
            graph: None,
        },
    );

    let (processing_started, processing_complete) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = processing_started.send(());
    });
    tokio::time::timeout(StdDuration::from_millis(50), processing_complete)
        .await
        .expect("the sov runtime can start processing while PostgreSQL is unavailable")
        .expect("the spawned check-in task completes its own work");
    assert!(killbot_rust::sov_feed::available_sov_store(&store_handle)
        .await
        .is_none());

    database.create().await;
    // Widened from 2s (ticket 14: observed flaky under heavy external
    // load with `Elapsed`, passing 3/3 in isolation). The 10ms poll
    // interval is unchanged; only the outer real-time budget grew, so a
    // healthy reconnect still returns almost immediately.
    let store = tokio::time::timeout(StdDuration::from_secs(30), async {
        loop {
            if let Some(store) = killbot_rust::sov_feed::available_sov_store(&store_handle).await {
                return store;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("background sov runtime reconnects after PostgreSQL recovery");

    // The recovered store is immediately usable for command handling, just
    // like the contract feed's recovery test proves for its own store.
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;
    assert_eq!(
        store
            .subscriptions()
            .await
            .expect("recovered store lists the subscription just persisted")
            .len(),
        1
    );

    collection_task.abort();
    assert!(collection_task
        .await
        .expect_err("background sov runtime is cancelled")
        .is_cancelled());
    database.destroy().await;
}

// --- T-minus alert stage tests (ticket 03) ---
//
// `evaluate_stage_cycle` is the ESI-free seam these tests exercise: it
// checks configured T-minus marks against persisted campaigns and the
// controlled clock alone, independent of `collect_cycle`'s sixty-second
// ESI poll.

#[tokio::test]
async fn a_campaign_across_virtual_time_gets_appeared_then_each_tminus_mark_once_and_nothing_after_start(
) {
    // Simulates one campaign across virtual time (spec acceptance test
    // list): `appeared` fires once, `T-120` fires once, `T-30` fires
    // once, and nothing fires once the campaign has started. Uses the
    // default marks (`subscribe` persists `options == {}`).
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(125),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("campaign appears within twelve hours");
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count appeared deliveries"),
        1
    );

    // Too early for T-120: remaining is still ~124m50s.
    let too_early = collector
        .evaluate_stage_cycle()
        .await
        .expect("stage evaluation before T-120 is due");
    assert_eq!(too_early.tminus_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);

    // Advance to exactly T-120.
    clock.advance(ChronoDuration::minutes(5) - ChronoDuration::seconds(10));
    let at_t120 = collector
        .evaluate_stage_cycle()
        .await
        .expect("T-120 mark is due");
    assert_eq!(at_t120.tminus_prepared, 1);
    assert_eq!(delivery.sent_count(), 2);
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("count T-120 deliveries"),
        1
    );

    // Advance to exactly T-30; calling twice in a row must not double-post.
    clock.advance(ChronoDuration::minutes(90));
    let at_t30 = collector
        .evaluate_stage_cycle()
        .await
        .expect("T-30 mark is due");
    assert_eq!(at_t30.tminus_prepared, 1);
    let repeat = collector
        .evaluate_stage_cycle()
        .await
        .expect("T-30 already fired");
    assert_eq!(repeat.tminus_prepared, 0);
    assert_eq!(delivery.sent_count(), 3);
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::TMinus(30))
            .await
            .expect("count T-30 deliveries"),
        1
    );

    // Advance past the start time: no further marks.
    clock.advance(ChronoDuration::minutes(31));
    let after_start = collector
        .evaluate_stage_cycle()
        .await
        .expect("campaign has already started");
    assert_eq!(after_start.tminus_prepared, 0);
    assert_eq!(delivery.sent_count(), 3);

    let sent = delivery.sent();
    assert!(sent
        .iter()
        .any(|d| d.stage == "appeared" && d.message.footer == "Appeared"));
    assert!(sent
        .iter()
        .any(|d| d.stage == "tminus:120" && d.message.footer == "T-120m"));
    assert!(sent
        .iter()
        .any(|d| d.stage == "tminus:30" && d.message.footer == "T-30m"));

    database.destroy().await;
}

#[tokio::test]
async fn both_tminus_marks_fire_in_one_pass_after_a_gap_skips_the_first_mark() {
    // Simulates downtime: the stage-evaluation pass never ran while the
    // campaign passed through T-120, so the next pass must fire both
    // T-120 and T-30 together, each once (spec: "When two marks are due
    // in the same pass ... both fire in that pass, each once").
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(20),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    collector.collect_cycle().await.expect("campaign appears");

    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("both marks are due at once");
    assert_eq!(report.tminus_prepared, 2);
    let sent = delivery.sent();
    assert!(sent.iter().any(|d| d.stage == "tminus:120"));
    assert!(sent.iter().any(|d| d.stage == "tminus:30"));

    database.destroy().await;
}

#[tokio::test]
async fn tminus_marks_fire_for_a_baseline_campaign_that_never_got_appeared() {
    // Sov Baseline campaigns produce no `appeared` alert, but must still
    // receive T-minus marks (spec: "Baseline campaigns get marks even
    // though they never got appeared").
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(20),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector
        .collect_cycle()
        .await
        .expect("baseline cycle establishes the campaign");
    assert!(store
        .campaign_is_baseline(baseline_campaign.campaign_id)
        .await
        .expect("read baseline flag"));
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count appeared deliveries"),
        0
    );

    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("marks fire for a baseline campaign");
    assert_eq!(report.tminus_prepared, 2);
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("count T-120 deliveries"),
        1
    );
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(30))
            .await
            .expect("count T-30 deliveries"),
        1
    );
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("appeared still never fires for a baseline campaign"),
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_tminus_mark_does_not_duplicate_across_a_simulated_restart() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(100),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let failing_delivery = Arc::new(FakeSovDelivery::new(true));
    let clock = VirtualSovClock::new(observed_at);
    let first_collector = collector(
        store.clone(),
        limiter.clone(),
        esi,
        failing_delivery.clone(),
        clock.clone(),
    );

    first_collector
        .collect_cycle()
        .await
        .expect("baseline cycle establishes the campaign");
    let report = first_collector
        .evaluate_stage_cycle()
        .await
        .expect("T-120 is prepared and its send fails transiently");
    assert_eq!(report.tminus_prepared, 1);
    assert_eq!(
        store
            .delivery_status_by_subject(baseline_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("read delivery status"),
        "prepared"
    );

    // Before the lease expires, a fresh collector (simulating a
    // restarted process) must not re-claim the still-leased delivery.
    let still_leased_esi = FakeSovereigntyEsi::new(vec![]);
    let succeeding_delivery = Arc::new(FakeSovDelivery::new(false));
    let second_collector = collector(
        store.clone(),
        limiter,
        still_leased_esi,
        succeeding_delivery.clone(),
        clock.clone(),
    );
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("unexpired lease is left alone");
    assert_eq!(succeeding_delivery.attempt_count(), 0);

    // After the lease expires (restart-safe recovery window), the next
    // collector reclaims the prepared row and sends it exactly once.
    clock.advance(ChronoDuration::minutes(3));
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("expired lease is reclaimed");
    assert_eq!(succeeding_delivery.sent_count(), 1);
    assert_eq!(
        store
            .delivery_status_by_subject(baseline_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("read delivery status"),
        "sent"
    );
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("no duplicate delivery row exists"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn custom_tminus_marks_replace_the_default_for_a_subscription() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_options(
        &store,
        "custom-marks",
        all_campaigns_filter(),
        None,
        serde_json::json!({"tminus_marks_minutes": [45]}),
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(40),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("custom T-45 mark is due");
    assert_eq!(report.tminus_prepared, 1);
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(45))
            .await
            .expect("count T-45 deliveries"),
        1
    );
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("default T-120 must not fire for a custom-marks subscription"),
        0
    );
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(30))
            .await
            .expect("default T-30 must not fire for a custom-marks subscription"),
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn an_empty_tminus_marks_option_disables_marks_for_a_subscription() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_options(
        &store,
        "no-marks",
        all_campaigns_filter(),
        None,
        serde_json::json!({"tminus_marks_minutes": []}),
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(10),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("marks are disabled for this subscription");
    assert_eq!(report.tminus_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_subscription_with_no_stored_marks_option_uses_the_default() {
    // Every subscription persisted before ticket 03 landed has
    // `options == {}` (no `tminus_marks_minutes` key at all); `subscribe`
    // persists exactly that, so it doubles as the pre-ticket fixture
    // (spec: "Existing subscriptions with no stored marks behave as
    // default [120, 30]").
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(120),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("default T-120 mark is due");
    assert_eq!(report.tminus_prepared, 1);
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("count T-120 deliveries"),
        1
    );
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(30))
            .await
            .expect("T-30 is not yet due"),
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn an_ended_campaign_never_gets_a_tminus_mark() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let ended_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(10),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![ended_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector
        .collect_cycle()
        .await
        .expect("baseline cycle establishes the campaign");
    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("campaign vanishes from the listing and is marked ended");
    assert!(store
        .campaign_ended_at(ended_campaign.campaign_id)
        .await
        .expect("read ended_at")
        .is_some());

    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("ended campaign is skipped");
    assert_eq!(report.tminus_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

// --- Fix round tests (ticket 03 review findings 1, 2, 4) ---

#[tokio::test]
async fn evaluate_stage_cycle_never_panics_on_an_out_of_range_mark_stored_directly_in_options() {
    // Review finding 1: `/sov_subscribe` rejects marks above the ceiling,
    // but `evaluate_stage_cycle` must not trust that every route into
    // `options` went through the command's validation (a future
    // migration, a direct database edit, ...). An ordinary huge `i64`
    // used to panic `ChronoDuration::minutes`; it must now simply never
    // be due.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_options(
        &store,
        "bypassed-marks",
        all_campaigns_filter(),
        None,
        serde_json::json!({"tminus_marks_minutes": [9_999_999_999_999_999i64, 30]}),
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(20),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![baseline_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    // Must return Ok (no panic) and only the well-formed 30-minute mark
    // fires; the absurd value never does.
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("an out-of-range mark never panics evaluate_stage_cycle");
    assert_eq!(report.tminus_prepared, 1);
    assert_eq!(
        store
            .count_deliveries_for(baseline_campaign.campaign_id, SovAlertStage::TMinus(30))
            .await
            .expect("count T-30 deliveries"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn subscription_lookup_returns_the_stored_row_and_none_when_absent() {
    // Covers the new `SovStore::subscription` lookup (review finding 2's
    // plumbing): `/sov_subscribe` uses this to read a previously-stored
    // `options` document before merging in a re-subscription's changes.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;

    assert!(store
        .subscription(1, 2, "does-not-exist")
        .await
        .expect("lookup a subscription that was never created")
        .is_none());

    subscribe_with_options(
        &store,
        "watch-all",
        all_campaigns_filter(),
        Some(555),
        serde_json::json!({"tminus_marks_minutes": [90]}),
    )
    .await;

    let found = store
        .subscription(1, 2, "watch-all")
        .await
        .expect("lookup an existing subscription")
        .expect("the subscription exists");
    assert_eq!(found.name, "watch-all");
    assert_eq!(found.role_id, Some(555));
    assert_eq!(
        found.options,
        serde_json::json!({"tminus_marks_minutes": [90]})
    );

    database.destroy().await;
}

#[tokio::test]
async fn re_persisting_a_subscription_with_the_previously_read_options_preserves_custom_marks() {
    // Review finding 2, at the persistence seam `/sov_subscribe` relies
    // on: subscribe with custom marks, then perform the exact sequence
    // the command performs when a later `/sov_subscribe` omits
    // `tminus_marks` -- look the row up, keep its `options` verbatim, and
    // upsert again (here with a changed filter, standing in for "only a
    // filter change") -- and prove the marks survive the round trip
    // through JSONB rather than being silently reset to the default. The
    // pure branch that decides *whether* to keep or replace `options`
    // (`SovSubscribeCommand::merge_options`) is unit-tested directly in
    // `src/commands/sov_subscribe.rs`; this proves the database side of
    // that contract.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;

    subscribe_with_options(
        &store,
        "watch-all",
        all_campaigns_filter(),
        None,
        serde_json::json!({"tminus_marks_minutes": [90]}),
    )
    .await;

    // Simulate `/sov_subscribe` re-run without `tminus_marks`: read the
    // existing row, keep its `options`, apply only the filter change.
    let existing = store
        .subscription(1, 2, "watch-all")
        .await
        .expect("lookup before re-subscribing")
        .expect("subscription exists");
    let changed_filter = SovFilter {
        root: SovFilterNode::Condition(SovFilterCondition::System(vec![30_004_737])),
    };
    subscribe_with_options(
        &store,
        "watch-all",
        changed_filter.clone(),
        None,
        existing.options.clone(),
    )
    .await;

    let after = store
        .subscription(1, 2, "watch-all")
        .await
        .expect("lookup after re-subscribing")
        .expect("subscription still exists");
    assert_eq!(
        after.options,
        serde_json::json!({"tminus_marks_minutes": [90]}),
        "custom marks must survive a re-subscription that did not touch them"
    );
    assert_eq!(after.filter, changed_filter, "the filter change did apply");

    // And an explicit new value still replaces whatever was stored.
    subscribe_with_options(
        &store,
        "watch-all",
        changed_filter,
        None,
        serde_json::json!({"tminus_marks_minutes": [120, 30]}),
    )
    .await;
    let replaced = store
        .subscription(1, 2, "watch-all")
        .await
        .expect("lookup after an explicit marks change")
        .expect("subscription still exists");
    assert_eq!(
        replaced.options,
        serde_json::json!({"tminus_marks_minutes": [120, 30]})
    );

    database.destroy().await;
}

#[tokio::test]
async fn re_subscribe_carrying_role_and_convenience_provenance_preserves_them_and_the_filter_match()
{
    // The database side of ticket 15's carry-forward contract (the pure
    // decision -- `SovSubscribeCommand::resolve_subscription` -- is
    // unit-tested in `src/commands/sov_subscribe.rs`). Persist a
    // subscription with a ping role plus the ticket-15 provenance keys and
    // a composed filter, then perform the exact write the command performs
    // when a later `/sov_subscribe` changes only the filter while carrying
    // the role and convenience leaves forward, and prove: the role_id
    // column survives, the provenance options round-trip through JSONB, and
    // the recomposed filter still matches the same campaign it did before.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target = campaign(
        1,
        30_004_737,
        99_000_001,
        observed_at + ChronoDuration::hours(2),
    );

    // A composed filter equivalent to `filter:<vulnerable_within 12h>`
    // plus `region_id`/`defender_alliance_id` convenience leaves.
    let region_leaf = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060]));
    let defender_leaf = SovFilterNode::Condition(SovFilterCondition::Defender {
        alliance_ids: vec![99_000_001],
        watchlist: false,
    });
    let explicit = SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 12 });
    let composed = SovFilter {
        root: SovFilterNode::And(vec![
            explicit.clone(),
            region_leaf.clone(),
            defender_leaf.clone(),
        ]),
    };
    let region_of = |system_id: i64| (system_id == 30_004_737).then_some(10_000_060);
    assert!(composed
        .root
        .matches(&target, observed_at, &region_of, &|_, _| None));

    let provenance = serde_json::json!({
        "explicit_filter": {"root": {"condition": {"vulnerable_within": {"hours": 12}}}},
        "region_id": 10_000_060,
        "defender_alliance_id": 99_000_001,
    });
    subscribe_with_options(
        &store,
        "front",
        composed.clone(),
        Some(555),
        provenance.clone(),
    )
    .await;

    // Simulate the command's carry-forward: read the stored row, keep its
    // role_id and provenance options, apply only a filter change that
    // recomposes the SAME convenience leaves onto a new explicit filter.
    let existing = store
        .subscription(1, 2, "front")
        .await
        .expect("lookup before re-subscribing")
        .expect("subscription exists");
    assert_eq!(existing.role_id, Some(555));
    assert_eq!(existing.options, provenance);

    let new_explicit = SovFilterNode::Condition(SovFilterCondition::EventType(vec![
        "ihub_defense".to_string(),
    ]));
    let recomposed = SovFilter {
        root: SovFilterNode::And(vec![new_explicit, region_leaf, defender_leaf]),
    };
    let mut carried_options = existing.options.clone();
    carried_options["explicit_filter"] =
        serde_json::json!({"root": {"condition": {"event_type": ["ihub_defense"]}}});
    subscribe_with_options(
        &store,
        "front",
        recomposed.clone(),
        existing.role_id,
        carried_options,
    )
    .await;

    let after = store
        .subscription(1, 2, "front")
        .await
        .expect("lookup after re-subscribing")
        .expect("subscription still exists");
    assert_eq!(
        after.role_id,
        Some(555),
        "the ping role must survive a filter-only re-subscribe"
    );
    assert_eq!(
        after.options["region_id"],
        serde_json::json!(10_000_060),
        "convenience provenance must round-trip through JSONB"
    );
    assert!(
        after
            .filter
            .root
            .matches(&target, observed_at, &region_of, &|_, _| None),
        "the recomposed filter must still match the same campaign"
    );

    database.destroy().await;
}

#[tokio::test]
async fn subscription_returns_err_for_a_row_whose_filter_json_is_corrupt() {
    // Ticket 15 review finding 1: `/sov_subscribe` must distinguish a store
    // read *error* from a confirmed absence, so it never composes a
    // first-time subscription (dropping the stored role/convenience) over a
    // row it merely failed to read. Prove the read seam it relies on --
    // `subscription()` -- surfaces an undeserializable row as `Err`, not
    // `Ok(None)`, so the command's `ExistingLookup::Unreadable` branch is
    // reachable in production.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;

    subscribe(
        &store,
        "front",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 6 }),
        },
        Some(555),
    )
    .await;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to corrupt the stored filter directly");
    // Valid JSONB, but not a valid `SovFilter` (no `root`): deserialization
    // fails when the row is read back.
    sqlx::query("UPDATE sov_subscriptions SET filter = $1 WHERE name = 'front'")
        .bind(serde_json::json!({"not_a_filter": true}))
        .execute(&pool)
        .await
        .expect("corrupt the stored filter directly, bypassing validate()");
    pool.close().await;

    let result = store.subscription(1, 2, "front").await;
    assert!(
        result.is_err(),
        "a row that fails to deserialize must surface as Err, not Ok(None)"
    );

    database.destroy().await;
}

// --- Second fix round test (ticket 03 review: VulnerableWithin hours ceiling) ---

#[tokio::test]
async fn a_filter_with_out_of_range_vulnerable_within_hours_never_panics_either_cycle_and_later_subscriptions_still_fire(
) {
    // Twin of the T-minus marks blocker: `VulnerableWithin { hours }`
    // stored directly in the filter jsonb -- bypassing
    // `/sov_subscribe`'s `validate()`, e.g. a row written before the
    // ceiling existed, or by hand -- must not panic `collect_cycle` or
    // `evaluate_stage_cycle`, and must not starve subscriptions ordered
    // after the poisoned one in the same pass.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    // Sorts before "z-healthy" below, so the loop reaches the poisoned
    // subscription first in every pass -- proving a later subscription
    // still gets evaluated rather than the whole cycle aborting.
    subscribe(
        &store,
        "a-poisoned",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 6 }),
        },
        None,
    )
    .await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to poison the stored filter directly");
    sqlx::query("UPDATE sov_subscriptions SET filter = $1 WHERE name = 'a-poisoned'")
        .bind(serde_json::json!({
            "root": {"condition": {"vulnerable_within": {"hours": 9_999_999_999_999_999i64}}}
        }))
        .execute(&pool)
        .await
        .expect("poison the stored filter directly, bypassing validate()");
    pool.close().await;

    subscribe(&store, "z-healthy", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(20),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector
        .collect_cycle()
        .await
        .expect("collect_cycle returns Ok despite the poisoned subscription's overflowing filter");
    // The healthy subscription, ordered after the poisoned one, still
    // gets its `appeared` alert.
    assert_eq!(report.alerts_prepared, 1);
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count appeared deliveries"),
        1
    );

    let stage_report = collector.evaluate_stage_cycle().await.expect(
        "evaluate_stage_cycle returns Ok despite the poisoned subscription's overflowing filter",
    );
    // Default marks (T-120 and T-30) both due for the healthy
    // subscription; the poisoned subscription contributes nothing.
    assert_eq!(stage_report.tminus_prepared, 2);
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::TMinus(120))
            .await
            .expect("count T-120 deliveries"),
        1
    );
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::TMinus(30))
            .await
            .expect("count T-30 deliveries"),
        1
    );

    database.destroy().await;
}

// --- Reachability tests (ticket 04) ---
//
// These inject a small in-test `FakeSovReachability` rather than reading
// the real `config/stargates.json`, per the ticket's testing decisions:
// the collector-cycle seam is exercised the same way as every other sov
// feed test, only with a fake reachability source standing in for the
// generated graph.

#[tokio::test]
async fn reachable_subscription_posts_only_the_reachable_campaign_and_the_embed_carries_the_route_line(
) {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 11,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let reachable_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );
    let unreachable_campaign = campaign(
        2,
        30_009_999,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![reachable_campaign.clone(), unreachable_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability: Arc<dyn SovReachabilitySource> = Arc::new(FakeSovReachability::with(vec![(
        30_004_737,
        1,
        vec![30_005_174, 30_004_737],
    )]));
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability,
    );

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    // Both campaigns are candidates (within the twelve-hour window), but
    // only the reachable one matches the subscription's Reachable leaf.
    assert_eq!(report.newly_appeared, 2);
    assert_eq!(report.alerts_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    let sent = delivery.sent();
    assert_eq!(sent[0].subject_id, reachable_campaign.campaign_id);
    let reachability_field = sent[0]
        .message
        .fields
        .iter()
        .find(|field| field.name == "Reachability")
        .expect("embed carries a Reachability field for the reachable campaign");
    assert!(reachability_field.value.contains("1 jump"));
    assert!(reachability_field.value.contains("Turnur"));
    assert!(reachability_field.value.contains("Jita"));
    assert_eq!(
        store
            .count_deliveries_for(unreachable_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count deliveries for the unreachable campaign"),
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn reachable_composes_with_defender_via_and() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "reachable-defender",
        SovFilter {
            root: SovFilterNode::And(vec![
                SovFilterNode::Condition(SovFilterCondition::Reachable {
                    max_jumps: 11,
                    allow_frigate_holes: false,
                }),
                SovFilterNode::Condition(SovFilterCondition::Defender {
                    alliance_ids: vec![99_006_751],
                    watchlist: false,
                }),
            ]),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    // Reachable and the right defender: must post.
    let matches_both = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );
    // Reachable but the wrong defender: must not post.
    let wrong_defender = campaign(
        2,
        30_004_737,
        99_012_982,
        observed_at + ChronoDuration::hours(2),
    );
    // Right defender but unreachable: must not post.
    let unreachable = campaign(
        3,
        30_009_999,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![
                matches_both.clone(),
                wrong_defender.clone(),
                unreachable.clone(),
            ],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability: Arc<dyn SovReachabilitySource> = Arc::new(FakeSovReachability::with(vec![(
        30_004_737,
        1,
        vec![30_005_174, 30_004_737],
    )]));
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability,
    );

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    assert_eq!(report.alerts_prepared, 1);
    let sent = delivery.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].subject_id, matches_both.campaign_id);

    database.destroy().await;
}

#[tokio::test]
async fn graph_unavailable_makes_reachable_never_match_but_other_subscriptions_still_fire() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "reachable-sub",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 11,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let new_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );

    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![new_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability: Arc<dyn SovReachabilitySource> = Arc::new(FakeSovReachability::unavailable());
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability,
    );

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    // "reachable-sub" never matches without a loaded graph; "watch-all" is
    // unaffected and still fires.
    assert_eq!(report.alerts_prepared, 1);
    let sent = delivery.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].subscription_name, "watch-all");
    assert_eq!(
        store
            .count_deliveries_for(new_campaign.campaign_id, SovAlertStage::Appeared)
            .await
            .expect("count deliveries"),
        1
    );

    database.destroy().await;
}

// --- Wanderer chain reachability (ticket 05) ---
//
// `FakeWandererChainSource` mirrors `FakeSovereigntyEsi`'s scripted-queue
// shape: connections responses are scripted per cycle (repeating the last
// once exhausted); systems are returned fixed since no test here reads
// system facts back (`WandererSystem` is retained on `ChainSnapshot` for
// forward compatibility only -- ticket 05's own acceptance criteria).
// `synthetic_stargate_graph` builds a `StargateGraph` from a plain edge
// list via the public `StargateGraphFile` shape, since
// `StargateGraph::from_edges` is a `#[cfg(test)]`-only helper invisible to
// this external integration-test crate.

struct FakeWandererChainSource {
    connection_responses: Vec<Result<Vec<WandererConnection>, WandererError>>,
    cursor: Mutex<usize>,
}

impl FakeWandererChainSource {
    fn new(connection_responses: Vec<Result<Vec<WandererConnection>, WandererError>>) -> Self {
        Self {
            connection_responses,
            cursor: Mutex::new(0),
        }
    }
}

#[async_trait]
impl WandererChainSource for FakeWandererChainSource {
    async fn fetch_systems(&self) -> Result<Vec<WandererSystem>, WandererError> {
        Ok(Vec::new())
    }

    async fn fetch_connections(&self) -> Result<Vec<WandererConnection>, WandererError> {
        assert!(
            !self.connection_responses.is_empty(),
            "fake wanderer chain source was called but has no scripted responses"
        );
        let mut cursor = self.cursor.lock().unwrap();
        let index = (*cursor).min(self.connection_responses.len() - 1);
        *cursor += 1;
        self.connection_responses[index].clone()
    }
}

fn synthetic_stargate_graph(edges: &[(i64, i64)]) -> StargateGraph {
    let mut adjacency: std::collections::BTreeMap<i64, Vec<i64>> =
        std::collections::BTreeMap::new();
    for &(a, b) in edges {
        adjacency.entry(a).or_default().push(b);
        adjacency.entry(b).or_default().push(a);
    }
    StargateGraph::from_file(StargateGraphFile {
        sde_version: "test".to_string(),
        source_last_modified: None,
        generated_at: Utc::now(),
        system_count: adjacency.len(),
        edge_count: edges.len(),
        adjacency,
    })
}

fn wormhole(
    source: i64,
    target: i64,
    mass_status: WandererMassStatus,
    time_status: WandererTimeStatus,
    ship_size_type: WandererShipSizeType,
) -> WandererConnection {
    WandererConnection {
        solar_system_source: source,
        solar_system_target: target,
        id: None,
        map_id: None,
        connection_type: WandererConnectionType::Wormhole,
        mass_status,
        time_status,
        ship_size_type,
        wormhole_type: None,
        locked: false,
    }
}

#[tokio::test]
async fn chain_collect_cycle_persists_a_snapshot_and_a_shortcut_beats_the_gate_route() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    // Home (1) to system 6 is 5 gate jumps without any chain data.
    let graph = synthetic_stargate_graph(&[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 1));
    assert_eq!(dynamic.reachable(6, false).unwrap().jumps, 5);

    let source = FakeWandererChainSource::new(vec![Ok(vec![wormhole(
        1,
        6,
        WandererMassStatus::Normal,
        WandererTimeStatus::Normal,
        WandererShipSizeType::Medium,
    )])]);
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());
    let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
        .with_clock(clock.clone());

    let report = collector.collect_cycle().await.expect("first chain cycle");
    assert!(report.topology_changed);
    assert!(report.recomputed);

    let info = dynamic
        .reachable(6, false)
        .expect("reachable via the chain shortcut");
    assert_eq!(info.jumps, 1);
    assert!(info.via_chain);

    let latest = store
        .latest_chain_snapshot()
        .await
        .expect("load latest snapshot")
        .expect("a snapshot exists after a successful fetch");
    assert_eq!(latest.connections.len(), 1);
    assert_eq!(latest.fetched_at, clock.now());

    database.destroy().await;
}

#[tokio::test]
async fn chain_collect_cycle_excludes_a_critical_mass_hole_from_every_route() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let graph = synthetic_stargate_graph(&[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 1));

    let source = FakeWandererChainSource::new(vec![Ok(vec![wormhole(
        1,
        6,
        WandererMassStatus::Critical,
        WandererTimeStatus::Normal,
        WandererShipSizeType::Medium,
    )])]);
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());
    let collector =
        SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone()).with_clock(clock);
    collector
        .collect_cycle()
        .await
        .expect("chain cycle with a critical-mass hole");

    let info = dynamic
        .reachable(6, false)
        .expect("still reachable by gate");
    assert_eq!(info.jumps, 5);
    assert!(!info.via_chain);

    database.destroy().await;
}

#[tokio::test]
async fn chain_collect_cycle_recomputes_when_an_existing_holes_mass_status_degrades_to_critical() {
    // Reviewer finding: a mass change on an *existing* edge (no edge
    // added or removed) must still trigger a recompute, and the hole
    // must stop being traversable in the freshly recomputed variants.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let graph = synthetic_stargate_graph(&[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 1));

    let source = FakeWandererChainSource::new(vec![
        Ok(vec![wormhole(
            1,
            6,
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
            WandererShipSizeType::Medium,
        )]),
        Ok(vec![wormhole(
            1,
            6,
            WandererMassStatus::Critical,
            WandererTimeStatus::Normal,
            WandererShipSizeType::Medium,
        )]),
    ]);
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());
    let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
        .with_clock(clock.clone());

    let first_report = collector.collect_cycle().await.expect("first chain cycle");
    assert!(first_report.recomputed);
    let via_shortcut = dynamic
        .reachable(6, false)
        .expect("reachable via the chain shortcut");
    assert_eq!(via_shortcut.jumps, 1);
    assert!(via_shortcut.via_chain);

    clock.advance(ChronoDuration::minutes(2));
    let second_report = collector
        .collect_cycle()
        .await
        .expect("second chain cycle, same edge now critical");
    assert!(
        second_report.recomputed,
        "an attribute-only change on an existing edge must still trigger a recompute"
    );
    let after_critical = dynamic
        .reachable(6, false)
        .expect("still reachable by gate once the hole goes critical");
    assert_eq!(after_critical.jumps, 5);
    assert!(!after_critical.via_chain);

    database.destroy().await;
}

#[tokio::test]
async fn chain_collect_cycle_excludes_frigate_hole_by_default_and_includes_it_when_allowed() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let graph = synthetic_stargate_graph(&[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 1));

    let source = FakeWandererChainSource::new(vec![Ok(vec![wormhole(
        1,
        6,
        WandererMassStatus::Normal,
        WandererTimeStatus::Normal,
        WandererShipSizeType::Frigate,
    )])]);
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());
    let collector =
        SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone()).with_clock(clock);
    collector
        .collect_cycle()
        .await
        .expect("chain cycle with a frigate-only hole");

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

    database.destroy().await;
}

#[tokio::test]
async fn chain_collect_cycle_keeps_the_last_snapshot_and_errors_on_a_wanderer_failure() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let graph = synthetic_stargate_graph(&[]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 1));

    let source = FakeWandererChainSource::new(vec![
        Ok(vec![wormhole(
            1,
            2,
            WandererMassStatus::Normal,
            WandererTimeStatus::Normal,
            WandererShipSizeType::Medium,
        )]),
        Err(WandererError::Status {
            status: 503,
            body_summary: "maintenance".to_string(),
        }),
    ]);
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());
    let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
        .with_clock(clock.clone());

    collector
        .collect_cycle()
        .await
        .expect("first cycle succeeds");
    let before = dynamic
        .reachable(2, false)
        .expect("reachable after the first successful fetch");
    let first_fetched_at = clock.now();

    clock.advance(ChronoDuration::minutes(2));
    let result = collector.collect_cycle().await;
    assert!(
        result.is_err(),
        "a Wanderer error must surface as Err, never a panic"
    );

    let after = dynamic
        .reachable(2, false)
        .expect("still reachable via the retained last-good snapshot");
    assert_eq!(
        before, after,
        "a failed fetch must not change reachability at all"
    );

    let latest = store
        .latest_chain_snapshot()
        .await
        .expect("load latest snapshot")
        .expect("the earlier successful snapshot is still the latest one");
    assert_eq!(
        latest.fetched_at, first_fetched_at,
        "a failed fetch must not persist a new snapshot row"
    );

    database.destroy().await;
}

#[tokio::test]
async fn chain_status_reports_stale_after_ten_minutes_and_reachability_keeps_the_last_good_snapshot(
) {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let graph = synthetic_stargate_graph(&[]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 1));

    let source = FakeWandererChainSource::new(vec![Ok(vec![wormhole(
        1,
        2,
        WandererMassStatus::Normal,
        WandererTimeStatus::Normal,
        WandererShipSizeType::Medium,
    )])]);
    let start = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let clock = VirtualSovClock::new(start);
    let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
        .with_clock(clock.clone());
    collector
        .collect_cycle()
        .await
        .expect("first successful chain cycle");

    assert_eq!(
        dynamic
            .reachable(2, false)
            .expect("route from the first fetch")
            .jumps,
        1
    );
    assert_eq!(dynamic.chain_status(clock.now()), SovChainStatus::Fresh);

    // `chain_status` takes `now` explicitly (no wall-clock read inside
    // `DynamicChainSovReachability`) precisely so this assertion can
    // advance the same virtual clock the fetch used, rather than sleeping
    // for real or reading the real wall clock (ticket 14 lesson: no
    // wall-clock-timing-dependent tests).
    clock.advance(ChronoDuration::minutes(11));
    match dynamic.chain_status(clock.now()) {
        SovChainStatus::Stale { since } => assert_eq!(since, start),
        other => panic!("expected Stale, got {other:?}"),
    }
    // Reachability itself is untouched by a stale status: the last good
    // (recomputed) snapshot keeps serving routes regardless.
    assert_eq!(
        dynamic
            .reachable(2, false)
            .expect("still serving the last good snapshot")
            .jumps,
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn eol_hole_path_risk_appears_in_the_delivered_sov_campaign_embed() {
    let database = TemporaryDatabase::new().await;
    let (sov_store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &sov_store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 11,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    // No stargate edge at all between these two systems: the only route
    // is the wormhole fetched below, so the embed's Path Risk line comes
    // entirely from the chain.
    let graph = synthetic_stargate_graph(&[]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 30_005_174));

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let chain_source = FakeWandererChainSource::new(vec![Ok(vec![wormhole(
        30_005_174,
        30_004_737,
        WandererMassStatus::Depleted,
        WandererTimeStatus::Eol4Hours,
        WandererShipSizeType::Medium,
    )])]);
    let chain_clock = VirtualSovClock::new(observed_at);
    let chain_collector =
        SovChainCollector::new(sov_store.clone(), Arc::new(chain_source), dynamic.clone())
            .with_clock(chain_clock);
    chain_collector
        .collect_cycle()
        .await
        .expect("chain fetch establishes the EOL wormhole route");

    let reachable_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![reachable_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let campaign_clock = VirtualSovClock::new(observed_at);
    let reachability: Arc<dyn SovReachabilitySource> = dynamic;
    let collector = collector_with_reachability(
        sov_store.clone(),
        limiter,
        esi,
        delivery.clone(),
        campaign_clock.clone(),
        reachability,
    );

    collector.collect_cycle().await.expect("baseline cycle");
    campaign_clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    assert_eq!(report.alerts_prepared, 1);
    let sent = delivery.sent();
    assert_eq!(sent.len(), 1);
    let risk_field = sent[0]
        .message
        .fields
        .iter()
        .find(|field| field.name == "Path Risk")
        .expect("embed carries a Path Risk field for a route crossing a wormhole");
    assert_eq!(risk_field.value, "worst hole: EOL <4h, mass <50%");

    database.destroy().await;
}

#[tokio::test]
async fn gate_only_route_has_no_path_risk_line_in_the_delivered_embed() {
    let database = TemporaryDatabase::new().await;
    let (sov_store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &sov_store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 11,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    // A direct stargate edge; the chain fetch this cycle has no
    // connections at all, so the resulting route is pure gates.
    let graph = synthetic_stargate_graph(&[(30_005_174, 30_004_737)]);
    let dynamic = Arc::new(DynamicChainSovReachability::new(graph, 30_005_174));

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let chain_source = FakeWandererChainSource::new(vec![Ok(Vec::new())]);
    let chain_clock = VirtualSovClock::new(observed_at);
    let chain_collector =
        SovChainCollector::new(sov_store.clone(), Arc::new(chain_source), dynamic.clone())
            .with_clock(chain_clock);
    chain_collector
        .collect_cycle()
        .await
        .expect("chain fetch with an empty connection list");

    let reachable_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![reachable_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let campaign_clock = VirtualSovClock::new(observed_at);
    let reachability: Arc<dyn SovReachabilitySource> = dynamic;
    let collector = collector_with_reachability(
        sov_store.clone(),
        limiter,
        esi,
        delivery.clone(),
        campaign_clock.clone(),
        reachability,
    );

    collector.collect_cycle().await.expect("baseline cycle");
    campaign_clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    assert_eq!(report.alerts_prepared, 1);
    let sent = delivery.sent();
    assert_eq!(sent.len(), 1);
    assert!(sent[0]
        .message
        .fields
        .iter()
        .any(|field| field.name == "Reachability"));
    assert!(
        !sent[0]
            .message
            .fields
            .iter()
            .any(|field| field.name == "Path Risk"),
        "a gate-only route must never carry a Path Risk line"
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_subscription_without_reachable_is_unaffected_by_dynamic_chain_reachability() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "defender-only",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![99_006_751],
                watchlist: false,
            }),
        },
        None,
    )
    .await;

    let graph = synthetic_stargate_graph(&[]);
    let dynamic: Arc<dyn SovReachabilitySource> =
        Arc::new(DynamicChainSovReachability::new(graph, 1));

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let unreachable_campaign = campaign(
        1,
        30_009_999,
        99_006_751,
        observed_at + ChronoDuration::hours(2),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![unreachable_campaign.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        dynamic,
    );

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("second cycle");

    // A `Defender`-only subscription never looks at reachability at all;
    // swapping `StaticSovReachability` for `DynamicChainSovReachability`
    // must not change that.
    assert_eq!(report.alerts_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].subscription_name, "defender-only");

    database.destroy().await;
}

// --- Became-reachable transitions (ticket 06) ---
//
// These use `VariableSovReachability`, whose jump count can change between
// `evaluate_stage_cycle` calls, to simulate a chain connection opening,
// closing, and re-opening -- the same seam every other sov feed test
// exercises (a real `SovCollector` against a temporary PostgreSQL
// database), only with reachability driven by the test rather than a
// fixed fake or the real Wanderer chain collector.

#[tokio::test]
async fn unreachable_to_reachable_transition_fires_the_reachable_stage_once_with_route_and_path_risk(
) {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");

    // First stage evaluation: still unreachable. Establishes the baseline
    // state row and never fires (spec: "reachable at first observation
    // never fires").
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    // The chain opens: now reachable in 3 jumps, with a risky wormhole on
    // the way.
    reachability.set_jumps(Some(3));
    reachability.set_path_risk(Some(PathRisk {
        worst_time_status: WandererTimeStatus::Eol4Hours,
        worst_mass_status: WandererMassStatus::Depleted,
    }));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("the false-to-true transition fires once");
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    let sent = delivery.sent();
    assert_eq!(sent[0].subject_id, target_campaign.campaign_id);
    assert_eq!(sent[0].stage, "reachable:1");
    assert_eq!(sent[0].message.footer, "now reachable");
    assert!(
        sent[0]
            .message
            .fields
            .iter()
            .any(|field| field.name == "Reachability"),
        "the reachable embed carries jumps and route, like every other stage"
    );
    assert!(
        sent[0]
            .message
            .fields
            .iter()
            .any(|field| field.name == "Path Risk"),
        "the reachable embed carries Path Risk, like every other stage"
    );

    // A repeated cycle with reachability unchanged never fires again.
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("unchanged reachability does not refire");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn reachable_unreachable_reachable_fires_twice_with_distinct_stage_keys() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");

    // Baseline is already reachable: never fires, per the "reachable at
    // first observation" rule.
    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachable observation");
    assert_eq!(report.reachable_prepared, 0);

    // Unreachable: no fire, but re-arms.
    reachability.set_jumps(None);
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("becomes unreachable");
    assert_eq!(report.reachable_prepared, 0);

    // First observed transition.
    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("first transition fires");
    assert_eq!(report.reachable_prepared, 1);

    // Unreachable again.
    reachability.set_jumps(None);
    collector
        .evaluate_stage_cycle()
        .await
        .expect("becomes unreachable again");

    // Second observed transition: a fresh, distinct stage key.
    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("second transition fires");
    assert_eq!(report.reachable_prepared, 1);

    assert_eq!(delivery.sent_count(), 2);
    let sent = delivery.sent();
    assert_eq!(sent[0].stage, "reachable:1");
    assert_eq!(sent[1].stage, "reachable:2");
    assert_ne!(sent[0].stage, sent[1].stage);
    assert!(sent.iter().all(|d| d.message.footer == "now reachable"));

    database.destroy().await;
}

#[tokio::test]
async fn reachable_at_first_observation_never_fires() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    // Reachable from the very first evaluation -- no prior state row
    // exists yet, so this must never fire.
    reachability.set_jumps(Some(1));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("first observation already reachable");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    let state = store
        .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
        .await
        .expect("lookup reachability state")
        .expect("baseline row was persisted");
    assert!(state.0, "baseline row stores the observed reachable value");
    assert_eq!(state.2, 0, "no transition has been observed yet");

    // The chain-opens-a-door moment still fires normally afterward: this
    // rule is about the first *observation*, not "this leaf never fires
    // for this campaign".
    reachability.set_jumps(None);
    collector
        .evaluate_stage_cycle()
        .await
        .expect("becomes unreachable");
    reachability.set_jumps(Some(1));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("a genuine transition afterward still fires");
    assert_eq!(report.reachable_prepared, 1);

    database.destroy().await;
}

#[tokio::test]
async fn restart_between_prepare_and_send_does_not_double_post_a_reachable_alert() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let failing_delivery = Arc::new(FakeSovDelivery::new(true));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let first_collector = collector_with_reachability(
        store.clone(),
        limiter.clone(),
        esi,
        failing_delivery.clone(),
        clock.clone(),
        reachability_source.clone(),
    );

    first_collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    first_collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation (unreachable)");

    reachability.set_jumps(Some(3));
    let report = first_collector
        .evaluate_stage_cycle()
        .await
        .expect("transition is prepared and its send fails transiently");
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(
        store
            .delivery_status_by_subject(target_campaign.campaign_id, SovAlertStage::Reachable(1))
            .await
            .expect("read delivery status"),
        "prepared"
    );

    // Before the lease expires, a fresh collector (simulating a
    // restarted process) must not re-claim the still-leased delivery.
    let still_leased_esi = FakeSovereigntyEsi::new(vec![]);
    let succeeding_delivery = Arc::new(FakeSovDelivery::new(false));
    let second_collector = collector_with_reachability(
        store.clone(),
        limiter,
        still_leased_esi,
        succeeding_delivery.clone(),
        clock.clone(),
        reachability_source,
    );
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("unexpired lease is left alone");
    assert_eq!(succeeding_delivery.attempt_count(), 0);

    // After the lease expires (restart-safe recovery window), the next
    // collector reclaims the prepared row and sends it exactly once.
    clock.advance(ChronoDuration::minutes(3));
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("expired lease is reclaimed");
    assert_eq!(succeeding_delivery.sent_count(), 1);
    assert_eq!(
        store
            .delivery_status_by_subject(target_campaign.campaign_id, SovAlertStage::Reachable(1))
            .await
            .expect("read delivery status"),
        "sent"
    );
    assert_eq!(
        store
            .count_deliveries_for(target_campaign.campaign_id, SovAlertStage::Reachable(1))
            .await
            .expect("no duplicate delivery row exists"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_subscription_without_a_reachable_leaf_never_gets_reachability_state_or_the_stage() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "defender-only",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![99_006_751],
                watchlist: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("evaluate with reachability unreachable");
    reachability.set_jumps(Some(1));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("evaluate after reachability changes");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert!(
        store
            .reachability_state_for_test(1, 2, "defender-only", target_campaign.campaign_id)
            .await
            .expect("lookup reachability state")
            .is_none(),
        "a subscription with no Reachable leaf must never get a state row"
    );

    database.destroy().await;
}

#[tokio::test]
async fn full_filter_gating_reachable_but_wrong_region_never_fires_the_stage() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "wrong-region",
        SovFilter {
            root: SovFilterNode::And(vec![
                SovFilterNode::Condition(SovFilterCondition::Reachable {
                    max_jumps: 5,
                    allow_frigate_holes: false,
                }),
                // The fixture directory maps 30_004_737 (the campaign
                // system below) to region 10_000_002; this never matches.
                SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_099])),
            ]),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation (unreachable)");

    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("transition is observed but the wrong region gates the alert");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    // The Reachable-leaf-only state still recorded the transition, proven
    // by the persisted sequence having advanced to 1 despite never
    // firing -- a later genuine (correct-region) transition would key
    // `reachable:2`, not re-use `reachable:1`. It also stays *pending*
    // (not announced): the region can never change for this campaign, so
    // this is the "wrong region forever" case fix round finding 2
    // requires stay gated -- not the "wrong region for now" case that
    // finding 2 requires eventually fire.
    let state = store
        .reachability_state_for_test(1, 2, "wrong-region", target_campaign.campaign_id)
        .await
        .expect("lookup reachability state")
        .expect("state row exists");
    assert!(state.0, "the Reachable leaf itself did transition to true");
    assert_eq!(
        state.2, 1,
        "the transition sequence advances even though the full filter never matched"
    );
    assert!(
        !state.3,
        "the announcement stays pending -- never marked announced -- while the region never matches"
    );

    // A further pass confirms it stays pending rather than firing once
    // for free or getting stuck unable to ever fire again.
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("still gated on a later pass");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn an_ended_campaign_never_fires_reachable_and_its_state_is_pruned_with_it() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::minutes(30),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![target_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation establishes the state row");
    assert!(store
        .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
        .await
        .expect("lookup reachability state")
        .is_some());

    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("campaign vanishes from the listing and is marked ended");
    assert!(store
        .campaign_ended_at(target_campaign.campaign_id)
        .await
        .expect("read ended_at")
        .is_some());

    // Pruned alongside its campaign.
    assert!(
        store
            .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
            .await
            .expect("lookup reachability state")
            .is_none(),
        "reachability state is pruned when its campaign is marked ended"
    );

    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("ended campaign is skipped entirely");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn two_subscriptions_with_different_max_jumps_transition_independently_on_the_same_campaign()
{
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "wide-budget",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;
    subscribe(
        &store,
        "narrow-budget",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 2,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline: unreachable for both subscriptions");

    // 3 jumps: within wide-budget's 5, over narrow-budget's 2. Only
    // wide-budget transitions.
    reachability.set_jumps(Some(3));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("wide-budget transitions alone");
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].subscription_name, "wide-budget");
    assert_eq!(delivery.sent()[0].stage, "reachable:1");

    // 10 jumps: over both budgets. wide-budget becomes unreachable again
    // (no fire); narrow-budget stays unreachable (unchanged).
    reachability.set_jumps(Some(10));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("wide-budget becomes unreachable again");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);

    // 1 jump: within both budgets. wide-budget re-fires its second
    // transition; narrow-budget fires its first, independently.
    reachability.set_jumps(Some(1));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("both subscriptions transition on this cycle");
    assert_eq!(report.reachable_prepared, 2);
    assert_eq!(delivery.sent_count(), 3);
    let sent = delivery.sent();
    assert!(
        sent.iter()
            .any(|d| d.subscription_name == "wide-budget" && d.stage == "reachable:2"),
        "wide-budget's second transition keys reachable:2"
    );
    assert!(
        sent.iter()
            .any(|d| d.subscription_name == "narrow-budget" && d.stage == "reachable:1"),
        "narrow-budget's first transition keys reachable:1, independent of wide-budget's sequence"
    );

    database.destroy().await;
}

// --- Fix round tests (ticket 06 review findings 1, 2) ---

#[tokio::test]
async fn graph_unavailable_across_two_passes_leaves_state_untouched_and_nothing_fires() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    reachability.set_graph_available(false);
    // Would be reachable if the graph were available -- proves the outage
    // gate is what suppresses this, not merely "jumps happen to be unset".
    reachability.set_jumps(Some(2));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    let report_one = collector
        .evaluate_stage_cycle()
        .await
        .expect("graph unavailable: first pass");
    assert_eq!(report_one.reachable_prepared, 0);
    let report_two = collector
        .evaluate_stage_cycle()
        .await
        .expect("graph unavailable: second pass");
    assert_eq!(report_two.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert!(
        store
            .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
            .await
            .expect("lookup reachability state")
            .is_none(),
        "no state row is ever created while the graph is unavailable"
    );

    database.destroy().await;
}

#[tokio::test]
async fn graph_returning_after_an_outage_leaves_a_previously_reachable_row_unchanged_with_no_burst()
{
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation (unreachable)");
    // Genuine transition before the outage, announced immediately (a bare
    // Reachable leaf is the whole filter here).
    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("genuine transition before the outage");
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);

    // The graph goes down for several passes: skipped entirely.
    reachability.set_graph_available(false);
    for _ in 0..3 {
        let report = collector
            .evaluate_stage_cycle()
            .await
            .expect("graph unavailable: skipped entirely");
        assert_eq!(report.reachable_prepared, 0);
    }
    assert_eq!(delivery.sent_count(), 1, "no burst while the graph is down");

    // The graph comes back, still reporting the same reachable value as
    // before the outage: this must read as Unchanged, not a fresh
    // transition -- no burst-fire for "everything reachable all along".
    reachability.set_graph_available(true);
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("graph recovers: state was already reachable and stays so");
    assert_eq!(
        report.reachable_prepared, 0,
        "recovery must not re-fire an already-announced reachable row"
    );
    assert_eq!(delivery.sent_count(), 1);
    let state = store
        .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
        .await
        .expect("lookup reachability state")
        .expect("state row exists");
    assert_eq!(state.2, 1, "the sequence did not advance across the outage");

    database.destroy().await;
}

#[tokio::test]
async fn a_campaign_first_seen_during_a_graph_outage_gets_baseline_on_first_available_observation()
{
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    reachability.set_graph_available(false);
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    // The campaign appears while the graph is down.
    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("graph unavailable: no observation recorded");
    assert!(
        store
            .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
            .await
            .expect("lookup reachability state")
            .is_none(),
        "no row exists yet -- the campaign was never observed while the graph was down"
    );

    // The graph comes back, already reachable.
    reachability.set_graph_available(true);
    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("first available observation establishes the baseline");
    assert_eq!(
        report.reachable_prepared, 0,
        "the first observation for this campaign is a baseline, even though the outage delayed it"
    );
    assert_eq!(delivery.sent_count(), 0);
    let state = store
        .reachability_state_for_test(1, 2, "roamers", target_campaign.campaign_id)
        .await
        .expect("lookup reachability state")
        .expect("baseline row now exists");
    assert!(state.0, "the baseline observed reachable = true");
    assert_eq!(state.2, 0, "no transition has been observed yet");

    database.destroy().await;
}

#[tokio::test]
async fn reachable_leaf_true_but_full_filter_not_yet_matching_fires_once_the_window_is_entered_with_the_same_sequence(
) {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "reachable-and-vulnerable-soon",
        SovFilter {
            root: SovFilterNode::And(vec![
                SovFilterNode::Condition(SovFilterCondition::Reachable {
                    max_jumps: 5,
                    allow_frigate_holes: false,
                }),
                SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 12 }),
            ]),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(20),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation (unreachable)");

    // The leaf flips true, but the campaign is still 20h out --
    // VulnerableWithin(12h) does not match yet, so nothing fires.
    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("leaf transitions but the full filter does not match yet");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);
    let state = store
        .reachability_state_for_test(
            1,
            2,
            "reachable-and-vulnerable-soon",
            target_campaign.campaign_id,
        )
        .await
        .expect("lookup reachability state")
        .expect("state row exists");
    assert!(state.0, "the leaf itself is reachable");
    assert_eq!(state.2, 1, "the transition was recorded");
    assert!(!state.3, "the announcement is still pending");

    // The clock advances into the window; a later pass, with the
    // reachability leaf's own value unchanged (still true, so the store
    // reports `Observed` rather than a fresh transition), must still
    // attempt the announcement and fire it now -- keyed by the SAME
    // sequence recorded at the original transition.
    clock.advance(ChronoDuration::hours(9));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("the pending announcement fires once the window is entered");
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].stage, "reachable:1");

    // A further pass does not double-post.
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("already announced");
    assert_eq!(report.reachable_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn unreachable_then_reachable_again_after_announcement_fires_a_new_sequence_once_the_filter_matches(
) {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(
        &store,
        "roamers",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        },
        None,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target_campaign = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let reachability = Arc::new(VariableSovReachability::new(30_004_737));
    let reachability_source: Arc<dyn SovReachabilitySource> = reachability.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline reachability observation (unreachable)");

    reachability.set_jumps(Some(2));
    let report = collector.evaluate_stage_cycle().await.expect(
        "first transition fires and is announced immediately (the full filter is just the leaf)",
    );
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(delivery.sent()[0].stage, "reachable:1");

    reachability.set_jumps(None);
    collector
        .evaluate_stage_cycle()
        .await
        .expect("becomes unreachable");

    reachability.set_jumps(Some(2));
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("second transition fires immediately with a new sequence");
    assert_eq!(report.reachable_prepared, 1);
    assert_eq!(delivery.sent_count(), 2);
    assert_eq!(delivery.sent()[1].stage, "reachable:2");

    database.destroy().await;
}

// --- Vulnerability Window shift into the guild timezone (ticket 07) ---

/// A trimmed six-entry slice of a live `GET /sovereignty/structures/`
/// response, captured against `esi.evetech.net` on 2026-08-27. Observed
/// response headers on the full (2708-entry) listing this was trimmed
/// from: `cache-control: public, max-age=300, must-revalidate,
/// stale-if-error=900`, `x-compatibility-date: 2020-01-01`,
/// `x-ratelimit-group: sovereignty`, `x-ratelimit-limit: 600/15m` (see
/// `src/sov_feed/esi.rs`'s `SovereigntyEsi` trait doc comment for the full
/// five-axis note).
#[tokio::test]
async fn fixture_shaped_structures_deserialize_and_flow_through_a_baseline_cycle() {
    let fixture = std::fs::read_to_string("resources/sov_structures_fixture.json")
        .expect("read live-shape sov structures fixture");
    let structures: Vec<SovStructure> =
        serde_json::from_str(&fixture).expect("fixture matches the ESI wire shape");
    assert!(!structures.is_empty());
    assert!(structures
        .iter()
        .all(|structure| SOV_HUB_STRUCTURE_TYPE_IDS.contains(&structure.structure_type_id)));

    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![Ok(EsiResponse::fresh(
        structures.clone(),
        fresh_metadata(observed_at, "s-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store, limiter, esi, delivery, clock);

    let report = collector
        .collect_structures_cycle()
        .await
        .expect("baseline cycle over live-shape structures fixture");
    assert!(report.baseline_established_this_cycle);
    assert_eq!(report.hubs_observed, structures.len());

    database.destroy().await;
}

#[tokio::test]
async fn collect_structures_cycle_persists_hubs_and_baseline_establishes_silently() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_hub = hub(
        1_000_000_000_001,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(6)),
        Some(observed_at + ChronoDuration::hours(9)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![Ok(EsiResponse::fresh(
        vec![baseline_hub.clone()],
        fresh_metadata(observed_at, "s-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery, clock);

    let report = collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle");
    assert!(report.baseline_established_this_cycle);
    assert_eq!(report.hubs_observed, 1);

    let open = store
        .open_structures()
        .await
        .expect("read persisted structures");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].structure_id, baseline_hub.structure_id);
    assert_eq!(open[0].alliance_id, Some(99_000_001));

    database.destroy().await;
}

#[tokio::test]
async fn collect_structures_cycle_ignores_a_non_hub_structure_type() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![Ok(EsiResponse::fresh(
        vec![non_hub_structure(1_000_000_000_002, 30_005_174)],
        fresh_metadata(observed_at, "s-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery, clock);

    let report = collector
        .collect_structures_cycle()
        .await
        .expect("structures cycle with a non-hub structure");
    assert_eq!(
        report.hubs_observed, 0,
        "a non-hub structure_type_id is never persisted"
    );
    assert!(store
        .open_structures()
        .await
        .expect("read persisted structures")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn tz_window_baseline_establishes_state_silently_regardless_of_overlap() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_tz(
        &store,
        "prime-time",
        all_campaigns_filter(),
        "00:00-04:00",
        true,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_101;
    // Already inside the window at first observation: baseline never
    // fires regardless of the observed value (spec: "Hubs present in the
    // first successful cycle establish state silently").
    let in_window_hub = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![Ok(EsiResponse::fresh(
        vec![in_window_hub],
        fresh_metadata(observed_at, "s-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("structures cycle");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline tz window observation");
    assert_eq!(report.tz_window_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    let state = store
        .tz_window_state_for_test(1, 2, "prime-time", structure_id)
        .await
        .expect("lookup tz window state")
        .expect("baseline row was persisted");
    assert!(state.0, "baseline row stores the observed in_window value");
    assert_eq!(state.2, 0, "no transition has been observed yet");

    database.destroy().await;
}

#[tokio::test]
async fn tz_window_transition_fires_once_and_unchanged_state_does_not_refire() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_tz(
        &store,
        "prime-time",
        all_campaigns_filter(),
        "00:00-04:00",
        true,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_102;
    // Baseline: outside the window (06:00-09:00 never touches 00:00-04:00).
    let outside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(6)),
        Some(observed_at + ChronoDuration::hours(9)),
    );
    // Shifted fully inside the window: >=60 minutes overlap.
    let inside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![
        Ok(EsiResponse::fresh(
            vec![outside_window],
            fresh_metadata(observed_at, "s-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window.clone()],
            fresh_metadata(observed_at, "s-2"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window],
            fresh_metadata(observed_at, "s-3"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline tz window observation (outside window)");
    assert_eq!(delivery.sent_count(), 0);

    collector
        .collect_structures_cycle()
        .await
        .expect("structures cycle with the window shifted in");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("transition into the window fires once");
    assert_eq!(report.tz_window_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].stage, "tz_window_entered:1");
    assert_eq!(
        delivery.sent()[0].message.footer,
        "vuln window entered 00:00\u{2013}04:00"
    );

    // A later pass observing the same in-window state must not re-fire.
    collector
        .collect_structures_cycle()
        .await
        .expect("structures cycle with the window unchanged");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("unchanged in-window state does not re-fire");
    assert_eq!(report.tz_window_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn tz_window_leaving_and_reentering_fires_again_with_a_new_key() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_tz(
        &store,
        "prime-time",
        all_campaigns_filter(),
        "00:00-04:00",
        true,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_103;
    let outside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(6)),
        Some(observed_at + ChronoDuration::hours(9)),
    );
    let inside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![
        Ok(EsiResponse::fresh(
            vec![outside_window.clone()],
            fresh_metadata(observed_at, "s-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window.clone()],
            fresh_metadata(observed_at, "s-2"),
        )),
        Ok(EsiResponse::fresh(
            vec![outside_window],
            fresh_metadata(observed_at, "s-3"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window],
            fresh_metadata(observed_at, "s-4"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline tz window observation");

    collector
        .collect_structures_cycle()
        .await
        .expect("shift into the window");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("first transition fires");
    assert_eq!(report.tz_window_prepared, 1);

    collector
        .collect_structures_cycle()
        .await
        .expect("shift out of the window");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("leaving the window never fires");

    collector
        .collect_structures_cycle()
        .await
        .expect("shift back into the window");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("re-entering the window fires again");
    assert_eq!(report.tz_window_prepared, 1);

    assert_eq!(delivery.sent_count(), 2);
    let sent = delivery.sent();
    assert_eq!(sent[0].stage, "tz_window_entered:1");
    assert_eq!(sent[1].stage, "tz_window_entered:2");
    assert_ne!(sent[0].stage, sent[1].stage);

    database.destroy().await;
}

#[tokio::test]
async fn tz_window_never_fires_when_tz_shift_enabled_is_false() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    // tz_shift_enabled defaults to false when omitted.
    subscribe(&store, "prime-time-disabled", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_104;
    let outside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(6)),
        Some(observed_at + ChronoDuration::hours(9)),
    );
    let inside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![
        Ok(EsiResponse::fresh(
            vec![outside_window],
            fresh_metadata(observed_at, "s-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window],
            fresh_metadata(observed_at, "s-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline observation");
    collector
        .collect_structures_cycle()
        .await
        .expect("shift into the window");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("evaluation with tz_shift_enabled false");
    assert_eq!(report.tz_window_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert!(
        store
            .tz_window_state_for_test(1, 2, "prime-time-disabled", structure_id)
            .await
            .expect("lookup tz window state")
            .is_none(),
        "a subscription without tz_shift_enabled never gets a state row at all"
    );

    database.destroy().await;
}

#[tokio::test]
async fn evaluate_stage_cycle_skips_the_structures_pass_when_no_subscription_enables_tz_shift() {
    // Fix round finding 3: even with hubs persisted, if no subscription
    // opts into `tz_shift_enabled` the tz-window pass records no
    // observations at all (and, behind that assertion, skips the
    // `open_structures()` read entirely). A hub sitting inside the window
    // must therefore leave no `sov_tz_window_state` row.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    // A subscription that never sets tz_shift_enabled (defaults false).
    subscribe(&store, "no-tz-here", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_120;
    let inside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![Ok(EsiResponse::fresh(
        vec![inside_window],
        fresh_metadata(observed_at, "s-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle persists the hub");
    // The hub is genuinely persisted, so the only reason no observation is
    // recorded is the tz-shift gate, not an empty structures table.
    assert_eq!(
        store
            .open_structures()
            .await
            .expect("read persisted structures")
            .len(),
        1
    );

    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("stage cycle with no tz-enabled subscription");
    assert_eq!(report.tz_window_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert!(
        store
            .tz_window_state_for_test(1, 2, "no-tz-here", structure_id)
            .await
            .expect("lookup tz window state")
            .is_none(),
        "no tz-enabled subscription means no observation is recorded"
    );

    database.destroy().await;
}

#[tokio::test]
async fn collect_structures_cycle_treats_an_empty_listing_as_a_no_op() {
    // Fix round finding 4: an empty successful (200) structures body must
    // not remove every hub (which would cascade sov_tz_window_state). It is
    // a no-op with a warn, leaving persisted hubs and tz state intact.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_tz(
        &store,
        "prime-time",
        all_campaigns_filter(),
        "00:00-04:00",
        true,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_121;
    let persisted_hub = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![
        Ok(EsiResponse::fresh(
            vec![persisted_hub],
            fresh_metadata(observed_at, "s-1"),
        )),
        // A later successful poll returns an empty listing.
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "s-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery, clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle persists the hub");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline tz observation writes a state row");
    let state_before = store
        .tz_window_state_for_test(1, 2, "prime-time", structure_id)
        .await
        .expect("lookup tz window state")
        .expect("baseline row was persisted");

    let report = collector
        .collect_structures_cycle()
        .await
        .expect("structures cycle over an empty listing");
    assert!(report.empty_listing, "an empty 200 body is flagged");
    assert_eq!(report.removed, 0, "no hub is removed on an empty listing");

    // The hub survives.
    let open = store
        .open_structures()
        .await
        .expect("read persisted structures");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].structure_id, structure_id);
    // The tz state row survives unchanged.
    let state_after = store
        .tz_window_state_for_test(1, 2, "prime-time", structure_id)
        .await
        .expect("lookup tz window state")
        .expect("tz state row still present after an empty listing");
    assert_eq!(state_after, state_before);

    database.destroy().await;
}

#[tokio::test]
async fn tz_window_is_gated_by_defender_and_region_leaves() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    // Only alerts for hubs owned by this alliance.
    subscribe_with_tz(
        &store,
        "watch-snuffed",
        SovFilter {
            root: SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![99_000_001],
                watchlist: false,
            }),
        },
        "00:00-04:00",
        true,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_105;
    let outside_window = hub(
        structure_id,
        30_005_174,
        Some(99_999_999), // wrong owner
        Some(observed_at + ChronoDuration::hours(6)),
        Some(observed_at + ChronoDuration::hours(9)),
    );
    let inside_window_wrong_owner = hub(
        structure_id,
        30_005_174,
        Some(99_999_999),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let inside_window_right_owner = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![
        Ok(EsiResponse::fresh(
            vec![outside_window],
            fresh_metadata(observed_at, "s-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window_wrong_owner],
            fresh_metadata(observed_at, "s-2"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window_right_owner],
            fresh_metadata(observed_at, "s-3"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery.clone(), clock);

    collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle");
    collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline observation");

    // In-window, but the wrong owner: the leaf's own state transitions,
    // but Defender never matches, so the alert never fires -- and the
    // announcement stays pending rather than lost.
    collector
        .collect_structures_cycle()
        .await
        .expect("shift into the window, wrong owner");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("in-window but wrong defender never fires");
    assert_eq!(report.tz_window_prepared, 0);
    assert_eq!(delivery.sent_count(), 0);

    // Same in-window state, now the right owner: the pending announcement
    // fires without needing a fresh transition.
    collector
        .collect_structures_cycle()
        .await
        .expect("same window, correct owner");
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("pending announcement fires once the defender matches");
    assert_eq!(report.tz_window_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn restart_between_prepare_and_send_does_not_double_post_a_tz_window_alert() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe_with_tz(
        &store,
        "prime-time",
        all_campaigns_filter(),
        "00:00-04:00",
        true,
    )
    .await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let structure_id = 1_000_000_000_106;
    let outside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(6)),
        Some(observed_at + ChronoDuration::hours(9)),
    );
    let inside_window = hub(
        structure_id,
        30_005_174,
        Some(99_000_001),
        Some(observed_at + ChronoDuration::hours(1)),
        Some(observed_at + ChronoDuration::hours(4)),
    );
    let esi = FakeSovereigntyEsi::new(vec![]).with_structures(vec![
        Ok(EsiResponse::fresh(
            vec![outside_window],
            fresh_metadata(observed_at, "s-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![inside_window],
            fresh_metadata(observed_at, "s-2"),
        )),
    ]);
    let failing_delivery = Arc::new(FakeSovDelivery::new(true));
    let clock = VirtualSovClock::new(observed_at);
    let first_collector = collector(
        store.clone(),
        limiter.clone(),
        esi,
        failing_delivery.clone(),
        clock.clone(),
    );

    first_collector
        .collect_structures_cycle()
        .await
        .expect("baseline structures cycle");
    first_collector
        .evaluate_stage_cycle()
        .await
        .expect("baseline tz window observation");

    first_collector
        .collect_structures_cycle()
        .await
        .expect("shift into the window");
    let report = first_collector
        .evaluate_stage_cycle()
        .await
        .expect("transition is prepared and its send fails transiently");
    assert_eq!(report.tz_window_prepared, 1);
    assert_eq!(
        store
            .structure_delivery_status(structure_id, SovAlertStage::TzWindowEntered(1))
            .await
            .expect("read delivery status"),
        "prepared"
    );

    let still_leased_esi = FakeSovereigntyEsi::new(vec![]);
    let succeeding_delivery = Arc::new(FakeSovDelivery::new(false));
    let second_collector = collector(
        store.clone(),
        limiter,
        still_leased_esi,
        succeeding_delivery.clone(),
        clock.clone(),
    );
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("unexpired lease is left alone");
    assert_eq!(succeeding_delivery.attempt_count(), 0);

    clock.advance(ChronoDuration::minutes(3));
    second_collector
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("expired lease is reclaimed");
    assert_eq!(succeeding_delivery.sent_count(), 1);
    assert_eq!(
        store
            .structure_delivery_status(structure_id, SovAlertStage::TzWindowEntered(1))
            .await
            .expect("read delivery status"),
        "sent"
    );
    assert_eq!(
        store
            .count_structure_deliveries_for(structure_id, SovAlertStage::TzWindowEntered(1))
            .await
            .expect("no duplicate delivery row exists"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn sov_map_owner_field_is_omitted_from_an_appeared_embed_when_the_map_has_not_loaded_the_system(
) {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_004_737,
        99_012_982,
        observed_at + ChronoDuration::hours(6),
    );
    let new_campaign = campaign(
        2,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![baseline_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![baseline_campaign, new_campaign],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store, limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("appeared cycle without a loaded sovereignty map");
    assert_eq!(delivery.sent_count(), 1);
    assert!(
        delivery.sent()[0]
            .message
            .fields
            .iter()
            .all(|field| field.name != "System Owner"),
        "the owner field is omitted, not shown as a placeholder, when the map has no row yet"
    );

    database.destroy().await;
}

#[tokio::test]
async fn sov_map_owner_field_appears_in_an_appeared_embed_once_the_map_is_loaded() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let baseline_campaign = campaign(
        1,
        30_004_737,
        99_012_982,
        observed_at + ChronoDuration::hours(6),
    );
    let new_campaign = campaign(
        2,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![baseline_campaign.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![baseline_campaign, new_campaign],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
    ])
    .with_map(vec![Ok(EsiResponse::fresh(
        vec![SovMapEntry {
            solar_system_id: 30_005_174,
            alliance_id: Some(99_003_581),
            corporation_id: Some(98_599_770),
            faction_id: None,
        }],
        fresh_metadata(observed_at, "m-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store, limiter, esi, delivery.clone(), clock.clone());

    collector
        .collect_map_cycle()
        .await
        .expect("map cycle loads the system's owner");
    collector.collect_cycle().await.expect("baseline cycle");
    clock.advance(ChronoDuration::seconds(10));
    collector
        .collect_cycle()
        .await
        .expect("appeared cycle with a loaded sovereignty map");
    assert_eq!(delivery.sent_count(), 1);
    let sent = delivery.sent();
    let owner_field = sent[0]
        .message
        .fields
        .iter()
        .find(|field| field.name == "System Owner")
        .expect("owner field present once the map is loaded");
    // Alliance takes priority over corporation (FakeSovTickerResolver
    // resolves every alliance ID to "TEST").
    assert_eq!(owner_field.value, "TEST");

    database.destroy().await;
}

#[tokio::test]
async fn collect_map_cycle_replaces_the_map_wholesale_each_successful_poll() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi = FakeSovereigntyEsi::new(vec![]).with_map(vec![
        Ok(EsiResponse::fresh(
            vec![
                SovMapEntry {
                    solar_system_id: 30_005_174,
                    alliance_id: Some(99_003_581),
                    corporation_id: None,
                    faction_id: None,
                },
                SovMapEntry {
                    solar_system_id: 30_004_737,
                    alliance_id: None,
                    corporation_id: None,
                    faction_id: Some(500_001),
                },
            ],
            fresh_metadata(observed_at, "m-1"),
        )),
        // Second poll: Turnur lost sov entirely (absent from the fresh
        // listing); Jita's faction changed.
        Ok(EsiResponse::fresh(
            vec![SovMapEntry {
                solar_system_id: 30_004_737,
                alliance_id: None,
                corporation_id: None,
                faction_id: Some(500_002),
            }],
            fresh_metadata(observed_at, "m-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery, clock);

    let report = collector
        .collect_map_cycle()
        .await
        .expect("first map cycle");
    assert_eq!(report.entries_observed, 2);
    assert_eq!(
        store
            .map_owner(30_005_174)
            .await
            .expect("read map owner")
            .expect("Turnur has an owner")
            .alliance_id,
        Some(99_003_581)
    );

    collector
        .collect_map_cycle()
        .await
        .expect("second map cycle");
    assert!(
        store
            .map_owner(30_005_174)
            .await
            .expect("read map owner")
            .is_none(),
        "a system absent from a fresh listing is removed wholesale"
    );
    assert_eq!(
        store
            .map_owner(30_004_737)
            .await
            .expect("read map owner")
            .expect("Jita still has an owner")
            .faction_id,
        Some(500_002)
    );

    database.destroy().await;
}

#[tokio::test]
async fn collect_map_cycle_treats_an_empty_listing_as_a_no_op() {
    // Fix round finding 4: an empty successful (200) map body must not
    // `replace_map` every current owner away. It is a no-op with a warn,
    // leaving existing owners intact.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let esi = FakeSovereigntyEsi::new(vec![]).with_map(vec![
        Ok(EsiResponse::fresh(
            vec![SovMapEntry {
                solar_system_id: 30_005_174,
                alliance_id: Some(99_003_581),
                corporation_id: None,
                faction_id: None,
            }],
            fresh_metadata(observed_at, "m-1"),
        )),
        // A later successful poll returns an empty listing.
        Ok(EsiResponse::fresh(
            vec![],
            fresh_metadata(observed_at, "m-2"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = collector(store.clone(), limiter, esi, delivery, clock);

    let report = collector
        .collect_map_cycle()
        .await
        .expect("first map cycle loads an owner");
    assert_eq!(report.entries_observed, 1);

    let report = collector
        .collect_map_cycle()
        .await
        .expect("second map cycle over an empty listing");
    assert!(report.empty_listing, "an empty 200 body is flagged");

    assert_eq!(
        store
            .map_owner(30_005_174)
            .await
            .expect("read map owner")
            .expect("owner survives an empty listing")
            .alliance_id,
        Some(99_003_581)
    );

    database.destroy().await;
}

// --- Consolidated fix round: chain restart/readiness (ticket 05 blocker /
//     ticket 06 readiness), nonce atomicity (tickets 02/03), lazy render
//     (ticket 03), and the ended-campaign observation guard (ticket 06). ---

fn reachable_filter(max_jumps: i64, allow_frigate_holes: bool) -> SovFilter {
    SovFilter {
        root: SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps,
            allow_frigate_holes,
        }),
    }
}

#[tokio::test]
async fn a_restart_recomputes_the_chain_on_the_first_fetch_even_when_topology_is_unchanged() {
    // Ticket 05 blocker: after a restart the in-memory variants start
    // stargate-only, and the freshly-fetched connections are byte-identical
    // to the snapshot persisted a poll ago, so `topology_changed` is false.
    // The chain must still be served after the first cycle because the first
    // successful fetch of a process forces a recompute regardless.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let edges: &[(i64, i64)] = &[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)];
    let connections = vec![wormhole(
        1,
        6,
        WandererMassStatus::Normal,
        WandererTimeStatus::Normal,
        WandererShipSizeType::Medium,
    )];
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());

    // First process: persist a snapshot carrying the chain shortcut.
    {
        let dynamic = Arc::new(DynamicChainSovReachability::new(
            synthetic_stargate_graph(edges),
            1,
        ));
        let source = FakeWandererChainSource::new(vec![Ok(connections.clone())]);
        let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
            .with_clock(clock.clone());
        collector
            .collect_cycle()
            .await
            .expect("first process persists a snapshot");
        assert_eq!(dynamic.reachable(6, false).unwrap().jumps, 1);
    }

    // Second process (restart): brand-new reachability, stargate-only until
    // it recomputes.
    let dynamic = Arc::new(DynamicChainSovReachability::new(
        synthetic_stargate_graph(edges),
        1,
    ));
    assert_eq!(
        dynamic.reachable(6, false).unwrap().jumps,
        5,
        "stargate-only before the first fetch of this process"
    );
    assert!(!dynamic.has_recomputed());
    let source = FakeWandererChainSource::new(vec![Ok(connections.clone())]);
    let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
        .with_clock(clock.clone());
    let report = collector
        .collect_cycle()
        .await
        .expect("restart first cycle");
    assert!(
        !report.topology_changed,
        "topology is unchanged from the persisted snapshot"
    );
    assert!(
        report.recomputed,
        "the first fetch of the process forces a recompute anyway"
    );
    let info = dynamic
        .reachable(6, false)
        .expect("chain shortcut present after restart");
    assert_eq!(info.jumps, 1);
    assert!(info.via_chain);
    assert!(dynamic.has_recomputed());

    database.destroy().await;
}

#[tokio::test]
async fn seeding_from_the_persisted_snapshot_serves_the_chain_before_any_fetch() {
    // Ticket 05 blocker: the start-up seed (called from `src/lib.rs` before
    // the poll loop) repopulates the in-memory chain from the last persisted
    // snapshot, so a restart during a Wanderer outage still serves the last
    // known chain instead of dropping to stargate-only until the map topology
    // next changes.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let edges: &[(i64, i64)] = &[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)];
    let connections = vec![wormhole(
        1,
        6,
        WandererMassStatus::Normal,
        WandererTimeStatus::Normal,
        WandererShipSizeType::Medium,
    )];
    let clock = VirtualSovClock::new(Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap());

    {
        let dynamic = Arc::new(DynamicChainSovReachability::new(
            synthetic_stargate_graph(edges),
            1,
        ));
        let source = FakeWandererChainSource::new(vec![Ok(connections.clone())]);
        let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
            .with_clock(clock.clone());
        collector.collect_cycle().await.expect("persist snapshot");
    }

    // Restart: seed before any fetch. The empty scripted source proves the
    // seed path issues no fetch of its own.
    let dynamic = Arc::new(DynamicChainSovReachability::new(
        synthetic_stargate_graph(edges),
        1,
    ));
    assert!(!dynamic.has_recomputed());
    let source = FakeWandererChainSource::new(vec![]);
    let collector = SovChainCollector::new(store.clone(), Arc::new(source), dynamic.clone())
        .with_clock(clock.clone());
    let seeded = collector
        .seed_reachability_from_persisted_snapshot()
        .await
        .expect("seed from the persisted snapshot");
    assert!(seeded, "a snapshot exists to seed from");
    assert!(dynamic.has_recomputed());
    let info = dynamic
        .reachable(6, false)
        .expect("chain served immediately after seeding, before any fetch");
    assert_eq!(info.jumps, 1);
    assert!(info.via_chain);
    assert_eq!(
        dynamic.chain_status(clock.now()),
        SovChainStatus::Fresh,
        "staleness is dated from the seeded snapshot's own fetched_at"
    );

    database.destroy().await;
}

#[tokio::test]
async fn evaluate_stage_cycle_records_no_reachability_observation_until_the_chain_is_ready() {
    // Ticket 06 readiness (tied to the ticket 05 blocker): when Wanderer is
    // configured, a `DynamicChainSovReachability` that has not fetched or been
    // seeded yet reports `graph_available() == true` but is not a faithful
    // picture of the live chain. The collector must record NO reachability
    // observation until the chain is ready, so a restart across a Wanderer
    // outage cannot manufacture a false->true transition and burst-fire "now
    // reachable".
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "roamers", reachable_filter(11, false), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let target = campaign(1, 6, 99_006_751, observed_at + ChronoDuration::hours(6));
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![target.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);

    let edges: &[(i64, i64)] = &[(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)];
    let dynamic = Arc::new(DynamicChainSovReachability::new(
        synthetic_stargate_graph(edges),
        1,
    ));
    assert!(!dynamic.has_recomputed());
    let reachability_source: Arc<dyn SovReachabilitySource> = dynamic.clone();
    let collector = collector_with_reachability(
        store.clone(),
        limiter,
        esi,
        delivery.clone(),
        clock.clone(),
        reachability_source,
    );

    collector
        .collect_cycle()
        .await
        .expect("baseline campaign cycle");

    // Chain not ready: no observation recorded at all, not even a baseline
    // row.
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("stage cycle while the chain is not ready");
    assert_eq!(report.reachable_prepared, 0);
    assert!(
        store
            .reachability_state_for_test(1, 2, "roamers", 1)
            .await
            .expect("read reachability state")
            .is_none(),
        "no reachability observation is recorded until the chain is ready"
    );

    // The chain becomes ready (a live fetch/seed would do this): the next
    // pass now records the baseline observation, which itself never fires.
    dynamic.recompute(&[]);
    assert!(dynamic.has_recomputed());
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("stage cycle once the chain is ready");
    assert_eq!(report.reachable_prepared, 0, "baseline never fires");
    let state = store
        .reachability_state_for_test(1, 2, "roamers", 1)
        .await
        .expect("read reachability state")
        .expect("a baseline observation is recorded once the chain is ready");
    assert!(state.0, "system 6 is gate-reachable in 5 jumps (<= 11)");
    assert!(
        state.3,
        "baseline rows are already announced so they never fire"
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_claimed_delivery_always_carries_a_stable_non_empty_nonce() {
    // Tickets 02/03: the delivery nonce is written atomically in the INSERT,
    // and `begin_delivery_attempt` COALESCEs it at claim time, so every
    // claimed delivery carries a stable, non-empty nonce equal to what a
    // restart replay would use -- even if the row is somehow observed
    // nonce-less (the historical race window between INSERT and a follow-up
    // UPDATE).
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let subscription = store
        .subscription(1, 2, "watch-all")
        .await
        .expect("read subscription")
        .expect("subscription exists");
    let message = SovNotificationMessage {
        title: "title".to_string(),
        fields: vec![],
        footer: "Appeared".to_string(),
    };
    let fresh = store
        .prepare_delivery(
            &subscription,
            "campaign",
            1,
            SovAlertStage::Appeared,
            &message,
        )
        .await
        .expect("prepare delivery");
    assert!(fresh);
    let ids = store
        .prepared_delivery_ids()
        .await
        .expect("prepared delivery ids");
    assert_eq!(ids.len(), 1);
    let id = ids[0];
    let expected = format!("sov-{id}");

    // The atomic INSERT already set the nonce (no nonce-less window exists).
    assert_eq!(
        store.delivery_nonce_for_test(id).await.expect("read nonce"),
        Some(expected.clone())
    );

    let t0 = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let claim1 = store
        .begin_delivery_attempt(id, t0)
        .await
        .expect("first claim")
        .expect("delivery claimed");
    assert!(!claim1.nonce.is_empty());
    assert_eq!(claim1.nonce, expected);

    // Reproduce the historical nonce-less window and prove the claim-time
    // COALESCE refills it with the same deterministic value a restart replay
    // would use.
    store
        .clear_delivery_nonce_for_test(id)
        .await
        .expect("clear nonce");
    let t1 = t0 + ChronoDuration::minutes(3); // past the two-minute lease
    let claim2 = store
        .begin_delivery_attempt(id, t1)
        .await
        .expect("second claim")
        .expect("delivery re-claimed after its lease expired");
    assert_eq!(
        claim2.nonce, expected,
        "the same stable non-empty nonce across two claims of the same row"
    );
    assert_eq!(
        store.delivery_nonce_for_test(id).await.expect("read nonce"),
        Some(expected),
        "the nonce is durable after the claim, even after being cleared"
    );

    database.destroy().await;
}

#[tokio::test]
async fn an_already_prepared_tminus_mark_is_not_re_rendered_on_a_later_pass() {
    // Ticket 03: the 30 s stage loop must not re-render the full notification
    // (directory/ticker/owner lookups + a `map_owner` query) for a mark that
    // is already prepared. A second pass over the same due mark performs no
    // render, observed through the directory resolver's call count.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    // 25 minutes out: both the T-120 and T-30 marks are due.
    let due_campaign = campaign(
        1,
        30_005_174,
        99_006_751,
        observed_at + ChronoDuration::minutes(25),
    );
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![due_campaign.clone()],
        fresh_metadata(observed_at, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let directory = Arc::new(FakeSovSystemDirectory::new());
    let collector = SovCollector::new(
        store.clone(),
        Arc::new(esi),
        limiter,
        delivery.clone(),
        directory.clone(),
        Arc::new(FakeSovTickerResolver),
        Arc::new(StaticSovReachability(None)),
    )
    .with_clock(clock.clone());

    collector
        .collect_cycle()
        .await
        .expect("baseline cycle establishes the campaign");

    let before_pass1 = directory.resolve_calls();
    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("first stage pass renders and prepares both marks");
    assert_eq!(report.tminus_prepared, 2);
    let after_pass1 = directory.resolve_calls();
    assert_eq!(
        after_pass1 - before_pass1,
        2,
        "the first pass renders each due mark exactly once"
    );

    let report = collector
        .evaluate_stage_cycle()
        .await
        .expect("second stage pass over already-prepared marks");
    assert_eq!(report.tminus_prepared, 0, "already prepared, nothing new");
    assert_eq!(
        directory.resolve_calls(),
        after_pass1,
        "an already-prepared mark performs no render on a later pass"
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_reachability_observation_for_an_ended_campaign_records_no_orphan_state_row() {
    // Ticket 06 nit: the stage loop reads `open_campaigns()` once then loops;
    // a concurrent `mark_ended` (a different task) can end a campaign in
    // between. A late observation must not re-insert an orphan state row for
    // the now-ended campaign, which `mark_ended` (fires once) would never
    // prune again.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    subscribe(&store, "roamers", reachable_filter(11, false), None).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    let ended = campaign(
        1,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    store
        .upsert_campaign(&ended, false, observed_at)
        .await
        .expect("insert campaign");
    store
        .mark_ended(
            &[ended.campaign_id],
            observed_at + ChronoDuration::minutes(1),
        )
        .await
        .expect("end the campaign");

    // A late observation for the ended campaign persists no row.
    store
        .record_reachability_observation(
            1,
            2,
            "roamers",
            ended.campaign_id,
            true,
            observed_at + ChronoDuration::minutes(2),
        )
        .await
        .expect("observe the ended campaign");
    assert!(
        store
            .reachability_state_for_test(1, 2, "roamers", ended.campaign_id)
            .await
            .expect("read reachability state")
            .is_none(),
        "no orphan reachability-state row for an ended campaign"
    );

    // Sanity: an open campaign still records its baseline row.
    let open = campaign(
        2,
        30_004_737,
        99_006_751,
        observed_at + ChronoDuration::hours(6),
    );
    store
        .upsert_campaign(&open, false, observed_at)
        .await
        .expect("insert open campaign");
    store
        .record_reachability_observation(
            1,
            2,
            "roamers",
            open.campaign_id,
            true,
            observed_at + ChronoDuration::minutes(2),
        )
        .await
        .expect("observe the open campaign");
    assert!(
        store
            .reachability_state_for_test(1, 2, "roamers", open.campaign_id)
            .await
            .expect("read reachability state")
            .is_some(),
        "an open campaign still gets its baseline state row"
    );

    database.destroy().await;
}

#[test]
fn wanderer_connections_fixture_drives_traversability_and_path_risk() {
    // Ticket 05 nit: exercise the real-shape connections fixture through the
    // traversability/Path-Risk path (not just a parse test), so a future
    // wire-shape drift is caught by behaviour. The fixture encodes one
    // critical hole, one EOL hole, and one frigate hole from home (Turnur).
    let raw = std::fs::read_to_string("resources/wanderer_connections_fixture.json")
        .expect("read connections fixture");
    let value: serde_json::Value = serde_json::from_str(&raw).expect("parse fixture json");
    let connections: Vec<WandererConnection> =
        serde_json::from_value(value["data"].clone()).expect("decode connections");

    // Guards the wire shape: the fixture must still carry the critical and
    // EOL holes the behaviour below depends on.
    assert!(
        connections
            .iter()
            .any(|c| c.mass_status == WandererMassStatus::Critical),
        "fixture carries a critical hole"
    );
    assert!(
        connections
            .iter()
            .any(|c| c.time_status != WandererTimeStatus::Normal),
        "fixture carries an EOL hole"
    );
    assert!(
        connections
            .iter()
            .any(|c| c.ship_size_type == WandererShipSizeType::Frigate),
        "fixture carries a frigate hole"
    );

    // Home is Turnur (30002086); no stargate edges among these systems, so
    // all reachability is chain-driven.
    const TURNUR: i64 = 30_002_086;
    const HUB_A: i64 = 31_000_005;
    const HUB_B: i64 = 31_000_006;
    const JITA: i64 = 30_004_737;
    let dynamic = DynamicChainSovReachability::new(synthetic_stargate_graph(&[]), TURNUR);
    dynamic.recompute(&connections);

    // The critical hole (Turnur <-> HUB_A) never yields a route. Without
    // frigate holes HUB_A's only alternative entry (via HUB_B) is gated
    // behind the frigate hole, so HUB_A is unreachable by default.
    assert!(
        dynamic.reachable(HUB_A, false).is_none(),
        "critical hole excluded and the alternative route needs a frigate hole"
    );

    // Jita is reachable via the Gate-type connection in one jump; a Gate is
    // not a wormhole, so there is no Path Risk on that route.
    let jita = dynamic
        .reachable(JITA, false)
        .expect("Jita reachable via the gate-type chain connection");
    assert_eq!(jita.jumps, 1);
    assert!(jita.via_chain);
    assert!(
        jita.path_risk.is_none(),
        "a gate-type connection contributes no wormhole Path Risk"
    );

    // The frigate hole (HUB_B <-> Jita) is excluded by default, so HUB_B is
    // unreachable without frigate holes; allowing them opens a two-jump route
    // (Turnur -gate-> Jita -frigate-> HUB_B) whose only wormhole is that
    // frigate hole (Normal), so Path Risk is present but not EOL.
    assert!(
        dynamic.reachable(HUB_B, false).is_none(),
        "frigate hole excluded by default"
    );
    let hub_b = dynamic
        .reachable(HUB_B, true)
        .expect("HUB_B reachable once frigate holes are allowed");
    assert_eq!(hub_b.jumps, 2);
    assert!(hub_b.via_chain);
    let hub_b_risk = hub_b
        .path_risk
        .expect("the frigate wormhole on the route carries a Path Risk line");
    assert_eq!(hub_b_risk.worst_time_status, WandererTimeStatus::Normal);

    // With frigate holes, HUB_A becomes reachable one hop further on
    // (Turnur -gate-> Jita -frigate-> HUB_B -EOL-> HUB_A), crossing the EOL
    // hole (conn2, time_status 2 == Eol4Hours). Path Risk reports that worst
    // hole -- driven entirely by the parsed fixture.
    let hub_a = dynamic
        .reachable(HUB_A, true)
        .expect("HUB_A reachable via the EOL hole once frigate holes are allowed");
    assert_eq!(hub_a.jumps, 3);
    assert!(hub_a.via_chain);
    let hub_a_risk = hub_a
        .path_risk
        .expect("the EOL hole on the route carries a Path Risk line");
    assert_eq!(
        hub_a_risk.worst_time_status,
        WandererTimeStatus::Eol4Hours,
        "the worst hole on the HUB_A route is the EOL hole"
    );
}

// --- Defender { watchlist } resolution (ticket 08) ---

/// A `SovWatchlistSource` whose watched alliance ids can be changed between
/// collector cycles, simulating a guild adding and later removing an
/// alliance from its watchlist.
struct FakeSovWatchlist {
    guild_id: u64,
    alliance_ids: Mutex<Vec<i64>>,
}

impl FakeSovWatchlist {
    fn new(guild_id: u64, alliance_ids: Vec<i64>) -> Self {
        Self {
            guild_id,
            alliance_ids: Mutex::new(alliance_ids),
        }
    }

    fn set(&self, alliance_ids: Vec<i64>) {
        *self.alliance_ids.lock().unwrap() = alliance_ids;
    }
}

#[async_trait]
impl SovWatchlistSource for FakeSovWatchlist {
    async fn watched_alliance_ids(&self, guild_id: u64) -> Result<Vec<i64>, sqlx::Error> {
        if guild_id == self.guild_id {
            Ok(self.alliance_ids.lock().unwrap().clone())
        } else {
            Ok(Vec::new())
        }
    }
}

fn defender_watchlist_filter() -> SovFilter {
    SovFilter {
        root: SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: Vec::new(),
            watchlist: true,
        }),
    }
}

#[tokio::test]
async fn defender_watchlist_leaf_matches_a_watched_alliance_and_stops_after_removal() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    // Subscriptions in test_sov_feed use guild_id 1, channel_id 2.
    subscribe(
        &store,
        "watchlist-defender",
        defender_watchlist_filter(),
        None,
    )
    .await;

    let watched_alliance = 99_006_751_i64;
    let watchlist = Arc::new(FakeSovWatchlist::new(1, vec![watched_alliance]));

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
    // Baseline campaign, then campaign A (defended by the watched alliance)
    // appears while the alliance is watched, then campaign B (also defended
    // by it) appears after removal.
    let baseline = campaign(
        1,
        30_005_174,
        watched_alliance,
        observed_at + ChronoDuration::hours(6),
    );
    let campaign_a = campaign(
        2,
        30_004_737,
        watched_alliance,
        observed_at + ChronoDuration::hours(3),
    );
    let campaign_b = campaign(
        3,
        30_004_737,
        watched_alliance,
        observed_at + ChronoDuration::hours(3),
    );
    let esi = FakeSovereigntyEsi::new(vec![
        Ok(EsiResponse::fresh(
            vec![baseline.clone()],
            fresh_metadata(observed_at, "etag-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![baseline.clone(), campaign_a.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(10), "etag-2"),
        )),
        Ok(EsiResponse::fresh(
            vec![baseline.clone(), campaign_a.clone(), campaign_b.clone()],
            fresh_metadata(observed_at + ChronoDuration::seconds(20), "etag-3"),
        )),
    ]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(observed_at);
    let collector = SovCollector::new(
        store.clone(),
        Arc::new(esi),
        limiter,
        delivery.clone(),
        Arc::new(FakeSovSystemDirectory::new()),
        Arc::new(FakeSovTickerResolver),
        Arc::new(StaticSovReachability(None)),
    )
    .with_clock(clock.clone())
    .with_watchlist_source(watchlist.clone());

    // Baseline cycle: no alerts.
    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    // Campaign A appears while the alliance is watched -> the watchlist
    // defender leaf resolves to the watched alliance and matches.
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("campaign A cycle");
    assert_eq!(report.alerts_prepared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].subject_id, campaign_a.campaign_id);

    // Remove the alliance from the watchlist; campaign B (defended by the
    // same, now-unwatched, alliance) appears -> the leaf resolves to an
    // empty alliance list and no longer matches.
    watchlist.set(Vec::new());
    clock.advance(ChronoDuration::seconds(10));
    let report = collector.collect_cycle().await.expect("campaign B cycle");
    assert_eq!(report.alerts_prepared, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

// --- Sov retention prune (ticket 16) ---

/// Prepares and immediately sends one `appeared` campaign delivery so its
/// `status` is `'sent'` and `sent_at` is the wall clock now. Retention tests
/// then pass a `now` far in the future to the prune so this row counts as
/// older than the retention window without any clock plumbing.
async fn prepare_and_send_campaign_delivery(
    store: &SovStore,
    subscription: &SovSubscription,
    subject_id: i64,
) {
    let message = SovNotificationMessage {
        title: "title".to_string(),
        fields: vec![],
        footer: "Appeared".to_string(),
    };
    let fresh = store
        .prepare_delivery(
            subscription,
            "campaign",
            subject_id,
            SovAlertStage::Appeared,
            &message,
        )
        .await
        .expect("prepare delivery");
    assert!(fresh);
    let id = *store
        .prepared_delivery_ids()
        .await
        .expect("prepared ids")
        .iter()
        .max()
        .expect("one prepared delivery");
    let claim = store
        .begin_delivery_attempt(id, Utc::now())
        .await
        .expect("claim delivery")
        .expect("delivery claimed");
    store
        .mark_delivery_sent(&claim, "discord-msg")
        .await
        .expect("mark delivery sent");
}

async fn subscription_fixture(store: &SovStore) -> SovSubscription {
    subscribe(store, "watch-all", all_campaigns_filter(), None).await;
    store
        .subscription(1, 2, "watch-all")
        .await
        .expect("read subscription")
        .expect("subscription exists")
}

#[tokio::test]
async fn prune_removes_old_sent_deliveries_whose_campaign_is_gone() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let subscription = subscription_fixture(&store).await;
    // No campaign 777 is ever inserted: the delivery is orphaned.
    prepare_and_send_campaign_delivery(&store, &subscription, 777).await;

    let counts = store
        .prune_expired_sov_data(
            Utc::now() + ChronoDuration::days(200),
            ChronoDuration::days(90),
        )
        .await
        .expect("prune");
    assert_eq!(counts.deliveries_deleted, 1);
    assert_eq!(
        store
            .count_deliveries_for(777, SovAlertStage::Appeared)
            .await
            .expect("count"),
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn prune_keeps_old_sent_deliveries_whose_campaign_is_still_listed() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let subscription = subscription_fixture(&store).await;

    let recent = Utc::now() + ChronoDuration::days(200);
    let listed = campaign(888, 30_005_174, 99_006_751, recent);
    store
        .upsert_campaign(&listed, false, recent)
        .await
        .expect("upsert campaign");
    prepare_and_send_campaign_delivery(&store, &subscription, 888).await;

    let counts = store
        .prune_expired_sov_data(recent, ChronoDuration::days(90))
        .await
        .expect("prune");
    assert_eq!(counts.deliveries_deleted, 0);
    assert_eq!(counts.campaigns_deleted, 0);
    assert_eq!(
        store
            .count_deliveries_for(888, SovAlertStage::Appeared)
            .await
            .expect("count"),
        1,
        "a delivery for a still-listed campaign is never pruned"
    );

    database.destroy().await;
}

#[tokio::test]
async fn prune_never_removes_prepared_deliveries() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let subscription = subscription_fixture(&store).await;
    // Prepared (never sent) and orphaned: still must survive the prune.
    let message = SovNotificationMessage {
        title: "title".to_string(),
        fields: vec![],
        footer: "Appeared".to_string(),
    };
    let fresh = store
        .prepare_delivery(
            &subscription,
            "campaign",
            999,
            SovAlertStage::Appeared,
            &message,
        )
        .await
        .expect("prepare delivery");
    assert!(fresh);

    let counts = store
        .prune_expired_sov_data(
            Utc::now() + ChronoDuration::days(200),
            ChronoDuration::days(90),
        )
        .await
        .expect("prune");
    assert_eq!(counts.deliveries_deleted, 0);
    assert_eq!(
        store
            .count_deliveries_for(999, SovAlertStage::Appeared)
            .await
            .expect("count"),
        1,
        "a prepared delivery is never pruned regardless of age"
    );

    database.destroy().await;
}

#[tokio::test]
async fn prune_never_removes_permanent_failure_deliveries() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    let subscription = subscription_fixture(&store).await;
    let message = SovNotificationMessage {
        title: "title".to_string(),
        fields: vec![],
        footer: "Appeared".to_string(),
    };
    let fresh = store
        .prepare_delivery(
            &subscription,
            "campaign",
            1010,
            SovAlertStage::Appeared,
            &message,
        )
        .await
        .expect("prepare delivery");
    assert!(fresh);
    let id = *store
        .prepared_delivery_ids()
        .await
        .expect("prepared ids")
        .iter()
        .max()
        .expect("one prepared delivery");
    let claim = store
        .begin_delivery_attempt(id, Utc::now())
        .await
        .expect("claim")
        .expect("claimed");
    store
        .record_delivery_failure(
            &claim,
            &SovDeliveryError::permanent("simulated permanent failure"),
            Utc::now(),
        )
        .await
        .expect("record permanent failure");

    let counts = store
        .prune_expired_sov_data(
            Utc::now() + ChronoDuration::days(200),
            ChronoDuration::days(90),
        )
        .await
        .expect("prune");
    assert_eq!(counts.deliveries_deleted, 0);
    assert_eq!(
        store
            .count_deliveries_for(1010, SovAlertStage::Appeared)
            .await
            .expect("count"),
        1,
        "a permanent-failure delivery is never pruned regardless of age"
    );

    database.destroy().await;
}

#[tokio::test]
async fn prune_keeps_campaigns_listed_within_retention_and_drops_older_ones_with_their_state() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch-all", all_campaigns_filter(), None).await;

    let now = Utc::now();
    // Campaign 201 was last listed 89 days ago (inside the window); 202 was
    // last listed 91 days ago (outside it).
    let kept = campaign(201, 30_005_174, 99_006_751, now);
    let dropped = campaign(202, 30_005_174, 99_006_751, now);
    store
        .upsert_campaign(&kept, false, now - ChronoDuration::days(89))
        .await
        .expect("upsert kept campaign");
    store
        .upsert_campaign(&dropped, false, now - ChronoDuration::days(91))
        .await
        .expect("upsert dropped campaign");

    // A reachability-state row for each campaign; the dropped campaign's row
    // must be pruned alongside it, the kept campaign's row must survive.
    store
        .record_reachability_observation(1, 2, "watch-all", 201, true, now)
        .await
        .expect("state for kept campaign");
    store
        .record_reachability_observation(1, 2, "watch-all", 202, true, now)
        .await
        .expect("state for dropped campaign");

    let counts = store
        .prune_expired_sov_data(now, ChronoDuration::days(90))
        .await
        .expect("prune");
    assert_eq!(counts.campaigns_deleted, 1);
    assert_eq!(counts.reachability_state_deleted, 1);

    assert!(store
        .campaign_exists_for_test(201)
        .await
        .expect("kept campaign exists"));
    assert!(!store
        .campaign_exists_for_test(202)
        .await
        .expect("dropped campaign gone"));
    assert!(store
        .reachability_state_for_test(1, 2, "watch-all", 201)
        .await
        .expect("read kept state")
        .is_some());
    assert!(store
        .reachability_state_for_test(1, 2, "watch-all", 202)
        .await
        .expect("read dropped state")
        .is_none());

    database.destroy().await;
}

#[tokio::test]
async fn collect_cycle_triggers_the_retention_prune() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    let subscription = subscription_fixture(&store).await;
    // An old, sent, orphaned delivery that only a prune can remove.
    prepare_and_send_campaign_delivery(&store, &subscription, 777).await;

    // Drive one full collection cycle with a clock 200 days ahead, so the
    // cycle's own `prune_retention(observed_at)` treats the just-sent
    // delivery as older than the 90-day retention window.
    let clock_now = Utc::now() + ChronoDuration::days(200);
    let esi = FakeSovereigntyEsi::new(vec![Ok(EsiResponse::fresh(
        vec![],
        fresh_metadata(clock_now, "etag-1"),
    ))]);
    let delivery = Arc::new(FakeSovDelivery::new(false));
    let clock = VirtualSovClock::new(clock_now);
    let collector = collector(store.clone(), limiter, esi, delivery, clock);
    collector.collect_cycle().await.expect("collect cycle");

    assert_eq!(
        store
            .count_deliveries_for(777, SovAlertStage::Appeared)
            .await
            .expect("count"),
        0,
        "collect_cycle must run the retention prune"
    );

    database.destroy().await;
}
