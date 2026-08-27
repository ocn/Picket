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
    DynamicChainSovReachability, PreparedSovDelivery, SovAlertStage, SovCampaign,
    SovChainCollector, SovChainStatus, SovClock, SovCollector, SovDelivery, SovDeliveryError,
    SovFilter, SovFilterCondition, SovFilterNode, SovReachabilityInfo, SovReachabilitySource,
    SovStore, SovSubscription, SovSystemDirectory, SovSystemInfo, SovTickerResolver,
    SovereigntyEsi, StargateGraph, StargateGraphFile, StaticSovReachability, WandererChainSource,
    WandererConnection, WandererConnectionType, WandererError, WandererMassStatus,
    WandererShipSizeType, WandererSystem, WandererTimeStatus,
};
use killbot_rust::spawn_sov_collection_loop;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use common::TemporaryDatabase;

// --- Fakes ---

struct FakeSovereigntyEsi {
    responses: Vec<Result<EsiResponse<Vec<SovCampaign>>, EsiError>>,
    cursor: Mutex<usize>,
}

impl FakeSovereigntyEsi {
    fn new(responses: Vec<Result<EsiResponse<Vec<SovCampaign>>, EsiError>>) -> Self {
        Self {
            responses,
            cursor: Mutex::new(0),
        }
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
        Self { systems }
    }
}

impl SovSystemDirectory for FakeSovSystemDirectory {
    fn resolve(&self, solar_system_id: i64) -> Option<SovSystemInfo> {
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
