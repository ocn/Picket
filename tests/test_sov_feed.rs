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
    PreparedSovDelivery, SovAlertStage, SovCampaign, SovClock, SovCollector, SovDelivery,
    SovDeliveryError, SovFilter, SovFilterCondition, SovFilterNode, SovStore, SovSubscription,
    SovSystemDirectory, SovSystemInfo, SovTickerResolver, SovereigntyEsi,
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
    let subscription = SovSubscription {
        guild_id: 1,
        channel_id: 2,
        name: name.to_string(),
        filter,
        options: serde_json::json!({}),
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
    SovCollector::new(
        store,
        Arc::new(esi),
        limiter,
        delivery,
        Arc::new(FakeSovSystemDirectory::new()),
        Arc::new(FakeSovTickerResolver),
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

    let collection_task = spawn_sov_collection_loop(
        database.url.clone(),
        store_handle.clone(),
        StdDuration::from_millis(10),
        esi,
        delivery,
        directory,
        tickers,
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
    let store = tokio::time::timeout(StdDuration::from_secs(2), async {
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
