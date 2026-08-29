//! Collector-cycle tests for the watchlist feed
//! (`.scratch/esi-intel-feeds/issues/08-watchlist-registry-and-corporation-join-leave-alerts.md`).
//!
//! Follows the sov feed's prior art (`tests/test_sov_feed.rs`): a fake
//! `WatchlistEsi` source, a fake resolver and delivery, a controlled clock,
//! and a temporary PostgreSQL database. Pure parsing is exercised without a
//! database in `src/watchlist_feed/model.rs`'s unit tests; these tests
//! exercise the collector cycle end to end: registry round-trip, per-alliance
//! silent baseline, a join and a leave each posting once, dedup across a
//! simulated restart, an alliance ESI error keeping its last snapshot,
//! conditional 304 handling, and limiter coordination through the shared row.

mod common;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use killbot_rust::contract_intelligence::ContractCollectionStore;
use killbot_rust::esi_cache::{CacheMetadata, EsiError, EsiLimiterStore, EsiResponse};
use killbot_rust::watchlist_feed::{
    alliance_resource_key, CorporationInfo, PreparedWatchlistDelivery, WarDetail, WarParty,
    WatchedEntity, WatchlistAlliance, WatchlistClock, WatchlistCollector, WatchlistCorporation,
    WatchlistDelivery, WatchlistDeliveryError, WatchlistEntityResolver, WatchlistEsi,
    WatchlistEventKind, WatchlistKind, WatchlistStore, WatchlistSubscription,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::TemporaryDatabase;

// --- Fakes ---

struct FakeWatchlistEsi {
    responses: Mutex<HashMap<i64, Vec<Result<EsiResponse<Vec<i64>>, EsiError>>>>,
    cursors: Mutex<HashMap<i64, usize>>,
    corp_responses: Mutex<HashMap<i64, Vec<Result<EsiResponse<CorporationInfo>, EsiError>>>>,
    corp_cursors: Mutex<HashMap<i64, usize>>,
    war_list: Mutex<HashMap<Option<i64>, Vec<Result<EsiResponse<Vec<i64>>, EsiError>>>>,
    war_list_cursors: Mutex<HashMap<Option<i64>, usize>>,
    war_details: Mutex<HashMap<i64, Vec<Result<EsiResponse<WarDetail>, EsiError>>>>,
    war_cursors: Mutex<HashMap<i64, usize>>,
    calls: AtomicUsize,
}

impl FakeWatchlistEsi {
    fn new() -> Self {
        Self {
            responses: Mutex::new(HashMap::new()),
            cursors: Mutex::new(HashMap::new()),
            corp_responses: Mutex::new(HashMap::new()),
            corp_cursors: Mutex::new(HashMap::new()),
            war_list: Mutex::new(HashMap::new()),
            war_list_cursors: Mutex::new(HashMap::new()),
            war_details: Mutex::new(HashMap::new()),
            war_cursors: Mutex::new(HashMap::new()),
            calls: AtomicUsize::new(0),
        }
    }

    /// Scripts the war-list responses for one `max_war_id` page key
    /// (`None` is the head page). Consumed in sequence per cycle.
    fn with_war_list_page(
        self,
        max_war_id: Option<i64>,
        responses: Vec<Result<EsiResponse<Vec<i64>>, EsiError>>,
    ) -> Self {
        self.war_list.lock().unwrap().insert(max_war_id, responses);
        self
    }

    fn with_war(
        self,
        war_id: i64,
        responses: Vec<Result<EsiResponse<WarDetail>, EsiError>>,
    ) -> Self {
        self.war_details.lock().unwrap().insert(war_id, responses);
        self
    }

    fn with_alliance(
        self,
        alliance_id: i64,
        responses: Vec<Result<EsiResponse<Vec<i64>>, EsiError>>,
    ) -> Self {
        self.responses
            .lock()
            .unwrap()
            .insert(alliance_id, responses);
        self
    }

    fn with_corporation(
        self,
        corporation_id: i64,
        responses: Vec<Result<EsiResponse<CorporationInfo>, EsiError>>,
    ) -> Self {
        self.corp_responses
            .lock()
            .unwrap()
            .insert(corporation_id, responses);
        self
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// How many times `fetch_war` was invoked for one war id (the per-id
    /// cursor advances on every call), so a test can assert a war was or was
    /// not re-fetched.
    fn war_fetch_count(&self, war_id: i64) -> usize {
        *self.war_cursors.lock().unwrap().get(&war_id).unwrap_or(&0)
    }

    /// How many times `fetch_wars` was invoked for one page key (`None` is the
    /// head; `Some(cursor)` is a downward page), so a test can assert the
    /// baseline paged below the cursor rather than re-reading the head.
    fn war_list_fetch_count(&self, max_war_id: Option<i64>) -> usize {
        *self
            .war_list_cursors
            .lock()
            .unwrap()
            .get(&max_war_id)
            .unwrap_or(&0)
    }
}

#[async_trait]
impl WatchlistEsi for FakeWatchlistEsi {
    async fn fetch_alliance_corporations(
        &self,
        alliance_id: i64,
        _etag: Option<String>,
    ) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let responses = self.responses.lock().unwrap();
        let scripted = responses.get(&alliance_id).unwrap_or_else(|| {
            panic!("no scripted watchlist responses for alliance {alliance_id}")
        });
        assert!(
            !scripted.is_empty(),
            "empty scripted watchlist responses for alliance {alliance_id}"
        );
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors.entry(alliance_id).or_insert(0);
        let index = (*cursor).min(scripted.len() - 1);
        *cursor += 1;
        scripted[index].clone()
    }

    async fn fetch_corporation_info(
        &self,
        corporation_id: i64,
        _etag: Option<String>,
    ) -> Result<EsiResponse<CorporationInfo>, EsiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let responses = self.corp_responses.lock().unwrap();
        // A corporation the test does not script (e.g. an alliance member
        // that a ticket-08 test never cared about) is a benign 304: no
        // snapshot, no events.
        let Some(scripted) = responses.get(&corporation_id) else {
            return Ok(EsiResponse::not_modified(CacheMetadata {
                etag: Some("unscripted".to_string()),
                expires_at: Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()),
                last_modified: None,
                expected_pages: None,
                error_limit_remain: Some(100),
                error_limit_reset: Some(60),
                rate_limit_group: None,
                rate_limit_limit: None,
                rate_limit_remaining: None,
                rate_limit_used: None,
                retry_after: None,
            }));
        };
        assert!(
            !scripted.is_empty(),
            "empty scripted corporation-info responses for corporation {corporation_id}"
        );
        let mut cursors = self.corp_cursors.lock().unwrap();
        let cursor = cursors.entry(corporation_id).or_insert(0);
        let index = (*cursor).min(scripted.len() - 1);
        *cursor += 1;
        scripted[index].clone()
    }

    async fn fetch_wars(
        &self,
        max_war_id: Option<i64>,
        _etag: Option<String>,
    ) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let responses = self.war_list.lock().unwrap();
        // An unscripted page is a benign 304 (real-past freshness so it never
        // short-circuits the collector's freshness gate) -- lets ticket-08/09
        // tests that never script wars run the war pass as a no-op.
        let Some(scripted) = responses.get(&max_war_id) else {
            return Ok(EsiResponse::not_modified(past_metadata("wars-unscripted")));
        };
        let mut cursors = self.war_list_cursors.lock().unwrap();
        let cursor = cursors.entry(max_war_id).or_insert(0);
        let index = (*cursor).min(scripted.len() - 1);
        *cursor += 1;
        scripted[index].clone()
    }

    async fn fetch_war(
        &self,
        war_id: i64,
        _etag: Option<String>,
    ) -> Result<EsiResponse<WarDetail>, EsiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let responses = self.war_details.lock().unwrap();
        let Some(scripted) = responses.get(&war_id) else {
            return Ok(EsiResponse::not_modified(past_metadata("war-unscripted")));
        };
        let mut cursors = self.war_cursors.lock().unwrap();
        let cursor = cursors.entry(war_id).or_insert(0);
        let index = (*cursor).min(scripted.len() - 1);
        *cursor += 1;
        scripted[index].clone()
    }
}

struct FakeResolver;

#[async_trait]
impl WatchlistEntityResolver for FakeResolver {
    async fn corporation(&self, corporation_id: i64) -> WatchlistCorporation {
        WatchlistCorporation {
            name: Some(format!("Corp {corporation_id}")),
            ticker: Some("CORP".to_string()),
        }
    }

    async fn alliance(&self, alliance_id: i64) -> WatchlistAlliance {
        WatchlistAlliance {
            name: Some(format!("Alliance {alliance_id}")),
            ticker: Some("ALLY".to_string()),
        }
    }
}

#[derive(Clone, Copy)]
enum FakeDeliveryBehavior {
    Succeed,
    FailTransient,
}

struct FakeWatchlistDelivery {
    behavior: Mutex<FakeDeliveryBehavior>,
    sent: Mutex<Vec<PreparedWatchlistDelivery>>,
    attempts: Mutex<usize>,
}

impl FakeWatchlistDelivery {
    fn new() -> Self {
        Self {
            behavior: Mutex::new(FakeDeliveryBehavior::Succeed),
            sent: Mutex::new(Vec::new()),
            attempts: Mutex::new(0),
        }
    }

    fn set_behavior(&self, behavior: FakeDeliveryBehavior) {
        *self.behavior.lock().unwrap() = behavior;
    }

    fn sent(&self) -> Vec<PreparedWatchlistDelivery> {
        self.sent.lock().unwrap().clone()
    }

    fn sent_count(&self) -> usize {
        self.sent.lock().unwrap().len()
    }

    fn attempt_count(&self) -> usize {
        *self.attempts.lock().unwrap()
    }
}

#[async_trait]
impl WatchlistDelivery for FakeWatchlistDelivery {
    async fn send(
        &self,
        delivery: PreparedWatchlistDelivery,
    ) -> Result<String, WatchlistDeliveryError> {
        *self.attempts.lock().unwrap() += 1;
        match *self.behavior.lock().unwrap() {
            FakeDeliveryBehavior::Succeed => {
                let message_id = format!("msg-{}", delivery.delivery_id);
                self.sent.lock().unwrap().push(delivery);
                Ok(message_id)
            }
            FakeDeliveryBehavior::FailTransient => Err(WatchlistDeliveryError::transient(
                "simulated transient delivery failure",
            )),
        }
    }
}

struct VirtualWatchlistClock {
    now: Mutex<DateTime<Utc>>,
}

impl VirtualWatchlistClock {
    fn new(now: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new(now),
        })
    }

    fn advance(&self, duration: ChronoDuration) {
        *self.now.lock().unwrap() += duration;
    }
}

impl WatchlistClock for VirtualWatchlistClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

// --- Helpers ---

const GUILD: u64 = 42;
const CHANNEL: u64 = 77;
const ALLIANCE: i64 = 99_004_901;

fn fresh_metadata(observed_at: DateTime<Utc>, etag: &str) -> CacheMetadata {
    CacheMetadata {
        etag: Some(etag.to_string()),
        // The corporation-list route caches for one hour; use a short window
        // so an advance between cycles re-polls.
        expires_at: Some(observed_at + ChronoDuration::seconds(1)),
        last_modified: Some(observed_at),
        expected_pages: None,
        error_limit_remain: Some(100),
        error_limit_reset: Some(60),
        rate_limit_group: None,
        rate_limit_limit: None,
        rate_limit_remaining: None,
        rate_limit_used: None,
        retry_after: None,
    }
}

/// Corporation-info cache metadata whose `expires_at` is fixed far in the
/// real past, so `is_fresh()` (which reads the real `Utc::now()`) is always
/// false and the collector re-polls corporation info every cycle regardless
/// of the virtual clock -- essential for the multi-week member-delta tests.
fn corp_metadata(etag: &str) -> CacheMetadata {
    CacheMetadata {
        etag: Some(etag.to_string()),
        expires_at: Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()),
        last_modified: Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()),
        expected_pages: None,
        error_limit_remain: Some(100),
        error_limit_reset: Some(60),
        rate_limit_group: None,
        rate_limit_limit: None,
        rate_limit_remaining: None,
        rate_limit_used: None,
        retry_after: None,
    }
}

/// Cache metadata whose `expires_at` is fixed in the real past, so
/// `is_fresh()` (which reads the real `Utc::now()`) is always false and the
/// collector re-polls the war list/details every cycle regardless of the
/// virtual clock. Mirrors `corp_metadata`.
fn past_metadata(etag: &str) -> CacheMetadata {
    corp_metadata(etag)
}

// --- Ticket 10: war fixtures ---

const WATCHED_CORP: i64 = 98_825_281;
const WATCHED_AGGRESSOR_ALLY: i64 = 99_002_974;
const WAR_BASELINE_ID: i64 = 762_000;
const WAR_ID: i64 = 762_489;

/// A war whose defender is the watched corporation and aggressor is an
/// alliance, mirroring the live fixture `resources/watchlist_war_762489.json`.
fn watched_war(war_id: i64, allies: Vec<WarParty>, retracted: bool, finished: bool) -> WarDetail {
    WarDetail {
        war_id,
        aggressor: WarParty {
            kind: WatchlistKind::Alliance,
            id: WATCHED_AGGRESSOR_ALLY,
        },
        defender: WarParty {
            kind: WatchlistKind::Corporation,
            id: WATCHED_CORP,
        },
        allies,
        declared: Some(Utc.with_ymd_and_hms(2026, 8, 20, 0, 0, 0).unwrap()),
        started: Some(Utc.with_ymd_and_hms(2026, 8, 21, 0, 0, 0).unwrap()),
        finished: finished.then(|| Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap()),
        retracted: retracted.then(|| Utc.with_ymd_and_hms(2026, 8, 26, 0, 0, 0).unwrap()),
        mutual: false,
        open_for_allies: true,
    }
}

fn war_detail_ok(detail: WarDetail, etag: &str) -> Result<EsiResponse<WarDetail>, EsiError> {
    Ok(EsiResponse::fresh(detail, past_metadata(etag)))
}

fn war_list_ok(ids: Vec<i64>, etag: &str) -> Result<EsiResponse<Vec<i64>>, EsiError> {
    Ok(EsiResponse::fresh(ids, past_metadata(etag)))
}

fn corp_info(
    member_count: i64,
    alliance_id: Option<i64>,
    etag: &str,
) -> Result<EsiResponse<CorporationInfo>, EsiError> {
    Ok(EsiResponse::fresh(
        CorporationInfo {
            member_count,
            alliance_id,
            name: Some("Corp".to_string()),
            ticker: Some("CORP".to_string()),
        },
        corp_metadata(etag),
    ))
}

async fn watch_corporation(store: &WatchlistStore, guild_id: u64, corporation_id: i64) {
    store
        .add_entity(
            &WatchedEntity {
                guild_id,
                kind: WatchlistKind::Corporation,
                entity_id: corporation_id,
                ticker: Some("CORP".to_string()),
                name: Some("Corp".to_string()),
            },
            Some(1),
            Utc::now(),
        )
        .await
        .expect("watch corporation");
}

async fn provisioned_stores(
    database: &TemporaryDatabase,
) -> (WatchlistStore, Arc<ContractCollectionStore>) {
    let limiter_store = ContractCollectionStore::connect(&database.url)
        .await
        .expect("connect contract store to apply migrations");
    let store = WatchlistStore::connect(&database.url)
        .await
        .expect("connect watchlist store");
    (store, Arc::new(limiter_store))
}

fn make_collector(
    store: WatchlistStore,
    limiter: Arc<ContractCollectionStore>,
    esi: Arc<FakeWatchlistEsi>,
    delivery: Arc<FakeWatchlistDelivery>,
    clock: Arc<VirtualWatchlistClock>,
) -> WatchlistCollector {
    WatchlistCollector::new(store, esi, limiter, delivery, Arc::new(FakeResolver)).with_clock(clock)
}

async fn watch_alliance(store: &WatchlistStore, guild_id: u64, alliance_id: i64) {
    store
        .add_entity(
            &WatchedEntity {
                guild_id,
                kind: WatchlistKind::Alliance,
                entity_id: alliance_id,
                ticker: Some("ALLY".to_string()),
                name: Some("Alliance".to_string()),
            },
            Some(1),
            Utc::now(),
        )
        .await
        .expect("watch alliance");
}

async fn subscribe(store: &WatchlistStore, name: &str) {
    let subscription = WatchlistSubscription {
        guild_id: GUILD,
        channel_id: CHANNEL,
        name: name.to_string(),
        event_kinds: killbot_rust::watchlist_feed::WATCHLIST_EVENT_KINDS.to_vec(),
        role_id: None,
        options: serde_json::json!({}),
    };
    store
        .upsert_subscription(&subscription)
        .await
        .expect("subscribe channel");
}

// --- Tests ---

#[tokio::test]
async fn add_remove_list_round_trip() {
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;

    let entity = WatchedEntity {
        guild_id: GUILD,
        kind: WatchlistKind::Alliance,
        entity_id: ALLIANCE,
        ticker: Some("B B C".to_string()),
        name: Some("Snuffed Out".to_string()),
    };
    assert!(store
        .add_entity(&entity, Some(1), Utc::now())
        .await
        .expect("add"));
    // Duplicate add is idempotent (updates, does not error, does not
    // duplicate).
    assert!(!store
        .add_entity(&entity, Some(1), Utc::now())
        .await
        .expect("re-add"));

    let listed = store.list_entities(GUILD).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].entity_id, ALLIANCE);
    assert_eq!(listed[0].name.as_deref(), Some("Snuffed Out"));
    assert_eq!(
        store.watched_alliance_ids(GUILD).await.expect("watched"),
        vec![ALLIANCE]
    );

    assert!(store
        .remove_entity(GUILD, WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("remove"));
    assert!(!store
        .remove_entity(GUILD, WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("re-remove"));
    assert!(store
        .list_entities(GUILD)
        .await
        .expect("list empty")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn first_snapshot_is_a_silent_baseline() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![Ok(EsiResponse::fresh(
            vec![1001, 1002, 1003],
            fresh_metadata(observed_at, "etag-1"),
        ))],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock);

    let report = collector.collect_cycle().await.expect("baseline cycle");
    assert_eq!(report.baselines_established, 1);
    assert_eq!(report.corp_joined, 0);
    assert_eq!(report.corp_left, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(store.delivery_count().await.expect("count"), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_join_and_a_leave_each_post_exactly_once() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            // Baseline: {1001, 1002}.
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at, "etag-1"),
            )),
            // 1003 joins.
            Ok(EsiResponse::fresh(
                vec![1001, 1002, 1003],
                fresh_metadata(observed_at + ChronoDuration::hours(1), "etag-2"),
            )),
            // 1001 leaves.
            Ok(EsiResponse::fresh(
                vec![1002, 1003],
                fresh_metadata(observed_at + ChronoDuration::hours(2), "etag-3"),
            )),
            // Steady state: no change.
            Ok(EsiResponse::fresh(
                vec![1002, 1003],
                fresh_metadata(observed_at + ChronoDuration::hours(3), "etag-4"),
            )),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(
        store.clone(),
        limiter,
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );

    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    clock.advance(ChronoDuration::hours(1));
    let join = collector.collect_cycle().await.expect("join cycle");
    assert_eq!(join.corp_joined, 1);
    assert_eq!(delivery.sent_count(), 1);
    let sent = delivery.sent();
    assert_eq!(sent[0].event_kind, "corp_joined");
    assert_eq!(sent[0].entity_id, ALLIANCE);
    assert!(sent[0].evidence_key.starts_with("1003:"));

    clock.advance(ChronoDuration::hours(1));
    let leave = collector.collect_cycle().await.expect("leave cycle");
    assert_eq!(leave.corp_left, 1);
    assert_eq!(delivery.sent_count(), 2);
    let sent = delivery.sent();
    assert_eq!(sent[1].event_kind, "corp_left");
    assert!(sent[1].evidence_key.starts_with("1001:"));

    // Steady state: no new deliveries.
    clock.advance(ChronoDuration::hours(1));
    let steady = collector.collect_cycle().await.expect("steady cycle");
    assert_eq!(steady.corp_joined, 0);
    assert_eq!(steady.corp_left, 0);
    assert_eq!(delivery.sent_count(), 2);

    database.destroy().await;
}

#[tokio::test]
async fn a_delivery_dedups_across_a_simulated_restart_via_the_lease_path() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            Ok(EsiResponse::fresh(
                vec![1001],
                fresh_metadata(observed_at, "etag-1"),
            )),
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at + ChronoDuration::hours(1), "etag-2"),
            )),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);

    // First process: baseline, then a join whose delivery FAILS transiently
    // (prepared row left with a backed-off lease).
    delivery.set_behavior(FakeDeliveryBehavior::FailTransient);
    let collector = make_collector(
        store.clone(),
        limiter.clone(),
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );
    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    collector.collect_cycle().await.expect("join cycle (fails)");
    assert!(delivery.attempt_count() >= 1);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(store.delivery_count().await.expect("count"), 1);

    // The prepared row carries a stable, deterministic nonce written
    // atomically in the INSERT (fix round finding 5, mirroring the sov
    // restart test): `wl-<id>`, exactly what a restart replay recomputes.
    let prepared = store
        .prepared_delivery_ids()
        .await
        .expect("prepared delivery ids");
    assert_eq!(prepared.len(), 1);
    let delivery_id = prepared[0];
    let expected_nonce = format!("wl-{delivery_id}");
    assert_eq!(
        store
            .delivery_nonce_for_test(delivery_id)
            .await
            .expect("read nonce"),
        Some(expected_nonce.clone())
    );
    // Reproduce the historical nonce-less window; the claim-time COALESCE
    // must refill it with the same deterministic value on re-claim.
    store
        .clear_delivery_nonce_for_test(delivery_id)
        .await
        .expect("clear nonce");

    // "Restart": a brand-new collector and delivery sink against the same
    // database. Advance past the transient lease so the prepared row is
    // reclaimable, then drain it -- it must send exactly once.
    clock.advance(ChronoDuration::minutes(10));
    let delivery2 = Arc::new(FakeWatchlistDelivery::new());
    let collector2 = make_collector(
        store.clone(),
        limiter,
        esi,
        delivery2.clone(),
        clock.clone(),
    );
    collector2
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("drain after restart");
    assert_eq!(delivery2.sent_count(), 1);
    // The re-claimed attempt reused the same stable nonce (COALESCE refilled
    // the cleared column), and it is durable after the claim.
    assert_eq!(delivery2.sent()[0].nonce, expected_nonce);
    assert_eq!(
        store
            .delivery_nonce_for_test(delivery_id)
            .await
            .expect("read nonce"),
        Some(expected_nonce)
    );
    // No duplicate row was ever created.
    assert_eq!(store.delivery_count().await.expect("count"), 1);

    database.destroy().await;
}

#[tokio::test]
async fn an_alliance_esi_error_keeps_the_last_snapshot_and_posts_nothing() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at, "etag-1"),
            )),
            Err(EsiError::retryable("simulated ESI 500", None)),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    // The error is swallowed per-alliance; the cycle succeeds overall.
    let report = collector.collect_cycle().await.expect("error cycle");
    assert_eq!(report.corp_joined, 0);
    assert_eq!(report.corp_left, 0);
    assert_eq!(delivery.sent_count(), 0);

    // Snapshot is intact: a later good listing that re-adds nothing produces
    // no spurious "left" for the corps that were only hidden by the error.
    let snapshot = store
        .alliance_corp_snapshot(ALLIANCE)
        .await
        .expect("snapshot");
    assert_eq!(snapshot.len(), 2);
    assert!(snapshot.values().all(Option::is_none));

    database.destroy().await;
}

#[tokio::test]
async fn a_conditional_304_makes_no_change() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at, "etag-1"),
            )),
            Ok(EsiResponse::not_modified(fresh_metadata(
                observed_at + ChronoDuration::hours(1),
                "etag-1",
            ))),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("304 cycle");
    assert_eq!(report.not_modified, 1);
    assert_eq!(report.corp_joined, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn an_active_shared_limiter_pauses_the_cycle_without_an_esi_call() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    // Record a pause deadline into the shared limiter row through the same
    // trait the collector reads. `active_esi_limiter_deadline` compares the
    // persisted `pause_until` against the database `now()`, so the deadline
    // is anchored far ahead of the real clock (not the virtual cycle clock)
    // to make the pause deterministic and non-flaky.
    let pause_at = Utc::now() + ChronoDuration::hours(24);
    let paused_metadata = CacheMetadata {
        retry_after: Some(pause_at),
        ..CacheMetadata::cached_for_seconds(0)
    };
    limiter
        .record_esi_limiter_at(&paused_metadata, pause_at)
        .await
        .expect("record limiter deadline");

    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![Ok(EsiResponse::fresh(
            vec![1001],
            fresh_metadata(observed_at, "etag-1"),
        ))],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi.clone(), delivery.clone(), clock);

    let report = collector.collect_cycle().await.expect("paused cycle");
    assert!(report.paused_until.is_some());
    assert_eq!(report.alliances_polled, 0);
    assert_eq!(esi.call_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_subscription_only_receives_its_selected_event_kinds() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    // Only corp_left is selected.
    store
        .upsert_subscription(&WatchlistSubscription {
            guild_id: GUILD,
            channel_id: CHANNEL,
            name: "leaves-only".to_string(),
            event_kinds: vec![WatchlistEventKind::CorpLeft],
            role_id: None,
            options: serde_json::json!({}),
        })
        .await
        .expect("subscribe");

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            Ok(EsiResponse::fresh(
                vec![1001],
                fresh_metadata(observed_at, "etag-1"),
            )),
            // 1002 joins -- not selected, so no delivery.
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at + ChronoDuration::hours(1), "etag-2"),
            )),
            // 1001 leaves -- selected.
            Ok(EsiResponse::fresh(
                vec![1002],
                fresh_metadata(observed_at + ChronoDuration::hours(2), "etag-3"),
            )),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    collector.collect_cycle().await.expect("join (ignored)");
    assert_eq!(delivery.sent_count(), 0);
    clock.advance(ChronoDuration::hours(1));
    collector.collect_cycle().await.expect("leave");
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "corp_left");

    database.destroy().await;
}

#[tokio::test]
async fn an_empty_200_is_a_no_op_and_does_not_flood_leaves() {
    // Fix round finding 1: a fresh (non-304) 200 whose corporation array is
    // empty must be treated as a no-op, not diffed. Diffing it against an
    // established baseline would mark every member left (flooding corp_left)
    // and then flood corp_joined on the next real listing.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            // Baseline: {1001, 1002}.
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at, "etag-1"),
            )),
            // A fresh 200 with an EMPTY corporation array -- must be ignored.
            Ok(EsiResponse::fresh(
                vec![],
                fresh_metadata(observed_at + ChronoDuration::hours(1), "etag-2"),
            )),
            // The next real listing is unchanged from the baseline.
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at + ChronoDuration::hours(2), "etag-3"),
            )),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    clock.advance(ChronoDuration::hours(1));
    let empty = collector.collect_cycle().await.expect("empty 200 cycle");
    assert_eq!(empty.empty_listings, 1);
    assert_eq!(empty.corp_left, 0);
    assert_eq!(empty.corp_joined, 0);
    assert_eq!(delivery.sent_count(), 0);
    // Snapshot untouched: both corps are still current members (no left_at).
    let snapshot = store
        .alliance_corp_snapshot(ALLIANCE)
        .await
        .expect("snapshot");
    assert_eq!(snapshot.len(), 2);
    assert!(snapshot.values().all(|left_at| left_at.is_none()));

    clock.advance(ChronoDuration::hours(1));
    let real = collector.collect_cycle().await.expect("real listing cycle");
    assert_eq!(real.empty_listings, 0);
    assert_eq!(real.corp_left, 0);
    assert_eq!(real.corp_joined, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn re_adding_an_unwatched_alliance_starts_a_fresh_silent_baseline() {
    // Fix round finding 4: when an alliance is removed from every guild and
    // later re-added, the stale snapshot/baseline must be cleared so the next
    // cycle is a silent baseline rather than a flood of the membership gap's
    // churn.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            // Baseline while first watched: {1001, 1002}.
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at, "etag-1"),
            )),
            // First listing of the *new* watch: membership changed entirely,
            // but it must be a silent baseline (no diff against the gap).
            Ok(EsiResponse::fresh(
                vec![3003, 3004],
                fresh_metadata(observed_at + ChronoDuration::hours(1), "etag-2"),
            )),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    // Remove from the only guild watching it, then re-add.
    assert!(store
        .remove_entity(GUILD, WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("remove"));
    watch_alliance(&store, GUILD, ALLIANCE).await;
    // The re-add cleared the stale snapshot and baseline marker.
    assert!(store
        .alliance_corp_snapshot(ALLIANCE)
        .await
        .expect("snapshot")
        .is_empty());
    assert!(!store
        .alliance_baseline_established(ALLIANCE)
        .await
        .expect("baseline established"));
    // ... and the alliance's conditional-request metadata, so the next cycle
    // fetches a fresh listing instead of trusting a still-valid ETag/TTL.
    let cached = store
        .cache_metadata(&alliance_resource_key(ALLIANCE))
        .await
        .expect("cache metadata");
    assert!(cached.etag.is_none());
    assert!(cached.expires_at.is_none());

    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("post re-add cycle");
    assert_eq!(report.baselines_established, 1);
    assert_eq!(report.corp_joined, 0);
    assert_eq!(report.corp_left, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn adding_an_alliance_already_watched_by_another_guild_preserves_state() {
    // Fix round finding 4, negative case: a second guild adding an alliance
    // that another guild already watches must NOT reset the snapshot/baseline,
    // so in-flight membership changes still emit events.
    const GUILD_B: u64 = 43;
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_alliance(
        ALLIANCE,
        vec![
            // Baseline: {1001, 1002}.
            Ok(EsiResponse::fresh(
                vec![1001, 1002],
                fresh_metadata(observed_at, "etag-1"),
            )),
            // 1003 joins.
            Ok(EsiResponse::fresh(
                vec![1001, 1002, 1003],
                fresh_metadata(observed_at + ChronoDuration::hours(1), "etag-2"),
            )),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    // A second guild adds the same alliance -- snapshot/baseline preserved.
    watch_alliance(&store, GUILD_B, ALLIANCE).await;
    assert!(store
        .alliance_baseline_established(ALLIANCE)
        .await
        .expect("baseline established"));
    assert_eq!(
        store
            .alliance_corp_snapshot(ALLIANCE)
            .await
            .expect("snapshot")
            .len(),
        2
    );

    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("join cycle");
    assert_eq!(report.corp_joined, 1);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

// --- Ticket 09: member deltas and corporation alliance changes ---

const CORP_A: i64 = 98_077_439;
const CORP_B: i64 = 98_343_297;

/// Alliance-list responses that keep a single member (CORP_A) with a short
/// (real-past) freshness window so the first couple of cycles re-poll.
fn single_member_alliance_responses(
    observed_at: DateTime<Utc>,
) -> Vec<Result<EsiResponse<Vec<i64>>, EsiError>> {
    vec![
        Ok(EsiResponse::fresh(
            vec![CORP_A],
            fresh_metadata(observed_at, "al-1"),
        )),
        Ok(EsiResponse::fresh(
            vec![CORP_A],
            fresh_metadata(observed_at, "al-2"),
        )),
    ]
}

#[tokio::test]
async fn a_first_corporation_snapshot_is_silent() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, CORP_A).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new().with_corporation(CORP_A, vec![corp_info(100, Some(1), "c-1")]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock);

    let report = collector.collect_cycle().await.expect("baseline cycle");
    assert_eq!(report.corp_info_fetched, 1);
    assert_eq!(report.member_delta, 0);
    assert_eq!(report.corp_changed_alliance, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(store.delivery_count().await.expect("count"), 0);
    // The corporation snapshot exists (baseline recorded).
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Corporation, CORP_A)
            .await
            .expect("latest"),
        Some(100)
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_watched_corporation_alliance_change_posts_once() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, CORP_A).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_corporation(
        CORP_A,
        vec![
            corp_info(100, Some(1), "c-1"), // baseline: alliance 1
            corp_info(100, Some(2), "c-2"), // changed to alliance 2 -> event
            corp_info(100, Some(2), "c-3"), // same alliance -> no event
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    clock.advance(ChronoDuration::hours(1));
    let change = collector.collect_cycle().await.expect("change cycle");
    assert_eq!(change.corp_changed_alliance, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "corp_changed_alliance");
    assert_eq!(delivery.sent()[0].entity_id, CORP_A);

    clock.advance(ChronoDuration::hours(1));
    let steady = collector.collect_cycle().await.expect("steady cycle");
    assert_eq!(steady.corp_changed_alliance, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn an_alliance_member_delta_fires_once_then_rearms_and_fires_again() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(ALLIANCE), "c-1"), // C1 baseline: 100
                    corp_info(110, Some(ALLIANCE), "c-2"), // C2 +10% -> fire up
                    corp_info(110, Some(ALLIANCE), "c-3"), // C3 still high -> silent
                    corp_info(100, Some(ALLIANCE), "c-4"), // C4 back inside -> re-arm
                    corp_info(110, Some(ALLIANCE), "c-5"), // C5 +10% again -> fire up
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    // C1 baseline (silent).
    let c1 = collector.collect_cycle().await.expect("baseline");
    assert_eq!(c1.member_delta, 0);
    assert_eq!(delivery.sent_count(), 0);

    // C2: 10% rise across seven days -> fires once.
    clock.advance(ChronoDuration::days(7));
    let c2 = collector.collect_cycle().await.expect("rise cycle");
    assert_eq!(c2.member_delta, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "member_delta");
    assert_eq!(delivery.sent()[0].entity_id, ALLIANCE);

    // C3: still above the band -> silent (already fired this crossing).
    clock.advance(ChronoDuration::hours(1));
    let c3 = collector.collect_cycle().await.expect("still high");
    assert_eq!(c3.member_delta, 0);
    assert_eq!(delivery.sent_count(), 1);

    // C4: returns inside the band -> re-arms, no event.
    clock.advance(ChronoDuration::days(7) - ChronoDuration::hours(1));
    let c4 = collector.collect_cycle().await.expect("re-arm");
    assert_eq!(c4.member_delta, 0);
    assert_eq!(delivery.sent_count(), 1);

    // C5: crosses again against a newer reference -> fires again.
    clock.advance(ChronoDuration::days(7));
    let c5 = collector.collect_cycle().await.expect("second crossing");
    assert_eq!(c5.member_delta, 1);
    assert_eq!(delivery.sent_count(), 2);

    database.destroy().await;
}

#[tokio::test]
async fn an_alliance_member_delta_fires_on_a_drop() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(ALLIANCE), "c-1"),
                    corp_info(90, Some(ALLIANCE), "c-2"), // -10% -> fire down
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::days(7));
    let drop = collector.collect_cycle().await.expect("drop cycle");
    assert_eq!(drop.member_delta, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "member_delta");

    database.destroy().await;
}

#[tokio::test]
async fn fewer_than_seven_days_of_history_posts_no_member_delta() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(ALLIANCE), "c-1"),
                    corp_info(150, Some(ALLIANCE), "c-2"), // huge, but only 3 days later
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::days(3));
    let report = collector
        .collect_cycle()
        .await
        .expect("short-history cycle");
    assert_eq!(report.member_delta, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_corporation_info_error_isolates_other_corporations() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(
                ALLIANCE,
                vec![Ok(EsiResponse::fresh(
                    vec![CORP_A, CORP_B],
                    fresh_metadata(observed_at, "al-1"),
                ))],
            )
            .with_corporation(
                CORP_A,
                vec![Err(EsiError::retryable("simulated corp-info 500", None))],
            )
            .with_corporation(CORP_B, vec![corp_info(200, Some(ALLIANCE), "b-1")]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock);

    let report = collector
        .collect_cycle()
        .await
        .expect("cycle with one corp error");
    assert_eq!(report.corp_info_errors, 1);
    // CORP_B still snapshotted despite CORP_A erroring.
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Corporation, CORP_B)
            .await
            .expect("b latest"),
        Some(200)
    );
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Corporation, CORP_A)
            .await
            .expect("a latest"),
        None
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_conditional_304_on_corporation_info_records_no_snapshot() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, CORP_A).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_corporation(
        CORP_A,
        vec![
            corp_info(100, Some(1), "c-1"),
            Ok(EsiResponse::not_modified(corp_metadata("c-1"))),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("304 cycle");
    assert_eq!(report.corp_info_not_modified, 1);
    assert_eq!(report.corp_info_fetched, 0);
    // Only the one baseline snapshot exists.
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Corporation, CORP_A)
            .await
            .expect("latest"),
        Some(100)
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_member_delta_delivery_dedups_across_a_simulated_restart() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(ALLIANCE), "c-1"),
                    corp_info(110, Some(ALLIANCE), "c-2"),
                    corp_info(110, Some(ALLIANCE), "c-3"),
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);

    // Baseline, then a crossing whose delivery FAILS transiently.
    delivery.set_behavior(FakeDeliveryBehavior::FailTransient);
    let collector = make_collector(
        store.clone(),
        limiter.clone(),
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );
    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::days(7));
    collector.collect_cycle().await.expect("crossing (fails)");
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(store.delivery_count().await.expect("count"), 1);

    // "Restart": a fresh collector + delivery sink, past the transient lease.
    clock.advance(ChronoDuration::minutes(10));
    let delivery2 = Arc::new(FakeWatchlistDelivery::new());
    let collector2 = make_collector(
        store.clone(),
        limiter,
        esi,
        delivery2.clone(),
        clock.clone(),
    );
    collector2
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("drain after restart");
    assert_eq!(delivery2.sent_count(), 1);
    assert_eq!(delivery2.sent()[0].event_kind, "member_delta");
    // No duplicate row, and a further cycle (still above the band) prepares
    // nothing new (band state disarmed + evidence dedup).
    clock.advance(ChronoDuration::hours(1));
    collector2
        .collect_cycle()
        .await
        .expect("post-restart steady cycle");
    assert_eq!(store.delivery_count().await.expect("count"), 1);

    database.destroy().await;
}

#[tokio::test]
async fn a_watched_corporation_added_while_an_alliance_member_ignores_pre_watch_history() {
    // Ticket-09 finding 1(a): a corporation snapshotted for days as a member
    // of a watched alliance, then added as a watched corporation mid-swing,
    // must not fire member_delta or corp_changed_alliance against that
    // pre-watch history on the next cycle -- it fires only once >=7 days of
    // post-add history exist.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    // Watch only the ALLIANCE; CORP_A is snapshotted as its member.
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(777), "c-1"), // T0: pre-watch, alliance 777
                    corp_info(112, Some(777), "c-2"), // T0+8d: mid-swing +12%
                    corp_info(112, Some(888), "c-3"), // post-add: alliance now 888
                    corp_info(130, Some(888), "c-4"), // +7d: big rise vs post-add baseline
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    // Eight days of CORP_A history accrue while it is only an alliance member.
    collector.collect_cycle().await.expect("member baseline");
    clock.advance(ChronoDuration::days(8));
    collector.collect_cycle().await.expect("member mid-swing");

    // Now watch CORP_A directly, mid-swing. The add resets its pre-watch
    // corporation snapshots and baseline.
    watch_corporation(&store, GUILD, CORP_A).await;
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Corporation, CORP_A)
            .await
            .expect("latest"),
        None
    );

    // Next cycle: no corp-level member_delta and no corp_changed_alliance,
    // despite the pre-watch swing and the 777 -> 888 alliance difference.
    clock.advance(ChronoDuration::hours(1));
    collector
        .collect_cycle()
        .await
        .expect("first post-add cycle");
    assert_eq!(
        store
            .deliveries_for_entity(WatchlistEventKind::MemberDelta, CORP_A)
            .await
            .expect("member delta count"),
        0
    );
    assert_eq!(
        store
            .deliveries_for_entity(WatchlistEventKind::CorpChangedAlliance, CORP_A)
            .await
            .expect("alliance change count"),
        0
    );

    // Only once >=7 days of post-add history exist does the corp delta fire.
    clock.advance(ChronoDuration::days(7));
    collector
        .collect_cycle()
        .await
        .expect("post-add matured cycle");
    assert_eq!(
        store
            .deliveries_for_entity(WatchlistEventKind::MemberDelta, CORP_A)
            .await
            .expect("member delta count after maturity"),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn re_adding_an_unwatched_alliance_resets_member_delta_state() {
    // Ticket-09 finding 1(b): remove + re-add an alliance whose band state is
    // disarmed and whose aggregate snapshots are stale -> the member-delta
    // state (snapshots, band, baseline) is reset and the next cycle is silent.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(ALLIANCE), "c-1"),
                    corp_info(130, Some(ALLIANCE), "c-2"), // +30% -> fire, disarm
                    corp_info(130, Some(ALLIANCE), "c-3"), // after re-add
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::days(7));
    collector.collect_cycle().await.expect("fire");
    assert_eq!(delivery.sent_count(), 1);
    // Band is disarmed and stale aggregate snapshots exist.
    assert!(!store
        .member_band_armed(WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("armed"));

    // Remove from the only guild watching it, then re-add.
    assert!(store
        .remove_entity(GUILD, WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("remove"));
    watch_alliance(&store, GUILD, ALLIANCE).await;
    // The re-add reset the member-delta state.
    assert!(store
        .member_band_armed(WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("re-armed"));
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Alliance, ALLIANCE)
            .await
            .expect("alliance snapshot cleared"),
        None
    );
    assert!(store
        .member_baseline(WatchlistKind::Alliance, ALLIANCE)
        .await
        .expect("baseline")
        .is_none());

    // Next cycle is silent (fresh baseline; no seven-day reference).
    clock.advance(ChronoDuration::hours(1));
    let after = collector.collect_cycle().await.expect("post re-add cycle");
    assert_eq!(after.member_delta, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn member_delta_crossings_use_a_monotonic_sequence_key() {
    // Ticket-09 finding 2: the evidence key derives from a monotonic
    // crossing_sequence (not the sliding reference timestamp), so the sequence
    // advances exactly once per crossing and a re-armed second crossing gets a
    // fresh, never-reused key.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(ALLIANCE, single_member_alliance_responses(observed_at))
            .with_corporation(
                CORP_A,
                vec![
                    corp_info(100, Some(ALLIANCE), "c-1"),
                    corp_info(110, Some(ALLIANCE), "c-2"), // fire up (seq 1)
                    corp_info(100, Some(ALLIANCE), "c-3"), // re-arm
                    corp_info(110, Some(ALLIANCE), "c-4"), // fire up again (seq 2)
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::days(7));
    collector.collect_cycle().await.expect("first crossing");
    assert_eq!(
        store
            .band_crossing_sequence_for_test(WatchlistKind::Alliance, ALLIANCE)
            .await
            .expect("seq 1"),
        Some(1)
    );
    assert_eq!(
        delivery.sent()[0].evidence_key,
        format!("alliance:{ALLIANCE}:up:1")
    );

    // Re-arm inside the band: the sequence must not advance.
    clock.advance(ChronoDuration::days(7));
    collector.collect_cycle().await.expect("re-arm");
    assert_eq!(
        store
            .band_crossing_sequence_for_test(WatchlistKind::Alliance, ALLIANCE)
            .await
            .expect("seq unchanged"),
        Some(1)
    );

    // Second crossing -> sequence 2, a distinct key, a second delivery.
    clock.advance(ChronoDuration::days(7));
    collector.collect_cycle().await.expect("second crossing");
    assert_eq!(
        store
            .band_crossing_sequence_for_test(WatchlistKind::Alliance, ALLIANCE)
            .await
            .expect("seq 2"),
        Some(2)
    );
    assert_eq!(delivery.sent_count(), 2);
    assert_eq!(
        delivery.sent()[1].evidence_key,
        format!("alliance:{ALLIANCE}:up:2")
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_persistently_failing_member_is_excluded_after_three_cycles() {
    // Ticket-09 finding 3: one member whose corporation-info fetch fails every
    // cycle blocks the alliance aggregate for two cycles; from the third it is
    // excluded and the aggregate is evaluated without it. A later success
    // resets the counter.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_alliance(&store, GUILD, ALLIANCE).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_alliance(
                ALLIANCE,
                vec![Ok(EsiResponse::fresh(
                    vec![CORP_A, CORP_B],
                    fresh_metadata(observed_at, "al-1"),
                ))],
            )
            .with_corporation(
                CORP_A,
                vec![
                    Err(EsiError::retryable("boom", None)),
                    Err(EsiError::retryable("boom", None)),
                    Err(EsiError::retryable("boom", None)),
                    corp_info(50, Some(ALLIANCE), "a-recover"),
                ],
            )
            .with_corporation(CORP_B, vec![corp_info(200, Some(ALLIANCE), "b-1")]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    // Cycle 1 and 2: CORP_A fails, aggregate blocked (a required member has no
    // snapshot).
    let c1 = collector.collect_cycle().await.expect("cycle 1");
    assert_eq!(c1.alliance_member_counts, 0);
    assert_eq!(
        store
            .corp_fetch_failures_for_test(CORP_A)
            .await
            .expect("f1"),
        Some(1)
    );
    clock.advance(ChronoDuration::hours(1));
    let c2 = collector.collect_cycle().await.expect("cycle 2");
    assert_eq!(c2.alliance_member_counts, 0);
    assert_eq!(
        store
            .corp_fetch_failures_for_test(CORP_A)
            .await
            .expect("f2"),
        Some(2)
    );

    // Cycle 3: CORP_A crosses the exclusion threshold; the aggregate is now
    // evaluated from CORP_B alone and CORP_A is surfaced as excluded.
    clock.advance(ChronoDuration::hours(1));
    let c3 = collector.collect_cycle().await.expect("cycle 3");
    assert_eq!(c3.alliance_member_counts, 1);
    assert_eq!(c3.excluded_members, 1);
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Alliance, ALLIANCE)
            .await
            .expect("aggregate"),
        Some(200)
    );
    assert_eq!(
        store
            .corp_fetch_failures_for_test(CORP_A)
            .await
            .expect("f3"),
        Some(3)
    );

    // Recovery: CORP_A succeeds, its counter resets, and it rejoins the sum.
    clock.advance(ChronoDuration::hours(1));
    let c4 = collector.collect_cycle().await.expect("cycle 4");
    assert_eq!(c4.alliance_member_counts, 1);
    assert_eq!(c4.excluded_members, 0);
    assert_eq!(
        store
            .corp_fetch_failures_for_test(CORP_A)
            .await
            .expect("f4"),
        None
    );
    assert_eq!(
        store
            .latest_member_count(WatchlistKind::Alliance, ALLIANCE)
            .await
            .expect("recovered aggregate"),
        Some(250)
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_subscription_selecting_only_member_delta_ignores_alliance_changes() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, CORP_A).await;
    // Only member_delta is selected; corp_changed_alliance is not.
    store
        .upsert_subscription(&WatchlistSubscription {
            guild_id: GUILD,
            channel_id: CHANNEL,
            name: "deltas-only".to_string(),
            event_kinds: vec![WatchlistEventKind::MemberDelta],
            role_id: None,
            options: serde_json::json!({}),
        })
        .await
        .expect("subscribe");

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_corporation(
        CORP_A,
        vec![
            corp_info(100, Some(1), "c-1"), // baseline
            corp_info(100, Some(2), "c-2"), // alliance change -> NOT selected
            corp_info(200, Some(2), "c-3"), // +100% after 7 days -> member_delta
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");

    // Alliance change: not selected -> no delivery.
    clock.advance(ChronoDuration::hours(1));
    let change = collector
        .collect_cycle()
        .await
        .expect("alliance change cycle");
    assert_eq!(change.corp_changed_alliance, 0);
    assert_eq!(delivery.sent_count(), 0);

    // Member delta after seven days: selected -> delivered.
    clock.advance(ChronoDuration::days(7));
    let delta = collector.collect_cycle().await.expect("member delta cycle");
    assert_eq!(delta.member_delta, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "member_delta");

    database.destroy().await;
}

// --- Ticket 10: war declarations ---

fn party(kind: WatchlistKind, id: i64) -> WarParty {
    WarParty { kind, id }
}

/// A war with explicit aggressor/defender/allies for the involvement tests.
fn war_with(
    war_id: i64,
    aggressor: WarParty,
    defender: WarParty,
    allies: Vec<WarParty>,
    retracted: bool,
    finished: bool,
) -> WarDetail {
    let mut detail = watched_war(war_id, allies, retracted, finished);
    detail.aggressor = aggressor;
    detail.defender = defender;
    detail
}

async fn subscribe_kinds(store: &WatchlistStore, name: &str, kinds: Vec<WatchlistEventKind>) {
    store
        .upsert_subscription(&WatchlistSubscription {
            guild_id: GUILD,
            channel_id: CHANNEL,
            name: name.to_string(),
            event_kinds: kinds,
            role_id: None,
            options: serde_json::json!({}),
        })
        .await
        .expect("subscribe kinds");
}

#[tokio::test]
async fn a_first_war_scan_is_a_silent_baseline() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(None, vec![war_list_ok(vec![WAR_ID], "l0")])
            .with_war(
                WAR_ID,
                vec![war_detail_ok(
                    watched_war(WAR_ID, vec![], false, false),
                    "w1",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock);

    let report = collector.collect_cycle().await.expect("baseline cycle");
    assert!(report.war_baseline_established);
    assert_eq!(report.war_declared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(store.delivery_count().await.expect("count"), 0);
    // The open war is stored (so a later retraction/finish can be tracked) and
    // the high-water mark is set to the scanned head.
    assert!(store.war_is_stored_for_test(WAR_ID).await.expect("stored"));
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(WAR_ID)
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_new_war_with_a_watched_aggressor_posts_war_declared_once() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let aggressor = party(WatchlistKind::Corporation, WATCHED_CORP);
    let defender = party(WatchlistKind::Alliance, 111);
    let detail = war_with(WAR_ID, aggressor, defender, vec![], false, false);

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![WAR_BASELINE_ID], "l0"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l1"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l2"),
                ],
            )
            .with_war(WAR_ID, vec![war_detail_ok(detail, "w1")]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    // Baseline: WAR_BASELINE_ID (unscripted detail -> benign 304, not stored),
    // mark set, nothing posted.
    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    // Declared.
    clock.advance(ChronoDuration::hours(1));
    let declared = collector.collect_cycle().await.expect("declared cycle");
    assert_eq!(declared.war_declared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_declared");
    assert_eq!(delivery.sent()[0].entity_id, WAR_ID);
    assert_eq!(delivery.sent()[0].evidence_key, WAR_ID.to_string());

    // Not again.
    clock.advance(ChronoDuration::hours(1));
    let steady = collector.collect_cycle().await.expect("steady cycle");
    assert_eq!(steady.war_declared, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn a_new_war_with_a_watched_ally_posts_war_declared() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    // Neither aggressor nor defender is watched; the watched corp is an ally.
    let detail = war_with(
        WAR_ID,
        party(WatchlistKind::Alliance, 111),
        party(WatchlistKind::Corporation, 222),
        vec![party(WatchlistKind::Corporation, WATCHED_CORP)],
        false,
        false,
    );

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![WAR_BASELINE_ID], "l0"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l1"),
                ],
            )
            .with_war(WAR_ID, vec![war_detail_ok(detail, "w1")]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let declared = collector.collect_cycle().await.expect("declared cycle");
    assert_eq!(declared.war_declared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_declared");
    assert!(store.war_is_stored_for_test(WAR_ID).await.expect("stored"));

    database.destroy().await;
}

#[tokio::test]
async fn wars_without_a_watched_party_are_stored_but_post_nothing() {
    // Ticket 10 fix-round: every inspected war is now stored (so a late-added
    // entity's pre-existing wars can be re-fetched), but a war with no
    // currently-watched party fans out to no guilds and posts nothing.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let detail = war_with(
        WAR_ID,
        party(WatchlistKind::Alliance, 111),
        party(WatchlistKind::Corporation, 222),
        vec![party(WatchlistKind::Alliance, 333)],
        false,
        false,
    );

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![WAR_BASELINE_ID], "l0"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l1"),
                ],
            )
            .with_war(WAR_ID, vec![war_detail_ok(detail, "w1")]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("scan cycle");
    assert_eq!(report.war_declared, 0);
    assert_eq!(delivery.sent_count(), 0);
    // Stored (for later watch derivation) but no delivery, and the mark
    // advanced past it.
    assert!(store.war_is_stored_for_test(WAR_ID).await.expect("stored"));
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(WAR_ID)
    );

    database.destroy().await;
}

#[tokio::test]
async fn the_high_water_scan_skips_ids_at_or_below_the_mark() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    // War 700 is below the baseline mark (762000); if it were fetched its
    // scripted Err would surface as a war error. It must be skipped.
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![WAR_BASELINE_ID], "l0"),
                    war_list_ok(vec![WAR_BASELINE_ID, 700], "l1"),
                ],
            )
            .with_war(700, vec![Err(EsiError::retryable("must not fetch", None))]),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("skip cycle");
    assert_eq!(report.wars_scanned, 0);
    assert_eq!(report.war_errors, 0);

    database.destroy().await;
}

#[tokio::test]
async fn an_ally_joining_a_stored_war_posts_war_ally_joined_once() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let ally = party(WatchlistKind::Alliance, 99_009_999);
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(None, vec![war_list_ok(vec![WAR_ID], "l0")])
            .with_war(
                WAR_ID,
                vec![
                    war_detail_ok(watched_war(WAR_ID, vec![], false, false), "w1"),
                    war_detail_ok(watched_war(WAR_ID, vec![ally], false, false), "w2"),
                    war_detail_ok(watched_war(WAR_ID, vec![ally], false, false), "w3"),
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    // Baseline stores the open war silently (no same-cycle re-fetch).
    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    // Re-fetch sees the new ally -> one war_ally_joined.
    clock.advance(ChronoDuration::hours(1));
    let joined = collector.collect_cycle().await.expect("ally cycle");
    assert_eq!(joined.war_ally_joined, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_ally_joined");
    assert_eq!(
        delivery.sent()[0].evidence_key,
        format!("{WAR_ID}:ally:alliance:99009999")
    );

    // No re-fire on a later cycle with the same ally set.
    clock.advance(ChronoDuration::hours(1));
    let steady = collector.collect_cycle().await.expect("steady cycle");
    assert_eq!(steady.war_ally_joined, 0);
    assert_eq!(delivery.sent_count(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn a_retraction_then_a_finish_each_post_once() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(None, vec![war_list_ok(vec![WAR_ID], "l0")])
            .with_war(
                WAR_ID,
                vec![
                    war_detail_ok(watched_war(WAR_ID, vec![], false, false), "w1"),
                    // retracted but still open (finished null)
                    war_detail_ok(watched_war(WAR_ID, vec![], true, false), "w2"),
                    // now finished
                    war_detail_ok(watched_war(WAR_ID, vec![], true, true), "w3"),
                    war_detail_ok(watched_war(WAR_ID, vec![], true, true), "w4"),
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    assert_eq!(delivery.sent_count(), 0);

    clock.advance(ChronoDuration::hours(1));
    let retracted = collector.collect_cycle().await.expect("retract cycle");
    assert_eq!(retracted.war_retracted, 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_retracted");

    clock.advance(ChronoDuration::hours(1));
    let finished = collector.collect_cycle().await.expect("finish cycle");
    assert_eq!(finished.war_finished, 1);
    assert_eq!(delivery.sent_count(), 2);
    assert_eq!(delivery.sent()[1].event_kind, "war_finished");

    // Finished wars are no longer re-fetched; nothing more posts.
    clock.advance(ChronoDuration::hours(1));
    let steady = collector.collect_cycle().await.expect("steady cycle");
    assert_eq!(steady.war_retracted, 0);
    assert_eq!(steady.war_finished, 0);
    assert_eq!(delivery.sent_count(), 2);

    database.destroy().await;
}

#[tokio::test]
async fn a_persistently_failing_war_is_skipped_after_three_cycles() {
    // Ticket 10 fix-round (finding 2): a non-404 war id that keeps failing
    // freezes the mark below it for the first two cycles, then is quarantined
    // and skipped on the third so the mark advances past it. Higher ids are
    // processed and posted every cycle regardless of the frozen id.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    // Baseline mark 100; then new ids 201, 202, 203. 202 errors (500).
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![100], "l0"),
                    war_list_ok(vec![203, 202, 201, 100], "l1"),
                ],
            )
            .with_war(
                201,
                vec![war_detail_ok(
                    watched_war(201, vec![], false, false),
                    "w201",
                )],
            )
            .with_war(202, vec![Err(EsiError::retryable("simulated 500", None))])
            .with_war(
                203,
                vec![war_detail_ok(
                    watched_war(203, vec![], false, false),
                    "w203",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");

    // Cycle 1: 201 processed, 202 fails (mark frozen at 201), 203 still
    // processed and posted this cycle.
    clock.advance(ChronoDuration::hours(1));
    let cycle1 = collector.collect_cycle().await.expect("error cycle 1");
    assert_eq!(cycle1.war_errors, 1);
    assert_eq!(cycle1.wars_skipped, 0);
    assert!(store.war_is_stored_for_test(201).await.expect("201 stored"));
    assert!(!store.war_is_stored_for_test(202).await.expect("202 stored"));
    assert!(store.war_is_stored_for_test(203).await.expect("203 stored"));
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(201)
    );
    assert!(delivery
        .sent()
        .iter()
        .any(|d| d.event_kind == "war_declared" && d.entity_id == 203));

    // Cycle 2: 202 fails again, mark still frozen below it.
    clock.advance(ChronoDuration::hours(1));
    let cycle2 = collector.collect_cycle().await.expect("error cycle 2");
    assert_eq!(cycle2.wars_skipped, 0);
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(201)
    );

    // Cycle 3: 202 quarantined and skipped; the mark advances past it to 203.
    clock.advance(ChronoDuration::hours(1));
    let cycle3 = collector.collect_cycle().await.expect("skip cycle");
    assert_eq!(cycle3.wars_skipped, 1);
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(203)
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_not_found_war_is_skipped_immediately_and_higher_ids_process() {
    // Ticket 10 fix-round (finding 2): a 404 is a permanent absence, skipped
    // on the first cycle so the mark advances past it and higher ids post.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![100], "l0"),
                    war_list_ok(vec![202, 201, 100], "l1"),
                ],
            )
            .with_war(201, vec![Err(EsiError::not_found("war 201 gone"))])
            .with_war(
                202,
                vec![war_detail_ok(
                    watched_war(202, vec![], false, false),
                    "w202",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("404 cycle");
    assert_eq!(report.wars_skipped, 1);
    assert!(!store.war_is_stored_for_test(201).await.expect("201 stored"));
    assert!(store.war_is_stored_for_test(202).await.expect("202 stored"));
    assert!(delivery
        .sent()
        .iter()
        .any(|d| d.event_kind == "war_declared" && d.entity_id == 202));
    // The mark advanced past the 404 to the head.
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(202)
    );

    database.destroy().await;
}

#[tokio::test]
async fn the_war_baseline_spans_cycles_and_completes() {
    // Ticket 10 fix-round-2 (finding 1): the silent baseline must inspect every
    // id that existed on the ORIGINAL head page, across multiple cycles bounded
    // by the detail-fetch cap. Because the head drifts every hour (new wars
    // enter at the head and push the oldest ids out of the window), a resume
    // cycle must page DOWNWARD from the cursor rather than re-read the head --
    // otherwise an id at the bottom of the original page rolls off the head
    // before the cursor reaches it and is never inspected. This test models the
    // drift: the head page's cursor pages are scripted per `max_war_id` key and
    // the bottom id (300) is only reachable via a downward page, not the head.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    // Original head page (newest first): head max 700, floor 300 (open watched).
    let original_head = vec![700, 600, 500, 400, 300];
    // By the time the baseline finishes, the head has drifted: 900 (declared
    // during the baseline, above the head max) entered and the oldest ids
    // (400, 300) rolled off the head window entirely.
    let drifted_head = vec![900, 700, 600, 500];
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            // Head page: cycle 1 pins head_max/floor; cycle 4's post-baseline
            // head-scan sees the drifted head (900 above the mark = declared).
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(original_head.clone(), "l0"),
                    war_list_ok(drifted_head.clone(), "l-drift"),
                ],
            )
            // Downward page below cursor 600 (cycle 2): 500, 400, 300 remain
            // reachable even though 400/300 have rolled off the head.
            .with_war_list_page(Some(600), vec![war_list_ok(vec![500, 400, 300], "l-600")])
            // Downward page below cursor 400 (cycle 3): 300 (the floor), plus
            // ids below the original floor (250, 200) that are out of scope.
            .with_war_list_page(Some(400), vec![war_list_ok(vec![300, 250, 200], "l-400")])
            .with_war(
                300,
                vec![war_detail_ok(
                    watched_war(300, vec![], false, false),
                    "w300",
                )],
            )
            .with_war(
                900,
                vec![war_detail_ok(
                    watched_war(900, vec![], false, false),
                    "w900",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    // Lower the detail cap to 2 so the 5-id original page needs three baseline
    // cycles, standing in for the production 2000-id page / 200 cap.
    let collector = make_collector(
        store.clone(),
        limiter,
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    )
    .with_war_detail_cap(2);

    // Cycle 1: baseline reads the head, inspects the newest 2 (700, 600);
    // cursor descends to 600. Not yet established; 300 not yet reached.
    let c1 = collector.collect_cycle().await.expect("baseline cycle 1");
    assert!(!c1.war_baseline_established);
    assert!(c1.war_baseline_pending);
    assert_eq!(delivery.sent_count(), 0);
    assert!(!store.war_is_stored_for_test(300).await.expect("300"));

    // Cycle 2: pages DOWNWARD from cursor 600 (not the head), inspects 500, 400.
    clock.advance(ChronoDuration::hours(1));
    let c2 = collector.collect_cycle().await.expect("baseline cycle 2");
    assert!(!c2.war_baseline_established);
    assert!(c2.war_baseline_pending);

    // Cycle 3: pages downward from cursor 400, inspects the floor (300) and
    // skips the out-of-scope ids below it (250, 200); completes at mark 700.
    clock.advance(ChronoDuration::hours(1));
    let c3 = collector.collect_cycle().await.expect("baseline cycle 3");
    assert!(c3.war_baseline_established);
    assert_eq!(delivery.sent_count(), 0);
    // The bottom-of-page open watched war, rolled off the head, is still stored.
    assert!(store.war_is_stored_for_test(300).await.expect("300 stored"));
    // The out-of-scope ids below the original floor were never inspected.
    assert!(!store.war_is_stored_for_test(250).await.expect("250"));
    assert!(!store.war_is_stored_for_test(200).await.expect("200"));
    assert_eq!(
        store.war_high_water_mark_for_test().await.expect("mark"),
        Some(700)
    );
    // The baseline paged downward instead of re-reading the head every cycle.
    assert!(
        esi.war_list_fetch_count(Some(600)) >= 1,
        "cycle 2 paged below cursor 600"
    );
    assert!(
        esi.war_list_fetch_count(Some(400)) >= 1,
        "cycle 3 paged below cursor 400"
    );
    assert_eq!(
        esi.war_list_fetch_count(None),
        1,
        "the head page is read only on the first baseline cycle"
    );

    // Cycle 4: the war declared during the baseline (900, above the head max)
    // posts once, and the bottom open watched war (300) is re-fetched.
    clock.advance(ChronoDuration::hours(1));
    let c4 = collector
        .collect_cycle()
        .await
        .expect("post-baseline cycle");
    assert_eq!(c4.war_declared, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_declared");
    assert_eq!(delivery.sent()[0].entity_id, 900);
    assert!(
        esi.war_fetch_count(300) >= 2,
        "the bottom open watched war is re-fetched after the baseline"
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_watched_open_war_404ing_on_refetch_is_removed_and_not_refetched() {
    // Ticket 10 fix-round-2 (finding 2): a stored open watched war that starts
    // 404ing (CCP deleted it) must be marked gone on the re-fetch so it drops
    // out of the open-war query -- otherwise its stale last_fetched_at
    // re-selects it FIRST every cycle forever. No event is emitted.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(None, vec![war_list_ok(vec![WAR_ID], "l0")])
            .with_war(
                WAR_ID,
                vec![
                    war_detail_ok(watched_war(WAR_ID, vec![], false, false), "w1"),
                    Err(EsiError::not_found("war gone")),
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(
        store.clone(),
        limiter,
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );

    // Baseline stores the open war silently (fetched once).
    collector.collect_cycle().await.expect("baseline");
    assert_eq!(esi.war_fetch_count(WAR_ID), 1);
    assert!(store.war_is_open_for_test(WAR_ID).await.expect("open"));

    // Cycle 2: the re-fetch 404s -> the war is marked removed (no event).
    clock.advance(ChronoDuration::hours(1));
    let removed = collector.collect_cycle().await.expect("404 cycle");
    assert_eq!(removed.wars_removed, 1);
    assert_eq!(removed.wars_refetched, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(esi.war_fetch_count(WAR_ID), 2);
    assert!(!store.war_is_open_for_test(WAR_ID).await.expect("closed"));

    // Cycle 3: the removed war is no longer selected for re-fetch (its
    // fetch count does not advance) and still posts nothing.
    clock.advance(ChronoDuration::hours(1));
    let steady = collector.collect_cycle().await.expect("steady cycle");
    assert_eq!(steady.wars_removed, 0);
    assert_eq!(esi.war_fetch_count(WAR_ID), 2);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_watched_open_war_500ing_on_refetch_rotates_to_the_back() {
    // Ticket 10 fix-round-2 (finding 2): a stored open watched war that returns
    // a transient 500 on re-fetch must still have its last_fetched_at bumped so
    // it rotates to the BACK of the least-recently-fetched order (retried after
    // other open wars), rather than staying pinned at the front forever.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    // Two open watched wars; 810 fails 500 on re-fetch, 820 succeeds.
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(None, vec![war_list_ok(vec![820, 810], "l0")])
            .with_war(
                810,
                vec![
                    war_detail_ok(watched_war(810, vec![], false, false), "w810"),
                    Err(EsiError::retryable("simulated 500", None)),
                ],
            )
            .with_war(
                820,
                vec![war_detail_ok(
                    watched_war(820, vec![], false, false),
                    "w820",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(
        store.clone(),
        limiter,
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );

    // Baseline stores both open wars (last_fetched == cycle-1 observed_at).
    collector.collect_cycle().await.expect("baseline");
    let base_810 = store
        .war_last_fetched_at_for_test(810)
        .await
        .expect("810")
        .unwrap();
    let base_820 = store
        .war_last_fetched_at_for_test(820)
        .await
        .expect("820")
        .unwrap();
    assert_eq!(base_810, base_820);

    // Cycle 2: 810's re-fetch 500s but its last_fetched_at is still bumped so
    // it rotates to the back (equal to 820's, both advanced past the baseline),
    // rather than remaining stuck at the old timestamp and re-selected first.
    clock.advance(ChronoDuration::hours(1));
    let cycle2 = collector.collect_cycle().await.expect("500 cycle");
    assert_eq!(cycle2.wars_refetch_errors, 1);
    let after_810 = store
        .war_last_fetched_at_for_test(810)
        .await
        .expect("810")
        .unwrap();
    let after_820 = store
        .war_last_fetched_at_for_test(820)
        .await
        .expect("820")
        .unwrap();
    assert!(
        after_810 > base_810,
        "the 500'd war's last_fetched_at advances so it does not re-select first forever"
    );
    assert_eq!(
        after_810, after_820,
        "the 500'd war rotates to the back alongside the successfully re-fetched war"
    );
    // Still open (a transient failure is not a removal).
    assert!(store.war_is_open_for_test(810).await.expect("open"));

    database.destroy().await;
}

#[tokio::test]
async fn a_limiter_pause_during_the_war_baseline_does_not_establish_it() {
    // Ticket 10 fix-round (finding 1): a limiter pause must never flip the
    // baseline flag. With no watched entities the alliance/member passes are
    // no-ops, so the war baseline runs; the war-list fetch records a pause
    // deadline that halts the baseline before any id is inspected.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let pause_at = Utc::now() + ChronoDuration::hours(24);
    let paused_list = Ok(EsiResponse::fresh(
        vec![500, 400, 300],
        CacheMetadata {
            retry_after: Some(pause_at),
            ..CacheMetadata::cached_for_seconds(0)
        },
    ));
    let esi = Arc::new(FakeWatchlistEsi::new().with_war_list_page(None, vec![paused_list]));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock);

    let report = collector
        .collect_cycle()
        .await
        .expect("paused baseline cycle");
    assert!(report.paused_until.is_some());
    assert!(!report.war_baseline_established);
    assert!(
        !store
            .war_baseline_established_for_test()
            .await
            .expect("baseline flag"),
        "a pause must not establish the baseline"
    );

    database.destroy().await;
}

#[tokio::test]
async fn a_late_added_entity_refetches_its_pre_existing_open_war() {
    // Ticket 10 fix-round (finding 3): a war stored while none of its parties
    // was watched is re-fetched once a party is added, tracking ally joins /
    // retraction / finish -- but `war_declared` is never emitted for it (it
    // predates the watch; a silent war baseline for that entity).
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    subscribe(&store, "watch").await;

    let late_corp = 222_222_222;
    let ally = party(WatchlistKind::Alliance, 999_000_111);
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![100], "l0"),
                    war_list_ok(vec![WAR_ID, 100], "l1"),
                    war_list_ok(vec![WAR_ID, 100], "l2"),
                ],
            )
            .with_war(
                WAR_ID,
                vec![
                    // Neither party watched yet; stored silently (no post).
                    war_detail_ok(
                        war_with(
                            WAR_ID,
                            party(WatchlistKind::Alliance, 111),
                            party(WatchlistKind::Corporation, late_corp),
                            vec![],
                            false,
                            false,
                        ),
                        "w1",
                    ),
                    // After the corp is watched, an ally appears on the re-fetch.
                    war_detail_ok(
                        war_with(
                            WAR_ID,
                            party(WatchlistKind::Alliance, 111),
                            party(WatchlistKind::Corporation, late_corp),
                            vec![ally],
                            false,
                            false,
                        ),
                        "w2",
                    ),
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    // Baseline (mark 100).
    collector.collect_cycle().await.expect("baseline");

    // Cycle: the war is stored, but no party is watched -> no delivery.
    clock.advance(ChronoDuration::hours(1));
    let stored = collector
        .collect_cycle()
        .await
        .expect("store unwatched war");
    assert_eq!(stored.war_declared, 0);
    assert_eq!(delivery.sent_count(), 0);
    assert!(store.war_is_stored_for_test(WAR_ID).await.expect("stored"));

    // Now watch the defender corporation.
    watch_corporation(&store, GUILD, late_corp).await;

    // Next cycle re-fetches the now-watched open war and posts the ally join.
    clock.advance(ChronoDuration::hours(1));
    let joined = collector.collect_cycle().await.expect("late-added cycle");
    assert_eq!(joined.war_ally_joined, 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_ally_joined");
    assert!(
        delivery
            .sent()
            .iter()
            .all(|d| d.event_kind != "war_declared"),
        "no war_declared is ever posted for a war that predates the watch"
    );

    database.destroy().await;
}

#[tokio::test]
async fn unwatching_the_only_guild_stops_refetching_the_war() {
    // Ticket 10 fix-round (finding 4): once the only watching entity is
    // removed, the open war is no longer re-fetched.
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(None, vec![war_list_ok(vec![WAR_ID], "l0")])
            .with_war(
                WAR_ID,
                vec![war_detail_ok(
                    watched_war(WAR_ID, vec![], false, false),
                    "w1",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(
        store.clone(),
        limiter,
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );

    // Baseline stores the open watched war (fetched once).
    collector.collect_cycle().await.expect("baseline");
    assert_eq!(esi.war_fetch_count(WAR_ID), 1);

    // Unwatch the only guild watching the war's party.
    store
        .remove_entity(GUILD, WatchlistKind::Corporation, WATCHED_CORP)
        .await
        .expect("remove watch");

    // Next cycle must not re-fetch the war.
    clock.advance(ChronoDuration::hours(1));
    collector.collect_cycle().await.expect("post-unwatch cycle");
    assert_eq!(
        esi.war_fetch_count(WAR_ID),
        1,
        "an unwatched war is no longer re-fetched"
    );

    database.destroy().await;
}

#[tokio::test]
async fn prune_drops_old_finished_wars_but_keeps_open_and_recent() {
    // Ticket 10 fix-round (finding 4 / retention): finished wars older than
    // the 30-day window and below the head mark are pruned; open and
    // recently-finished wars are kept.
    let database = TemporaryDatabase::new().await;
    let (store, _limiter) = provisioned_stores(&database).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
    store
        .record_war_scan_mark(1_000_000, observed_at)
        .await
        .expect("set mark");

    let make = |war_id: i64, finished: Option<DateTime<Utc>>| WarDetail {
        war_id,
        aggressor: WarParty {
            kind: WatchlistKind::Alliance,
            id: 111,
        },
        defender: WarParty {
            kind: WatchlistKind::Corporation,
            id: 222,
        },
        allies: vec![],
        declared: Some(observed_at - ChronoDuration::days(90)),
        started: Some(observed_at - ChronoDuration::days(89)),
        finished,
        retracted: None,
        mutual: false,
        open_for_allies: false,
    };
    // Old finished (61 days), recent finished (3 days), and open.
    store
        .upsert_war(
            &make(100, Some(observed_at - ChronoDuration::days(61))),
            observed_at,
        )
        .await
        .expect("old");
    store
        .upsert_war(
            &make(200, Some(observed_at - ChronoDuration::days(3))),
            observed_at,
        )
        .await
        .expect("recent");
    store
        .upsert_war(&make(300, None), observed_at)
        .await
        .expect("open");

    let pruned = store.prune_finished_wars(observed_at).await.expect("prune");
    assert_eq!(pruned, 1);
    assert!(!store.war_is_stored_for_test(100).await.expect("100"));
    assert!(store.war_is_stored_for_test(200).await.expect("200"));
    assert!(store.war_is_stored_for_test(300).await.expect("300"));

    database.destroy().await;
}

#[tokio::test]
async fn a_conditional_304_on_the_war_list_makes_no_change() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(FakeWatchlistEsi::new().with_war_list_page(
        None,
        vec![
            war_list_ok(vec![WAR_BASELINE_ID], "l0"),
            Ok(EsiResponse::not_modified(past_metadata("l0"))),
        ],
    ));
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    let report = collector.collect_cycle().await.expect("304 cycle");
    assert!(report.war_not_modified >= 1);
    assert_eq!(report.war_declared, 0);
    assert_eq!(delivery.sent_count(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_war_declared_delivery_dedups_across_a_simulated_restart() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    subscribe(&store, "watch").await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![WAR_BASELINE_ID], "l0"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l1"),
                ],
            )
            .with_war(
                WAR_ID,
                vec![war_detail_ok(
                    watched_war(WAR_ID, vec![], false, false),
                    "w1",
                )],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);

    delivery.set_behavior(FakeDeliveryBehavior::FailTransient);
    let collector = make_collector(
        store.clone(),
        limiter.clone(),
        esi.clone(),
        delivery.clone(),
        clock.clone(),
    );
    collector.collect_cycle().await.expect("baseline");
    clock.advance(ChronoDuration::hours(1));
    collector.collect_cycle().await.expect("declared (fails)");
    assert_eq!(delivery.sent_count(), 0);
    assert_eq!(store.delivery_count().await.expect("count"), 1);

    // Restart: fresh collector + delivery sink, past the transient lease.
    clock.advance(ChronoDuration::minutes(10));
    let delivery2 = Arc::new(FakeWatchlistDelivery::new());
    let collector2 = make_collector(
        store.clone(),
        limiter,
        esi,
        delivery2.clone(),
        clock.clone(),
    );
    collector2
        .deliver_claimable_for_test(clock.now())
        .await
        .expect("drain after restart");
    assert_eq!(delivery2.sent_count(), 1);
    assert_eq!(delivery2.sent()[0].event_kind, "war_declared");
    // A further cycle prepares nothing new (dedup on the war id evidence key).
    clock.advance(ChronoDuration::hours(1));
    collector2
        .collect_cycle()
        .await
        .expect("post-restart cycle");
    assert_eq!(store.delivery_count().await.expect("count"), 1);

    database.destroy().await;
}

#[tokio::test]
async fn a_subscription_selecting_only_war_kinds_receives_only_those() {
    let database = TemporaryDatabase::new().await;
    let (store, limiter) = provisioned_stores(&database).await;
    watch_corporation(&store, GUILD, WATCHED_CORP).await;
    // Only war_finished is selected -- war_declared must not deliver.
    subscribe_kinds(&store, "wars-only", vec![WatchlistEventKind::WarFinished]).await;

    let observed_at = Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap();
    let esi = Arc::new(
        FakeWatchlistEsi::new()
            .with_war_list_page(
                None,
                vec![
                    war_list_ok(vec![WAR_BASELINE_ID], "l0"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l1"),
                    war_list_ok(vec![WAR_ID, WAR_BASELINE_ID], "l2"),
                ],
            )
            .with_war(
                WAR_ID,
                vec![
                    war_detail_ok(watched_war(WAR_ID, vec![], false, false), "w1"),
                    war_detail_ok(watched_war(WAR_ID, vec![], false, true), "w2"),
                ],
            ),
    );
    let delivery = Arc::new(FakeWatchlistDelivery::new());
    let clock = VirtualWatchlistClock::new(observed_at);
    let collector = make_collector(store.clone(), limiter, esi, delivery.clone(), clock.clone());

    collector.collect_cycle().await.expect("baseline");

    // Declared -> not selected -> no delivery.
    clock.advance(ChronoDuration::hours(1));
    let declared = collector.collect_cycle().await.expect("declared cycle");
    assert_eq!(declared.war_declared, 0);
    assert_eq!(delivery.sent_count(), 0);

    // Finished -> selected -> delivered.
    clock.advance(ChronoDuration::hours(1));
    let finished = collector.collect_cycle().await.expect("finish cycle");
    assert_eq!(finished.war_finished, 1);
    assert_eq!(delivery.sent_count(), 1);
    assert_eq!(delivery.sent()[0].event_kind, "war_finished");

    database.destroy().await;
}
