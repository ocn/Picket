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
    alliance_resource_key, PreparedWatchlistDelivery, WatchedEntity, WatchlistAlliance,
    WatchlistClock, WatchlistCollector, WatchlistCorporation, WatchlistDelivery,
    WatchlistDeliveryError, WatchlistEntityResolver, WatchlistEsi, WatchlistEventKind,
    WatchlistKind, WatchlistStore, WatchlistSubscription,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::TemporaryDatabase;

// --- Fakes ---

struct FakeWatchlistEsi {
    responses: Mutex<HashMap<i64, Vec<Result<EsiResponse<Vec<i64>>, EsiError>>>>,
    cursors: Mutex<HashMap<i64, usize>>,
    calls: AtomicUsize,
}

impl FakeWatchlistEsi {
    fn new() -> Self {
        Self {
            responses: Mutex::new(HashMap::new()),
            cursors: Mutex::new(HashMap::new()),
            calls: AtomicUsize::new(0),
        }
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

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
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
