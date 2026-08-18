use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use killbot_rust::commands::health::{render_health_response, HEALTH_OPERATOR_ID};
use killbot_rust::config::{self, AppConfig, AppState};
use killbot_rust::contract_intelligence::{
    available_contract_store, collection_retry_delay, contract_regional_concurrency_from,
    new_contract_store_handle, spawn_contract_collection_loop_with_notifications,
    AppStateContractPingLimiter, CacheMetadata, CollectionOutcome, ContractCollectionStore,
    ContractCollector, ContractContextLimiter, ContractContextRequirements, ContractContextValue,
    ContractDelivery, ContractDeliveryClock, ContractDeliveryError, ContractEmbedContext,
    ContractEventAction, ContractEventActions, ContractEventKind, ContractFilter,
    ContractFilterCondition, ContractFilterNode, ContractItemDirection, ContractItemProbe,
    ContractLocationContext, ContractObservationContext, ContractPingLimiter, ContractPingType,
    ContractRequestPacer, ContractResolutionState, ContractSubscription, DeliveryFailureKind,
    DeliveryRecord, DeliveryStatus, EsiError, EsiResponse, HealthCheck, HealthClock, HealthCycle,
    HealthRuntimeConfig, HealthStatus, HttpPublicContractEsi, PreparedContractDelivery,
    PublicContract, PublicContractEsi, PublicContractItem, ShipGroupLookup, ShipGroupResolver,
    SolarSystemPosition,
};
use killbot_rust::esi::EsiClient;
use killbot_rust::feed::{FeedError, FeedHealthProvider, FeedHealthTelemetry, KillmailFeed};
use killbot_rust::location_evidence::{
    execute_operator_location_evidence_cli, LocationEvidenceClass, LocationEvidenceService,
    OperatorLocationEvidenceCliResult,
};
use killbot_rust::models::{ZkData, ZkDataNoEsi};
use killbot_rust::pipeline::{run_producer, run_producer_with_health, ProcessedResult};
use moka::future::Cache;
use serde_json::Value;
use sha2::{Digest, Sha384};
use sqlx::migrate::{Migration, MigrationType, Migrator};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Row};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{mpsc, Barrier, Mutex, Notify, Semaphore};
use url::Url;

static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const ESI_PUBLIC_CONTRACT_SUMMARY_FIXTURE: &str =
    include_str!("../resources/contracts/public-contract-summary.json");
const ESI_PUBLIC_CONTRACT_ITEMS_FIXTURE: &str =
    include_str!("../resources/contracts/public-contract-items.json");

fn app_state_for_ping_limiter() -> Arc<AppState> {
    Arc::new(AppState {
        systems: Arc::new(Default::default()),
        ships: Arc::new(Default::default()),
        names: Arc::new(Default::default()),
        tickers: Arc::new(Default::default()),
        group_names: Arc::new(Default::default()),
        subscriptions: Arc::new(Default::default()),
        app_config: Arc::new(AppConfig {
            discord_bot_token: String::new(),
            discord_client_id: 0,
            eve_client_id: String::new(),
            eve_client_secret: String::new(),
            esi_http_timeout_secs: 15,
            killmail_process_timeout_secs: 60,
            redisq_connect_timeout_secs: 10,
            redisq_request_timeout_secs: 60,
            r2z2_connect_timeout_secs: 10,
            r2z2_request_timeout_secs: 15,
            r2z2_poll_interval_secs: 6,
            r2z2_max_consecutive_404s: 10,
            r2z2_resync_timeout_secs: 300,
            killmail_feed_provider: Default::default(),
            killmail_post_process_sleep_ms: 0,
            killmail_workers: 4,
            killmail_queue_size: 512,
        }),
        esi_client: EsiClient::default(),
        celestial_cache: Cache::new(10),
        systems_file_lock: Mutex::new(()),
        ships_file_lock: Mutex::new(()),
        names_file_lock: Mutex::new(()),
        tickers_file_lock: Mutex::new(()),
        group_names_file_lock: Mutex::new(()),
        subscriptions_file_lock: Mutex::new(()),
        last_ping_times: Mutex::new(HashMap::new()),
        user_standings: Arc::new(Default::default()),
        user_standings_file_lock: Mutex::new(()),
        sso_states: Arc::new(Default::default()),
    })
}

struct OneShotHttpServer {
    base_url: String,
    handle: JoinHandle<()>,
}

struct WireReply {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: &'static str,
}

struct SequenceHttpServer {
    base_url: String,
    requests: Arc<StdMutex<Vec<String>>>,
    request_times: Arc<StdMutex<Vec<DateTime<Utc>>>>,
    handle: JoinHandle<()>,
}

impl SequenceHttpServer {
    fn start(replies: Vec<WireReply>) -> Self {
        Self::start_with_request_pacer(replies, None)
    }

    fn start_with_pacer(replies: Vec<WireReply>, pacer: Arc<AdvancingRequestPacer>) -> Self {
        Self::start_with_request_pacer(replies, Some(pacer))
    }

    fn start_with_request_pacer(
        replies: Vec<WireReply>,
        pacer: Option<Arc<AdvancingRequestPacer>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ESI HTTP listener");
        let address = listener.local_addr().expect("fake ESI listener address");
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let recorded_requests = requests.clone();
        let request_times = Arc::new(StdMutex::new(Vec::new()));
        let recorded_request_times = request_times.clone();
        let handle = std::thread::spawn(move || {
            for reply in replies {
                let (mut stream, _) = listener.accept().expect("accept fake ESI HTTP request");
                let mut request = [0_u8; 4096];
                let length = stream
                    .read(&mut request)
                    .expect("read fake ESI HTTP request");
                recorded_requests
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request[..length]).into_owned());
                if let Some(pacer) = &pacer {
                    recorded_request_times.lock().unwrap().push(pacer.now());
                }
                let response = format!(
                    "HTTP/1.1 {} test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                    reply.status,
                    reply.body.len(),
                    reply
                        .headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .collect::<String>(),
                    reply.body,
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write fake ESI HTTP response");
            }
        });
        Self {
            base_url: format!("http://{address}/"),
            requests,
            request_times,
            handle,
        }
    }

    fn finish(self) {
        self.handle.join().expect("join fake ESI HTTP listener");
    }
}

impl OneShotHttpServer {
    fn start(status: u16, headers: &[(&str, &str)], body: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ESI HTTP listener");
        let address = listener.local_addr().expect("fake ESI listener address");
        let response = format!(
            "HTTP/1.1 {status} test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
            body.len(),
            headers
                .iter()
                .map(|(name, value)| format!("{name}: {value}\r\n"))
                .collect::<String>(),
            body,
        );
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept fake ESI HTTP request");
            let mut request = [0_u8; 4096];
            let _ = stream
                .read(&mut request)
                .expect("read fake ESI HTTP request");
            stream
                .write_all(response.as_bytes())
                .expect("write fake ESI HTTP response");
        });
        Self {
            base_url: format!("http://{address}/"),
            handle,
        }
    }

    fn finish(self) {
        self.handle.join().expect("join fake ESI HTTP listener");
    }
}

struct TemporaryDatabase {
    admin_url: String,
    database_name: String,
    url: String,
}

impl TemporaryDatabase {
    async fn new() -> Self {
        let database = Self::unavailable().await;
        database.create().await;
        database
    }

    async fn unavailable() -> Self {
        let admin_url =
            std::env::var("CONTRACT_TEST_DATABASE_URL").expect("CONTRACT_TEST_DATABASE_URL must point to a PostgreSQL instance for contract integration tests (for example postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts)");
        let sequence = DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "contract_intelligence_test_{}_{}",
            std::process::id(),
            sequence
        );
        let mut url = Url::parse(&admin_url).expect("valid CONTRACT_TEST_DATABASE_URL");
        url.set_path(&format!("/{database_name}"));
        Self {
            admin_url,
            database_name,
            url: url.to_string(),
        }
    }

    async fn create(&self) {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
            .expect("connect to contract test PostgreSQL");
        sqlx::query(&format!("CREATE DATABASE {}", self.database_name))
            .execute(&admin)
            .await
            .expect("create temporary contract test database");
        admin.close().await;
    }

    async fn store(&self) -> ContractCollectionStore {
        ContractCollectionStore::connect(&self.url)
            .await
            .expect("connect contract collection store")
    }

    async fn destroy(self) {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
            .expect("reconnect to contract test PostgreSQL");
        sqlx::query(&format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.database_name
        ))
        .execute(&admin)
        .await
        .expect("drop temporary contract test database");
        admin.close().await;
    }
}

async fn install_public_location_evidence_failure_trigger(pool: &sqlx::PgPool) {
    sqlx::query(
        "CREATE FUNCTION fail_public_location_evidence_test_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.evidence_class = 'public_npc' THEN RAISE EXCEPTION 'synthetic public location retention failure'; END IF; RETURN NEW; END; $$",
    )
    .execute(pool)
    .await
    .expect("create synthetic public-evidence failure function");
    sqlx::query("CREATE TRIGGER fail_public_location_evidence_test_insert BEFORE INSERT ON location_evidence FOR EACH ROW EXECUTE FUNCTION fail_public_location_evidence_test_insert()")
        .execute(pool)
        .await
        .expect("fail public evidence retention deterministically");
}

async fn remove_public_location_evidence_failure_trigger(pool: &sqlx::PgPool) {
    sqlx::query("DROP TRIGGER fail_public_location_evidence_test_insert ON location_evidence")
        .execute(pool)
        .await
        .expect("remove synthetic public-evidence failure trigger");
    sqlx::query("DROP FUNCTION fail_public_location_evidence_test_insert()")
        .execute(pool)
        .await
        .expect("remove synthetic public-evidence failure function");
}

#[tokio::test]
async fn operator_location_evidence_cli_adds_and_inspects_an_auditable_mapping() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let expires_at = Utc.with_ymd_and_hms(2026, 8, 18, 12, 0, 0).unwrap();
    let location_id = 1_035_466_617_946_i64;
    let add = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617946",
            "--structure-id",
            "1035466617946",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--observed-at",
            "2026-08-17T12:00:00Z",
            "--expires-at",
            "2026-08-18T12:00:00Z",
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "fleet scout report 2026-08-17T12:00Z",
        ],
        observed_at,
    )
    .await
    .expect("add validated operator evidence through the restricted CLI");
    let evidence_id = match add {
        OperatorLocationEvidenceCliResult::Added(evidence) => {
            assert_eq!(evidence.location_id, location_id);
            assert_eq!(evidence.structure_id, Some(location_id));
            assert_eq!(evidence.station_id, None);
            assert_eq!(evidence.solar_system_id, 30_002_086);
            assert_eq!(evidence.region_id, Some(10_000_003));
            assert_eq!(evidence.evidence_class, LocationEvidenceClass::Operator);
            assert_eq!(evidence.actor, "operator:146451271497416704");
            assert_eq!(evidence.provenance, "fleet scout report 2026-08-17T12:00Z");
            assert_eq!(evidence.observed_at, observed_at);
            assert_eq!(evidence.expires_at, Some(expires_at));
            evidence.id
        }
        result => panic!("expected evidence add result, got {result:?}"),
    };
    let inspect = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &evidence_id.to_string()],
        observed_at,
    )
    .await
    .expect("inspect the persisted evidence through the restricted CLI");
    match inspect {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert_eq!(evidence.id, evidence_id);
            assert_eq!(audit.len(), 1);
            assert_eq!(audit[0].action, "added");
            assert_eq!(audit[0].actor, "operator:146451271497416704");
            assert_eq!(audit[0].provenance, "fleet scout report 2026-08-17T12:00Z");
        }
        result => panic!("expected evidence inspection result, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_first_public_location_evidence_writes_are_idempotent() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to force overlapping public-evidence writes");
    sqlx::query(
        "CREATE FUNCTION pause_location_evidence_test_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END; $$",
    )
    .execute(&raw_pool)
    .await
    .expect("create deterministic first-write pause");
    sqlx::query("CREATE TRIGGER pause_location_evidence_test_insert BEFORE INSERT ON location_evidence FOR EACH ROW EXECUTE FUNCTION pause_location_evidence_test_insert()")
        .execute(&raw_pool)
        .await
        .expect("pause concurrent first public evidence inserts");
    raw_pool.close().await;

    let now = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let location_id = 60_003_760_i64;
    let barrier = Arc::new(Barrier::new(2));
    let first_store = store.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        LocationEvidenceService::new(&first_store)
            .record_public_npc(
                location_id,
                Some(location_id),
                30_002_086,
                Some(10_000_003),
                now,
                now,
            )
            .await
    });
    let second_store = store.clone();
    let second_barrier = barrier.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        LocationEvidenceService::new(&second_store)
            .record_public_npc(
                location_id,
                Some(location_id),
                30_002_086,
                Some(10_000_003),
                now,
                now,
            )
            .await
    });
    let first = first.await.expect("join first public write");
    let second = second.await.expect("join second public write");
    let first = first.expect("first public write succeeds");
    let second = second.expect("second public write succeeds idempotently");
    assert_eq!(first.id, second.id);
    let current = LocationEvidenceService::new(&store)
        .resolve(location_id, now)
        .await
        .expect("resolve current evidence")
        .expect("retain one current public record");
    assert_eq!(current.id, first.id);
    assert_eq!(current.evidence_class, LocationEvidenceClass::PublicNpc);
    database.destroy().await;
}

#[tokio::test]
async fn older_public_location_observation_cannot_replace_newer_conflicting_system() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let location_id = 60_003_762_i64;
    let newer_observed_at = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let current = LocationEvidenceService::new(&store)
        .record_public_npc(
            location_id,
            Some(location_id),
            30_003_089,
            Some(10_000_002),
            newer_observed_at,
            newer_observed_at,
        )
        .await
        .expect("record the newer public station location");
    let retained = LocationEvidenceService::new(&store)
        .record_public_npc(
            location_id,
            Some(location_id),
            30_002_086,
            Some(10_000_003),
            newer_observed_at - chrono::Duration::minutes(1),
            newer_observed_at + chrono::Duration::minutes(1),
        )
        .await
        .expect("ignore the older conflicting public observation");
    assert_eq!(retained.id, current.id);
    assert_eq!(retained.solar_system_id, 30_003_089);
    assert_eq!(retained.region_id, Some(10_000_002));
    let inspection = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &current.id.to_string()],
        newer_observed_at,
    )
    .await
    .expect("inspect unchanged current public evidence");
    match inspection {
        OperatorLocationEvidenceCliResult::Inspected { audit, .. } => {
            assert_eq!(audit.len(), 1);
        }
        result => panic!("expected inspected evidence, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test]
async fn newer_conflicting_public_system_does_not_carry_over_the_old_region() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let location_id = 60_003_765_i64;
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let current = LocationEvidenceService::new(&store)
        .record_public_npc(
            location_id,
            Some(location_id),
            30_003_089,
            Some(10_000_002),
            observed_at,
            observed_at,
        )
        .await
        .expect("record the current public station location");
    let replacement = LocationEvidenceService::new(&store)
        .record_public_npc(
            location_id,
            Some(location_id),
            30_002_086,
            None,
            observed_at + chrono::Duration::minutes(1),
            observed_at + chrono::Duration::minutes(1),
        )
        .await
        .expect("record the newer conflicting public station location");
    assert_ne!(replacement.id, current.id);
    assert_eq!(replacement.solar_system_id, 30_002_086);
    assert_eq!(replacement.region_id, None);
    assert_eq!(replacement.supersedes_id, Some(current.id));
    let inspection = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &current.id.to_string()],
        observed_at,
    )
    .await
    .expect("inspect the superseded public evidence");
    match inspection {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert!(evidence.superseded_at.is_some());
            assert_eq!(audit.len(), 2);
        }
        result => panic!("expected inspected evidence, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test]
async fn public_location_region_facts_are_monotonic_for_same_station_and_system() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let observed_at = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let known_region_location_id = 60_003_763_i64;
    let known_region = LocationEvidenceService::new(&store)
        .record_public_npc(
            known_region_location_id,
            Some(known_region_location_id),
            30_003_089,
            Some(10_000_002),
            observed_at,
            observed_at,
        )
        .await
        .expect("record the public station with its known region");
    let retained = LocationEvidenceService::new(&store)
        .record_public_npc(
            known_region_location_id,
            Some(known_region_location_id),
            30_003_089,
            None,
            observed_at + chrono::Duration::minutes(1),
            observed_at + chrono::Duration::minutes(1),
        )
        .await
        .expect("retain the known region when a later public response omits it");
    assert_eq!(retained.id, known_region.id);
    assert_eq!(retained.region_id, Some(10_000_002));
    let inspection = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &known_region.id.to_string()],
        observed_at,
    )
    .await
    .expect("inspect the unchanged known-region public row");
    match inspection {
        OperatorLocationEvidenceCliResult::Inspected { audit, .. } => {
            assert_eq!(audit.len(), 1);
        }
        result => panic!("expected inspected evidence, got {result:?}"),
    }

    let missing_region_location_id = 60_003_764_i64;
    let missing_region = LocationEvidenceService::new(&store)
        .record_public_npc(
            missing_region_location_id,
            Some(missing_region_location_id),
            30_002_086,
            None,
            observed_at,
            observed_at,
        )
        .await
        .expect("record public station before region is available");
    let enriched = LocationEvidenceService::new(&store)
        .record_public_npc(
            missing_region_location_id,
            Some(missing_region_location_id),
            30_002_086,
            Some(10_000_003),
            observed_at + chrono::Duration::minutes(1),
            observed_at + chrono::Duration::minutes(1),
        )
        .await
        .expect("enrich a later public station observation with its region");
    assert_ne!(enriched.id, missing_region.id);
    assert_eq!(enriched.region_id, Some(10_000_003));
    assert_eq!(enriched.supersedes_id, Some(missing_region.id));
    let inspection = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &missing_region.id.to_string()],
        observed_at,
    )
    .await
    .expect("inspect the superseded regionless public row");
    match inspection {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert!(evidence.superseded_at.is_some());
            assert_eq!(audit.len(), 2);
        }
        result => panic!("expected inspected evidence, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test]
async fn public_location_evidence_retention_failure_does_not_abort_listed_delivery() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(vec![], HashMap::new())),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline before the listed contract");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "public-location-delivery".to_string(),
            description: "deliver from public location context".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
                    config::SystemRange {
                        system_id: 30_002_086,
                        range: 1.0,
                    },
                ])),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the public-location subscription");
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to fail optional public retention");
    install_public_location_evidence_failure_trigger(&raw_pool).await;
    raw_pool.close().await;
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(PositionEsi {
            inner: regional_esi(
                vec![contract.clone()],
                HashMap::from([(
                    contract.contract_id,
                    Ok(EsiResponse::fresh(vec![], expiring_cache())),
                )]),
            ),
            contexts: HashMap::from([(
                contract.contract_id,
                ContractObservationContext {
                    solar_system_id: Some(30_002_086),
                    ..ContractObservationContext::default()
                },
            )]),
            positions: HashMap::from([(30_002_086, position_at_light_years(0.0))]),
            position_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("optional public location retention must not abort listed delivery");
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert!(LocationEvidenceService::new(&store)
        .resolve(contract.start_location_id, Utc::now())
        .await
        .expect("read absent optional evidence")
        .is_none());
    database.destroy().await;
}

#[tokio::test]
async fn snapshot_retries_public_location_retention_without_duplicate_listed_delivery() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(vec![], HashMap::new())),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline before snapshot retention");
    store
        .upsert_contract_subscription(&contract_subscription(
            "snapshot-location-retention",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the listed subscription");
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to fail snapshot public retention");
    install_public_location_evidence_failure_trigger(&raw_pool).await;
    raw_pool.close().await;
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let failed_snapshot = Arc::new(SnapshottingEsi {
        inner: regional_esi(
            vec![contract.clone()],
            HashMap::from([(
                contract.contract_id,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        ),
        contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
            ContractEmbedContext {
                observed_at: Some(Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap()),
                location: ContractLocationContext {
                    location_kind: Some("Station".to_string()),
                    solar_system_id: Some(30_002_086),
                    region_id: Some(10_000_003),
                    ..ContractLocationContext::default()
                },
                ..ContractEmbedContext::default()
            },
            CacheMetadata::cached_for_seconds(60),
        ))]),
        calls: StdMutex::new(Vec::new()),
    });
    let failed = ContractCollector::new(store.clone(), failed_snapshot.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .collect_cycle()
        .await
        .expect("optional snapshot retention does not abort the committed listed event");
    assert_eq!(failed.events.len(), 1);
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert!(store
        .observed_embed_context(10_000_002, contract.contract_id)
        .await
        .expect("read absent snapshot after failed retention")
        .is_none());
    assert!(LocationEvidenceService::new(&store)
        .resolve(contract.start_location_id, Utc::now())
        .await
        .expect("read absent failed evidence")
        .is_none());
    assert!(store
        .unresolved_collection_failures()
        .await
        .expect("read retryable snapshot retention failure")
        .iter()
        .any(|failure| failure.contract_id == Some(contract.contract_id)
            && failure.failure_kind == "embed_context_enrichment"));

    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to restore public retention");
    remove_public_location_evidence_failure_trigger(&raw_pool).await;
    raw_pool.close().await;
    let repaired_snapshot = Arc::new(SnapshottingEsi {
        inner: regional_esi(
            vec![contract.clone()],
            HashMap::from([(
                contract.contract_id,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        ),
        contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
            ContractEmbedContext {
                observed_at: Some(Utc.with_ymd_and_hms(2026, 8, 17, 12, 1, 0).unwrap()),
                location: ContractLocationContext {
                    location_kind: Some("Station".to_string()),
                    solar_system_id: Some(30_002_086),
                    region_id: Some(10_000_003),
                    ..ContractLocationContext::default()
                },
                ..ContractEmbedContext::default()
            },
            CacheMetadata::cached_for_seconds(60),
        ))]),
        calls: StdMutex::new(Vec::new()),
    });
    let repaired = ContractCollector::new(store.clone(), repaired_snapshot.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .collect_cycle()
        .await
        .expect("retry the missing snapshot after public retention recovers");
    assert!(repaired.events.is_empty());
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert!(store
        .observed_embed_context(10_000_002, contract.contract_id)
        .await
        .expect("read repaired snapshot")
        .is_some());
    assert_eq!(
        LocationEvidenceService::new(&store)
            .resolve(contract.start_location_id, Utc::now())
            .await
            .expect("resolve repaired public evidence")
            .expect("persisted public evidence")
            .evidence_class,
        LocationEvidenceClass::PublicNpc
    );
    assert!(!store
        .unresolved_collection_failures()
        .await
        .expect("resolve snapshot failure after repair")
        .iter()
        .any(|failure| failure.contract_id == Some(contract.contract_id)
            && failure.failure_kind == "embed_context_enrichment"));
    assert_eq!(failed_snapshot.calls.lock().unwrap().as_slice(), &[44]);
    assert_eq!(repaired_snapshot.calls.lock().unwrap().as_slice(), &[44]);
    database.destroy().await;
}

#[tokio::test]
async fn deferred_operator_location_rechecks_security_after_evidence_moves_systems() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    let now = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(vec![], HashMap::new())),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline before deferred context evaluation");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "moved-operator-security".to_string(),
            description: "security and proximity require current operator location evidence"
                .to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::And(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::SecurityRange {
                        min: 0.4,
                        max: 0.6,
                    }),
                    ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
                        config::SystemRange {
                            system_id: 30_003_089,
                            range: 1.0,
                        },
                    ])),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the deferred security subscription");
    let first = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "60003760",
            "--station-id",
            "60003760",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            "2026-08-19T12:00:00Z",
            "--actor",
            "operator:one",
            "--provenance",
            "initial station report",
        ],
        now,
    )
    .await
    .expect("add initial lower-precedence station evidence");
    let first_id = match first {
        OperatorLocationEvidenceCliResult::Added(evidence) => evidence.id.to_string(),
        result => panic!("expected initial operator evidence, got {result:?}"),
    };
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let first_esi = Arc::new(SecurityBySystemPositionEsi {
        inner: regional_esi(
            vec![contract.clone()],
            HashMap::from([(
                contract.contract_id,
                Ok(EsiResponse::fresh(vec![], expiring_cache())),
            )]),
        ),
        contexts: HashMap::new(),
        positions: HashMap::from([(30_003_089, position_at_light_years(0.0))]),
        security_statuses: HashMap::from([(30_002_086, 0.5)]),
        security_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), first_esi.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .with_delivery_clock(Arc::new(FixedDeliveryClock(StdMutex::new(now))))
        .collect_cycle()
        .await
        .expect("defer while the old operator system position is unavailable");
    assert!(delivery.sent.lock().unwrap().is_empty());
    assert_eq!(
        first_esi.security_calls.lock().unwrap().as_slice(),
        &[30_002_086]
    );
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect the deferred context");
    let deferred_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM contract_deferred_subscription_matches WHERE subscription_id = 'moved-operator-security'",
    )
    .fetch_one(&raw_pool)
    .await
    .expect("count persisted deferred context");
    let deferred_event: Value = sqlx::query_scalar(
        "SELECT event FROM contract_deferred_subscription_matches WHERE subscription_id = 'moved-operator-security'",
    )
    .fetch_one(&raw_pool)
    .await
    .expect("read persisted deferred event context");
    raw_pool.close().await;
    assert_eq!(deferred_count, 1);
    assert_eq!(
        deferred_event["context"]["location_evidence_class"],
        "operator"
    );
    assert_eq!(deferred_event["context"]["security_status"], 0.5);

    execute_operator_location_evidence_cli(
        &store,
        &[
            "supersede",
            "--id",
            &first_id,
            "--system-id",
            "30003089",
            "--region-id",
            "10000002",
            "--expires-at",
            "2026-08-20T12:01:00Z",
            "--actor",
            "operator:two",
            "--provenance",
            "moved station report",
        ],
        now + chrono::Duration::minutes(1),
    )
    .await
    .expect("supersede the operator evidence with the moved system");
    let moved_esi = Arc::new(SecurityBySystemPositionEsi {
        inner: regional_esi(
            vec![contract.clone()],
            HashMap::from([(
                contract.contract_id,
                Ok(EsiResponse::fresh(vec![], expiring_cache())),
            )]),
        ),
        contexts: HashMap::new(),
        positions: HashMap::from([(30_003_089, position_at_light_years(0.0))]),
        security_statuses: HashMap::from([(30_003_089, 0.9)]),
        security_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), moved_esi.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .with_delivery_clock(Arc::new(FixedDeliveryClock(StdMutex::new(
            now + chrono::Duration::minutes(2),
        ))))
        .collect_cycle()
        .await
        .expect("re-evaluate the deferred event with moved operator evidence");
    assert_eq!(
        moved_esi.security_calls.lock().unwrap().as_slice(),
        &[30_003_089]
    );
    assert!(delivery.sent.lock().unwrap().is_empty());
    database.destroy().await;
}

#[tokio::test]
async fn operator_location_evidence_cli_rejects_unknown_options_and_nonfuture_expiry() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let now = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let unknown_option = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617947",
            "--structure-id",
            "1035466617947",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            "2026-08-18T12:00:00Z",
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "fleet scout report",
            "--provenence",
            "misspelled",
        ],
        now,
    )
    .await
    .expect_err("reject a mutation option that would otherwise be silently ignored");
    assert!(unknown_option.contains("unknown location-evidence option: --provenence"));
    let negative_evidence_id =
        execute_operator_location_evidence_cli(&store, &["inspect", "--id", "-1"], now)
            .await
            .expect_err("reject non-positive evidence identifiers before querying lifecycle state");
    assert!(negative_evidence_id.contains("--id must be a positive integer"));
    let expired = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617948",
            "--structure-id",
            "1035466617948",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            "2026-08-17T11:59:59Z",
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "fleet scout report",
        ],
        now,
    )
    .await
    .expect_err("reject operator evidence that is already expired at mutation time");
    assert!(expired.contains("operator evidence expiry must be in the future"));
    let future_observation = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617949",
            "--structure-id",
            "1035466617949",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--observed-at",
            "2026-08-17T12:00:01Z",
            "--expires-at",
            "2026-08-18T12:00:00Z",
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "fleet scout report",
        ],
        now,
    )
    .await
    .expect_err("reject a future observation timestamp from the operator CLI");
    assert!(
        future_observation.contains("operator evidence observation time cannot be in the future")
    );
    database.destroy().await;
}

#[tokio::test]
async fn operator_location_evidence_cli_supersedes_expires_and_lists_only_operator_evidence() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let first_observed_at = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let first = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617950",
            "--structure-id",
            "1035466617950",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            "2026-08-18T12:00:00Z",
            "--actor",
            "operator:one",
            "--provenance",
            "first scout report",
        ],
        first_observed_at,
    )
    .await
    .expect("add original operator evidence");
    let first_id = match first {
        OperatorLocationEvidenceCliResult::Added(evidence) => evidence.id,
        result => panic!("expected added evidence, got {result:?}"),
    };
    let first_id_text = first_id.to_string();
    let superseded = execute_operator_location_evidence_cli(
        &store,
        &[
            "supersede",
            "--id",
            &first_id_text,
            "--system-id",
            "30003089",
            "--region-id",
            "10000002",
            "--expires-at",
            "2026-08-19T12:00:00Z",
            "--actor",
            "operator:two",
            "--provenance",
            "updated scout report",
        ],
        first_observed_at + chrono::Duration::minutes(1),
    )
    .await
    .expect("supersede only the current operator evidence");
    let replacement_id = match superseded {
        OperatorLocationEvidenceCliResult::Superseded(evidence) => {
            assert_eq!(evidence.location_id, 1_035_466_617_950);
            assert_eq!(evidence.solar_system_id, 30_003_089);
            assert_eq!(evidence.region_id, Some(10_000_002));
            assert_eq!(evidence.supersedes_id, Some(first_id));
            evidence.id
        }
        result => panic!("expected superseded evidence, got {result:?}"),
    };
    let inspected_first = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &first_id_text],
        first_observed_at,
    )
    .await
    .expect("inspect superseded evidence");
    match inspected_first {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert!(evidence.superseded_at.is_some());
            assert_eq!(
                evidence.superseded_by_actor.as_deref(),
                Some("operator:two")
            );
            assert_eq!(
                audit
                    .iter()
                    .map(|entry| entry.action.as_str())
                    .collect::<Vec<_>>(),
                ["added", "superseded"]
            );
        }
        result => panic!("expected inspection result, got {result:?}"),
    }
    let replacement_id_text = replacement_id.to_string();
    let expired = execute_operator_location_evidence_cli(
        &store,
        &[
            "expire",
            "--id",
            &replacement_id_text,
            "--actor",
            "operator:three",
            "--provenance",
            "structure removed from watch list",
        ],
        first_observed_at + chrono::Duration::minutes(2),
    )
    .await
    .expect("expire only the replacement operator evidence");
    assert!(matches!(
        expired,
        OperatorLocationEvidenceCliResult::Expired(_)
    ));
    let listed = execute_operator_location_evidence_cli(
        &store,
        &["list", "--location-id", "1035466617950"],
        first_observed_at,
    )
    .await
    .expect("list the evidence history without mutating it");
    match listed {
        OperatorLocationEvidenceCliResult::Listed(evidence) => {
            assert_eq!(evidence.len(), 2);
            assert_eq!(evidence[0].id, first_id);
            assert_eq!(evidence[1].id, replacement_id);
            assert!(evidence[1].expired_at.is_some());
        }
        result => panic!("expected evidence list, got {result:?}"),
    }
    let inspected_replacement = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &replacement_id_text],
        first_observed_at,
    )
    .await
    .expect("inspect explicitly expired operator evidence");
    match inspected_replacement {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert!(evidence.expired_at.is_some());
            assert_eq!(
                audit
                    .iter()
                    .map(|entry| entry.action.as_str())
                    .collect::<Vec<_>>(),
                ["added", "expired"]
            );
            assert_eq!(audit[1].actor, "operator:three");
        }
        result => panic!("expected evidence inspection result, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test]
async fn location_evidence_audit_and_supersession_use_mutation_time_not_observed_time() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let added_at = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let backdated_observed_at = added_at - chrono::Duration::hours(1);
    let added = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617953",
            "--structure-id",
            "1035466617953",
            "--system-id",
            "30002086",
            "--observed-at",
            "2026-08-17T11:00:00Z",
            "--expires-at",
            "2026-08-19T12:00:00Z",
            "--actor",
            "operator:one",
            "--provenance",
            "backdated scout report",
        ],
        added_at,
    )
    .await
    .expect("retain the supplied backdated observation time");
    let original_id = match added {
        OperatorLocationEvidenceCliResult::Added(evidence) => {
            assert_eq!(evidence.observed_at, backdated_observed_at);
            evidence.id.to_string()
        }
        result => panic!("expected added evidence, got {result:?}"),
    };
    let original = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &original_id],
        added_at,
    )
    .await
    .expect("inspect backdated evidence audit");
    match original {
        OperatorLocationEvidenceCliResult::Inspected { audit, .. } => {
            assert_eq!(audit[0].occurred_at, added_at);
        }
        result => panic!("expected inspected evidence, got {result:?}"),
    }
    let superseded_at = added_at + chrono::Duration::minutes(10);
    let replacement_observed_at = added_at - chrono::Duration::minutes(30);
    let replacement = execute_operator_location_evidence_cli(
        &store,
        &[
            "supersede",
            "--id",
            &original_id,
            "--system-id",
            "30003089",
            "--observed-at",
            "2026-08-17T11:30:00Z",
            "--expires-at",
            "2026-08-20T12:00:00Z",
            "--actor",
            "operator:two",
            "--provenance",
            "backdated replacement scout report",
        ],
        superseded_at,
    )
    .await
    .expect("supersede while preserving supplied observation time");
    let replacement_id = match replacement {
        OperatorLocationEvidenceCliResult::Superseded(evidence) => {
            assert_eq!(evidence.observed_at, replacement_observed_at);
            evidence.id.to_string()
        }
        result => panic!("expected superseded evidence, got {result:?}"),
    };
    let original = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &original_id],
        superseded_at,
    )
    .await
    .expect("inspect superseded source evidence");
    match original {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert_eq!(evidence.superseded_at, Some(superseded_at));
            assert_eq!(audit[1].occurred_at, superseded_at);
        }
        result => panic!("expected inspected source evidence, got {result:?}"),
    }
    let replacement = execute_operator_location_evidence_cli(
        &store,
        &["inspect", "--id", &replacement_id],
        superseded_at,
    )
    .await
    .expect("inspect replacement evidence audit");
    match replacement {
        OperatorLocationEvidenceCliResult::Inspected { audit, .. } => {
            assert_eq!(audit[0].occurred_at, superseded_at);
        }
        result => panic!("expected inspected replacement evidence, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test]
async fn future_dated_location_evidence_is_not_selected_before_its_observation_time() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let now = Utc::now();
    let expires_at = (now + chrono::Duration::days(2)).to_rfc3339();
    execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "60003761",
            "--station-id",
            "60003761",
            "--system-id",
            "30002086",
            "--expires-at",
            &expires_at,
            "--actor",
            "operator:one",
            "--provenance",
            "current operator report",
        ],
        now,
    )
    .await
    .expect("add current operator evidence");
    let future_observed_at = now + chrono::Duration::days(1);
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed future access-qualified evidence");
    sqlx::query("INSERT INTO location_evidence (location_id, evidence_class, station_id, solar_system_id, provenance, actor, observed_at) VALUES ($1, 'access_qualified', $1, $2, 'future scoped lookup', 'system:ticket06-seam', $3)")
        .bind(60_003_761_i64)
        .bind(30_003_089_i64)
        .bind(future_observed_at)
        .execute(&raw_pool)
        .await
        .expect("seed future-dated access-qualified evidence through its future writer seam");
    raw_pool.close().await;
    drop(store);
    let restarted_store = database.store().await;
    let selected_now = LocationEvidenceService::new(&restarted_store)
        .resolve(60_003_761, now)
        .await
        .expect("resolve current evidence")
        .expect("operator evidence remains current before future observation");
    assert_eq!(selected_now.evidence_class, LocationEvidenceClass::Operator);
    assert_eq!(selected_now.solar_system_id, 30_002_086);
    let selected_later = LocationEvidenceService::new(&restarted_store)
        .resolve(
            60_003_761,
            future_observed_at + chrono::Duration::seconds(1),
        )
        .await
        .expect("resolve evidence after the future observation time")
        .expect("access-qualified evidence becomes selectable when observed");
    assert_eq!(
        selected_later.evidence_class,
        LocationEvidenceClass::AccessQualified
    );
    assert_eq!(selected_later.solar_system_id, 30_003_089);
    database.destroy().await;
}

#[tokio::test]
async fn adding_after_time_expiry_audits_the_automatic_operator_expiration() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let first_now = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    let first = execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617952",
            "--structure-id",
            "1035466617952",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            "2026-08-17T12:01:00Z",
            "--actor",
            "operator:one",
            "--provenance",
            "short-lived scout report",
        ],
        first_now,
    )
    .await
    .expect("add short-lived operator evidence");
    let first_id = match first {
        OperatorLocationEvidenceCliResult::Added(evidence) => evidence.id.to_string(),
        result => panic!("expected added evidence, got {result:?}"),
    };
    execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617952",
            "--structure-id",
            "1035466617952",
            "--system-id",
            "30003089",
            "--region-id",
            "10000002",
            "--expires-at",
            "2026-08-19T12:00:00Z",
            "--actor",
            "operator:two",
            "--provenance",
            "replacement scout report",
        ],
        first_now + chrono::Duration::minutes(2),
    )
    .await
    .expect("replace operator evidence only after its prior expiry");
    let inspected =
        execute_operator_location_evidence_cli(&store, &["inspect", "--id", &first_id], first_now)
            .await
            .expect("inspect automatically expired evidence");
    match inspected {
        OperatorLocationEvidenceCliResult::Inspected { evidence, audit } => {
            assert!(evidence.expired_at.is_some());
            assert_eq!(evidence.superseded_at, None);
            assert_eq!(
                audit
                    .iter()
                    .map(|entry| entry.action.as_str())
                    .collect::<Vec<_>>(),
                ["added", "expired"]
            );
            assert_eq!(audit[1].actor, "system:expiry");
        }
        result => panic!("expected evidence inspection, got {result:?}"),
    }
    database.destroy().await;
}

#[tokio::test]
async fn operator_evidence_resolves_deferred_player_structure_proximity_after_store_recreation() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let location_id = 1_035_466_617_951_i64;
    let mut player_structure_contract = item_exchange_contract(44);
    player_structure_contract.start_location_id = location_id;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish a silent public-contract baseline");
    for (id, center) in [("near-turnur", 30_002_086), ("near-kurniainen", 30_003_089)] {
        store
            .upsert_contract_subscription(&ContractSubscription {
                guild_id: 42,
                channel_id: 77,
                id: id.to_string(),
                description: id.to_string(),
                filter: ContractFilter {
                    root: ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(
                        vec![config::SystemRange {
                            system_id: center,
                            range: 1.0,
                        }],
                    )),
                },
                event_actions: ContractEventActions {
                    listed: ContractEventAction::Post,
                    ..ContractEventActions::default()
                },
            })
            .await
            .expect("persist a proximity subscription");
    }
    let positions = HashMap::from([
        (30_002_086, position_at_light_years(0.0)),
        (30_003_089, position_at_light_years(20.0)),
    ]);
    let unknown_delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(PositionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(
                        vec![player_structure_contract.clone()],
                        expiring_page(1),
                    )),
                )]),
                items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
            },
            contexts: HashMap::new(),
            positions: positions.clone(),
            position_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::new())),
        unknown_delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("defer the player-structure proximity match without evidence");
    assert!(unknown_delivery.sent.lock().unwrap().is_empty());
    let now = Utc::now();
    execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "1035466617951",
            "--structure-id",
            "1035466617951",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            &(now + chrono::Duration::days(1)).to_rfc3339(),
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "scout report for retained structure location",
        ],
        now,
    )
    .await
    .expect("add evidence without any subscription identity");
    drop(store);
    let restarted_store = database.store().await;
    let selected = LocationEvidenceService::new(&restarted_store)
        .resolve(location_id, now)
        .await
        .expect("read retained evidence after reconstructing the store")
        .expect("operator evidence remains selected before expiry");
    assert_eq!(selected.evidence_class, LocationEvidenceClass::Operator);
    assert_eq!(selected.solar_system_id, 30_002_086);
    let resolved_delivery = Arc::new(RecordingDelivery {
        store: restarted_store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        restarted_store.clone(),
        Arc::new(PositionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(
                        vec![player_structure_contract],
                        expiring_page(1),
                    )),
                )]),
                items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
            },
            contexts: HashMap::new(),
            positions,
            position_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::new())),
        resolved_delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("re-evaluate deferred proximity from retained operator evidence");
    let sent = resolved_delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].subscription_id, "near-turnur");
    assert_eq!(sent[0].contract_id, 44);
    drop(sent);
    database.destroy().await;
}

#[tokio::test]
async fn fresh_public_npc_location_evidence_outranks_retained_operator_evidence_after_restart() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    let now = Utc::now();
    let public_observed_at = now + chrono::Duration::seconds(1);
    execute_operator_location_evidence_cli(
        &store,
        &[
            "add",
            "--location-id",
            "60003760",
            "--station-id",
            "60003760",
            "--system-id",
            "30002086",
            "--region-id",
            "10000003",
            "--expires-at",
            &(now + chrono::Duration::days(1)).to_rfc3339(),
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "outdated station report",
        ],
        now,
    )
    .await
    .expect("retain lower-precedence operator station evidence");
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline before the public station contract");
    for (id, center) in [
        ("operator-system", 30_002_086),
        ("public-system", 30_003_089),
    ] {
        store
            .upsert_contract_subscription(&ContractSubscription {
                guild_id: 42,
                channel_id: 77,
                id: id.to_string(),
                description: id.to_string(),
                filter: ContractFilter {
                    root: ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(
                        vec![config::SystemRange {
                            system_id: center,
                            range: 1.0,
                        }],
                    )),
                },
                event_actions: ContractEventActions {
                    listed: ContractEventAction::Post,
                    ..ContractEventActions::default()
                },
            })
            .await
            .expect("persist a public-precedence proximity subscription");
    }
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(PositionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
                )]),
                items: HashMap::from([(
                    contract.contract_id,
                    Ok(EsiResponse::fresh(vec![], expiring_cache())),
                )]),
            },
            contexts: HashMap::from([(
                contract.contract_id,
                ContractObservationContext {
                    solar_system_id: Some(30_003_089),
                    ..ContractObservationContext::default()
                },
            )]),
            positions: HashMap::from([
                (30_002_086, position_at_light_years(0.0)),
                (30_003_089, position_at_light_years(20.0)),
            ]),
            position_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .with_delivery_clock(Arc::new(FixedDeliveryClock(StdMutex::new(
        public_observed_at,
    ))))
    .collect_cycle()
    .await
    .expect("prefer the fresh public station location during proximity evaluation");
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].subscription_id, "public-system");
    drop(sent);
    let evidence = LocationEvidenceService::new(&store)
        .resolve(contract.start_location_id, public_observed_at)
        .await
        .expect("resolve precedence from retained evidence")
        .expect("public ESI result is retained");
    assert_eq!(evidence.evidence_class, LocationEvidenceClass::PublicNpc);
    assert_eq!(evidence.solar_system_id, 30_003_089);
    let public_id = evidence.id.to_string();
    let public_expire = execute_operator_location_evidence_cli(
        &store,
        &[
            "expire",
            "--id",
            &public_id,
            "--actor",
            "operator:146451271497416704",
            "--provenance",
            "must not expire system-owned evidence",
        ],
        now,
    )
    .await
    .expect_err("operator CLI cannot mutate authoritative public evidence");
    assert!(public_expire.contains("not current operator evidence"));
    drop(store);
    let restarted_store = database.store().await;
    let selected_after_restart = LocationEvidenceService::new(&restarted_store)
        .resolve(contract.start_location_id, public_observed_at)
        .await
        .expect("resolve retained precedence after store recreation")
        .expect("public evidence remains selected after restart");
    assert_eq!(
        selected_after_restart.evidence_class,
        LocationEvidenceClass::PublicNpc
    );
    assert_eq!(selected_after_restart.solar_system_id, 30_003_089);
    let retained = execute_operator_location_evidence_cli(
        &restarted_store,
        &["list", "--location-id", "60003760"],
        now,
    )
    .await
    .expect("list separately retained evidence classes");
    match retained {
        OperatorLocationEvidenceCliResult::Listed(evidence) => {
            assert_eq!(evidence.len(), 2);
            assert!(evidence.iter().any(|entry| entry.evidence_class
                == LocationEvidenceClass::Operator
                && entry.superseded_at.is_none()));
            assert!(evidence
                .iter()
                .any(|entry| entry.evidence_class == LocationEvidenceClass::PublicNpc));
        }
        result => panic!("expected retained evidence list, got {result:?}"),
    }
    database.destroy().await;
}

#[derive(Default)]
struct FakeEsi {
    regions: Vec<i64>,
    pages: HashMap<(i64, u32), Result<EsiResponse<Vec<PublicContract>>, EsiError>>,
    items: HashMap<i64, Result<EsiResponse<Vec<PublicContractItem>>, EsiError>>,
}

struct GatedRegionalEsi {
    slow_region: i64,
    slow_started: Arc<Notify>,
    slow_release: Arc<Notify>,
    pages: HashMap<i64, Vec<PublicContract>>,
    items: HashMap<i64, Vec<PublicContractItem>>,
    probes: StdMutex<Vec<Result<ContractItemProbe, EsiError>>>,
}

struct InFlightLimiterEsi {
    first_region: i64,
    second_region: i64,
    third_region: i64,
    first_started: Arc<Notify>,
    second_started: Arc<Notify>,
    first_release: Arc<Notify>,
    second_release: Arc<Notify>,
    calls: StdMutex<Vec<i64>>,
}

struct AdvancingRequestPacer {
    now: StdMutex<DateTime<Utc>>,
    waits: StdMutex<Vec<DateTime<Utc>>>,
}

struct GatedAdvancingRequestPacer {
    now: StdMutex<DateTime<Utc>>,
    waits: StdMutex<Vec<DateTime<Utc>>>,
    wait_entered: Arc<Notify>,
    wait_release: Arc<Notify>,
}

struct TimestampedRateBucketEsi {
    pacer: Arc<AdvancingRequestPacer>,
    calls: StdMutex<Vec<(String, DateTime<Utc>)>>,
    regions: Vec<i64>,
    discovery_metadata: CacheMetadata,
    page_metadata: StdMutex<Vec<CacheMetadata>>,
}

struct DelayedSameGroupPacingEsi {
    pacer: Arc<GatedAdvancingRequestPacer>,
    second_started: Arc<Notify>,
    second_release: Arc<Notify>,
    calls: StdMutex<Vec<(i64, DateTime<Utc>)>>,
}

struct GatedProbeBatchEsi {
    inner: FakeEsi,
    probes: StdMutex<Vec<Result<ContractItemProbe, EsiError>>>,
    probe_count: AtomicU64,
    first_probe_started: Arc<Notify>,
    first_probe_release: Arc<Notify>,
    second_probe_started: Arc<Notify>,
    second_probe_release: Arc<Notify>,
}

struct ContextualFakeEsi {
    inner: FakeEsi,
    context: ContractObservationContext,
}

struct PositionEsi {
    inner: FakeEsi,
    contexts: HashMap<i64, ContractObservationContext>,
    positions: HashMap<u32, SolarSystemPosition>,
    position_calls: StdMutex<Vec<u32>>,
}

struct SecurityBySystemPositionEsi {
    inner: FakeEsi,
    contexts: HashMap<i64, ContractObservationContext>,
    positions: HashMap<u32, SolarSystemPosition>,
    security_statuses: HashMap<i64, f64>,
    security_calls: StdMutex<Vec<i64>>,
}

struct PositionedResolutionEsi {
    inner: ResolutionEsi,
    positions: HashMap<u32, SolarSystemPosition>,
    position_calls: StdMutex<Vec<u32>>,
}

struct LimitedPositionEsi {
    inner: PositionEsi,
}

struct CountingContextEsi {
    inner: FakeEsi,
    context_calls: StdMutex<usize>,
}

struct ExhaustedWireManifestEsi {
    http: HttpPublicContractEsi,
    contracts: Vec<PublicContract>,
    manifests: HashMap<i64, EsiResponse<Vec<PublicContractItem>>>,
    unavailable_contract_id: i64,
}

struct HttpRegionsAndTerminalProbeEsi {
    http: Arc<HttpPublicContractEsi>,
}

#[derive(Default)]
struct ConditionalEsi {
    received_etags: StdMutex<Vec<Option<String>>>,
}

struct StopsAfterRateBoundaryEsi {
    retry_after: chrono::DateTime<Utc>,
    calls: StdMutex<Vec<String>>,
}

struct StopsBetweenContextCallsEsi {
    inner: FakeEsi,
    calls: StdMutex<Vec<String>>,
}

struct SnapshottingEsi {
    inner: FakeEsi,
    contexts: StdMutex<Vec<Result<EsiResponse<ContractEmbedContext>, EsiError>>>,
    calls: StdMutex<Vec<i64>>,
}

#[derive(Default)]
struct BoundaryContextLimiter {
    request_checks: StdMutex<usize>,
    responses: StdMutex<usize>,
    blocked: StdMutex<bool>,
}

fn expiring_metadata(etag: &str, expected_pages: Option<u32>) -> CacheMetadata {
    CacheMetadata {
        etag: Some(etag.to_string()),
        expires_at: Some(Utc::now()),
        last_modified: None,
        expected_pages,
        error_limit_remain: None,
        error_limit_reset: None,
        rate_limit_group: None,
        rate_limit_limit: None,
        rate_limit_remaining: None,
        rate_limit_used: None,
        retry_after: None,
    }
}

#[async_trait]
impl PublicContractEsi for ConditionalEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.received_etags
            .lock()
            .unwrap()
            .push(etag.map(str::to_owned));
        Ok(match etag {
            Some(_) => EsiResponse::not_modified(CacheMetadata::cached_for_seconds(0)),
            None => EsiResponse::fresh(vec![10_000_002], expiring_metadata("regions-v1", None)),
        })
    }

    async fn public_contracts_page(
        &self,
        _region_id: i64,
        _page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.received_etags
            .lock()
            .unwrap()
            .push(etag.map(str::to_owned));
        Ok(match etag {
            Some(_) => EsiResponse::not_modified(CacheMetadata::cached_for_seconds(0)),
            None => EsiResponse::fresh(
                vec![item_exchange_contract(44)],
                expiring_metadata("page-v1", Some(1)),
            ),
        })
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.received_etags
            .lock()
            .unwrap()
            .push(etag.map(str::to_owned));
        Ok(match etag {
            Some(_) => EsiResponse::not_modified(CacheMetadata::cached_for_seconds(0)),
            None => EsiResponse::fresh(vec![], expiring_metadata("items-v1", None)),
        })
    }
}

#[async_trait]
impl PublicContractEsi for FakeEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        Ok(EsiResponse::fresh(
            self.regions.clone(),
            CacheMetadata::cached_for_seconds(60),
        ))
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.pages
            .get(&(region_id, page))
            .expect("fake ESI page configured")
            .clone()
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.items
            .get(&contract_id)
            .expect("fake ESI manifest configured")
            .clone()
    }
}

#[async_trait]
impl PublicContractEsi for GatedRegionalEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        let mut regions = self.pages.keys().copied().collect::<Vec<_>>();
        regions.sort_unstable();
        Ok(EsiResponse::fresh(
            regions,
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        assert_eq!(page, 1, "the controlled regional fixture has one page");
        if region_id == self.slow_region {
            self.slow_started.notify_one();
            self.slow_release.notified().await;
        }
        Ok(EsiResponse::fresh(
            self.pages
                .get(&region_id)
                .expect("configured regional page")
                .clone(),
            expiring_page(1),
        ))
    }

    async fn public_contract_items_probe(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        self.probes.lock().unwrap().remove(0)
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        Ok(EsiResponse::fresh(
            self.items
                .get(&contract_id)
                .expect("configured contract manifest")
                .clone(),
            expiring_cache(),
        ))
    }

    async fn observed_contract_embed_context(
        &self,
        _contract: &PublicContract,
        _items: &[PublicContractItem],
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        Ok(EsiResponse::fresh(
            ContractEmbedContext {
                item_names: std::collections::BTreeMap::from([(587, "Rifter".to_string())]),
                ..ContractEmbedContext::default()
            },
            CacheMetadata::cached_for_seconds(60),
        ))
    }
}

#[async_trait]
impl PublicContractEsi for InFlightLimiterEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        Ok(EsiResponse::fresh(
            vec![self.first_region, self.second_region, self.third_region],
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        assert_eq!(page, 1, "the controlled limiter fixture has one page");
        self.calls.lock().unwrap().push(region_id);
        if region_id == self.first_region {
            self.first_started.notify_one();
            self.first_release.notified().await;
            return Ok(EsiResponse::fresh(
                vec![],
                CacheMetadata {
                    rate_limit_group: Some("public-contracts".to_string()),
                    rate_limit_limit: Some("1/1m".to_string()),
                    rate_limit_remaining: Some(0),
                    ..expiring_page(1)
                },
            ));
        }
        assert_eq!(
            region_id, self.second_region,
            "the persisted limiter must prevent a third regional ESI request"
        );
        self.second_started.notify_one();
        self.second_release.notified().await;
        Ok(EsiResponse::fresh(vec![], expiring_page(1)))
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        panic!("the limiter fixture has no contract manifests")
    }
}

#[async_trait]
impl ContractRequestPacer for AdvancingRequestPacer {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }

    async fn wait_until(&self, deadline: DateTime<Utc>) {
        self.waits.lock().unwrap().push(deadline);
        let mut now = self.now.lock().unwrap();
        *now = std::cmp::max(*now, deadline);
    }
}

impl GatedAdvancingRequestPacer {
    fn advance_to(&self, now: DateTime<Utc>) {
        *self.now.lock().unwrap() = now;
    }
}

#[async_trait]
impl ContractRequestPacer for GatedAdvancingRequestPacer {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }

    async fn wait_until(&self, deadline: DateTime<Utc>) {
        self.waits.lock().unwrap().push(deadline);
        self.wait_entered.notify_one();
        self.wait_release.notified().await;
        let mut now = self.now.lock().unwrap();
        *now = std::cmp::max(*now, deadline);
    }
}

#[async_trait]
impl PublicContractEsi for TimestampedRateBucketEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.calls
            .lock()
            .unwrap()
            .push(("regions".to_string(), self.pacer.now()));
        Ok(EsiResponse::fresh(
            self.regions.clone(),
            self.discovery_metadata.clone(),
        ))
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        assert_eq!(page, 1, "the rate-bucket fixture has one page per region");
        self.calls
            .lock()
            .unwrap()
            .push((format!("page:{region_id}"), self.pacer.now()));
        Ok(EsiResponse::fresh(
            vec![],
            self.page_metadata.lock().unwrap().remove(0),
        ))
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        panic!("the rate-bucket fixture has no contract manifests")
    }
}

#[async_trait]
impl PublicContractEsi for DelayedSameGroupPacingEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        Ok(EsiResponse::fresh(
            vec![10_000_002, 10_000_003, 10_000_004],
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        assert_eq!(page, 1, "the delayed-rate fixture has one page per region");
        self.calls
            .lock()
            .unwrap()
            .push((region_id, self.pacer.now()));
        match region_id {
            10_000_002 => Ok(EsiResponse::fresh(
                vec![],
                CacheMetadata {
                    rate_limit_group: Some("public-contracts".to_string()),
                    rate_limit_limit: Some("20/60s".to_string()),
                    rate_limit_remaining: Some(2),
                    ..expiring_page(1)
                },
            )),
            10_000_003 => {
                self.second_started.notify_one();
                self.second_release.notified().await;
                Ok(EsiResponse::fresh(
                    vec![],
                    CacheMetadata {
                        rate_limit_group: Some("public-contracts".to_string()),
                        rate_limit_limit: Some("20/60s".to_string()),
                        rate_limit_remaining: Some(18),
                        ..expiring_page(1)
                    },
                ))
            }
            10_000_004 => Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            region_id => panic!("unexpected delayed-rate fixture region {region_id}"),
        }
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        panic!("the delayed-rate fixture has no contract manifests")
    }
}

#[async_trait]
impl PublicContractEsi for GatedProbeBatchEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn public_contract_items_probe(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        match self.probe_count.fetch_add(1, Ordering::SeqCst) {
            0 => {
                self.first_probe_started.notify_one();
                self.first_probe_release.notified().await;
            }
            1 => {
                self.second_probe_started.notify_one();
                self.second_probe_release.notified().await;
            }
            _ => panic!("the controlled terminal-probe fixture permits exactly two probes"),
        }
        self.probes.lock().unwrap().remove(0)
    }
}

#[async_trait]
impl PublicContractEsi for ExhaustedWireManifestEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        Ok(EsiResponse::fresh(
            vec![10_000_002],
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    async fn public_contracts_page(
        &self,
        _region_id: i64,
        _page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        Ok(EsiResponse::fresh(
            self.contracts.clone(),
            CacheMetadata::page_cached_for_seconds(1, 0),
        ))
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        if contract_id == self.unavailable_contract_id {
            self.http.public_contract_items(contract_id, etag).await
        } else {
            Ok(self
                .manifests
                .get(&contract_id)
                .expect("successful manifest configured")
                .clone())
        }
    }
}

#[async_trait]
impl PublicContractEsi for HttpRegionsAndTerminalProbeEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.http.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        _region_id: i64,
        page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        assert_eq!(
            page, 1,
            "the terminal-probe wire fixture has one summary page"
        );
        Ok(EsiResponse::fresh(vec![], expiring_page(1)))
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        panic!("the terminal-probe wire fixture has no summary manifests")
    }

    async fn public_contract_items_probe(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        self.http
            .public_contract_items_probe(contract_id, etag)
            .await
    }
}

struct FailingContractEsi;

struct PausedFailingContractEsi {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct SingleInlineKillmailFeed {
    item: Mutex<Option<ZkDataNoEsi>>,
}

#[async_trait]
impl KillmailFeed for SingleInlineKillmailFeed {
    async fn next(&self) -> Result<Option<ZkDataNoEsi>, FeedError> {
        if let Some(item) = self.item.lock().await.take() {
            return Ok(Some(item));
        }
        std::future::pending().await
    }

    fn health_provider(&self) -> FeedHealthProvider {
        FeedHealthProvider::R2z2
    }
}

struct ParseFailingR2z2Feed;

#[async_trait]
impl KillmailFeed for ParseFailingR2z2Feed {
    async fn next(&self) -> Result<Option<ZkDataNoEsi>, FeedError> {
        Err(FeedError::Parse(
            "synthetic malformed R2Z2 envelope".to_string(),
        ))
    }

    fn health_provider(&self) -> FeedHealthProvider {
        FeedHealthProvider::R2z2
    }
}

struct TransportFailingR2z2Feed {
    called: Arc<Notify>,
}

#[async_trait]
impl KillmailFeed for TransportFailingR2z2Feed {
    async fn next(&self) -> Result<Option<ZkDataNoEsi>, FeedError> {
        self.called.notify_one();
        Err(FeedError::Transport(
            "synthetic R2Z2 connection failure".to_string(),
        ))
    }

    fn health_provider(&self) -> FeedHealthProvider {
        FeedHealthProvider::R2z2
    }
}

#[async_trait]
impl PublicContractEsi for FailingContractEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        Err(EsiError::retryable("ESI is unavailable", None))
    }

    async fn public_contracts_page(
        &self,
        _region_id: i64,
        _page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        Err(EsiError::retryable("ESI is unavailable", None))
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        Err(EsiError::retryable("ESI is unavailable", None))
    }
}

#[async_trait]
impl PublicContractEsi for PausedFailingContractEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.entered.notify_one();
        self.release.notified().await;
        Err(EsiError::retryable("ESI is unavailable", None))
    }

    async fn public_contracts_page(
        &self,
        _region_id: i64,
        _page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        Err(EsiError::retryable("ESI is unavailable", None))
    }

    async fn public_contract_items(
        &self,
        _contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        Err(EsiError::retryable("ESI is unavailable", None))
    }
}

#[async_trait]
impl PublicContractEsi for SnapshottingEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_embed_context(
        &self,
        contract: &PublicContract,
        _items: &[PublicContractItem],
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        self.calls.lock().unwrap().push(contract.contract_id);
        self.contexts.lock().unwrap().remove(0)
    }
}

#[async_trait]
impl ContractContextLimiter for BoundaryContextLimiter {
    async fn request_allowed(&self) -> Result<bool, EsiError> {
        *self.request_checks.lock().unwrap() += 1;
        Ok(!*self.blocked.lock().unwrap())
    }

    async fn record_response(&self, metadata: &CacheMetadata) -> Result<(), EsiError> {
        *self.responses.lock().unwrap() += 1;
        if metadata.rate_limit_remaining == Some(0) {
            *self.blocked.lock().unwrap() = true;
        }
        Ok(())
    }
}

#[async_trait]
impl PublicContractEsi for ContextualFakeEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_context(
        &self,
        _contract: &PublicContract,
        _requirements: ContractContextRequirements,
    ) -> Result<EsiResponse<ContractObservationContext>, EsiError> {
        Ok(EsiResponse::fresh(
            self.context.clone(),
            CacheMetadata::cached_for_seconds(0),
        ))
    }
}

#[async_trait]
impl PublicContractEsi for PositionEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_context(
        &self,
        contract: &PublicContract,
        _requirements: ContractContextRequirements,
    ) -> Result<EsiResponse<ContractObservationContext>, EsiError> {
        Ok(EsiResponse::fresh(
            self.contexts
                .get(&contract.contract_id)
                .cloned()
                .unwrap_or_default(),
            CacheMetadata::cached_for_seconds(60),
        ))
    }

    async fn solar_system_position(
        &self,
        solar_system_id: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<SolarSystemPosition>, EsiError> {
        self.position_calls.lock().unwrap().push(solar_system_id);
        self.positions
            .get(&solar_system_id)
            .copied()
            .map(|position| EsiResponse::fresh(position, CacheMetadata::cached_for_seconds(60)))
            .ok_or_else(|| EsiError::retryable("position unavailable", None))
    }
}

#[async_trait]
impl PublicContractEsi for SecurityBySystemPositionEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_context(
        &self,
        contract: &PublicContract,
        _requirements: ContractContextRequirements,
    ) -> Result<EsiResponse<ContractObservationContext>, EsiError> {
        Ok(EsiResponse::fresh(
            self.contexts
                .get(&contract.contract_id)
                .cloned()
                .unwrap_or_default(),
            CacheMetadata::cached_for_seconds(60),
        ))
    }

    async fn observed_solar_system_security(
        &self,
        _contract: &PublicContract,
        solar_system_id: i64,
    ) -> Result<EsiResponse<ContractContextValue<f64>>, EsiError> {
        self.security_calls.lock().unwrap().push(solar_system_id);
        self.security_statuses
            .get(&solar_system_id)
            .copied()
            .map(|security_status| {
                EsiResponse::fresh(
                    ContractContextValue::Resolved(security_status),
                    CacheMetadata::cached_for_seconds(60),
                )
            })
            .ok_or_else(|| EsiError::retryable("security unavailable", None))
    }

    async fn solar_system_position(
        &self,
        solar_system_id: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<SolarSystemPosition>, EsiError> {
        self.positions
            .get(&solar_system_id)
            .copied()
            .map(|position| EsiResponse::fresh(position, CacheMetadata::cached_for_seconds(60)))
            .ok_or_else(|| EsiError::retryable("position unavailable", None))
    }
}

#[async_trait]
impl PublicContractEsi for PositionedResolutionEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn public_contract_items_probe(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        self.inner
            .public_contract_items_probe(contract_id, etag)
            .await
    }

    async fn solar_system_position(
        &self,
        solar_system_id: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<SolarSystemPosition>, EsiError> {
        self.position_calls.lock().unwrap().push(solar_system_id);
        self.positions
            .get(&solar_system_id)
            .copied()
            .map(|position| EsiResponse::fresh(position, CacheMetadata::cached_for_seconds(60)))
            .ok_or_else(|| EsiError::retryable("position unavailable", None))
    }
}

#[async_trait]
impl PublicContractEsi for LimitedPositionEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_context(
        &self,
        contract: &PublicContract,
        requirements: ContractContextRequirements,
    ) -> Result<EsiResponse<ContractObservationContext>, EsiError> {
        self.inner
            .observed_contract_context(contract, requirements)
            .await
    }

    async fn solar_system_position(
        &self,
        solar_system_id: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<SolarSystemPosition>, EsiError> {
        let mut response = self
            .inner
            .solar_system_position(solar_system_id, etag)
            .await?;
        if self.inner.position_calls.lock().unwrap().len() == 1 {
            response.metadata.rate_limit_limit = Some("1/1m".to_string());
            response.metadata.rate_limit_remaining = Some(0);
        }
        Ok(response)
    }
}

#[async_trait]
impl PublicContractEsi for CountingContextEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_context(
        &self,
        _contract: &PublicContract,
        _requirements: ContractContextRequirements,
    ) -> Result<EsiResponse<ContractObservationContext>, EsiError> {
        *self.context_calls.lock().unwrap() += 1;
        Ok(EsiResponse::fresh(
            ContractObservationContext::default(),
            CacheMetadata::cached_for_seconds(0),
        ))
    }
}

#[async_trait]
impl PublicContractEsi for StopsAfterRateBoundaryEsi {
    async fn regions(&self, _etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.calls.lock().unwrap().push("regions".to_string());
        Ok(EsiResponse::fresh(
            vec![10_000_002, 10_000_003],
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        _page: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.calls.lock().unwrap().push(format!("page:{region_id}"));
        Ok(EsiResponse::fresh(
            vec![item_exchange_contract(region_id)],
            CacheMetadata::page_cached_for_seconds(1, 0),
        ))
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("items:{contract_id}"));
        Err(EsiError::retryable("rate limited", Some(self.retry_after)))
    }
}

#[async_trait]
impl PublicContractEsi for StopsBetweenContextCallsEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn observed_contract_solar_system(
        &self,
        _contract: &PublicContract,
    ) -> Result<EsiResponse<ContractContextValue<i64>>, EsiError> {
        self.calls.lock().unwrap().push("solar-system".to_string());
        Ok(EsiResponse::fresh(
            ContractContextValue::Resolved(30_000_142),
            CacheMetadata {
                rate_limit_limit: Some("1/1m".to_string()),
                rate_limit_remaining: Some(0),
                ..CacheMetadata::cached_for_seconds(0)
            },
        ))
    }

    async fn observed_solar_system_security(
        &self,
        _contract: &PublicContract,
        _solar_system_id: i64,
    ) -> Result<EsiResponse<ContractContextValue<f64>>, EsiError> {
        self.calls.lock().unwrap().push("security".to_string());
        Ok(EsiResponse::fresh(
            ContractContextValue::Resolved(-0.5),
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    async fn observed_issuer_affiliation(
        &self,
        _contract: &PublicContract,
    ) -> Result<EsiResponse<ContractContextValue<i64>>, EsiError> {
        self.calls.lock().unwrap().push("affiliation".to_string());
        Ok(EsiResponse::fresh(
            ContractContextValue::Resolved(99_000_111),
            CacheMetadata::cached_for_seconds(0),
        ))
    }
}

struct StaticShipGroups(HashMap<i64, i64>);

struct CountingShipGroups {
    groups: HashMap<i64, i64>,
    calls: AtomicU64,
}

#[async_trait]
impl ShipGroupResolver for StaticShipGroups {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup {
        ShipGroupLookup::Resolved(self.0.get(&type_id).copied())
    }
}

#[async_trait]
impl ShipGroupResolver for CountingShipGroups {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup {
        self.calls.fetch_add(1, Ordering::Relaxed);
        ShipGroupLookup::Resolved(self.groups.get(&type_id).copied())
    }
}

struct EventuallyResolvedShipGroup {
    results: StdMutex<Vec<ShipGroupLookup>>,
}

#[async_trait]
impl ShipGroupResolver for EventuallyResolvedShipGroup {
    async fn group_for_type(&self, _type_id: i64) -> ShipGroupLookup {
        self.results.lock().unwrap().remove(0)
    }
}

struct RecordingDelivery {
    store: ContractCollectionStore,
    sent: StdMutex<Vec<PreparedContractDelivery>>,
}

struct NotifyingDelivery {
    store: ContractCollectionStore,
    sent: StdMutex<Vec<PreparedContractDelivery>>,
    delivered: Arc<Notify>,
}

struct FirstDeliveryGate {
    store: ContractCollectionStore,
    sent: StdMutex<Vec<PreparedContractDelivery>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    gate_open: AtomicBool,
}

struct NoopDelivery;

struct ScriptedDelivery {
    store: ContractCollectionStore,
    attempts: StdMutex<Vec<PreparedContractDelivery>>,
    outcomes: StdMutex<Vec<Result<String, ContractDeliveryError>>>,
}

struct RecordingContractPingLimiter {
    outcomes: StdMutex<Vec<bool>>,
    channels: StdMutex<Vec<u64>>,
}

struct FixedDeliveryClock(StdMutex<chrono::DateTime<Utc>>);

struct FixedHealthClock(StdMutex<chrono::DateTime<Utc>>);

impl FixedHealthClock {
    fn advance(&self, duration: chrono::Duration) {
        *self.0.lock().unwrap() += duration;
    }
}

impl HealthClock for FixedHealthClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

struct ResolutionEsi {
    inner: FakeEsi,
    probes: StdMutex<Vec<Result<ContractItemProbe, EsiError>>>,
    probe_calls: StdMutex<Vec<(i64, Option<String>)>>,
}

#[async_trait]
impl PublicContractEsi for ResolutionEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.inner.regions(etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.inner
            .public_contracts_page(region_id, page, etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.inner.public_contract_items(contract_id, etag).await
    }

    async fn public_contract_items_probe(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        self.probe_calls
            .lock()
            .unwrap()
            .push((contract_id, etag.map(str::to_owned)));
        self.probes.lock().unwrap().remove(0)
    }
}

struct RemovingDelivery {
    store: ContractCollectionStore,
    sent: StdMutex<Vec<PreparedContractDelivery>>,
}

#[async_trait]
impl ContractDelivery for RemovingDelivery {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        assert!(self
            .store
            .remove_contract_subscription(
                delivery.guild_id,
                delivery.channel_id,
                &delivery.subscription_id,
            )
            .await
            .expect("unsubscribe while a delivery is prepared"));
        self.sent.lock().unwrap().push(delivery);
        Ok("discord-message-id-after-unsubscribe".to_string())
    }
}

#[async_trait]
impl ContractDelivery for RecordingDelivery {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        let record = self
            .store
            .delivery_record(delivery.delivery_id)
            .await
            .expect("read the persisted delivery before Discord is called")
            .expect("delivery exists before Discord is called");
        assert_eq!(record.status, DeliveryStatus::Prepared);
        self.sent.lock().unwrap().push(delivery);
        Ok("discord-message-id".to_string())
    }
}

#[async_trait]
impl ContractDelivery for NotifyingDelivery {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        let record = self
            .store
            .delivery_record(delivery.delivery_id)
            .await
            .expect("read the persisted delivery before Discord is called")
            .expect("delivery exists before Discord is called");
        assert_eq!(record.status, DeliveryStatus::Prepared);
        self.sent.lock().unwrap().push(delivery);
        self.delivered.notify_one();
        Ok("discord-message-id".to_string())
    }
}

#[async_trait]
impl ContractDelivery for FirstDeliveryGate {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        let record = self
            .store
            .delivery_record(delivery.delivery_id)
            .await
            .expect("read the persisted delivery before Discord is called")
            .expect("delivery exists before Discord is called");
        assert_eq!(record.status, DeliveryStatus::Prepared);
        if self.gate_open.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.sent.lock().unwrap().push(delivery);
        Ok("discord-message-id".to_string())
    }
}

#[async_trait]
impl ContractDelivery for NoopDelivery {
    async fn send(
        &self,
        _delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        Ok("ignored".to_string())
    }
}

#[async_trait]
impl ContractDelivery for ScriptedDelivery {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        let record = self
            .store
            .delivery_record(delivery.delivery_id)
            .await
            .expect("read the persisted delivery before Discord is called")
            .expect("delivery exists before Discord is called");
        assert_eq!(record.status, DeliveryStatus::Prepared);
        self.attempts.lock().unwrap().push(delivery);
        self.outcomes.lock().unwrap().remove(0)
    }
}

#[async_trait]
impl ContractPingLimiter for RecordingContractPingLimiter {
    async fn try_acquire(&self, channel_id: u64) -> bool {
        self.channels.lock().unwrap().push(channel_id);
        self.outcomes.lock().unwrap().remove(0)
    }
}

impl ContractDeliveryClock for FixedDeliveryClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

fn contract_subscription(
    id: &str,
    direction: ContractItemDirection,
    type_ids: Vec<i64>,
    ship_group_ids: Vec<i64>,
    listed: ContractEventAction,
) -> ContractSubscription {
    ContractSubscription {
        guild_id: 42,
        channel_id: 77,
        id: id.to_string(),
        description: format!("{id} subscription"),
        filter: ContractFilter::listed_ship(direction, type_ids, ship_group_ids),
        event_actions: ContractEventActions {
            listed,
            ..ContractEventActions::default()
        },
    }
}

fn confirmed_ship_subscription(
    id: &str,
    event_kind: ContractEventKind,
    direction: ContractItemDirection,
    action: ContractEventAction,
) -> ContractSubscription {
    ContractSubscription {
        guild_id: 42,
        channel_id: 77,
        id: id.to_string(),
        description: format!("{id} subscription"),
        filter: ContractFilter {
            root: ContractFilterNode::And(vec![
                ContractFilterNode::Condition(ContractFilterCondition::EventKinds(vec![
                    event_kind,
                ])),
                ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                    direction,
                    ids: vec![587],
                }),
            ]),
        },
        event_actions: ContractEventActions {
            listed: ContractEventAction::Ignore,
            sale_confirmed: if event_kind == ContractEventKind::SaleConfirmed {
                action
            } else {
                ContractEventAction::Ignore
            },
            purchase_confirmed: if event_kind == ContractEventKind::PurchaseConfirmed {
                action
            } else {
                ContractEventAction::Ignore
            },
            ..ContractEventActions::default()
        },
    }
}

fn terminal_ship_subscription(
    id: &str,
    event_kind: ContractEventKind,
    action: ContractEventAction,
) -> ContractSubscription {
    ContractSubscription {
        guild_id: 42,
        channel_id: 77,
        id: id.to_string(),
        description: format!("{id} subscription"),
        filter: ContractFilter {
            root: ContractFilterNode::And(vec![
                ContractFilterNode::Condition(ContractFilterCondition::EventKinds(vec![
                    event_kind,
                ])),
                ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                    direction: ContractItemDirection::Offered,
                    ids: vec![587],
                }),
            ]),
        },
        event_actions: ContractEventActions {
            listed: ContractEventAction::Ignore,
            expired: if event_kind == ContractEventKind::Expired {
                action
            } else {
                ContractEventAction::Ignore
            },
            closed_outcome_unknown: if event_kind == ContractEventKind::ClosedOutcomeUnknown {
                action
            } else {
                ContractEventAction::Ignore
            },
            ..ContractEventActions::default()
        },
    }
}

fn item_exchange_contract(contract_id: i64) -> PublicContract {
    PublicContract {
        contract_id,
        availability: Some("public".to_string()),
        buyout: None,
        collateral: Some(0.0),
        contract_type: "item_exchange".to_string(),
        date_expired: Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap(),
        date_issued: Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap(),
        days_to_complete: 0,
        end_location_id: Some(60_003_760),
        for_corporation: false,
        issuer_corporation_id: 98_000_001,
        issuer_id: 90_000_001,
        price: 1_500_000_000.0,
        reward: 0.0,
        start_location_id: 60_003_760,
        title: Some("this title must not determine the manifest".to_string()),
        volume: 115_000.0,
    }
}

fn expiring_page(expected_pages: u32) -> CacheMetadata {
    CacheMetadata::page_cached_for_seconds(expected_pages, 0)
}

fn expiring_cache() -> CacheMetadata {
    CacheMetadata::cached_for_seconds(0)
}

fn cached_item_metadata(etag: &str, seconds: i64) -> CacheMetadata {
    CacheMetadata {
        etag: Some(etag.to_string()),
        ..CacheMetadata::cached_for_seconds(seconds)
    }
}

fn regional_esi(
    contracts: Vec<PublicContract>,
    items: HashMap<i64, Result<EsiResponse<Vec<PublicContractItem>>, EsiError>>,
) -> FakeEsi {
    FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(contracts, expiring_page(1))),
        )]),
        items,
    }
}

fn position_at_light_years(x: f64) -> SolarSystemPosition {
    SolarSystemPosition {
        x: x * 9_460_730_472_580_800.0,
        y: 0.0,
        z: 0.0,
    }
}

fn offered_ship(record_id: i64) -> PublicContractItem {
    PublicContractItem {
        record_id,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_000 + record_id),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    }
}

fn requested_ship(record_id: i64) -> PublicContractItem {
    PublicContractItem {
        is_included: false,
        ..offered_ship(record_id)
    }
}

#[test]
fn collection_failures_use_bounded_exponential_backoff_without_violating_retry_after() {
    let interval = Duration::from_secs(1);
    assert_eq!(collection_retry_delay(interval, None, 1), interval);
    assert_eq!(
        collection_retry_delay(interval, None, 2),
        Duration::from_secs(2)
    );
    assert_eq!(
        collection_retry_delay(interval, None, 99),
        Duration::from_secs(300)
    );
    let retry_after = Utc::now() + chrono::Duration::seconds(30);
    assert!(collection_retry_delay(interval, Some(retry_after), 1) >= Duration::from_secs(29));
}

#[test]
fn contract_regional_concurrency_is_conservative_and_strict() {
    assert_eq!(contract_regional_concurrency_from(None).unwrap(), 2);
    assert_eq!(contract_regional_concurrency_from(Some("1")).unwrap(), 1);
    assert_eq!(contract_regional_concurrency_from(Some("4")).unwrap(), 4);
    for value in ["0", "5", "two", " 2", "2 "] {
        assert!(
            contract_regional_concurrency_from(Some(value)).is_err(),
            "{value:?} must not silently change the collection rate"
        );
    }
}

#[tokio::test]
async fn pre_expiry_no_content_confirms_only_pure_matched_ship_sales_and_purchases() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut sale = item_exchange_contract(44);
    sale.date_expired = Utc::now() + chrono::Duration::hours(1);
    let mut purchase = item_exchange_contract(45);
    purchase.date_expired = Utc::now() + chrono::Duration::hours(1);
    purchase.price = 0.0;
    purchase.reward = 1_500_000_000.0;
    let mut zero_isk = item_exchange_contract(46);
    zero_isk.date_expired = Utc::now() + chrono::Duration::hours(1);
    zero_isk.price = 0.0;
    let mut malformed_isk = item_exchange_contract(47);
    malformed_isk.date_expired = Utc::now() + chrono::Duration::hours(1);
    malformed_isk.price = -1.0;
    let mut barter = item_exchange_contract(48);
    barter.date_expired = Utc::now() + chrono::Duration::hours(1);
    let baseline_items = HashMap::from([
        (
            44,
            Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
        ),
        (
            45,
            Ok(EsiResponse::fresh(
                vec![requested_ship(2)],
                expiring_cache(),
            )),
        ),
        (
            46,
            Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
        ),
        (
            47,
            Ok(EsiResponse::fresh(vec![offered_ship(4)], expiring_cache())),
        ),
        (
            48,
            Ok(EsiResponse::fresh(
                vec![offered_ship(5), requested_ship(6), requested_ship(7)],
                expiring_cache(),
            )),
        ),
    ]);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![
                        sale.clone(),
                        purchase.clone(),
                        zero_isk.clone(),
                        malformed_isk.clone(),
                        barter.clone(),
                    ],
                    expiring_page(1),
                )),
            )]),
            items: baseline_items,
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "sale",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::PostAndPing,
        ))
        .await
        .expect("persist sale subscription");
    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "purchase",
            ContractEventKind::PurchaseConfirmed,
            ContractItemDirection::Requested,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist purchase subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let resolver = ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![
            Ok(ContractItemProbe::NoContent(expiring_cache())),
            Ok(ContractItemProbe::NoContent(expiring_cache())),
            Ok(ContractItemProbe::NoContent(expiring_cache())),
            Ok(ContractItemProbe::NoContent(expiring_cache())),
            Ok(ContractItemProbe::NoContent(expiring_cache())),
        ]),
        probe_calls: StdMutex::new(Vec::new()),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(resolver)).with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
        delivery.clone(),
    );

    let report = collector
        .collect_cycle()
        .await
        .expect("resolve the disappeared public contracts");

    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![
            ContractEventKind::SaleConfirmed,
            ContractEventKind::PurchaseConfirmed,
        ]
    );
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().any(|message| message.ping));
    assert!(sent.iter().all(|message| {
        message
            .message
            .fields
            .iter()
            .any(|field| field.name == "Evidence")
            && message
                .message
                .fields
                .iter()
                .any(|field| field.name == "Delay")
    }));
    drop(sent);

    let resolutions = store
        .contract_resolution_records()
        .await
        .expect("read retained acceptance evidence");
    assert_eq!(resolutions.len(), 5);
    assert!(resolutions
        .iter()
        .all(|record| record.state == ContractResolutionState::AcceptanceConfirmed));
    let (issuer_history, corporation_history) = store
        .contract_party_history(sale.issuer_id, sale.issuer_corporation_id)
        .await
        .expect("summarize local confirmed contract history");
    assert_eq!(issuer_history.confirmed_sales, 1);
    assert_eq!(issuer_history.confirmed_purchases, 1);
    assert_eq!(issuer_history.unknown_closures, 0);
    assert_eq!(corporation_history, issuer_history);

    let history_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to adjust retained local history");
    sqlx::query(
        "UPDATE contract_resolution_cases \
         SET acceptance_evidence_at = CASE contract_id \
             WHEN 44 THEN now() - interval '90 days' - interval '1 second' \
             WHEN 45 THEN now() - interval '90 days' + interval '1 second' \
         END \
         WHERE contract_id IN (44, 45)",
    )
    .execute(&history_pool)
    .await
    .expect("place one history event on each side of the 90-day boundary");
    history_pool.close().await;
    let (issuer_history, _) = store
        .contract_party_history(sale.issuer_id, sale.issuer_corporation_id)
        .await
        .expect("exclude old history while retaining an in-window boundary event");
    assert_eq!(issuer_history.confirmed_sales, 0);
    assert_eq!(issuer_history.confirmed_purchases, 1);
    assert_eq!(
        issuer_history
            .most_recent_confirmed
            .as_ref()
            .map(|event| event.kind),
        Some(ContractEventKind::PurchaseConfirmed)
    );
    let relisted = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![
                        sale.clone(),
                        purchase.clone(),
                        zero_isk.clone(),
                        malformed_isk.clone(),
                        barter.clone(),
                    ],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![requested_ship(2)],
                        expiring_cache(),
                    )),
                ),
                (
                    46,
                    Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
                ),
                (
                    47,
                    Ok(EsiResponse::fresh(vec![offered_ship(4)], expiring_cache())),
                ),
                (
                    48,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship(5), requested_ship(6), requested_ship(7)],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("a later public observation cannot invalidate acceptance evidence");
    assert!(relisted.events.is_empty());

    let repeated = collector
        .collect_cycle()
        .await
        .expect("leave positive acceptance evidence terminal");
    assert!(repeated.events.is_empty());
    assert_eq!(delivery.sent.lock().unwrap().len(), 2);
    database.destroy().await;
}

#[tokio::test]
async fn contract_cycle_release_seam_preserves_all_public_lifecycle_outcomes_and_deliveries() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut sale = item_exchange_contract(44);
    sale.date_expired = Utc::now() + chrono::Duration::hours(1);
    let mut purchase = item_exchange_contract(45);
    purchase.date_expired = Utc::now() + chrono::Duration::hours(1);
    purchase.price = 0.0;
    purchase.reward = 1_500_000_000.0;
    let mut expired = item_exchange_contract(46);
    expired.date_expired = Utc::now() - chrono::Duration::minutes(1);
    let mut unknown = item_exchange_contract(47);
    unknown.date_expired = Utc::now() + chrono::Duration::hours(1);
    let mut listed = item_exchange_contract(48);
    listed.date_expired = Utc::now() + chrono::Duration::hours(1);

    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let ship_groups = Arc::new(StaticShipGroups(HashMap::from([(587, 659)])));
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![
                        sale.clone(),
                        purchase.clone(),
                        expired.clone(),
                        unknown.clone(),
                    ],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    sale.contract_id,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship(1)],
                        cached_item_metadata("sale-v1", 3_600),
                    )),
                ),
                (
                    purchase.contract_id,
                    Ok(EsiResponse::fresh(
                        vec![requested_ship(2)],
                        cached_item_metadata("purchase-v1", 3_600),
                    )),
                ),
                (
                    expired.contract_id,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship(3)],
                        cached_item_metadata("expired-v1", 3_600),
                    )),
                ),
                (
                    unknown.contract_id,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship(4)],
                        cached_item_metadata("unknown-v1", 3_600),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(ship_groups.clone(), delivery.clone())
    .collect_cycle()
    .await
    .expect("establish a silent regional baseline");
    assert!(delivery.sent.lock().unwrap().is_empty());

    for subscription in [
        contract_subscription(
            "listed",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ),
        confirmed_ship_subscription(
            "sale",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ),
        confirmed_ship_subscription(
            "purchase",
            ContractEventKind::PurchaseConfirmed,
            ContractItemDirection::Requested,
            ContractEventAction::Post,
        ),
        terminal_ship_subscription(
            "expired",
            ContractEventKind::Expired,
            ContractEventAction::Post,
        ),
        terminal_ship_subscription(
            "unknown",
            ContractEventKind::ClosedOutcomeUnknown,
            ContractEventAction::Post,
        ),
        contract_subscription(
            "nonmatching",
            ContractItemDirection::Offered,
            vec![999],
            vec![],
            ContractEventAction::Post,
        ),
    ] {
        store
            .upsert_contract_subscription(&subscription)
            .await
            .expect("persist lifecycle subscription");
    }

    let first_transition = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![listed.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                listed.contract_id,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(5)],
                    cached_item_metadata("listed-v1", 3_600),
                )),
            )]),
        }),
    )
    .with_notifications(ship_groups.clone(), delivery.clone())
    .collect_cycle()
    .await
    .expect("record a listing and public disappearance");
    assert!(first_transition
        .events
        .iter()
        .any(|event| event.kind == ContractEventKind::Listed));
    assert!(first_transition
        .events
        .iter()
        .any(|event| event.kind == ContractEventKind::Expired));

    let records = store
        .contract_resolution_records()
        .await
        .expect("persist awaiting and terminal lifecycle state");
    assert!(records.iter().any(|record| {
        record.contract_id == sale.contract_id
            && record.state == ContractResolutionState::AwaitingResolution
    }));
    assert!(records.iter().any(|record| {
        record.contract_id == purchase.contract_id
            && record.state == ContractResolutionState::AwaitingResolution
    }));
    assert!(records.iter().any(|record| {
        record.contract_id == unknown.contract_id
            && record.state == ContractResolutionState::AwaitingResolution
    }));
    assert!(records.iter().any(|record| {
        record.contract_id == expired.contract_id
            && record.state == ContractResolutionState::Expired
    }));

    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to release-seam database");
    sqlx::query(
        "UPDATE contract_resolution_cases SET next_probe_at = now() - interval '1 second' WHERE contract_id IN (44,45,47)",
    )
    .execute(&raw_pool)
    .await
    .expect("make persisted awaiting cases eligible for public resolution");
    sqlx::query(
        "UPDATE esi_cache_metadata SET expires_at = now() - interval '1 second' WHERE resource_key IN ('contracts/public/items/44','contracts/public/items/45','contracts/public/items/47')",
    )
    .execute(&raw_pool)
    .await
    .expect("expire cached public item representations before resolution probes");
    raw_pool.close().await;

    let resolved = ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![listed], expiring_page(1))),
                )]),
                items: HashMap::new(),
            },
            probes: StdMutex::new(vec![
                Ok(ContractItemProbe::NoContent(expiring_cache())),
                Ok(ContractItemProbe::NoContent(expiring_cache())),
                Ok(ContractItemProbe::NotFound(expiring_cache())),
            ]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(ship_groups, delivery.clone())
    .collect_cycle()
    .await
    .expect("resolve persisted public evidence");
    assert!(resolved
        .events
        .iter()
        .any(|event| event.kind == ContractEventKind::SaleConfirmed));
    assert!(resolved
        .events
        .iter()
        .any(|event| event.kind == ContractEventKind::PurchaseConfirmed));
    assert!(resolved
        .events
        .iter()
        .any(|event| event.kind == ContractEventKind::ClosedOutcomeUnknown));

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 5);
    assert!(sent
        .iter()
        .all(|delivery| delivery.subscription_id != "nonmatching"));
    assert!(sent.iter().all(|delivery| !delivery.ping));
    drop(sent);
    let delivery_records = store
        .delivery_records()
        .await
        .expect("prepared deliveries are committed before send");
    assert_eq!(delivery_records.len(), 5);
    assert!(delivery_records
        .iter()
        .all(|record| record.status == DeliveryStatus::Sent));
    database.destroy().await;
}

#[tokio::test]
async fn a_resolution_probe_failure_does_not_discard_prior_listings_or_confirmations() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut accepted_sale = item_exchange_contract(44);
    accepted_sale.date_expired = Utc::now() + chrono::Duration::hours(1);
    let mut retryable_resolution = item_exchange_contract(45);
    retryable_resolution.date_expired = Utc::now() + chrono::Duration::hours(1);
    let listed = item_exchange_contract(46);

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![accepted_sale.clone(), retryable_resolution.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "listed",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist listed subscription");
    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "sale",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist sale subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let resolver = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![listed.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                46,
                Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
            )]),
        },
        probes: StdMutex::new(vec![
            Ok(ContractItemProbe::NoContent(expiring_cache())),
            Err(EsiError::retryable("transient item probe failure", None)),
        ]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    let collector = ContractCollector::new(store.clone(), resolver.clone()).with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
        delivery.clone(),
    );

    let first = collector
        .collect_cycle()
        .await
        .expect("the independent resolution failure must not abort the cycle");
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![ContractEventKind::Listed, ContractEventKind::SaleConfirmed]
    );
    assert_eq!(delivery.sent.lock().unwrap().len(), 2);
    let resolutions = store
        .contract_resolution_records()
        .await
        .expect("read resolution retry state");
    assert_eq!(resolutions.len(), 2);
    assert_eq!(
        resolutions[0].state,
        ContractResolutionState::AcceptanceConfirmed
    );
    assert_eq!(
        resolutions[1].state,
        ContractResolutionState::AwaitingResolution
    );
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("read the scoped retryable resolution failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].region_id, Some(10_000_002));
    assert_eq!(failures[0].contract_id, Some(45));
    assert_eq!(
        failures[0].resource_key.as_deref(),
        Some("contracts/public/items/45")
    );
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed an unrelated resolution failure");
    sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail) VALUES ($1,$2,$3,now(),$4,$5)")
        .bind(10_000_002_i64)
        .bind(99_i64)
        .bind("contracts/public/items/99")
        .bind("resolution_probe")
        .bind("unrelated retryable probe")
        .execute(&raw_pool)
        .await
        .expect("seed unrelated resolution failure");
    sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = now() - interval '1 second' WHERE region_id = 10000002 AND contract_id = 45")
        .execute(&raw_pool)
        .await
        .expect("advance the controlled retry boundary before testing restart recovery");
    raw_pool.close().await;

    let restarted_resolver = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![listed], expiring_page(1))),
            )]),
            items: HashMap::from([(
                46,
                Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
            )]),
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::Available(EsiResponse::fresh(
            vec![offered_ship(2)],
            cached_item_metadata("items-45-v2", 60),
        )))]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    let second = ContractCollector::new(store.clone(), restarted_resolver.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect("a restarted collector retries the unresolved independent case");
    assert!(second.events.is_empty());
    assert_eq!(delivery.sent.lock().unwrap().len(), 2);
    assert_eq!(
        resolver.probe_calls.lock().unwrap().as_slice(),
        [(44, None), (45, None)]
    );
    assert_eq!(
        restarted_resolver.probe_calls.lock().unwrap().as_slice(),
        [(45, None)]
    );
    let remaining_failures = store
        .unresolved_collection_failures()
        .await
        .expect("successful retry resolves only its matching failure");
    assert_eq!(remaining_failures.len(), 1);
    assert_eq!(remaining_failures[0].contract_id, Some(99));
    assert_eq!(
        remaining_failures[0].resource_key.as_deref(),
        Some("contracts/public/items/99")
    );
    database.destroy().await;
}

#[tokio::test]
async fn fresh_cached_item_evidence_resolves_only_its_matching_probe_failure_after_restart() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(1)],
                    cached_item_metadata("items-44-v1", 3_600),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish a cached public contract baseline");
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("record a disappearance deferred by fresh cached item evidence");

    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed restart state");
    for (contract_id, resource_key) in [
        (44_i64, "contracts/public/items/44"),
        (99_i64, "contracts/public/items/99"),
    ] {
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail) VALUES (10000002,$1,$2,now(),'resolution_probe','simulated crash before resolution cleanup')")
            .bind(contract_id)
            .bind(resource_key)
            .execute(&raw_pool)
            .await
            .expect("seed scoped probe failure");
    }
    sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = now() - interval '1 second' WHERE region_id = 10000002 AND contract_id = 44")
        .execute(&raw_pool)
        .await
        .expect("make the cached evidence immediately eligible after restart");
    raw_pool.close().await;

    let restarted_esi = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), restarted_esi.clone())
        .collect_cycle()
        .await
        .expect("reuse the persisted 200/304 representation after restart");

    assert!(restarted_esi.probe_calls.lock().unwrap().is_empty());
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("successful cached evidence resolves only the matching failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].contract_id, Some(99));
    assert_eq!(
        failures[0].resource_key.as_deref(),
        Some("contracts/public/items/99")
    );
    database.destroy().await;
}

#[tokio::test]
async fn persisted_no_content_evidence_resolves_only_its_matching_probe_failure_after_restart() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(1)],
                    cached_item_metadata("items-44-v1", 3_600),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish a public contract baseline");
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("record an awaiting resolution");

    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed persisted 204 restart state");
    sqlx::query("UPDATE contract_resolution_cases SET state = 'acceptance_confirmed', acceptance_evidence_at = now(), acceptance_response_metadata = '{}'::jsonb, notification_pending = FALSE WHERE region_id = 10000002 AND contract_id = 44")
        .execute(&raw_pool)
        .await
        .expect("simulate committed 204 evidence before failure cleanup");
    for (contract_id, resource_key) in [
        (44_i64, "contracts/public/items/44"),
        (99_i64, "contracts/public/items/99"),
    ] {
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail) VALUES (10000002,$1,$2,now(),'resolution_probe','simulated crash before resolution cleanup')")
            .bind(contract_id)
            .bind(resource_key)
            .execute(&raw_pool)
            .await
            .expect("seed scoped probe failure");
    }
    raw_pool.close().await;

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("reconcile persisted 204 acceptance evidence after restart");

    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("persisted 204 evidence resolves only the matching failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].contract_id, Some(99));
    assert_eq!(
        failures[0].resource_key.as_deref(),
        Some("contracts/public/items/99")
    );
    database.destroy().await;
}

#[tokio::test]
async fn cached_item_evidence_defers_resolution_for_200_and_conditional_304() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(1)],
                    cached_item_metadata("items-v1", 3_600),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish a cached public contract baseline");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let resolver = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![
            Ok(ContractItemProbe::Available(EsiResponse::fresh(
                vec![offered_ship(1)],
                cached_item_metadata("items-v2", 3_600),
            ))),
            Ok(ContractItemProbe::Available(EsiResponse::not_modified(
                CacheMetadata::cached_for_seconds(3_600),
            ))),
        ]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    let collector = ContractCollector::new(store.clone(), resolver.clone()).with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
        delivery.clone(),
    );

    let first = collector
        .collect_cycle()
        .await
        .expect("cached items cannot establish acceptance");
    assert!(first.events.is_empty());
    assert!(resolver.probe_calls.lock().unwrap().is_empty());
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to the temporary contract database");

    sqlx::query("UPDATE esi_cache_metadata SET expires_at = now() - interval '1 second' WHERE resource_key = 'contracts/public/items/44'")
        .execute(&raw_pool)
        .await
        .expect("expire the cached item representation");
    sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = now() - interval '1 second' WHERE contract_id = 44")
        .execute(&raw_pool)
        .await
        .expect("schedule the cached item probe");
    let second = collector
        .collect_cycle()
        .await
        .expect("a fresh 200 item representation remains non-terminal evidence");
    assert!(second.events.is_empty());

    sqlx::query("UPDATE esi_cache_metadata SET expires_at = now() - interval '1 second' WHERE resource_key = 'contracts/public/items/44'")
        .execute(&raw_pool)
        .await
        .expect("expire the fresh 200 item representation");
    sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = now() - interval '1 second' WHERE contract_id = 44")
        .execute(&raw_pool)
        .await
        .expect("schedule the conditional item probe");
    let third = collector
        .collect_cycle()
        .await
        .expect("a conditional 304 item representation remains non-terminal evidence");
    assert!(third.events.is_empty());
    assert_eq!(
        resolver.probe_calls.lock().unwrap().as_slice(),
        [
            (44, Some("items-v1".to_string())),
            (44, Some("items-v2".to_string()))
        ]
    );
    assert!(delivery.sent.lock().unwrap().is_empty());
    database.destroy().await;
}

#[tokio::test]
async fn expired_and_unknown_terminal_outcomes_use_independent_actions_and_are_idempotent() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut expired = item_exchange_contract(44);
    expired.date_expired = Utc::now() - chrono::Duration::minutes(1);
    let mut unresolved = item_exchange_contract(45);
    unresolved.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![expired.clone(), unresolved.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship(1)],
                        cached_item_metadata("expired-v1", 3_600),
                    )),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&terminal_ship_subscription(
            "expired",
            ContractEventKind::Expired,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist expiry subscription");
    store
        .upsert_contract_subscription(&terminal_ship_subscription(
            "unknown",
            ContractEventKind::ClosedOutcomeUnknown,
            ContractEventAction::PostAndPing,
        ))
        .await
        .expect("persist unknown-outcome subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let resolver = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::NotFound(expiring_cache()))]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(store.clone(), resolver.clone())
        .collect_cycle()
        .await
        .expect("classify only the evidence available to public collection");
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![
            ContractEventKind::Expired,
            ContractEventKind::ClosedOutcomeUnknown,
        ]
    );
    assert!(delivery.sent.lock().unwrap().is_empty());
    assert_eq!(
        store
            .contract_resolution_records()
            .await
            .expect("read terminal resolution records")
            .iter()
            .map(|record| record.state)
            .collect::<Vec<_>>(),
        vec![
            ContractResolutionState::Expired,
            ContractResolutionState::ClosedOutcomeUnknown,
        ]
    );

    let collector = ContractCollector::new(store.clone(), resolver).with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
        delivery.clone(),
    );
    let recovered = collector
        .collect_cycle()
        .await
        .expect("restart and deliver the pending terminal outcomes");
    assert_eq!(
        recovered
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![
            ContractEventKind::Expired,
            ContractEventKind::ClosedOutcomeUnknown,
        ]
    );
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().any(|message| message.ping));
    assert!(sent.iter().any(|message| !message.ping));
    for message in sent.iter() {
        let expected_title = match message.event_kind {
            ContractEventKind::Expired => "expired",
            ContractEventKind::ClosedOutcomeUnknown => "closed (outcome unknown)",
            _ => panic!("expected a nonfinancial terminal event"),
        };
        assert!(message.message.title.contains(expected_title));
        assert!(message
            .message
            .fields
            .iter()
            .all(|field| !matches!(field.name.as_str(), "Event" | "Delay")));
        assert!(message
            .message
            .fields
            .iter()
            .any(|field| field.name == "Evidence"));
    }
    drop(sent);
    assert_eq!(
        store
            .contract_resolution_records()
            .await
            .expect("read terminal resolution records")
            .iter()
            .map(|record| record.state)
            .collect::<Vec<_>>(),
        vec![
            ContractResolutionState::Expired,
            ContractResolutionState::ClosedOutcomeUnknown,
        ]
    );

    let repeated = collector
        .collect_cycle()
        .await
        .expect("terminal lifecycle processing is locally idempotent");
    assert!(repeated.events.is_empty());
    assert_eq!(delivery.sent.lock().unwrap().len(), 2);
    database.destroy().await;
}

#[tokio::test]
async fn no_content_at_or_after_expiry_is_expired_not_acceptance_confirmed() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(1)],
                    cached_item_metadata("items-v1", 3_600),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish public baseline");

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("record disappearance while contract evidence is still unresolved");
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to temporary contract database");
    let expired_at = Utc::now() - chrono::Duration::minutes(1);
    sqlx::query(
        "UPDATE contract_resolution_cases SET contract = jsonb_set(contract, '{date_expired}', to_jsonb($1::text), true), next_probe_at = now() - interval '1 second' WHERE contract_id = 44",
    )
    .bind(expired_at.to_rfc3339())
    .execute(&raw_pool)
    .await
    .expect("move pending resolution to its known expiry boundary");
    sqlx::query(
        "UPDATE esi_cache_metadata SET expires_at = now() - interval '1 second' WHERE resource_key = 'contracts/public/items/44'",
    )
    .execute(&raw_pool)
    .await
    .expect("require a fresh item probe at the expiry boundary");
    let resolver = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::NoContent(expiring_cache()))]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(store.clone(), resolver.clone())
        .collect_cycle()
        .await
        .expect("classify the expiry-boundary no-content response");
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![ContractEventKind::Expired]
    );
    assert_eq!(
        resolver.probe_calls.lock().unwrap().as_slice(),
        [(44, Some("items-v1".to_string()))]
    );
    assert_eq!(
        store
            .contract_resolution_records()
            .await
            .expect("read expiry record")[0]
            .state,
        ContractResolutionState::Expired
    );
    database.destroy().await;
}

#[tokio::test]
async fn reappearing_awaiting_contract_returns_to_observed_without_a_terminal_event() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    let baseline_items = HashMap::from([(
        44,
        Ok(EsiResponse::fresh(
            vec![offered_ship(1)],
            cached_item_metadata("items-v1", 3_600),
        )),
    )]);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: baseline_items.clone(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish public baseline");
    let disappeared = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("record an unresolved disappearance");
    assert!(disappeared.events.is_empty());
    assert_eq!(
        store
            .contract_resolution_records()
            .await
            .expect("read awaiting state")[0]
            .state,
        ContractResolutionState::AwaitingResolution
    );
    let reappeared = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract], expiring_page(1))),
            )]),
            items: baseline_items,
        }),
    )
    .collect_cycle()
    .await
    .expect("return a reappearing contract to observed");
    assert!(reappeared.events.is_empty());
    assert!(store
        .contract_resolution_records()
        .await
        .expect("read cleared awaiting state")
        .is_empty());
    database.destroy().await;
}

async fn resolve_candidate_branch_after_unknown_location(
    resolved_solar_system_id: i64,
) -> (usize, usize, Vec<String>) {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let offered_hel = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let offered_titan = PublicContractItem {
        record_id: 2,
        type_id: 19_720,
        quantity: 1,
        is_included: true,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "local-hel-or-contextual-titan".to_string(),
            description: "Hel or Titan in one system".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Or(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                        direction: ContractItemDirection::Offered,
                        ids: vec![587],
                    }),
                    ContractFilterNode::And(vec![
                        ContractFilterNode::Condition(ContractFilterCondition::SolarSystems(vec![
                            30_000_142,
                        ])),
                        ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                            direction: ContractItemDirection::Offered,
                            ids: vec![19_720],
                        }),
                    ]),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the local-or-contextual-candidate filter");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let unknown_context_esi = Arc::new(CountingContextEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline.clone(), listed.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![offered_hel.clone(), offered_titan.clone()],
                        expiring_cache(),
                    )),
                ),
            ]),
        },
        context_calls: StdMutex::new(0),
    });

    ContractCollector::new(store.clone(), unknown_context_esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 659), (19_720, 30)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect("defer while the candidate-bearing location branch is unresolved");
    let first_context_calls = *unknown_context_esi.context_calls.lock().unwrap();
    let first_deliveries = delivery.sent.lock().unwrap().len();

    ContractCollector::new(
        store.clone(),
        Arc::new(ContextualFakeEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
                )]),
                items: HashMap::from([
                    (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                    (
                        45,
                        Ok(EsiResponse::fresh(
                            vec![offered_hel, offered_titan],
                            expiring_cache(),
                        )),
                    ),
                ]),
            },
            context: ContractObservationContext {
                solar_system_id: Some(resolved_solar_system_id),
                ..ContractObservationContext::default()
            },
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659), (19_720, 30)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("resolve the candidate-bearing location branch on the next cycle");

    let titles = delivery
        .sent
        .lock()
        .unwrap()
        .iter()
        .map(|delivery| delivery.message.title.clone())
        .collect();
    database.destroy().await;
    (first_context_calls, first_deliveries, titles)
}

#[tokio::test]
async fn light_year_ranges_match_either_center_include_the_boundary_and_cache_centers() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = (45..49).map(item_exchange_contract).collect::<Vec<_>>();
    let mut all_contracts = vec![baseline.clone()];
    all_contracts.extend(listed.clone());
    let items = all_contracts
        .iter()
        .map(|contract| {
            (
                contract.contract_id,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(contract.contract_id)],
                    expiring_cache(),
                )),
            )
        })
        .collect::<HashMap<_, _>>();

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline], expiring_page(1))),
            )]),
            items: items.clone(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "near-either-center".to_string(),
            description: "within eight light-years of either center".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
                    config::SystemRange {
                        system_id: 30_002_086,
                        range: 8.0,
                    },
                    config::SystemRange {
                        system_id: 30_003_089,
                        range: 8.0,
                    },
                ])),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the range subscription");
    let persisted = store
        .contract_subscriptions_for_channel(42, 77)
        .await
        .expect("read the persisted range subscription");
    assert!(matches!(
        persisted[0].filter.root,
        ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(ref ranges))
            if ranges == &vec![
                config::SystemRange { system_id: 30_002_086, range: 8.0 },
                config::SystemRange { system_id: 30_003_089, range: 8.0 },
            ]
    ));
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let positions = HashMap::from([
        (30_002_086, position_at_light_years(0.0)),
        (30_003_089, position_at_light_years(20.0)),
        (30_000_045, position_at_light_years(0.0)),
        (30_000_046, position_at_light_years(20.0)),
        (30_000_047, position_at_light_years(10.0)),
        (30_000_048, position_at_light_years(8.0)),
    ]);
    let contexts = HashMap::from([
        (
            45,
            ContractObservationContext {
                solar_system_id: Some(30_000_045),
                ..ContractObservationContext::default()
            },
        ),
        (
            46,
            ContractObservationContext {
                solar_system_id: Some(30_000_046),
                ..ContractObservationContext::default()
            },
        ),
        (
            47,
            ContractObservationContext {
                solar_system_id: Some(30_000_047),
                ..ContractObservationContext::default()
            },
        ),
        (
            48,
            ContractObservationContext {
                solar_system_id: Some(30_000_048),
                ..ContractObservationContext::default()
            },
        ),
    ]);
    let esi = Arc::new(PositionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(all_contracts, expiring_page(1))),
            )]),
            items,
        },
        contexts,
        positions,
        position_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .collect_cycle()
        .await
        .expect("evaluate range positions");

    let sent_contract_ids = delivery
        .sent
        .lock()
        .unwrap()
        .iter()
        .map(|delivery| delivery.contract_id)
        .collect::<Vec<_>>();
    assert_eq!(sent_contract_ids, vec![45, 46, 48]);
    let position_calls = esi.position_calls.lock().unwrap();
    assert_eq!(
        position_calls
            .iter()
            .filter(|system_id| **system_id == 30_002_086)
            .count(),
        1
    );
    assert_eq!(
        position_calls
            .iter()
            .filter(|system_id| **system_id == 30_003_089)
            .count(),
        1
    );
    drop(position_calls);
    database.destroy().await;
}

#[tokio::test]
async fn light_year_ranges_do_not_fetch_positions_after_a_cheap_local_miss() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let items = HashMap::from([
        (
            44,
            Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
        ),
        (
            45,
            Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
        ),
    ]);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(vec![baseline.clone()], items.clone())),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "sale-near-center".to_string(),
            description: "sale events near a center".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::And(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::EventKinds(vec![
                        ContractEventKind::SaleConfirmed,
                    ])),
                    ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
                        config::SystemRange {
                            system_id: 30_002_086,
                            range: 8.0,
                        },
                    ])),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the cheap-miss subscription");
    let esi = Arc::new(PositionEsi {
        inner: regional_esi(vec![baseline, listed], items),
        contexts: HashMap::new(),
        positions: HashMap::new(),
        position_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::new())),
            Arc::new(RecordingDelivery {
                store: store.clone(),
                sent: StdMutex::new(Vec::new()),
            }),
        )
        .collect_cycle()
        .await
        .expect("skip range enrichment after the local event-kind miss");
    assert!(esi.position_calls.lock().unwrap().is_empty());
    database.destroy().await;
}

#[tokio::test]
async fn unavailable_light_year_positions_remain_deferred_even_through_not() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let items = HashMap::from([
        (
            44,
            Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
        ),
        (
            45,
            Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
        ),
    ]);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(vec![baseline.clone()], items.clone())),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    let range = ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
        config::SystemRange {
            system_id: 30_002_086,
            range: 8.0,
        },
    ]));
    for (id, root) in [
        ("unknown-center", range.clone()),
        (
            "not-unknown-center",
            ContractFilterNode::Not(Box::new(range)),
        ),
    ] {
        store
            .upsert_contract_subscription(&ContractSubscription {
                guild_id: 42,
                channel_id: 77,
                id: id.to_string(),
                description: id.to_string(),
                filter: ContractFilter { root },
                event_actions: ContractEventActions {
                    listed: ContractEventAction::Post,
                    ..ContractEventActions::default()
                },
            })
            .await
            .expect("persist the unavailable-position subscription");
    }
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let esi = Arc::new(PositionEsi {
        inner: regional_esi(vec![baseline, listed], items),
        contexts: HashMap::from([(
            45,
            ContractObservationContext {
                solar_system_id: Some(30_000_045),
                ..ContractObservationContext::default()
            },
        )]),
        positions: HashMap::from([(30_000_045, position_at_light_years(0.0))]),
        position_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .collect_cycle()
        .await
        .expect("defer incomplete positions");
    assert!(delivery.sent.lock().unwrap().is_empty());
    assert!(esi.position_calls.lock().unwrap().contains(&30_002_086));
    database.destroy().await;
}

#[tokio::test]
async fn persisted_limiter_stops_the_next_light_year_reference_lookup() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let items = HashMap::from([
        (
            44,
            Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
        ),
        (
            45,
            Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
        ),
    ]);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(vec![baseline.clone()], items.clone())),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "limiter-bounded-centers".to_string(),
            description: "range lookups stop at the persisted limiter".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
                    config::SystemRange {
                        system_id: 30_002_086,
                        range: 8.0,
                    },
                    config::SystemRange {
                        system_id: 30_003_089,
                        range: 8.0,
                    },
                ])),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the two-center subscription");
    let esi = Arc::new(LimitedPositionEsi {
        inner: PositionEsi {
            inner: regional_esi(vec![baseline, listed], items),
            contexts: HashMap::from([(
                45,
                ContractObservationContext {
                    solar_system_id: Some(30_000_045),
                    solar_system_position: Some(position_at_light_years(0.0)),
                    ..ContractObservationContext::default()
                },
            )]),
            positions: HashMap::from([
                (30_002_086, position_at_light_years(0.0)),
                (30_003_089, position_at_light_years(20.0)),
            ]),
            position_calls: StdMutex::new(Vec::new()),
        },
    });
    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::new())),
            Arc::new(RecordingDelivery {
                store: store.clone(),
                sent: StdMutex::new(Vec::new()),
            }),
        )
        .collect_cycle()
        .await
        .expect("defer second center after the limiter boundary");
    assert_eq!(
        esi.inner.position_calls.lock().unwrap().as_slice(),
        &[30_000_045],
        "the limiter records the event-position request and prevents either reference lookup"
    );
    database.destroy().await;
}

#[tokio::test]
async fn http_esi_accepts_a_public_contract_when_false_for_corporation_is_omitted() {
    let server = OneShotHttpServer::start(
        200,
        &[("X-Pages", "1"), ("Cache-Control", "max-age=60")],
        ESI_PUBLIC_CONTRACT_SUMMARY_FIXTURE,
    );
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");

    let response = esi
        .public_contracts_page(10_000_002, 1, None)
        .await
        .expect("decode public contract HTTP response");

    let contract = response.value.expect("response body").remove(0);
    assert!(!contract.for_corporation);
    assert_eq!(contract.contract_id, 234_057_619);
    assert_eq!(contract.price, 388_000_000.0);
    assert_eq!(contract.end_location_id, Some(60_003_760));
    server.finish();
}

#[tokio::test]
async fn http_esi_preserves_public_blueprint_item_facts() {
    let server = OneShotHttpServer::start(
        200,
        &[("Cache-Control", "max-age=60")],
        ESI_PUBLIC_CONTRACT_ITEMS_FIXTURE,
    );
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");

    let response = esi
        .public_contract_items(44, None)
        .await
        .expect("decode public contract item HTTP response");

    let item = response.value.expect("response body").remove(0);
    assert_eq!(item.is_blueprint_copy, Some(true));
    assert_eq!(item.material_efficiency, Some(0));
    assert_eq!(item.runs, Some(1));
    assert_eq!(item.time_efficiency, Some(0));
    assert_eq!(item.type_id, 48_473);
    server.finish();
}

#[tokio::test]
async fn http_esi_distinguishes_a_no_content_item_probe_from_a_json_item_response() {
    let server = OneShotHttpServer::start(204, &[("Cache-Control", "max-age=60")], "");
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");

    let probe = esi
        .public_contract_items_probe(44, Some("\"items-v1\""))
        .await
        .expect("decode the positive acceptance probe");

    assert!(matches!(probe, ContractItemProbe::NoContent(_)));
    server.finish();
}

#[tokio::test]
async fn http_esi_distinguishes_not_found_from_acceptance_evidence() {
    let server = OneShotHttpServer::start(404, &[("Cache-Control", "max-age=60")], "");
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");

    let probe = esi
        .public_contract_items_probe(44, Some("\"items-v1\""))
        .await
        .expect("decode the unavailable public contract probe");

    assert!(matches!(probe, ContractItemProbe::NotFound(_)));
    server.finish();
}

#[tokio::test]
async fn wire_304_responses_preserve_last_modified_for_paginated_snapshot_validation() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let page_one = r#"[{"collateral":0.0,"contract_id":44,"date_expired":"2026-08-14T12:00:00Z","date_issued":"2026-08-13T12:00:00Z","days_to_complete":0,"issuer_corporation_id":98000001,"issuer_id":90000001,"price":1500000000.0,"reward":0.0,"start_location_id":60003760,"type":"item_exchange","volume":115000.0}]"#;
    let page_two = r#"[{"collateral":0.0,"contract_id":45,"date_expired":"2026-08-14T12:00:00Z","date_issued":"2026-08-13T12:00:00Z","days_to_complete":0,"issuer_corporation_id":98000001,"issuer_id":90000001,"price":1500000000.0,"reward":0.0,"start_location_id":60003760,"type":"item_exchange","volume":115000.0}]"#;
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=0"), ("ETag", "\"regions-v1\"")],
            body: "[10000002]",
        },
        WireReply {
            status: 200,
            headers: vec![
                ("Cache-Control", "max-age=0"),
                ("ETag", "\"page-one-v1\""),
                ("Last-Modified", "Thu, 13 Aug 2026 12:00:00 GMT"),
                ("X-Pages", "2"),
            ],
            body: page_one,
        },
        WireReply {
            status: 200,
            headers: vec![
                ("Cache-Control", "max-age=0"),
                ("ETag", "\"page-two-v1\""),
                ("Last-Modified", "Thu, 13 Aug 2026 12:00:00 GMT"),
                ("X-Pages", "2"),
            ],
            body: page_two,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=0"), ("ETag", "\"items-44-v1\"")],
            body: "[]",
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=0"), ("ETag", "\"items-45-v1\"")],
            body: "[]",
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"Jita IV - Moon 4","system_id":30000142}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"Jita","security_status":0.9,"constellation_id":20000020}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"region_id":10000002}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"The Forge"}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"}]"#,
        },
        WireReply {
            status: 304,
            headers: vec![("Cache-Control", "max-age=0")],
            body: "",
        },
        WireReply {
            status: 304,
            headers: vec![("Cache-Control", "max-age=0")],
            body: "",
        },
        WireReply {
            status: 200,
            headers: vec![
                ("Cache-Control", "max-age=0"),
                ("ETag", "\"page-two-v2\""),
                ("Last-Modified", "Thu, 13 Aug 2026 12:00:00 GMT"),
                ("X-Pages", "2"),
            ],
            body: page_two,
        },
        WireReply {
            status: 304,
            headers: vec![("Cache-Control", "max-age=0")],
            body: "",
        },
        WireReply {
            status: 304,
            headers: vec![("Cache-Control", "max-age=0")],
            body: "",
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");
    let collector = ContractCollector::new(store, Arc::new(esi));

    collector
        .collect_cycle()
        .await
        .expect("first baseline cycle");
    let report = collector
        .collect_cycle()
        .await
        .expect("304 revalidation cycle");

    assert_eq!(
        report.regions,
        vec![CollectionOutcome::Complete {
            region_id: 10_000_002,
            observed_contracts: 2,
        }]
    );
    assert!(server.requests.lock().unwrap()[11]
        .to_ascii_lowercase()
        .contains("if-none-match: \"regions-v1\""));
    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn wire_eof_retry_reenters_durable_pacing_after_restart() {
    let database = TemporaryDatabase::new().await;
    let started_at = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed durable pacing test time");
    let pacer = Arc::new(AdvancingRequestPacer {
        now: StdMutex::new(started_at),
        waits: StdMutex::new(Vec::new()),
    });
    let server = SequenceHttpServer::start_with_pacer(
        vec![
            WireReply {
                status: 200,
                headers: vec![
                    ("X-Ratelimit-Group", "public-contracts"),
                    ("X-Ratelimit-Limit", "20/60s"),
                    ("X-Ratelimit-Remaining", "2"),
                ],
                body: "",
            },
            WireReply {
                status: 200,
                headers: vec![],
                body: "[]",
            },
        ],
        pacer.clone(),
    );
    let esi = Arc::new(
        HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
    );
    let collector = ContractCollector::new(database.store().await, esi.clone())
        .with_request_pacer(pacer.clone());

    let error = collector
        .collect_cycle()
        .await
        .expect_err("an EOF returns to the admitted collector seam");
    assert_eq!(
        server.requests.lock().unwrap().len(),
        1,
        "the HTTP client must not retry a successful malformed body behind the collector"
    );
    assert!(error.to_string().contains("EOF while parsing a value"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect first response pacing");
    let first_deadline = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_request_at FROM esi_collection_limiter_state WHERE limiter_scope = TRUE",
    )
    .fetch_one(&pool)
    .await
    .expect("persist malformed response rate metadata");
    assert_eq!(first_deadline, started_at + chrono::Duration::seconds(30));
    pool.close().await;

    let restarted = ContractCollector::new(database.store().await, esi)
        .with_request_pacer(pacer.clone())
        .collect_cycle()
        .await
        .expect("restart re-enters durable admission before retrying the wire request");
    assert_eq!(
        restarted.regions,
        Vec::<CollectionOutcome>::new(),
        "the headerless retry has no regions to scan"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    assert_eq!(
        pacer.waits.lock().unwrap().as_slice(),
        &[started_at + chrono::Duration::seconds(30)],
        "the retry waits on the first response's persisted low-water deadline"
    );
    assert_eq!(
        server.request_times.lock().unwrap().as_slice(),
        &[started_at, started_at + chrono::Duration::seconds(30)],
        "the second wire request happens only after durable pacing advances the injected clock"
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect restart pacing");
    let next_deadline = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_request_at FROM esi_collection_limiter_state WHERE limiter_scope = TRUE",
    )
    .fetch_one(&pool)
    .await
    .expect("headerless retry retains the reserved low-water state");
    assert_eq!(next_deadline, started_at + chrono::Duration::seconds(90));
    pool.close().await;

    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn wire_empty_success_body_does_not_retry_across_an_esi_error_limit_boundary() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let server = SequenceHttpServer::start(vec![WireReply {
        status: 200,
        headers: vec![
            ("X-ESI-Error-Limit-Remain", "0"),
            ("X-ESI-Error-Limit-Reset", "60"),
        ],
        body: "",
    }]);
    let esi = Arc::new(
        HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
    );
    let collector = ContractCollector::new(store, esi.clone());

    let error = collector
        .collect_cycle()
        .await
        .expect_err("error-limit boundary suppresses the body retry");
    assert!(error.to_string().contains("EOF while parsing a value"));

    let restarted_collector = ContractCollector::new(database.store().await, esi);
    let error = restarted_collector
        .collect_cycle()
        .await
        .expect_err("error-limit pause survives restart");
    assert!(error.to_string().contains("persisted global ESI limiter"));
    assert_eq!(server.requests.lock().unwrap().len(), 1);

    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn malformed_terminal_probe_persists_limiter_metadata_and_blocks_restart() {
    const REGION: i64 = 10_000_002;
    const CONTRACT: i64 = 44;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed a due terminal probe");
    sqlx::query("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES ($1,$2,$3,$4,now(),now(),now() - interval '1 second','awaiting_resolution')")
        .bind(REGION)
        .bind(CONTRACT)
        .bind(serde_json::to_value(item_exchange_contract(CONTRACT)).expect("serialize terminal contract"))
        .bind(serde_json::json!({"offered_items": [], "requested_items": []}))
        .execute(&pool)
        .await
        .expect("seed due terminal probe");
    pool.close().await;
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=0")],
            body: "[10000002]",
        },
        WireReply {
            status: 200,
            headers: vec![
                ("X-ESI-Error-Limit-Remain", "0"),
                ("X-ESI-Error-Limit-Reset", "60"),
            ],
            body: "{",
        },
    ]);
    let http = Arc::new(
        HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
    );
    let esi = Arc::new(HttpRegionsAndTerminalProbeEsi { http });

    let report = ContractCollector::new(store, esi.clone())
        .collect_cycle()
        .await
        .expect("a malformed terminal probe remains an isolated regional failure");
    assert_eq!(
        report.regions,
        vec![CollectionOutcome::BaselineEstablished { region_id: REGION }]
    );
    assert_eq!(
        server.requests.lock().unwrap().len(),
        2,
        "the first cycle sends discovery and one malformed terminal probe"
    );

    let error = ContractCollector::new(database.store().await, esi)
        .collect_cycle()
        .await
        .expect_err("the malformed probe's persisted limiter boundary blocks restart discovery");
    assert!(error.to_string().contains("persisted global ESI limiter"));
    assert_eq!(
        server.requests.lock().unwrap().len(),
        2,
        "restart must make no wire request after the malformed 2xx limiter boundary"
    );

    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn wire_rate_limit_headers_stop_the_collector_before_the_next_request() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let server = SequenceHttpServer::start(vec![WireReply {
        status: 200,
        headers: vec![
            ("Cache-Control", "max-age=0"),
            ("X-Ratelimit-Group", "public-contracts"),
            ("X-Ratelimit-Limit", "150/15m"),
            ("X-Ratelimit-Remaining", "0"),
            ("X-Ratelimit-Used", "150"),
        ],
        body: "[10000002]",
    }]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");
    let collector = ContractCollector::new(store, Arc::new(esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("rate boundary stops before a regional attempt starts");

    assert!(report.retry_after.is_some());
    assert!(report.regions.is_empty());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to confirm no fabricated limiter-boundary batch");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM regional_observation_batches WHERE region_id = 10000002",
        )
        .fetch_one(&pool)
        .await
        .expect("count unstarted regional batches"),
        0,
        "a shared limiter after discovery must not fabricate a regional attempt"
    );
    pool.close().await;
    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn wire_retry_after_and_esi_error_headers_pause_a_restarted_collector() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let server = SequenceHttpServer::start(vec![WireReply {
        status: 429,
        headers: vec![
            ("Retry-After", "60"),
            ("X-ESI-Error-Limit-Remain", "0"),
            ("X-ESI-Error-Limit-Reset", "120"),
        ],
        body: "[]",
    }]);
    let esi = Arc::new(
        HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
    );
    let collector = ContractCollector::new(store, esi.clone());

    let error = collector
        .collect_cycle()
        .await
        .expect_err("retry-after response pauses the collector");
    assert!(error.to_string().contains("429"));

    let restarted_collector = ContractCollector::new(database.store().await, esi);
    let error = restarted_collector
        .collect_cycle()
        .await
        .expect_err("retry-after pause survives restart");
    assert!(error.to_string().contains("persisted global ESI limiter"));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn wire_420_error_limit_boundary_pauses_a_restarted_collector() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let server = SequenceHttpServer::start(vec![WireReply {
        status: 420,
        headers: vec![
            ("X-ESI-Error-Limit-Remain", "0"),
            ("X-ESI-Error-Limit-Reset", "60"),
        ],
        body: "[]",
    }]);
    let esi = Arc::new(
        HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
    );
    let collector = ContractCollector::new(store, esi.clone());

    let error = collector
        .collect_cycle()
        .await
        .expect_err("420 response pauses the collector");
    assert!(error.to_string().contains("420"));

    let restarted_collector = ContractCollector::new(database.store().await, esi);
    let error = restarted_collector
        .collect_cycle()
        .await
        .expect_err("420 error limit pause survives restart");
    assert!(error.to_string().contains("persisted global ESI limiter"));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn exhausted_manifest_retries_defer_only_that_contract_and_preserve_the_baseline() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let first_contract = item_exchange_contract(44);
    let pending_contract = item_exchange_contract(45);
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("X-ESI-Error-Limit-Remain", "100")],
            body: "",
        },
        WireReply {
            status: 200,
            headers: vec![("X-ESI-Error-Limit-Remain", "100")],
            body: ESI_PUBLIC_CONTRACT_ITEMS_FIXTURE,
        },
    ]);
    let esi = Arc::new(ExhaustedWireManifestEsi {
        http: HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
        contracts: vec![first_contract.clone(), pending_contract.clone()],
        manifests: HashMap::from([(
            44,
            EsiResponse::fresh(vec![offered_ship(1)], expiring_cache()),
        )]),
        unavailable_contract_id: 45,
    });

    let baseline = ContractCollector::new(store.clone(), esi.clone())
        .collect_cycle()
        .await
        .expect("one unavailable manifest does not invalidate a conclusive summary");
    assert_eq!(
        baseline.regions,
        vec![CollectionOutcome::BaselineEstablished {
            region_id: 10_000_002,
        }]
    );
    assert!(baseline.events.is_empty());
    assert_eq!(
        server.requests.lock().unwrap().len(),
        1,
        "the malformed manifest is one admitted failed attempt, not hidden HTTP retries"
    );
    assert_eq!(
        store
            .region_contracts(10_000_002)
            .await
            .expect("resolved regional contracts")
            .iter()
            .map(|contract| contract.contract_id)
            .collect::<Vec<_>>(),
        vec![44]
    );
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("scoped manifest failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].region_id, Some(10_000_002));
    assert_eq!(failures[0].contract_id, Some(45));
    assert_eq!(
        failures[0].resource_key.as_deref(),
        Some("contracts/public/items/45")
    );
    let validation_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect durable pending state");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM contract_manifest_pending")
            .fetch_one(&validation_pool)
            .await
            .expect("count pending manifests"),
        1
    );
    let batch = sqlx::query("SELECT outcome, expected_pages, pages_attempted, pages_observed, observed_contract_count, resolved_contract_count, manifest_failure_count, consistency_evidence, failure_classification, failure_detail, retry_after FROM regional_observation_batches WHERE region_id = 10000002 ORDER BY id")
        .fetch_one(&validation_pool)
        .await
        .expect("read compact manifest-failure batch evidence");
    assert_eq!(batch.get::<String, _>("outcome"), "complete");
    assert_eq!(batch.get::<Option<i32>, _>("expected_pages"), Some(1));
    assert_eq!(batch.get::<i32, _>("pages_attempted"), 1);
    assert_eq!(batch.get::<i32, _>("pages_observed"), 1);
    assert_eq!(batch.get::<i64, _>("observed_contract_count"), 2);
    assert_eq!(batch.get::<i64, _>("resolved_contract_count"), 1);
    assert_eq!(batch.get::<i64, _>("manifest_failure_count"), 1);
    assert_eq!(
        batch.get::<Option<String>, _>("failure_classification"),
        Some("manifest_collection".to_string())
    );
    assert!(batch
        .get::<Option<String>, _>("failure_detail")
        .expect("compact failure detail")
        .contains("1 manifest failures"));
    assert!(batch
        .get::<Option<DateTime<Utc>>, _>("retry_after")
        .is_none());
    let evidence: Value = batch.get("consistency_evidence");
    assert_eq!(evidence["manifest_failure_count"], serde_json::json!(1));
    assert!(evidence.get("manifest_failure_contract_ids").is_none());

    let recovered = ContractCollector::new(store.clone(), esi)
        .collect_cycle()
        .await
        .expect(
            "a restarted collector re-enters admission and backfills the pending baseline manifest",
        );
    assert!(recovered.events.is_empty());
    assert_eq!(
        store
            .region_contracts(10_000_002)
            .await
            .expect("backfilled regional contracts")
            .iter()
            .map(|contract| contract.contract_id)
            .collect::<Vec<_>>(),
        vec![44, 45]
    );
    assert!(store
        .unresolved_collection_failures()
        .await
        .expect("resolved manifest failure")
        .is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM contract_manifest_pending")
            .fetch_one(&validation_pool)
            .await
            .expect("count recovered pending manifests"),
        0
    );
    assert_eq!(
        server.requests.lock().unwrap().len(),
        2,
        "the later valid manifest is a new admitted wire request after restart"
    );
    validation_pool.close().await;

    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn new_contract_with_a_deferred_manifest_delivers_once_after_recovery() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let new_contract = item_exchange_contract(45);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract.clone()],
            HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        )),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "deferred-listing",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist listing subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    let deferred = ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract.clone(), new_contract.clone()],
            HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (45, Err(EsiError::retryable("manifest unavailable", None))),
            ]),
        )),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("new contract presence is complete while its manifest is deferred");
    assert_eq!(
        deferred.regions,
        vec![CollectionOutcome::Complete {
            region_id: 10_000_002,
            observed_contracts: 2,
        }]
    );
    assert!(deferred.events.is_empty());
    assert!(delivery.sent.lock().unwrap().is_empty());
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("contract-scoped manifest failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].contract_id, Some(45));

    let recovered = ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract.clone(), new_contract.clone()],
            HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        )),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("resolved manifest creates the deferred listing");
    assert_eq!(recovered.events.len(), 1);
    assert_eq!(recovered.events[0].contract.contract_id, 45);
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);

    let repeated = ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract, new_contract],
            HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        )),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("resolved listing remains idempotent");
    assert!(repeated.events.is_empty());
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn disappearing_pending_manifest_creates_no_event_or_resolution_case() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let disappearing_contract = item_exchange_contract(45);
    ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract.clone()],
            HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        )),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    let deferred = ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract.clone(), disappearing_contract],
            HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (45, Err(EsiError::retryable("manifest unavailable", None))),
            ]),
        )),
    )
    .collect_cycle()
    .await
    .expect("persist the pending manifest without an event");
    assert_eq!(
        deferred.regions,
        vec![CollectionOutcome::Complete {
            region_id: 10_000_002,
            observed_contracts: 2,
        }]
    );
    assert!(deferred.events.is_empty());

    let disappeared = ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![baseline_contract],
            HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        )),
    )
    .collect_cycle()
    .await
    .expect("disappearance without a manifest remains unclassified");
    assert!(disappeared.events.is_empty());
    assert!(store
        .contract_resolution_records()
        .await
        .expect("resolution records")
        .iter()
        .all(|resolution| resolution.contract_id != 45));
    assert!(store
        .unresolved_collection_failures()
        .await
        .expect("resolved disappeared manifest failure")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn complete_first_observation_establishes_a_silent_regional_baseline() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let public_contract = item_exchange_contract(44);
    let non_item_exchange = PublicContract {
        contract_type: "courier".to_string(),
        contract_id: 45,
        ..public_contract.clone()
    };
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![public_contract, non_item_exchange],
                CacheMetadata::page_cached_for_seconds(1, 60),
            )),
        )]),
        items: HashMap::from([(
            44,
            Ok(EsiResponse::fresh(
                vec![
                    PublicContractItem {
                        record_id: 1,
                        type_id: 587,
                        quantity: 1,
                        is_included: true,
                        item_id: Some(1_001),
                        raw_quantity: None,
                        is_singleton: None,
                        is_blueprint_copy: None,
                        material_efficiency: None,
                        runs: None,
                        time_efficiency: None,
                    },
                    PublicContractItem {
                        record_id: 2,
                        type_id: 34,
                        quantity: 500,
                        is_included: false,
                        item_id: Some(1_002),
                        raw_quantity: None,
                        is_singleton: None,
                        is_blueprint_copy: None,
                        material_efficiency: None,
                        runs: None,
                        time_efficiency: None,
                    },
                ],
                CacheMetadata::cached_for_seconds(60),
            )),
        )]),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(fake_esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("complete collection cycle");

    assert_eq!(report.events.len(), 0);
    assert_eq!(
        report.regions,
        vec![CollectionOutcome::BaselineEstablished {
            region_id: 10_000_002
        }]
    );
    let region = store
        .region_state(10_000_002)
        .await
        .expect("region state")
        .expect("stored region");
    assert!(region.baseline_at.is_some());
    assert_eq!(region.complete_observations, 1);
    let contracts = store
        .region_contracts(10_000_002)
        .await
        .expect("stored contracts");
    assert_eq!(contracts.len(), 1);
    assert_eq!(contracts[0].contract_id, 44);
    assert_eq!(contracts[0].offered_items[0].type_id, 587);
    assert_eq!(contracts[0].requested_items[0].type_id, 34);

    database.destroy().await;
}

#[tokio::test]
async fn inconsistent_pagination_is_inconclusive_and_creates_no_presence_or_baseline() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([
            (
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    expiring_page(2),
                )),
            ),
            (
                (10_000_002, 2),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(45)],
                    expiring_page(3),
                )),
            ),
        ]),
        items: HashMap::new(),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(fake_esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("inconclusive observation is retained");

    assert!(matches!(
        report.regions.as_slice(),
        [CollectionOutcome::Inconclusive {
            region_id: 10_000_002,
            ..
        }]
    ));
    assert!(store
        .region_state(10_000_002)
        .await
        .expect("region state")
        .is_none());
    assert_eq!(
        store
            .storage_counts()
            .await
            .expect("storage counts")
            .presence_intervals,
        0
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect the partial pagination batch");
    let batch = sqlx::query("SELECT outcome, expected_pages, pages_attempted, pages_observed, observed_contract_count, resolved_contract_count, manifest_failure_count, consistency_evidence, failure_classification, failure_detail, retry_after FROM regional_observation_batches WHERE region_id = 10000002")
        .fetch_one(&pool)
        .await
        .expect("retain the inconsistent pagination attempt");
    assert_eq!(batch.get::<String, _>("outcome"), "inconclusive");
    assert_eq!(batch.get::<Option<i32>, _>("expected_pages"), Some(2));
    assert_eq!(batch.get::<i32, _>("pages_attempted"), 2);
    assert_eq!(batch.get::<i32, _>("pages_observed"), 2);
    assert_eq!(batch.get::<i64, _>("observed_contract_count"), 2);
    assert_eq!(batch.get::<i64, _>("resolved_contract_count"), 0);
    assert_eq!(batch.get::<i64, _>("manifest_failure_count"), 0);
    assert_eq!(
        batch.get::<String, _>("failure_classification"),
        "consistency"
    );
    assert!(batch.get::<String, _>("failure_detail").contains("X-Pages"));
    assert!(batch
        .get::<Option<DateTime<Utc>>, _>("retry_after")
        .is_none());
    let evidence: Value = batch.get("consistency_evidence");
    assert_eq!(evidence["pages_attempted"], serde_json::json!(2));
    assert_eq!(evidence["pages_observed"], serde_json::json!(2));
    assert_eq!(evidence["expected_pages"], serde_json::json!(2));
    assert_eq!(
        evidence["consistency"]["expected_pages_matches_first"],
        serde_json::json!(false)
    );
    assert_eq!(
        evidence["first_page"]["expected_pages"],
        serde_json::json!(2)
    );
    assert_eq!(
        evidence["last_page"]["expected_pages"],
        serde_json::json!(3)
    );
    pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn differing_last_modified_values_across_pages_are_inconclusive() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let first_page_metadata = CacheMetadata {
        last_modified: Some(Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).unwrap()),
        ..expiring_page(2)
    };
    let second_page_metadata = CacheMetadata {
        last_modified: Some(Utc.with_ymd_and_hms(2026, 8, 13, 12, 1, 0).unwrap()),
        ..expiring_page(2)
    };
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([
            (
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    first_page_metadata,
                )),
            ),
            (
                (10_000_002, 2),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(45)],
                    second_page_metadata,
                )),
            ),
        ]),
        items: HashMap::new(),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(fake_esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("inconclusive observation is retained");

    assert!(matches!(
        report.regions.as_slice(),
        [CollectionOutcome::Inconclusive {
            region_id: 10_000_002,
            ..
        }]
    ));
    assert_eq!(
        store
            .storage_counts()
            .await
            .expect("storage counts")
            .presence_intervals,
        0
    );

    database.destroy().await;
}

#[tokio::test]
async fn duplicated_contract_ids_across_pages_are_inconclusive() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([
            (
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    expiring_page(2),
                )),
            ),
            (
                (10_000_002, 2),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    expiring_page(2),
                )),
            ),
        ]),
        items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(fake_esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("inconclusive observation is retained");

    assert!(matches!(
        report.regions.as_slice(),
        [CollectionOutcome::Inconclusive {
            region_id: 10_000_002,
            ..
        }]
    ));
    assert_eq!(
        store
            .storage_counts()
            .await
            .expect("storage counts")
            .presence_intervals,
        0
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect the duplicate-id batch");
    let batch = sqlx::query("SELECT expected_pages, pages_attempted, pages_observed, observed_contract_count, resolved_contract_count, manifest_failure_count, consistency_evidence, failure_classification, retry_after FROM regional_observation_batches WHERE region_id = 10000002")
        .fetch_one(&pool)
        .await
        .expect("retain the duplicate-id attempt");
    assert_eq!(batch.get::<Option<i32>, _>("expected_pages"), Some(2));
    assert_eq!(batch.get::<i32, _>("pages_attempted"), 2);
    assert_eq!(batch.get::<i32, _>("pages_observed"), 2);
    assert_eq!(batch.get::<i64, _>("observed_contract_count"), 2);
    assert_eq!(batch.get::<i64, _>("resolved_contract_count"), 0);
    assert_eq!(batch.get::<i64, _>("manifest_failure_count"), 0);
    assert_eq!(
        batch.get::<String, _>("failure_classification"),
        "consistency"
    );
    assert!(batch
        .get::<Option<DateTime<Utc>>, _>("retry_after")
        .is_none());
    let evidence: Value = batch.get("consistency_evidence");
    assert_eq!(
        evidence["consistency"]["duplicate_contract_count"],
        serde_json::json!(1)
    );
    pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn missing_manifest_establishes_the_summary_baseline_without_a_manifest_presence() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![item_exchange_contract(44)],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([(44, Err(EsiError::retryable("manifest unavailable", None)))]),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(fake_esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("summary observation remains conclusive");

    assert_eq!(
        report.regions,
        vec![CollectionOutcome::BaselineEstablished {
            region_id: 10_000_002,
        }]
    );
    assert_eq!(
        store.storage_counts().await.expect("storage counts"),
        killbot_rust::contract_intelligence::StorageCounts {
            contract_facts: 1,
            manifests: 0,
            presence_intervals: 0,
        }
    );
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("manifest failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].contract_id, Some(44));
    assert_eq!(failures[0].failure_kind, "manifest_collection");

    database.destroy().await;
}

#[tokio::test]
async fn esi_retry_boundaries_are_reported_for_the_next_contract_collection_cycle() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let retry_after = Utc::now() + chrono::Duration::seconds(30);
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![item_exchange_contract(44)],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([(
            44,
            Err(EsiError::retryable("rate limited", Some(retry_after))),
        )]),
    };
    let collector = ContractCollector::new(store, Arc::new(fake_esi));

    let report = collector
        .collect_cycle()
        .await
        .expect("inconclusive observation is retained");

    assert_eq!(report.retry_after, Some(retry_after));
    database.destroy().await;
}

#[tokio::test]
async fn a_rate_boundary_stops_follow_on_requests_and_persists_the_pause() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let retry_after = Utc::now() + chrono::Duration::seconds(30);
    let esi = Arc::new(StopsAfterRateBoundaryEsi {
        retry_after,
        calls: StdMutex::new(Vec::new()),
    });
    let collector = ContractCollector::new(store, esi.clone());

    let report = collector
        .collect_cycle()
        .await
        .expect("first region is retained as inconclusive");

    assert_eq!(report.retry_after, Some(retry_after));
    assert_eq!(
        esi.calls.lock().unwrap().as_slice(),
        ["regions", "page:10000002", "items:10000002"]
    );

    let restarted_collector = ContractCollector::new(database.store().await, esi.clone());
    let error = restarted_collector
        .collect_cycle()
        .await
        .expect_err("persisted limiter boundary blocks a restarted collector");
    assert!(error.to_string().contains("persisted global ESI limiter"));
    assert_eq!(
        esi.calls.lock().unwrap().as_slice(),
        ["regions", "page:10000002", "items:10000002"]
    );

    database.destroy().await;
}

#[tokio::test]
async fn unchanged_contracts_reuse_facts_and_manifests_while_extending_one_presence_interval() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let fake_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![item_exchange_contract(44)],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([(
            44,
            Ok(EsiResponse::fresh(
                vec![PublicContractItem {
                    record_id: 1,
                    type_id: 587,
                    quantity: 1,
                    is_included: true,
                    item_id: Some(1_001),
                    raw_quantity: None,
                    is_singleton: None,
                    is_blueprint_copy: None,
                    material_efficiency: None,
                    runs: None,
                    time_efficiency: None,
                }],
                expiring_cache(),
            )),
        )]),
    };
    let collector = ContractCollector::new(store.clone(), Arc::new(fake_esi));

    collector
        .collect_cycle()
        .await
        .expect("first complete cycle");
    let first_last_observed_at = store
        .region_contracts(10_000_002)
        .await
        .expect("first stored contract")[0]
        .last_observed_at;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let report = collector
        .collect_cycle()
        .await
        .expect("second complete cycle");

    assert_eq!(
        report.regions,
        vec![CollectionOutcome::Complete {
            region_id: 10_000_002,
            observed_contracts: 1
        }]
    );
    assert_eq!(
        store
            .region_state(10_000_002)
            .await
            .expect("region state")
            .expect("stored region")
            .complete_observations,
        2
    );
    assert_eq!(
        store.storage_counts().await.expect("storage counts"),
        killbot_rust::contract_intelligence::StorageCounts {
            contract_facts: 1,
            manifests: 1,
            presence_intervals: 1
        }
    );
    assert!(
        store
            .region_contracts(10_000_002)
            .await
            .expect("second stored contract")[0]
            .last_observed_at
            > first_last_observed_at
    );

    database.destroy().await;
}

#[tokio::test]
async fn stale_cached_responses_are_conditionally_revalidated_without_losing_pagination_metadata() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let fake_esi = Arc::new(ConditionalEsi::default());
    let collector = ContractCollector::new(store.clone(), fake_esi.clone());

    collector
        .collect_cycle()
        .await
        .expect("first collection cycle");
    let report = collector
        .collect_cycle()
        .await
        .expect("conditional collection cycle");

    assert_eq!(
        report.regions,
        vec![CollectionOutcome::Complete {
            region_id: 10_000_002,
            observed_contracts: 1
        }]
    );
    assert_eq!(
        fake_esi
            .received_etags
            .lock()
            .unwrap()
            .iter()
            .filter(|etag| etag.is_some())
            .count(),
        3
    );
    assert_eq!(
        store
            .storage_counts()
            .await
            .expect("storage counts")
            .presence_intervals,
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn contract_failures_do_not_block_the_killmail_producer_path() {
    let database_retry = tokio::spawn(
        killbot_rust::contract_intelligence::run_contract_collection_loop(
            "postgres://killbot_contracts:killbot_contracts@127.0.0.1:1/never".to_string(),
            Duration::from_secs(60),
            Duration::from_millis(100),
        ),
    );
    let database = TemporaryDatabase::new().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let collector = ContractCollector::new(
        database.store().await,
        Arc::new(PausedFailingContractEsi {
            entered: entered.clone(),
            release: release.clone(),
        }),
    );
    let collector_failure = tokio::spawn(async move { collector.collect_cycle().await });
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("contract collector reaches its ESI request");

    let fixture: ZkData =
        serde_json::from_str(include_str!("../resources/106140056_small_bubble.json"))
            .expect("parse inline killmail fixture");
    let (result_tx, mut result_rx) = mpsc::channel(1);
    let producer = tokio::spawn(run_producer(
        Box::new(SingleInlineKillmailFeed {
            item: Mutex::new(Some(ZkDataNoEsi {
                kill_id: fixture.kill_id,
                zkb: fixture.zkb,
                inline_killmail: Some(fixture.killmail),
            })),
        }),
        app_state_for_ping_limiter(),
        result_tx,
        Arc::new(Semaphore::new(1)),
    ));

    let processed = tokio::time::timeout(Duration::from_secs(1), result_rx.recv())
        .await
        .expect("killmail producer must make progress while contract work is failing")
        .expect("producer sends a result");
    assert!(matches!(
        processed,
        ProcessedResult::NoMatch {
            kill_id: 106_140_056,
            ..
        }
    ));
    assert!(
        !database_retry.is_finished(),
        "database retry runtime remains isolated from the killmail producer"
    );
    release.notify_one();
    let collector_error = tokio::time::timeout(Duration::from_secs(1), collector_failure)
        .await
        .expect("failing collector finishes")
        .expect("join failing collector")
        .expect_err("simulated ESI collector failure is surfaced");
    assert!(collector_error.to_string().contains("ESI is unavailable"));

    producer.abort();
    database_retry.abort();
    database.destroy().await;
}

#[tokio::test]
async fn r2z2_producer_telemetry_is_persisted_by_health_and_recovers_after_a_parse_failure() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to producer telemetry database");
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current heartbeat");
    sqlx::query("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES (10000002, $1, 1, $1)")
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed current regional progress");
    sqlx::query(
        "INSERT INTO esi_cache_metadata (resource_key, updated_at) VALUES ('esi:regions', $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current ESI progress");

    let fixture: ZkData =
        serde_json::from_str(include_str!("../resources/106140056_small_bubble.json"))
            .expect("parse inline killmail fixture");
    let telemetry = Arc::new(FeedHealthTelemetry::new());
    let (success_tx, mut success_rx) = mpsc::channel(1);
    let success_producer = tokio::spawn(run_producer_with_health(
        Box::new(SingleInlineKillmailFeed {
            item: Mutex::new(Some(ZkDataNoEsi {
                kill_id: fixture.kill_id,
                zkb: fixture.zkb.clone(),
                inline_killmail: Some(fixture.killmail.clone()),
            })),
        }),
        app_state_for_ping_limiter(),
        success_tx,
        Arc::new(Semaphore::new(1)),
        telemetry.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(1), success_rx.recv())
        .await
        .expect("producer returns a valid R2Z2 item")
        .expect("producer dispatches a result");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let telemetry_snapshot = telemetry.snapshot().await;
            if telemetry_snapshot.r2z2_progress_at.is_some()
                && telemetry_snapshot.validation_success_at.is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("producer records current R2Z2 progress and feed validation");

    let clock = Arc::new(FixedHealthClock(StdMutex::new(now)));
    let cycle =
        HealthCycle::new(store.clone(), clock.clone()).with_feed_telemetry(telemetry.clone(), true);
    let initial_snapshot = cycle
        .run_once()
        .await
        .expect("persist producer telemetry in the next health snapshot");
    assert!(initial_snapshot
        .checks
        .iter()
        .any(|check| check.key == "r2z2_progress" && check.status == HealthStatus::Healthy));
    assert!(initial_snapshot
        .checks
        .iter()
        .any(|check| check.key == "feed_validation" && check.status == HealthStatus::Healthy));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM health_feed_telemetry WHERE source IN ('r2z2_progress', 'feed_validation')",
        )
        .fetch_one(&pool)
        .await
        .expect("read persisted producer telemetry"),
        2
    );

    let (failure_tx, _failure_rx) = mpsc::channel(1);
    let failure_producer = tokio::spawn(run_producer_with_health(
        Box::new(ParseFailingR2z2Feed),
        app_state_for_ping_limiter(),
        failure_tx,
        Arc::new(Semaphore::new(1)),
        telemetry.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if telemetry.snapshot().await.validation_failures == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("producer records a parse failure without waiting for PostgreSQL");
    let transport_called = Arc::new(Notify::new());
    let (transport_tx, _transport_rx) = mpsc::channel(1);
    let transport_producer = tokio::spawn(run_producer_with_health(
        Box::new(TransportFailingR2z2Feed {
            called: transport_called.clone(),
        }),
        app_state_for_ping_limiter(),
        transport_tx,
        Arc::new(Semaphore::new(1)),
        telemetry.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(1), transport_called.notified())
        .await
        .expect("producer reaches the transport failure");
    assert_eq!(
        telemetry.snapshot().await.validation_failures,
        1,
        "transport failures are progress/connectivity evidence, not validation failures"
    );
    clock.advance(chrono::Duration::seconds(1));
    assert!(cycle
        .run_once()
        .await
        .expect("persist the producer parse failure")
        .checks
        .iter()
        .any(|check| check.key == "feed_validation" && check.status == HealthStatus::Degraded));

    let (recovery_tx, mut recovery_rx) = mpsc::channel(1);
    let recovery_producer = tokio::spawn(run_producer_with_health(
        Box::new(SingleInlineKillmailFeed {
            item: Mutex::new(Some(ZkDataNoEsi {
                kill_id: fixture.kill_id,
                zkb: fixture.zkb,
                inline_killmail: Some(fixture.killmail),
            })),
        }),
        app_state_for_ping_limiter(),
        recovery_tx,
        Arc::new(Semaphore::new(1)),
        telemetry.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(1), recovery_rx.recv())
        .await
        .expect("producer returns a recovery R2Z2 item")
        .expect("producer dispatches recovery result");
    clock.advance(chrono::Duration::seconds(1));
    assert!(cycle
        .run_once()
        .await
        .expect("persist recovered producer validation")
        .checks
        .iter()
        .any(|check| check.key == "feed_validation" && check.status == HealthStatus::Healthy));
    clock.advance(chrono::Duration::seconds(1));
    let redisq_cycle = HealthCycle::new(store, clock).with_feed_telemetry(telemetry.clone(), false);
    assert!(redisq_cycle
        .run_once()
        .await
        .expect("evaluate RedisQ health without a R2Z2 check")
        .checks
        .iter()
        .all(|check| check.key != "r2z2_progress"));

    success_producer.abort();
    failure_producer.abort();
    transport_producer.abort();
    recovery_producer.abort();
    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn contract_migrations_apply_to_clean_and_already_current_databases() {
    let database = TemporaryDatabase::new().await;
    let clean_store = database.store().await;
    let validation_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to clean migrated database");
    for table in [
        "regional_collection_metadata",
        "contract_manifest_pending",
        "contract_subscriptions",
        "contract_outbound_deliveries",
        "contract_resolution_cases",
        "contract_observed_embed_contexts",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(&validation_pool)
            .await
            .expect("read migrated table from clean database");
        assert!(exists, "migration created {table}");
    }
    assert_eq!(
        clean_store
            .storage_counts()
            .await
            .expect("read from clean migrated database"),
        killbot_rust::contract_intelligence::StorageCounts {
            contract_facts: 0,
            manifests: 0,
            presence_intervals: 0,
        }
    );
    let current_store = ContractCollectionStore::connect(&database.url)
        .await
        .expect("reapply migrations to an already-current database");
    assert_eq!(
        current_store
            .storage_counts()
            .await
            .expect("read from already-current database"),
        killbot_rust::contract_intelligence::StorageCounts {
            contract_facts: 0,
            manifests: 0,
            presence_intervals: 0,
        }
    );
    let invalid_lifecycle = sqlx::query(
        "INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES (1, 1, '{}'::jsonb, '{}'::jsonb, now(), now(), now(), 'invalid')",
    )
    .execute(&validation_pool)
    .await
    .expect_err("invalid resolution lifecycle state is rejected");
    assert_eq!(
        invalid_lifecycle
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514")
    );

    sqlx::query(
        "INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES (42, 77, 'dedupe', 'migration validation', '{}'::jsonb, '{}'::jsonb)",
    )
    .execute(&validation_pool)
    .await
    .expect("insert delivery subscription");
    sqlx::query(
        "INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status, delivery_nonce) VALUES (42, 77, 'dedupe', 45, 'sale_confirmed', '{}'::jsonb, '{}'::jsonb, FALSE, 'prepared', 'ci-dedupe-1')",
    )
    .execute(&validation_pool)
    .await
    .expect("insert initial delivery identity");
    let duplicate_delivery = sqlx::query(
        "INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status, delivery_nonce) VALUES (42, 77, 'dedupe', 45, 'sale_confirmed', '{}'::jsonb, '{}'::jsonb, FALSE, 'prepared', 'ci-dedupe-2')",
    )
    .execute(&validation_pool)
    .await
    .expect_err("duplicate contract delivery identity is rejected");
    assert_eq!(
        duplicate_delivery
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23505")
    );
    let delivery_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM contract_outbound_deliveries WHERE guild_id = 42 AND channel_id = 77 AND subscription_id = 'dedupe' AND contract_id = 45 AND event_kind = 'sale_confirmed'",
    )
    .fetch_one(&validation_pool)
    .await
    .expect("count delivery identity rows");
    assert_eq!(delivery_count, 1);

    ContractCollectionStore::connect(&database.url)
        .await
        .expect("reapply migrations after constraint validation");
    validation_pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn contract_subscribe_persistence_replaces_and_removes_the_invoking_channel_subscription() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let original = contract_subscription(
        "ships",
        ContractItemDirection::Offered,
        vec![587],
        vec![],
        ContractEventAction::Post,
    );

    store
        .upsert_contract_subscription(&original)
        .await
        .expect("create contract subscription");
    let replacement = ContractSubscription {
        description: "replacement subscription".to_string(),
        filter: ContractFilter::listed_ship(ContractItemDirection::Offered, vec![], vec![25]),
        ..original
    };
    store
        .upsert_contract_subscription(&replacement)
        .await
        .expect("replace contract subscription for invoking channel");

    assert_eq!(
        store
            .contract_subscriptions_for_channel(42, 77)
            .await
            .expect("read contract subscriptions"),
        vec![replacement.clone()]
    );
    assert!(store
        .remove_contract_subscription(42, 77, "ships")
        .await
        .expect("remove contract subscription"));
    assert!(store
        .contract_subscriptions_for_channel(42, 77)
        .await
        .expect("read removed subscriptions")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn contract_delivery_migration_replaces_the_subscription_cascade_with_restrict() {
    let database = TemporaryDatabase::new().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to pre-upgrade database");
    pool.execute("CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .expect("create migration ledger for the pre-upgrade database");
    for (version, sql) in [
        (
            20260813000000_i64,
            include_str!("../migrations/20260813000000_create_contract_intelligence.sql"),
        ),
        (
            20260813000001_i64,
            include_str!(
                "../migrations/20260813000001_add_contract_collection_representation_metadata.sql"
            ),
        ),
        (
            20260813000002_i64,
            include_str!(
                "../migrations/20260813000002_add_contract_subscriptions_and_deliveries.sql"
            ),
        ),
    ] {
        pool.execute(sql)
            .await
            .expect("apply pre-ticket-correction migration");
        sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, 'pre-correction migration', TRUE, $2, 0)")
            .bind(version)
            .bind(Sha384::digest(sql.as_bytes()).to_vec())
            .execute(&pool)
            .await
            .expect("record pre-ticket-correction migration");
    }
    pool.close().await;

    let store = database.store().await;
    drop(store);
    let verify_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to upgraded database");
    let delete_rule = sqlx::query_scalar::<_, String>(
        "SELECT referential_constraints.delete_rule FROM information_schema.referential_constraints JOIN information_schema.table_constraints ON referential_constraints.constraint_name = table_constraints.constraint_name AND referential_constraints.constraint_schema = table_constraints.constraint_schema WHERE table_constraints.table_schema = current_schema() AND table_constraints.table_name = 'contract_outbound_deliveries' AND table_constraints.constraint_name = 'contract_delivery_subscription_history_fkey'",
    )
    .fetch_one(&verify_pool)
    .await
    .expect("read delivery foreign-key delete rule");
    assert_eq!(delete_rule, "RESTRICT");
    verify_pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn contract_delivery_ping_type_migration_defaults_legacy_rows_to_here() {
    let database = TemporaryDatabase::new().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to pre-ping-type database");
    pool.execute("CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .expect("create migration ledger for the pre-ping-type database");
    for (version, sql) in [
        (
            20260813000000_i64,
            include_str!("../migrations/20260813000000_create_contract_intelligence.sql"),
        ),
        (
            20260813000001_i64,
            include_str!(
                "../migrations/20260813000001_add_contract_collection_representation_metadata.sql"
            ),
        ),
        (
            20260813000002_i64,
            include_str!(
                "../migrations/20260813000002_add_contract_subscriptions_and_deliveries.sql"
            ),
        ),
        (
            20260813000003_i64,
            include_str!("../migrations/20260813000003_preserve_contract_delivery_history.sql"),
        ),
        (
            20260813000004_i64,
            include_str!("../migrations/20260813000004_add_contract_acceptance_resolution.sql"),
        ),
        (
            20260813000005_i64,
            include_str!(
                "../migrations/20260813000005_add_contract_nonfinancial_terminal_states.sql"
            ),
        ),
        (
            20260813000006_i64,
            include_str!("../migrations/20260813000006_add_contract_embed_context.sql"),
        ),
        (
            20260813000007_i64,
            include_str!("../migrations/20260813000007_make_contract_delivery_restart_safe.sql"),
        ),
    ] {
        pool.execute(sql)
            .await
            .expect("apply pre-ping-type migration");
        sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, 'pre-ping-type migration', TRUE, $2, 0)")
            .bind(version)
            .bind(Sha384::digest(sql.as_bytes()).to_vec())
            .execute(&pool)
            .await
            .expect("record pre-ping-type migration");
    }
    sqlx::query("INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES (42, 77, 'legacy', 'legacy subscription', '{\"root\":{\"condition\":{\"event_kinds\":[\"listed\"]}}}', '{\"listed\":\"post_and_ping\"}')")
        .execute(&pool)
        .await
        .expect("insert legacy subscription");
    sqlx::query("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status, delivery_nonce) VALUES (42, 77, 'legacy', 45, 'listed', '{}', '{\"title\":\"legacy\",\"fields\":[]}', TRUE, 'prepared', 'ci-legacy')")
        .execute(&pool)
        .await
        .expect("insert legacy prepared delivery");
    pool.close().await;

    let store = database.store().await;
    assert_eq!(
        store
            .contract_subscriptions_for_channel(42, 77)
            .await
            .expect("read legacy subscription")[0]
            .event_actions
            .listed,
        ContractEventAction::PostAndPing
    );
    let verify_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to upgraded database");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT ping_type FROM contract_outbound_deliveries WHERE subscription_id = 'legacy'",
        )
        .fetch_one(&verify_pool)
        .await
        .expect("read migrated legacy ping type"),
        "here"
    );
    verify_pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn new_listed_ship_contracts_notify_each_matching_subscription_once_after_baseline() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let offered_ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let requested_ship = PublicContractItem {
        record_id: 2,
        type_id: 34,
        quantity: 2,
        is_included: false,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let baseline_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![baseline_contract.clone()],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([(
            44,
            Ok(EsiResponse::fresh(
                vec![offered_ship.clone(), requested_ship.clone()],
                expiring_cache(),
            )),
        )]),
    };
    ContractCollector::new(store.clone(), Arc::new(baseline_esi))
        .collect_cycle()
        .await
        .expect("establish the silent baseline");

    for subscription in [
        contract_subscription(
            "offered-type",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ),
        contract_subscription(
            "offered-group",
            ContractItemDirection::Offered,
            vec![],
            vec![25],
            ContractEventAction::PostAndPing,
        ),
        contract_subscription(
            "requested-group",
            ContractItemDirection::Requested,
            vec![],
            vec![26],
            ContractEventAction::Post,
        ),
        contract_subscription(
            "ignored",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Ignore,
        ),
        contract_subscription(
            "wrong-direction",
            ContractItemDirection::Requested,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ),
    ] {
        store
            .upsert_contract_subscription(&subscription)
            .await
            .expect("persist contract subscription");
    }

    let mut listed_contract = item_exchange_contract(45);
    listed_contract.title = Some("@everyone offers <@1234> a ship".to_string());
    let listing_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![baseline_contract, listed_contract.clone()],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([
            (
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship.clone(), requested_ship.clone()],
                    expiring_cache(),
                )),
            ),
            (
                45,
                Ok(EsiResponse::fresh(
                    vec![offered_ship.clone(), requested_ship.clone()],
                    expiring_cache(),
                )),
            ),
        ]),
    };
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let collector = ContractCollector::new(store.clone(), Arc::new(listing_esi))
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25), (34, 26)]))),
            delivery.clone(),
        );

    let report = collector
        .collect_cycle()
        .await
        .expect("collect a post-baseline listing");

    assert_eq!(report.events.len(), 1);
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(
        sent.len(),
        3,
        "each matching subscription gets its own message"
    );
    assert!(sent.iter().any(|delivery| delivery.ping));
    assert!(sent.iter().all(|delivery| {
        delivery
            .message
            .description
            .as_deref()
            .is_some_and(|description| {
                description.lines().next().is_some_and(|line| {
                    line.starts_with("`<url=\"contract:0//45\">") && line.ends_with("</url>`")
                }) && !description.contains("@everyone")
                    && !description.contains("<@1234>")
            })
            && delivery.message.fields.iter().all(|field| {
                !matches!(
                    field.name.as_str(),
                    "Event" | "Contract Address" | "Issuer" | "Offered" | "Requested"
                )
            })
    }));
    drop(sent);
    assert!(store
        .observed_embed_context(10_000_002, 45)
        .await
        .expect("load observed embed context")
        .and_then(|context| context.observed_at)
        .is_some());
    assert_eq!(
        store
            .delivery_records()
            .await
            .expect("read persisted deliveries")
            .iter()
            .filter(|delivery| delivery.status == DeliveryStatus::Sent)
            .count(),
        3
    );

    let repeated = collector
        .collect_cycle()
        .await
        .expect("repeat the same complete observation");
    assert!(repeated.events.is_empty());
    assert_eq!(delivery.sent.lock().unwrap().len(), 3);
    assert_eq!(
        store
            .delivery_records()
            .await
            .expect("read idempotent persisted deliveries")
            .len(),
        3
    );

    database.destroy().await;
}

#[tokio::test]
async fn complete_observations_snapshot_embed_context_without_a_subscription_or_listing_event() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    let esi = Arc::new(SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        },
        contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
            ContractEmbedContext {
                issuer_character_name: Some("Observed Issuer".to_string()),
                ..ContractEmbedContext::default()
            },
            CacheMetadata::cached_for_seconds(60),
        ))]),
        calls: StdMutex::new(Vec::new()),
    });

    let report = ContractCollector::new(store.clone(), esi.clone())
        .collect_cycle()
        .await
        .expect("record a complete silent baseline");

    assert_eq!(
        report.regions,
        vec![CollectionOutcome::BaselineEstablished {
            region_id: 10_000_002
        }]
    );
    assert!(report.events.is_empty());
    assert_eq!(esi.calls.lock().unwrap().as_slice(), &[44]);
    assert_eq!(
        store
            .observed_embed_context(10_000_002, 44)
            .await
            .expect("load baseline snapshot")
            .and_then(|context| context.issuer_character_name),
        Some("Observed Issuer".to_string())
    );

    database.destroy().await;
}

#[tokio::test]
async fn missing_embed_context_is_backfilled_and_retried_after_a_transient_failure() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    let esi = Arc::new(SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        },
        contexts: StdMutex::new(vec![Err(EsiError::retryable(
            "temporary context failure",
            None,
        ))]),
        calls: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(store.clone(), esi.clone())
        .collect_cycle()
        .await
        .expect("preserve the complete observation when enrichment is transiently unavailable");
    assert!(store
        .observed_embed_context(10_000_002, 44)
        .await
        .expect("read absent transient snapshot")
        .is_none());
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("read the scoped embed-context failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].region_id, Some(10_000_002));
    assert_eq!(failures[0].contract_id, Some(44));
    assert_eq!(
        failures[0].resource_key.as_deref(),
        Some("contract-embed-context:44")
    );
    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed an unrelated enrichment failure");
    sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail) VALUES ($1,$2,$3,now(),$4,$5)")
        .bind(10_000_002_i64)
        .bind(45_i64)
        .bind("contract-embed-context:45")
        .bind("embed_context_enrichment")
        .bind("unrelated retryable enrichment")
        .execute(&raw_pool)
        .await
        .expect("seed unrelated enrichment failure");
    raw_pool.close().await;

    let restarted_esi = Arc::new(SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        },
        contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
            ContractEmbedContext {
                issuer_corporation_name: Some("Recovered Corporation".to_string()),
                ..ContractEmbedContext::default()
            },
            CacheMetadata::cached_for_seconds(60),
        ))]),
        calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), restarted_esi.clone())
        .collect_cycle()
        .await
        .expect("a restarted collector retries the missing observation-time enrichment");
    assert_eq!(esi.calls.lock().unwrap().as_slice(), &[44]);
    assert_eq!(restarted_esi.calls.lock().unwrap().as_slice(), &[44]);
    assert_eq!(
        store
            .observed_embed_context(10_000_002, 44)
            .await
            .expect("load retried snapshot")
            .and_then(|context| context.issuer_corporation_name),
        Some("Recovered Corporation".to_string())
    );
    let remaining_failures = store
        .unresolved_collection_failures()
        .await
        .expect("successful enrichment resolves only its matching failure");
    assert_eq!(remaining_failures.len(), 1);
    assert_eq!(remaining_failures[0].contract_id, Some(45));
    assert_eq!(
        remaining_failures[0].resource_key.as_deref(),
        Some("contract-embed-context:45")
    );

    database.destroy().await;
}

#[tokio::test]
async fn saved_embed_context_resolves_only_its_matching_failure_after_restart() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    ContractCollector::new(
        store.clone(),
        Arc::new(SnapshottingEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
                )]),
                items: HashMap::from([(
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                )]),
            },
            contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
                ContractEmbedContext {
                    issuer_character_name: Some("Persisted Issuer".to_string()),
                    ..ContractEmbedContext::default()
                },
                CacheMetadata::cached_for_seconds(60),
            ))]),
            calls: StdMutex::new(Vec::new()),
        }),
    )
    .collect_cycle()
    .await
    .expect("persist observation-time context before the simulated crash");

    let raw_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed restart failures");
    for (contract_id, resource_key) in [
        (44_i64, "contract-embed-context:44"),
        (45_i64, "contract-embed-context:45"),
    ] {
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail) VALUES (10000002,$1,$2,now(),'embed_context_enrichment','simulated crash before resolution cleanup')")
            .bind(contract_id)
            .bind(resource_key)
            .execute(&raw_pool)
            .await
            .expect("seed scoped enrichment failure");
    }
    raw_pool.close().await;

    let restarted_esi = Arc::new(SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        },
        contexts: StdMutex::new(vec![]),
        calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), restarted_esi.clone())
        .collect_cycle()
        .await
        .expect("reuse the saved embed context after restart");

    assert!(restarted_esi.calls.lock().unwrap().is_empty());
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("saved context resolves only its matching failure");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].contract_id, Some(45));
    assert_eq!(
        failures[0].resource_key.as_deref(),
        Some("contract-embed-context:45")
    );
    database.destroy().await;
}

#[tokio::test]
async fn snapshot_backfill_cap_counts_missing_contexts_and_terminal_delivery_uses_last_snapshot() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contracts = (44..54).map(item_exchange_contract).collect::<Vec<_>>();
    let items = contracts
        .iter()
        .map(|contract| {
            (
                contract.contract_id,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(contract.contract_id)],
                    expiring_cache(),
                )),
            )
        })
        .collect::<HashMap<_, _>>();
    let contexts = contracts
        .iter()
        .map(|contract| {
            Ok(EsiResponse::fresh(
                ContractEmbedContext {
                    issuer_character_name: Some(format!(
                        "Backfilled Issuer {}",
                        contract.contract_id
                    )),
                    ..ContractEmbedContext::default()
                },
                CacheMetadata::cached_for_seconds(60),
            ))
        })
        .collect::<Vec<_>>();
    let esi = Arc::new(SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(contracts.clone(), expiring_page(1))),
            )]),
            items: items.clone(),
        },
        contexts: StdMutex::new(contexts),
        calls: StdMutex::new(Vec::new()),
    });
    let collector = ContractCollector::new(store.clone(), esi.clone());

    collector
        .collect_cycle()
        .await
        .expect("snapshot the first bounded context batch");
    collector
        .collect_cycle()
        .await
        .expect("backfill stable contracts after the first bounded batch");

    assert_eq!(
        esi.calls.lock().unwrap().as_slice(),
        contracts
            .iter()
            .map(|contract| contract.contract_id)
            .collect::<Vec<_>>()
    );
    for contract in &contracts {
        assert!(
            store
                .observed_embed_context(10_000_002, contract.contract_id)
                .await
                .expect("read backfilled observation context")
                .is_some(),
            "contract {} was not backfilled",
            contract.contract_id
        );
    }

    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "terminal-after-backfill",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist a terminal-only subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let last_contract = contracts.last().expect("last stable contract");
    let resolver = ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    contracts[..contracts.len() - 1].to_vec(),
                    expiring_page(1),
                )),
            )]),
            items,
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::NoContent(expiring_cache()))]),
        probe_calls: StdMutex::new(Vec::new()),
    };

    ContractCollector::new(store.clone(), Arc::new(resolver))
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect("deliver the terminal event with backfilled observation context");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].contract_id, last_contract.contract_id);
    assert!(sent[0]
        .message
        .description
        .as_deref()
        .is_some_and(|description| description.lines().any(|line| {
            line == format!("issuer: Backfilled Issuer {}", last_contract.contract_id)
        }) && !description
            .contains(&last_contract.issuer_id.to_string())));
    drop(sent);

    database.destroy().await;
}

#[tokio::test]
async fn observation_context_enrichment_stops_at_the_persisted_limiter_boundary() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let first = item_exchange_contract(44);
    let second = item_exchange_contract(45);
    let esi = Arc::new(SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![first.clone(), second.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        },
        contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
            ContractEmbedContext::default(),
            CacheMetadata {
                rate_limit_limit: Some("1/1m".to_string()),
                rate_limit_remaining: Some(0),
                ..CacheMetadata::cached_for_seconds(60)
            },
        ))]),
        calls: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(store.clone(), esi.clone())
        .collect_cycle()
        .await
        .expect("retain the complete observation while deferring later context enrichment");

    assert_eq!(esi.calls.lock().unwrap().as_slice(), &[44]);
    assert!(store
        .observed_embed_context(10_000_002, first.contract_id)
        .await
        .expect("read first snapshot")
        .is_some());
    assert!(store
        .observed_embed_context(10_000_002, second.contract_id)
        .await
        .expect("read deferred snapshot")
        .is_none());

    database.destroy().await;
}

#[tokio::test]
async fn terminal_only_subscription_uses_the_persisted_observation_time_context() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    let baseline_esi = SnapshottingEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        },
        contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
            ContractEmbedContext {
                issuer_character_name: Some("Observed Issuer".to_string()),
                issuer_corporation_name: Some("Observed Corporation".to_string()),
                issuer_alliance_id: Some(99_000_111),
                issuer_alliance_name: Some("Observed Alliance".to_string()),
                ..ContractEmbedContext::default()
            },
            CacheMetadata::cached_for_seconds(60),
        ))]),
        calls: StdMutex::new(Vec::new()),
    };
    ContractCollector::new(store.clone(), Arc::new(baseline_esi))
        .collect_cycle()
        .await
        .expect("snapshot the silent baseline contract");
    let mut terminal_subscription = confirmed_ship_subscription(
        "terminal-only-sale",
        ContractEventKind::SaleConfirmed,
        ContractItemDirection::Offered,
        ContractEventAction::Post,
    );
    terminal_subscription.filter.root = ContractFilterNode::And(vec![
        terminal_subscription.filter.root,
        ContractFilterNode::Condition(ContractFilterCondition::ObservedAffiliationAlliances(vec![
            99_000_111,
        ])),
    ]);
    store
        .upsert_contract_subscription(&terminal_subscription)
        .await
        .expect("persist a terminal-only subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let resolver = ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::NoContent(expiring_cache()))]),
        probe_calls: StdMutex::new(Vec::new()),
    };

    ContractCollector::new(store.clone(), Arc::new(resolver))
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect("confirm the disappeared public sale");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert!(sent[0]
        .message
        .description
        .as_deref()
        .is_some_and(|description| {
            description
                .lines()
                .any(|line| line == "issuer: [Observed Alliance] Observed Issuer")
                && !description.contains("90000001")
                && !description.contains("98000001")
                && !description.contains("99000111")
        }));
    assert!(sent[0]
        .message
        .fields
        .iter()
        .all(|field| field.name != "Observed Affiliation"));
    assert!(sent[0]
        .message
        .fields
        .iter()
        .all(|field| !matches!(field.name.as_str(), "Buyer" | "Counterparty")));
    drop(sent);

    database.destroy().await;
}

#[tokio::test]
async fn terminal_light_year_range_uses_the_observation_snapshot_after_restart() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    ContractCollector::new(
        store.clone(),
        Arc::new(SnapshottingEsi {
            inner: regional_esi(
                vec![contract.clone()],
                HashMap::from([(
                    44,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                )]),
            ),
            contexts: StdMutex::new(vec![Ok(EsiResponse::fresh(
                ContractEmbedContext {
                    location: killbot_rust::contract_intelligence::ContractLocationContext {
                        solar_system_id: Some(30_000_044),
                        solar_system_position: Some(position_at_light_years(0.0)),
                        ..Default::default()
                    },
                    ..ContractEmbedContext::default()
                },
                CacheMetadata::cached_for_seconds(60),
            ))]),
            calls: StdMutex::new(Vec::new()),
        }),
    )
    .collect_cycle()
    .await
    .expect("persist the observation-time solar-system position");

    let mut subscription = confirmed_ship_subscription(
        "terminal-near-turnur",
        ContractEventKind::SaleConfirmed,
        ContractItemDirection::Offered,
        ContractEventAction::Post,
    );
    subscription.filter.root = ContractFilterNode::And(vec![
        subscription.filter.root,
        ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
            config::SystemRange {
                system_id: 30_002_086,
                range: 8.0,
            },
        ])),
    ]);
    store
        .upsert_contract_subscription(&subscription)
        .await
        .expect("persist the terminal range subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let restarted_esi = Arc::new(PositionedResolutionEsi {
        inner: ResolutionEsi {
            inner: regional_esi(vec![], HashMap::new()),
            probes: StdMutex::new(vec![Ok(ContractItemProbe::NoContent(expiring_cache()))]),
            probe_calls: StdMutex::new(Vec::new()),
        },
        positions: HashMap::from([(30_002_086, position_at_light_years(0.0))]),
        position_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), restarted_esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect("resolve the terminal event after restart");

    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert_eq!(
        restarted_esi.position_calls.lock().unwrap().as_slice(),
        &[30_002_086],
        "terminal filtering may load the static center but never the event's current location"
    );
    database.destroy().await;
}

#[tokio::test]
async fn a_transient_ship_group_lookup_is_retried_after_the_listing_cycle() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let listed_contract = item_exchange_contract(45);
    let offered_ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let baseline_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![baseline_contract.clone()],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([(
            44,
            Ok(EsiResponse::fresh(
                vec![offered_ship.clone()],
                expiring_cache(),
            )),
        )]),
    };
    ContractCollector::new(store.clone(), Arc::new(baseline_esi))
        .collect_cycle()
        .await
        .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "frigates",
            ContractItemDirection::Offered,
            vec![],
            vec![25],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist a ship group subscription");

    let listing_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![baseline_contract.clone(), listed_contract.clone()],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([
            (
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship.clone()],
                    expiring_cache(),
                )),
            ),
            (
                45,
                Ok(EsiResponse::fresh(
                    vec![offered_ship.clone()],
                    expiring_cache(),
                )),
            ),
        ]),
    };
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let collector = ContractCollector::new(store.clone(), Arc::new(listing_esi))
        .with_notifications(
            Arc::new(EventuallyResolvedShipGroup {
                results: StdMutex::new(vec![ShipGroupLookup::TemporarilyUnavailable]),
            }),
            delivery.clone(),
        );

    collector
        .collect_cycle()
        .await
        .expect("record the listing despite a transient group lookup failure");
    assert!(delivery.sent.lock().unwrap().is_empty());

    let restarted_collector = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract, listed_contract],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship.clone()],
                        expiring_cache(),
                    )),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship], expiring_cache())),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(EventuallyResolvedShipGroup {
            results: StdMutex::new(vec![ShipGroupLookup::Resolved(Some(25))]),
        }),
        delivery.clone(),
    );
    restarted_collector
        .collect_cycle()
        .await
        .expect("retry the deferred subscription match after restart");
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .delivery_records()
            .await
            .expect("read deferred delivery")
            .len(),
        1
    );

    database.destroy().await;
}

#[tokio::test]
async fn unsubscribe_during_a_prepared_delivery_preserves_delivery_history() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let listed_contract = item_exchange_contract(45);
    let offered_ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship.clone()],
                    expiring_cache(),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist contract subscription");

    let delivery = Arc::new(RemovingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract, listed_contract],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship.clone()],
                        expiring_cache(),
                    )),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship], expiring_cache())),
                ),
            ]),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("finish the delivery that raced with unsubscribe");

    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert!(store
        .contract_subscriptions_for_channel(42, 77)
        .await
        .expect("removed subscriptions")
        .is_empty());
    assert_eq!(
        store
            .delivery_records()
            .await
            .expect("preserved delivery history"),
        vec![DeliveryRecord {
            id: 1,
            status: DeliveryStatus::Sent,
            discord_message_id: Some("discord-message-id-after-unsubscribe".to_string()),
        }]
    );

    database.destroy().await;
}

#[tokio::test]
async fn oversized_matched_bundles_produce_a_compact_embed_and_preserve_the_full_event() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let listed_contract = item_exchange_contract(45);
    let oversized_bundle: Vec<_> = (0..240)
        .map(|index| PublicContractItem {
            record_id: index,
            type_id: 1_000_000 + index,
            quantity: index + 1,
            is_included: true,
            item_id: Some(10_000 + index),
            raw_quantity: None,
            is_singleton: None,
            is_blueprint_copy: None,
            material_efficiency: None,
            runs: None,
            time_efficiency: None,
        })
        .collect();
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "every-oversized-bundle-item",
            ContractItemDirection::Offered,
            oversized_bundle.iter().map(|item| item.type_id).collect(),
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist contract subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract, listed_contract],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        oversized_bundle.clone(),
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("deliver oversized matching bundle");

    let sent = delivery.sent.lock().unwrap();
    let message = &sent[0].message;
    let field_value_lengths: usize = message
        .fields
        .iter()
        .map(|field| field.value.chars().count())
        .sum();
    let field_name_lengths: usize = message
        .fields
        .iter()
        .map(|field| field.name.chars().count())
        .sum();
    assert!(message
        .fields
        .iter()
        .all(|field| field.value.chars().count() <= 1024));
    assert!(
        message.title.chars().count()
            + message
                .description
                .as_deref()
                .map(str::chars)
                .map(Iterator::count)
                .unwrap_or(0)
            + message
                .footer
                .as_deref()
                .map(str::chars)
                .map(Iterator::count)
                .unwrap_or(0)
            + field_name_lengths
            + field_value_lengths
            <= 6000
    );
    assert!(message.description.as_deref().is_some_and(|description| {
        description.lines().next().is_some_and(|line| {
            line.starts_with("`<url=\"contract:0//45\">") && line.ends_with("</url>`")
        }) && !description.contains("Type 1000001")
    }));
    assert!(message
        .fields
        .iter()
        .all(|field| !matches!(field.name.as_str(), "Offered" | "Contract Address")));
    let delivery_id = sent[0].delivery_id;
    drop(sent);
    let persisted_event = store
        .delivery_event(delivery_id)
        .await
        .expect("read persisted event")
        .expect("delivery event exists");
    assert_eq!(persisted_event.offered_items.len(), 240);

    database.destroy().await;
}

#[tokio::test]
async fn a_listing_without_a_manifest_cannot_match_or_notify_a_ship_subscription() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let offered_ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let baseline_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![baseline_contract.clone()],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([(
            44,
            Ok(EsiResponse::fresh(
                vec![offered_ship.clone()],
                expiring_cache(),
            )),
        )]),
    };
    ContractCollector::new(store.clone(), Arc::new(baseline_esi))
        .collect_cycle()
        .await
        .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist a ship subscription");

    let listing_esi = FakeEsi {
        regions: vec![10_000_002],
        pages: HashMap::from([(
            (10_000_002, 1),
            Ok(EsiResponse::fresh(
                vec![baseline_contract, item_exchange_contract(45)],
                expiring_page(1),
            )),
        )]),
        items: HashMap::from([
            (
                44,
                Ok(EsiResponse::fresh(vec![offered_ship], expiring_cache())),
            ),
            (45, Err(EsiError::retryable("manifest unavailable", None))),
        ]),
    };
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(store.clone(), Arc::new(listing_esi))
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect("retain the missing manifest as an inconclusive observation");

    assert!(report.events.is_empty());
    assert!(delivery.sent.lock().unwrap().is_empty());
    assert!(store
        .delivery_records()
        .await
        .expect("read absent deliveries")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn contract_delivery_retries_with_a_stable_nonce_and_retains_permanent_failures() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let listed_contract = item_exchange_contract(45);
    let ship = offered_ship(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    for (id, action) in [
        ("a-transient", ContractEventAction::PostAndPingEveryone),
        ("b-ambiguous", ContractEventAction::PostAndPing),
        ("c-permanent", ContractEventAction::PostAndPing),
    ] {
        store
            .upsert_contract_subscription(&contract_subscription(
                id,
                ContractItemDirection::Offered,
                vec![587],
                vec![],
                action,
            ))
            .await
            .expect("persist matching contract subscription");
    }
    let delivery = Arc::new(ScriptedDelivery {
        store: store.clone(),
        attempts: StdMutex::new(Vec::new()),
        outcomes: StdMutex::new(vec![
            Err(ContractDeliveryError::transient("Discord rate limited")),
            Ok("transient-retry-message".to_string()),
            Err(ContractDeliveryError::ambiguous("connection closed")),
            Ok("ambiguous-retry-message".to_string()),
            Err(ContractDeliveryError::permanent(
                "missing Send Messages permission",
            )),
        ]),
    });
    let app_state = app_state_for_ping_limiter();
    let ping_limiter = Arc::new(AppStateContractPingLimiter::new(app_state.clone()));
    let clock = Arc::new(FixedDeliveryClock(StdMutex::new(
        Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap(),
    )));
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract, listed_contract],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
                ),
                (45, Ok(EsiResponse::fresh(vec![ship], expiring_cache()))),
            ]),
        }),
    )
    .with_notifications_and_ping_limiter(
        Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
        delivery.clone(),
        ping_limiter.clone(),
    )
    .with_delivery_clock(clock)
    .collect_cycle()
    .await
    .expect("deliver the listed contracts with immediate retries");

    let attempts = delivery.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 5);
    assert_eq!(attempts[0].subscription_id, "a-transient");
    assert_eq!(attempts[0].nonce, attempts[1].nonce);
    assert!(attempts[0].enforce_nonce);
    assert!(attempts[1].enforce_nonce);
    assert!(attempts[0].ping);
    assert_eq!(attempts[0].ping_type, ContractPingType::Everyone);
    assert_eq!(attempts[1].ping_type, ContractPingType::Everyone);
    assert_eq!(attempts[2].ping_type, ContractPingType::Here);
    assert!(attempts[1..].iter().all(|attempt| !attempt.ping));
    drop(attempts);
    assert!(
        !config::try_acquire_channel_ping(&app_state.last_ping_times, 77).await,
        "the first failed contract send consumes the shared killmail ping window"
    );

    let records = store.delivery_records().await.expect("read deliveries");
    assert_eq!(
        records
            .iter()
            .filter(|record| record.status == DeliveryStatus::Sent)
            .count(),
        2
    );
    let failed = records
        .iter()
        .find(|record| record.status == DeliveryStatus::Failed)
        .expect("retain permanent failure");
    assert_eq!(
        store
            .delivery_failure(failed.id)
            .await
            .expect("read delivery failure")
            .expect("permanent failure context"),
        (
            DeliveryFailureKind::Permanent,
            "missing Send Messages permission".to_string()
        )
    );

    database.destroy().await;
}

#[tokio::test]
async fn killmail_ping_window_suppresses_contract_ping_without_suppressing_delivery() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let listed_contract = item_exchange_contract(45);
    let ship = offered_ship(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::PostAndPing,
        ))
        .await
        .expect("persist matching subscription");

    let app_state = app_state_for_ping_limiter();
    assert!(
        config::try_acquire_channel_ping(&app_state.last_ping_times, 77).await,
        "a killmail ping acquires the shared channel window first"
    );
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract, listed_contract],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
                ),
                (45, Ok(EsiResponse::fresh(vec![ship], expiring_cache()))),
            ]),
        }),
    )
    .with_notifications_and_ping_limiter(
        Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
        delivery.clone(),
        Arc::new(AppStateContractPingLimiter::new(app_state)),
    )
    .collect_cycle()
    .await
    .expect("send the contract embed even though the shared ping window is occupied");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert!(
        !sent[0].ping,
        "the occupied killmail window suppresses only the contract mention"
    );

    database.destroy().await;
}

#[tokio::test]
async fn prepared_contract_delivery_survives_restart_and_retries_without_nonce_enforcement_after_window(
) {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let listed_contract = item_exchange_contract(45);
    let second_listed_contract = item_exchange_contract(46);
    let ship = offered_ship(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::PostAndPingEveryone,
        ))
        .await
        .expect("persist matching subscription");

    let clock = Arc::new(FixedDeliveryClock(StdMutex::new(
        Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap(),
    )));
    let first_delivery = Arc::new(ScriptedDelivery {
        store: store.clone(),
        attempts: StdMutex::new(Vec::new()),
        outcomes: StdMutex::new(vec![
            Err(ContractDeliveryError::ambiguous("connection closed")),
            Err(ContractDeliveryError::ambiguous("connection closed")),
            Err(ContractDeliveryError::transient(
                "Discord unavailable\nretry @everyone",
            )),
            Err(ContractDeliveryError::transient(
                "Discord unavailable\nretry @everyone",
            )),
        ]),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract, listed_contract, second_listed_contract],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
                ),
                (45, Ok(EsiResponse::fresh(vec![ship], expiring_cache()))),
                (
                    46,
                    Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
                ),
            ]),
        }),
    )
    .with_notifications_and_ping_limiter(
        Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
        first_delivery.clone(),
        Arc::new(RecordingContractPingLimiter {
            outcomes: StdMutex::new(vec![true, false, false, false]),
            channels: StdMutex::new(Vec::new()),
        }),
    )
    .with_delivery_clock(clock.clone())
    .collect_cycle()
    .await
    .expect("leave ambiguous delivery prepared after its immediate retry");

    let first_attempts = first_delivery.attempts.lock().unwrap();
    assert_eq!(first_attempts.len(), 4);
    assert_eq!(first_attempts[0].nonce, first_attempts[1].nonce);
    assert_eq!(first_attempts[2].nonce, first_attempts[3].nonce);
    assert!(first_attempts.iter().all(|attempt| attempt.enforce_nonce));
    assert!(first_attempts[0].ping);
    assert!(!first_attempts[1].ping);
    assert!(first_attempts
        .iter()
        .all(|attempt| attempt.ping_type == ContractPingType::Everyone));
    let nonces = first_attempts
        .chunks_exact(2)
        .map(|attempts| (attempts[0].contract_id, attempts[0].nonce.clone()))
        .collect::<HashMap<_, _>>();
    drop(first_attempts);
    let prepared = store
        .delivery_records()
        .await
        .expect("read prepared deliveries");
    assert_eq!(prepared.len(), 2);
    assert!(prepared
        .iter()
        .all(|record| record.status == DeliveryStatus::Prepared));
    let failures = store
        .unresolved_delivery_failures()
        .await
        .expect("read retryable delivery failures for operators");
    assert_eq!(failures.len(), 2);
    assert_eq!(
        failures
            .iter()
            .map(|failure| (failure.contract_id, failure.failure_kind.clone()))
            .collect::<Vec<_>>(),
        vec![
            (45, DeliveryFailureKind::Ambiguous),
            (46, DeliveryFailureKind::Transient),
        ]
    );
    assert_eq!(failures[1].status, DeliveryStatus::Prepared);
    assert_eq!(failures[1].attempt_count, 2);
    assert_eq!(
        failures[1].detail,
        "Discord unavailable retry @\u{200b}everyone"
    );

    *clock.0.lock().unwrap() += chrono::Duration::minutes(6);
    let limiter_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to set the persisted ESI limiter boundary");
    sqlx::query(
        "INSERT INTO esi_collection_limiter_state (limiter_scope, pause_until) VALUES (TRUE, now() + interval '1 hour') ON CONFLICT (limiter_scope) DO UPDATE SET pause_until = EXCLUDED.pause_until",
    )
    .execute(&limiter_pool)
    .await
    .expect("activate the persisted ESI limiter boundary");
    limiter_pool.close().await;
    let restarted_delivery = Arc::new(ScriptedDelivery {
        store: store.clone(),
        attempts: StdMutex::new(Vec::new()),
        outcomes: StdMutex::new(vec![
            Ok("post-window-message-45".to_string()),
            Ok("post-window-message-46".to_string()),
        ]),
    });
    let restart_error = ContractCollector::new(store.clone(), Arc::new(FailingContractEsi))
        .with_notifications_and_ping_limiter(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
            restarted_delivery.clone(),
            Arc::new(RecordingContractPingLimiter {
                outcomes: StdMutex::new(vec![true, false]),
                channels: StdMutex::new(Vec::new()),
            }),
        )
        .with_delivery_clock(clock)
        .collect_cycle()
        .await
        .expect_err(
            "the persisted ESI limiter remains active after replaying the prepared delivery",
        );
    assert!(restart_error
        .to_string()
        .contains("persisted global ESI limiter"));

    let restarted_attempts = restarted_delivery.attempts.lock().unwrap();
    assert_eq!(restarted_attempts.len(), 2);
    assert!(restarted_attempts.iter().all(|attempt| {
        attempt.nonce == nonces[&attempt.contract_id]
            && !attempt.enforce_nonce
            && attempt.ping_type == ContractPingType::Everyone
    }));
    assert!(restarted_attempts[0].ping);
    assert!(!restarted_attempts[1].ping);
    drop(restarted_attempts);
    assert!(store
        .delivery_records()
        .await
        .expect("read sent deliveries")
        .iter()
        .all(|record| record.status == DeliveryStatus::Sent));
    assert!(store
        .unresolved_delivery_failures()
        .await
        .expect("successful retry resolves operator-visible delivery failures")
        .is_empty());
    for delivery in prepared {
        assert!(store
            .delivery_failure(delivery.id)
            .await
            .expect("read resolved delivery failure")
            .is_none());
    }

    database.destroy().await;
}

#[tokio::test]
async fn prepared_delivery_replays_before_region_discovery_outage() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    store
        .upsert_contract_subscription(&contract_subscription(
            "prepared",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the subscription that owned the prior prepared delivery");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to create the prior prepared delivery");
    sqlx::query("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, ping_type, status, delivery_nonce) VALUES (42, 77, 'prepared', 45, 'listed', '{}', '{\"title\":\"prior contract notification\",\"fields\":[]}', FALSE, 'here', 'prepared', 'ci-prepared')")
        .execute(&pool)
        .await
        .expect("persist the delivery from the prior process");
    pool.close().await;

    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let error = ContractCollector::new(store.clone(), Arc::new(FailingContractEsi))
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
            delivery.clone(),
        )
        .collect_cycle()
        .await
        .expect_err("the simulated ESI region outage still follows prepared delivery replay");
    assert!(error.to_string().contains("ESI is unavailable"));
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].nonce, "ci-prepared");
    drop(sent);
    assert_eq!(
        store
            .delivery_records()
            .await
            .expect("read delivered record")[0]
            .status,
        DeliveryStatus::Sent
    );

    database.destroy().await;
}

#[tokio::test]
async fn bounded_regional_collection_dispatches_a_fast_enriched_listing_before_a_slow_region() {
    const FAST_REGION: i64 = 10_000_002;
    const SLOW_REGION: i64 = 10_000_003;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FAST_REGION, SLOW_REGION],
            pages: HashMap::from([
                (
                    (FAST_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (SLOW_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
            ]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish independent silent regional baselines");
    store
        .upsert_contract_subscription(&contract_subscription(
            "fast-listed-ship",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the fast-region listing subscription");

    let listed_contract = item_exchange_contract(44);
    let slow_listed_contract = item_exchange_contract(45);
    let esi = Arc::new(GatedRegionalEsi {
        slow_region: SLOW_REGION,
        slow_started: Arc::new(Notify::new()),
        slow_release: Arc::new(Notify::new()),
        pages: HashMap::from([
            (FAST_REGION, vec![listed_contract.clone()]),
            (SLOW_REGION, vec![slow_listed_contract.clone()]),
        ]),
        items: HashMap::from([(44, vec![offered_ship(1)]), (45, vec![offered_ship(2)])]),
        probes: StdMutex::new(Vec::new()),
    });
    let delivery = Arc::new(NotifyingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
        delivered: Arc::new(Notify::new()),
    });
    let slow_started = esi.slow_started.notified();
    let delivered = delivery.delivered.notified();
    let ship_groups = Arc::new(CountingShipGroups {
        groups: HashMap::from([(587, 25)]),
        calls: AtomicU64::new(0),
    });
    let collector = ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(ship_groups.clone(), delivery.clone())
        .with_delivery_clock(Arc::new(FixedDeliveryClock(StdMutex::new(
            Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap(),
        ))))
        .with_max_concurrent_regions(2);
    let cycle = tokio::spawn(async move { collector.collect_cycle().await });

    tokio::time::timeout(Duration::from_secs(2), slow_started)
        .await
        .expect("the slow regional attempt starts concurrently");
    tokio::time::timeout(Duration::from_secs(2), delivered)
        .await
        .expect("the fast region reaches delivery without waiting for the slow region");
    assert!(
        !cycle.is_finished(),
        "the slow attempt remains in flight when the fast listing is delivered"
    );
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].contract_id, listed_contract.contract_id);
    assert!(
        sent[0].message.title.contains("Rifter"),
        "snapshot enrichment completes before the fast listing is dispatched: {}",
        sent[0].message.title
    );
    drop(sent);

    esi.slow_release.notify_one();
    let report = cycle
        .await
        .expect("join bounded regional cycle")
        .expect("complete the released slow regional attempt");
    assert_eq!(
        report
            .regions
            .iter()
            .map(|outcome| match outcome {
                CollectionOutcome::BaselineEstablished { region_id }
                | CollectionOutcome::RecoveryBaselineEstablished { region_id }
                | CollectionOutcome::Complete { region_id, .. }
                | CollectionOutcome::Inconclusive { region_id, .. } => *region_id,
            })
            .collect::<Vec<_>>(),
        vec![FAST_REGION, SLOW_REGION],
        "the final report preserves discovery order despite completion-order dispatch"
    );
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.contract.contract_id)
            .collect::<Vec<_>>(),
        vec![
            listed_contract.contract_id,
            slow_listed_contract.contract_id
        ],
        "the final report preserves discovery order even when fast delivery happened first"
    );
    assert_eq!(
        ship_groups.calls.load(Ordering::Relaxed),
        2,
        "the immediate dispatch evaluates each listed event once and does not re-evaluate it at cycle end"
    );

    database.destroy().await;
}

#[tokio::test]
async fn started_regional_attempts_commit_while_parent_delivery_is_gated() {
    const FAST_REGION: i64 = 10_000_002;
    const SLOW_REGION: i64 = 10_000_003;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FAST_REGION, SLOW_REGION],
            pages: HashMap::from([
                (
                    (FAST_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (SLOW_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
            ]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish independent silent regional baselines");
    store
        .upsert_contract_subscription(&contract_subscription(
            "started-attempts-progress",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the listing subscription");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to mark the regional baseline boundary");
    let baseline_batch_id: i64 =
        sqlx::query_scalar("SELECT max(id) FROM regional_observation_batches")
            .fetch_one(&pool)
            .await
            .expect("read the regional baseline boundary");

    let esi = Arc::new(GatedRegionalEsi {
        slow_region: SLOW_REGION,
        slow_started: Arc::new(Notify::new()),
        slow_release: Arc::new(Notify::new()),
        pages: HashMap::from([
            (FAST_REGION, vec![item_exchange_contract(44)]),
            (SLOW_REGION, vec![item_exchange_contract(45)]),
        ]),
        items: HashMap::from([(44, vec![offered_ship(1)]), (45, vec![offered_ship(2)])]),
        probes: StdMutex::new(Vec::new()),
    });
    let delivery = Arc::new(FirstDeliveryGate {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        gate_open: AtomicBool::new(true),
    });
    let slow_started = esi.slow_started.notified();
    let delivery_entered = delivery.entered.notified();
    let collector = ContractCollector::new(store, esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
            delivery.clone(),
        )
        .with_max_concurrent_regions(2);
    let cycle = tokio::spawn(async move { collector.collect_cycle().await });

    tokio::time::timeout(Duration::from_secs(2), slow_started)
        .await
        .expect("the second regional attempt starts before parent postprocessing");
    tokio::time::timeout(Duration::from_secs(2), delivery_entered)
        .await
        .expect("the first regional delivery reaches its controlled parent gate");
    esi.slow_release.notify_one();
    let committed_before_delivery_release = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let committed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM regional_observation_batches WHERE id > $1 AND region_id = $2 AND outcome = 'complete'",
            )
            .bind(baseline_batch_id)
            .bind(SLOW_REGION)
            .fetch_one(&pool)
            .await
            .expect("read the independently committed second regional batch");
            if committed == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if committed_before_delivery_release.is_err() {
        delivery.release.notify_one();
        cycle
            .await
            .expect("join failed current scheduler after releasing delivery")
            .expect("finish current scheduler after releasing delivery");
        panic!(
            "the second regional attempt did not commit while first-region parent delivery was gated"
        );
    }
    delivery.release.notify_one();
    let report = cycle
        .await
        .expect("join bounded regional collection")
        .expect("finish bounded regional collection");
    assert_eq!(
        report
            .regions
            .iter()
            .map(|outcome| match outcome {
                CollectionOutcome::BaselineEstablished { region_id }
                | CollectionOutcome::RecoveryBaselineEstablished { region_id }
                | CollectionOutcome::Complete { region_id, .. }
                | CollectionOutcome::Inconclusive { region_id, .. } => *region_id,
            })
            .collect::<Vec<_>>(),
        vec![FAST_REGION, SLOW_REGION],
        "completed reports remain in region discovery order"
    );
    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn parent_postprocess_error_drains_started_successes_into_restartable_delivery() {
    const FIRST_REGION: i64 = 10_000_002;
    const SECOND_REGION: i64 = 10_000_003;
    const FIRST_CONTRACT: i64 = 44;
    const SECOND_CONTRACT: i64 = 45;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FIRST_REGION, SECOND_REGION],
            pages: HashMap::from([
                (
                    (FIRST_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (SECOND_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
            ]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish independent silent regional baselines");
    store
        .upsert_contract_subscription(&contract_subscription(
            "postprocess-drain",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the listing subscription");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to install the isolated parent postprocess failure");
    sqlx::query("CREATE FUNCTION fail_first_embed_snapshot_test_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.contract_id = 44 THEN RAISE EXCEPTION 'synthetic first parent postprocess failure'; END IF; RETURN NEW; END; $$")
        .execute(&pool)
        .await
        .expect("create first-snapshot failure function");
    sqlx::query("CREATE TRIGGER fail_first_embed_snapshot_test_insert BEFORE INSERT ON contract_observed_embed_contexts FOR EACH ROW EXECUTE FUNCTION fail_first_embed_snapshot_test_insert()")
        .execute(&pool)
        .await
        .expect("fail only the first completed regional postprocess");

    let esi = Arc::new(GatedRegionalEsi {
        slow_region: SECOND_REGION,
        slow_started: Arc::new(Notify::new()),
        slow_release: Arc::new(Notify::new()),
        pages: HashMap::from([
            (FIRST_REGION, vec![item_exchange_contract(FIRST_CONTRACT)]),
            (SECOND_REGION, vec![item_exchange_contract(SECOND_CONTRACT)]),
        ]),
        items: HashMap::from([
            (FIRST_CONTRACT, vec![offered_ship(1)]),
            (SECOND_CONTRACT, vec![offered_ship(2)]),
        ]),
        probes: StdMutex::new(Vec::new()),
    });
    let initial_delivery = Arc::new(ScriptedDelivery {
        store: store.clone(),
        attempts: StdMutex::new(Vec::new()),
        outcomes: StdMutex::new(vec![
            Err(ContractDeliveryError::transient(
                "first replayable send failure",
            )),
            Err(ContractDeliveryError::transient(
                "second replayable send failure",
            )),
        ]),
    });
    let slow_started = esi.slow_started.notified();
    let collector = ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
            initial_delivery.clone(),
        )
        .with_max_concurrent_regions(2);
    let cycle = tokio::spawn(async move { collector.collect_cycle().await });

    tokio::time::timeout(Duration::from_secs(2), slow_started)
        .await
        .expect("both regional attempts start before the parent failure");
    esi.slow_release.notify_one();
    let error = cycle
        .await
        .expect("join the collector that retains started work")
        .expect_err("the first parent snapshot failure remains observable after draining");
    assert!(
        error
            .to_string()
            .contains("synthetic first parent postprocess failure"),
        "the original parent error is returned after the started attempt drain: {error}"
    );
    assert_eq!(
        initial_delivery.attempts.lock().unwrap().len(),
        2,
        "the independently completed second region prepares and attempts its replayable listing before returning the parent error"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM contract_outbound_deliveries WHERE contract_id = $1 AND status = 'prepared'",
        )
        .bind(SECOND_CONTRACT)
        .fetch_one(&pool)
        .await
        .expect("read the replayable second-region listing"),
        1,
        "the second region retains exactly one prepared listing for restart"
    );
    sqlx::query(
        "DROP TRIGGER fail_first_embed_snapshot_test_insert ON contract_observed_embed_contexts",
    )
    .execute(&pool)
    .await
    .expect("remove the first-snapshot failure trigger before restart");
    sqlx::query("DROP FUNCTION fail_first_embed_snapshot_test_insert()")
        .execute(&pool)
        .await
        .expect("remove the first-snapshot failure function before restart");

    let restarted_delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FIRST_REGION, SECOND_REGION],
            pages: HashMap::from([
                (
                    (FIRST_REGION, 1),
                    Ok(EsiResponse::fresh(
                        vec![item_exchange_contract(FIRST_CONTRACT)],
                        expiring_page(1),
                    )),
                ),
                (
                    (SECOND_REGION, 1),
                    Ok(EsiResponse::fresh(
                        vec![item_exchange_contract(SECOND_CONTRACT)],
                        expiring_page(1),
                    )),
                ),
            ]),
            items: HashMap::from([
                (
                    FIRST_CONTRACT,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    SECOND_CONTRACT,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
        restarted_delivery.clone(),
    )
    .with_max_concurrent_regions(2)
    .collect_cycle()
    .await
    .expect("restart replays the prepared second-region listing once");
    let sent = restarted_delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "restart sends only the retained listing");
    assert_eq!(sent[0].contract_id, SECOND_CONTRACT);
    drop(sent);
    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn listings_and_each_terminal_delivery_precede_gated_later_resolution_probes() {
    const REGION: i64 = 10_000_002;
    const FIRST_TERMINAL: i64 = 44;
    const SECOND_TERMINAL: i64 = 45;
    const LISTED: i64 = 46;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut first_terminal = item_exchange_contract(FIRST_TERMINAL);
    first_terminal.date_expired = Utc::now() + chrono::Duration::hours(1);
    let mut second_terminal = item_exchange_contract(SECOND_TERMINAL);
    second_terminal.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![REGION],
            pages: HashMap::from([(
                (REGION, 1),
                Ok(EsiResponse::fresh(
                    vec![first_terminal.clone(), second_terminal.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    FIRST_TERMINAL,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    SECOND_TERMINAL,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish disappearing contracts as a silent baseline");
    for subscription in [
        contract_subscription(
            "listed-before-probes",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ),
        confirmed_ship_subscription(
            "terminal-before-next-probe",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ),
    ] {
        store
            .upsert_contract_subscription(&subscription)
            .await
            .expect("persist immediate lifecycle delivery subscription");
    }

    let esi = Arc::new(GatedProbeBatchEsi {
        inner: FakeEsi {
            regions: vec![REGION],
            pages: HashMap::from([(
                (REGION, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(LISTED)],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                LISTED,
                Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
            )]),
        },
        probes: StdMutex::new(vec![
            Ok(ContractItemProbe::NoContent(expiring_cache())),
            Ok(ContractItemProbe::NoContent(expiring_cache())),
        ]),
        probe_count: AtomicU64::new(0),
        first_probe_started: Arc::new(Notify::new()),
        first_probe_release: Arc::new(Notify::new()),
        second_probe_started: Arc::new(Notify::new()),
        second_probe_release: Arc::new(Notify::new()),
    });
    let delivery = Arc::new(NotifyingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
        delivered: Arc::new(Notify::new()),
    });
    let first_delivery = delivery.delivered.notified();
    let first_probe_started = esi.first_probe_started.notified();
    let collector = ContractCollector::new(store, esi.clone()).with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
        delivery.clone(),
    );
    let cycle = tokio::spawn(async move { collector.collect_cycle().await });

    if tokio::time::timeout(Duration::from_secs(2), first_delivery)
        .await
        .is_err()
    {
        esi.first_probe_release.notify_one();
        esi.second_probe_release.notify_one();
        cycle
            .await
            .expect("join the released listing-before-probe regression")
            .expect("finish the released listing-before-probe regression");
        panic!("the ready listing waited behind the first terminal probe");
    }
    assert_eq!(delivery.sent.lock().unwrap()[0].contract_id, LISTED);
    let first_terminal_delivery = delivery.delivered.notified();
    tokio::time::timeout(Duration::from_secs(2), first_probe_started)
        .await
        .expect("the first terminal probe begins after the listing delivery");
    esi.first_probe_release.notify_one();
    if tokio::time::timeout(Duration::from_secs(2), first_terminal_delivery)
        .await
        .is_err()
    {
        esi.second_probe_release.notify_one();
        cycle
            .await
            .expect("join the released first-terminal regression")
            .expect("finish the released first-terminal regression");
        panic!("the first terminal result waited behind the later gated probe");
    }
    assert_eq!(delivery.sent.lock().unwrap()[1].contract_id, FIRST_TERMINAL);
    let second_probe_started = esi.second_probe_started.notified();
    tokio::time::timeout(Duration::from_secs(2), second_probe_started)
        .await
        .expect("the second terminal probe begins only after first terminal dispatch");
    esi.second_probe_release.notify_one();
    cycle
        .await
        .expect("join the released lifecycle collection")
        .expect("finish the released lifecycle collection");
    database.destroy().await;
}

#[tokio::test]
async fn transient_terminal_probe_failures_back_off_so_the_ninth_due_case_advances() {
    const REGION: i64 = 10_000_002;
    const FIRST_CONTRACT: i64 = 44;
    const NINTH_CONTRACT: i64 = 52;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![REGION],
            pages: HashMap::from([(
                (REGION, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the regional baseline before seeding due cases");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed more than one regional terminal-probe batch");
    for contract_id in FIRST_CONTRACT..=NINTH_CONTRACT {
        sqlx::query("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES ($1,$2,$3,$4,now(),now(),now() - interval '1 second','awaiting_resolution')")
            .bind(REGION)
            .bind(contract_id)
            .bind(serde_json::to_value(item_exchange_contract(contract_id)).expect("serialize the due resolution contract"))
            .bind(serde_json::json!({"offered_items": [], "requested_items": []}))
            .execute(&pool)
            .await
            .expect("seed a due regional terminal probe");
    }

    let first_pass = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![REGION],
            pages: HashMap::from([(
                (REGION, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(
            (FIRST_CONTRACT..NINTH_CONTRACT)
                .map(|_| {
                    Err(EsiError::retryable(
                        "transient terminal probe failure",
                        None,
                    ))
                })
                .collect(),
        ),
        probe_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store.clone(), first_pass.clone())
        .collect_cycle()
        .await
        .expect("isolated transient probe failures do not abort the regional cycle");
    assert_eq!(
        first_pass
            .probe_calls
            .lock()
            .unwrap()
            .iter()
            .map(|(contract_id, _)| *contract_id)
            .collect::<Vec<_>>(),
        (FIRST_CONTRACT..NINTH_CONTRACT).collect::<Vec<_>>(),
        "the first bounded regional pass attempts the first eight due cases"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM contract_resolution_cases WHERE region_id = $1 AND contract_id < $2 AND next_probe_at > now()",
        )
        .bind(REGION)
        .bind(NINTH_CONTRACT)
        .fetch_one(&pool)
        .await
        .expect("read durable retry scheduling for transient probe failures"),
        8,
        "each transient failure advances its persisted next-probe boundary before the next fair pass"
    );

    let second_pass = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![REGION],
            pages: HashMap::from([(
                (REGION, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::Available(EsiResponse::fresh(
            vec![],
            CacheMetadata::cached_for_seconds(60),
        )))]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(store, second_pass.clone())
        .collect_cycle()
        .await
        .expect("the fair follow-up pass resolves the still-due ninth case");
    assert_eq!(
        second_pass.probe_calls.lock().unwrap().as_slice(),
        [(NINTH_CONTRACT, None)],
        "the ninth case advances instead of the first eight being selected again"
    );
    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn shared_rate_bucket_paces_concurrent_regional_requests_with_controlled_time() {
    const FIRST_REGION: i64 = 10_000_002;
    const SECOND_REGION: i64 = 10_000_003;

    let database = TemporaryDatabase::new().await;
    let started_at = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed rate-bucket test time");
    let pacer = Arc::new(AdvancingRequestPacer {
        now: StdMutex::new(started_at),
        waits: StdMutex::new(Vec::new()),
    });
    let metadata = CacheMetadata {
        rate_limit_group: Some("public-contracts".to_string()),
        rate_limit_limit: Some("20/60s".to_string()),
        rate_limit_remaining: Some(2),
        ..expiring_page(1)
    };
    let esi = Arc::new(TimestampedRateBucketEsi {
        pacer: pacer.clone(),
        calls: StdMutex::new(Vec::new()),
        regions: vec![FIRST_REGION, SECOND_REGION],
        discovery_metadata: metadata,
        page_metadata: StdMutex::new(vec![
            CacheMetadata {
                rate_limit_group: Some("healthy-other-group".to_string()),
                rate_limit_limit: Some("100/60s".to_string()),
                rate_limit_remaining: Some(99),
                ..expiring_page(1)
            },
            expiring_page(1),
        ]),
    });
    let report = ContractCollector::new(database.store().await, esi.clone())
        .with_request_pacer(pacer.clone())
        .with_max_concurrent_regions(2)
        .collect_cycle()
        .await
        .expect("low but positive bucket capacity waits rather than rejects regional work");
    assert_eq!(
        report
            .regions
            .iter()
            .map(|outcome| match outcome {
                CollectionOutcome::BaselineEstablished { region_id }
                | CollectionOutcome::RecoveryBaselineEstablished { region_id }
                | CollectionOutcome::Complete { region_id, .. }
                | CollectionOutcome::Inconclusive { region_id, .. } => *region_id,
            })
            .collect::<Vec<_>>(),
        vec![FIRST_REGION, SECOND_REGION],
        "soft pacing retains both started regional attempts"
    );
    let waits = pacer.waits.lock().unwrap();
    assert!(
        waits.iter().all(
            |deadline| *deadline == started_at + chrono::Duration::seconds(30)
                || *deadline == started_at + chrono::Duration::seconds(90)
        ),
        "only the persisted low-bucket boundaries are waited"
    );
    assert_eq!(
        waits.last(),
        Some(&(started_at + chrono::Duration::seconds(90))),
        "the headerless and different-group responses retain the later staggered admission"
    );
    drop(waits);
    let page_times = esi
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(call, at)| call.starts_with("page:").then_some(*at))
        .collect::<Vec<_>>();
    assert_eq!(
        page_times,
        vec![
            started_at + chrono::Duration::seconds(30),
            started_at + chrono::Duration::seconds(90),
        ],
        "no concurrent page request bursts through the near-exhausted shared bucket"
    );
    database.destroy().await;
}

#[tokio::test]
async fn delayed_same_group_response_retains_active_pacing_for_later_and_restarted_requests() {
    let database = TemporaryDatabase::new().await;
    let started_at = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed delayed-response pacing test time");
    let pacer = Arc::new(GatedAdvancingRequestPacer {
        now: StdMutex::new(started_at),
        waits: StdMutex::new(Vec::new()),
        wait_entered: Arc::new(Notify::new()),
        wait_release: Arc::new(Notify::new()),
    });
    let esi = Arc::new(DelayedSameGroupPacingEsi {
        pacer: pacer.clone(),
        second_started: Arc::new(Notify::new()),
        second_release: Arc::new(Notify::new()),
        calls: StdMutex::new(Vec::new()),
    });
    let first_collector = ContractCollector::new(database.store().await, esi.clone())
        .with_request_pacer(pacer.clone())
        .with_max_concurrent_regions(2);
    let first_cycle = tokio::spawn(async move { first_collector.collect_cycle().await });

    tokio::time::timeout(Duration::from_secs(1), esi.second_started.notified())
        .await
        .expect("two regional pages start before either response is postprocessed");
    tokio::time::timeout(Duration::from_secs(1), pacer.wait_entered.notified())
        .await
        .expect("the third region waits on the first low-water response");
    let delayed_response_at = started_at + chrono::Duration::seconds(1);
    pacer.advance_to(delayed_response_at);
    esi.second_release.notify_one();

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to observe the delayed same-group response");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let updated_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT updated_at FROM esi_collection_limiter_state WHERE limiter_scope = TRUE",
            )
            .fetch_optional(&pool)
            .await
            .expect("read delayed-response limiter state")
            .flatten();
            if updated_at.is_some_and(|updated_at| updated_at >= delayed_response_at) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the delayed same-group response is persisted before releasing the third request");
    let limiter = sqlx::query(
        "SELECT pacing_active, next_request_at FROM esi_collection_limiter_state WHERE limiter_scope = TRUE",
    )
    .fetch_one(&pool)
    .await
    .expect("read pacing state after delayed same-group response");
    assert!(
        limiter.get::<bool, _>("pacing_active"),
        "a delayed healthy response in the same group cannot clear the newer active low-water pacing"
    );
    assert_eq!(
        limiter.get::<Option<DateTime<Utc>>, _>("next_request_at"),
        Some(started_at + chrono::Duration::seconds(30)),
        "the delayed healthy response retains the low-water deadline until reservation consumes it"
    );
    pool.close().await;

    pacer.wait_release.notify_one();
    let first_report = tokio::time::timeout(Duration::from_secs(1), first_cycle)
        .await
        .expect("release the third regional request")
        .expect("join first collection cycle")
        .expect("complete all three regional attempts");
    assert_eq!(first_report.regions.len(), 3);
    assert_eq!(
        esi.calls
            .lock()
            .unwrap()
            .iter()
            .find_map(|(region_id, at)| (*region_id == 10_000_004).then_some(*at)),
        Some(started_at + chrono::Duration::seconds(30)),
        "the third request reaches the wire only after its active low-water admission wait"
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to expire regional discovery before restart");
    sqlx::query("DELETE FROM esi_cache_metadata WHERE resource_key = 'regions'")
        .execute(&pool)
        .await
        .expect("force restart to re-enter the shared admission seam");
    pool.close().await;
    let restarted = ContractCollector::new(
        database.store().await,
        Arc::new(FakeEsi {
            regions: vec![],
            pages: HashMap::new(),
            items: HashMap::new(),
        }),
    )
    .with_request_pacer(pacer.clone());
    let restarted_cycle = tokio::spawn(async move { restarted.collect_cycle().await });
    tokio::time::timeout(Duration::from_secs(1), pacer.wait_entered.notified())
        .await
        .expect("restart retains the reservation made by the third request");
    assert_eq!(
        pacer.waits.lock().unwrap().as_slice(),
        &[
            started_at + chrono::Duration::seconds(30),
            started_at + chrono::Duration::seconds(90),
        ],
        "the restarted collector still waits after the delayed same-group response"
    );
    pacer.wait_release.notify_one();
    restarted_cycle
        .await
        .expect("join restarted collector")
        .expect("restart completes after its retained pacing boundary");
    database.destroy().await;
}

#[tokio::test]
async fn bounded_regional_collection_dispatches_fast_terminal_evidence_before_a_slow_region() {
    const FAST_REGION: i64 = 10_000_002;
    const SLOW_REGION: i64 = 10_000_003;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut sale = item_exchange_contract(44);
    sale.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FAST_REGION, SLOW_REGION],
            pages: HashMap::from([
                (
                    (FAST_REGION, 1),
                    Ok(EsiResponse::fresh(vec![sale.clone()], expiring_page(1))),
                ),
                (
                    (SLOW_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
            ]),
            items: HashMap::from([(
                sale.contract_id,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        }),
    )
    .with_max_concurrent_regions(2)
    .collect_cycle()
    .await
    .expect("establish independent regional baselines before disappearance evidence");
    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "fast-sale-confirmed",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the terminal evidence subscription");

    let esi = Arc::new(GatedRegionalEsi {
        slow_region: SLOW_REGION,
        slow_started: Arc::new(Notify::new()),
        slow_release: Arc::new(Notify::new()),
        pages: HashMap::from([(FAST_REGION, vec![]), (SLOW_REGION, vec![])]),
        items: HashMap::new(),
        probes: StdMutex::new(vec![Ok(ContractItemProbe::NoContent(expiring_cache()))]),
    });
    let delivery = Arc::new(NotifyingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
        delivered: Arc::new(Notify::new()),
    });
    let slow_started = esi.slow_started.notified();
    let delivered = delivery.delivered.notified();
    let collector = ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 25)]))),
            delivery.clone(),
        )
        .with_max_concurrent_regions(2);
    let cycle = tokio::spawn(async move { collector.collect_cycle().await });

    tokio::time::timeout(Duration::from_secs(2), slow_started)
        .await
        .expect("the unrelated slow regional attempt starts");
    if tokio::time::timeout(Duration::from_secs(2), delivered)
        .await
        .is_err()
    {
        esi.slow_release.notify_one();
        cycle
            .await
            .expect("join the released cycle after the failing assertion")
            .expect("complete the released slow region");
        panic!("conclusive fast terminal evidence waited for an unrelated slow region");
    }
    assert!(
        !cycle.is_finished(),
        "the fast terminal notification is sent while the slow region remains in flight"
    );
    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].contract_id, sale.contract_id);
    drop(sent);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect immediate terminal notification state");
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT notification_pending FROM contract_resolution_cases WHERE region_id = $1 AND contract_id = $2",
    )
    .bind(FAST_REGION)
    .bind(sale.contract_id)
    .fetch_one(&pool)
    .await
    .expect("read the fast terminal notification state"));
    pool.close().await;

    esi.slow_release.notify_one();
    let report = cycle
        .await
        .expect("join bounded terminal-evidence cycle")
        .expect("finish the released slow region");
    assert!(report.events.iter().any(|event| {
        event.region_id == FAST_REGION && event.kind == ContractEventKind::SaleConfirmed
    }));

    database.destroy().await;
}

#[tokio::test]
async fn shared_limiter_stops_unstarted_regions_and_preserves_a_completed_batch_after_restart() {
    const FIRST_REGION: i64 = 10_000_002;
    const SECOND_REGION: i64 = 10_000_003;
    const THIRD_REGION: i64 = 10_000_004;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FIRST_REGION, SECOND_REGION, THIRD_REGION],
            pages: HashMap::from([
                (
                    (FIRST_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (SECOND_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (THIRD_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
            ]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish three independent regional baselines");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to mark the baseline boundary");
    let baseline_batch_id: i64 =
        sqlx::query_scalar("SELECT max(id) FROM regional_observation_batches")
            .fetch_one(&pool)
            .await
            .expect("read the baseline batch boundary");
    pool.close().await;

    let boundary_metadata = CacheMetadata {
        rate_limit_group: Some("public-contracts".to_string()),
        rate_limit_limit: Some("1/1m".to_string()),
        rate_limit_remaining: Some(0),
        ..expiring_page(1)
    };
    let report = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FIRST_REGION, SECOND_REGION, THIRD_REGION],
            pages: HashMap::from([(
                (FIRST_REGION, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    boundary_metadata,
                )),
            )]),
            items: HashMap::new(),
        }),
    )
    .with_max_concurrent_regions(1)
    .collect_cycle()
    .await
    .expect("the completed first region survives the shared limiter boundary");

    assert!(report.retry_after.is_some());
    assert_eq!(
        report.regions,
        vec![CollectionOutcome::Complete {
            region_id: FIRST_REGION,
            observed_contracts: 1,
        }],
        "unstarted regions have no fabricated complete or inconclusive outcomes"
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect post-boundary batches");
    let batches = sqlx::query(
        "SELECT region_id, outcome FROM regional_observation_batches WHERE id > $1 ORDER BY id",
    )
    .bind(baseline_batch_id)
    .fetch_all(&pool)
    .await
    .expect("read only newly attempted regional batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].get::<i64, _>("region_id"), FIRST_REGION);
    assert_eq!(batches[0].get::<String, _>("outcome"), "complete");
    pool.close().await;

    let restart_error =
        ContractCollector::new(database.store().await, Arc::new(FakeEsi::default()))
            .with_max_concurrent_regions(1)
            .collect_cycle()
            .await
            .expect_err("the persisted limiter blocks discovery after restart");
    assert!(restart_error
        .to_string()
        .contains("persisted global ESI limiter"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to confirm restart did not write a batch");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM regional_observation_batches WHERE id > $1",
        )
        .bind(baseline_batch_id)
        .fetch_one(&pool)
        .await
        .expect("count post-boundary batches after restart"),
        1
    );
    pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn shared_limiter_does_not_cancel_an_already_started_region_or_start_a_third_region() {
    const FIRST_REGION: i64 = 10_000_002;
    const SECOND_REGION: i64 = 10_000_003;
    const THIRD_REGION: i64 = 10_000_004;

    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![FIRST_REGION, SECOND_REGION, THIRD_REGION],
            pages: HashMap::from([
                (
                    (FIRST_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (SECOND_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
                (
                    (THIRD_REGION, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                ),
            ]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish three independent regional baselines");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to mark the cap-two baseline boundary");
    let baseline_batch_id: i64 =
        sqlx::query_scalar("SELECT max(id) FROM regional_observation_batches")
            .fetch_one(&pool)
            .await
            .expect("read the cap-two baseline batch boundary");
    sqlx::query("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES ($1,$2,$3,$4,now(),now(),now() - interval '1 second','awaiting_resolution')")
        .bind(FIRST_REGION)
        .bind(99_i64)
        .bind(serde_json::to_value(item_exchange_contract(99)).expect("serialize the dormant resolution contract"))
        .bind(serde_json::json!({"offered_items": [], "requested_items": []}))
        .execute(&pool)
        .await
        .expect("seed an otherwise due resolution probe before the shared limiter boundary");
    pool.close().await;

    let esi = Arc::new(InFlightLimiterEsi {
        first_region: FIRST_REGION,
        second_region: SECOND_REGION,
        third_region: THIRD_REGION,
        first_started: Arc::new(Notify::new()),
        second_started: Arc::new(Notify::new()),
        first_release: Arc::new(Notify::new()),
        second_release: Arc::new(Notify::new()),
        calls: StdMutex::new(Vec::new()),
    });
    let first_started = esi.first_started.notified();
    let second_started = esi.second_started.notified();
    let collector =
        ContractCollector::new(store.clone(), esi.clone()).with_max_concurrent_regions(2);
    let cycle = tokio::spawn(async move { collector.collect_cycle().await });

    tokio::time::timeout(Duration::from_secs(2), first_started)
        .await
        .expect("the first regional attempt starts");
    tokio::time::timeout(Duration::from_secs(2), second_started)
        .await
        .expect("the second regional attempt was already in flight before the limiter boundary");
    esi.first_release.notify_one();
    tokio::task::yield_now().await;
    assert!(
        !esi.calls.lock().unwrap().contains(&THIRD_REGION),
        "the limiter-hitting completed region must not open a third ESI attempt"
    );
    assert!(
        !cycle.is_finished(),
        "the already-started second region remains allowed to finish its own attempt"
    );
    esi.second_release.notify_one();
    let report = cycle
        .await
        .expect("join the cap-two limiter cycle")
        .expect("both already-started regions finish cleanly");
    assert!(report.retry_after.is_some());
    assert_eq!(
        report
            .regions
            .iter()
            .map(|outcome| match outcome {
                CollectionOutcome::BaselineEstablished { region_id }
                | CollectionOutcome::RecoveryBaselineEstablished { region_id }
                | CollectionOutcome::Complete { region_id, .. }
                | CollectionOutcome::Inconclusive { region_id, .. } => *region_id,
            })
            .collect::<Vec<_>>(),
        vec![FIRST_REGION, SECOND_REGION],
        "the report retains the two actual attempts and omits the unstarted third region"
    );
    assert!(
        !esi.calls.lock().unwrap().contains(&THIRD_REGION),
        "the persisted limiter also prevents a delayed third attempt"
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect retained cap-two batches");
    let batches = sqlx::query(
        "SELECT region_id, outcome FROM regional_observation_batches WHERE id > $1 ORDER BY id",
    )
    .bind(baseline_batch_id)
    .fetch_all(&pool)
    .await
    .expect("read the two actually started batches");
    assert_eq!(batches.len(), 2);
    let batch_outcomes = batches
        .iter()
        .map(|batch| {
            (
                batch.get::<i64, _>("region_id"),
                batch.get::<String, _>("outcome"),
            )
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(batch_outcomes[&FIRST_REGION], "complete");
    assert_eq!(batch_outcomes[&SECOND_REGION], "complete");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM contract_collection_failures WHERE region_id = $1 AND contract_id = $2 AND failure_kind = 'resolution_probe'",
        )
        .bind(FIRST_REGION)
        .bind(99_i64)
        .fetch_one(&pool)
        .await
        .expect("ensure the limiter did not manufacture a resolution failure"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM contract_resolution_cases WHERE region_id = $1 AND contract_id = $2",
        )
        .bind(FIRST_REGION)
        .bind(99_i64)
        .fetch_one(&pool)
        .await
        .expect("keep the unprobed regional case awaiting a future allowed cycle"),
        "awaiting_resolution"
    );
    pool.close().await;

    database.destroy().await;
}

#[tokio::test]
async fn a_material_collection_gap_reestablishes_a_silent_recovery_baseline() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let prior_contract = item_exchange_contract(44);
    let new_contract = item_exchange_contract(45);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![prior_contract], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship(1)],
                    cached_item_metadata("prior-items", 3_600),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the initial regional baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the listing subscription");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to make the regional state stale");
    sqlx::query(
        "UPDATE regional_observation_batches SET attempt_started_at = now() - interval '2 hours', completed_at = now() - interval '2 hours' WHERE region_id = 10000002",
    )
    .execute(&pool)
    .await
    .expect("simulate an outage-spanning collection gap");
    pool.close().await;

    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![new_contract], expiring_page(1))),
            )]),
            items: HashMap::from([(
                45,
                Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
            )]),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("recover a complete regional baseline");

    assert_eq!(
        report.regions,
        vec![CollectionOutcome::RecoveryBaselineEstablished {
            region_id: 10_000_002
        }]
    );
    assert!(report.events.is_empty());
    assert!(delivery.sent.lock().unwrap().is_empty());
    assert_eq!(
        store
            .region_contracts(10_000_002)
            .await
            .expect("read recovered contracts")
            .iter()
            .map(|contract| contract.contract_id)
            .collect::<Vec<_>>(),
        vec![44, 45]
    );

    database.destroy().await;
}

#[tokio::test]
async fn recovery_retains_stale_nonfinancial_closures_without_discord_delivery() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut expired = item_exchange_contract(44);
    expired.date_expired = Utc::now() - chrono::Duration::minutes(1);
    let mut unknown = item_exchange_contract(45);
    unknown.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![expired.clone(), unknown.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship(1)],
                        cached_item_metadata("expired-items", 3_600),
                    )),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the initial regional baseline");
    for (id, kind) in [
        ("expired", ContractEventKind::Expired),
        ("unknown", ContractEventKind::ClosedOutcomeUnknown),
    ] {
        store
            .upsert_contract_subscription(&terminal_ship_subscription(
                id,
                kind,
                ContractEventAction::Post,
            ))
            .await
            .expect("persist terminal-state subscription");
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to make the regional state stale");
    sqlx::query(
        "UPDATE regional_observation_batches SET attempt_started_at = now() - interval '2 hours', completed_at = now() - interval '2 hours' WHERE region_id = 10000002",
    )
    .execute(&pool)
    .await
    .expect("simulate an outage-spanning collection gap");
    pool.close().await;

    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                )]),
                items: HashMap::new(),
            },
            probes: StdMutex::new(vec![Ok(ContractItemProbe::NotFound(expiring_cache()))]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("retain stale terminal lifecycle facts during recovery");

    assert!(report.events.is_empty());
    assert!(delivery.sent.lock().unwrap().is_empty());
    assert_eq!(
        store
            .contract_resolution_records()
            .await
            .expect("read stale terminal lifecycle facts")
            .iter()
            .map(|record| record.state)
            .collect::<Vec<_>>(),
        vec![
            ContractResolutionState::Expired,
            ContractResolutionState::ClosedOutcomeUnknown,
        ]
    );

    database.destroy().await;
}

#[tokio::test]
async fn recovery_still_notifies_fresh_pre_expiry_acceptance_evidence() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the initial regional baseline");
    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "sales",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist sale confirmation subscription");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to make the regional state stale");
    sqlx::query(
        "UPDATE regional_observation_batches SET attempt_started_at = now() - interval '2 hours', completed_at = now() - interval '2 hours' WHERE region_id = 10000002",
    )
    .execute(&pool)
    .await
    .expect("simulate an outage-spanning collection gap");
    pool.close().await;

    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                )]),
                items: HashMap::new(),
            },
            probes: StdMutex::new(vec![Ok(ContractItemProbe::NoContent(expiring_cache()))]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("confirm fresh acceptance evidence after recovery");

    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![ContractEventKind::SaleConfirmed]
    );
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn cleaning_expired_successful_scan_diagnostics_preserves_contract_facts_and_failures() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("persist a contract fact and presence interval");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to seed retained diagnostics");
    sqlx::query("INSERT INTO regional_observations (region_id, observed_at, outcome, expected_pages) VALUES (100000002, now() - interval '91 days', 'complete', 1)")
        .execute(&pool)
        .await
        .expect("seed an expired successful scan diagnostic");
    sqlx::query("INSERT INTO regional_observations (region_id, observed_at, outcome, detail) VALUES (100000002, now() - interval '91 days', 'inconclusive', 'retain this failure diagnostic')")
        .execute(&pool)
        .await
        .expect("seed an inconclusive scan diagnostic");
    sqlx::query("INSERT INTO contract_collection_failures (region_id, observed_at, failure_kind, detail) VALUES (100000002, now() - interval '91 days', 'resolution_probe', 'retain this unresolved failure')")
        .execute(&pool)
        .await
        .expect("seed an unresolved collection failure");
    pool.close().await;

    assert_eq!(
        store
            .prune_successful_scan_diagnostics()
            .await
            .expect("clean only expired successful scan diagnostics"),
        1
    );
    assert_eq!(
        store
            .storage_counts()
            .await
            .expect("contract facts survive diagnostic cleanup"),
        killbot_rust::contract_intelligence::StorageCounts {
            contract_facts: 1,
            manifests: 1,
            presence_intervals: 1,
        }
    );
    let failures = store
        .unresolved_collection_failures()
        .await
        .expect("unresolved failures survive diagnostic cleanup");
    assert_eq!(failures.len(), 1);
    assert!(store
        .classify_collection_failure(failures[0].id, "operator-reviewed")
        .await
        .expect("classify a retained collection failure"));
    assert!(store
        .unresolved_collection_failures()
        .await
        .expect("classified failure is no longer unresolved")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn a_successful_regional_scan_resolves_its_prior_inconclusive_failure() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let failed = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Err(EsiError::retryable("temporary summary outage", None)),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("retain the incomplete regional observation");
    assert!(matches!(
        failed.regions.as_slice(),
        [CollectionOutcome::Inconclusive { .. }]
    ));
    assert_eq!(
        store
            .unresolved_collection_failures()
            .await
            .expect("read unresolved collection failure")
            .len(),
        1
    );

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![item_exchange_contract(44)],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("complete the regional scan after recovery");
    assert!(store
        .unresolved_collection_failures()
        .await
        .expect("the successful scan resolves the regional failure")
        .is_empty());

    database.destroy().await;
}

#[tokio::test]
async fn prepared_contract_deliveries_are_sent_by_ship_priority_then_isk_then_confirmation_time() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline_contract = item_exchange_contract(44);
    let baseline_ship = offered_ship(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![baseline_ship.clone()],
                    expiring_cache(),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![19_720, 587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist matching subscription");

    let mut titan_contract = item_exchange_contract(45);
    titan_contract.price = 1_000_000_000.0;
    let mut lower_value_frigate = item_exchange_contract(46);
    lower_value_frigate.price = 5_000_000_000.0;
    let mut higher_value_frigate = item_exchange_contract(47);
    higher_value_frigate.price = 10_000_000_000.0;
    let mut older_equal_value_frigate = item_exchange_contract(48);
    older_equal_value_frigate.price = 10_000_000_000.0;
    older_equal_value_frigate.date_issued -= chrono::Duration::hours(1);
    let titan_ship = PublicContractItem {
        type_id: 19_720,
        ..offered_ship(2)
    };
    let lower_value_frigate_ship = offered_ship(3);
    let higher_value_frigate_ship = offered_ship(4);
    let older_equal_value_frigate_ship = offered_ship(5);
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![
                        baseline_contract,
                        lower_value_frigate,
                        titan_contract,
                        higher_value_frigate,
                        older_equal_value_frigate,
                    ],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![baseline_ship], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![titan_ship], expiring_cache())),
                ),
                (
                    46,
                    Ok(EsiResponse::fresh(
                        vec![lower_value_frigate_ship],
                        expiring_cache(),
                    )),
                ),
                (
                    47,
                    Ok(EsiResponse::fresh(
                        vec![higher_value_frigate_ship],
                        expiring_cache(),
                    )),
                ),
                (
                    48,
                    Ok(EsiResponse::fresh(
                        vec![older_equal_value_frigate_ship],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(19_720, 30), (587, 25)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("deliver sorted contract notifications");

    assert_eq!(
        delivery
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|delivery| delivery.contract_id)
            .collect::<Vec<_>>(),
        vec![45, 48, 47, 46]
    );

    database.destroy().await;
}

#[tokio::test]
async fn contract_runtime_recovers_the_shared_store_after_postgres_becomes_available() {
    let database = TemporaryDatabase::unavailable().await;
    let store_handle = new_contract_store_handle();
    let collection_task = spawn_contract_collection_loop_with_notifications(
        database.url.clone(),
        store_handle.clone(),
        Duration::from_millis(10),
        Duration::from_millis(10),
        Arc::new(StaticShipGroups(HashMap::new())),
        Arc::new(NoopDelivery),
        Arc::new(RecordingContractPingLimiter {
            outcomes: StdMutex::new(Vec::new()),
            channels: StdMutex::new(Vec::new()),
        }),
    );

    let (processing_started, processing_complete) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = processing_started.send(());
    });
    tokio::time::timeout(Duration::from_millis(50), processing_complete)
        .await
        .expect("the existing runtime can start processing while PostgreSQL is unavailable")
        .expect("the existing runtime completes its work");
    assert!(available_contract_store(&store_handle).await.is_none());

    database.create().await;
    let store = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(store) = available_contract_store(&store_handle).await {
                return store;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background contract runtime reconnects after PostgreSQL recovery");

    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("recovered commands can persist a contract subscription");
    let report = ContractCollector::new(
        (*store).clone(),
        Arc::new(FakeEsi {
            regions: vec![],
            ..FakeEsi::default()
        }),
    )
    .collect_cycle()
    .await
    .expect("recovered collector can use the shared store");
    assert!(report.regions.is_empty());

    collection_task.abort();
    assert!(collection_task
        .await
        .expect_err("background contract runtime is cancelled")
        .is_cancelled());
    database.destroy().await;
}

#[tokio::test]
async fn recursive_contract_filters_match_every_public_fact_once_per_contract() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let mut listed = item_exchange_contract(45);
    listed.for_corporation = false;
    listed.issuer_id = 90_000_111;
    listed.issuer_corporation_id = 98_000_111;
    listed.price = 1_500_000_000.0;
    listed.reward = 2_500_000_000.0;
    listed.title = Some("HEL exchange in Delve".to_string());

    let offered_hel = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let requested_titan = PublicContractItem {
        record_id: 2,
        type_id: 19_720,
        quantity: 1,
        is_included: false,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_hel.clone()],
                    expiring_cache(),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    let conditions = vec![
        ContractFilterCondition::EventKinds(vec![ContractEventKind::Listed]),
        ContractFilterCondition::OfferedItems,
        ContractFilterCondition::RequestedItems,
        ContractFilterCondition::ItemTypes {
            direction: ContractItemDirection::Offered,
            ids: vec![587],
        },
        ContractFilterCondition::ShipGroups {
            direction: ContractItemDirection::Offered,
            ids: vec![659],
        },
        ContractFilterCondition::MinimumIsk {
            direction: ContractItemDirection::Requested,
            value: 1_000_000_000.0,
        },
        ContractFilterCondition::MaximumIsk {
            direction: ContractItemDirection::Requested,
            value: 2_000_000_000.0,
        },
        ContractFilterCondition::MinimumIsk {
            direction: ContractItemDirection::Offered,
            value: 2_000_000_000.0,
        },
        ContractFilterCondition::MaximumIsk {
            direction: ContractItemDirection::Offered,
            value: 3_000_000_000.0,
        },
        ContractFilterCondition::Regions(vec![10_000_002]),
        ContractFilterCondition::SolarSystems(vec![30_000_142]),
        ContractFilterCondition::SecurityRange {
            min: -0.6,
            max: -0.4,
        },
        ContractFilterCondition::LocationIds(vec![60_003_760]),
        ContractFilterCondition::IssuerCharacters(vec![90_000_111]),
        ContractFilterCondition::IssuerCorporations(vec![98_000_111]),
        ContractFilterCondition::ObservedAffiliationAlliances(vec![99_000_111]),
        ContractFilterCondition::PersonalIssuance,
        ContractFilterCondition::TitleFragment("hEl ExChAnGe".to_string()),
        ContractFilterCondition::TitleFragment("this does not match".to_string()),
    ];
    for (index, condition) in conditions.into_iter().enumerate() {
        let root = if index + 1 == 19 {
            ContractFilterNode::And(vec![
                ContractFilterNode::Condition(ContractFilterCondition::EventKinds(vec![
                    ContractEventKind::Listed,
                ])),
                ContractFilterNode::Or(vec![
                    ContractFilterNode::Condition(condition),
                    ContractFilterNode::Not(Box::new(ContractFilterNode::Condition(
                        ContractFilterCondition::IssuerCharacters(vec![1]),
                    ))),
                ]),
            ])
        } else {
            ContractFilterNode::Condition(condition)
        };
        store
            .upsert_contract_subscription(&ContractSubscription {
                guild_id: 42,
                channel_id: 77,
                id: format!("condition-{index}"),
                description: format!("condition {index}"),
                filter: ContractFilter { root },
                event_actions: ContractEventActions {
                    listed: ContractEventAction::Post,
                    ..ContractEventActions::default()
                },
            })
            .await
            .expect("persist a recursive contract filter");
    }

    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(
        store.clone(),
        Arc::new(ContextualFakeEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
                )]),
                items: HashMap::from([
                    (
                        44,
                        Ok(EsiResponse::fresh(
                            vec![offered_hel.clone()],
                            expiring_cache(),
                        )),
                    ),
                    (
                        45,
                        Ok(EsiResponse::fresh(
                            vec![offered_hel, requested_titan],
                            expiring_cache(),
                        )),
                    ),
                ]),
            },
            context: ContractObservationContext {
                solar_system_id: Some(30_000_142),
                security_status: Some(-0.5),
                observed_affiliation_alliance_id: Some(99_000_111),
                ..ContractObservationContext::default()
            },
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659), (19_720, 30)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("evaluate every recursive filter through the collection seam");

    assert_eq!(
        report.events.len(),
        1,
        "one event for the multi-ship bundle"
    );
    assert_eq!(delivery.sent.lock().unwrap().len(), 19);
    let sent = delivery.sent.lock().unwrap();
    let matching_ship_deliveries: Vec<_> = sent
        .iter()
        .filter(|delivery| {
            matches!(
                delivery.subscription_id.as_str(),
                "condition-3" | "condition-4"
            )
        })
        .collect();
    assert_eq!(matching_ship_deliveries.len(), 2);
    assert!(matching_ship_deliveries
        .iter()
        .all(|delivery| delivery.message.title.contains("587")));

    database.destroy().await;
}

#[tokio::test]
async fn unknown_location_does_not_make_a_negated_context_filter_eligible() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let mut listed = item_exchange_contract(45);
    listed.for_corporation = true;
    let offered_ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "corporation-unknown-location".to_string(),
            description: "corporation-issued contracts outside one system".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::And(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::CorporationIssuance),
                    ContractFilterNode::Not(Box::new(ContractFilterNode::Condition(
                        ContractFilterCondition::SolarSystems(vec![30_000_142]),
                    ))),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the context-sensitive corporation filter");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "corporation-only".to_string(),
            description: "all corporation-issued contracts".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Condition(ContractFilterCondition::CorporationIssuance),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the corporation issuance filter");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(ContextualFakeEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
                )]),
                items: HashMap::from([
                    (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                    (
                        45,
                        Ok(EsiResponse::fresh(vec![offered_ship], expiring_cache())),
                    ),
                ]),
            },
            context: ContractObservationContext::default(),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("evaluate the unresolved context filter");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(
        sent.len(),
        1,
        "the plain corporation condition should match"
    );
    assert_eq!(sent[0].subscription_id, "corporation-only");
    assert!(
        sent.iter()
            .all(|delivery| delivery.subscription_id != "corporation-unknown-location"),
        "an unknown location must not satisfy Not(SolarSystems)"
    );

    database.destroy().await;
}

#[tokio::test]
async fn local_filter_mismatches_do_not_trigger_contract_context_enrichment() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "no-context-for-local-miss".to_string(),
            description: "never matches this title".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::And(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::TitleFragment(
                        "not present in the title".to_string(),
                    )),
                    ContractFilterNode::Condition(ContractFilterCondition::SolarSystems(vec![
                        30_000_142,
                    ])),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the locally rejecting context filter");
    let esi = Arc::new(CountingContextEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (45, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
            ]),
        },
        context_calls: StdMutex::new(0),
    });

    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::new())),
            Arc::new(NoopDelivery),
        )
        .collect_cycle()
        .await
        .expect("evaluate the locally rejecting filter");

    assert_eq!(*esi.context_calls.lock().unwrap(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_definite_ship_group_mismatch_skips_later_context_enrichment() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let offered_hel = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "titan-in-one-system".to_string(),
            description: "titans in one system".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::And(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::ShipGroups {
                        direction: ContractItemDirection::Offered,
                        ids: vec![30],
                    }),
                    ContractFilterNode::Condition(ContractFilterCondition::SolarSystems(vec![
                        30_000_142,
                    ])),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the staged ship and location filter");
    let esi = Arc::new(CountingContextEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![offered_hel], expiring_cache())),
                ),
            ]),
        },
        context_calls: StdMutex::new(0),
    });

    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::from([(587, 659)]))),
            Arc::new(NoopDelivery),
        )
        .collect_cycle()
        .await
        .expect("reject the non-Titan before resolving location context");

    assert_eq!(*esi.context_calls.lock().unwrap(), 0);

    database.destroy().await;
}

#[tokio::test]
async fn a_local_or_match_skips_unneeded_context_enrichment() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let mut listed = item_exchange_contract(45);
    listed.title = Some("Immediate Hel sale".to_string());

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "title-or-system".to_string(),
            description: "matching title or one system".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Or(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::TitleFragment(
                        "hel sale".to_string(),
                    )),
                    ContractFilterNode::Condition(ContractFilterCondition::SolarSystems(vec![
                        30_000_142,
                    ])),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the local-or-context filter");
    let esi = Arc::new(CountingContextEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (45, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
            ]),
        },
        context_calls: StdMutex::new(0),
    });
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
        .collect_cycle()
        .await
        .expect("match the local Or branch without resolving the unused location branch");

    assert_eq!(*esi.context_calls.lock().unwrap(), 0);
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);

    database.destroy().await;
}

#[tokio::test]
async fn a_deferred_or_candidate_context_match_selects_the_higher_priority_ship() {
    let (first_context_calls, first_deliveries, titles) =
        resolve_candidate_branch_after_unknown_location(30_000_142).await;

    assert_eq!(first_context_calls, 1);
    assert_eq!(first_deliveries, 0);
    assert_eq!(titles, vec!["Type 19720 listed for 1.5B"]);
}

#[tokio::test]
async fn a_deferred_or_candidate_context_miss_keeps_the_local_primary_ship() {
    let (first_context_calls, first_deliveries, titles) =
        resolve_candidate_branch_after_unknown_location(30_000_143).await;

    assert_eq!(first_context_calls, 1);
    assert_eq!(first_deliveries, 0);
    assert_eq!(titles, vec!["Type 587 listed for 1.5B"]);
}

#[tokio::test]
async fn a_context_rate_boundary_stops_later_context_requests_for_the_same_event() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "all-context-facts".to_string(),
            description: "requires every remotely resolved context fact".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::And(vec![
                    ContractFilterNode::Condition(ContractFilterCondition::SolarSystems(vec![
                        30_000_142,
                    ])),
                    ContractFilterNode::Condition(ContractFilterCondition::SecurityRange {
                        min: -0.6,
                        max: -0.4,
                    }),
                    ContractFilterNode::Condition(
                        ContractFilterCondition::ObservedAffiliationAlliances(vec![99_000_111]),
                    ),
                ]),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the all-context filter");
    let esi = Arc::new(StopsBetweenContextCallsEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (45, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
            ]),
        },
        calls: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(store.clone(), esi.clone())
        .with_notifications(
            Arc::new(StaticShipGroups(HashMap::new())),
            Arc::new(NoopDelivery),
        )
        .collect_cycle()
        .await
        .expect("defer context-dependent notification after the limiter boundary");

    assert_eq!(
        *esi.calls.lock().unwrap(),
        vec!["solar-system"],
        "the persisted limiter must be checked before every later context request"
    );

    database.destroy().await;
}

#[tokio::test]
async fn contract_notifications_render_only_filter_matched_ship_items() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let mut listed = item_exchange_contract(45);
    listed.reward = 2_500_000_000.0;
    let offered_ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let requested_ship = PublicContractItem {
        record_id: 2,
        type_id: 19_720,
        quantity: 1,
        is_included: false,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![offered_ship.clone()],
                    expiring_cache(),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    for (id, root) in [
        (
            "requested-branch",
            ContractFilterNode::Or(vec![
                ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                    direction: ContractItemDirection::Offered,
                    ids: vec![999_999],
                }),
                ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                    direction: ContractItemDirection::Requested,
                    ids: vec![19_720],
                }),
            ]),
        ),
        (
            "offered-isk",
            ContractFilterNode::Condition(ContractFilterCondition::MinimumIsk {
                direction: ContractItemDirection::Offered,
                value: 2_000_000_000.0,
            }),
        ),
    ] {
        store
            .upsert_contract_subscription(&ContractSubscription {
                guild_id: 42,
                channel_id: 77,
                id: id.to_string(),
                description: id.to_string(),
                filter: ContractFilter { root },
                event_actions: ContractEventActions {
                    listed: ContractEventAction::Post,
                    ..ContractEventActions::default()
                },
            })
            .await
            .expect("persist a directional contract filter");
    }
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship.clone()],
                        expiring_cache(),
                    )),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![offered_ship, requested_ship],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659), (19_720, 30)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("render the complete two-sided contract facts");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    let requested = sent
        .iter()
        .find(|delivery| delivery.subscription_id == "requested-branch")
        .expect("requested-item delivery");
    assert_eq!(requested.message.title, "Type 19720 listed for 2.5B");
    assert!(requested
        .message
        .description
        .as_deref()
        .is_some_and(|description| description
            .starts_with("`<url=\"contract:0//45\">Type 19720 - Location 60003760</url>`")
            && !description.contains("Type 587")));
    let isk_only = sent
        .iter()
        .find(|delivery| delivery.subscription_id == "offered-isk")
        .expect("ISK-only delivery");
    assert_eq!(isk_only.message.title, "Public contract listed for 2.5B");
    assert!(isk_only
        .message
        .description
        .as_deref()
        .is_some_and(|description| description
            .starts_with("`<url=\"contract:0//45\">Public contract - Location 60003760</url>`")
            && !description.contains("Type 587")
            && !description.contains("Type 19720")));
    assert!(sent
        .iter()
        .all(|delivery| delivery.message.fields.iter().all(|field| {
            !matches!(
                field.name.as_str(),
                "Offered" | "Requested" | "Requested ISK" | "Offered ISK"
            )
        })));
    drop(sent);

    database.destroy().await;
}

#[tokio::test]
async fn primary_display_ship_is_selected_only_from_matching_ship_items() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let matching_ship = PublicContractItem {
        record_id: 2,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let unrelated_nonship = PublicContractItem {
        record_id: 1,
        type_id: 34,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(
                    vec![matching_ship.clone()],
                    expiring_cache(),
                )),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "only-the-ship",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist a matching ship filter");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(
                        vec![matching_ship.clone()],
                        expiring_cache(),
                    )),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![unrelated_nonship, matching_ship],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 27)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("select the matching subcapital ship for display");

    assert_eq!(
        delivery.sent.lock().unwrap()[0].message.title,
        "Type 587 listed for 1.5B"
    );

    database.destroy().await;
}

#[tokio::test]
async fn primary_display_ship_uses_all_matching_or_branches_independent_of_branch_order() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let offered_hel = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let offered_titan = PublicContractItem {
        record_id: 2,
        type_id: 19_720,
        quantity: 1,
        is_included: true,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    for (id, type_ids) in [("hel-first", [587, 19_720]), ("titan-first", [19_720, 587])] {
        store
            .upsert_contract_subscription(&ContractSubscription {
                guild_id: 42,
                channel_id: 77,
                id: id.to_string(),
                description: id.to_string(),
                filter: ContractFilter {
                    root: ContractFilterNode::Or(
                        type_ids
                            .into_iter()
                            .map(|type_id| {
                                ContractFilterNode::Condition(ContractFilterCondition::ItemTypes {
                                    direction: ContractItemDirection::Offered,
                                    ids: vec![type_id],
                                })
                            })
                            .collect(),
                    ),
                },
                event_actions: ContractEventActions {
                    listed: ContractEventAction::Post,
                    ..ContractEventActions::default()
                },
            })
            .await
            .expect("persist a reversed ship branch filter");
    }
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![offered_hel, offered_titan],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659), (19_720, 30)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("select a primary from every matching positive Or branch");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent
        .iter()
        .all(|delivery| delivery.message.title == "Type 19720 listed for 1.5B"));
    drop(sent);

    database.destroy().await;
}

#[tokio::test]
async fn primary_display_ship_uses_all_group_matches_independent_of_manifest_order() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let first_listed = item_exchange_contract(45);
    let second_listed = item_exchange_contract(46);
    let offered_hel = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let offered_titan = PublicContractItem {
        record_id: 2,
        type_id: 19_720,
        quantity: 1,
        is_included: true,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");

    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "strategic-groups".to_string(),
            description: "supers and titans".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Condition(ContractFilterCondition::ShipGroups {
                    direction: ContractItemDirection::Offered,
                    ids: vec![659, 30],
                }),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the multi-group filter");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline, first_listed, second_listed],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![offered_hel.clone(), offered_titan.clone()],
                        expiring_cache(),
                    )),
                ),
                (
                    46,
                    Ok(EsiResponse::fresh(
                        vec![offered_titan, offered_hel],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::from([(587, 659), (19_720, 30)]))),
        delivery.clone(),
    )
    .collect_cycle()
    .await
    .expect("select the strategic primary from every matching group item");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent
        .iter()
        .all(|delivery| delivery.message.title == "Type 19720 listed for 1.5B"));
    drop(sent);

    database.destroy().await;
}

#[tokio::test]
async fn a_transient_additional_group_candidate_defers_primary_selection() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let offered_hel = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };
    let offered_titan = PublicContractItem {
        record_id: 2,
        type_id: 19_720,
        quantity: 1,
        is_included: true,
        item_id: Some(1_002),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(44, Ok(EsiResponse::fresh(vec![], expiring_cache())))]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&ContractSubscription {
            guild_id: 42,
            channel_id: 77,
            id: "complete-group-candidates".to_string(),
            description: "supers and titans".to_string(),
            filter: ContractFilter {
                root: ContractFilterNode::Condition(ContractFilterCondition::ShipGroups {
                    direction: ContractItemDirection::Offered,
                    ids: vec![659, 30],
                }),
            },
            event_actions: ContractEventActions {
                listed: ContractEventAction::Post,
                ..ContractEventActions::default()
            },
        })
        .await
        .expect("persist the multi-group filter");
    let groups = Arc::new(EventuallyResolvedShipGroup {
        results: StdMutex::new(vec![
            ShipGroupLookup::Resolved(Some(659)),
            ShipGroupLookup::TemporarilyUnavailable,
            ShipGroupLookup::Resolved(Some(659)),
            ShipGroupLookup::Resolved(Some(30)),
        ]),
    });
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline.clone(), listed.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![offered_hel.clone(), offered_titan.clone()],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(groups.clone(), delivery.clone())
    .collect_cycle()
    .await
    .expect("defer while an additional group candidate is unresolved");
    assert!(delivery.sent.lock().unwrap().is_empty());

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (44, Ok(EsiResponse::fresh(vec![], expiring_cache()))),
                (
                    45,
                    Ok(EsiResponse::fresh(
                        vec![offered_hel, offered_titan],
                        expiring_cache(),
                    )),
                ),
            ]),
        }),
    )
    .with_notifications(groups, delivery.clone())
    .collect_cycle()
    .await
    .expect("resolve every group candidate before choosing the primary");

    let sent = delivery.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].message.title, "Type 19720 listed for 1.5B");
    drop(sent);

    database.destroy().await;
}

#[tokio::test]
async fn a_transient_primary_ship_lookup_defers_instead_of_committing_a_fallback_title() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let baseline = item_exchange_contract(44);
    let listed = item_exchange_contract(45);
    let ship = PublicContractItem {
        record_id: 1,
        type_id: 587,
        quantity: 1,
        is_included: true,
        item_id: Some(1_001),
        raw_quantity: None,
        is_singleton: None,
        is_blueprint_copy: None,
        material_efficiency: None,
        runs: None,
        time_efficiency: None,
    };

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                44,
                Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the silent baseline");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ship-title-after-lookup",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist the matching ship filter");
    let groups = Arc::new(EventuallyResolvedShipGroup {
        results: StdMutex::new(vec![
            ShipGroupLookup::TemporarilyUnavailable,
            ShipGroupLookup::Resolved(Some(659)),
        ]),
    });
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![baseline.clone(), listed.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
                ),
                (
                    45,
                    Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
                ),
            ]),
        }),
    )
    .with_notifications(groups.clone(), delivery.clone())
    .collect_cycle()
    .await
    .expect("defer while the primary ship lookup is unavailable");
    assert!(delivery.sent.lock().unwrap().is_empty());

    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![baseline, listed], expiring_page(1))),
            )]),
            items: HashMap::from([
                (
                    44,
                    Ok(EsiResponse::fresh(vec![ship.clone()], expiring_cache())),
                ),
                (45, Ok(EsiResponse::fresh(vec![ship], expiring_cache()))),
            ]),
        }),
    )
    .with_notifications(groups, delivery.clone())
    .collect_cycle()
    .await
    .expect("retry the deferred primary ship lookup");
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert_eq!(
        delivery.sent.lock().unwrap()[0].message.title,
        "Type 587 listed for 1.5B"
    );

    database.destroy().await;
}

#[test]
fn contract_filter_validation_rejects_invalid_trees_ranges_money_and_identifiers() {
    for root in [
        ContractFilterNode::And(vec![]),
        ContractFilterNode::Condition(ContractFilterCondition::SecurityRange {
            min: 0.5,
            max: -0.5,
        }),
        ContractFilterNode::Condition(ContractFilterCondition::MinimumIsk {
            direction: ContractItemDirection::Requested,
            value: f64::NAN,
        }),
        ContractFilterNode::Condition(ContractFilterCondition::LocationIds(vec![0])),
        ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
            config::SystemRange {
                system_id: 30_002_086,
                range: f64::NAN,
            },
        ])),
    ] {
        assert!(ContractFilter { root }.validate().is_err());
    }
    assert_eq!(
        ContractFilter {
            root: ContractFilterNode::Condition(ContractFilterCondition::SolarSystems(vec![
                30_000_142,
            ])),
        }
        .context_requirements(),
        ContractContextRequirements {
            solar_system: true,
            security_status: false,
            observed_affiliation: false,
            ly_ranges: vec![],
        }
    );
}

#[test]
fn light_year_range_filters_use_a_durable_backwards_compatible_serde_grammar() {
    let legacy: ContractFilter =
        serde_json::from_str(r#"{"root":{"condition":{"event_kinds":["listed"]}}}"#)
            .expect("existing persisted subscriptions remain readable");
    assert!(matches!(
        legacy.root,
        ContractFilterNode::Condition(ContractFilterCondition::EventKinds(_))
    ));
    let filter = ContractFilter {
        root: ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(vec![
            config::SystemRange {
                system_id: 30_002_086,
                range: 8.0,
            },
        ])),
    };
    assert_eq!(
        serde_json::to_value(&filter).expect("serialize the range filter"),
        serde_json::json!({
            "root": {
                "condition": {
                    "ly_range_from": [{"system_id": 30002086, "range": 8.0}]
                }
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<ContractFilter>(
            serde_json::to_value(&filter).expect("round-trip range filter"),
        )
        .expect("deserialize range filter"),
        filter
    );
}

#[tokio::test]
async fn invalid_contract_filter_cannot_partially_replace_a_persisted_subscription() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let original = contract_subscription(
        "ships",
        ContractItemDirection::Offered,
        vec![587],
        vec![],
        ContractEventAction::Post,
    );
    store
        .upsert_contract_subscription(&original)
        .await
        .expect("persist the original subscription");
    let invalid_replacement = ContractSubscription {
        description: "invalid replacement".to_string(),
        filter: ContractFilter {
            root: ContractFilterNode::Or(vec![]),
        },
        ..original.clone()
    };
    assert!(store
        .upsert_contract_subscription(&invalid_replacement)
        .await
        .is_err());
    assert_eq!(
        store
            .contract_subscriptions_for_channel(42, 77)
            .await
            .expect("read the unchanged subscription"),
        vec![original]
    );
    database.destroy().await;
}

#[tokio::test]
async fn http_esi_resolves_context_facts_independently_for_filters() {
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"{"system_id":30000142}"#,
        },
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"{"security_status":-0.5}"#,
        },
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"{"position":{"x":9460730472580800.0,"y":0.0,"z":0.0}}"#,
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct local HTTP ESI client");
    let contract = item_exchange_contract(44);
    let solar_system = esi
        .observed_contract_solar_system(&contract)
        .await
        .expect("resolve the observed solar system");
    assert_eq!(
        solar_system.value.expect("solar system response"),
        ContractContextValue::Resolved(30_000_142)
    );
    let security = esi
        .observed_solar_system_security(&contract, 30_000_142)
        .await
        .expect("resolve the observed security status");
    assert_eq!(
        security.value.expect("security response"),
        ContractContextValue::Resolved(-0.5)
    );
    let affiliation = esi
        .observed_issuer_affiliation(&contract)
        .await
        .expect("resolve the observed affiliation");
    assert_eq!(
        affiliation.value.expect("affiliation response"),
        ContractContextValue::Resolved(99_000_111)
    );
    assert_eq!(
        esi.solar_system_position(30_000_142, None)
            .await
            .expect("resolve the solar-system position")
            .value
            .expect("solar-system position response"),
        position_at_light_years(1.0)
    );
    server.finish();
}

#[tokio::test]
async fn http_esi_snapshots_public_embed_context_from_public_endpoints() {
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"Jita IV - Moon 4","system_id":30000142}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"Jita","security_status":0.9,"constellation_id":20000020}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"region_id":10000002}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"The Forge"}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"},{"id":587,"name":"Rifter"}]"#,
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct local HTTP ESI client");
    let context = esi
        .observed_contract_embed_context(&item_exchange_contract(44), &[offered_ship(1)])
        .await
        .expect("resolve public embed context")
        .value
        .expect("context response");

    assert_eq!(
        context.location.location_name.as_deref(),
        Some("Jita IV - Moon 4")
    );
    assert_eq!(context.location.location_kind.as_deref(), Some("Station"));
    assert_eq!(context.location.solar_system_name.as_deref(), Some("Jita"));
    assert_eq!(context.location.region_name.as_deref(), Some("The Forge"));
    assert_eq!(context.issuer_character_name.as_deref(), Some("Issuer"));
    assert_eq!(
        context.issuer_corporation_name.as_deref(),
        Some("Issuer Corp")
    );
    assert_eq!(
        context.issuer_alliance_name.as_deref(),
        Some("Issuer Alliance")
    );
    assert_eq!(
        context.item_names.get(&587).map(String::as_str),
        Some("Rifter")
    );
    let shared_context = esi
        .observed_contract_embed_context(&item_exchange_contract(45), &[offered_ship(2)])
        .await
        .expect("reuse fresh public context resources across observed contracts")
        .value
        .expect("shared context response");
    assert_eq!(
        shared_context.issuer_character_name.as_deref(),
        Some("Issuer")
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        6,
        "fresh entity context is shared across contracts"
    );
    assert!(requests[0].starts_with("GET /universe/stations/60003760/"));
    assert!(requests[1].starts_with("GET /universe/systems/30000142/"));
    assert!(requests[4].starts_with("POST /characters/affiliation/"));
    assert!(requests[5].starts_with("POST /universe/names/"));
    drop(requests);
    server.finish();
}

#[tokio::test]
async fn http_esi_revalidates_expired_context_with_an_etag() {
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("ETag", "system-v1"), ("Cache-Control", "max-age=0")],
            body: r#"{"name":"Jita","security_status":0.9,"constellation_id":20000020}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"region_id":10000002}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"The Forge"}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"},{"id":587,"name":"Rifter"}]"#,
        },
        WireReply {
            status: 304,
            headers: vec![("Cache-Control", "max-age=60")],
            body: "",
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct local HTTP ESI client");
    let mut contract = item_exchange_contract(44);
    contract.start_location_id = 30_000_142;

    esi.observed_contract_embed_context(&contract, &[offered_ship(1)])
        .await
        .expect("cache the initial public context");
    let context = esi
        .observed_contract_embed_context(&contract, &[offered_ship(1)])
        .await
        .expect("reuse the 304 representation")
        .value
        .expect("revalidated context response");

    assert_eq!(context.location.solar_system_name.as_deref(), Some("Jita"));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests[5]
        .to_ascii_lowercase()
        .contains("if-none-match: system-v1"));
    drop(requests);
    server.finish();
}

#[tokio::test]
async fn collection_cycles_retain_context_representations_and_etags_after_a_late_failure() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let contract = item_exchange_contract(44);
    let page = r#"[{"collateral":0.0,"contract_id":44,"date_expired":"2026-08-14T12:00:00Z","date_issued":"2026-08-13T12:00:00Z","days_to_complete":0,"issuer_corporation_id":98000001,"issuer_id":90000001,"price":1500000000.0,"reward":0.0,"start_location_id":60003760,"type":"item_exchange","volume":115000.0}]"#;
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: "[10000002]",
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60"), ("X-Pages", "1")],
            body: page,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"is_included":true,"is_singleton":true,"quantity":1,"record_id":1,"type_id":587}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![("ETag", "station-v1"), ("Cache-Control", "max-age=0")],
            body: r#"{"name":"Jita IV - Moon 4","system_id":30000142}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"Jita","security_status":0.9,"constellation_id":20000020}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"region_id":10000002}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"{"name":"The Forge"}"#,
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 500,
            headers: vec![],
            body: "{}",
        },
        WireReply {
            status: 304,
            headers: vec![("Cache-Control", "max-age=60")],
            body: "",
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"},{"id":587,"name":"Rifter"}]"#,
        },
    ]);
    let esi = Arc::new(
        HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct local HTTP ESI client"),
    );
    let collector = ContractCollector::new(store.clone(), esi);

    collector
        .collect_cycle()
        .await
        .expect("retain the complete observation after late context failure");
    assert!(store
        .observed_embed_context(10_000_002, contract.contract_id)
        .await
        .expect("read absent context after late failure")
        .is_none());
    collector
        .collect_cycle()
        .await
        .expect("retry context enrichment using retained representations");

    assert_eq!(
        store
            .observed_embed_context(10_000_002, contract.contract_id)
            .await
            .expect("read context after cross-cycle retry")
            .and_then(|context| context.location.solar_system_name),
        Some("Jita".to_string())
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 11);
    assert!(requests[3].starts_with("GET /universe/stations/60003760/"));
    assert!(requests[9].starts_with("GET /universe/stations/60003760/"));
    assert!(requests[9]
        .to_ascii_lowercase()
        .contains("if-none-match: station-v1"));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("GET /universe/systems/30000142/"))
            .count(),
        1
    );
    drop(requests);
    server.finish();
    database.destroy().await;
}

#[tokio::test]
async fn http_esi_stops_an_enrichment_chain_at_a_persisted_limiter_boundary() {
    let server = SequenceHttpServer::start(vec![WireReply {
        status: 200,
        headers: vec![
            ("X-RateLimit-Limit", "1/1m"),
            ("X-RateLimit-Remaining", "0"),
        ],
        body: r#"{"name":"Jita IV - Moon 4","system_id":30000142}"#,
    }]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct local HTTP ESI client");
    let limiter = BoundaryContextLimiter::default();

    let error = esi
        .observed_contract_embed_context_limited(
            &item_exchange_contract(44),
            &[offered_ship(1)],
            &limiter,
        )
        .await
        .expect_err("stop before the next context request after the recorded boundary");

    assert!(error.to_string().contains("limiter boundary"));
    assert_eq!(*limiter.responses.lock().unwrap(), 1);
    assert_eq!(*limiter.request_checks.lock().unwrap(), 2);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    drop(requests);
    server.finish();
}

#[tokio::test]
async fn http_esi_skips_scoped_structure_resolution_for_public_contracts() {
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"},{"id":587,"name":"Rifter"}]"#,
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct unauthenticated public ESI client");
    let mut contract = item_exchange_contract(44);
    contract.start_location_id = 1_035_466_617_946;

    let context = esi
        .observed_contract_embed_context(&contract, &[offered_ship(1)])
        .await
        .expect("an inaccessible structure does not abort public enrichment")
        .value
        .expect("context response");

    assert!(context.location.location_name.is_none());
    assert!(context.location.solar_system_id.is_none());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("POST /characters/affiliation/"));
    assert!(requests[1].starts_with("POST /universe/names/"));
    assert!(requests
        .iter()
        .all(|request| !request.contains("/universe/structures/")));
    drop(requests);
    server.finish();
}

#[tokio::test]
async fn http_esi_continues_after_an_inaccessible_station_lookup() {
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 401,
            headers: vec![],
            body: "{}",
        },
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"[{"alliance_id":99000111}]"#,
        },
        WireReply {
            status: 200,
            headers: vec![],
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"},{"id":587,"name":"Rifter"}]"#,
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct unauthenticated public ESI client");

    let context = esi
        .observed_contract_embed_context(&item_exchange_contract(44), &[offered_ship(1)])
        .await
        .expect("an inaccessible location lookup does not abort issuer enrichment")
        .value
        .expect("partial public context");

    assert!(context.location.location_name.is_none());
    assert_eq!(context.issuer_character_name.as_deref(), Some("Issuer"));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("GET /universe/stations/60003760/"));
    assert!(requests[1].starts_with("POST /characters/affiliation/"));
    drop(requests);
    server.finish();
}

#[tokio::test]
async fn http_esi_station_not_found_keeps_location_context_indeterminate() {
    let server = SequenceHttpServer::start(vec![WireReply {
        status: 404,
        headers: vec![],
        body: "{}",
    }]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct local HTTP ESI client");

    let response = esi
        .observed_contract_solar_system(&item_exchange_contract(44))
        .await
        .expect("a missing station is not an affirmative location result");

    assert_eq!(
        response.value.expect("indeterminate location response"),
        ContractContextValue::Indeterminate
    );
    server.finish();
}

#[test]
fn health_configuration_rejects_malformed_zero_and_misordered_thresholds() {
    for (name, value) in [
        ("HEALTH_EVALUATION_INTERVAL_SECS", "zero"),
        ("HEALTH_HEARTBEAT_DEGRADED_SECS", "0"),
        ("HEALTH_POSTGRES_CRITICAL_FAILURES", "0"),
    ] {
        assert!(
            HealthRuntimeConfig::from_settings(&HashMap::from([(
                name.to_string(),
                value.to_string()
            )]))
            .is_err(),
            "{name}={value} is rejected rather than silently defaulted"
        );
    }
    assert!(
        HealthRuntimeConfig::from_settings(&HashMap::from([
            (
                "HEALTH_ESI_PROGRESS_DEGRADED_SECS".to_string(),
                "900".to_string()
            ),
            (
                "HEALTH_ESI_PROGRESS_CRITICAL_SECS".to_string(),
                "900".to_string()
            ),
        ]))
        .is_err(),
        "equal degraded and critical thresholds are rejected"
    );
    assert!(
        HealthRuntimeConfig::from_settings(&HashMap::from([(
            "HEALTH_HEARTBEAT_DEGRADED_SECS".to_string(),
            i64::MAX.to_string(),
        )]))
        .is_err(),
        "an extreme positive duration must disable health instead of panicking"
    );
    assert_eq!(
        HealthRuntimeConfig::from_settings(&HashMap::new())
            .expect("absent settings use the accepted defaults")
            .thresholds
            .postgres_critical_failures,
        3,
        "the Ticket 05 watchdog receives the configured PostgreSQL three-failure threshold"
    );
}

fn synthetic_migrator(migrations: Vec<Migration>) -> Migrator {
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    }
}

fn synthetic_migration(version: i64, description: &'static str, sql: &'static str) -> Migration {
    Migration::new(
        version,
        Cow::Borrowed(description),
        MigrationType::Simple,
        Cow::Borrowed(sql),
        false,
    )
}

#[test]
fn health_command_renders_the_persisted_snapshot_only_for_the_configured_operator() {
    let snapshot = killbot_rust::contract_intelligence::HealthSnapshot {
        status: HealthStatus::Degraded,
        observed_at: Utc
            .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .expect("fixed health time"),
        checks: vec![killbot_rust::contract_intelligence::HealthCheck {
            key: "contract_progress".to_string(),
            status: HealthStatus::Degraded,
            observed_at: Utc
                .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
                .single()
                .expect("fixed health time"),
            evidence: "region 10000002: regional contract progress is 960s old".to_string(),
            consecutive_failures: 0,
        }],
    };
    let response = render_health_response(HEALTH_OPERATOR_ID, &snapshot)
        .expect("operator receives the persisted health snapshot");
    assert!(response.contains("Health: degraded"));
    assert!(response.contains("contract_progress: degraded"));
    assert!(render_health_response(HEALTH_OPERATOR_ID + 1, &snapshot).is_none());
}

#[tokio::test]
async fn health_schema_failure_isolated_but_core_migration_failure_still_rejects_connect() {
    let health_database = TemporaryDatabase::new().await;
    let _ = health_database.store().await;
    let health_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&health_database.url)
        .await
        .expect("connect to introduce a health-only schema conflict");
    health_pool
        .execute("DROP TABLE health_backlog_metrics, health_feed_telemetry, health_discord_views, health_transitions, health_check_results, health_snapshots, bot_heartbeats")
        .await
        .expect("remove applied health schema");
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 20260817000001")
        .execute(&health_pool)
        .await
        .expect("make the health migrations pending again");
    health_pool
        .execute("CREATE TABLE health_snapshots (conflicting_column INTEGER)")
        .await
        .expect("create a health-only migration conflict");

    let isolated_store = ContractCollectionStore::connect(&health_database.url)
        .await
        .expect("health migration failure does not reject the core store");
    ContractCollector::new(
        isolated_store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect("health schema failure does not block contract collection");
    let health_cycle = HealthCycle::new(
        isolated_store,
        Arc::new(FixedHealthClock(StdMutex::new(Utc::now()))),
    );
    for attempt in 0..2 {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), health_cycle.run_observationally())
                .await
                .expect("a failed health initialization releases SQLx's migration advisory lock")
                .is_none(),
            "health initialization retry {attempt} remains disabled until its own migration succeeds"
        );
    }
    health_pool.close().await;
    health_database.destroy().await;

    let core_database = TemporaryDatabase::unavailable().await;
    core_database.create().await;
    let core_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&core_database.url)
        .await
        .expect("connect to introduce a core schema conflict");
    core_pool
        .execute("CREATE TABLE public_contract_facts (conflicting_column INTEGER)")
        .await
        .expect("create a core migration conflict");
    assert!(
        ContractCollectionStore::connect(&core_database.url)
            .await
            .is_err(),
        "a non-health migration failure must still reject core store initialization"
    );
    core_pool.close().await;
    core_database.destroy().await;

    assert_health_migration_continuation_and_lock_release().await;
}

async fn assert_health_migration_continuation_and_lock_release() {
    const EARLY_CORE: i64 = 70000001;
    const EARLIER_HEALTH: i64 = 70000002;
    const FAILING_HEALTH: i64 = 70000003;
    const LATER_CORE: i64 = 70000004;
    const EARLY_TABLE: &str = "synthetic_health_isolation_early_core";
    const EARLIER_HEALTH_TABLE: &str = "synthetic_health_isolation_earlier_health";
    const LATER_TABLE: &str = "synthetic_health_isolation_later_core";

    let migrator = synthetic_migrator(vec![
        synthetic_migration(
            EARLY_CORE,
            "early core",
            "CREATE TABLE synthetic_health_isolation_early_core (id INTEGER)",
        ),
        synthetic_migration(
            EARLIER_HEALTH,
            "earlier health",
            "CREATE TABLE synthetic_health_isolation_earlier_health (id INTEGER)",
        ),
        synthetic_migration(
            FAILING_HEALTH,
            "failing health",
            "CREATE TABLE synthetic_health_isolation_failing_health (id INTEGER)",
        ),
        synthetic_migration(
            LATER_CORE,
            "later core",
            "CREATE TABLE synthetic_health_isolation_later_core (id INTEGER)",
        ),
    ]);

    let database = TemporaryDatabase::unavailable().await;
    database.create().await;
    let conflict_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to create the synthetic health migration conflict");
    conflict_pool
        .execute(
            "CREATE TABLE synthetic_health_isolation_failing_health (conflicting_column INTEGER)",
        )
        .await
        .expect("create health-only synthetic conflict");
    conflict_pool.close().await;

    let store = ContractCollectionStore::connect_with_migrator_for_test(
        &database.url,
        &migrator,
        &[EARLIER_HEALTH, FAILING_HEALTH],
    )
    .await
    .expect("an allow-listed health migration failure continues with later core migrations");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect synthetic migration results");
    for table in [EARLY_TABLE, EARLIER_HEALTH_TABLE, LATER_TABLE] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(&pool)
            .await
            .expect("inspect synthetic core migration result");
        assert!(
            exists,
            "{table} applied despite the health migration failure"
        );
    }

    tokio::time::timeout(
        Duration::from_secs(2),
        ContractCollectionStore::connect_with_migrator_for_test(
            &database.url,
            &migrator,
            &[EARLIER_HEALTH, FAILING_HEALTH],
        ),
    )
    .await
    .expect("a subsequent migration attempt does not wait on a leaked advisory lock")
    .expect("a subsequent migration attempt remains usable");
    pool.close().await;
    drop(store);
    database.destroy().await;

    let later_failure_database = TemporaryDatabase::unavailable().await;
    later_failure_database.create().await;
    let later_conflict_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&later_failure_database.url)
        .await
        .expect("connect to create synthetic health and later-core conflicts");
    later_conflict_pool
        .execute("CREATE TABLE synthetic_health_isolation_failing_health (conflicting_column INTEGER); CREATE TABLE synthetic_health_isolation_later_core (conflicting_column INTEGER)")
        .await
        .expect("create synthetic health and later-core conflicts");
    later_conflict_pool.close().await;
    assert!(
        ContractCollectionStore::connect_with_migrator_for_test(
            &later_failure_database.url,
            &migrator,
            &[EARLIER_HEALTH, FAILING_HEALTH],
        )
        .await
        .is_err(),
        "a later core migration failure remains fatal after an isolated health failure"
    );
    let later_inspection_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&later_failure_database.url)
        .await
        .expect("connect to inspect the later-core failure path");
    let early_applied: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(EARLY_TABLE)
        .fetch_one(&later_inspection_pool)
        .await
        .expect("inspect early core migration result");
    assert!(
        early_applied,
        "the fallback reached the later core migration"
    );
    later_inspection_pool.close().await;
    later_failure_database.destroy().await;

    let early_failure_database = TemporaryDatabase::unavailable().await;
    early_failure_database.create().await;
    let early_conflict_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&early_failure_database.url)
        .await
        .expect("connect to create synthetic early-core conflict");
    early_conflict_pool
        .execute("CREATE TABLE synthetic_health_isolation_early_core (conflicting_column INTEGER)")
        .await
        .expect("create synthetic early-core conflict");
    early_conflict_pool.close().await;
    assert!(
        ContractCollectionStore::connect_with_migrator_for_test(
            &early_failure_database.url,
            &migrator,
            &[EARLIER_HEALTH, FAILING_HEALTH],
        )
        .await
        .is_err(),
        "an earlier core migration failure remains fatal and cannot enter the health fallback"
    );
    let early_inspection_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&early_failure_database.url)
        .await
        .expect("connect to inspect early-core failure path");
    let later_applied: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(LATER_TABLE)
        .fetch_one(&early_inspection_pool)
        .await
        .expect("inspect later-core absence");
    assert!(
        !later_applied,
        "the fallback does not run after an earlier core migration failure"
    );
    early_inspection_pool.close().await;
    early_failure_database.destroy().await;
}

#[test]
fn health_command_response_is_below_the_discord_limit_and_reports_truncation() {
    let snapshot = killbot_rust::contract_intelligence::HealthSnapshot {
        status: HealthStatus::Critical,
        observed_at: Utc
            .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .expect("fixed health time"),
        checks: (0..80)
            .map(|index| HealthCheck {
                key: format!("check-{index}"),
                status: HealthStatus::Critical,
                observed_at: Utc
                    .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
                    .single()
                    .expect("fixed health time"),
                evidence: "x".repeat(120),
                consecutive_failures: 1,
            })
            .collect(),
    };

    let response = render_health_response(HEALTH_OPERATOR_ID, &snapshot)
        .expect("operator receives the health snapshot");
    assert!(response.chars().count() < 2_000);
    assert!(response.contains("additional check(s) truncated"));
}

#[tokio::test]
async fn health_cycle_caps_regional_evidence_and_reports_the_omitted_region_count() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to the regional health-evidence database");
    let now = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed health time");
    sqlx::query(
        "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current heartbeat");
    sqlx::query(
        "INSERT INTO esi_cache_metadata (resource_key, updated_at) VALUES ('esi:regions', $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current ESI progress");
    for region_id in 10_000_001..10_000_041 {
        sqlx::query("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES ($1, $2, 1, $2)")
            .bind(region_id)
            .bind(now - chrono::Duration::minutes(16))
            .execute(&pool)
            .await
            .expect("seed stale regional progress");
    }

    let snapshot = HealthCycle::new(store, Arc::new(FixedHealthClock(StdMutex::new(now))))
        .run_once()
        .await
        .expect("persist bounded regional health evidence");
    let regional = snapshot
        .checks
        .iter()
        .find(|check| check.key == "contract_progress")
        .expect("contract progress check");
    assert_eq!(regional.status, HealthStatus::Degraded);
    assert!(regional
        .evidence
        .contains("additional stale region(s) truncated"));
    assert!(regional.evidence.len() < 2_000);

    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn concurrent_health_cycles_serialize_transition_comparison_and_insert() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&database.url)
        .await
        .expect("connect to the concurrent health-cycle database");
    let now = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed concurrent health time");
    sqlx::query(
        "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed a current heartbeat");
    sqlx::query("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES (10000002, $1, 1, $1)")
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed current contract progress");
    sqlx::query(
        "INSERT INTO esi_cache_metadata (resource_key, updated_at) VALUES ('esi:regions', $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current ESI progress");
    HealthCycle::new(
        store.clone(),
        Arc::new(FixedHealthClock(StdMutex::new(now))),
    )
    .run_once()
    .await
    .expect("persist the initial healthy snapshot");
    sqlx::query("INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES (42, 77, 'concurrent-health', 'concurrent health delivery', '{}'::jsonb, '{}'::jsonb)")
        .execute(&pool)
        .await
        .expect("seed the concurrent health subscription");
    sqlx::query("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status, delivery_nonce, prepared_at) VALUES (42, 77, 'concurrent-health', 44, 'listed', '{}'::jsonb, '{}'::jsonb, FALSE, 'prepared', 'concurrent-health', $1)")
        .bind(now - chrono::Duration::minutes(6))
        .execute(&pool)
        .await
        .expect("seed an overdue prepared delivery");

    let mut lock_transaction = pool.begin().await.expect("begin advisory lock transaction");
    sqlx::query("SELECT pg_advisory_xact_lock(7_142_300_993_001)")
        .execute(&mut *lock_transaction)
        .await
        .expect("hold the health transition advisory lock");
    let first_cycle = HealthCycle::new(
        store.clone(),
        Arc::new(FixedHealthClock(StdMutex::new(
            now + chrono::Duration::seconds(1),
        ))),
    );
    let second_cycle = HealthCycle::new(
        database.store().await,
        Arc::new(FixedHealthClock(StdMutex::new(
            now + chrono::Duration::seconds(2),
        ))),
    );
    let mut first = tokio::spawn(async move { first_cycle.run_once().await });
    let mut second = tokio::spawn(async move { second_cycle.run_once().await });
    assert!(
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                _ = &mut first => {},
                _ = &mut second => {},
            }
        })
        .await
        .is_err(),
        "both cycles wait for the transaction-scoped health transition lock"
    );
    lock_transaction
        .commit()
        .await
        .expect("release the health transition advisory lock");

    assert_eq!(
        first
            .await
            .expect("join the first serialized health cycle")
            .expect("first serialized health cycle succeeds")
            .status,
        HealthStatus::Degraded
    );
    assert_eq!(
        second
            .await
            .expect("join the second serialized health cycle")
            .expect("second serialized health cycle succeeds")
            .status,
        HealthStatus::Degraded
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM health_transitions WHERE check_key = 'prepared_delivery' AND status = 'degraded'")
            .fetch_one(&pool)
            .await
            .expect("count the serialized prepared-delivery transition"),
        1
    );

    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn older_health_cycle_completion_does_not_overwrite_newer_persisted_health_state() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to ordered health-cycle database");
    let now = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed ordered health time");
    sqlx::query(
        "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed initial heartbeat");
    sqlx::query("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES (10000002, $1, 1, $1)")
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed regional health evidence");
    sqlx::query(
        "INSERT INTO esi_cache_metadata (resource_key, updated_at) VALUES ('esi:regions', $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed ESI health evidence");
    let telemetry = Arc::new(FeedHealthTelemetry::new());
    telemetry.record_r2z2_progress_at(now).await;
    telemetry.record_validation_success_at(now).await;
    let newer_at = now + chrono::Duration::seconds(2);
    let older_at = now + chrono::Duration::seconds(1);
    HealthCycle::new(
        store.clone(),
        Arc::new(FixedHealthClock(StdMutex::new(newer_at))),
    )
    .with_feed_telemetry(telemetry, true)
    .run_once()
    .await
    .expect("persist the newer health cycle first");
    sqlx::query("INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES (42, 77, 'equal-health', 'equal timestamp health delivery', '{}'::jsonb, '{}'::jsonb)")
        .execute(&pool)
        .await
        .expect("seed an equal-timestamp prepared delivery");
    sqlx::query("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status, delivery_nonce, prepared_at) VALUES (42, 77, 'equal-health', 44, 'listed', '{}'::jsonb, '{}'::jsonb, FALSE, 'prepared', 'equal-health', $1)")
        .bind(now - chrono::Duration::minutes(6))
        .execute(&pool)
        .await
        .expect("make the equal-timestamp snapshot observably different");
    assert_eq!(
        HealthCycle::new(
            store.clone(),
            Arc::new(FixedHealthClock(StdMutex::new(newer_at))),
        )
        .with_feed_telemetry(Arc::new(FeedHealthTelemetry::new()), true)
        .run_once()
        .await
        .expect("evaluate a same-time overlapping health cycle")
        .status,
        HealthStatus::Degraded
    );
    HealthCycle::new(
        database.store().await,
        Arc::new(FixedHealthClock(StdMutex::new(older_at))),
    )
    .with_feed_telemetry(Arc::new(FeedHealthTelemetry::new()), true)
    .run_once()
    .await
    .expect("complete the older health cycle after the newer one");

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM health_snapshots WHERE singleton = TRUE",
        )
        .fetch_one(&pool)
        .await
        .expect("preserve the first same-timestamp health snapshot"),
        "healthy"
    );
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT observed_at FROM health_snapshots WHERE singleton = TRUE",
        )
        .fetch_one(&pool)
        .await
        .expect("read monotonic health snapshot"),
        newer_at
    );
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT min(observed_at) FROM health_check_results",
        )
        .fetch_one(&pool)
        .await
        .expect("read monotonic health checks"),
        newer_at
    );
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT updated_at FROM health_feed_telemetry WHERE source = 'feed_validation'",
        )
        .fetch_one(&pool)
        .await
        .expect("read monotonic feed state"),
        newer_at
    );
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT observed_at FROM bot_heartbeats WHERE component = 'bot'",
        )
        .fetch_one(&pool)
        .await
        .expect("read monotonic heartbeat"),
        newer_at
    );

    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn health_monitor_session_restart_does_not_renew_missing_evidence_grace() {
    let database = TemporaryDatabase::new().await;
    let start = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed health start time");
    let clock = Arc::new(FixedHealthClock(StdMutex::new(start)));
    let store = database.store().await;

    store
        .begin_health_monitor_session(clock.now())
        .await
        .expect("begin the initial health monitor session");
    clock.advance(chrono::Duration::minutes(16));
    store
        .begin_health_monitor_session(clock.now())
        .await
        .expect("restart the health monitor session without replacing its durable anchor");

    assert_eq!(
        HealthCycle::new(store, clock)
            .run_once()
            .await
            .expect("evaluate missing evidence after a monitor restart")
            .status,
        HealthStatus::Degraded
    );

    database.destroy().await;
}

#[tokio::test]
async fn health_cycle_applies_a_durable_startup_grace_to_missing_contract_and_esi_evidence() {
    let database = TemporaryDatabase::new().await;
    let start = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed health start time");
    let clock = Arc::new(FixedHealthClock(StdMutex::new(start)));
    let feed_telemetry = Arc::new(FeedHealthTelemetry::new());

    let cold_start_snapshot = HealthCycle::new(database.store().await, clock.clone())
        .with_feed_telemetry(feed_telemetry.clone(), true)
        .run_once()
        .await
        .expect("the cold-start health cycle persists its grace anchor");
    assert_eq!(cold_start_snapshot.status, HealthStatus::Healthy);
    assert!(cold_start_snapshot
        .checks
        .iter()
        .any(|check| check.key == "r2z2_progress" && check.status == HealthStatus::Healthy));
    assert!(cold_start_snapshot
        .checks
        .iter()
        .any(|check| check.key == "feed_validation" && check.status == HealthStatus::Healthy));

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to update the independently refreshed heartbeat");
    clock.advance(chrono::Duration::minutes(4));
    sqlx::query("UPDATE bot_heartbeats SET observed_at = $1 WHERE component = 'bot'")
        .bind(clock.now())
        .execute(&pool)
        .await
        .expect("refresh the bot heartbeat without creating contract or ESI evidence");
    let degraded_snapshot = HealthCycle::new(database.store().await, clock.clone())
        .with_feed_telemetry(feed_telemetry.clone(), true)
        .run_once()
        .await
        .expect("the restarted cycle reads the durable grace anchor");
    assert_eq!(degraded_snapshot.status, HealthStatus::Degraded);
    assert!(degraded_snapshot
        .checks
        .iter()
        .any(|check| check.key == "r2z2_progress" && check.status == HealthStatus::Degraded));
    assert!(degraded_snapshot
        .checks
        .iter()
        .any(|check| check.key == "feed_validation" && check.status == HealthStatus::Degraded));

    clock.advance(chrono::Duration::minutes(12));
    sqlx::query("UPDATE bot_heartbeats SET observed_at = $1 WHERE component = 'bot'")
        .bind(clock.now())
        .execute(&pool)
        .await
        .expect("keep the independently refreshed heartbeat current");
    let critical_snapshot = HealthCycle::new(database.store().await, clock.clone())
        .with_feed_telemetry(feed_telemetry, true)
        .run_once()
        .await
        .expect("missing evidence becomes critical after the startup grace");
    assert_eq!(critical_snapshot.status, HealthStatus::Critical);
    assert!(critical_snapshot
        .checks
        .iter()
        .any(|check| check.key == "r2z2_progress" && check.status == HealthStatus::Critical));
    assert!(critical_snapshot
        .checks
        .iter()
        .any(|check| check.key == "feed_validation" && check.status == HealthStatus::Critical));

    clock.advance(chrono::Duration::minutes(15));
    sqlx::query("UPDATE bot_heartbeats SET observed_at = $1 WHERE component = 'bot'")
        .bind(clock.now())
        .execute(&pool)
        .await
        .expect("keep the independently refreshed heartbeat current through contract grace");
    let all_missing_critical = HealthCycle::new(database.store().await, clock.clone())
        .with_feed_telemetry(Arc::new(FeedHealthTelemetry::new()), true)
        .run_once()
        .await
        .expect("missing contract and ESI evidence become critical after their startup grace");
    assert!(all_missing_critical
        .checks
        .iter()
        .any(|check| check.key == "contract_progress" && check.status == HealthStatus::Critical));
    assert!(all_missing_critical
        .checks
        .iter()
        .any(|check| check.key == "esi_progress" && check.status == HealthStatus::Critical));

    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn health_cycle_persists_thresholds_recovers_after_restart_and_isolates_store_failure() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to health-cycle database");
    let now = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed health time");
    let clock = Arc::new(FixedHealthClock(StdMutex::new(now)));
    let initial_progress_at = now - chrono::Duration::minutes(1);
    sqlx::query(
        "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1)",
    )
    .bind(initial_progress_at)
    .execute(&pool)
    .await
    .expect("seed initial heartbeat");
    sqlx::query("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES (10000002, $1, 1, $1)")
        .bind(initial_progress_at)
        .execute(&pool)
        .await
        .expect("seed initial contract progress");
    sqlx::query(
        "INSERT INTO esi_cache_metadata (resource_key, updated_at) VALUES ('esi:regions', $1)",
    )
    .bind(initial_progress_at)
    .execute(&pool)
    .await
    .expect("seed initial ESI progress");

    let cycle = HealthCycle::new(store.clone(), clock.clone());
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("persist healthy health snapshot")
            .status,
        HealthStatus::Healthy
    );

    clock.advance(chrono::Duration::minutes(31));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("persist stale heartbeat and contract progress")
            .status,
        HealthStatus::Critical
    );
    clock.advance(chrono::Duration::seconds(1));
    let current_time = clock.now();
    sqlx::query(
        "UPDATE regional_collection_metadata SET last_complete_at = $1 WHERE region_id = 10000002",
    )
    .bind(current_time)
    .execute(&pool)
    .await
    .expect("recover contract progress evidence");
    sqlx::query("UPDATE esi_cache_metadata SET updated_at = $1 WHERE resource_key = 'esi:regions'")
        .bind(current_time)
        .execute(&pool)
        .await
        .expect("recover ESI progress evidence");
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("persist recovered health snapshot")
            .status,
        HealthStatus::Healthy
    );
    let transitions_before_restart: i64 =
        sqlx::query_scalar("SELECT count(*) FROM health_transitions")
            .fetch_one(&pool)
            .await
            .expect("count persisted health transitions");
    let restarted_cycle = HealthCycle::new(database.store().await, clock.clone());
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("read the persisted health state after restart")
            .status,
        HealthStatus::Healthy
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM health_transitions")
            .fetch_one(&pool)
            .await
            .expect("restarted health cycle does not duplicate transitions"),
        transitions_before_restart
    );
    sqlx::query("INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES (42, 77, 'health', 'health delivery', '{}'::jsonb, '{}'::jsonb)")
        .execute(&pool)
        .await
        .expect("seed health delivery subscription");
    sqlx::query("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status, delivery_nonce, prepared_at) VALUES (42, 77, 'health', 44, 'listed', '{}'::jsonb, '{}'::jsonb, FALSE, 'prepared', 'health-prepared', $1)")
        .bind(clock.now() - chrono::Duration::minutes(6))
        .execute(&pool)
        .await
        .expect("seed old prepared delivery");
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("evaluate prepared delivery age")
            .status,
        HealthStatus::Degraded
    );
    clock.advance(chrono::Duration::seconds(1));
    sqlx::query("UPDATE contract_outbound_deliveries SET status = 'sent', sent_at = $1 WHERE delivery_nonce = 'health-prepared'")
        .bind(clock.now())
        .execute(&pool)
        .await
        .expect("resolve prepared delivery");
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("recover prepared delivery health")
            .status,
        HealthStatus::Healthy
    );
    clock.advance(chrono::Duration::seconds(1));
    sqlx::query("UPDATE contract_outbound_deliveries SET status = 'failed', failure_kind = 'permanent', failure_resolved_at = NULL WHERE delivery_nonce = 'health-prepared'")
        .execute(&pool)
        .await
        .expect("seed permanent delivery failure");
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("evaluate permanent delivery failure")
            .status,
        HealthStatus::Critical
    );
    clock.advance(chrono::Duration::seconds(1));
    sqlx::query("UPDATE contract_outbound_deliveries SET failure_resolved_at = $1 WHERE delivery_nonce = 'health-prepared'")
        .bind(clock.now())
        .execute(&pool)
        .await
        .expect("resolve permanent delivery failure");
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("recover permanent delivery health")
            .status,
        HealthStatus::Healthy
    );

    clock.advance(chrono::Duration::seconds(1));
    sqlx::query("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES (10000002, 99, '{}'::jsonb, '{}'::jsonb, $1, $1, $2, 'awaiting_resolution')")
        .bind(clock.now() - chrono::Duration::minutes(31))
        .bind(clock.now() - chrono::Duration::minutes(31))
        .execute(&pool)
        .await
        .expect("seed old due resolution backlog");
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("evaluate due resolution backlog")
            .status,
        HealthStatus::Degraded
    );
    clock.advance(chrono::Duration::seconds(1));
    sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = $1 WHERE region_id = 10000002 AND contract_id = 99")
        .bind(clock.now() + chrono::Duration::minutes(1))
        .execute(&pool)
        .await
        .expect("defer resolved backlog probe");
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("recover due resolution backlog health")
            .status,
        HealthStatus::Healthy
    );
    sqlx::query(
        "DELETE FROM contract_resolution_cases WHERE region_id = 10000002 AND contract_id = 99",
    )
    .execute(&pool)
    .await
    .expect("remove synthetic health-only resolution backlog");

    pool.execute("DROP TABLE health_snapshots")
        .await
        .expect("break health storage only");
    assert!(restarted_cycle.run_observationally().await.is_none());
    let collector = ContractCollector::new(
        store,
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        }),
    );
    collector
        .collect_cycle()
        .await
        .expect("health storage failure does not block contract collection");

    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn health_cycle_persists_feed_progress_validation_and_a_stable_growing_backlog_trend() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to health telemetry database");
    let now = Utc
        .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
        .single()
        .expect("fixed health telemetry time");
    let clock = Arc::new(FixedHealthClock(StdMutex::new(now)));
    sqlx::query(
        "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current heartbeat");
    sqlx::query("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES (10000002, $1, 1, $1)")
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed current regional progress");
    sqlx::query(
        "INSERT INTO esi_cache_metadata (resource_key, updated_at) VALUES ('esi:regions', $1)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed current ESI progress");
    sqlx::query("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES (10000002, 99, '{}'::jsonb, '{}'::jsonb, $1, $1, $2, 'awaiting_resolution')")
        .bind(now - chrono::Duration::minutes(31))
        .bind(now - chrono::Duration::minutes(31))
        .execute(&pool)
        .await
        .expect("seed an aged backlog baseline");

    let telemetry = Arc::new(FeedHealthTelemetry::new());
    telemetry.record_r2z2_progress_at(now).await;
    telemetry.record_validation_success_at(now).await;
    let cycle =
        HealthCycle::new(store.clone(), clock.clone()).with_feed_telemetry(telemetry.clone(), true);
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("persist the initial bounded backlog baseline")
            .status,
        HealthStatus::Healthy
    );

    sqlx::query("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES (10000002, 100, '{}'::jsonb, '{}'::jsonb, $1, $1, $2, 'awaiting_resolution')")
        .bind(now - chrono::Duration::minutes(31))
        .bind(now - chrono::Duration::minutes(31))
        .execute(&pool)
        .await
        .expect("grow the aged resolution backlog");
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("persist the growing backlog state")
            .status,
        HealthStatus::Degraded
    );
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("keep a grown backlog unhealthy while it remains unchanged")
            .status,
        HealthStatus::Degraded
    );
    sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = $1 WHERE region_id = 10000002 AND contract_id = 100")
        .bind(now + chrono::Duration::minutes(1))
        .execute(&pool)
        .await
        .expect("shrink the aged resolution backlog");
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("clear the growing backlog health state after a shrink")
            .status,
        HealthStatus::Healthy
    );

    telemetry.record_validation_failure_at(now).await;
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("persist a feed validation failure")
            .status,
        HealthStatus::Degraded
    );
    telemetry.record_validation_failure_at(now).await;
    telemetry.record_validation_failure_at(now).await;
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("apply the feed validation critical threshold")
            .status,
        HealthStatus::Critical
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>(
            "SELECT consecutive_failures FROM health_feed_telemetry WHERE source = 'feed_validation'",
        )
        .fetch_one(&pool)
        .await
        .expect("read persisted validation failures"),
        3
    );

    let restarted_cycle = HealthCycle::new(database.store().await, clock.clone())
        .with_feed_telemetry(Arc::new(FeedHealthTelemetry::new()), true);
    clock.advance(chrono::Duration::seconds(1));
    assert_eq!(
        restarted_cycle
            .run_once()
            .await
            .expect("retain feed validation evidence over a process restart")
            .status,
        HealthStatus::Critical
    );
    telemetry.record_validation_success_at(now).await;
    clock.advance(chrono::Duration::minutes(4));
    assert_eq!(
        cycle
            .run_once()
            .await
            .expect("apply the R2Z2 progress threshold")
            .checks
            .iter()
            .find(|check| check.key == "r2z2_progress")
            .expect("R2Z2 progress check")
            .status,
        HealthStatus::Degraded
    );

    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn regional_observation_batch_and_health_snapshot_migrations_apply_to_clean_and_current_databases(
) {
    let clean_database = TemporaryDatabase::new().await;
    let clean_store = clean_database.store().await;
    let clean_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&clean_database.url)
        .await
        .expect("connect to clean migrated database");
    let clean_migration_count: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&clean_pool)
        .await
        .expect("read clean migration ledger");
    assert_eq!(clean_migration_count, 16);
    let clean_pacing_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = 'esi_collection_limiter_state' AND column_name = 'next_request_at')",
    )
    .fetch_one(&clean_pool)
    .await
    .expect("read clean limiter pacing column");
    assert!(
        clean_pacing_column_exists,
        "clean migration adds ESI pacing state"
    );
    let batch_table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind("regional_observation_batches")
        .fetch_one(&clean_pool)
        .await
        .expect("read clean regional batch table");
    assert!(
        batch_table_exists,
        "clean migration creates regional batch ledger"
    );
    for table in ["location_evidence", "location_evidence_audit"] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(&clean_pool)
            .await
            .expect("read clean location-evidence table");
        assert!(exists, "clean migration creates {table}");
    }
    for table in [
        "bot_heartbeats",
        "health_snapshots",
        "health_check_results",
        "health_transitions",
        "health_discord_views",
        "health_feed_telemetry",
        "health_backlog_metrics",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(&clean_pool)
            .await
            .expect("read clean health table");
        assert!(exists, "clean migration creates {table}");
    }
    let clean_started_at_is_not_null: bool = sqlx::query_scalar(
        "SELECT is_nullable = 'NO' FROM information_schema.columns WHERE table_name = 'bot_heartbeats' AND column_name = 'started_at'",
    )
    .fetch_one(&clean_pool)
    .await
    .expect("read clean heartbeat startup anchor");
    assert!(clean_started_at_is_not_null);
    let clean_batch_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM regional_observation_batches")
            .fetch_one(&clean_pool)
            .await
            .expect("read clean regional batch ledger");
    assert_eq!(clean_batch_count, 0);
    assert_eq!(
        clean_store
            .storage_counts()
            .await
            .expect("read clean contract state")
            .presence_intervals,
        0
    );
    clean_pool.close().await;
    clean_database.destroy().await;

    let current_database = TemporaryDatabase::unavailable().await;
    current_database.create().await;
    let current_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&current_database.url)
        .await
        .expect("connect to current production migration database");
    current_pool
        .execute("CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .expect("create current production migration ledger");
    for (version, sql) in [
        (
            20260813000000_i64,
            include_str!("../migrations/20260813000000_create_contract_intelligence.sql"),
        ),
        (
            20260813000001_i64,
            include_str!(
                "../migrations/20260813000001_add_contract_collection_representation_metadata.sql"
            ),
        ),
        (
            20260813000002_i64,
            include_str!(
                "../migrations/20260813000002_add_contract_subscriptions_and_deliveries.sql"
            ),
        ),
        (
            20260813000003_i64,
            include_str!("../migrations/20260813000003_preserve_contract_delivery_history.sql"),
        ),
        (
            20260813000004_i64,
            include_str!("../migrations/20260813000004_add_contract_acceptance_resolution.sql"),
        ),
        (
            20260813000005_i64,
            include_str!(
                "../migrations/20260813000005_add_contract_nonfinancial_terminal_states.sql"
            ),
        ),
        (
            20260813000006_i64,
            include_str!("../migrations/20260813000006_add_contract_embed_context.sql"),
        ),
        (
            20260813000007_i64,
            include_str!("../migrations/20260813000007_make_contract_delivery_restart_safe.sql"),
        ),
        (
            20260813000008_i64,
            include_str!("../migrations/20260813000008_persist_contract_delivery_ping_type.sql"),
        ),
        (
            20260814000000_i64,
            include_str!("../migrations/20260814000000_recover_contract_collection.sql"),
        ),
        (
            20260814000001_i64,
            include_str!("../migrations/20260814000001_record_contract_failure_lifecycles.sql"),
        ),
        (
            20260814000002_i64,
            include_str!("../migrations/20260814000002_defer_contract_manifests.sql"),
        ),
    ] {
        current_pool
            .execute(sql)
            .await
            .expect("apply current production migration");
        sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES ($1, 'current production migration', TRUE, $2, 0)")
            .bind(version)
            .bind(Sha384::digest(sql.as_bytes()).to_vec())
            .execute(&current_pool)
            .await
            .expect("record current production migration");
    }
    current_pool
        .execute("INSERT INTO regional_collection_metadata (region_id, baseline_at, complete_observations, last_complete_at) VALUES (10000002, '2026-08-17T00:00:00Z', 4, '2026-08-17T01:00:00Z'), (10000003, '2026-08-17T00:00:00Z', 0, NULL)")
        .await
        .expect("seed trustworthy and insufficient regional metadata");
    let resolved_current_contract = item_exchange_contract(44);
    let pending_only_contract = item_exchange_contract(46);
    sqlx::query("INSERT INTO public_contract_facts (fact_hash, contract_id, facts) VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)")
        .bind("legacy-fact-current")
        .bind(44_i64)
        .bind(serde_json::to_value(&resolved_current_contract).expect("serialize current contract"))
        .bind("legacy-fact-historic")
        .bind(45_i64)
        .bind(serde_json::json!({}))
        .bind("legacy-fact-pending")
        .bind(46_i64)
        .bind(serde_json::to_value(&pending_only_contract).expect("serialize pending contract"))
        .execute(&current_pool)
        .await
        .expect("seed current production contract facts");
    current_pool
        .execute("INSERT INTO contract_item_manifests (manifest_hash, manifest) VALUES ('legacy-manifest-current', '{\"offered_items\":[],\"requested_items\":[]}'::jsonb), ('legacy-manifest-historic', '{\"offered_items\":[],\"requested_items\":[]}'::jsonb); INSERT INTO presence_intervals (region_id, contract_id, fact_hash, manifest_hash, first_observed_at, last_observed_at) VALUES (10000002, 44, 'legacy-fact-current', 'legacy-manifest-current', '2026-08-17T00:00:00Z', '2026-08-17T01:00:00Z'), (10000002, 45, 'legacy-fact-historic', 'legacy-manifest-historic', '2026-08-16T22:00:00Z', '2026-08-16T23:00:00Z'); INSERT INTO contract_manifest_pending (region_id, contract_id, fact_hash, first_observed_at, last_observed_at) VALUES (10000002, 46, 'legacy-fact-pending', '2026-08-17T00:00:00Z', '2026-08-17T01:00:00Z'); INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail) VALUES (10000002, 46, 'contracts/public/items/46', '2026-08-17T01:00:00Z', 'manifest_collection', 'legacy unresolved manifest'); INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES (42, 77, 'legacy', 'legacy subscription', '{}'::jsonb, '{}'::jsonb); INSERT INTO contract_deferred_subscription_matches (guild_id, channel_id, subscription_id, contract_id, event_kind, event) VALUES (42, 77, 'legacy', 44, 'listed', '{}'::jsonb); INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, status, delivery_nonce) VALUES (42, 77, 'legacy', 44, 'listed', '{}'::jsonb, '{}'::jsonb, 'prepared', 'ci-legacy')")
        .await
        .expect("seed current production contract state");
    current_pool.close().await;

    let upgraded_store = current_database.store().await;
    let upgraded_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&current_database.url)
        .await
        .expect("connect to upgraded production database");
    let upgraded_batch_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind("regional_observation_batches")
            .fetch_one(&upgraded_pool)
            .await
            .expect("read upgraded regional batch table");
    assert!(
        upgraded_batch_table_exists,
        "current production migration creates regional batch ledger"
    );
    let upgraded_pacing_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = 'esi_collection_limiter_state' AND column_name = 'next_request_at')",
    )
    .fetch_one(&upgraded_pool)
    .await
    .expect("read upgraded limiter pacing column");
    assert!(
        upgraded_pacing_column_exists,
        "current production migration adds ESI pacing state"
    );
    for table in ["location_evidence", "location_evidence_audit"] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(&upgraded_pool)
            .await
            .expect("read upgraded location-evidence table");
        assert!(exists, "current production migration creates {table}");
    }
    for table in [
        "bot_heartbeats",
        "health_snapshots",
        "health_check_results",
        "health_transitions",
        "health_discord_views",
        "health_feed_telemetry",
        "health_backlog_metrics",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(table)
            .fetch_one(&upgraded_pool)
            .await
            .expect("read upgraded health table");
        assert!(exists, "current production migration creates {table}");
    }
    let upgraded_started_at_is_not_null: bool = sqlx::query_scalar(
        "SELECT is_nullable = 'NO' FROM information_schema.columns WHERE table_name = 'bot_heartbeats' AND column_name = 'started_at'",
    )
    .fetch_one(&upgraded_pool)
    .await
    .expect("read upgraded heartbeat startup anchor");
    assert!(upgraded_started_at_is_not_null);
    let trusted_initialized: bool = sqlx::query_scalar("SELECT current_recovery_epoch_id IS NOT NULL AND NOT requires_silent_baseline FROM regional_collection_metadata WHERE region_id = 10000002")
        .fetch_one(&upgraded_pool)
        .await
        .expect("read trusted regional continuity");
    assert!(trusted_initialized);
    let initialized_recovery_epoch_id: i64 = sqlx::query_scalar(
        "SELECT current_recovery_epoch_id FROM regional_collection_metadata WHERE region_id = 10000002",
    )
    .fetch_one(&upgraded_pool)
    .await
    .expect("read migration-initialized recovery epoch");
    let insufficient_requires_baseline: bool = sqlx::query_scalar("SELECT current_recovery_epoch_id IS NULL AND requires_silent_baseline FROM regional_collection_metadata WHERE region_id = 10000003")
        .fetch_one(&upgraded_pool)
        .await
        .expect("read insufficient regional continuity");
    assert!(insufficient_requires_baseline);
    for (table, expected_count) in [
        ("public_contract_facts", 3),
        ("presence_intervals", 2),
        ("contract_subscriptions", 1),
        ("contract_deferred_subscription_matches", 1),
        ("contract_outbound_deliveries", 1),
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&upgraded_pool)
            .await
            .expect("read preserved current production state");
        assert_eq!(count, expected_count, "migration preserves {table}");
    }
    let seeded_epoch_contracts: Vec<i64> = sqlx::query_scalar("SELECT presence.contract_id FROM regional_epoch_contract_presence AS presence JOIN regional_collection_metadata AS metadata ON metadata.current_recovery_epoch_id = presence.recovery_epoch_id WHERE metadata.region_id = 10000002 ORDER BY presence.contract_id")
        .fetch_all(&upgraded_pool)
        .await
        .expect("seed only the final complete snapshot into the epoch");
    assert_eq!(seeded_epoch_contracts, vec![44, 46]);
    assert_eq!(
        upgraded_store
            .storage_counts()
            .await
            .expect("read upgraded contract state")
            .presence_intervals,
        2
    );

    let disappearance = Arc::new(ResolutionEsi {
        inner: FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![], expiring_page(1))),
            )]),
            items: HashMap::new(),
        },
        probes: StdMutex::new(vec![Ok(ContractItemProbe::NotPublic(expiring_cache()))]),
        probe_calls: StdMutex::new(Vec::new()),
    });
    let disappearance_report =
        ContractCollector::new(upgraded_store.clone(), disappearance.clone())
            .with_recovery_gap(chrono::Duration::milliseconds(i64::MAX))
            .collect_cycle()
            .await
            .expect("resolve an upgraded same-epoch disappearance");
    assert_eq!(
        disappearance_report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![ContractEventKind::ClosedOutcomeUnknown]
    );
    assert_eq!(
        disappearance.probe_calls.lock().unwrap().as_slice(),
        &[(44, None)]
    );
    let epoch_presence_after_disappearance: Vec<i64> = sqlx::query_scalar("SELECT presence.contract_id FROM regional_epoch_contract_presence AS presence JOIN regional_collection_metadata AS metadata ON metadata.current_recovery_epoch_id = presence.recovery_epoch_id WHERE metadata.region_id = 10000002 ORDER BY presence.contract_id")
        .fetch_all(&upgraded_pool)
        .await
        .expect("retain migration-seeded same-epoch presence after disappearance");
    assert_eq!(epoch_presence_after_disappearance, vec![44, 46]);
    let pending_only_fact_retained: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM public_contract_facts WHERE fact_hash = 'legacy-fact-pending' AND contract_id = 46)",
    )
    .fetch_one(&upgraded_pool)
    .await
    .expect("retain the pending-only contract fact after disappearance");
    assert!(pending_only_fact_retained);
    let pending_only_failure_resolved: bool = sqlx::query_scalar(
        "SELECT resolved_at IS NOT NULL FROM contract_collection_failures WHERE region_id = 10000002 AND contract_id = 46 AND resource_key = 'contracts/public/items/46' AND failure_kind = 'manifest_collection'",
    )
    .fetch_one(&upgraded_pool)
    .await
    .expect("record the pending-only disappearance through its manifest failure lifecycle");
    assert!(pending_only_failure_resolved);
    let pending_only_absence_batch = sqlx::query("SELECT recovery_epoch_id = $1 AS migration_epoch, outcome, observed_contract_count FROM regional_observation_batches WHERE region_id = 10000002 ORDER BY id DESC LIMIT 1")
        .bind(initialized_recovery_epoch_id)
        .fetch_one(&upgraded_pool)
        .await
        .expect("record the pending-only absence in the regional batch ledger");
    assert!(pending_only_absence_batch.get::<bool, _>("migration_epoch"));
    assert_eq!(
        pending_only_absence_batch.get::<String, _>("outcome"),
        "complete"
    );
    assert_eq!(
        pending_only_absence_batch.get::<i64, _>("observed_contract_count"),
        0
    );
    let upgraded_resolution_records = upgraded_store
        .contract_resolution_records()
        .await
        .expect("read upgraded same-epoch resolution");
    assert_eq!(upgraded_resolution_records.len(), 1);
    assert_eq!(upgraded_resolution_records[0].region_id, 10_000_002);
    assert_eq!(upgraded_resolution_records[0].contract_id, 44);
    assert_eq!(
        upgraded_resolution_records[0].state,
        ContractResolutionState::ClosedOutcomeUnknown
    );

    upgraded_pool.close().await;
    current_database.destroy().await;
}

#[tokio::test]
async fn regional_recovery_epochs_isolate_stale_and_healthy_regions_before_and_after_restart() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let stale_region = 10_000_002;
    let healthy_region = 10_000_003;
    let stale_prior = item_exchange_contract(44);
    let healthy_prior = item_exchange_contract(55);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![stale_region, healthy_region],
            pages: HashMap::from([
                (
                    (stale_region, 1),
                    Ok(EsiResponse::fresh(
                        vec![stale_prior.clone()],
                        expiring_page(1),
                    )),
                ),
                (
                    (healthy_region, 1),
                    Ok(EsiResponse::fresh(
                        vec![healthy_prior.clone()],
                        expiring_page(1),
                    )),
                ),
            ]),
            items: HashMap::from([
                (
                    stale_prior.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    healthy_prior.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish independent regional baselines");
    store
        .upsert_contract_subscription(&contract_subscription(
            "ships",
            ContractItemDirection::Offered,
            vec![587],
            vec![],
            ContractEventAction::Post,
        ))
        .await
        .expect("persist listing subscription");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to age only the stale region");
    sqlx::query("UPDATE regional_observation_batches SET attempt_started_at = now() - interval '2 hours', completed_at = now() - interval '2 hours' WHERE region_id = $1")
        .bind(stale_region)
        .execute(&pool)
        .await
        .expect("create a stale regional continuity gap");
    pool.close().await;

    let stale_recovery_contract = item_exchange_contract(45);
    let healthy_listed_contract = item_exchange_contract(56);
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let recovery = ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: FakeEsi {
                regions: vec![stale_region, healthy_region],
                pages: HashMap::from([
                    (
                        (stale_region, 1),
                        Ok(EsiResponse::fresh(
                            vec![stale_recovery_contract.clone()],
                            expiring_page(1),
                        )),
                    ),
                    (
                        (healthy_region, 1),
                        Ok(EsiResponse::fresh(
                            vec![healthy_prior.clone(), healthy_listed_contract.clone()],
                            expiring_page(1),
                        )),
                    ),
                ]),
                items: HashMap::from([
                    (
                        stale_recovery_contract.contract_id,
                        Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
                    ),
                    (
                        healthy_prior.contract_id,
                        Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                    ),
                    (
                        healthy_listed_contract.contract_id,
                        Ok(EsiResponse::fresh(vec![offered_ship(4)], expiring_cache())),
                    ),
                ]),
            },
            probes: StdMutex::new(vec![Ok(ContractItemProbe::NotFound(expiring_cache()))]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("recover only the stale region");
    assert_eq!(
        recovery.regions,
        vec![
            CollectionOutcome::RecoveryBaselineEstablished {
                region_id: stale_region
            },
            CollectionOutcome::Complete {
                region_id: healthy_region,
                observed_contracts: 2
            },
        ]
    );
    assert_eq!(
        recovery
            .events
            .iter()
            .map(|event| (event.region_id, event.contract.contract_id, event.kind))
            .collect::<Vec<_>>(),
        vec![(
            healthy_region,
            healthy_listed_contract.contract_id,
            ContractEventKind::Listed,
        )]
    );
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);

    let epoch_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect regional batch evidence");
    let stale_epochs: Vec<i64> = sqlx::query_scalar("SELECT recovery_epoch_id FROM regional_observation_batches WHERE region_id = $1 ORDER BY id")
        .bind(stale_region)
        .fetch_all(&epoch_pool)
        .await
        .expect("read stale regional batch epochs");
    let healthy_epochs: Vec<i64> = sqlx::query_scalar("SELECT recovery_epoch_id FROM regional_observation_batches WHERE region_id = $1 ORDER BY id")
        .bind(healthy_region)
        .fetch_all(&epoch_pool)
        .await
        .expect("read healthy regional batch epochs");
    assert_eq!(stale_epochs.len(), 2);
    assert_eq!(healthy_epochs.len(), 2);
    assert_ne!(stale_epochs[0], stale_epochs[1]);
    assert_eq!(healthy_epochs[0], healthy_epochs[1]);
    epoch_pool.close().await;

    drop(delivery);
    drop(store);
    let restarted_store = database.store().await;
    let stale_continuous_contract = item_exchange_contract(46);
    let healthy_continuous_contract = item_exchange_contract(57);
    let restarted_delivery = Arc::new(RecordingDelivery {
        store: restarted_store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let restarted = ContractCollector::new(
        restarted_store.clone(),
        Arc::new(FakeEsi {
            regions: vec![stale_region, healthy_region],
            pages: HashMap::from([
                (
                    (stale_region, 1),
                    Ok(EsiResponse::fresh(
                        vec![
                            stale_recovery_contract.clone(),
                            stale_continuous_contract.clone(),
                        ],
                        expiring_page(1),
                    )),
                ),
                (
                    (healthy_region, 1),
                    Ok(EsiResponse::fresh(
                        vec![
                            healthy_prior.clone(),
                            healthy_listed_contract.clone(),
                            healthy_continuous_contract.clone(),
                        ],
                        expiring_page(1),
                    )),
                ),
            ]),
            items: HashMap::from([
                (
                    stale_recovery_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(3)], expiring_cache())),
                ),
                (
                    stale_continuous_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(5)], expiring_cache())),
                ),
                (
                    healthy_prior.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
                (
                    healthy_listed_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(4)], expiring_cache())),
                ),
                (
                    healthy_continuous_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(6)], expiring_cache())),
                ),
            ]),
        }),
    )
    .with_notifications(
        Arc::new(StaticShipGroups(HashMap::new())),
        restarted_delivery.clone(),
    )
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("continue both regions after restart");
    assert_eq!(
        restarted.regions,
        vec![
            CollectionOutcome::Complete {
                region_id: stale_region,
                observed_contracts: 2
            },
            CollectionOutcome::Complete {
                region_id: healthy_region,
                observed_contracts: 3
            },
        ]
    );
    assert_eq!(
        restarted
            .events
            .iter()
            .map(|event| (event.region_id, event.contract.contract_id, event.kind))
            .collect::<Vec<_>>(),
        vec![
            (
                stale_region,
                stale_continuous_contract.contract_id,
                ContractEventKind::Listed,
            ),
            (
                healthy_region,
                healthy_continuous_contract.contract_id,
                ContractEventKind::Listed,
            ),
        ]
    );
    assert_eq!(restarted_delivery.sent.lock().unwrap().len(), 2);

    let epoch_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("reconnect to inspect restarted regional batch evidence");
    let stale_epochs: Vec<i64> = sqlx::query_scalar("SELECT recovery_epoch_id FROM regional_observation_batches WHERE region_id = $1 ORDER BY id")
        .bind(stale_region)
        .fetch_all(&epoch_pool)
        .await
        .expect("read restarted stale regional batch epochs");
    let healthy_epochs: Vec<i64> = sqlx::query_scalar("SELECT recovery_epoch_id FROM regional_observation_batches WHERE region_id = $1 ORDER BY id")
        .bind(healthy_region)
        .fetch_all(&epoch_pool)
        .await
        .expect("read restarted healthy regional batch epochs");
    assert_eq!(stale_epochs.len(), 3);
    assert_eq!(healthy_epochs.len(), 3);
    assert_ne!(stale_epochs[0], stale_epochs[1]);
    assert_eq!(stale_epochs[1], stale_epochs[2]);
    assert!(healthy_epochs
        .windows(2)
        .all(|epochs| epochs[0] == epochs[1]));
    epoch_pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn http_esi_classifies_only_exact_public_contract_forbidden_bodies() {
    let accepted_server = OneShotHttpServer::start(
        403,
        &[("Cache-Control", "max-age=60")],
        r#"{"error":"Contract accepted by player, requires authorization"}"#,
    );
    let accepted_esi =
        HttpPublicContractEsi::with_base_url(&accepted_server.base_url, Duration::from_secs(1))
            .expect("construct accepted-player ESI client");
    assert!(matches!(
        accepted_esi
            .public_contract_items_probe(44, None)
            .await
            .expect("classify exact accepted-player response"),
        ContractItemProbe::AcceptedByPlayer(_)
    ));
    accepted_server.finish();

    let unavailable_server = OneShotHttpServer::start(
        403,
        &[("Cache-Control", "max-age=60")],
        r#"{"error":"Contract not public"}"#,
    );
    let unavailable_esi =
        HttpPublicContractEsi::with_base_url(&unavailable_server.base_url, Duration::from_secs(1))
            .expect("construct unavailable-contract ESI client");
    assert!(matches!(
        unavailable_esi
            .public_contract_items_probe(44, None)
            .await
            .expect("classify exact unavailable-contract response"),
        ContractItemProbe::NotPublic(_)
    ));
    unavailable_server.finish();

    let unrelated_server = OneShotHttpServer::start(
        403,
        &[("Cache-Control", "max-age=60")],
        r#"{"error":"forbidden for another reason"}"#,
    );
    let unrelated_esi =
        HttpPublicContractEsi::with_base_url(&unrelated_server.base_url, Duration::from_secs(1))
            .expect("construct unrelated-forbidden ESI client");
    assert!(unrelated_esi
        .public_contract_items_probe(44, None)
        .await
        .is_err());
    unrelated_server.finish();
}

#[tokio::test]
async fn accepted_player_evidence_confirms_only_before_contract_expiry() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut expired_contract = item_exchange_contract(44);
    expired_contract.date_expired = Utc::now() - chrono::Duration::minutes(1);
    let mut fresh_contract = item_exchange_contract(45);
    fresh_contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![expired_contract.clone(), fresh_contract.clone()],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    expired_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    fresh_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish public contract baseline");
    store
        .upsert_contract_subscription(&confirmed_ship_subscription(
            "sales",
            ContractEventKind::SaleConfirmed,
            ContractItemDirection::Offered,
            ContractEventAction::Post,
        ))
        .await
        .expect("persist sale subscription");
    let delivery = Arc::new(RecordingDelivery {
        store: store.clone(),
        sent: StdMutex::new(Vec::new()),
    });
    let report = ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: regional_esi(vec![], HashMap::new()),
            probes: StdMutex::new(vec![
                Ok(ContractItemProbe::AcceptedByPlayer(expiring_cache())),
                Ok(ContractItemProbe::AcceptedByPlayer(expiring_cache())),
            ]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_notifications(Arc::new(StaticShipGroups(HashMap::new())), delivery.clone())
    .collect_cycle()
    .await
    .expect("classify exact public contract evidence");

    assert_eq!(
        report
            .events
            .iter()
            .map(|event| (event.contract.contract_id, event.kind))
            .collect::<Vec<_>>(),
        vec![
            (expired_contract.contract_id, ContractEventKind::Expired),
            (fresh_contract.contract_id, ContractEventKind::SaleConfirmed),
        ]
    );
    assert_eq!(delivery.sent.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .contract_resolution_records()
            .await
            .expect("read terminal states")
            .iter()
            .map(|record| (record.contract_id, record.state))
            .collect::<Vec<_>>(),
        vec![
            (
                expired_contract.contract_id,
                ContractResolutionState::Expired
            ),
            (
                fresh_contract.contract_id,
                ContractResolutionState::AcceptanceConfirmed,
            ),
        ]
    );
    database.destroy().await;
}

#[tokio::test]
async fn recovery_epoch_tracks_summary_presence_through_a_failed_manifest_then_disappearance() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut contract = item_exchange_contract(44);
    contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                contract.contract_id,
                Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
            )]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish the initial public contract baseline");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to age the initial regional batch");
    sqlx::query(
        "UPDATE regional_observation_batches SET attempt_started_at = now() - interval '2 hours', completed_at = now() - interval '2 hours' WHERE region_id = 10000002",
    )
    .execute(&pool)
    .await
    .expect("create a regional recovery gap");
    pool.close().await;

    let recovery = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(vec![contract.clone()], expiring_page(1))),
            )]),
            items: HashMap::from([(
                contract.contract_id,
                Err(EsiError::retryable("manifest unavailable", None)),
            )]),
        }),
    )
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("retain summary-only presence during the recovery baseline");
    assert_eq!(
        recovery.regions,
        vec![CollectionOutcome::RecoveryBaselineEstablished {
            region_id: 10_000_002,
        }]
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect recovery-epoch presence");
    let recovery_presence_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM regional_epoch_contract_presence WHERE region_id = 10000002 AND recovery_epoch_id = (SELECT current_recovery_epoch_id FROM regional_collection_metadata WHERE region_id = 10000002)",
    )
    .fetch_one(&pool)
    .await
    .expect("read summary-level epoch presence");
    assert_eq!(recovery_presence_count, 1);
    pool.close().await;

    let disappearance = ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                )]),
                items: HashMap::new(),
            },
            probes: StdMutex::new(vec![Ok(ContractItemProbe::NotFound(expiring_cache()))]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .with_recovery_gap(chrono::Duration::minutes(30))
    .collect_cycle()
    .await
    .expect("detect the disappearance from summary-level epoch presence");
    assert_eq!(
        disappearance
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![ContractEventKind::ClosedOutcomeUnknown]
    );

    database.destroy().await;
}

#[tokio::test]
async fn acceptance_evidence_preserves_no_content_and_accepted_player_response_kinds() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let mut no_content_contract = item_exchange_contract(44);
    no_content_contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    let mut accepted_player_contract = item_exchange_contract(45);
    accepted_player_contract.date_expired = Utc::now() + chrono::Duration::hours(1);
    ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Ok(EsiResponse::fresh(
                    vec![
                        no_content_contract.clone(),
                        accepted_player_contract.clone(),
                    ],
                    expiring_page(1),
                )),
            )]),
            items: HashMap::from([
                (
                    no_content_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(1)], expiring_cache())),
                ),
                (
                    accepted_player_contract.contract_id,
                    Ok(EsiResponse::fresh(vec![offered_ship(2)], expiring_cache())),
                ),
            ]),
        }),
    )
    .collect_cycle()
    .await
    .expect("establish public contract summaries before resolution");

    ContractCollector::new(
        store.clone(),
        Arc::new(ResolutionEsi {
            inner: FakeEsi {
                regions: vec![10_000_002],
                pages: HashMap::from([(
                    (10_000_002, 1),
                    Ok(EsiResponse::fresh(vec![], expiring_page(1))),
                )]),
                items: HashMap::new(),
            },
            probes: StdMutex::new(vec![
                Ok(ContractItemProbe::NoContent(expiring_cache())),
                Ok(ContractItemProbe::AcceptedByPlayer(expiring_cache())),
            ]),
            probe_calls: StdMutex::new(Vec::new()),
        }),
    )
    .collect_cycle()
    .await
    .expect("record both pre-expiry acceptance evidence kinds");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect durable acceptance provenance");
    let evidence_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT detail ->> 'response' FROM contract_lifecycle_evidence WHERE evidence_kind = 'acceptance_confirmed' ORDER BY contract_id",
    )
    .fetch_all(&pool)
    .await
    .expect("read durable acceptance response kinds");
    assert_eq!(
        evidence_kinds,
        vec![
            "204_no_content".to_string(),
            "403_accepted_by_player".to_string(),
        ]
    );
    pool.close().await;
    database.destroy().await;
}

#[tokio::test]
async fn inconclusive_attempt_evidence_rolls_back_when_failure_lifecycle_insert_is_rejected() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to reject the final inconclusive lifecycle insert");
    sqlx::query("ALTER TABLE contract_collection_failures ADD CONSTRAINT reject_inconclusive_attempt_lifecycle CHECK (failure_kind <> 'inconclusive_observation')")
        .execute(&pool)
        .await
        .expect("install deterministic rollback failure");
    pool.close().await;

    let error = ContractCollector::new(
        store.clone(),
        Arc::new(FakeEsi {
            regions: vec![10_000_002],
            pages: HashMap::from([(
                (10_000_002, 1),
                Err(EsiError::retryable("regional page unavailable", None)),
            )]),
            items: HashMap::new(),
        }),
    )
    .collect_cycle()
    .await
    .expect_err("the injected lifecycle failure aborts the regional attempt");
    assert!(error
        .to_string()
        .contains("reject_inconclusive_attempt_lifecycle"));

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to verify the failed attempt left no partial evidence");
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT \
            (SELECT count(*) FROM regional_observations WHERE region_id = 10000002), \
            (SELECT count(*) FROM regional_observation_batches WHERE region_id = 10000002), \
            (SELECT count(*) FROM contract_collection_failures WHERE region_id = 10000002)",
    )
    .fetch_one(&pool)
    .await
    .expect("count attempt evidence after rollback");
    assert_eq!(counts, (0, 0, 0));
    pool.close().await;
    database.destroy().await;
}
