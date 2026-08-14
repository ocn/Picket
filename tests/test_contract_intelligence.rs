use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use killbot_rust::contract_intelligence::{
    available_contract_store, new_contract_store_handle,
    spawn_contract_collection_loop_with_notifications, CacheMetadata, CollectionOutcome,
    ContractCollectionStore, ContractCollector, ContractDelivery, ContractDeliveryError,
    ContractEventAction, ContractEventActions, ContractEventKind, ContractFilter,
    ContractItemDirection, ContractSubscription, DeliveryRecord, DeliveryStatus, EsiError,
    EsiResponse, HttpPublicContractEsi, PreparedContractDelivery, PublicContract,
    PublicContractEsi, PublicContractItem, ShipGroupLookup, ShipGroupResolver,
};
use sha2::{Digest, Sha384};
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread::JoinHandle;
use std::time::Duration;
use url::Url;

static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    handle: JoinHandle<()>,
}

impl SequenceHttpServer {
    fn start(replies: Vec<WireReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake ESI HTTP listener");
        let address = listener.local_addr().expect("fake ESI listener address");
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let recorded_requests = requests.clone();
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

#[derive(Default)]
struct FakeEsi {
    regions: Vec<i64>,
    pages: HashMap<(i64, u32), Result<EsiResponse<Vec<PublicContract>>, EsiError>>,
    items: HashMap<i64, Result<EsiResponse<Vec<PublicContractItem>>, EsiError>>,
}

#[derive(Default)]
struct ConditionalEsi {
    received_etags: StdMutex<Vec<Option<String>>>,
}

struct StopsAfterRateBoundaryEsi {
    retry_after: chrono::DateTime<Utc>,
    calls: StdMutex<Vec<String>>,
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

struct StaticShipGroups(HashMap<i64, i64>);

#[async_trait]
impl ShipGroupResolver for StaticShipGroups {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup {
        ShipGroupLookup::Resolved(self.0.get(&type_id).copied())
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

struct NoopDelivery;

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
impl ContractDelivery for NoopDelivery {
    async fn send(
        &self,
        _delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        Ok("ignored".to_string())
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
        filter: ContractFilter {
            events: vec![ContractEventKind::Listed],
            item_direction: direction,
            type_ids,
            ship_group_ids,
        },
        event_actions: ContractEventActions { listed },
    }
}

fn item_exchange_contract(contract_id: i64) -> PublicContract {
    PublicContract {
        contract_id,
        availability: Some("public".to_string()),
        buyout: None,
        collateral: Some(0.0),
        contract_type: "item_exchange".to_string(),
        date_expired: Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap(),
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

#[tokio::test]
async fn http_esi_accepts_a_public_contract_when_false_for_corporation_is_omitted() {
    let server = OneShotHttpServer::start(
        200,
        &[("X-Pages", "1"), ("Cache-Control", "max-age=60")],
        r#"[{"availability":"public","buyout":2500000000.0,"collateral":750000000.0,"contract_id":44,"date_expired":"2026-08-14T12:00:00Z","date_issued":"2026-08-13T12:00:00Z","days_to_complete":0,"issuer_corporation_id":98000001,"issuer_id":90000001,"price":1500000000.0,"reward":0.0,"start_location_id":60003760,"type":"item_exchange","volume":115000.0}]"#,
    );
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");

    let response = esi
        .public_contracts_page(10_000_002, 1, None)
        .await
        .expect("decode public contract HTTP response");

    let contract = response.value.expect("response body").remove(0);
    assert!(!contract.for_corporation);
    assert_eq!(contract.buyout, Some(2_500_000_000.0));
    assert_eq!(contract.collateral, Some(750_000_000.0));
    server.finish();
}

#[tokio::test]
async fn http_esi_preserves_public_blueprint_item_facts() {
    let server = OneShotHttpServer::start(
        200,
        &[("Cache-Control", "max-age=60")],
        r#"[{"is_blueprint_copy":true,"is_included":true,"item_id":1001,"material_efficiency":10,"quantity":1,"record_id":1,"runs":3,"time_efficiency":20,"type_id":587}]"#,
    );
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");

    let response = esi
        .public_contract_items(44, None)
        .await
        .expect("decode public contract item HTTP response");

    let item = response.value.expect("response body").remove(0);
    assert_eq!(item.is_blueprint_copy, Some(true));
    assert_eq!(item.material_efficiency, Some(10));
    assert_eq!(item.runs, Some(3));
    assert_eq!(item.time_efficiency, Some(20));
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
    assert!(server.requests.lock().unwrap()[5]
        .to_ascii_lowercase()
        .contains("if-none-match: \"regions-v1\""));
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
        .expect("rate boundary creates an inconclusive regional observation");

    assert!(report.retry_after.is_some());
    assert!(matches!(
        report.regions.as_slice(),
        [CollectionOutcome::Inconclusive {
            region_id: 10_000_002,
            ..
        }]
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
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

    database.destroy().await;
}

#[tokio::test]
async fn missing_manifest_is_inconclusive_and_does_not_create_a_presence_interval() {
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
async fn contract_database_retries_do_not_block_an_unrelated_runtime_task() {
    let collection = tokio::spawn(
        killbot_rust::contract_intelligence::run_contract_collection_loop(
            "postgres://killbot_contracts:killbot_contracts@127.0.0.1:1/never".to_string(),
            Duration::from_secs(60),
            Duration::from_millis(100),
        ),
    );

    let unrelated_task = tokio::time::timeout(Duration::from_millis(50), async { 7_u8 }).await;

    assert_eq!(
        unrelated_task.expect("contract retry must not block runtime"),
        7
    );
    assert!(!collection.is_finished());
    collection.abort();
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
        filter: ContractFilter {
            ship_group_ids: vec![25],
            ..original.filter.clone()
        },
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
            .fields
            .iter()
            .any(|field| field.name == "Contract Address" && field.value == "contract:0//45")
            && delivery
                .message
                .fields
                .iter()
                .any(|field| field.name == "Issuer" && field.value == "90000001")
            && delivery.message.description.as_deref()
                == Some("@\u{200b}everyone offers <@\u{200b}1234> a ship")
    }));
    drop(sent);
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
    assert!(message.title.chars().count() + field_name_lengths + field_value_lengths <= 6000);
    assert!(message.fields.iter().any(|field| {
        field.name == "Offered Item"
            && field.value.contains("Type 1000000 ×1")
            && field.value.contains("+232 more")
    }));
    assert!(message
        .fields
        .iter()
        .any(|field| { field.name == "Contract Address" && field.value == "contract:0//45" }));
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
