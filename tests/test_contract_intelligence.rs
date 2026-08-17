use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use killbot_rust::config::{self, AppConfig, AppState};
use killbot_rust::contract_intelligence::{
    available_contract_store, collection_retry_delay, new_contract_store_handle,
    spawn_contract_collection_loop_with_notifications, AppStateContractPingLimiter, CacheMetadata,
    CollectionOutcome, ContractCollectionStore, ContractCollector, ContractContextLimiter,
    ContractContextRequirements, ContractContextValue, ContractDelivery, ContractDeliveryClock,
    ContractDeliveryError, ContractEmbedContext, ContractEventAction, ContractEventActions,
    ContractEventKind, ContractFilter, ContractFilterCondition, ContractFilterNode,
    ContractItemDirection, ContractItemProbe, ContractObservationContext, ContractPingLimiter,
    ContractPingType, ContractResolutionState, ContractSubscription, DeliveryFailureKind,
    DeliveryRecord, DeliveryStatus, EsiError, EsiResponse, HttpPublicContractEsi,
    PreparedContractDelivery, PublicContract, PublicContractEsi, PublicContractItem,
    ShipGroupLookup, ShipGroupResolver, SolarSystemPosition,
};
use killbot_rust::esi::EsiClient;
use killbot_rust::feed::{FeedError, KillmailFeed};
use killbot_rust::models::{ZkData, ZkDataNoEsi};
use killbot_rust::pipeline::{run_producer, ProcessedResult};
use moka::future::Cache;
use serde_json::Value;
use sha2::{Digest, Sha384};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Row};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify, Semaphore};
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
async fn wire_empty_success_body_is_retried_without_invalidating_the_region() {
    let database = TemporaryDatabase::new().await;
    let store = database.store().await;
    let server = SequenceHttpServer::start(vec![
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60")],
            body: "[10000002]",
        },
        WireReply {
            status: 200,
            headers: vec![("Cache-Control", "max-age=60"), ("X-Pages", "1")],
            body: ESI_PUBLIC_CONTRACT_SUMMARY_FIXTURE,
        },
        WireReply {
            status: 200,
            headers: vec![
                ("Cache-Control", "max-age=60"),
                ("ETag", "\"items-empty\""),
                ("X-ESI-Error-Limit-Remain", "100"),
                ("X-ESI-Error-Limit-Reset", "60"),
            ],
            body: "",
        },
        WireReply {
            status: 200,
            headers: vec![
                ("Cache-Control", "max-age=60"),
                ("ETag", "\"items-v1\""),
                ("X-ESI-Error-Limit-Remain", "99"),
                ("X-ESI-Error-Limit-Reset", "60"),
            ],
            body: ESI_PUBLIC_CONTRACT_ITEMS_FIXTURE,
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
            body: r#"[{"id":90000001,"name":"Issuer"},{"id":98000001,"name":"Issuer Corp"},{"id":99000111,"name":"Issuer Alliance"},{"id":48473,"name":"Blueprint"}]"#,
        },
    ]);
    let esi = HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
        .expect("construct HTTP ESI client");
    let collector = ContractCollector::new(store.clone(), Arc::new(esi));

    let first = collector
        .collect_cycle()
        .await
        .expect("transient empty body is retried within the collection cycle");
    assert_eq!(
        first.regions,
        vec![CollectionOutcome::BaselineEstablished {
            region_id: 10_000_002,
        }]
    );
    let request_count = server.requests.lock().unwrap().len();
    assert_eq!(request_count, 10);
    assert!(server.requests.lock().unwrap()[2].contains("/contracts/public/items/234057619/"));
    assert!(server.requests.lock().unwrap()[3].contains("/contracts/public/items/234057619/"));

    let second = collector
        .collect_cycle()
        .await
        .expect("successful retry representation remains cached");
    assert_eq!(
        second.regions,
        vec![CollectionOutcome::Complete {
            region_id: 10_000_002,
            observed_contracts: 1,
        }]
    );
    assert_eq!(server.requests.lock().unwrap().len(), request_count);
    assert_eq!(
        store
            .region_state(10_000_002)
            .await
            .expect("region state")
            .expect("stored region")
            .complete_observations,
        2
    );

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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect to inspect the limiter-boundary batch");
    let batch = sqlx::query("SELECT pages_attempted, pages_observed, observed_contract_count, resolved_contract_count, manifest_failure_count, consistency_evidence, failure_classification, failure_detail, retry_after FROM regional_observation_batches WHERE region_id = 10000002")
        .fetch_one(&pool)
        .await
        .expect("retain the limiter-boundary attempt");
    assert_eq!(batch.get::<i32, _>("pages_attempted"), 1);
    assert_eq!(batch.get::<i32, _>("pages_observed"), 0);
    assert_eq!(batch.get::<i64, _>("observed_contract_count"), 0);
    assert_eq!(batch.get::<i64, _>("resolved_contract_count"), 0);
    assert_eq!(batch.get::<i64, _>("manifest_failure_count"), 0);
    assert_eq!(batch.get::<String, _>("failure_classification"), "esi");
    assert!(batch
        .get::<String, _>("failure_detail")
        .contains("persisted global ESI limiter"));
    assert!(batch
        .get::<Option<DateTime<Utc>>, _>("retry_after")
        .is_some());
    let evidence: Value = batch.get("consistency_evidence");
    assert!(evidence["failure_response"]["retry_after"]
        .as_str()
        .is_some());
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
            body: "",
        },
        WireReply {
            status: 200,
            headers: vec![("X-ESI-Error-Limit-Remain", "100")],
            body: "",
        },
    ]);
    let esi = ExhaustedWireManifestEsi {
        http: HttpPublicContractEsi::with_base_url(&server.base_url, Duration::from_secs(1))
            .expect("construct HTTP ESI client"),
        contracts: vec![first_contract.clone(), pending_contract.clone()],
        manifests: HashMap::from([(
            44,
            EsiResponse::fresh(vec![offered_ship(1)], expiring_cache()),
        )]),
        unavailable_contract_id: 45,
    };

    let baseline = ContractCollector::new(store.clone(), Arc::new(esi))
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
    assert_eq!(server.requests.lock().unwrap().len(), 3);
    server.finish();
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

    let recovered = ContractCollector::new(
        store.clone(),
        Arc::new(regional_esi(
            vec![first_contract, pending_contract],
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
    .collect_cycle()
    .await
    .expect("a restarted collector backfills the pending baseline manifest");
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
    validation_pool.close().await;

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

#[tokio::test]
async fn regional_observation_batch_migration_applies_to_clean_and_current_databases() {
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
    assert_eq!(clean_migration_count, 13);
    let batch_table_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind("regional_observation_batches")
        .fetch_one(&clean_pool)
        .await
        .expect("read clean regional batch table");
    assert!(
        batch_table_exists,
        "clean migration creates regional batch ledger"
    );
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
