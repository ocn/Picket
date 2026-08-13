use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use killbot_rust::contract_intelligence::{
    CacheMetadata, CollectionOutcome, ContractCollectionStore, ContractCollector, EsiError,
    EsiResponse, PublicContract, PublicContractEsi, PublicContractItem,
};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use url::Url;

static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TemporaryDatabase {
    admin_url: String,
    database_name: String,
    url: String,
}

impl TemporaryDatabase {
    async fn new() -> Self {
        let admin_url =
            std::env::var("CONTRACT_TEST_DATABASE_URL").expect("test database URL is present");
        let sequence = DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "contract_intelligence_test_{}_{}",
            std::process::id(),
            sequence
        );
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("connect to contract test PostgreSQL");
        sqlx::query(&format!("CREATE DATABASE {database_name}"))
            .execute(&admin)
            .await
            .expect("create temporary contract test database");
        admin.close().await;

        let mut url = Url::parse(&admin_url).expect("valid CONTRACT_TEST_DATABASE_URL");
        url.set_path(&format!("/{database_name}"));
        Self {
            admin_url,
            database_name,
            url: url.to_string(),
        }
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

fn expiring_metadata(etag: &str, expected_pages: Option<u32>) -> CacheMetadata {
    CacheMetadata {
        etag: Some(etag.to_string()),
        expires_at: Some(Utc::now()),
        expected_pages,
        error_limit_remain: None,
        error_limit_reset: None,
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

fn item_exchange_contract(contract_id: i64) -> PublicContract {
    PublicContract {
        contract_id,
        availability: Some("public".to_string()),
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

fn contract_test_database_is_configured() -> bool {
    std::env::var("CONTRACT_TEST_DATABASE_URL").is_ok()
}

fn expiring_page(expected_pages: u32) -> CacheMetadata {
    CacheMetadata::page_cached_for_seconds(expected_pages, 0)
}

fn expiring_cache() -> CacheMetadata {
    CacheMetadata::cached_for_seconds(0)
}

#[tokio::test]
async fn complete_first_observation_establishes_a_silent_regional_baseline() {
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
                    },
                    PublicContractItem {
                        record_id: 2,
                        type_id: 34,
                        quantity: 500,
                        is_included: false,
                        item_id: Some(1_002),
                        raw_quantity: None,
                        is_singleton: None,
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
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
async fn duplicated_contract_ids_across_pages_are_inconclusive() {
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
async fn unchanged_contracts_reuse_facts_and_manifests_while_extending_one_presence_interval() {
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
    tokio::time::sleep(Duration::from_millis(2)).await;
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

    database.destroy().await;
}

#[tokio::test]
async fn stale_cached_responses_are_conditionally_revalidated_without_losing_pagination_metadata() {
    if !contract_test_database_is_configured() {
        eprintln!("skipping: CONTRACT_TEST_DATABASE_URL is not configured");
        return;
    }
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
