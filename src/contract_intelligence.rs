use crate::discord_bot::SHIP_GROUP_PRIORITY;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{HeaderMap, ETAG, EXPIRES, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use serde::de::{DeserializeOwned, Error as DeError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
const CONTRACT_COLLECTOR_USER_AGENT: &str = "killbot-rust Global Public Contract Intelligence";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PublicContract {
    pub contract_id: i64,
    #[serde(default)]
    pub availability: Option<String>,
    #[serde(default)]
    pub buyout: Option<f64>,
    #[serde(default)]
    pub collateral: Option<f64>,
    #[serde(rename = "type")]
    pub contract_type: String,
    pub date_expired: DateTime<Utc>,
    pub date_issued: DateTime<Utc>,
    pub days_to_complete: i32,
    #[serde(default)]
    pub end_location_id: Option<i64>,
    #[serde(default)]
    pub for_corporation: bool,
    pub issuer_corporation_id: i64,
    pub issuer_id: i64,
    pub price: f64,
    pub reward: f64,
    pub start_location_id: i64,
    #[serde(default)]
    pub title: Option<String>,
    pub volume: f64,
}

impl PublicContract {
    fn is_public_item_exchange(&self) -> bool {
        self.contract_type == "item_exchange"
            && self
                .availability
                .as_deref()
                .map(|availability| availability == "public")
                .unwrap_or(true)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PublicContractItem {
    pub record_id: i64,
    pub type_id: i64,
    pub quantity: i64,
    pub is_included: bool,
    #[serde(default)]
    pub item_id: Option<i64>,
    #[serde(default)]
    pub raw_quantity: Option<i64>,
    #[serde(default)]
    pub is_singleton: Option<bool>,
    #[serde(default)]
    pub is_blueprint_copy: Option<bool>,
    #[serde(default)]
    pub material_efficiency: Option<i64>,
    #[serde(default)]
    pub runs: Option<i64>,
    #[serde(default)]
    pub time_efficiency: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CacheMetadata {
    pub etag: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_modified: Option<DateTime<Utc>>,
    pub expected_pages: Option<u32>,
    pub error_limit_remain: Option<i64>,
    pub error_limit_reset: Option<i64>,
    pub rate_limit_group: Option<String>,
    pub rate_limit_limit: Option<String>,
    pub rate_limit_remaining: Option<i64>,
    pub rate_limit_used: Option<i64>,
    pub retry_after: Option<DateTime<Utc>>,
}

impl CacheMetadata {
    pub fn cached_for_seconds(seconds: i64) -> Self {
        Self {
            etag: None,
            expires_at: Some(Utc::now() + ChronoDuration::seconds(seconds)),
            last_modified: None,
            expected_pages: None,
            error_limit_remain: None,
            error_limit_reset: None,
            rate_limit_group: None,
            rate_limit_limit: None,
            rate_limit_remaining: None,
            rate_limit_used: None,
            retry_after: None,
        }
    }

    pub fn page_cached_for_seconds(expected_pages: u32, seconds: i64) -> Self {
        Self {
            expected_pages: Some(expected_pages),
            ..Self::cached_for_seconds(seconds)
        }
    }

    fn is_fresh(&self) -> bool {
        self.expires_at
            .map(|expires_at| expires_at > Utc::now())
            .unwrap_or(false)
    }

    fn retry_is_active(&self) -> bool {
        self.retry_after
            .map(|retry_after| retry_after > Utc::now())
            .unwrap_or(false)
    }

    fn collection_pause_until(&self) -> Option<DateTime<Utc>> {
        self.retry_after.or_else(|| {
            (self.rate_limit_remaining.unwrap_or(1) <= 0)
                .then(|| {
                    self.rate_limit_limit
                        .as_deref()
                        .and_then(rate_limit_window_seconds)
                })
                .flatten()
                .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
        })
    }
}

#[derive(Clone, Debug)]
pub struct EsiResponse<T> {
    pub value: Option<T>,
    pub metadata: CacheMetadata,
    pub not_modified: bool,
}

impl<T> EsiResponse<T> {
    pub fn fresh(value: T, metadata: CacheMetadata) -> Self {
        Self {
            value: Some(value),
            metadata,
            not_modified: false,
        }
    }

    pub fn not_modified(metadata: CacheMetadata) -> Self {
        Self {
            value: None,
            metadata,
            not_modified: true,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ContractItemProbe {
    Available(EsiResponse<Vec<PublicContractItem>>),
    Accepted(CacheMetadata),
}

impl ContractItemProbe {
    fn metadata(&self) -> &CacheMetadata {
        match self {
            Self::Available(response) => &response.metadata,
            Self::Accepted(metadata) => metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractContextValue<T> {
    Resolved(T),
    DefinitivelyAbsent,
    Indeterminate,
}

#[derive(Clone, Debug)]
pub struct EsiError {
    message: String,
    status: Option<StatusCode>,
    retry_after: Option<DateTime<Utc>>,
    metadata: CacheMetadata,
}

impl EsiError {
    pub fn retryable(message: impl Into<String>, retry_after: Option<DateTime<Utc>>) -> Self {
        Self {
            message: message.into(),
            status: None,
            retry_after,
            metadata: CacheMetadata {
                retry_after,
                ..CacheMetadata::cached_for_seconds(0)
            },
        }
    }

    fn from_metadata(
        message: impl Into<String>,
        status: Option<StatusCode>,
        metadata: CacheMetadata,
    ) -> Self {
        Self {
            message: message.into(),
            status,
            retry_after: metadata.retry_after,
            metadata,
        }
    }

    fn is_not_found(&self) -> bool {
        self.status == Some(StatusCode::NOT_FOUND)
    }
}

impl Display for EsiError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for EsiError {}

#[async_trait]
pub trait PublicContractEsi: Send + Sync {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError>;
    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError>;
    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError>;
    async fn public_contract_items_probe(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        Ok(ContractItemProbe::Available(
            self.public_contract_items(contract_id, etag).await?,
        ))
    }

    async fn observed_contract_solar_system(
        &self,
        contract: &PublicContract,
    ) -> Result<EsiResponse<ContractContextValue<i64>>, EsiError> {
        let response = self
            .observed_contract_context(
                contract,
                ContractContextRequirements {
                    solar_system: true,
                    ..ContractContextRequirements::default()
                },
            )
            .await?;
        Ok(EsiResponse {
            value: response.value.map(|context| {
                context
                    .solar_system_id
                    .map(ContractContextValue::Resolved)
                    .unwrap_or_else(|| match context.solar_system_resolution {
                        ContractContextResolution::DefinitivelyAbsent => {
                            ContractContextValue::DefinitivelyAbsent
                        }
                        ContractContextResolution::Indeterminate
                        | ContractContextResolution::Resolved
                        | ContractContextResolution::TemporarilyUnavailable => {
                            ContractContextValue::Indeterminate
                        }
                    })
            }),
            metadata: response.metadata,
            not_modified: response.not_modified,
        })
    }

    async fn observed_solar_system_security(
        &self,
        contract: &PublicContract,
        _solar_system_id: i64,
    ) -> Result<EsiResponse<ContractContextValue<f64>>, EsiError> {
        let response = self
            .observed_contract_context(
                contract,
                ContractContextRequirements {
                    security_status: true,
                    ..ContractContextRequirements::default()
                },
            )
            .await?;
        Ok(EsiResponse {
            value: response.value.map(|context| {
                context
                    .security_status
                    .filter(|security_status| security_status.is_finite())
                    .map(ContractContextValue::Resolved)
                    .unwrap_or_else(|| match context.security_status_resolution {
                        ContractContextResolution::DefinitivelyAbsent => {
                            ContractContextValue::DefinitivelyAbsent
                        }
                        ContractContextResolution::Indeterminate
                        | ContractContextResolution::Resolved
                        | ContractContextResolution::TemporarilyUnavailable => {
                            ContractContextValue::Indeterminate
                        }
                    })
            }),
            metadata: response.metadata,
            not_modified: response.not_modified,
        })
    }

    async fn observed_issuer_affiliation(
        &self,
        contract: &PublicContract,
    ) -> Result<EsiResponse<ContractContextValue<i64>>, EsiError> {
        let response = self
            .observed_contract_context(
                contract,
                ContractContextRequirements {
                    observed_affiliation: true,
                    ..ContractContextRequirements::default()
                },
            )
            .await?;
        Ok(EsiResponse {
            value: response.value.map(|context| {
                context
                    .observed_affiliation_alliance_id
                    .map(ContractContextValue::Resolved)
                    .unwrap_or_else(|| match context.observed_affiliation_alliance_resolution {
                        ContractContextResolution::DefinitivelyAbsent => {
                            ContractContextValue::DefinitivelyAbsent
                        }
                        ContractContextResolution::Indeterminate
                        | ContractContextResolution::Resolved
                        | ContractContextResolution::TemporarilyUnavailable => {
                            ContractContextValue::Indeterminate
                        }
                    })
            }),
            metadata: response.metadata,
            not_modified: response.not_modified,
        })
    }

    async fn observed_contract_context(
        &self,
        _contract: &PublicContract,
        _requirements: ContractContextRequirements,
    ) -> Result<EsiResponse<ContractObservationContext>, EsiError> {
        Ok(EsiResponse::fresh(
            ContractObservationContext::default(),
            CacheMetadata::cached_for_seconds(0),
        ))
    }
}

pub struct HttpPublicContractEsi {
    client: Client,
    base_url: String,
}

impl HttpPublicContractEsi {
    pub fn new(timeout: Duration) -> Result<Self, reqwest::Error> {
        Self::with_base_url("https://esi.evetech.net/latest/", timeout)
    }

    pub fn with_base_url(
        base_url: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .user_agent(CONTRACT_COLLECTOR_USER_AGENT)
                .build()?,
            base_url: base_url.into(),
        })
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        etag: Option<&str>,
    ) -> Result<EsiResponse<T>, EsiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(url);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let response = request
            .send()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        let metadata = cache_metadata(response.headers());
        if response.status() == StatusCode::NOT_MODIFIED {
            return Ok(EsiResponse::not_modified(metadata));
        }
        if !response.status().is_success() {
            let retry_after = metadata.retry_after.or_else(|| {
                if matches!(response.status(), StatusCode::TOO_MANY_REQUESTS)
                    || response.status().as_u16() == 420
                {
                    metadata
                        .error_limit_reset
                        .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
                } else {
                    None
                }
            });
            let mut metadata = metadata;
            metadata.retry_after = retry_after;
            return Err(EsiError::from_metadata(
                format!("ESI returned {}", response.status()),
                Some(response.status()),
                metadata,
            ));
        }
        let value = response
            .json()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        Ok(EsiResponse::fresh(value, metadata))
    }

    async fn public_contract_items_probe(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        let url = format!("{}contracts/public/items/{contract_id}/", self.base_url);
        let mut request = self.client.get(url);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let response = request
            .send()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        let metadata = cache_metadata(response.headers());
        match response.status() {
            StatusCode::NO_CONTENT => Ok(ContractItemProbe::Accepted(metadata)),
            StatusCode::NOT_MODIFIED => Ok(ContractItemProbe::Available(
                EsiResponse::not_modified(metadata),
            )),
            status if status.is_success() => {
                let value = response
                    .json()
                    .await
                    .map_err(|error| EsiError::retryable(error.to_string(), None))?;
                Ok(ContractItemProbe::Available(EsiResponse::fresh(
                    value, metadata,
                )))
            }
            status => {
                let retry_after = metadata.retry_after.or_else(|| {
                    if matches!(status, StatusCode::TOO_MANY_REQUESTS) || status.as_u16() == 420 {
                        metadata
                            .error_limit_reset
                            .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
                    } else {
                        None
                    }
                });
                let mut metadata = metadata;
                metadata.retry_after = retry_after;
                Err(EsiError::from_metadata(
                    format!("ESI returned {status}"),
                    Some(status),
                    metadata,
                ))
            }
        }
    }
}

#[async_trait]
impl PublicContractEsi for HttpPublicContractEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.get("universe/regions/", etag).await
    }

    async fn public_contracts_page(
        &self,
        region_id: i64,
        page: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContract>>, EsiError> {
        self.get(&format!("contracts/public/{region_id}/?page={page}"), etag)
            .await
    }

    async fn public_contract_items(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Vec<PublicContractItem>>, EsiError> {
        self.get(&format!("contracts/public/items/{contract_id}/"), etag)
            .await
    }

    async fn public_contract_items_probe(
        &self,
        contract_id: i64,
        etag: Option<&str>,
    ) -> Result<ContractItemProbe, EsiError> {
        HttpPublicContractEsi::public_contract_items_probe(self, contract_id, etag).await
    }

    async fn observed_contract_solar_system(
        &self,
        contract: &PublicContract,
    ) -> Result<EsiResponse<ContractContextValue<i64>>, EsiError> {
        #[derive(Deserialize)]
        struct Station {
            system_id: i64,
        }

        match u32::try_from(contract.start_location_id) {
            Ok(location_id) if (30_000_000..33_000_000).contains(&location_id) => {
                Ok(EsiResponse::fresh(
                    ContractContextValue::Resolved(i64::from(location_id)),
                    CacheMetadata::cached_for_seconds(0),
                ))
            }
            Ok(location_id) => match self
                .get::<Station>(&format!("universe/stations/{location_id}/"), None)
                .await
            {
                Ok(response) => Ok(EsiResponse {
                    value: response
                        .value
                        .map(|station| ContractContextValue::Resolved(station.system_id)),
                    metadata: response.metadata,
                    not_modified: response.not_modified,
                }),
                Err(error) if error.is_not_found() => Ok(EsiResponse::fresh(
                    ContractContextValue::Indeterminate,
                    error.metadata,
                )),
                Err(error) => Err(error),
            },
            Err(_) => Ok(EsiResponse::fresh(
                ContractContextValue::Indeterminate,
                CacheMetadata::cached_for_seconds(0),
            )),
        }
    }

    async fn observed_solar_system_security(
        &self,
        _contract: &PublicContract,
        solar_system_id: i64,
    ) -> Result<EsiResponse<ContractContextValue<f64>>, EsiError> {
        #[derive(Deserialize)]
        struct SolarSystem {
            security_status: f64,
        }

        match self
            .get::<SolarSystem>(&format!("universe/systems/{solar_system_id}/"), None)
            .await
        {
            Ok(response) => Ok(EsiResponse {
                value: response.value.map(|system| {
                    if system.security_status.is_finite() {
                        ContractContextValue::Resolved(system.security_status)
                    } else {
                        ContractContextValue::Indeterminate
                    }
                }),
                metadata: response.metadata,
                not_modified: response.not_modified,
            }),
            Err(error) if error.is_not_found() => Ok(EsiResponse::fresh(
                ContractContextValue::Indeterminate,
                error.metadata,
            )),
            Err(error) => Err(error),
        }
    }

    async fn observed_issuer_affiliation(
        &self,
        contract: &PublicContract,
    ) -> Result<EsiResponse<ContractContextValue<i64>>, EsiError> {
        #[derive(Deserialize)]
        struct Affiliation {
            alliance_id: Option<i64>,
        }

        let response = self
            .client
            .post(format!("{}characters/affiliation/", self.base_url))
            .json(&[contract.issuer_id])
            .send()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        let mut metadata = cache_metadata(response.headers());
        if !response.status().is_success() {
            let status = response.status();
            metadata.retry_after = metadata.retry_after.or_else(|| {
                if matches!(status, StatusCode::TOO_MANY_REQUESTS) || status.as_u16() == 420 {
                    metadata
                        .error_limit_reset
                        .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
                } else {
                    None
                }
            });
            return Err(EsiError::from_metadata(
                format!("ESI returned {status}"),
                Some(status),
                metadata,
            ));
        }
        let affiliations = response
            .json::<Vec<Affiliation>>()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        let value = affiliations.into_iter().next().map_or(
            ContractContextValue::Indeterminate,
            |affiliation| match affiliation.alliance_id {
                Some(alliance_id) => ContractContextValue::Resolved(alliance_id),
                None => ContractContextValue::DefinitivelyAbsent,
            },
        );
        Ok(EsiResponse::fresh(value, metadata))
    }
}

fn cache_metadata(headers: &HeaderMap) -> CacheMetadata {
    let mut metadata = CacheMetadata {
        etag: headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        expires_at: headers
            .get(EXPIRES)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_http_time),
        last_modified: headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_http_time),
        expected_pages: header_number(headers, "x-pages").and_then(|value| value.try_into().ok()),
        error_limit_remain: header_number(headers, "x-esi-error-limit-remain"),
        error_limit_reset: header_number(headers, "x-esi-error-limit-reset"),
        rate_limit_group: headers
            .get("x-ratelimit-group")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        rate_limit_limit: headers
            .get("x-ratelimit-limit")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        rate_limit_remaining: header_number(headers, "x-ratelimit-remaining"),
        rate_limit_used: header_number(headers, "x-ratelimit-used"),
        retry_after: headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_retry_after),
    };
    if let Some(seconds) = headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .and_then(cache_max_age)
    {
        metadata.expires_at = Some(Utc::now() + ChronoDuration::seconds(seconds));
    }
    if metadata.retry_after.is_none() && metadata.error_limit_remain.unwrap_or(1) <= 0 {
        metadata.retry_after = metadata
            .error_limit_reset
            .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds));
    }
    metadata
}

fn header_number(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn parse_http_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

fn parse_retry_after(value: &str) -> Option<DateTime<Utc>> {
    value
        .parse::<i64>()
        .ok()
        .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
        .or_else(|| parse_http_time(value))
}

fn cache_max_age(value: &str) -> Option<i64> {
    value.split(',').map(str::trim).find_map(|directive| {
        directive
            .strip_prefix("max-age=")
            .and_then(|seconds| seconds.parse().ok())
    })
}

fn rate_limit_window_seconds(value: &str) -> Option<i64> {
    let (_, window) = value.split_once('/')?;
    let (number, unit) = window.split_at(window.len().checked_sub(1)?);
    let count = number.parse::<i64>().ok()?;
    match unit {
        "s" => Some(count),
        "m" => count.checked_mul(60),
        "h" => count.checked_mul(60 * 60),
        "d" => count.checked_mul(60 * 60 * 24),
        _ => None,
    }
}

#[derive(Clone)]
pub struct ContractCollectionStore {
    pool: PgPool,
}

pub type ContractStoreHandle = Arc<RwLock<Option<Arc<ContractCollectionStore>>>>;

pub fn new_contract_store_handle() -> ContractStoreHandle {
    Arc::new(RwLock::new(None))
}

pub async fn available_contract_store(
    store_handle: &ContractStoreHandle,
) -> Option<Arc<ContractCollectionStore>> {
    store_handle.read().await.clone()
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegionState {
    pub baseline_at: Option<DateTime<Utc>>,
    pub complete_observations: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredContract {
    pub contract_id: i64,
    pub offered_items: Vec<PublicContractItem>,
    pub requested_items: Vec<PublicContractItem>,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageCounts {
    pub contract_facts: i64,
    pub manifests: i64,
    pub presence_intervals: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContractResolutionState {
    AwaitingResolution,
    AcceptanceConfirmed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContractResolutionRecord {
    pub region_id: i64,
    pub contract_id: i64,
    pub state: ContractResolutionState,
    pub last_public_observed_at: DateTime<Utc>,
    pub absence_observed_at: DateTime<Utc>,
    pub acceptance_evidence_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractEventKind {
    Listed,
    SaleConfirmed,
    PurchaseConfirmed,
}

impl ContractEventKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Listed => "listed",
            Self::SaleConfirmed => "sale_confirmed",
            Self::PurchaseConfirmed => "purchase_confirmed",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractItemDirection {
    Offered,
    Requested,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractEventAction {
    Ignore,
    Post,
    PostAndPing,
}

impl Default for ContractEventAction {
    fn default() -> Self {
        Self::Ignore
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEventActions {
    pub listed: ContractEventAction,
    #[serde(default)]
    pub sale_confirmed: ContractEventAction,
    #[serde(default)]
    pub purchase_confirmed: ContractEventAction,
}

impl Default for ContractEventActions {
    fn default() -> Self {
        Self {
            listed: ContractEventAction::Ignore,
            sale_confirmed: ContractEventAction::Ignore,
            purchase_confirmed: ContractEventAction::Ignore,
        }
    }
}

impl ContractEventActions {
    pub fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ContractFilter {
    pub root: ContractFilterNode,
}

impl ContractFilter {
    pub fn listed_ship(
        direction: ContractItemDirection,
        type_ids: Vec<i64>,
        ship_group_ids: Vec<i64>,
    ) -> Self {
        let mut items = Vec::new();
        if !type_ids.is_empty() {
            items.push(ContractFilterNode::Condition(
                ContractFilterCondition::ItemTypes {
                    direction,
                    ids: type_ids,
                },
            ));
        }
        if !ship_group_ids.is_empty() {
            items.push(ContractFilterNode::Condition(
                ContractFilterCondition::ShipGroups {
                    direction,
                    ids: ship_group_ids,
                },
            ));
        }
        let item_filter = match items.len() {
            0 => ContractFilterNode::And(Vec::new()),
            1 => items.pop().expect("one item filter"),
            _ => ContractFilterNode::Or(items),
        };
        Self {
            root: ContractFilterNode::And(vec![
                ContractFilterNode::Condition(ContractFilterCondition::EventKinds(vec![
                    ContractEventKind::Listed,
                ])),
                item_filter,
            ]),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.root.validate(0, &mut 0)
    }

    pub fn context_requirements(&self) -> ContractContextRequirements {
        self.root.context_requirements()
    }
}

impl<'de> Deserialize<'de> for ContractFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct FilterDocument {
            root: ContractFilterNode,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyContractFilter {
            events: Vec<ContractEventKind>,
            item_direction: ContractItemDirection,
            type_ids: Vec<i64>,
            ship_group_ids: Vec<i64>,
        }

        let value = Value::deserialize(deserializer)?;
        if value.get("root").is_some() {
            let document =
                serde_json::from_value::<FilterDocument>(value).map_err(D::Error::custom)?;
            return Ok(Self {
                root: document.root,
            });
        }
        let legacy =
            serde_json::from_value::<LegacyContractFilter>(value).map_err(D::Error::custom)?;
        let mut item_filters = Vec::new();
        if !legacy.type_ids.is_empty() {
            item_filters.push(ContractFilterNode::Condition(
                ContractFilterCondition::ItemTypes {
                    direction: legacy.item_direction,
                    ids: legacy.type_ids,
                },
            ));
        }
        if !legacy.ship_group_ids.is_empty() {
            item_filters.push(ContractFilterNode::Condition(
                ContractFilterCondition::ShipGroups {
                    direction: legacy.item_direction,
                    ids: legacy.ship_group_ids,
                },
            ));
        }
        let item_filter = match item_filters.len() {
            0 => ContractFilterNode::And(Vec::new()),
            1 => item_filters.pop().expect("one legacy item filter"),
            _ => ContractFilterNode::Or(item_filters),
        };
        Ok(Self {
            root: ContractFilterNode::And(vec![
                ContractFilterNode::Condition(ContractFilterCondition::EventKinds(legacy.events)),
                item_filter,
            ]),
        })
    }
}

const MAX_CONTRACT_FILTER_DEPTH: usize = 16;
const MAX_CONTRACT_FILTER_NODES: usize = 128;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ContractFilterNode {
    Condition(ContractFilterCondition),
    And(Vec<ContractFilterNode>),
    Or(Vec<ContractFilterNode>),
    Not(Box<ContractFilterNode>),
}

impl ContractFilterNode {
    fn validate(&self, depth: usize, node_count: &mut usize) -> Result<(), String> {
        if depth > MAX_CONTRACT_FILTER_DEPTH {
            return Err(format!(
                "contract filter nesting cannot exceed {MAX_CONTRACT_FILTER_DEPTH} levels"
            ));
        }
        *node_count += 1;
        if *node_count > MAX_CONTRACT_FILTER_NODES {
            return Err(format!(
                "contract filters cannot contain more than {MAX_CONTRACT_FILTER_NODES} nodes"
            ));
        }
        match self {
            Self::Condition(condition) => condition.validate(),
            Self::And(nodes) | Self::Or(nodes) => {
                if nodes.is_empty() {
                    return Err("AND and OR contract filter nodes must not be empty".to_string());
                }
                for node in nodes {
                    node.validate(depth + 1, node_count)?;
                }
                Ok(())
            }
            Self::Not(node) => node.validate(depth + 1, node_count),
        }
    }

    fn context_requirements(&self) -> ContractContextRequirements {
        match self {
            Self::Condition(condition) => condition.context_requirements(),
            Self::And(nodes) | Self::Or(nodes) => nodes.iter().fold(
                ContractContextRequirements::default(),
                |requirements, node| requirements.union(node.context_requirements()),
            ),
            Self::Not(node) => node.context_requirements(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ContractFilterCondition {
    EventKinds(Vec<ContractEventKind>),
    OfferedItems,
    RequestedItems,
    ItemTypes {
        direction: ContractItemDirection,
        ids: Vec<i64>,
    },
    ShipGroups {
        direction: ContractItemDirection,
        ids: Vec<i64>,
    },
    MinimumIsk {
        direction: ContractItemDirection,
        value: f64,
    },
    MaximumIsk {
        direction: ContractItemDirection,
        value: f64,
    },
    Regions(Vec<i64>),
    SolarSystems(Vec<i64>),
    SecurityRange {
        min: f64,
        max: f64,
    },
    LocationIds(Vec<i64>),
    IssuerCharacters(Vec<i64>),
    IssuerCorporations(Vec<i64>),
    ObservedAffiliationAlliances(Vec<i64>),
    PersonalIssuance,
    CorporationIssuance,
    TitleFragment(String),
}

impl Eq for ContractFilterCondition {}

impl ContractFilterCondition {
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::EventKinds(events) => validate_ids(events, "event kind"),
            Self::OfferedItems
            | Self::RequestedItems
            | Self::PersonalIssuance
            | Self::CorporationIssuance => Ok(()),
            Self::ItemTypes { ids, .. } => validate_i64_ids(ids, "item type ID"),
            Self::ShipGroups { ids, .. } => validate_i64_ids(ids, "ship group ID"),
            Self::MinimumIsk { value, .. } | Self::MaximumIsk { value, .. } => {
                validate_money(*value)
            }
            Self::Regions(ids) => validate_i64_ids(ids, "region ID"),
            Self::SolarSystems(ids) => validate_i64_ids(ids, "solar system ID"),
            Self::SecurityRange { min, max } => {
                if !min.is_finite() || !max.is_finite() || min > max {
                    Err(
                        "security range must have finite minimum and maximum values in order"
                            .to_string(),
                    )
                } else {
                    Ok(())
                }
            }
            Self::LocationIds(ids) => validate_i64_ids(ids, "location ID"),
            Self::IssuerCharacters(ids) => validate_i64_ids(ids, "issuer character ID"),
            Self::IssuerCorporations(ids) => validate_i64_ids(ids, "issuer corporation ID"),
            Self::ObservedAffiliationAlliances(ids) => {
                validate_i64_ids(ids, "observed affiliation alliance ID")
            }
            Self::TitleFragment(fragment) => {
                if fragment.trim().is_empty() {
                    Err("title fragment cannot be empty".to_string())
                } else if fragment.chars().count() > 256 {
                    Err("title fragment cannot exceed 256 characters".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }

    fn context_requirements(&self) -> ContractContextRequirements {
        match self {
            Self::SolarSystems(_) => ContractContextRequirements {
                solar_system: true,
                ..ContractContextRequirements::default()
            },
            Self::SecurityRange { .. } => ContractContextRequirements {
                solar_system: true,
                security_status: true,
                ..ContractContextRequirements::default()
            },
            Self::ObservedAffiliationAlliances(_) => ContractContextRequirements {
                observed_affiliation: true,
                ..ContractContextRequirements::default()
            },
            _ => ContractContextRequirements::default(),
        }
    }
}

fn validate_ids<T>(ids: &[T], label: &str) -> Result<(), String> {
    if ids.is_empty() {
        Err(format!("provide at least one {label}"))
    } else {
        Ok(())
    }
}

fn validate_i64_ids(ids: &[i64], label: &str) -> Result<(), String> {
    validate_ids(ids, label)?;
    if ids.iter().any(|id| *id <= 0) {
        Err(format!("{label}s must be positive integers"))
    } else {
        Ok(())
    }
}

fn validate_money(value: f64) -> Result<(), String> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err("ISK values must be finite, non-negative numbers".to_string())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContractContextRequirements {
    pub solar_system: bool,
    pub security_status: bool,
    pub observed_affiliation: bool,
}

impl ContractContextRequirements {
    fn union(self, other: Self) -> Self {
        Self {
            solar_system: self.solar_system || other.solar_system,
            security_status: self.security_status || other.security_status,
            observed_affiliation: self.observed_affiliation || other.observed_affiliation,
        }
    }

    fn is_empty(self) -> bool {
        !self.solar_system && !self.security_status && !self.observed_affiliation
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractContextResolution {
    #[default]
    Indeterminate,
    Resolved,
    DefinitivelyAbsent,
    TemporarilyUnavailable,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractObservationContext {
    pub solar_system_id: Option<i64>,
    #[serde(default)]
    pub solar_system_resolution: ContractContextResolution,
    pub security_status: Option<f64>,
    #[serde(default)]
    pub security_status_resolution: ContractContextResolution,
    pub observed_affiliation_alliance_id: Option<i64>,
    #[serde(default)]
    pub observed_affiliation_alliance_resolution: ContractContextResolution,
}

impl ContractObservationContext {
    fn normalize_resolutions(&mut self) {
        if self.solar_system_id.is_some() {
            self.solar_system_resolution = ContractContextResolution::Resolved;
        }
        if self.security_status.is_some_and(f64::is_finite) {
            self.security_status_resolution = ContractContextResolution::Resolved;
        }
        if self.observed_affiliation_alliance_id.is_some() {
            self.observed_affiliation_alliance_resolution = ContractContextResolution::Resolved;
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractSubscription {
    pub guild_id: u64,
    pub channel_id: u64,
    pub id: String,
    pub description: String,
    pub filter: ContractFilter,
    pub event_actions: ContractEventActions,
}

impl ContractSubscription {
    pub fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("subscription ID cannot be empty".to_string());
        }
        if self.description.trim().is_empty() {
            return Err("subscription description cannot be empty".to_string());
        }
        self.filter.validate()?;
        self.event_actions.validate()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractEmbedField {
    pub name: String,
    pub value: String,
    pub inline: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractNotificationMessage {
    pub title: String,
    pub description: Option<String>,
    pub fields: Vec<ContractEmbedField>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PreparedContractDelivery {
    pub delivery_id: i64,
    pub guild_id: u64,
    pub channel_id: u64,
    pub subscription_id: String,
    pub contract_id: i64,
    pub event_kind: ContractEventKind,
    pub ping: bool,
    pub message: ContractNotificationMessage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryStatus {
    Prepared,
    Sent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryRecord {
    pub id: i64,
    pub status: DeliveryStatus,
    pub discord_message_id: Option<String>,
}

#[derive(Clone, Debug)]
struct DeferredContractMatch {
    subscription: ContractSubscription,
    event: ContractEvent,
}

#[derive(Clone, Debug)]
pub struct ContractDeliveryError(pub String);

impl Display for ContractDeliveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl std::error::Error for ContractDeliveryError {}

#[async_trait]
pub trait ContractDelivery: Send + Sync {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError>;
}

#[async_trait]
pub trait ShipGroupResolver: Send + Sync {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup;
}

struct CycleShipGroupResolver<'a> {
    inner: &'a dyn ShipGroupResolver,
    cache: Mutex<HashMap<i64, ShipGroupLookup>>,
}

impl<'a> CycleShipGroupResolver<'a> {
    fn new(inner: &'a dyn ShipGroupResolver) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ShipGroupResolver for CycleShipGroupResolver<'_> {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup {
        if let Some(group) = self.cache.lock().await.get(&type_id).copied() {
            return group;
        }
        let group = self.inner.group_for_type(type_id).await;
        self.cache.lock().await.insert(type_id, group);
        group
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShipGroupLookup {
    Resolved(Option<i64>),
    TemporarilyUnavailable,
}

#[derive(Clone, Debug)]
struct CachedResponse {
    response: Value,
    metadata: CacheMetadata,
}

impl ContractCollectionStore {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn region_state(&self, region_id: i64) -> Result<Option<RegionState>, sqlx::Error> {
        let row = sqlx::query("SELECT baseline_at, complete_observations FROM regional_collection_metadata WHERE region_id = $1")
            .bind(region_id).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| RegionState {
            baseline_at: row.get("baseline_at"),
            complete_observations: row.get("complete_observations"),
        }))
    }

    pub async fn region_contracts(
        &self,
        region_id: i64,
    ) -> Result<Vec<StoredContract>, sqlx::Error> {
        let rows = sqlx::query("SELECT presence_intervals.contract_id, presence_intervals.first_observed_at, presence_intervals.last_observed_at, contract_item_manifests.manifest FROM presence_intervals JOIN contract_item_manifests ON presence_intervals.manifest_hash = contract_item_manifests.manifest_hash WHERE presence_intervals.region_id = $1 ORDER BY presence_intervals.contract_id")
            .bind(region_id).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let manifest: ItemManifest =
                    serde_json::from_value(row.get("manifest")).map_err(json_to_sqlx)?;
                Ok(StoredContract {
                    contract_id: row.get("contract_id"),
                    offered_items: manifest.offered_items,
                    requested_items: manifest.requested_items,
                    first_observed_at: row.get("first_observed_at"),
                    last_observed_at: row.get("last_observed_at"),
                })
            })
            .collect()
    }

    pub async fn storage_counts(&self) -> Result<StorageCounts, sqlx::Error> {
        let row = sqlx::query("SELECT (SELECT count(*) FROM public_contract_facts) AS contract_facts, (SELECT count(*) FROM contract_item_manifests) AS manifests, (SELECT count(*) FROM presence_intervals) AS presence_intervals")
            .fetch_one(&self.pool).await?;
        Ok(StorageCounts {
            contract_facts: row.get("contract_facts"),
            manifests: row.get("manifests"),
            presence_intervals: row.get("presence_intervals"),
        })
    }

    pub async fn contract_resolution_records(
        &self,
    ) -> Result<Vec<ContractResolutionRecord>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, state, last_public_observed_at, absence_observed_at, acceptance_evidence_at FROM contract_resolution_cases ORDER BY region_id, contract_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(contract_resolution_record_from_row)
            .collect()
    }

    pub async fn upsert_contract_subscription(
        &self,
        subscription: &ContractSubscription,
    ) -> Result<(), sqlx::Error> {
        subscription
            .validate()
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        sqlx::query("INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (guild_id, channel_id, subscription_id) DO UPDATE SET description = EXCLUDED.description, filter = EXCLUDED.filter, event_actions = EXCLUDED.event_actions, deleted_at = NULL, updated_at = now()")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(&subscription.description)
            .bind(serde_json::to_value(&subscription.filter).map_err(json_to_sqlx)?)
            .bind(serde_json::to_value(&subscription.event_actions).map_err(json_to_sqlx)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn contract_subscriptions_for_channel(
        &self,
        guild_id: u64,
        channel_id: u64,
    ) -> Result<Vec<ContractSubscription>, sqlx::Error> {
        sqlx::query("SELECT guild_id, channel_id, subscription_id, description, filter, event_actions FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND deleted_at IS NULL ORDER BY subscription_id")
            .bind(guild_id as i64)
            .bind(channel_id as i64)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(contract_subscription_from_row)
            .collect()
    }

    pub async fn remove_contract_subscription(
        &self,
        guild_id: u64,
        channel_id: u64,
        id: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query("UPDATE contract_subscriptions SET deleted_at = now(), updated_at = now() WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND deleted_at IS NULL")
            .bind(guild_id as i64)
            .bind(channel_id as i64)
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 1 {
            sqlx::query("DELETE FROM contract_deferred_subscription_matches WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3")
                .bind(guild_id as i64)
                .bind(channel_id as i64)
                .bind(id)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn delivery_record(
        &self,
        delivery_id: i64,
    ) -> Result<Option<DeliveryRecord>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, status, discord_message_id FROM contract_outbound_deliveries WHERE id = $1",
        )
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(delivery_record_from_row).transpose()
    }

    pub async fn delivery_records(&self) -> Result<Vec<DeliveryRecord>, sqlx::Error> {
        sqlx::query(
            "SELECT id, status, discord_message_id FROM contract_outbound_deliveries ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(delivery_record_from_row)
        .collect()
    }

    async fn all_contract_subscriptions(&self) -> Result<Vec<ContractSubscription>, sqlx::Error> {
        sqlx::query("SELECT guild_id, channel_id, subscription_id, description, filter, event_actions FROM contract_subscriptions WHERE deleted_at IS NULL ORDER BY guild_id, channel_id, subscription_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(contract_subscription_from_row)
            .collect()
    }

    pub async fn delivery_event(
        &self,
        delivery_id: i64,
    ) -> Result<Option<ContractEvent>, sqlx::Error> {
        let event = sqlx::query_scalar::<_, Value>(
            "SELECT event FROM contract_outbound_deliveries WHERE id = $1",
        )
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await?;
        event
            .map(|event| serde_json::from_value(event).map_err(json_to_sqlx))
            .transpose()
    }

    async fn deferred_contract_matches(&self) -> Result<Vec<DeferredContractMatch>, sqlx::Error> {
        sqlx::query("SELECT contract_subscriptions.guild_id, contract_subscriptions.channel_id, contract_subscriptions.subscription_id, contract_subscriptions.description, contract_subscriptions.filter, contract_subscriptions.event_actions, contract_deferred_subscription_matches.event FROM contract_deferred_subscription_matches JOIN contract_subscriptions USING (guild_id, channel_id, subscription_id) WHERE contract_subscriptions.deleted_at IS NULL ORDER BY contract_deferred_subscription_matches.created_at, contract_subscriptions.guild_id, contract_subscriptions.channel_id, contract_subscriptions.subscription_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let event = serde_json::from_value(row.get("event")).map_err(json_to_sqlx)?;
                Ok(DeferredContractMatch {
                    subscription: contract_subscription_from_row(row)?,
                    event,
                })
            })
            .collect()
    }

    async fn defer_contract_match(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO contract_deferred_subscription_matches (guild_id, channel_id, subscription_id, contract_id, event_kind, event) SELECT $1,$2,$3,$4,$5,$6 WHERE EXISTS (SELECT 1 FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND deleted_at IS NULL) ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind) DO UPDATE SET event = EXCLUDED.event, updated_at = now()")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .bind(serde_json::to_value(event).map_err(json_to_sqlx)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn resolve_deferred_contract_match(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM contract_deferred_subscription_matches WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND contract_id = $4 AND event_kind = $5")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn prepare_delivery(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        message: &ContractNotificationMessage,
        ping: bool,
    ) -> Result<Option<PreparedContractDelivery>, sqlx::Error> {
        let delivery_id = sqlx::query_scalar::<_, i64>("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status) SELECT $1,$2,$3,$4,$5,$6,$7,$8,'prepared' WHERE EXISTS (SELECT 1 FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND deleted_at IS NULL) ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind) DO NOTHING RETURNING id")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .bind(serde_json::to_value(event).map_err(json_to_sqlx)?)
            .bind(serde_json::to_value(message).map_err(json_to_sqlx)?)
            .bind(ping)
            .fetch_optional(&self.pool)
            .await?;
        Ok(delivery_id.map(|delivery_id| PreparedContractDelivery {
            delivery_id,
            guild_id: subscription.guild_id,
            channel_id: subscription.channel_id,
            subscription_id: subscription.id.clone(),
            contract_id: event.contract.contract_id,
            event_kind: event.kind.clone(),
            ping,
            message: message.clone(),
        }))
    }

    async fn mark_delivery_sent(
        &self,
        delivery_id: i64,
        discord_message_id: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET status = 'sent', discord_message_id = $2, sent_at = now() WHERE id = $1 AND status = 'prepared'")
            .bind(delivery_id)
            .bind(discord_message_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {delivery_id} was not prepared when Discord returned success"
            )));
        }
        Ok(())
    }

    async fn cache(&self, resource_key: &str) -> Result<Option<CachedResponse>, sqlx::Error> {
        let row = sqlx::query("SELECT etag, expires_at, last_modified, expected_pages, error_limit_remain, error_limit_reset, retry_after, response FROM esi_cache_metadata WHERE resource_key = $1")
            .bind(resource_key).fetch_optional(&self.pool).await?;
        Ok(row.and_then(|row| {
            row.get::<Option<Value>, _>("response")
                .map(|response| CachedResponse {
                    response,
                    metadata: CacheMetadata {
                        etag: row.get("etag"),
                        expires_at: row.get("expires_at"),
                        last_modified: row.get("last_modified"),
                        expected_pages: row
                            .get::<Option<i32>, _>("expected_pages")
                            .map(|value| value as u32),
                        error_limit_remain: row.get("error_limit_remain"),
                        error_limit_reset: row.get("error_limit_reset"),
                        rate_limit_group: None,
                        rate_limit_limit: None,
                        rate_limit_remaining: None,
                        rate_limit_used: None,
                        retry_after: row.get("retry_after"),
                    },
                })
        }))
    }

    async fn save_cache(
        &self,
        resource_key: &str,
        response: &Value,
        metadata: &CacheMetadata,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO esi_cache_metadata (resource_key, etag, expires_at, last_modified, expected_pages, error_limit_remain, error_limit_reset, retry_after, response, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now()) ON CONFLICT (resource_key) DO UPDATE SET etag = EXCLUDED.etag, expires_at = EXCLUDED.expires_at, last_modified = EXCLUDED.last_modified, expected_pages = EXCLUDED.expected_pages, error_limit_remain = EXCLUDED.error_limit_remain, error_limit_reset = EXCLUDED.error_limit_reset, retry_after = EXCLUDED.retry_after, response = EXCLUDED.response, updated_at = now()")
            .bind(resource_key).bind(&metadata.etag).bind(metadata.expires_at).bind(metadata.last_modified).bind(metadata.expected_pages.map(|value| value as i32)).bind(metadata.error_limit_remain).bind(metadata.error_limit_reset).bind(metadata.retry_after).bind(response).execute(&self.pool).await?;
        Ok(())
    }

    async fn active_esi_limiter_deadline(&self) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT pause_until FROM esi_collection_limiter_state WHERE limiter_scope = TRUE AND pause_until > now()",
        )
        .fetch_optional(&self.pool)
        .await
    }

    async fn record_esi_limiter(&self, metadata: &CacheMetadata) -> Result<(), sqlx::Error> {
        let pause_until = metadata.collection_pause_until();
        sqlx::query("INSERT INTO esi_collection_limiter_state (limiter_scope, pause_until, error_limit_remain, error_limit_reset, rate_limit_group, rate_limit_limit, rate_limit_remaining, rate_limit_used, updated_at) VALUES (TRUE,$1,$2,$3,$4,$5,$6,$7,now()) ON CONFLICT (limiter_scope) DO UPDATE SET pause_until = CASE WHEN EXCLUDED.pause_until IS NULL THEN esi_collection_limiter_state.pause_until WHEN esi_collection_limiter_state.pause_until IS NULL OR esi_collection_limiter_state.pause_until < EXCLUDED.pause_until THEN EXCLUDED.pause_until ELSE esi_collection_limiter_state.pause_until END, error_limit_remain = EXCLUDED.error_limit_remain, error_limit_reset = EXCLUDED.error_limit_reset, rate_limit_group = EXCLUDED.rate_limit_group, rate_limit_limit = EXCLUDED.rate_limit_limit, rate_limit_remaining = EXCLUDED.rate_limit_remaining, rate_limit_used = EXCLUDED.rate_limit_used, updated_at = now()")
            .bind(pause_until)
            .bind(metadata.error_limit_remain)
            .bind(metadata.error_limit_reset)
            .bind(&metadata.rate_limit_group)
            .bind(&metadata.rate_limit_limit)
            .bind(metadata.rate_limit_remaining)
            .bind(metadata.rate_limit_used)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn record_complete(
        &self,
        region_id: i64,
        expected_pages: u32,
        contracts: &[ObservedContract],
    ) -> Result<RecordComplete, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let now = Utc::now();
        sqlx::query("INSERT INTO regional_collection_metadata (region_id) VALUES ($1) ON CONFLICT (region_id) DO NOTHING").bind(region_id).execute(&mut *transaction).await?;
        let baseline_at: Option<DateTime<Utc>> = sqlx::query(
            "SELECT baseline_at FROM regional_collection_metadata WHERE region_id = $1 FOR UPDATE",
        )
        .bind(region_id)
        .fetch_one(&mut *transaction)
        .await?
        .get("baseline_at");
        let mut newly_observed = Vec::new();
        for observed in contracts {
            let previously_observed = sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM presence_intervals WHERE region_id = $1 AND contract_id = $2)")
                .bind(region_id)
                .bind(observed.contract.contract_id)
                .fetch_one(&mut *transaction)
                .await?;
            if baseline_at.is_some() && !previously_observed {
                newly_observed.push(observed.clone());
            }
            let facts = serde_json::to_value(&observed.contract).map_err(json_to_sqlx)?;
            let manifest = serde_json::to_value(&observed.manifest).map_err(json_to_sqlx)?;
            sqlx::query("INSERT INTO public_contract_facts (fact_hash, contract_id, facts) VALUES ($1,$2,$3) ON CONFLICT (fact_hash) DO NOTHING").bind(&observed.fact_hash).bind(observed.contract.contract_id).bind(facts).execute(&mut *transaction).await?;
            sqlx::query("INSERT INTO contract_item_manifests (manifest_hash, manifest) VALUES ($1,$2) ON CONFLICT (manifest_hash) DO NOTHING").bind(&observed.manifest_hash).bind(manifest).execute(&mut *transaction).await?;
            sqlx::query("INSERT INTO presence_intervals (region_id, contract_id, fact_hash, manifest_hash, first_observed_at, last_observed_at) VALUES ($1,$2,$3,$4,$5,$5) ON CONFLICT (region_id, contract_id, fact_hash, manifest_hash) DO UPDATE SET last_observed_at = EXCLUDED.last_observed_at").bind(region_id).bind(observed.contract.contract_id).bind(&observed.fact_hash).bind(&observed.manifest_hash).bind(now).execute(&mut *transaction).await?;
        }
        let observed_ids: Vec<_> = contracts
            .iter()
            .map(|observed| observed.contract.contract_id)
            .collect();
        sqlx::query("DELETE FROM contract_resolution_cases WHERE region_id = $1 AND state = 'awaiting_resolution' AND contract_id = ANY($2)")
            .bind(region_id)
            .bind(&observed_ids)
            .execute(&mut *transaction)
            .await?;
        let last_observed = sqlx::query("SELECT DISTINCT ON (presence_intervals.contract_id) presence_intervals.contract_id, presence_intervals.last_observed_at, public_contract_facts.facts, contract_item_manifests.manifest FROM presence_intervals JOIN public_contract_facts ON public_contract_facts.fact_hash = presence_intervals.fact_hash JOIN contract_item_manifests ON contract_item_manifests.manifest_hash = presence_intervals.manifest_hash WHERE presence_intervals.region_id = $1 ORDER BY presence_intervals.contract_id, presence_intervals.last_observed_at DESC")
            .bind(region_id)
            .fetch_all(&mut *transaction)
            .await?;
        let observed_ids: HashSet<_> = observed_ids.into_iter().collect();
        for row in last_observed {
            let contract_id: i64 = row.get("contract_id");
            if observed_ids.contains(&contract_id) {
                continue;
            }
            let contract: PublicContract =
                serde_json::from_value(row.get("facts")).map_err(json_to_sqlx)?;
            let manifest: ItemManifest =
                serde_json::from_value(row.get("manifest")).map_err(json_to_sqlx)?;
            let last_public_observed_at: DateTime<Utc> = row.get("last_observed_at");
            let inserted = sqlx::query_scalar::<_, i64>("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state) VALUES ($1,$2,$3,$4,$5,$6,COALESCE((SELECT expires_at FROM esi_cache_metadata WHERE resource_key = $7),$6),'awaiting_resolution') ON CONFLICT (region_id, contract_id) DO NOTHING RETURNING contract_id")
                .bind(region_id)
                .bind(contract_id)
                .bind(serde_json::to_value(&contract).map_err(json_to_sqlx)?)
                .bind(serde_json::to_value(&manifest).map_err(json_to_sqlx)?)
                .bind(last_public_observed_at)
                .bind(now)
                .bind(format!("contracts/public/items/{contract_id}"))
                .fetch_optional(&mut *transaction)
                .await?;
            if inserted.is_some() {
                sqlx::query("INSERT INTO contract_lifecycle_evidence (region_id, contract_id, evidence_kind, observed_at, detail) VALUES ($1,$2,'no_longer_public',$3,$4)")
                    .bind(region_id)
                    .bind(contract_id)
                    .bind(now)
                    .bind(serde_json::json!({
                        "last_public_observed_at": last_public_observed_at,
                        "absence_observed_at": now,
                    }))
                    .execute(&mut *transaction)
                    .await?;
            }
        }
        sqlx::query("INSERT INTO regional_observations (region_id, observed_at, outcome, expected_pages) VALUES ($1,$2,'complete',$3)").bind(region_id).bind(now).bind(expected_pages as i32).execute(&mut *transaction).await?;
        sqlx::query("UPDATE regional_collection_metadata SET baseline_at = COALESCE(baseline_at, $2), complete_observations = complete_observations + 1, last_complete_at = $2 WHERE region_id = $1").bind(region_id).bind(now).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(RecordComplete {
            baseline_established: baseline_at.is_none(),
            observed_contracts: contracts.len(),
            newly_observed,
        })
    }

    async fn record_inconclusive(
        &self,
        region_id: i64,
        detail: &str,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        sqlx::query("INSERT INTO regional_observations (region_id, observed_at, outcome, detail) VALUES ($1,$2,'inconclusive',$3)").bind(region_id).bind(now).bind(detail).execute(&self.pool).await?;
        self.record_failure(
            Some(region_id),
            "inconclusive_observation",
            detail,
            retry_after,
        )
        .await
    }

    async fn record_failure(
        &self,
        region_id: Option<i64>,
        failure_kind: &str,
        detail: &str,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        sqlx::query("INSERT INTO contract_collection_failures (region_id, observed_at, failure_kind, detail, retry_after) VALUES ($1,$2,$3,$4,$5)").bind(region_id).bind(now).bind(failure_kind).bind(detail).bind(retry_after).execute(&self.pool).await?;
        Ok(())
    }

    async fn awaiting_resolution_cases(
        &self,
    ) -> Result<Vec<AwaitingContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at FROM contract_resolution_cases WHERE state = 'awaiting_resolution' AND next_probe_at <= now() ORDER BY region_id, contract_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(AwaitingContractResolution {
                    region_id: row.get("region_id"),
                    contract_id: row.get("contract_id"),
                    contract: serde_json::from_value(row.get("contract")).map_err(json_to_sqlx)?,
                    manifest: serde_json::from_value(row.get("manifest")).map_err(json_to_sqlx)?,
                    last_public_observed_at: row.get("last_public_observed_at"),
                    absence_observed_at: row.get("absence_observed_at"),
                })
            })
            .collect()
    }

    async fn schedule_resolution_probe(
        &self,
        region_id: i64,
        contract_id: i64,
        next_probe_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = $3, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution'")
            .bind(region_id)
            .bind(contract_id)
            .bind(next_probe_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn confirm_acceptance(
        &self,
        resolution: &AwaitingContractResolution,
        evidence_response_at: DateTime<Utc>,
        metadata: &CacheMetadata,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let confirmed = sqlx::query_scalar::<_, i64>("UPDATE contract_resolution_cases SET state = 'acceptance_confirmed', acceptance_evidence_at = $3, acceptance_response_metadata = $4, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution' RETURNING contract_id")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(evidence_response_at)
            .bind(serde_json::to_value(metadata).map_err(json_to_sqlx)?)
            .fetch_optional(&mut *transaction)
            .await?;
        if confirmed.is_some() {
            sqlx::query("INSERT INTO contract_lifecycle_evidence (region_id, contract_id, evidence_kind, observed_at, detail) VALUES ($1,$2,'acceptance_confirmed',$3,$4)")
                .bind(resolution.region_id)
                .bind(resolution.contract_id)
                .bind(evidence_response_at)
                .bind(serde_json::json!({
                    "last_public_observed_at": resolution.last_public_observed_at,
                    "absence_observed_at": resolution.absence_observed_at,
                    "evidence_response_at": evidence_response_at,
                    "response": "204_no_content",
                }))
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(confirmed.is_some())
    }
}

fn contract_subscription_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ContractSubscription, sqlx::Error> {
    Ok(ContractSubscription {
        guild_id: row.get::<i64, _>("guild_id") as u64,
        channel_id: row.get::<i64, _>("channel_id") as u64,
        id: row.get("subscription_id"),
        description: row.get("description"),
        filter: serde_json::from_value(row.get("filter")).map_err(json_to_sqlx)?,
        event_actions: serde_json::from_value(row.get("event_actions")).map_err(json_to_sqlx)?,
    })
}

fn delivery_record_from_row(row: sqlx::postgres::PgRow) -> Result<DeliveryRecord, sqlx::Error> {
    let status = match row.get::<String, _>("status").as_str() {
        "prepared" => DeliveryStatus::Prepared,
        "sent" => DeliveryStatus::Sent,
        value => {
            return Err(sqlx::Error::Protocol(format!(
                "unknown delivery status: {value}"
            )))
        }
    };
    Ok(DeliveryRecord {
        id: row.get("id"),
        status,
        discord_message_id: row.get("discord_message_id"),
    })
}

fn contract_resolution_record_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ContractResolutionRecord, sqlx::Error> {
    let state = match row.get::<String, _>("state").as_str() {
        "awaiting_resolution" => ContractResolutionState::AwaitingResolution,
        "acceptance_confirmed" => ContractResolutionState::AcceptanceConfirmed,
        value => {
            return Err(sqlx::Error::Protocol(format!(
                "unknown resolution state: {value}"
            )))
        }
    };
    Ok(ContractResolutionRecord {
        region_id: row.get("region_id"),
        contract_id: row.get("contract_id"),
        state,
        last_public_observed_at: row.get("last_public_observed_at"),
        absence_observed_at: row.get("absence_observed_at"),
        acceptance_evidence_at: row.get("acceptance_evidence_at"),
    })
}

fn json_to_sqlx(error: serde_json::Error) -> sqlx::Error {
    sqlx::Error::Protocol(error.to_string())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ItemManifest {
    offered_items: Vec<PublicContractItem>,
    requested_items: Vec<PublicContractItem>,
}

#[derive(Clone, Debug)]
struct ObservedContract {
    contract: PublicContract,
    manifest: ItemManifest,
    fact_hash: String,
    manifest_hash: String,
}

#[derive(Clone, Debug)]
struct AwaitingContractResolution {
    region_id: i64,
    contract_id: i64,
    contract: PublicContract,
    manifest: ItemManifest,
    last_public_observed_at: DateTime<Utc>,
    absence_observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct RecordComplete {
    baseline_established: bool,
    observed_contracts: usize,
    newly_observed: Vec<ObservedContract>,
}

#[derive(Debug)]
pub enum ContractCollectionError {
    Database(sqlx::Error),
    Esi(EsiError),
    Cache(String),
}

impl Display for ContractCollectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database: {error}"),
            Self::Esi(error) => write!(formatter, "ESI: {error}"),
            Self::Cache(error) => write!(formatter, "cache: {error}"),
        }
    }
}
impl std::error::Error for ContractCollectionError {}
impl From<sqlx::Error> for ContractCollectionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}
impl From<EsiError> for ContractCollectionError {
    fn from(error: EsiError) -> Self {
        Self::Esi(error)
    }
}

impl ContractCollectionError {
    fn retry_after(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Esi(error) => error.retry_after,
            Self::Database(_) | Self::Cache(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CollectionOutcome {
    BaselineEstablished {
        region_id: i64,
    },
    Complete {
        region_id: i64,
        observed_contracts: usize,
    },
    Inconclusive {
        region_id: i64,
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContractEvent {
    pub region_id: i64,
    pub kind: ContractEventKind,
    pub contract: PublicContract,
    pub offered_items: Vec<PublicContractItem>,
    pub requested_items: Vec<PublicContractItem>,
    #[serde(default)]
    pub context: ContractObservationContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_evidence: Option<ContractAcceptanceEvidence>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContractAcceptanceEvidence {
    pub last_public_observed_at: DateTime<Utc>,
    pub absence_observed_at: DateTime<Utc>,
    pub evidence_response_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CollectionReport {
    pub regions: Vec<CollectionOutcome>,
    pub events: Vec<ContractEvent>,
    pub retry_after: Option<DateTime<Utc>>,
}

pub struct ContractCollector {
    store: ContractCollectionStore,
    esi: Arc<dyn PublicContractEsi>,
    notifications: Option<ContractNotifications>,
}

struct ContractNotifications {
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
}

enum NotificationResolution {
    Complete,
    Deferred,
}

impl ContractCollector {
    pub fn new(store: ContractCollectionStore, esi: Arc<dyn PublicContractEsi>) -> Self {
        Self {
            store,
            esi,
            notifications: None,
        }
    }

    pub fn with_notifications(
        mut self,
        ship_groups: Arc<dyn ShipGroupResolver>,
        delivery: Arc<dyn ContractDelivery>,
    ) -> Self {
        self.notifications = Some(ContractNotifications {
            ship_groups,
            delivery,
        });
        self
    }

    pub async fn collect_cycle(&self) -> Result<CollectionReport, ContractCollectionError> {
        self.ensure_esi_limiter_allows_requests().await?;
        let regions = match self.regions().await {
            Ok(regions) => regions,
            Err(error) => {
                self.store
                    .record_failure(
                        None,
                        "region_discovery",
                        &error.to_string(),
                        error.retry_after(),
                    )
                    .await?;
                return Err(error);
            }
        };
        let mut outcomes = Vec::with_capacity(regions.len());
        let mut events = Vec::new();
        let mut retry_after = None;
        for region_id in regions {
            match self.collect_region(region_id).await {
                Ok(recorded) => {
                    let observed_contracts = recorded.observed_contracts;
                    events.extend(recorded.newly_observed.into_iter().map(|observed| {
                        ContractEvent {
                            region_id,
                            kind: ContractEventKind::Listed,
                            contract: observed.contract,
                            offered_items: observed.manifest.offered_items,
                            requested_items: observed.manifest.requested_items,
                            context: ContractObservationContext::default(),
                            acceptance_evidence: None,
                        }
                    }));
                    outcomes.push(if recorded.baseline_established {
                        CollectionOutcome::BaselineEstablished { region_id }
                    } else {
                        CollectionOutcome::Complete {
                            region_id,
                            observed_contracts,
                        }
                    });
                }
                Err(error) => {
                    let detail = error.to_string();
                    let regional_retry_after = error.retry_after();
                    self.store
                        .record_inconclusive(region_id, &detail, regional_retry_after)
                        .await?;
                    retry_after = std::cmp::max(retry_after, regional_retry_after);
                    outcomes.push(CollectionOutcome::Inconclusive {
                        region_id,
                        reason: detail,
                    });
                    if self.store.active_esi_limiter_deadline().await?.is_some() {
                        break;
                    }
                }
            }
        }
        events.extend(self.resolve_awaiting_resolutions().await?);
        self.notify(&events).await?;
        Ok(CollectionReport {
            regions: outcomes,
            events,
            retry_after,
        })
    }

    async fn collect_region(
        &self,
        region_id: i64,
    ) -> Result<RecordComplete, ContractCollectionError> {
        let first_page = self.contract_page(region_id, 1).await?;
        let expected_pages = first_page
            .1
            .expected_pages
            .ok_or_else(|| ContractCollectionError::Cache("page 1 omitted X-Pages".to_string()))?;
        if expected_pages == 0 {
            return Err(ContractCollectionError::Cache(
                "X-Pages must be at least 1".to_string(),
            ));
        }
        let mut contracts = first_page.0;
        let first_page_last_modified = first_page.1.last_modified;
        for page in 2..=expected_pages {
            let (mut page_contracts, metadata) = self.contract_page(region_id, page).await?;
            if metadata.expected_pages != Some(expected_pages) {
                return Err(ContractCollectionError::Cache(format!(
                    "page {page} reported inconsistent X-Pages"
                )));
            }
            if metadata.last_modified != first_page_last_modified {
                return Err(ContractCollectionError::Cache(format!(
                    "page {page} reported inconsistent Last-Modified"
                )));
            }
            contracts.append(&mut page_contracts);
        }
        let mut contract_ids = HashSet::with_capacity(contracts.len());
        if contracts
            .iter()
            .any(|contract| !contract_ids.insert(contract.contract_id))
        {
            return Err(ContractCollectionError::Cache(
                "a contract appeared on more than one page".to_string(),
            ));
        }
        let public_contracts: Vec<_> = contracts
            .into_iter()
            .filter(PublicContract::is_public_item_exchange)
            .collect();
        let mut observed = Vec::with_capacity(public_contracts.len());
        for contract in public_contracts {
            let mut items = self.contract_items(contract.contract_id).await?;
            items.sort_by_key(|item| item.record_id);
            let manifest = ItemManifest {
                offered_items: items
                    .iter()
                    .filter(|item| item.is_included)
                    .cloned()
                    .collect(),
                requested_items: items
                    .iter()
                    .filter(|item| !item.is_included)
                    .cloned()
                    .collect(),
            };
            let fact_hash = content_hash(&contract)?;
            let manifest_hash = content_hash(&manifest)?;
            observed.push(ObservedContract {
                contract,
                manifest,
                fact_hash,
                manifest_hash,
            });
        }
        Ok(self
            .store
            .record_complete(region_id, expected_pages, &observed)
            .await?)
    }

    async fn resolve_awaiting_resolutions(
        &self,
    ) -> Result<Vec<ContractEvent>, ContractCollectionError> {
        let mut events = Vec::new();
        for resolution in self.store.awaiting_resolution_cases().await? {
            let key = format!("contracts/public/items/{}", resolution.contract_id);
            let cached = self.store.cache(&key).await?;
            if let Some(cached) = &cached {
                if cached.metadata.is_fresh() {
                    self.store
                        .schedule_resolution_probe(
                            resolution.region_id,
                            resolution.contract_id,
                            cached
                                .metadata
                                .expires_at
                                .expect("fresh cache has expiration"),
                        )
                        .await?;
                    continue;
                }
            }
            let etag = cached
                .as_ref()
                .and_then(|cached| cached.metadata.etag.clone());
            self.ensure_esi_limiter_allows_requests().await?;
            let probe = self
                .record_item_probe_result(
                    self.esi
                        .public_contract_items_probe(resolution.contract_id, etag.as_deref())
                        .await,
                )
                .await?;
            match probe {
                ContractItemProbe::Available(response) => {
                    let (items, metadata) = if response.not_modified {
                        let cached = cached.as_ref().ok_or_else(|| {
                            ContractCollectionError::Cache(format!(
                                "{key} returned 304 without a cached item representation"
                            ))
                        })?;
                        (
                            cached.response.clone(),
                            merge_cache_metadata(&cached.metadata, response.metadata),
                        )
                    } else {
                        (
                            serde_json::to_value(response.value.ok_or_else(|| {
                                ContractCollectionError::Cache(format!(
                                    "{key} returned no item representation"
                                ))
                            })?)
                            .map_err(|error| ContractCollectionError::Cache(error.to_string()))?,
                            response.metadata,
                        )
                    };
                    self.store.save_cache(&key, &items, &metadata).await?;
                    self.store
                        .schedule_resolution_probe(
                            resolution.region_id,
                            resolution.contract_id,
                            metadata.expires_at.unwrap_or_else(Utc::now),
                        )
                        .await?;
                }
                ContractItemProbe::Accepted(metadata) => {
                    let evidence_response_at = Utc::now();
                    if evidence_response_at >= resolution.contract.date_expired {
                        self.store
                            .schedule_resolution_probe(
                                resolution.region_id,
                                resolution.contract_id,
                                evidence_response_at + ChronoDuration::minutes(5),
                            )
                            .await?;
                        continue;
                    }
                    if self
                        .store
                        .confirm_acceptance(&resolution, evidence_response_at, &metadata)
                        .await?
                    {
                        if let Some(kind) = confirmed_contract_event_kind(&resolution) {
                            events.push(ContractEvent {
                                region_id: resolution.region_id,
                                kind,
                                contract: resolution.contract,
                                offered_items: resolution.manifest.offered_items,
                                requested_items: resolution.manifest.requested_items,
                                context: ContractObservationContext::default(),
                                acceptance_evidence: Some(ContractAcceptanceEvidence {
                                    last_public_observed_at: resolution.last_public_observed_at,
                                    absence_observed_at: resolution.absence_observed_at,
                                    evidence_response_at,
                                }),
                            });
                        }
                    }
                }
            }
        }
        Ok(events)
    }

    async fn notify(&self, events: &[ContractEvent]) -> Result<(), ContractCollectionError> {
        let Some(notifications) = &self.notifications else {
            return Ok(());
        };
        let ship_groups = CycleShipGroupResolver::new(&*notifications.ship_groups);
        let deferred_matches = self.store.deferred_contract_matches().await?;
        let subscriptions = self.store.all_contract_subscriptions().await?;
        for deferred in deferred_matches {
            let event = self
                .event_with_observed_context(
                    &deferred.event,
                    std::slice::from_ref(&deferred.subscription),
                    &ship_groups,
                )
                .await?;
            match self
                .notify_subscription(&deferred.subscription, &event, &ship_groups, notifications)
                .await?
            {
                NotificationResolution::Complete => {
                    self.store
                        .resolve_deferred_contract_match(&deferred.subscription, &deferred.event)
                        .await?;
                }
                NotificationResolution::Deferred => {}
            }
        }
        for event in events {
            let event = self
                .event_with_observed_context(event, &subscriptions, &ship_groups)
                .await?;
            for subscription in &subscriptions {
                if matches!(
                    self.notify_subscription(subscription, &event, &ship_groups, notifications)
                        .await?,
                    NotificationResolution::Deferred
                ) {
                    self.store
                        .defer_contract_match(subscription, &event)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn event_with_observed_context(
        &self,
        event: &ContractEvent,
        subscriptions: &[ContractSubscription],
        ship_groups: &dyn ShipGroupResolver,
    ) -> Result<ContractEvent, ContractCollectionError> {
        let mut requirements = ContractContextRequirements::default();
        for subscription in subscriptions {
            if matches!(
                contract_event_action(subscription, &event.kind),
                ContractEventAction::Ignore
            ) {
                continue;
            }
            let mut evaluator = ContractFilterEvaluator::new(event, ship_groups);
            let plan =
                plan_contract_filter_context(&subscription.filter.root, &mut evaluator, true).await;
            requirements = requirements.union(plan.requirements);
        }
        if requirements.is_empty() {
            return Ok(event.clone());
        }
        let mut event = event.clone();
        event.context.normalize_resolutions();

        if (requirements.solar_system || requirements.security_status)
            && event.context.solar_system_id.is_none()
            && !matches!(
                event.context.solar_system_resolution,
                ContractContextResolution::DefinitivelyAbsent
            )
        {
            if self
                .context_request_allowed(event.contract.contract_id)
                .await?
            {
                let response = self
                    .record_context_esi_result(
                        event.contract.contract_id,
                        self.esi
                            .observed_contract_solar_system(&event.contract)
                            .await,
                    )
                    .await?;
                apply_solar_system_context(&mut event.context, response);
            } else {
                event.context.solar_system_resolution =
                    ContractContextResolution::TemporarilyUnavailable;
            }
        }

        if requirements.security_status {
            if let Some(solar_system_id) = event.context.solar_system_id {
                if event.context.security_status.is_none()
                    && !matches!(
                        event.context.security_status_resolution,
                        ContractContextResolution::DefinitivelyAbsent
                    )
                {
                    if self
                        .context_request_allowed(event.contract.contract_id)
                        .await?
                    {
                        let response = self
                            .record_context_esi_result(
                                event.contract.contract_id,
                                self.esi
                                    .observed_solar_system_security(
                                        &event.contract,
                                        solar_system_id,
                                    )
                                    .await,
                            )
                            .await?;
                        apply_security_context(&mut event.context, response);
                    } else {
                        event.context.security_status_resolution =
                            ContractContextResolution::TemporarilyUnavailable;
                    }
                }
            } else if matches!(
                event.context.solar_system_resolution,
                ContractContextResolution::TemporarilyUnavailable
            ) {
                event.context.security_status_resolution =
                    ContractContextResolution::TemporarilyUnavailable;
            }
        }

        if requirements.observed_affiliation
            && event.context.observed_affiliation_alliance_id.is_none()
            && !matches!(
                event.context.observed_affiliation_alliance_resolution,
                ContractContextResolution::DefinitivelyAbsent
            )
        {
            if self
                .context_request_allowed(event.contract.contract_id)
                .await?
            {
                let response = self
                    .record_context_esi_result(
                        event.contract.contract_id,
                        self.esi.observed_issuer_affiliation(&event.contract).await,
                    )
                    .await?;
                apply_observed_affiliation_context(&mut event.context, response);
            } else {
                event.context.observed_affiliation_alliance_resolution =
                    ContractContextResolution::TemporarilyUnavailable;
            }
        }
        Ok(event)
    }

    async fn context_request_allowed(
        &self,
        contract_id: i64,
    ) -> Result<bool, ContractCollectionError> {
        match self.ensure_esi_limiter_allows_requests().await {
            Ok(()) => Ok(true),
            Err(ContractCollectionError::Esi(error)) => {
                warn!(
                    contract_id,
                    "contract context is deferred while the persisted ESI limiter boundary is active: {error}"
                );
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    async fn record_context_esi_result<T>(
        &self,
        contract_id: i64,
        result: Result<EsiResponse<T>, EsiError>,
    ) -> Result<Option<EsiResponse<T>>, ContractCollectionError> {
        match result {
            Ok(response) => {
                self.store.record_esi_limiter(&response.metadata).await?;
                Ok(Some(response))
            }
            Err(error) => {
                self.store.record_esi_limiter(&error.metadata).await?;
                warn!(
                    contract_id,
                    "contract context will be retried before context-dependent notifications: {error}"
                );
                Ok(None)
            }
        }
    }

    async fn notify_subscription(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        ship_groups: &dyn ShipGroupResolver,
        notifications: &ContractNotifications,
    ) -> Result<NotificationResolution, ContractCollectionError> {
        let action = contract_event_action(subscription, &event.kind);
        if matches!(action, ContractEventAction::Ignore) {
            return Ok(NotificationResolution::Complete);
        }
        let mut evaluator = ContractFilterEvaluator::new(event, ship_groups);
        match evaluator.matches(&subscription.filter.root).await {
            ContractFilterMatch::Matched => {}
            ContractFilterMatch::Unmatched => return Ok(NotificationResolution::Complete),
            ContractFilterMatch::Deferred => return Ok(NotificationResolution::Deferred),
        }
        let required_direction = match event.kind {
            ContractEventKind::Listed => None,
            ContractEventKind::SaleConfirmed => Some(ContractItemDirection::Offered),
            ContractEventKind::PurchaseConfirmed => Some(ContractItemDirection::Requested),
        };
        if required_direction.is_some_and(|direction| !evaluator.has_matching_ship_item(direction))
        {
            return Ok(NotificationResolution::Complete);
        }
        let primary_item = match evaluator.primary_display_item().await {
            PrimaryDisplayItem::Resolved(item) => item,
            PrimaryDisplayItem::Deferred => return Ok(NotificationResolution::Deferred),
        };
        let message = contract_notification_message(event, primary_item);
        let ping = matches!(action, ContractEventAction::PostAndPing);
        let Some(prepared) = self
            .store
            .prepare_delivery(subscription, event, &message, ping)
            .await?
        else {
            return Ok(NotificationResolution::Complete);
        };
        match notifications.delivery.send(prepared.clone()).await {
            Ok(message_id) => {
                self.store
                    .mark_delivery_sent(prepared.delivery_id, &message_id)
                    .await?
            }
            Err(error) => warn!(
                subscription_id = %subscription.id,
                contract_id = event.contract.contract_id,
                "contract delivery remains prepared after Discord failure: {error}"
            ),
        }
        Ok(NotificationResolution::Complete)
    }

    async fn regions(&self) -> Result<Vec<i64>, ContractCollectionError> {
        let cached = self.store.cache("regions").await?;
        if let Some(value) = self.fresh_cached("regions", &cached)? {
            return Ok(value.0);
        }
        let etag = cached
            .as_ref()
            .and_then(|cached| cached.metadata.etag.clone());
        self.ensure_esi_limiter_allows_requests().await?;
        let response = self
            .record_esi_result(self.esi.regions(etag.as_deref()).await)
            .await?;
        Ok(self.resolve_response("regions", cached, response).await?.0)
    }

    async fn contract_page(
        &self,
        region_id: i64,
        page: u32,
    ) -> Result<(Vec<PublicContract>, CacheMetadata), ContractCollectionError> {
        let key = format!("contracts/public/{region_id}/page/{page}");
        let cached = self.store.cache(&key).await?;
        if let Some(value) = self.fresh_cached(&key, &cached)? {
            return Ok(value);
        }
        let etag = cached
            .as_ref()
            .and_then(|cached| cached.metadata.etag.clone());
        self.ensure_esi_limiter_allows_requests().await?;
        let response = self
            .record_esi_result(
                self.esi
                    .public_contracts_page(region_id, page, etag.as_deref())
                    .await,
            )
            .await?;
        self.resolve_response(&key, cached, response).await
    }

    async fn contract_items(
        &self,
        contract_id: i64,
    ) -> Result<Vec<PublicContractItem>, ContractCollectionError> {
        let key = format!("contracts/public/items/{contract_id}");
        let cached = self.store.cache(&key).await?;
        if let Some(value) = self.fresh_cached(&key, &cached)? {
            return Ok(value.0);
        }
        let etag = cached
            .as_ref()
            .and_then(|cached| cached.metadata.etag.clone());
        self.ensure_esi_limiter_allows_requests().await?;
        let response = self
            .record_esi_result(
                self.esi
                    .public_contract_items(contract_id, etag.as_deref())
                    .await,
            )
            .await?;
        Ok(self.resolve_response(&key, cached, response).await?.0)
    }

    async fn ensure_esi_limiter_allows_requests(&self) -> Result<(), ContractCollectionError> {
        if let Some(retry_after) = self.store.active_esi_limiter_deadline().await? {
            return Err(EsiError::retryable(
                "persisted global ESI limiter boundary remains active",
                Some(retry_after),
            )
            .into());
        }
        Ok(())
    }

    async fn record_esi_result<T>(
        &self,
        result: Result<EsiResponse<T>, EsiError>,
    ) -> Result<EsiResponse<T>, ContractCollectionError> {
        match result {
            Ok(response) => {
                self.store.record_esi_limiter(&response.metadata).await?;
                Ok(response)
            }
            Err(error) => {
                self.store.record_esi_limiter(&error.metadata).await?;
                Err(error.into())
            }
        }
    }

    async fn record_item_probe_result(
        &self,
        result: Result<ContractItemProbe, EsiError>,
    ) -> Result<ContractItemProbe, ContractCollectionError> {
        match result {
            Ok(probe) => {
                self.store.record_esi_limiter(probe.metadata()).await?;
                Ok(probe)
            }
            Err(error) => {
                self.store.record_esi_limiter(&error.metadata).await?;
                Err(error.into())
            }
        }
    }

    fn fresh_cached<T>(
        &self,
        key: &str,
        cached: &Option<CachedResponse>,
    ) -> Result<Option<(T, CacheMetadata)>, ContractCollectionError>
    where
        T: DeserializeOwned,
    {
        let Some(cached) = cached else {
            return Ok(None);
        };
        if cached.metadata.is_fresh() {
            return serde_json::from_value(cached.response.clone())
                .map(|value| Some((value, cached.metadata.clone())))
                .map_err(|error| ContractCollectionError::Cache(error.to_string()));
        }
        if cached.metadata.retry_is_active() {
            return Err(EsiError::retryable(
                format!("ESI retry boundary remains active for {key}"),
                cached.metadata.retry_after,
            )
            .into());
        }
        Ok(None)
    }

    async fn resolve_response<T>(
        &self,
        key: &str,
        cached: Option<CachedResponse>,
        response: EsiResponse<T>,
    ) -> Result<(T, CacheMetadata), ContractCollectionError>
    where
        T: DeserializeOwned + Serialize,
    {
        let (value, metadata) = if response.not_modified {
            let cached = cached.as_ref().ok_or_else(|| {
                ContractCollectionError::Cache(format!(
                    "{key} returned 304 without a cached response"
                ))
            })?;
            (
                cached.response.clone(),
                merge_cache_metadata(&cached.metadata, response.metadata),
            )
        } else {
            (
                serde_json::to_value(response.value.ok_or_else(|| {
                    ContractCollectionError::Cache(format!("{key} returned no response body"))
                })?)
                .map_err(|error| ContractCollectionError::Cache(error.to_string()))?,
                response.metadata,
            )
        };
        self.store.save_cache(key, &value, &metadata).await?;
        let parsed = serde_json::from_value(value)
            .map_err(|error| ContractCollectionError::Cache(error.to_string()))?;
        Ok((parsed, metadata))
    }
}

fn apply_solar_system_context(
    context: &mut ContractObservationContext,
    response: Option<EsiResponse<ContractContextValue<i64>>>,
) {
    match response.and_then(|response| response.value) {
        Some(ContractContextValue::Resolved(solar_system_id)) => {
            context.solar_system_id = Some(solar_system_id);
            context.solar_system_resolution = ContractContextResolution::Resolved;
        }
        Some(ContractContextValue::DefinitivelyAbsent) => {
            context.solar_system_resolution = ContractContextResolution::DefinitivelyAbsent;
        }
        Some(ContractContextValue::Indeterminate) | None => {
            context.solar_system_resolution = ContractContextResolution::Indeterminate;
        }
    }
}

fn apply_security_context(
    context: &mut ContractObservationContext,
    response: Option<EsiResponse<ContractContextValue<f64>>>,
) {
    match response.and_then(|response| response.value) {
        Some(ContractContextValue::Resolved(security_status)) if security_status.is_finite() => {
            context.security_status = Some(security_status);
            context.security_status_resolution = ContractContextResolution::Resolved;
        }
        Some(ContractContextValue::DefinitivelyAbsent) => {
            context.security_status_resolution = ContractContextResolution::DefinitivelyAbsent;
        }
        Some(ContractContextValue::Resolved(_))
        | Some(ContractContextValue::Indeterminate)
        | None => {
            context.security_status_resolution = ContractContextResolution::Indeterminate;
        }
    }
}

fn apply_observed_affiliation_context(
    context: &mut ContractObservationContext,
    response: Option<EsiResponse<ContractContextValue<i64>>>,
) {
    match response.and_then(|response| response.value) {
        Some(ContractContextValue::Resolved(alliance_id)) => {
            context.observed_affiliation_alliance_id = Some(alliance_id);
            context.observed_affiliation_alliance_resolution = ContractContextResolution::Resolved;
        }
        Some(ContractContextValue::DefinitivelyAbsent) => {
            context.observed_affiliation_alliance_resolution =
                ContractContextResolution::DefinitivelyAbsent;
        }
        Some(ContractContextValue::Indeterminate) | None => {
            context.observed_affiliation_alliance_resolution =
                ContractContextResolution::Indeterminate;
        }
    }
}

fn contract_event_action(
    subscription: &ContractSubscription,
    event_kind: &ContractEventKind,
) -> ContractEventAction {
    match event_kind {
        ContractEventKind::Listed => subscription.event_actions.listed,
        ContractEventKind::SaleConfirmed => subscription.event_actions.sale_confirmed,
        ContractEventKind::PurchaseConfirmed => subscription.event_actions.purchase_confirmed,
    }
}

fn confirmed_contract_event_kind(
    resolution: &AwaitingContractResolution,
) -> Option<ContractEventKind> {
    if resolution.manifest.requested_items.is_empty()
        && !resolution.manifest.offered_items.is_empty()
        && positive_isk(resolution.contract.price)
    {
        return Some(ContractEventKind::SaleConfirmed);
    }
    if resolution.manifest.offered_items.is_empty()
        && !resolution.manifest.requested_items.is_empty()
        && positive_isk(resolution.contract.reward)
    {
        return Some(ContractEventKind::PurchaseConfirmed);
    }
    None
}

fn positive_isk(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ContractFilterMatch {
    Matched,
    Unmatched,
    Deferred,
}

struct ContractFilterEvaluator<'a> {
    event: &'a ContractEvent,
    ship_groups: &'a dyn ShipGroupResolver,
    group_cache: HashMap<i64, ShipGroupLookup>,
    matching_ship_items: HashSet<(ContractItemDirection, i64)>,
    matching_ship_items_deferred: bool,
}

impl<'a> ContractFilterEvaluator<'a> {
    fn new(event: &'a ContractEvent, ship_groups: &'a dyn ShipGroupResolver) -> Self {
        Self {
            event,
            ship_groups,
            group_cache: HashMap::new(),
            matching_ship_items: HashSet::new(),
            matching_ship_items_deferred: false,
        }
    }

    async fn matches(&mut self, node: &ContractFilterNode) -> ContractFilterMatch {
        self.matching_ship_items.clear();
        self.matching_ship_items_deferred = false;
        evaluate_contract_filter_node(node.clone(), self, true).await
    }

    async fn group_for_type(&mut self, type_id: i64) -> ShipGroupLookup {
        if let Some(group) = self.group_cache.get(&type_id).copied() {
            return group;
        }
        let group = self.ship_groups.group_for_type(type_id).await;
        self.group_cache.insert(type_id, group);
        group
    }

    async fn primary_display_item(&mut self) -> PrimaryDisplayItem<'a> {
        if self.matching_ship_items_deferred {
            return PrimaryDisplayItem::Deferred;
        }
        let mut primary: Option<(&PublicContractItem, usize)> = None;
        for (direction, item) in contract_items_with_direction(self.event) {
            if !self
                .matching_ship_items
                .contains(&(direction, item.record_id))
            {
                continue;
            }
            let priority = match self.group_for_type(item.type_id).await {
                ShipGroupLookup::Resolved(Some(group_id)) => {
                    strategic_ship_group_priority(group_id)
                }
                ShipGroupLookup::Resolved(None) => continue,
                ShipGroupLookup::TemporarilyUnavailable => return PrimaryDisplayItem::Deferred,
            };
            let candidate = (item, priority);
            if primary.as_ref().is_none_or(|(current, current_priority)| {
                (candidate.1, candidate.0.type_id, candidate.0.record_id)
                    < (*current_priority, current.type_id, current.record_id)
            }) {
                primary = Some(candidate);
            }
        }
        PrimaryDisplayItem::Resolved(primary.map(|(item, _)| item))
    }

    fn has_matching_ship_item(&self, direction: ContractItemDirection) -> bool {
        self.matching_ship_items
            .iter()
            .any(|(item_direction, _)| *item_direction == direction)
    }
}

enum PrimaryDisplayItem<'a> {
    Resolved(Option<&'a PublicContractItem>),
    Deferred,
}

struct ContractFilterContextPlan {
    result: ContractFilterMatch,
    requirements: ContractContextRequirements,
    waits_for_non_context: bool,
    confirmed_ship_items: HashSet<(ContractItemDirection, i64)>,
    potential_ship_items: HashSet<(ContractItemDirection, i64)>,
}

impl ContractFilterContextPlan {
    fn resolved(result: ContractFilterMatch) -> Self {
        Self {
            result,
            requirements: ContractContextRequirements::default(),
            waits_for_non_context: false,
            confirmed_ship_items: HashSet::new(),
            potential_ship_items: HashSet::new(),
        }
    }

    fn all_ship_items(&self) -> HashSet<(ContractItemDirection, i64)> {
        self.confirmed_ship_items
            .union(&self.potential_ship_items)
            .copied()
            .collect()
    }
}

fn plan_contract_filter_context<'borrow, 'event>(
    node: &'borrow ContractFilterNode,
    evaluator: &'borrow mut ContractFilterEvaluator<'event>,
    collect_matching_ship_items: bool,
) -> Pin<Box<dyn Future<Output = ContractFilterContextPlan> + Send + 'borrow>> {
    Box::pin(async move {
        match node {
            ContractFilterNode::Condition(condition) => {
                if collect_matching_ship_items {
                    match condition {
                        ContractFilterCondition::ItemTypes { direction, ids } => {
                            let confirmed_ship_items =
                                items_for_direction(evaluator.event, *direction)
                                    .iter()
                                    .filter(|item| ids.contains(&item.type_id))
                                    .map(|item| (*direction, item.record_id))
                                    .collect::<HashSet<_>>();
                            return ContractFilterContextPlan {
                                result: if confirmed_ship_items.is_empty() {
                                    ContractFilterMatch::Unmatched
                                } else {
                                    ContractFilterMatch::Matched
                                },
                                requirements: ContractContextRequirements::default(),
                                waits_for_non_context: false,
                                confirmed_ship_items,
                                potential_ship_items: HashSet::new(),
                            };
                        }
                        ContractFilterCondition::ShipGroups { direction, ids } => {
                            let mut confirmed_ship_items = HashSet::new();
                            let mut potential_ship_items = HashSet::new();
                            for item in items_for_direction(evaluator.event, *direction) {
                                match evaluator.group_for_type(item.type_id).await {
                                    ShipGroupLookup::Resolved(Some(group_id))
                                        if ids.contains(&group_id) =>
                                    {
                                        confirmed_ship_items.insert((*direction, item.record_id));
                                    }
                                    ShipGroupLookup::TemporarilyUnavailable => {
                                        potential_ship_items.insert((*direction, item.record_id));
                                    }
                                    ShipGroupLookup::Resolved(Some(_))
                                    | ShipGroupLookup::Resolved(None) => {}
                                }
                            }
                            return ContractFilterContextPlan {
                                result: if !confirmed_ship_items.is_empty() {
                                    ContractFilterMatch::Matched
                                } else if !potential_ship_items.is_empty() {
                                    ContractFilterMatch::Deferred
                                } else {
                                    ContractFilterMatch::Unmatched
                                },
                                requirements: ContractContextRequirements::default(),
                                waits_for_non_context: !potential_ship_items.is_empty(),
                                confirmed_ship_items,
                                potential_ship_items,
                            };
                        }
                        _ => {}
                    }
                }
                let result = evaluate_contract_filter_condition(condition, evaluator, false).await;
                if !matches!(result, ContractFilterMatch::Deferred) {
                    return ContractFilterContextPlan::resolved(result);
                }
                let requirements = condition.context_requirements();
                ContractFilterContextPlan {
                    result,
                    requirements,
                    waits_for_non_context: requirements.is_empty(),
                    confirmed_ship_items: HashSet::new(),
                    potential_ship_items: HashSet::new(),
                }
            }
            ContractFilterNode::And(nodes) => {
                let mut requirements = ContractContextRequirements::default();
                let mut deferred = false;
                let mut waits_for_non_context = false;
                let mut confirmed_ship_items = HashSet::new();
                let mut potential_ship_items = HashSet::new();
                for node in nodes {
                    let plan =
                        plan_contract_filter_context(node, evaluator, collect_matching_ship_items)
                            .await;
                    match plan.result {
                        ContractFilterMatch::Matched => {
                            requirements = requirements.union(plan.requirements);
                            waits_for_non_context |= plan.waits_for_non_context;
                            confirmed_ship_items.extend(plan.confirmed_ship_items);
                            potential_ship_items.extend(plan.potential_ship_items);
                        }
                        ContractFilterMatch::Unmatched => {
                            return ContractFilterContextPlan::resolved(
                                ContractFilterMatch::Unmatched,
                            );
                        }
                        ContractFilterMatch::Deferred => {
                            deferred = true;
                            requirements = requirements.union(plan.requirements);
                            waits_for_non_context |= plan.waits_for_non_context;
                            potential_ship_items.extend(plan.confirmed_ship_items);
                            potential_ship_items.extend(plan.potential_ship_items);
                        }
                    }
                }
                if deferred {
                    potential_ship_items.extend(confirmed_ship_items.drain());
                }
                ContractFilterContextPlan {
                    result: if deferred {
                        ContractFilterMatch::Deferred
                    } else {
                        ContractFilterMatch::Matched
                    },
                    requirements: if waits_for_non_context {
                        ContractContextRequirements::default()
                    } else {
                        requirements
                    },
                    waits_for_non_context,
                    confirmed_ship_items,
                    potential_ship_items,
                }
            }
            ContractFilterNode::Or(nodes) => {
                let mut matched_plans = Vec::new();
                let mut deferred_plans = Vec::new();
                for node in nodes {
                    let plan =
                        plan_contract_filter_context(node, evaluator, collect_matching_ship_items)
                            .await;
                    match plan.result {
                        ContractFilterMatch::Matched => matched_plans.push(plan),
                        ContractFilterMatch::Unmatched => {}
                        ContractFilterMatch::Deferred => deferred_plans.push(plan),
                    }
                }
                if matched_plans.is_empty() {
                    if deferred_plans.is_empty() {
                        return ContractFilterContextPlan::resolved(ContractFilterMatch::Unmatched);
                    }
                    let mut requirements = ContractContextRequirements::default();
                    let mut waits_for_non_context = false;
                    let mut potential_ship_items = HashSet::new();
                    for plan in deferred_plans {
                        requirements = requirements.union(plan.requirements);
                        waits_for_non_context |= plan.waits_for_non_context;
                        potential_ship_items.extend(plan.confirmed_ship_items);
                        potential_ship_items.extend(plan.potential_ship_items);
                    }
                    return ContractFilterContextPlan {
                        result: ContractFilterMatch::Deferred,
                        requirements: if waits_for_non_context {
                            ContractContextRequirements::default()
                        } else {
                            requirements
                        },
                        waits_for_non_context,
                        confirmed_ship_items: HashSet::new(),
                        potential_ship_items,
                    };
                }

                let mut requirements = ContractContextRequirements::default();
                let mut waits_for_non_context = false;
                let mut confirmed_ship_items = HashSet::new();
                let mut potential_ship_items = HashSet::new();
                for plan in matched_plans {
                    requirements = requirements.union(plan.requirements);
                    waits_for_non_context |= plan.waits_for_non_context;
                    confirmed_ship_items.extend(plan.confirmed_ship_items);
                    potential_ship_items.extend(plan.potential_ship_items);
                }
                for plan in deferred_plans {
                    let candidate_items = plan.all_ship_items();
                    if !candidate_items.is_subset(&confirmed_ship_items) {
                        requirements = requirements.union(plan.requirements);
                        waits_for_non_context |= plan.waits_for_non_context;
                        potential_ship_items.extend(candidate_items);
                    }
                }
                ContractFilterContextPlan {
                    result: ContractFilterMatch::Matched,
                    requirements: if waits_for_non_context {
                        ContractContextRequirements::default()
                    } else {
                        requirements
                    },
                    waits_for_non_context,
                    confirmed_ship_items,
                    potential_ship_items,
                }
            }
            ContractFilterNode::Not(node) => {
                let plan = plan_contract_filter_context(node, evaluator, false).await;
                ContractFilterContextPlan {
                    result: match plan.result {
                        ContractFilterMatch::Matched => ContractFilterMatch::Unmatched,
                        ContractFilterMatch::Unmatched => ContractFilterMatch::Matched,
                        ContractFilterMatch::Deferred => ContractFilterMatch::Deferred,
                    },
                    requirements: plan.requirements,
                    waits_for_non_context: plan.waits_for_non_context,
                    confirmed_ship_items: HashSet::new(),
                    potential_ship_items: HashSet::new(),
                }
            }
        }
    })
}

fn evaluate_contract_filter_node<'borrow, 'event>(
    node: ContractFilterNode,
    evaluator: &'borrow mut ContractFilterEvaluator<'event>,
    collect_matching_ship_items: bool,
) -> Pin<Box<dyn Future<Output = ContractFilterMatch> + Send + 'borrow>> {
    Box::pin(async move {
        match node {
            ContractFilterNode::Condition(condition) => {
                evaluate_contract_filter_condition(
                    &condition,
                    evaluator,
                    collect_matching_ship_items,
                )
                .await
            }
            ContractFilterNode::And(nodes) => {
                let initial_matches = evaluator.matching_ship_items.clone();
                let initial_matches_deferred = evaluator.matching_ship_items_deferred;
                let mut deferred = false;
                for node in nodes {
                    match evaluate_contract_filter_node(
                        node,
                        evaluator,
                        collect_matching_ship_items,
                    )
                    .await
                    {
                        ContractFilterMatch::Matched => {}
                        ContractFilterMatch::Unmatched => {
                            evaluator.matching_ship_items = initial_matches;
                            evaluator.matching_ship_items_deferred = initial_matches_deferred;
                            return ContractFilterMatch::Unmatched;
                        }
                        ContractFilterMatch::Deferred => deferred = true,
                    }
                }
                if deferred {
                    let branch_may_add_ship_items = collect_matching_ship_items
                        && evaluator.matching_ship_items != initial_matches;
                    evaluator.matching_ship_items = initial_matches;
                    evaluator.matching_ship_items_deferred = initial_matches_deferred
                        || evaluator.matching_ship_items_deferred
                        || branch_may_add_ship_items;
                    ContractFilterMatch::Deferred
                } else {
                    ContractFilterMatch::Matched
                }
            }
            ContractFilterNode::Or(nodes) => {
                let initial_matches = evaluator.matching_ship_items.clone();
                let initial_matches_deferred = evaluator.matching_ship_items_deferred;
                let mut matching_ship_items = initial_matches.clone();
                let mut matching_ship_items_deferred = initial_matches_deferred;
                let mut matched = false;
                let mut deferred = false;
                for node in nodes {
                    evaluator.matching_ship_items = initial_matches.clone();
                    evaluator.matching_ship_items_deferred = initial_matches_deferred;
                    match evaluate_contract_filter_node(
                        node,
                        evaluator,
                        collect_matching_ship_items,
                    )
                    .await
                    {
                        ContractFilterMatch::Matched => {
                            matched = true;
                            matching_ship_items
                                .extend(evaluator.matching_ship_items.iter().copied());
                            matching_ship_items_deferred |= evaluator.matching_ship_items_deferred;
                        }
                        ContractFilterMatch::Unmatched => {}
                        ContractFilterMatch::Deferred => {
                            deferred = true;
                            matching_ship_items_deferred |= evaluator.matching_ship_items_deferred
                                || (collect_matching_ship_items
                                    && evaluator.matching_ship_items != initial_matches);
                        }
                    }
                }
                if matched {
                    evaluator.matching_ship_items = matching_ship_items;
                    evaluator.matching_ship_items_deferred = matching_ship_items_deferred;
                    ContractFilterMatch::Matched
                } else if deferred {
                    evaluator.matching_ship_items = initial_matches;
                    evaluator.matching_ship_items_deferred = matching_ship_items_deferred;
                    ContractFilterMatch::Deferred
                } else {
                    evaluator.matching_ship_items = initial_matches;
                    evaluator.matching_ship_items_deferred = initial_matches_deferred;
                    ContractFilterMatch::Unmatched
                }
            }
            ContractFilterNode::Not(node) => {
                match evaluate_contract_filter_node(*node, evaluator, false).await {
                    ContractFilterMatch::Matched => ContractFilterMatch::Unmatched,
                    ContractFilterMatch::Unmatched => ContractFilterMatch::Matched,
                    ContractFilterMatch::Deferred => ContractFilterMatch::Deferred,
                }
            }
        }
    })
}

async fn evaluate_contract_filter_condition(
    condition: &ContractFilterCondition,
    evaluator: &mut ContractFilterEvaluator<'_>,
    collect_matching_ship_items: bool,
) -> ContractFilterMatch {
    let event = evaluator.event;
    match condition {
        ContractFilterCondition::EventKinds(kinds) => bool_match(kinds.contains(&event.kind)),
        ContractFilterCondition::OfferedItems => bool_match(!event.offered_items.is_empty()),
        ContractFilterCondition::RequestedItems => bool_match(!event.requested_items.is_empty()),
        ContractFilterCondition::ItemTypes { direction, ids } => {
            let matching_items: Vec<_> = items_for_direction(event, *direction)
                .iter()
                .filter(|item| ids.contains(&item.type_id))
                .collect();
            if collect_matching_ship_items {
                evaluator.matching_ship_items.extend(
                    matching_items
                        .iter()
                        .map(|item| (*direction, item.record_id)),
                );
            }
            bool_match(!matching_items.is_empty())
        }
        ContractFilterCondition::ShipGroups { direction, ids } => {
            let mut matched = false;
            let mut deferred = false;
            for item in items_for_direction(event, *direction) {
                match evaluator.group_for_type(item.type_id).await {
                    ShipGroupLookup::Resolved(Some(group_id)) if ids.contains(&group_id) => {
                        if collect_matching_ship_items {
                            evaluator
                                .matching_ship_items
                                .insert((*direction, item.record_id));
                        }
                        matched = true;
                    }
                    ShipGroupLookup::TemporarilyUnavailable => deferred = true,
                    ShipGroupLookup::Resolved(Some(_)) | ShipGroupLookup::Resolved(None) => {}
                }
            }
            if collect_matching_ship_items && deferred {
                evaluator.matching_ship_items_deferred = true;
            }
            if matched {
                ContractFilterMatch::Matched
            } else if deferred {
                ContractFilterMatch::Deferred
            } else {
                ContractFilterMatch::Unmatched
            }
        }
        ContractFilterCondition::MinimumIsk { direction, value } => {
            money_match(relevant_isk(event, *direction), |amount| amount >= *value)
        }
        ContractFilterCondition::MaximumIsk { direction, value } => {
            money_match(relevant_isk(event, *direction), |amount| amount <= *value)
        }
        ContractFilterCondition::Regions(ids) => bool_match(ids.contains(&event.region_id)),
        ContractFilterCondition::SolarSystems(ids) => context_match(
            event.context.solar_system_id,
            event.context.solar_system_resolution,
            |system_id| ids.contains(&system_id),
        ),
        ContractFilterCondition::SecurityRange { min, max } => context_match(
            event.context.security_status,
            event.context.security_status_resolution,
            |security_status| {
                security_status.is_finite() && security_status >= *min && security_status <= *max
            },
        ),
        ContractFilterCondition::LocationIds(ids) => {
            bool_match(ids.contains(&event.contract.start_location_id))
        }
        ContractFilterCondition::IssuerCharacters(ids) => {
            bool_match(ids.contains(&event.contract.issuer_id))
        }
        ContractFilterCondition::IssuerCorporations(ids) => {
            bool_match(ids.contains(&event.contract.issuer_corporation_id))
        }
        ContractFilterCondition::ObservedAffiliationAlliances(ids) => context_match(
            event.context.observed_affiliation_alliance_id,
            event.context.observed_affiliation_alliance_resolution,
            |alliance_id| ids.contains(&alliance_id),
        ),
        ContractFilterCondition::PersonalIssuance => bool_match(!event.contract.for_corporation),
        ContractFilterCondition::CorporationIssuance => bool_match(event.contract.for_corporation),
        ContractFilterCondition::TitleFragment(fragment) => bool_match(
            event
                .contract
                .title
                .as_deref()
                .is_some_and(|title| title.to_lowercase().contains(&fragment.to_lowercase())),
        ),
    }
}

fn bool_match(matches: bool) -> ContractFilterMatch {
    if matches {
        ContractFilterMatch::Matched
    } else {
        ContractFilterMatch::Unmatched
    }
}

fn money_match(amount: f64, predicate: impl FnOnce(f64) -> bool) -> ContractFilterMatch {
    bool_match(amount.is_finite() && amount >= 0.0 && predicate(amount))
}

fn context_match<T>(
    value: Option<T>,
    resolution: ContractContextResolution,
    predicate: impl FnOnce(T) -> bool,
) -> ContractFilterMatch {
    match value {
        Some(value) => bool_match(predicate(value)),
        None if matches!(resolution, ContractContextResolution::DefinitivelyAbsent) => {
            ContractFilterMatch::Unmatched
        }
        None => ContractFilterMatch::Deferred,
    }
}

fn items_for_direction(
    event: &ContractEvent,
    direction: ContractItemDirection,
) -> &[PublicContractItem] {
    match direction {
        ContractItemDirection::Offered => &event.offered_items,
        ContractItemDirection::Requested => &event.requested_items,
    }
}

fn contract_items_with_direction(
    event: &ContractEvent,
) -> impl Iterator<Item = (ContractItemDirection, &PublicContractItem)> {
    event
        .offered_items
        .iter()
        .map(|item| (ContractItemDirection::Offered, item))
        .chain(
            event
                .requested_items
                .iter()
                .map(|item| (ContractItemDirection::Requested, item)),
        )
}

fn relevant_isk(event: &ContractEvent, direction: ContractItemDirection) -> f64 {
    match direction {
        ContractItemDirection::Offered => event.contract.reward,
        ContractItemDirection::Requested => event.contract.price,
    }
}

fn strategic_ship_group_priority(group_id: i64) -> usize {
    SHIP_GROUP_PRIORITY
        .iter()
        .position(|priority| i64::from(*priority) == group_id)
        .unwrap_or(usize::MAX)
}

fn contract_notification_message(
    event: &ContractEvent,
    primary_item: Option<&PublicContractItem>,
) -> ContractNotificationMessage {
    let location_id = event.contract.start_location_id;
    let event_label = match event.kind {
        ContractEventKind::Listed => "Listed",
        ContractEventKind::SaleConfirmed => "Sale Confirmed",
        ContractEventKind::PurchaseConfirmed => "Purchase Confirmed",
    };
    let mut message = ContractNotificationMessage {
        title: primary_item.map_or_else(
            || format!("Public contract {}", event_label.to_lowercase()),
            |item| {
                format!(
                    "Public contract {}: Type {}",
                    event_label.to_lowercase(),
                    item.type_id
                )
            },
        ),
        description: event
            .contract
            .title
            .as_deref()
            .filter(|title| !title.is_empty())
            .map(sanitize_discord_text)
            .map(|title| bounded_text(&title, 4096)),
        fields: vec![
            ContractEmbedField {
                name: "Event".to_string(),
                value: event_label.to_string(),
                inline: true,
            },
            ContractEmbedField {
                name: "Offered Item".to_string(),
                value: compact_bundle_summary(&event.offered_items),
                inline: false,
            },
            ContractEmbedField {
                name: "Requested Item".to_string(),
                value: compact_bundle_summary(&event.requested_items),
                inline: false,
            },
            ContractEmbedField {
                name: "Requested ISK".to_string(),
                value: format!("{:.2} ISK", event.contract.price),
                inline: true,
            },
            ContractEmbedField {
                name: "Offered ISK".to_string(),
                value: format!("{:.2} ISK", event.contract.reward),
                inline: true,
            },
            ContractEmbedField {
                name: "Observed Location".to_string(),
                value: format!("Location ID {location_id}"),
                inline: true,
            },
            ContractEmbedField {
                name: "Issuer".to_string(),
                value: event.contract.issuer_id.to_string(),
                inline: true,
            },
            ContractEmbedField {
                name: "Contract ID".to_string(),
                value: event.contract.contract_id.to_string(),
                inline: true,
            },
            ContractEmbedField {
                name: "Contract Address".to_string(),
                value: format!("contract:0//{}", event.contract.contract_id),
                inline: false,
            },
        ],
    };
    if let Some(evidence) = &event.acceptance_evidence {
        message.fields.push(ContractEmbedField {
            name: "Acceptance Evidence".to_string(),
            value: format!(
                "Public through {}; absent at {}; 204 received {}",
                evidence.last_public_observed_at.to_rfc3339(),
                evidence.absence_observed_at.to_rfc3339(),
                evidence.evidence_response_at.to_rfc3339(),
            ),
            inline: false,
        });
        let latency_seconds = (Utc::now() - evidence.evidence_response_at)
            .num_seconds()
            .max(0);
        message.fields.push(ContractEmbedField {
            name: "Observed Notification Latency".to_string(),
            value: format!("{latency_seconds}s after acceptance evidence"),
            inline: true,
        });
    }
    bound_embed_message(&mut message);
    message
}

const MAX_EMBED_TITLE_CHARACTERS: usize = 256;
const MAX_EMBED_DESCRIPTION_CHARACTERS: usize = 4096;
const MAX_EMBED_FIELD_NAME_CHARACTERS: usize = 256;
const MAX_EMBED_FIELD_VALUE_CHARACTERS: usize = 1024;
const MAX_EMBED_CHARACTERS: usize = 6000;
const MAX_BUNDLE_ITEMS_IN_EMBED: usize = 8;

fn compact_bundle_summary(items: &[PublicContractItem]) -> String {
    let visible = items
        .iter()
        .take(MAX_BUNDLE_ITEMS_IN_EMBED)
        .map(|item| format!("Type {} ×{}", item.type_id, item.quantity))
        .collect::<Vec<_>>()
        .join(", ");
    let hidden = items.len().saturating_sub(MAX_BUNDLE_ITEMS_IN_EMBED);
    if visible.is_empty() {
        "None".to_string()
    } else if hidden == 0 {
        visible
    } else {
        format!("{visible}\n+{hidden} more")
    }
}

fn bound_embed_message(message: &mut ContractNotificationMessage) {
    message.title = bounded_text(&message.title, MAX_EMBED_TITLE_CHARACTERS);
    message.description = message
        .description
        .as_deref()
        .map(|description| bounded_text(description, MAX_EMBED_DESCRIPTION_CHARACTERS));
    for field in &mut message.fields {
        field.name = bounded_text(&field.name, MAX_EMBED_FIELD_NAME_CHARACTERS);
        field.value = bounded_text(&field.value, MAX_EMBED_FIELD_VALUE_CHARACTERS);
    }
    let fixed_characters = message.title.chars().count()
        + message
            .fields
            .iter()
            .map(|field| field.name.chars().count() + field.value.chars().count())
            .sum::<usize>();
    let description_budget = MAX_EMBED_CHARACTERS.saturating_sub(fixed_characters);
    message.description = message
        .description
        .as_deref()
        .filter(|_| description_budget > 0)
        .map(|description| bounded_text(description, description_budget));
}

fn bounded_text(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    if limit == 0 {
        return String::new();
    }
    if limit <= 3 {
        return "…".repeat(limit);
    }
    format!("{}…", value.chars().take(limit - 1).collect::<String>())
}

pub fn sanitize_discord_text(value: &str) -> String {
    value.replace('@', "@\u{200b}")
}

fn merge_cache_metadata(cached: &CacheMetadata, response: CacheMetadata) -> CacheMetadata {
    CacheMetadata {
        etag: response.etag.or_else(|| cached.etag.clone()),
        expires_at: response.expires_at.or(cached.expires_at),
        last_modified: response.last_modified.or(cached.last_modified),
        expected_pages: response.expected_pages.or(cached.expected_pages),
        error_limit_remain: response.error_limit_remain.or(cached.error_limit_remain),
        error_limit_reset: response.error_limit_reset.or(cached.error_limit_reset),
        rate_limit_group: response
            .rate_limit_group
            .or_else(|| cached.rate_limit_group.clone()),
        rate_limit_limit: response
            .rate_limit_limit
            .or_else(|| cached.rate_limit_limit.clone()),
        rate_limit_remaining: response
            .rate_limit_remaining
            .or(cached.rate_limit_remaining),
        rate_limit_used: response.rate_limit_used.or(cached.rate_limit_used),
        retry_after: response.retry_after.or(cached.retry_after),
    }
}

fn content_hash<T: Serialize>(value: &T) -> Result<String, ContractCollectionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| ContractCollectionError::Cache(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub async fn run_contract_collection_loop(
    database_url: String,
    interval: Duration,
    esi_timeout: Duration,
) {
    run_contract_collection_loop_inner(database_url, interval, esi_timeout, None).await;
}

pub fn spawn_contract_collection_loop_with_notifications(
    database_url: String,
    store_handle: ContractStoreHandle,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_contract_collection_loop_with_notifications(
        database_url,
        store_handle,
        interval,
        esi_timeout,
        ship_groups,
        delivery,
    ))
}

pub async fn run_contract_collection_loop_with_notifications(
    database_url: String,
    store_handle: ContractStoreHandle,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
) {
    let notifications = ContractNotifications {
        ship_groups,
        delivery,
    };
    loop {
        let mut delay = interval;
        match ContractCollectionStore::connect(&database_url).await {
            Ok(store) => {
                let store = Arc::new(store);
                *store_handle.write().await = Some(store.clone());
                match HttpPublicContractEsi::new(esi_timeout) {
                    Ok(esi) => {
                        let collector = ContractCollector::new((*store).clone(), Arc::new(esi))
                            .with_notifications(
                                notifications.ship_groups.clone(),
                                notifications.delivery.clone(),
                            );
                        match collector.collect_cycle().await {
                            Ok(report) => {
                                delay = collection_retry_delay(interval, report.retry_after);
                                info!(
                                    regions = report.regions.len(),
                                    "contract collection cycle finished"
                                )
                            }
                            Err(error) => {
                                delay = collection_retry_delay(interval, error.retry_after());
                                warn!("contract collection paused after failure: {error}")
                            }
                        }
                    }
                    Err(error) => warn!("contract collection HTTP client unavailable: {error}"),
                }
            }
            Err(error) => {
                *store_handle.write().await = None;
                warn!("contract collection database unavailable; retrying without an in-memory fallback: {error}")
            }
        }
        tokio::time::sleep(delay).await;
    }
}

async fn run_contract_collection_loop_inner(
    database_url: String,
    interval: Duration,
    esi_timeout: Duration,
    notifications: Option<ContractNotifications>,
) {
    loop {
        let mut delay = interval;
        match ContractCollectionStore::connect(&database_url).await {
            Ok(store) => {
                let esi = match HttpPublicContractEsi::new(esi_timeout) { Ok(esi) => Arc::new(esi), Err(error) => { warn!("contract collection HTTP client unavailable: {error}"); tokio::time::sleep(interval).await; continue; } };
                let mut collector = ContractCollector::new(store, esi);
                if let Some(notifications) = &notifications {
                    collector = collector.with_notifications(
                        notifications.ship_groups.clone(),
                        notifications.delivery.clone(),
                    );
                }
                match collector.collect_cycle().await {
                    Ok(report) => {
                        delay = collection_retry_delay(interval, report.retry_after);
                        info!(regions = report.regions.len(), "contract collection cycle finished")
                    }
                    Err(error) => {
                        delay = collection_retry_delay(interval, error.retry_after());
                        warn!("contract collection paused after failure: {error}")
                    }
                }
            }
            Err(error) => warn!("contract collection database unavailable; retrying without an in-memory fallback: {error}"),
        }
        tokio::time::sleep(delay).await;
    }
}

fn collection_retry_delay(interval: Duration, retry_after: Option<DateTime<Utc>>) -> Duration {
    let retry_delay = retry_after
        .and_then(|retry_after| (retry_after - Utc::now()).to_std().ok())
        .unwrap_or_default();
    interval.max(retry_delay)
}
