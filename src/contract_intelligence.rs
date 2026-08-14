use crate::config::SystemRange;
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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
const CONTRACT_COLLECTOR_USER_AGENT: &str = "killbot-rust Global Public Contract Intelligence";
const ESI_BODY_MAX_ATTEMPTS: usize = 3;
const ESI_BODY_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const MAX_EMBED_CONTEXT_ENRICHMENTS_PER_CYCLE: usize = 8;
const DISCORD_NONCE_ENFORCEMENT_WINDOW: ChronoDuration = ChronoDuration::minutes(5);
const DEFAULT_COLLECTION_RECOVERY_GAP: ChronoDuration = ChronoDuration::minutes(15);
const REGION_DISCOVERY_RESOURCE_KEY: &str = "esi:regions";
const METERS_PER_LIGHT_YEAR: f64 = 9_460_730_472_580_800.0;

fn regional_snapshot_resource_key(region_id: i64) -> String {
    format!("esi:public-contracts:region:{region_id}")
}

fn embed_context_resource_key(contract_id: i64) -> String {
    format!("contract-embed-context:{contract_id}")
}

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
            let reset_seconds = if self.error_limit_remain.unwrap_or(1) <= 0 {
                self.error_limit_reset
            } else if self.rate_limit_remaining.unwrap_or(1) <= 0 {
                self.rate_limit_limit
                    .as_deref()
                    .and_then(rate_limit_window_seconds)
            } else {
                None
            };
            reset_seconds.map(|seconds| Utc::now() + ChronoDuration::seconds(seconds))
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
    NoContent(CacheMetadata),
    NotFound(CacheMetadata),
}

impl ContractItemProbe {
    fn metadata(&self) -> &CacheMetadata {
        match self {
            Self::Available(response) => &response.metadata,
            Self::NoContent(metadata) => metadata,
            Self::NotFound(metadata) => metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractContextValue<T> {
    Resolved(T),
    DefinitivelyAbsent,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct SolarSystemPosition {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Eq for SolarSystemPosition {}

impl SolarSystemPosition {
    fn is_finite(self) -> bool {
        self.x.is_finite() && self.y.is_finite() && self.z.is_finite()
    }

    fn distance_in_light_years(self, other: Self) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt() / METERS_PER_LIGHT_YEAR
    }
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

    /// Public presentation data gathered while a contract remains observable.
    /// Implementations that cannot resolve it safely return an empty context;
    /// notification delivery must not depend on enrichment.
    async fn observed_contract_embed_context(
        &self,
        _contract: &PublicContract,
        _items: &[PublicContractItem],
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        Ok(EsiResponse::fresh(
            ContractEmbedContext::default(),
            CacheMetadata::cached_for_seconds(0),
        ))
    }

    /// The collector supplies a persisted rate-limit boundary for the HTTP implementation.
    /// Test and alternate implementations can retain the aggregate-only default.
    async fn observed_contract_embed_context_limited(
        &self,
        contract: &PublicContract,
        items: &[PublicContractItem],
        _limiter: &dyn ContractContextLimiter,
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        self.observed_contract_embed_context(contract, items).await
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

    /// The public ESI solar-system endpoint exposes immutable Cartesian coordinates in metres.
    /// The collector owns durable caching and limiter accounting for these lookups.
    async fn solar_system_position(
        &self,
        _solar_system_id: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<SolarSystemPosition>, EsiError> {
        Err(EsiError::retryable(
            "solar-system position lookup is unavailable",
            None,
        ))
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

#[async_trait]
pub trait ContractContextLimiter: Send + Sync {
    async fn request_allowed(&self) -> Result<bool, EsiError>;
    async fn record_response(&self, metadata: &CacheMetadata) -> Result<(), EsiError>;
}

struct NoopContractContextLimiter;

#[async_trait]
impl ContractContextLimiter for NoopContractContextLimiter {
    async fn request_allowed(&self) -> Result<bool, EsiError> {
        Ok(true)
    }

    async fn record_response(&self, _metadata: &CacheMetadata) -> Result<(), EsiError> {
        Ok(())
    }
}

pub struct HttpPublicContractEsi {
    client: Client,
    base_url: String,
    context_cache: Mutex<HashMap<String, CachedResponse>>,
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
            context_cache: Mutex::new(HashMap::new()),
        })
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        etag: Option<&str>,
    ) -> Result<EsiResponse<T>, EsiError> {
        let url = format!("{}{}", self.base_url, path);
        for attempt in 0..ESI_BODY_MAX_ATTEMPTS {
            let mut request = self.client.get(&url);
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
            let body = match response.bytes().await {
                Ok(body) => body,
                Err(error) => {
                    if body_retry_allowed(attempt, &metadata) {
                        tokio::time::sleep(body_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(EsiError::from_metadata(error.to_string(), None, metadata));
                }
            };
            match serde_json::from_slice(&body) {
                Ok(value) => return Ok(EsiResponse::fresh(value, metadata)),
                Err(error) if error.is_eof() && body_retry_allowed(attempt, &metadata) => {
                    tokio::time::sleep(body_retry_delay(attempt)).await;
                }
                Err(error) => {
                    return Err(EsiError::from_metadata(
                        format!("error decoding response body: {error}"),
                        None,
                        metadata,
                    ));
                }
            }
        }
        unreachable!("ESI body attempt loop always returns on its final attempt")
    }

    async fn post_json<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<EsiResponse<T>, EsiError> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(body)
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
        let value = response
            .json()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        Ok(EsiResponse::fresh(value, metadata))
    }

    async fn guarded_context_get<T: DeserializeOwned>(
        &self,
        path: &str,
        etag: Option<&str>,
        limiter: &dyn ContractContextLimiter,
    ) -> Result<EsiResponse<T>, EsiError> {
        if !limiter.request_allowed().await? {
            return Err(EsiError::retryable(
                "persisted global ESI limiter boundary remains active",
                None,
            ));
        }
        let response = self.get(path, etag).await;
        match &response {
            Ok(response) => limiter.record_response(&response.metadata).await?,
            Err(error) => limiter.record_response(&error.metadata).await?,
        }
        response
    }

    async fn guarded_context_post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
        limiter: &dyn ContractContextLimiter,
    ) -> Result<EsiResponse<T>, EsiError> {
        if !limiter.request_allowed().await? {
            return Err(EsiError::retryable(
                "persisted global ESI limiter boundary remains active",
                None,
            ));
        }
        let response = self.post_json(path, body).await;
        match &response {
            Ok(response) => limiter.record_response(&response.metadata).await?,
            Err(error) => limiter.record_response(&error.metadata).await?,
        }
        response
    }

    async fn cached_context_get<T: DeserializeOwned>(
        &self,
        cache_key: &str,
        path: &str,
        limiter: &dyn ContractContextLimiter,
    ) -> Result<Option<EsiResponse<T>>, EsiError> {
        let cached = self.context_cache.lock().await.get(cache_key).cloned();
        if let Some(cached) = cached.as_ref().filter(|cached| cached.metadata.is_fresh()) {
            return cached_context_response(cached.clone())
                .map(Some)
                .map_err(|error| EsiError::retryable(error.to_string(), None));
        }
        let etag = cached
            .as_ref()
            .and_then(|cached| cached.metadata.etag.as_deref());
        let response = match self.guarded_context_get::<Value>(path, etag, limiter).await {
            Ok(response) => response,
            Err(error)
                if error.is_not_found()
                    || matches!(
                        error.status,
                        Some(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
                    ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let cached = if response.not_modified {
            let cached = cached.ok_or_else(|| {
                EsiError::retryable(
                    format!("{path} returned 304 without a cached representation"),
                    None,
                )
            })?;
            CachedResponse {
                response: cached.response,
                metadata: merge_cache_metadata(&cached.metadata, response.metadata),
            }
        } else {
            CachedResponse {
                response: response.value.ok_or_else(|| {
                    EsiError::retryable(format!("{path} returned no representation"), None)
                })?,
                metadata: response.metadata,
            }
        };
        self.context_cache
            .lock()
            .await
            .insert(cache_key.to_string(), cached.clone());
        cached_context_response(cached)
            .map(Some)
            .map_err(|error| EsiError::retryable(error.to_string(), None))
    }

    async fn cached_context_post<T: DeserializeOwned, B: Serialize>(
        &self,
        cache_key: &str,
        path: &str,
        body: &B,
        limiter: &dyn ContractContextLimiter,
    ) -> Result<EsiResponse<T>, EsiError> {
        if let Some(cached) = self.context_cache.lock().await.get(cache_key).cloned() {
            if cached.metadata.is_fresh() {
                return cached_context_response(cached)
                    .map_err(|error| EsiError::retryable(error.to_string(), None));
            }
        }
        let response = self
            .guarded_context_post::<Value, _>(path, body, limiter)
            .await?;
        let cached = CachedResponse {
            response: response.value.ok_or_else(|| {
                EsiError::retryable(format!("{path} returned no representation"), None)
            })?,
            metadata: response.metadata,
        };
        self.context_cache
            .lock()
            .await
            .insert(cache_key.to_string(), cached.clone());
        cached_context_response(cached)
            .map_err(|error| EsiError::retryable(error.to_string(), None))
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
            StatusCode::NO_CONTENT => Ok(ContractItemProbe::NoContent(metadata)),
            StatusCode::NOT_FOUND => Ok(ContractItemProbe::NotFound(metadata)),
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

    async fn solar_system_position(
        &self,
        solar_system_id: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<SolarSystemPosition>, EsiError> {
        #[derive(Deserialize)]
        struct SolarSystem {
            position: Option<SolarSystemPosition>,
        }

        let response = self
            .get::<SolarSystem>(&format!("universe/systems/{solar_system_id}/"), etag)
            .await?;
        let metadata = response.metadata;
        if response.not_modified {
            return Ok(EsiResponse::not_modified(metadata));
        }
        let position = response
            .value
            .and_then(|system| system.position)
            .filter(|position| position.is_finite())
            .ok_or_else(|| {
                EsiError::from_metadata(
                    "ESI solar-system response omitted a finite position",
                    None,
                    metadata.clone(),
                )
            })?;
        Ok(EsiResponse::fresh(position, metadata))
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

    async fn observed_contract_embed_context(
        &self,
        contract: &PublicContract,
        items: &[PublicContractItem],
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        self.observed_contract_embed_context_limited(contract, items, &NoopContractContextLimiter)
            .await
    }

    async fn observed_contract_embed_context_limited(
        &self,
        contract: &PublicContract,
        items: &[PublicContractItem],
        limiter: &dyn ContractContextLimiter,
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        #[derive(Deserialize)]
        struct Station {
            name: String,
            system_id: i64,
        }
        #[derive(Deserialize)]
        struct SolarSystem {
            name: String,
            security_status: f64,
            constellation_id: i64,
            position: Option<SolarSystemPosition>,
        }
        #[derive(Deserialize)]
        struct Constellation {
            region_id: i64,
        }
        #[derive(Deserialize)]
        struct Region {
            name: String,
        }
        #[derive(Deserialize)]
        struct Affiliation {
            alliance_id: Option<i64>,
        }
        #[derive(Deserialize)]
        struct Name {
            id: i64,
            name: String,
        }

        let mut context = ContractEmbedContext {
            observed_at: Some(Utc::now()),
            ..ContractEmbedContext::default()
        };
        let mut metadata = CacheMetadata::cached_for_seconds(0);
        let location_id = contract.start_location_id;
        if let Ok(system_id) = u32::try_from(location_id) {
            if (30_000_000..33_000_000).contains(&system_id) {
                context.location.solar_system_id = Some(i64::from(system_id));
            } else if let Some(response) = self
                .cached_context_get::<Station>(
                    &format!("universe/stations/{system_id}"),
                    &format!("universe/stations/{system_id}/"),
                    limiter,
                )
                .await?
            {
                metadata = merge_cache_metadata(&metadata, response.metadata);
                if let Some(station) = response.value {
                    context.location.location_name = Some(station.name);
                    context.location.location_kind = Some("Station".to_string());
                    context.location.solar_system_id = Some(station.system_id);
                }
            }
        }
        if let Some(system_id) = context.location.solar_system_id {
            if let Some(response) = self
                .cached_context_get::<SolarSystem>(
                    &format!("universe/systems/{system_id}"),
                    &format!("universe/systems/{system_id}/"),
                    limiter,
                )
                .await?
            {
                metadata = merge_cache_metadata(&metadata, response.metadata);
                if let Some(system) = response.value {
                    context.location.solar_system_name = Some(system.name);
                    context.location.security_status = system
                        .security_status
                        .is_finite()
                        .then_some(system.security_status);
                    context.location.solar_system_position =
                        system.position.filter(|position| position.is_finite());
                    if let Some(response) = self
                        .cached_context_get::<Constellation>(
                            &format!("universe/constellations/{}", system.constellation_id),
                            &format!("universe/constellations/{}/", system.constellation_id),
                            limiter,
                        )
                        .await?
                    {
                        metadata = merge_cache_metadata(&metadata, response.metadata);
                        if let Some(constellation) = response.value {
                            context.location.region_id = Some(constellation.region_id);
                            if let Some(response) = self
                                .cached_context_get::<Region>(
                                    &format!("universe/regions/{}", constellation.region_id),
                                    &format!("universe/regions/{}/", constellation.region_id),
                                    limiter,
                                )
                                .await?
                            {
                                metadata = merge_cache_metadata(&metadata, response.metadata);
                                if let Some(region) = response.value {
                                    context.location.region_name = Some(region.name);
                                }
                            }
                        }
                    }
                }
            }
        }
        let affiliation_ids = vec![contract.issuer_id];
        let affiliation = self
            .cached_context_post::<Vec<Affiliation>, _>(
                &format!("characters/affiliation/{}", contract.issuer_id),
                "characters/affiliation/",
                &affiliation_ids,
                limiter,
            )
            .await?;
        metadata = merge_cache_metadata(&metadata, affiliation.metadata);
        context.issuer_alliance_id = affiliation
            .value
            .and_then(|affiliations| affiliations.into_iter().next())
            .and_then(|affiliation| affiliation.alliance_id);

        let mut names = BTreeSet::new();
        names.insert(contract.issuer_id);
        names.insert(contract.issuer_corporation_id);
        if let Some(alliance_id) = context.issuer_alliance_id {
            names.insert(alliance_id);
        }
        names.extend(items.iter().map(|item| item.type_id));
        if !names.is_empty() {
            let name_ids = names.into_iter().collect::<Vec<_>>();
            let names_key = format!(
                "universe/names/{}",
                name_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let response = self
                .cached_context_post::<Vec<Name>, _>(
                    &names_key,
                    "universe/names/",
                    &name_ids,
                    limiter,
                )
                .await?;
            metadata = merge_cache_metadata(&metadata, response.metadata);
            for name in response.value.unwrap_or_default() {
                if name.id == contract.issuer_id {
                    context.issuer_character_name = Some(name.name);
                } else if name.id == contract.issuer_corporation_id {
                    context.issuer_corporation_name = Some(name.name);
                } else if Some(name.id) == context.issuer_alliance_id {
                    context.issuer_alliance_name = Some(name.name);
                } else {
                    context.item_names.insert(name.id, name.name);
                }
            }
        }
        Ok(EsiResponse::fresh(context, metadata))
    }
}

fn body_retry_allowed(attempt: usize, metadata: &CacheMetadata) -> bool {
    attempt + 1 < ESI_BODY_MAX_ATTEMPTS
        && metadata.error_limit_remain.unwrap_or(1) > 0
        && metadata.rate_limit_remaining.unwrap_or(1) > 0
        && metadata
            .collection_pause_until()
            .map(|pause_until| pause_until <= Utc::now())
            .unwrap_or(true)
}

fn body_retry_delay(attempt: usize) -> Duration {
    ESI_BODY_RETRY_BASE_DELAY * (1_u32 << attempt)
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
pub struct ContractCollectionFailure {
    pub id: i64,
    pub region_id: Option<i64>,
    pub contract_id: Option<i64>,
    pub resource_key: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub failure_kind: String,
    pub detail: String,
    pub retry_after: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContractPartyHistory {
    pub confirmed_sales: i64,
    pub confirmed_purchases: i64,
    pub unknown_closures: i64,
    pub most_recent_confirmed: Option<MostRecentConfirmedContractEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MostRecentConfirmedContractEvent {
    pub kind: ContractEventKind,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractResolutionState {
    AwaitingResolution,
    AcceptanceConfirmed,
    Expired,
    ClosedOutcomeUnknown,
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
    Expired,
    ClosedOutcomeUnknown,
}

impl ContractEventKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Listed => "listed",
            Self::SaleConfirmed => "sale_confirmed",
            Self::PurchaseConfirmed => "purchase_confirmed",
            Self::Expired => "expired",
            Self::ClosedOutcomeUnknown => "closed_outcome_unknown",
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
    PostAndPingEveryone,
}

impl Default for ContractEventAction {
    fn default() -> Self {
        Self::Ignore
    }
}

impl ContractEventAction {
    pub fn ping_type(&self) -> Option<ContractPingType> {
        match self {
            Self::PostAndPing => Some(ContractPingType::Here),
            Self::PostAndPingEveryone => Some(ContractPingType::Everyone),
            Self::Ignore | Self::Post => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractPingType {
    Here,
    Everyone,
}

impl Default for ContractPingType {
    fn default() -> Self {
        Self::Here
    }
}

impl ContractPingType {
    pub fn content(self) -> &'static str {
        match self {
            Self::Here => "@here",
            Self::Everyone => "@everyone",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Here => "here",
            Self::Everyone => "everyone",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEventActions {
    #[serde(default)]
    pub listed: ContractEventAction,
    #[serde(default)]
    pub sale_confirmed: ContractEventAction,
    #[serde(default)]
    pub purchase_confirmed: ContractEventAction,
    #[serde(default)]
    pub expired: ContractEventAction,
    #[serde(default)]
    pub closed_outcome_unknown: ContractEventAction,
}

impl Default for ContractEventActions {
    fn default() -> Self {
        Self {
            listed: ContractEventAction::Ignore,
            sale_confirmed: ContractEventAction::Ignore,
            purchase_confirmed: ContractEventAction::Ignore,
            expired: ContractEventAction::Ignore,
            closed_outcome_unknown: ContractEventAction::Ignore,
        }
    }
}

impl ContractEventActions {
    pub fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    pub fn configure_ping_type(&mut self, ping_type: ContractPingType) {
        let configured_action = match ping_type {
            ContractPingType::Here => ContractEventAction::PostAndPing,
            ContractPingType::Everyone => ContractEventAction::PostAndPingEveryone,
        };
        for action in [
            &mut self.listed,
            &mut self.sale_confirmed,
            &mut self.purchase_confirmed,
            &mut self.expired,
            &mut self.closed_outcome_unknown,
        ] {
            if action.ping_type().is_some() {
                *action = configured_action;
            }
        }
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
    LyRangeFrom(Vec<SystemRange>),
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
            Self::LyRangeFrom(ranges) => validate_contract_system_ranges(ranges),
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
            Self::LyRangeFrom(ranges) => ContractContextRequirements {
                solar_system: true,
                ly_ranges: ranges.clone(),
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

fn validate_contract_system_ranges(ranges: &[SystemRange]) -> Result<(), String> {
    if ranges.is_empty() {
        return Err("provide at least one light-year system range".to_string());
    }
    if ranges.iter().any(|range| range.system_id == 0) {
        return Err("light-year range system IDs must be positive integers".to_string());
    }
    if ranges
        .iter()
        .any(|range| !range.range.is_finite() || range.range <= 0.0)
    {
        return Err("light-year ranges must be finite, positive values".to_string());
    }
    Ok(())
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContractContextRequirements {
    pub solar_system: bool,
    pub security_status: bool,
    pub observed_affiliation: bool,
    pub ly_ranges: Vec<SystemRange>,
}

impl ContractContextRequirements {
    fn union(self, other: Self) -> Self {
        let mut ly_ranges = self.ly_ranges;
        for range in other.ly_ranges {
            if !ly_ranges.contains(&range) {
                ly_ranges.push(range);
            }
        }
        Self {
            solar_system: self.solar_system || other.solar_system,
            security_status: self.security_status || other.security_status,
            observed_affiliation: self.observed_affiliation || other.observed_affiliation,
            ly_ranges,
        }
    }

    fn is_empty(&self) -> bool {
        !self.solar_system
            && !self.security_status
            && !self.observed_affiliation
            && self.ly_ranges.is_empty()
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
    #[serde(default)]
    pub solar_system_position: Option<SolarSystemPosition>,
    #[serde(default)]
    pub solar_system_position_resolution: ContractContextResolution,
    #[serde(default)]
    pub range_center_positions: BTreeMap<u32, SolarSystemPosition>,
    pub security_status: Option<f64>,
    #[serde(default)]
    pub security_status_resolution: ContractContextResolution,
    pub observed_affiliation_alliance_id: Option<i64>,
    #[serde(default)]
    pub observed_affiliation_alliance_resolution: ContractContextResolution,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractLocationContext {
    pub location_name: Option<String>,
    pub location_kind: Option<String>,
    pub solar_system_id: Option<i64>,
    pub solar_system_name: Option<String>,
    #[serde(default)]
    pub solar_system_position: Option<SolarSystemPosition>,
    pub security_status: Option<f64>,
    pub region_id: Option<i64>,
    pub region_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEmbedContext {
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub location: ContractLocationContext,
    pub issuer_character_name: Option<String>,
    pub issuer_corporation_name: Option<String>,
    pub issuer_alliance_id: Option<i64>,
    pub issuer_alliance_name: Option<String>,
    #[serde(default)]
    pub item_names: BTreeMap<i64, String>,
}

impl ContractEmbedContext {
    fn merge_missing_from(&mut self, source: &Self) {
        self.observed_at = self.observed_at.or(source.observed_at);
        self.location.location_name = self
            .location
            .location_name
            .clone()
            .or_else(|| source.location.location_name.clone());
        self.location.location_kind = self
            .location
            .location_kind
            .clone()
            .or_else(|| source.location.location_kind.clone());
        self.location.solar_system_id = self
            .location
            .solar_system_id
            .or(source.location.solar_system_id);
        self.location.solar_system_name = self
            .location
            .solar_system_name
            .clone()
            .or_else(|| source.location.solar_system_name.clone());
        self.location.solar_system_position = self
            .location
            .solar_system_position
            .or(source.location.solar_system_position);
        self.location.security_status = self
            .location
            .security_status
            .or(source.location.security_status);
        self.location.region_id = self.location.region_id.or(source.location.region_id);
        self.location.region_name = self
            .location
            .region_name
            .clone()
            .or_else(|| source.location.region_name.clone());
        self.issuer_character_name = self
            .issuer_character_name
            .clone()
            .or_else(|| source.issuer_character_name.clone());
        self.issuer_corporation_name = self
            .issuer_corporation_name
            .clone()
            .or_else(|| source.issuer_corporation_name.clone());
        self.issuer_alliance_id = self.issuer_alliance_id.or(source.issuer_alliance_id);
        self.issuer_alliance_name = self
            .issuer_alliance_name
            .clone()
            .or_else(|| source.issuer_alliance_name.clone());
        for (id, name) in &source.item_names {
            self.item_names.entry(*id).or_insert_with(|| name.clone());
        }
    }
}

impl ContractObservationContext {
    fn normalize_resolutions(&mut self) {
        if self.solar_system_id.is_some() {
            self.solar_system_resolution = ContractContextResolution::Resolved;
        }
        if self
            .solar_system_position
            .is_some_and(SolarSystemPosition::is_finite)
        {
            self.solar_system_position_resolution = ContractContextResolution::Resolved;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer: Option<String>,
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
    pub ping_type: ContractPingType,
    pub nonce: String,
    pub enforce_nonce: bool,
    pub message: ContractNotificationMessage,
}

struct DeliveryOrdering {
    strategic_priority: usize,
    relevant_isk: f64,
    confirmed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryStatus {
    Prepared,
    Sent,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryRecord {
    pub id: i64,
    pub status: DeliveryStatus,
    pub discord_message_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryFailureKind {
    Transient,
    Ambiguous,
    Permanent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractDeliveryFailure {
    pub delivery_id: i64,
    pub subscription_id: String,
    pub contract_id: i64,
    pub status: DeliveryStatus,
    pub failure_kind: DeliveryFailureKind,
    pub detail: String,
    pub attempt_count: i32,
    pub last_attempt_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
struct DeferredContractMatch {
    subscription: ContractSubscription,
    event: ContractEvent,
}

#[derive(Clone, Debug)]
pub enum ContractDeliveryError {
    Transient(String),
    Ambiguous(String),
    Permanent(String),
}

impl ContractDeliveryError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self::Transient(message.into())
    }

    pub fn ambiguous(message: impl Into<String>) -> Self {
        Self::Ambiguous(message.into())
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent(message.into())
    }

    fn message(&self) -> &str {
        match self {
            Self::Transient(message) | Self::Ambiguous(message) | Self::Permanent(message) => {
                message
            }
        }
    }
}

impl Display for ContractDeliveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message())
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

pub trait ContractDeliveryClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

struct SystemContractDeliveryClock;

impl ContractDeliveryClock for SystemContractDeliveryClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[async_trait]
pub trait ContractPingLimiter: Send + Sync {
    async fn try_acquire(&self, channel_id: u64) -> bool;
}

struct UnrestrictedContractPingLimiter;

#[async_trait]
impl ContractPingLimiter for UnrestrictedContractPingLimiter {
    async fn try_acquire(&self, _channel_id: u64) -> bool {
        true
    }
}

pub struct AppStateContractPingLimiter {
    app_state: Arc<crate::config::AppState>,
}

impl AppStateContractPingLimiter {
    pub fn new(app_state: Arc<crate::config::AppState>) -> Self {
        Self { app_state }
    }
}

#[async_trait]
impl ContractPingLimiter for AppStateContractPingLimiter {
    async fn try_acquire(&self, channel_id: u64) -> bool {
        crate::config::try_acquire_channel_ping(&self.app_state.last_ping_times, channel_id).await
    }
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

fn cached_context_response<T: DeserializeOwned>(
    cached: CachedResponse,
) -> Result<EsiResponse<T>, serde_json::Error> {
    let value = serde_json::from_value(cached.response)?;
    Ok(EsiResponse::fresh(value, cached.metadata))
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

    pub async fn prune_successful_scan_diagnostics(&self) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query(
            "DELETE FROM regional_observations WHERE outcome = 'complete' AND observed_at < now() - interval '90 days'",
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    pub async fn unresolved_collection_failures(
        &self,
    ) -> Result<Vec<ContractCollectionFailure>, sqlx::Error> {
        sqlx::query("SELECT id, region_id, contract_id, resource_key, observed_at, failure_kind, detail, retry_after FROM contract_collection_failures WHERE resolved_at IS NULL AND classification IS NULL ORDER BY observed_at, id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(ContractCollectionFailure {
                    id: row.get("id"),
                    region_id: row.get("region_id"),
                    contract_id: row.get("contract_id"),
                    resource_key: row.get("resource_key"),
                    observed_at: row.get("observed_at"),
                    failure_kind: row.get("failure_kind"),
                    detail: row.get("detail"),
                    retry_after: row.get("retry_after"),
                })
            })
            .collect()
    }

    pub async fn classify_collection_failure(
        &self,
        failure_id: i64,
        classification: &str,
    ) -> Result<bool, sqlx::Error> {
        if classification.trim().is_empty() {
            return Err(sqlx::Error::Protocol(
                "collection failure classification must not be empty".to_string(),
            ));
        }
        let result = sqlx::query("UPDATE contract_collection_failures SET classification = $2, resolved_at = now() WHERE id = $1 AND resolved_at IS NULL AND classification IS NULL")
            .bind(failure_id)
            .bind(classification)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn observed_embed_context(
        &self,
        region_id: i64,
        contract_id: i64,
    ) -> Result<Option<ContractEmbedContext>, sqlx::Error> {
        sqlx::query_scalar::<_, Value>(
            "SELECT context FROM contract_observed_embed_contexts WHERE region_id = $1 AND contract_id = $2",
        )
        .bind(region_id)
        .bind(contract_id)
        .fetch_optional(&self.pool)
        .await?
        .map(|context| serde_json::from_value(context).map_err(json_to_sqlx))
        .transpose()
    }

    async fn save_observed_embed_context(
        &self,
        region_id: i64,
        contract_id: i64,
        context: &ContractEmbedContext,
    ) -> Result<(), sqlx::Error> {
        let mut context = context.clone();
        let observed_at = context.observed_at.unwrap_or_else(Utc::now);
        context.observed_at = Some(observed_at);
        let mut transaction = self.pool.begin().await?;
        sqlx::query("INSERT INTO contract_observed_embed_contexts (region_id, contract_id, context, observed_at) VALUES ($1,$2,$3,$4) ON CONFLICT (region_id, contract_id) DO NOTHING")
            .bind(region_id)
            .bind(contract_id)
            .bind(serde_json::to_value(context).map_err(json_to_sqlx)?)
            .bind(observed_at)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE contract_collection_failures SET resolved_at = now() WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'embed_context_enrichment' AND resolved_at IS NULL AND classification IS NULL")
            .bind(region_id)
            .bind(contract_id)
            .bind(embed_context_resource_key(contract_id))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn contract_party_history(
        &self,
        issuer_id: i64,
        issuer_corporation_id: i64,
    ) -> Result<(ContractPartyHistory, ContractPartyHistory), sqlx::Error> {
        let load = |identity_field: &'static str, identity: i64| async move {
            let query = format!(
                "SELECT \
                    count(*) FILTER (WHERE state = 'acceptance_confirmed' \
                        AND jsonb_array_length(COALESCE(manifest->'requested_items', '[]'::jsonb)) = 0 \
                        AND COALESCE((contract->>'price')::double precision, 0) > 0) AS confirmed_sales, \
                    count(*) FILTER (WHERE state = 'acceptance_confirmed' \
                        AND jsonb_array_length(COALESCE(manifest->'offered_items', '[]'::jsonb)) = 0 \
                        AND COALESCE((contract->>'reward')::double precision, 0) > 0) AS confirmed_purchases, \
                    count(*) FILTER (WHERE state = 'closed_outcome_unknown') AS unknown_closures \
                 FROM contract_resolution_cases \
                 WHERE (contract->>'{identity_field}')::bigint = $1 \
                   AND COALESCE(acceptance_evidence_at, updated_at) >= now() - interval '90 days' \
                   AND state IN ('acceptance_confirmed', 'closed_outcome_unknown')"
            );
            let counts = sqlx::query(&query)
                .bind(identity)
                .fetch_one(&self.pool)
                .await?;
            let latest_query = format!(
                "SELECT \
                    CASE WHEN jsonb_array_length(COALESCE(manifest->'requested_items', '[]'::jsonb)) = 0 \
                         AND COALESCE((contract->>'price')::double precision, 0) > 0 \
                         THEN 'sale_confirmed' ELSE 'purchase_confirmed' END AS event_kind, \
                    acceptance_evidence_at \
                 FROM contract_resolution_cases \
                 WHERE (contract->>'{identity_field}')::bigint = $1 \
                   AND state = 'acceptance_confirmed' \
                   AND acceptance_evidence_at >= now() - interval '90 days' \
                   AND ( \
                        (jsonb_array_length(COALESCE(manifest->'requested_items', '[]'::jsonb)) = 0 \
                         AND COALESCE((contract->>'price')::double precision, 0) > 0) \
                     OR (jsonb_array_length(COALESCE(manifest->'offered_items', '[]'::jsonb)) = 0 \
                         AND COALESCE((contract->>'reward')::double precision, 0) > 0) \
                   ) \
                 ORDER BY acceptance_evidence_at DESC LIMIT 1"
            );
            let latest = sqlx::query(&latest_query)
                .bind(identity)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| {
                    let event_kind = match row.get::<String, _>("event_kind").as_str() {
                        "sale_confirmed" => ContractEventKind::SaleConfirmed,
                        "purchase_confirmed" => ContractEventKind::PurchaseConfirmed,
                        value => {
                            return Err(sqlx::Error::Protocol(format!(
                                "unknown confirmed contract event kind: {value}"
                            )))
                        }
                    };
                    Ok(MostRecentConfirmedContractEvent {
                        kind: event_kind,
                        observed_at: row.get("acceptance_evidence_at"),
                    })
                })
                .transpose()?;
            Ok::<_, sqlx::Error>(ContractPartyHistory {
                confirmed_sales: counts.get("confirmed_sales"),
                confirmed_purchases: counts.get("confirmed_purchases"),
                unknown_closures: counts.get("unknown_closures"),
                most_recent_confirmed: latest,
            })
        };
        Ok((
            load("issuer_id", issuer_id).await?,
            load("issuer_corporation_id", issuer_corporation_id).await?,
        ))
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

    pub async fn delivery_failure(
        &self,
        delivery_id: i64,
    ) -> Result<Option<(DeliveryFailureKind, String)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT failure_kind, last_error FROM contract_outbound_deliveries WHERE id = $1 AND failure_kind IS NOT NULL AND failure_resolved_at IS NULL",
        )
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let kind = delivery_failure_kind_from_str(&row.get::<String, _>("failure_kind"))?;
            Ok((kind, row.get("last_error")))
        })
        .transpose()
    }

    pub async fn unresolved_delivery_failures(
        &self,
    ) -> Result<Vec<ContractDeliveryFailure>, sqlx::Error> {
        sqlx::query("SELECT id, subscription_id, contract_id, status, failure_kind, last_error, attempt_count, last_attempt_at FROM contract_outbound_deliveries WHERE failure_kind IS NOT NULL AND failure_resolved_at IS NULL ORDER BY last_attempt_at, id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(ContractDeliveryFailure {
                    delivery_id: row.get("id"),
                    subscription_id: row.get("subscription_id"),
                    contract_id: row.get("contract_id"),
                    status: delivery_status_from_str(&row.get::<String, _>("status"))?,
                    failure_kind: delivery_failure_kind_from_str(&row.get::<String, _>("failure_kind"))?,
                    detail: row.get("last_error"),
                    attempt_count: row.get("attempt_count"),
                    last_attempt_at: row.get("last_attempt_at"),
                })
            })
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
        ping_type: ContractPingType,
        ordering: DeliveryOrdering,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let delivery_id = sqlx::query_scalar::<_, i64>("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, ping_type, status, delivery_nonce, strategic_priority, relevant_isk, confirmed_at) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,'prepared',$10,$11,$12,$13 WHERE EXISTS (SELECT 1 FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND deleted_at IS NULL) ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind) DO NOTHING RETURNING id")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .bind(serde_json::to_value(event).map_err(json_to_sqlx)?)
            .bind(serde_json::to_value(message).map_err(json_to_sqlx)?)
            .bind(ping)
            .bind(ping_type.as_str())
            .bind("pending")
            .bind(i64::try_from(ordering.strategic_priority).unwrap_or(i64::MAX))
            .bind(ordering.relevant_isk)
            .bind(ordering.confirmed_at)
            .fetch_optional(&mut *transaction)
            .await?;
        if let Some(delivery_id) = delivery_id {
            sqlx::query(
                "UPDATE contract_outbound_deliveries SET delivery_nonce = $2 WHERE id = $1",
            )
            .bind(delivery_id)
            .bind(format!("ci-{delivery_id}"))
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await
    }

    async fn prepared_deliveries(&self) -> Result<Vec<PreparedContractDelivery>, sqlx::Error> {
        sqlx::query("SELECT id, guild_id, channel_id, subscription_id, contract_id, event_kind, message, ping, ping_type, delivery_nonce, nonce_window_until FROM contract_outbound_deliveries WHERE status = 'prepared' ORDER BY strategic_priority ASC, relevant_isk DESC, confirmed_at ASC, id ASC")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(prepared_contract_delivery_from_row)
            .collect()
    }

    async fn begin_delivery_attempt(
        &self,
        delivery_id: i64,
        attempted_at: DateTime<Utc>,
    ) -> Result<Option<PreparedContractDelivery>, sqlx::Error> {
        let nonce_window_until = attempted_at + DISCORD_NONCE_ENFORCEMENT_WINDOW;
        let row = sqlx::query("UPDATE contract_outbound_deliveries SET attempt_count = attempt_count + 1, first_attempt_at = COALESCE(first_attempt_at, $2), last_attempt_at = $2, nonce_window_until = COALESCE(nonce_window_until, $3) WHERE id = $1 AND status = 'prepared' RETURNING id, guild_id, channel_id, subscription_id, contract_id, event_kind, message, ping, ping_type, delivery_nonce, nonce_window_until")
            .bind(delivery_id)
            .bind(attempted_at)
            .bind(nonce_window_until)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let nonce_window_until = row.get::<Option<DateTime<Utc>>, _>("nonce_window_until");
            let mut delivery = prepared_contract_delivery_from_row(row)?;
            delivery.enforce_nonce = nonce_window_until.is_some_and(|until| until > attempted_at);
            Ok(delivery)
        })
        .transpose()
    }

    async fn mark_delivery_sent(
        &self,
        delivery_id: i64,
        discord_message_id: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET status = 'sent', discord_message_id = $2, sent_at = now(), failure_resolved_at = CASE WHEN failure_kind IS NULL THEN failure_resolved_at ELSE now() END WHERE id = $1 AND status = 'prepared'")
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

    async fn mark_delivery_failed(
        &self,
        delivery_id: i64,
        error: &ContractDeliveryError,
        failed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET status = 'failed', failure_kind = 'permanent', last_error = $2, failed_at = $3, failure_resolved_at = NULL WHERE id = $1 AND status = 'prepared'")
            .bind(delivery_id)
            .bind(sanitize_contract_failure_detail(error.message()))
            .bind(failed_at)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {delivery_id} was not prepared when Discord returned a permanent failure"
            )));
        }
        Ok(())
    }

    async fn mark_delivery_retryable(
        &self,
        delivery_id: i64,
        error: &ContractDeliveryError,
    ) -> Result<(), sqlx::Error> {
        let failure_kind = match error {
            ContractDeliveryError::Transient(_) => "transient",
            ContractDeliveryError::Ambiguous(_) => "ambiguous",
            ContractDeliveryError::Permanent(_) => {
                return Err(sqlx::Error::Protocol(
                    "permanent contract delivery failure cannot remain prepared".to_string(),
                ))
            }
        };
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET failure_kind = $2, last_error = $3, failure_resolved_at = NULL WHERE id = $1 AND status = 'prepared'")
            .bind(delivery_id)
            .bind(failure_kind)
            .bind(sanitize_contract_failure_detail(error.message()))
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {delivery_id} was not prepared when Discord returned a retryable failure"
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
        summaries: &[SummaryObservedContract],
        resolved_contracts: &[ObservedContract],
        manifest_failures: &[ManifestCollectionFailure],
        recovery_gap: ChronoDuration,
    ) -> Result<RecordComplete, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let now = Utc::now();
        sqlx::query("INSERT INTO regional_collection_metadata (region_id) VALUES ($1) ON CONFLICT (region_id) DO NOTHING").bind(region_id).execute(&mut *transaction).await?;
        let regional_metadata = sqlx::query(
            "SELECT baseline_at, last_complete_at FROM regional_collection_metadata WHERE region_id = $1 FOR UPDATE",
        )
        .bind(region_id)
        .fetch_one(&mut *transaction)
        .await?;
        let baseline_at: Option<DateTime<Utc>> = regional_metadata.get("baseline_at");
        let last_complete_at: Option<DateTime<Utc>> = regional_metadata.get("last_complete_at");
        let recovery_baseline = baseline_at.is_some()
            && last_complete_at.is_some_and(|last_complete_at| {
                now.signed_duration_since(last_complete_at) > recovery_gap
            });
        let resolved_by_id = resolved_contracts
            .iter()
            .map(|observed| (observed.contract.contract_id, observed))
            .collect::<HashMap<_, _>>();
        let previously_observed = sqlx::query_scalar::<_, i64>(
            "SELECT DISTINCT contract_id FROM presence_intervals WHERE region_id = $1",
        )
        .bind(region_id)
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
        let pending_by_id = sqlx::query("SELECT contract_id, first_observed_at, notify_listed_on_resolution FROM contract_manifest_pending WHERE region_id = $1")
            .bind(region_id)
            .fetch_all(&mut *transaction)
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get::<i64, _>("contract_id"),
                    (
                        row.get::<DateTime<Utc>, _>("first_observed_at"),
                        row.get::<bool, _>("notify_listed_on_resolution"),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut newly_observed = Vec::new();
        for summary in summaries {
            let pending = pending_by_id.get(&summary.contract.contract_id);
            let pending_first_observed_at = pending.map(|pending| pending.0);
            let pending_notification = pending.is_some_and(|pending| pending.1);
            let had_manifest = previously_observed.contains(&summary.contract.contract_id);
            let was_observed = had_manifest || pending.is_some();
            let facts = serde_json::to_value(&summary.contract).map_err(json_to_sqlx)?;
            sqlx::query("INSERT INTO public_contract_facts (fact_hash, contract_id, facts) VALUES ($1,$2,$3) ON CONFLICT (fact_hash) DO NOTHING")
                .bind(&summary.fact_hash)
                .bind(summary.contract.contract_id)
                .bind(facts)
                .execute(&mut *transaction)
                .await?;
            if let Some(observed) = resolved_by_id.get(&summary.contract.contract_id) {
                if baseline_at.is_some()
                    && !recovery_baseline
                    && (pending_notification || !was_observed)
                {
                    newly_observed.push((*observed).clone());
                }
                let manifest = serde_json::to_value(&observed.manifest).map_err(json_to_sqlx)?;
                sqlx::query("INSERT INTO contract_item_manifests (manifest_hash, manifest) VALUES ($1,$2) ON CONFLICT (manifest_hash) DO NOTHING")
                    .bind(&observed.manifest_hash)
                    .bind(manifest)
                    .execute(&mut *transaction)
                    .await?;
                sqlx::query("INSERT INTO presence_intervals (region_id, contract_id, fact_hash, manifest_hash, first_observed_at, last_observed_at) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (region_id, contract_id, fact_hash, manifest_hash) DO UPDATE SET last_observed_at = EXCLUDED.last_observed_at")
                    .bind(region_id)
                    .bind(observed.contract.contract_id)
                    .bind(&observed.fact_hash)
                    .bind(&observed.manifest_hash)
                    .bind(pending_first_observed_at.unwrap_or(now))
                    .bind(now)
                    .execute(&mut *transaction)
                    .await?;
                if pending.is_some() {
                    sqlx::query("DELETE FROM contract_manifest_pending WHERE region_id = $1 AND contract_id = $2")
                        .bind(region_id)
                        .bind(observed.contract.contract_id)
                        .execute(&mut *transaction)
                        .await?;
                    sqlx::query("UPDATE contract_collection_failures SET resolved_at = $3 WHERE region_id = $1 AND contract_id = $2 AND resource_key = $4 AND failure_kind = 'manifest_collection' AND resolved_at IS NULL AND classification IS NULL")
                        .bind(region_id)
                        .bind(observed.contract.contract_id)
                        .bind(now)
                        .bind(format!(
                            "contracts/public/items/{}",
                            observed.contract.contract_id
                        ))
                        .execute(&mut *transaction)
                        .await?;
                }
            } else {
                let notify_listed_on_resolution = !recovery_baseline
                    && (pending_notification || (baseline_at.is_some() && !was_observed));
                sqlx::query("INSERT INTO contract_manifest_pending (region_id, contract_id, fact_hash, first_observed_at, last_observed_at, notify_listed_on_resolution) VALUES ($1,$2,$3,$4,$4,$5) ON CONFLICT (region_id, contract_id) DO UPDATE SET fact_hash = EXCLUDED.fact_hash, last_observed_at = EXCLUDED.last_observed_at, notify_listed_on_resolution = EXCLUDED.notify_listed_on_resolution")
                    .bind(region_id)
                    .bind(summary.contract.contract_id)
                    .bind(&summary.fact_hash)
                    .bind(now)
                    .bind(notify_listed_on_resolution)
                    .execute(&mut *transaction)
                    .await?;
                if had_manifest {
                    sqlx::query("UPDATE presence_intervals SET last_observed_at = $3 WHERE ctid = (SELECT ctid FROM presence_intervals WHERE region_id = $1 AND contract_id = $2 ORDER BY last_observed_at DESC LIMIT 1)")
                        .bind(region_id)
                        .bind(summary.contract.contract_id)
                        .bind(now)
                        .execute(&mut *transaction)
                        .await?;
                }
            }
        }
        for failure in manifest_failures {
            sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail, retry_after) VALUES ($1,$2,$3,$4,'manifest_collection',$5,$6)")
                .bind(region_id)
                .bind(failure.contract_id)
                .bind(format!("contracts/public/items/{}", failure.contract_id))
                .bind(now)
                .bind(sanitize_contract_failure_detail(&failure.detail))
                .bind(failure.retry_after)
                .execute(&mut *transaction)
                .await?;
        }
        let observed_ids: Vec<_> = summaries
            .iter()
            .map(|observed| observed.contract.contract_id)
            .collect();
        sqlx::query("DELETE FROM contract_resolution_cases WHERE region_id = $1 AND state = 'awaiting_resolution' AND contract_id = ANY($2)")
            .bind(region_id)
            .bind(&observed_ids)
            .execute(&mut *transaction)
            .await?;
        let disappeared_pending = sqlx::query_scalar::<_, i64>("DELETE FROM contract_manifest_pending WHERE region_id = $1 AND NOT (contract_id = ANY($2)) RETURNING contract_id")
            .bind(region_id)
            .bind(&observed_ids)
            .fetch_all(&mut *transaction)
            .await?;
        for contract_id in disappeared_pending {
            sqlx::query("UPDATE contract_collection_failures SET resolved_at = $3 WHERE region_id = $1 AND contract_id = $2 AND resource_key = $4 AND failure_kind = 'manifest_collection' AND resolved_at IS NULL AND classification IS NULL")
                .bind(region_id)
                .bind(contract_id)
                .bind(now)
                .bind(format!("contracts/public/items/{contract_id}"))
                .execute(&mut *transaction)
                .await?;
        }
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
            let cached_probe_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT expires_at FROM esi_cache_metadata WHERE resource_key = $1",
            )
            .bind(format!("contracts/public/items/{contract_id}"))
            .fetch_optional(&mut *transaction)
            .await?
            .flatten();
            let next_probe_at = cached_probe_at
                .map(|expires_at| std::cmp::min(expires_at, contract.date_expired))
                .unwrap_or(now);
            if recovery_baseline {
                sqlx::query("UPDATE contract_resolution_cases SET suppresses_nonfinancial_notification = TRUE, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution'")
                    .bind(region_id)
                    .bind(contract_id)
                    .execute(&mut *transaction)
                    .await?;
            }
            let inserted = sqlx::query_scalar::<_, i64>("INSERT INTO contract_resolution_cases (region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, next_probe_at, state, suppresses_nonfinancial_notification) VALUES ($1,$2,$3,$4,$5,$6,$7,'awaiting_resolution',$8) ON CONFLICT (region_id, contract_id) DO NOTHING RETURNING contract_id")
                .bind(region_id)
                .bind(contract_id)
                .bind(serde_json::to_value(&contract).map_err(json_to_sqlx)?)
                .bind(serde_json::to_value(&manifest).map_err(json_to_sqlx)?)
                .bind(last_public_observed_at)
                .bind(now)
                .bind(next_probe_at)
                .bind(recovery_baseline)
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
        sqlx::query("UPDATE contract_collection_failures SET resolved_at = $2 WHERE region_id = $1 AND resource_key = $3 AND failure_kind = 'inconclusive_observation' AND resolved_at IS NULL AND classification IS NULL")
            .bind(region_id)
            .bind(now)
            .bind(regional_snapshot_resource_key(region_id))
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE regional_collection_metadata SET baseline_at = COALESCE(baseline_at, $2), complete_observations = complete_observations + 1, last_complete_at = $2, recovery_baseline_at = CASE WHEN $3 THEN $2 ELSE recovery_baseline_at END WHERE region_id = $1").bind(region_id).bind(now).bind(recovery_baseline).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(RecordComplete {
            baseline_established: baseline_at.is_none(),
            recovery_baseline,
            observed_contracts: summaries.len(),
            newly_observed,
            observed: resolved_contracts.to_vec(),
            retry_after: manifest_failures
                .iter()
                .filter_map(|failure| failure.retry_after)
                .max(),
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
            None,
            Some(&regional_snapshot_resource_key(region_id)),
            "inconclusive_observation",
            detail,
            retry_after,
        )
        .await
    }

    async fn record_failure(
        &self,
        region_id: Option<i64>,
        contract_id: Option<i64>,
        resource_key: Option<&str>,
        failure_kind: &str,
        detail: &str,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail, retry_after) VALUES ($1,$2,$3,$4,$5,$6,$7)")
            .bind(region_id)
            .bind(contract_id)
            .bind(resource_key)
            .bind(now)
            .bind(failure_kind)
            .bind(sanitize_contract_failure_detail(detail))
            .bind(retry_after)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn resolve_collection_failures(
        &self,
        region_id: Option<i64>,
        contract_id: Option<i64>,
        resource_key: Option<&str>,
        failure_kind: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE contract_collection_failures SET resolved_at = now() WHERE region_id IS NOT DISTINCT FROM $1 AND contract_id IS NOT DISTINCT FROM $2 AND resource_key IS NOT DISTINCT FROM $3 AND failure_kind = $4 AND resolved_at IS NULL AND classification IS NULL")
            .bind(region_id)
            .bind(contract_id)
            .bind(resource_key)
            .bind(failure_kind)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn awaiting_resolution_cases(
        &self,
    ) -> Result<Vec<AwaitingContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, suppresses_nonfinancial_notification FROM contract_resolution_cases WHERE state = 'awaiting_resolution' AND next_probe_at <= now() ORDER BY region_id, contract_id")
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
                    suppresses_nonfinancial_notification: row
                        .get("suppresses_nonfinancial_notification"),
                })
            })
            .collect()
    }

    async fn pending_terminal_notifications(
        &self,
    ) -> Result<Vec<TerminalContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, acceptance_evidence_at, state FROM contract_resolution_cases WHERE state <> 'awaiting_resolution' AND notification_pending = TRUE ORDER BY region_id, contract_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(TerminalContractResolution {
                    region_id: row.get("region_id"),
                    contract_id: row.get("contract_id"),
                    contract: serde_json::from_value(row.get("contract")).map_err(json_to_sqlx)?,
                    manifest: serde_json::from_value(row.get("manifest")).map_err(json_to_sqlx)?,
                    last_public_observed_at: row.get("last_public_observed_at"),
                    absence_observed_at: row.get("absence_observed_at"),
                    acceptance_evidence_at: row.get("acceptance_evidence_at"),
                    state: contract_resolution_state_from_str(&row.get::<String, _>("state"))?,
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
        let mut transaction = self.pool.begin().await?;
        sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = $3, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution'")
            .bind(region_id)
            .bind(contract_id)
            .bind(next_probe_at)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE contract_collection_failures SET resolved_at = now() WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'resolution_probe' AND resolved_at IS NULL AND classification IS NULL")
            .bind(region_id)
            .bind(contract_id)
            .bind(format!("contracts/public/items/{contract_id}"))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn resolve_terminal_resolution_failures(&self) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE contract_collection_failures AS failure SET resolved_at = now() FROM contract_resolution_cases AS resolution WHERE resolution.state <> 'awaiting_resolution' AND failure.region_id = resolution.region_id AND failure.contract_id = resolution.contract_id AND failure.resource_key = 'contracts/public/items/' || resolution.contract_id::text AND failure.failure_kind = 'resolution_probe' AND failure.resolved_at IS NULL AND failure.classification IS NULL")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn confirm_acceptance(
        &self,
        resolution: &AwaitingContractResolution,
        evidence_response_at: DateTime<Utc>,
        metadata: &CacheMetadata,
        notification_pending: bool,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let confirmed = sqlx::query_scalar::<_, i64>("UPDATE contract_resolution_cases SET state = 'acceptance_confirmed', acceptance_evidence_at = $3, acceptance_response_metadata = $4, notification_pending = $5, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution' RETURNING contract_id")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(evidence_response_at)
            .bind(serde_json::to_value(metadata).map_err(json_to_sqlx)?)
            .bind(notification_pending)
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
            sqlx::query("UPDATE contract_collection_failures SET resolved_at = now() WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'resolution_probe' AND resolved_at IS NULL AND classification IS NULL")
                .bind(resolution.region_id)
                .bind(resolution.contract_id)
                .bind(format!(
                    "contracts/public/items/{}",
                    resolution.contract_id
                ))
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(confirmed.is_some())
    }

    async fn resolve_nonfinancial_terminal(
        &self,
        resolution: &AwaitingContractResolution,
        state: ContractResolutionState,
        observed_at: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let (state_name, evidence_kind) = match state {
            ContractResolutionState::Expired => ("expired", "expired"),
            ContractResolutionState::ClosedOutcomeUnknown => {
                ("closed_outcome_unknown", "closed_outcome_unknown")
            }
            ContractResolutionState::AwaitingResolution
            | ContractResolutionState::AcceptanceConfirmed => {
                return Err(sqlx::Error::Protocol(
                    "nonfinancial terminal state must be expired or closed_outcome_unknown"
                        .to_string(),
                ))
            }
        };
        let mut transaction = self.pool.begin().await?;
        let resolved = sqlx::query_scalar::<_, i64>("UPDATE contract_resolution_cases SET state = $3, notification_pending = NOT suppresses_nonfinancial_notification, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution' RETURNING contract_id")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(state_name)
            .fetch_optional(&mut *transaction)
            .await?;
        if resolved.is_some() {
            sqlx::query("INSERT INTO contract_lifecycle_evidence (region_id, contract_id, evidence_kind, observed_at, detail) VALUES ($1,$2,$3,$4,$5)")
                .bind(resolution.region_id)
                .bind(resolution.contract_id)
                .bind(evidence_kind)
                .bind(observed_at)
                .bind(serde_json::json!({
                    "last_public_observed_at": resolution.last_public_observed_at,
                    "absence_observed_at": resolution.absence_observed_at,
                }))
                .execute(&mut *transaction)
                .await?;
            sqlx::query("UPDATE contract_collection_failures SET resolved_at = now() WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'resolution_probe' AND resolved_at IS NULL AND classification IS NULL")
                .bind(resolution.region_id)
                .bind(resolution.contract_id)
                .bind(format!(
                    "contracts/public/items/{}",
                    resolution.contract_id
                ))
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(resolved.is_some())
    }

    async fn mark_terminal_notifications_reported(
        &self,
        cases: &[ResolutionCaseKey],
    ) -> Result<(), sqlx::Error> {
        for case in cases {
            sqlx::query("UPDATE contract_resolution_cases SET notification_pending = FALSE, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state <> 'awaiting_resolution' AND notification_pending = TRUE")
                .bind(case.region_id)
                .bind(case.contract_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
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
    Ok(DeliveryRecord {
        id: row.get("id"),
        status: delivery_status_from_str(&row.get::<String, _>("status"))?,
        discord_message_id: row.get("discord_message_id"),
    })
}

fn delivery_status_from_str(value: &str) -> Result<DeliveryStatus, sqlx::Error> {
    match value {
        "prepared" => Ok(DeliveryStatus::Prepared),
        "sent" => Ok(DeliveryStatus::Sent),
        "failed" => Ok(DeliveryStatus::Failed),
        value => Err(sqlx::Error::Protocol(format!(
            "unknown delivery status: {value}"
        ))),
    }
}

fn delivery_failure_kind_from_str(value: &str) -> Result<DeliveryFailureKind, sqlx::Error> {
    match value {
        "transient" => Ok(DeliveryFailureKind::Transient),
        "ambiguous" => Ok(DeliveryFailureKind::Ambiguous),
        "permanent" => Ok(DeliveryFailureKind::Permanent),
        value => Err(sqlx::Error::Protocol(format!(
            "unknown contract delivery failure kind: {value}"
        ))),
    }
}

fn prepared_contract_delivery_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<PreparedContractDelivery, sqlx::Error> {
    let event_kind = match row.get::<String, _>("event_kind").as_str() {
        "listed" => ContractEventKind::Listed,
        "sale_confirmed" => ContractEventKind::SaleConfirmed,
        "purchase_confirmed" => ContractEventKind::PurchaseConfirmed,
        "expired" => ContractEventKind::Expired,
        "closed_outcome_unknown" => ContractEventKind::ClosedOutcomeUnknown,
        value => {
            return Err(sqlx::Error::Protocol(format!(
                "unknown contract delivery event kind: {value}"
            )))
        }
    };
    Ok(PreparedContractDelivery {
        delivery_id: row.get("id"),
        guild_id: row.get::<i64, _>("guild_id") as u64,
        channel_id: row.get::<i64, _>("channel_id") as u64,
        subscription_id: row.get("subscription_id"),
        contract_id: row.get("contract_id"),
        event_kind,
        ping: row.get("ping"),
        ping_type: contract_ping_type_from_str(&row.get::<String, _>("ping_type"))?,
        nonce: row.get("delivery_nonce"),
        enforce_nonce: false,
        message: serde_json::from_value(row.get("message")).map_err(json_to_sqlx)?,
    })
}

fn contract_ping_type_from_str(value: &str) -> Result<ContractPingType, sqlx::Error> {
    match value {
        "here" => Ok(ContractPingType::Here),
        "everyone" => Ok(ContractPingType::Everyone),
        value => Err(sqlx::Error::Protocol(format!(
            "unknown contract delivery ping type: {value}"
        ))),
    }
}

fn contract_resolution_record_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ContractResolutionRecord, sqlx::Error> {
    let state = contract_resolution_state_from_str(&row.get::<String, _>("state"))?;
    Ok(ContractResolutionRecord {
        region_id: row.get("region_id"),
        contract_id: row.get("contract_id"),
        state,
        last_public_observed_at: row.get("last_public_observed_at"),
        absence_observed_at: row.get("absence_observed_at"),
        acceptance_evidence_at: row.get("acceptance_evidence_at"),
    })
}

fn contract_resolution_state_from_str(value: &str) -> Result<ContractResolutionState, sqlx::Error> {
    Ok(match value {
        "awaiting_resolution" => ContractResolutionState::AwaitingResolution,
        "acceptance_confirmed" => ContractResolutionState::AcceptanceConfirmed,
        "expired" => ContractResolutionState::Expired,
        "closed_outcome_unknown" => ContractResolutionState::ClosedOutcomeUnknown,
        value => {
            return Err(sqlx::Error::Protocol(format!(
                "unknown resolution state: {value}"
            )))
        }
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
struct SummaryObservedContract {
    contract: PublicContract,
    fact_hash: String,
}

#[derive(Clone, Debug)]
struct ManifestCollectionFailure {
    contract_id: i64,
    detail: String,
    retry_after: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
struct AwaitingContractResolution {
    region_id: i64,
    contract_id: i64,
    contract: PublicContract,
    manifest: ItemManifest,
    last_public_observed_at: DateTime<Utc>,
    absence_observed_at: DateTime<Utc>,
    suppresses_nonfinancial_notification: bool,
}

#[derive(Clone, Debug)]
struct TerminalContractResolution {
    region_id: i64,
    contract_id: i64,
    contract: PublicContract,
    manifest: ItemManifest,
    last_public_observed_at: DateTime<Utc>,
    absence_observed_at: DateTime<Utc>,
    acceptance_evidence_at: Option<DateTime<Utc>>,
    state: ContractResolutionState,
}

#[derive(Clone, Copy, Debug)]
struct ResolutionCaseKey {
    region_id: i64,
    contract_id: i64,
}

#[derive(Clone, Debug)]
struct RecordComplete {
    baseline_established: bool,
    recovery_baseline: bool,
    observed_contracts: usize,
    newly_observed: Vec<ObservedContract>,
    observed: Vec<ObservedContract>,
    retry_after: Option<DateTime<Utc>>,
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
    RecoveryBaselineEstablished {
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
    #[serde(default)]
    pub embed_context: ContractEmbedContext,
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
    delivery_clock: Arc<dyn ContractDeliveryClock>,
    recovery_gap: ChronoDuration,
}

struct ContractNotifications {
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
}

enum NotificationResolution {
    Complete,
    Deferred,
}

#[derive(Default)]
struct ResolutionBatch {
    events: Vec<ContractEvent>,
    notification_cases: Vec<ResolutionCaseKey>,
    retry_after: Option<DateTime<Utc>>,
}

struct PersistedContractContextLimiter {
    store: ContractCollectionStore,
}

#[async_trait]
impl ContractContextLimiter for PersistedContractContextLimiter {
    async fn request_allowed(&self) -> Result<bool, EsiError> {
        self.store
            .active_esi_limiter_deadline()
            .await
            .map(|deadline| deadline.is_none())
            .map_err(|error| EsiError::retryable(error.to_string(), None))
    }

    async fn record_response(&self, metadata: &CacheMetadata) -> Result<(), EsiError> {
        self.store
            .record_esi_limiter(metadata)
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))
    }
}

impl ContractCollector {
    pub fn new(store: ContractCollectionStore, esi: Arc<dyn PublicContractEsi>) -> Self {
        Self {
            store,
            esi,
            notifications: None,
            delivery_clock: Arc::new(SystemContractDeliveryClock),
            recovery_gap: DEFAULT_COLLECTION_RECOVERY_GAP,
        }
    }

    pub fn with_notifications(
        self,
        ship_groups: Arc<dyn ShipGroupResolver>,
        delivery: Arc<dyn ContractDelivery>,
    ) -> Self {
        self.with_notifications_and_ping_limiter(
            ship_groups,
            delivery,
            Arc::new(UnrestrictedContractPingLimiter),
        )
    }

    pub fn with_notifications_and_ping_limiter(
        mut self,
        ship_groups: Arc<dyn ShipGroupResolver>,
        delivery: Arc<dyn ContractDelivery>,
        ping_limiter: Arc<dyn ContractPingLimiter>,
    ) -> Self {
        self.notifications = Some(ContractNotifications {
            ship_groups,
            delivery,
            ping_limiter,
        });
        self
    }

    pub fn with_delivery_clock(mut self, delivery_clock: Arc<dyn ContractDeliveryClock>) -> Self {
        self.delivery_clock = delivery_clock;
        self
    }

    pub fn with_recovery_gap(mut self, recovery_gap: ChronoDuration) -> Self {
        self.recovery_gap = recovery_gap.max(ChronoDuration::zero());
        self
    }

    pub async fn collect_cycle(&self) -> Result<CollectionReport, ContractCollectionError> {
        if let Some(notifications) = &self.notifications {
            self.deliver_prepared_notifications(notifications).await?;
        }
        self.ensure_esi_limiter_allows_requests().await?;
        let regions = match self.regions().await {
            Ok(regions) => {
                self.store
                    .resolve_collection_failures(
                        None,
                        None,
                        Some(REGION_DISCOVERY_RESOURCE_KEY),
                        "region_discovery",
                    )
                    .await?;
                regions
            }
            Err(error) => {
                self.store
                    .record_failure(
                        None,
                        None,
                        Some(REGION_DISCOVERY_RESOURCE_KEY),
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
        let mut remaining_embed_context_enrichments = MAX_EMBED_CONTEXT_ENRICHMENTS_PER_CYCLE;
        for region_id in regions {
            match self.collect_region(region_id).await {
                Ok(recorded) => {
                    let observed_contracts = recorded.observed_contracts;
                    retry_after = std::cmp::max(retry_after, recorded.retry_after);
                    let consumed = self
                        .snapshot_observed_contracts(
                            region_id,
                            &recorded.observed,
                            remaining_embed_context_enrichments,
                        )
                        .await?;
                    remaining_embed_context_enrichments =
                        remaining_embed_context_enrichments.saturating_sub(consumed);
                    events.extend(recorded.newly_observed.into_iter().map(|observed| {
                        ContractEvent {
                            region_id,
                            kind: ContractEventKind::Listed,
                            contract: observed.contract,
                            offered_items: observed.manifest.offered_items,
                            requested_items: observed.manifest.requested_items,
                            context: ContractObservationContext::default(),
                            acceptance_evidence: None,
                            embed_context: ContractEmbedContext::default(),
                        }
                    }));
                    outcomes.push(if recorded.baseline_established {
                        CollectionOutcome::BaselineEstablished { region_id }
                    } else if recorded.recovery_baseline {
                        CollectionOutcome::RecoveryBaselineEstablished { region_id }
                    } else {
                        CollectionOutcome::Complete {
                            region_id,
                            observed_contracts,
                        }
                    });
                    if self.store.active_esi_limiter_deadline().await?.is_some() {
                        break;
                    }
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
        let resolution_batch = self.resolve_awaiting_resolutions().await?;
        retry_after = std::cmp::max(retry_after, resolution_batch.retry_after);
        events.extend(resolution_batch.events);
        self.notify(&events).await?;
        if self.notifications.is_some() {
            self.store
                .mark_terminal_notifications_reported(&resolution_batch.notification_cases)
                .await?;
        }
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
        let summaries = public_contracts
            .into_iter()
            .map(|contract| {
                Ok(SummaryObservedContract {
                    fact_hash: content_hash(&contract)?,
                    contract,
                })
            })
            .collect::<Result<Vec<_>, ContractCollectionError>>()?;
        let mut observed = Vec::with_capacity(summaries.len());
        let mut manifest_failures = Vec::new();
        let mut limiter_active = false;
        for summary in &summaries {
            if limiter_active {
                continue;
            }
            match self.contract_items(summary.contract.contract_id).await {
                Ok(mut items) => {
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
                    let manifest_hash = content_hash(&manifest)?;
                    observed.push(ObservedContract {
                        contract: summary.contract.clone(),
                        manifest,
                        fact_hash: summary.fact_hash.clone(),
                        manifest_hash,
                    });
                }
                Err(error @ ContractCollectionError::Database(_)) => return Err(error),
                Err(error) => {
                    manifest_failures.push(ManifestCollectionFailure {
                        contract_id: summary.contract.contract_id,
                        detail: error.to_string(),
                        retry_after: error.retry_after(),
                    });
                    limiter_active = self.store.active_esi_limiter_deadline().await?.is_some();
                }
            }
        }
        Ok(self
            .store
            .record_complete(
                region_id,
                expected_pages,
                &summaries,
                &observed,
                &manifest_failures,
                self.recovery_gap,
            )
            .await?)
    }

    async fn resolve_awaiting_resolutions(
        &self,
    ) -> Result<ResolutionBatch, ContractCollectionError> {
        let mut batch = ResolutionBatch::default();
        self.store.resolve_terminal_resolution_failures().await?;
        for resolution in self.store.pending_terminal_notifications().await? {
            if let Some(event) = terminal_resolution_event(&resolution) {
                batch.events.push(event);
                batch.notification_cases.push(ResolutionCaseKey {
                    region_id: resolution.region_id,
                    contract_id: resolution.contract_id,
                });
            } else {
                warn!(
                    region_id = resolution.region_id,
                    contract_id = resolution.contract_id,
                    "terminal notification recovery found an unreportable resolution"
                );
            }
        }
        for resolution in self.store.awaiting_resolution_cases().await? {
            let resolution_key = ResolutionCaseKey {
                region_id: resolution.region_id,
                contract_id: resolution.contract_id,
            };
            match self.resolve_awaiting_resolution(&resolution).await {
                Ok(Some(event)) => {
                    batch.events.push(event);
                    batch.notification_cases.push(resolution_key);
                }
                Ok(None) => {}
                Err(error) => {
                    let retry_after = error.retry_after();
                    warn!(
                        region_id = resolution.region_id,
                        contract_id = resolution.contract_id,
                        "contract resolution remains retryable after an independent failure: {error}"
                    );
                    if let Err(record_error) = self
                        .store
                        .record_failure(
                            Some(resolution.region_id),
                            Some(resolution.contract_id),
                            Some(&format!(
                                "contracts/public/items/{}",
                                resolution.contract_id
                            )),
                            "resolution_probe",
                            &error.to_string(),
                            retry_after,
                        )
                        .await
                    {
                        warn!(
                            region_id = resolution.region_id,
                            contract_id = resolution.contract_id,
                            "failed to persist contract resolution failure: {record_error}"
                        );
                    }
                    batch.retry_after = std::cmp::max(batch.retry_after, retry_after);
                    match self.store.active_esi_limiter_deadline().await {
                        Ok(Some(_)) => break,
                        Ok(None) => {}
                        Err(limiter_error) => warn!(
                            region_id = resolution.region_id,
                            contract_id = resolution.contract_id,
                            "could not read the persisted ESI limiter after a resolution failure: {limiter_error}"
                        ),
                    }
                }
            }
        }
        Ok(batch)
    }

    async fn resolve_awaiting_resolution(
        &self,
        resolution: &AwaitingContractResolution,
    ) -> Result<Option<ContractEvent>, ContractCollectionError> {
        let key = format!("contracts/public/items/{}", resolution.contract_id);
        let cached = self.store.cache(&key).await?;
        if let Some(cached) = &cached {
            if cached.metadata.is_fresh() {
                if Utc::now() >= resolution.contract.date_expired {
                    return self
                        .resolve_nonfinancial_terminal(
                            resolution,
                            ContractResolutionState::Expired,
                            Utc::now(),
                        )
                        .await;
                }
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
                return Ok(None);
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
                if Utc::now() >= resolution.contract.date_expired {
                    return self
                        .resolve_nonfinancial_terminal(
                            resolution,
                            ContractResolutionState::Expired,
                            Utc::now(),
                        )
                        .await;
                }
                self.store
                    .schedule_resolution_probe(
                        resolution.region_id,
                        resolution.contract_id,
                        metadata.expires_at.unwrap_or_else(Utc::now),
                    )
                    .await?;
                Ok(None)
            }
            ContractItemProbe::NoContent(metadata) => {
                let evidence_response_at = Utc::now();
                if evidence_response_at >= resolution.contract.date_expired {
                    return self
                        .resolve_nonfinancial_terminal(
                            resolution,
                            ContractResolutionState::Expired,
                            evidence_response_at,
                        )
                        .await;
                }
                let event = acceptance_event(
                    resolution.region_id,
                    &resolution.contract,
                    &resolution.manifest,
                    resolution.last_public_observed_at,
                    resolution.absence_observed_at,
                    evidence_response_at,
                );
                if self
                    .store
                    .confirm_acceptance(
                        resolution,
                        evidence_response_at,
                        &metadata,
                        event.is_some(),
                    )
                    .await?
                {
                    Ok(event)
                } else {
                    Ok(None)
                }
            }
            ContractItemProbe::NotFound(_) => {
                let observed_at = Utc::now();
                let state = if observed_at >= resolution.contract.date_expired {
                    ContractResolutionState::Expired
                } else {
                    ContractResolutionState::ClosedOutcomeUnknown
                };
                self.resolve_nonfinancial_terminal(resolution, state, observed_at)
                    .await
            }
        }
    }

    async fn resolve_nonfinancial_terminal(
        &self,
        resolution: &AwaitingContractResolution,
        state: ContractResolutionState,
        observed_at: DateTime<Utc>,
    ) -> Result<Option<ContractEvent>, ContractCollectionError> {
        if self
            .store
            .resolve_nonfinancial_terminal(resolution, state, observed_at)
            .await?
            && !resolution.suppresses_nonfinancial_notification
        {
            return Ok(nonfinancial_terminal_event(resolution, state));
        }
        Ok(None)
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
                .notify_subscription(&deferred.subscription, &event, &ship_groups)
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
                    self.notify_subscription(subscription, &event, &ship_groups)
                        .await?,
                    NotificationResolution::Deferred
                ) {
                    self.store
                        .defer_contract_match(subscription, &event)
                        .await?;
                }
            }
        }
        self.deliver_prepared_notifications(notifications).await?;
        Ok(())
    }

    async fn event_with_observed_context(
        &self,
        event: &ContractEvent,
        subscriptions: &[ContractSubscription],
        ship_groups: &dyn ShipGroupResolver,
    ) -> Result<ContractEvent, ContractCollectionError> {
        let mut event = self.event_with_persisted_observation_context(event).await?;
        let mut requirements = ContractContextRequirements::default();
        for subscription in subscriptions {
            if matches!(
                contract_event_action(subscription, &event.kind),
                ContractEventAction::Ignore
            ) {
                continue;
            }
            let mut evaluator = ContractFilterEvaluator::new(&event, ship_groups);
            let plan =
                plan_contract_filter_context(&subscription.filter.root, &mut evaluator, true).await;
            requirements = requirements.union(plan.requirements);
        }
        if requirements.is_empty() {
            return Ok(event);
        }
        event.context.normalize_resolutions();

        if !matches!(event.kind, ContractEventKind::Listed) {
            self.load_range_center_positions(&mut event, &requirements)
                .await?;
            mark_terminal_context_unavailable(&mut event.context, requirements);
            return Ok(event);
        }

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

        if !requirements.ly_ranges.is_empty() {
            if event.context.solar_system_position.is_none() {
                if let Some(system_id) = event
                    .context
                    .solar_system_id
                    .and_then(|system_id| u32::try_from(system_id).ok())
                {
                    if let Some(position) = self
                        .cached_solar_system_position(event.contract.contract_id, system_id)
                        .await?
                    {
                        event.context.solar_system_position = Some(position);
                        event.context.solar_system_position_resolution =
                            ContractContextResolution::Resolved;
                    } else {
                        event.context.solar_system_position_resolution =
                            ContractContextResolution::TemporarilyUnavailable;
                    }
                } else {
                    event.context.solar_system_position_resolution =
                        ContractContextResolution::TemporarilyUnavailable;
                }
            }
            self.load_range_center_positions(&mut event, &requirements)
                .await?;
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

    async fn event_with_persisted_observation_context(
        &self,
        event: &ContractEvent,
    ) -> Result<ContractEvent, ContractCollectionError> {
        let mut observed = event.clone();
        if let Some(snapshot) = self
            .store
            .observed_embed_context(event.region_id, event.contract.contract_id)
            .await?
        {
            observed.embed_context.merge_missing_from(&snapshot);
            observed.context.solar_system_id = observed
                .context
                .solar_system_id
                .or(snapshot.location.solar_system_id);
            observed.context.security_status = observed.context.security_status.or(snapshot
                .location
                .security_status
                .filter(|value| value.is_finite()));
            observed.context.solar_system_position = observed
                .context
                .solar_system_position
                .or(snapshot.location.solar_system_position);
            observed.context.observed_affiliation_alliance_id = observed
                .context
                .observed_affiliation_alliance_id
                .or(snapshot.issuer_alliance_id);
        }
        observed.context.normalize_resolutions();
        Ok(observed)
    }

    async fn load_range_center_positions(
        &self,
        event: &mut ContractEvent,
        requirements: &ContractContextRequirements,
    ) -> Result<(), ContractCollectionError> {
        if requirements.ly_ranges.is_empty() || event.context.solar_system_position.is_none() {
            return Ok(());
        }
        for range in &requirements.ly_ranges {
            if event
                .context
                .range_center_positions
                .contains_key(&range.system_id)
            {
                continue;
            }
            let Some(position) = self
                .cached_solar_system_position(event.contract.contract_id, range.system_id)
                .await?
            else {
                continue;
            };
            event
                .context
                .range_center_positions
                .insert(range.system_id, position);
        }
        Ok(())
    }

    async fn cached_solar_system_position(
        &self,
        contract_id: i64,
        solar_system_id: u32,
    ) -> Result<Option<SolarSystemPosition>, ContractCollectionError> {
        let key = format!("universe/systems/{solar_system_id}");
        let cached = self.store.cache(&key).await?;
        if let Some((position, _)) = self.fresh_cached::<SolarSystemPosition>(&key, &cached)? {
            return Ok(position.is_finite().then_some(position));
        }
        if !self.context_request_allowed(contract_id).await? {
            return Ok(None);
        }
        let etag = cached
            .as_ref()
            .and_then(|cached| cached.metadata.etag.clone());
        let Some(response) = self
            .record_context_esi_result(
                contract_id,
                self.esi
                    .solar_system_position(solar_system_id, etag.as_deref())
                    .await,
            )
            .await?
        else {
            return Ok(None);
        };
        let (position, _) = self.resolve_response(&key, cached, response).await?;
        Ok(position.is_finite().then_some(position))
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
                    "contract context will be retried during a later complete observation: {error}"
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
            ContractEventKind::Expired | ContractEventKind::ClosedOutcomeUnknown => None,
        };
        if required_direction.is_some_and(|direction| !evaluator.has_matching_ship_item(direction))
        {
            return Ok(NotificationResolution::Complete);
        }
        let (primary_item, strategic_priority) = match evaluator.primary_display_item().await {
            PrimaryDisplayItem::Resolved {
                item,
                strategic_priority,
            } => (item, strategic_priority),
            PrimaryDisplayItem::Deferred => return Ok(NotificationResolution::Deferred),
        };
        let event = self.enrich_event_for_embed(event).await?;
        let (issuer_history, corporation_history) = self
            .store
            .contract_party_history(
                event.contract.issuer_id,
                event.contract.issuer_corporation_id,
            )
            .await?;
        let message = contract_notification_message(
            &event,
            primary_item,
            &issuer_history,
            &corporation_history,
        );
        let ping_type = action.ping_type();
        let ping = ping_type.is_some();
        let relevant_isk = event.contract.price.max(event.contract.reward);
        let confirmed_at = match event.kind {
            ContractEventKind::SaleConfirmed | ContractEventKind::PurchaseConfirmed => event
                .acceptance_evidence
                .as_ref()
                .map(|evidence| evidence.evidence_response_at)
                .unwrap_or(event.contract.date_issued),
            ContractEventKind::Expired | ContractEventKind::ClosedOutcomeUnknown => {
                event.contract.date_expired
            }
            ContractEventKind::Listed => event.contract.date_issued,
        };
        self.store
            .prepare_delivery(
                subscription,
                &event,
                &message,
                ping,
                ping_type.unwrap_or_default(),
                DeliveryOrdering {
                    strategic_priority,
                    relevant_isk,
                    confirmed_at,
                },
            )
            .await?;
        Ok(NotificationResolution::Complete)
    }

    async fn deliver_prepared_notifications(
        &self,
        notifications: &ContractNotifications,
    ) -> Result<(), ContractCollectionError> {
        for pending in self.store.prepared_deliveries().await? {
            let retry = self
                .deliver_prepared_notification(&pending, notifications)
                .await?;
            if retry {
                self.deliver_prepared_notification(&pending, notifications)
                    .await?;
            }
        }
        Ok(())
    }

    async fn deliver_prepared_notification(
        &self,
        pending: &PreparedContractDelivery,
        notifications: &ContractNotifications,
    ) -> Result<bool, ContractCollectionError> {
        let attempted_at = self.delivery_clock.now();
        let Some(mut prepared) = self
            .store
            .begin_delivery_attempt(pending.delivery_id, attempted_at)
            .await?
        else {
            return Ok(false);
        };
        if prepared.ping {
            prepared.ping = notifications
                .ping_limiter
                .try_acquire(prepared.channel_id)
                .await;
        }
        match notifications.delivery.send(prepared.clone()).await {
            Ok(message_id) => {
                self.store
                    .mark_delivery_sent(prepared.delivery_id, &message_id)
                    .await?;
                Ok(false)
            }
            Err(ContractDeliveryError::Permanent(error)) => {
                let error = ContractDeliveryError::Permanent(error);
                self.store
                    .mark_delivery_failed(prepared.delivery_id, &error, attempted_at)
                    .await?;
                warn!(
                    delivery_id = prepared.delivery_id,
                    subscription_id = %prepared.subscription_id,
                    contract_id = prepared.contract_id,
                    "contract delivery failed permanently: {error}"
                );
                Ok(false)
            }
            Err(
                error @ (ContractDeliveryError::Transient(_) | ContractDeliveryError::Ambiguous(_)),
            ) => {
                self.store
                    .mark_delivery_retryable(prepared.delivery_id, &error)
                    .await?;
                warn!(
                    delivery_id = prepared.delivery_id,
                    subscription_id = %prepared.subscription_id,
                    contract_id = prepared.contract_id,
                    "contract delivery remains prepared after retryable Discord failure: {error}"
                );
                Ok(true)
            }
        }
    }

    async fn enrich_event_for_embed(
        &self,
        event: &ContractEvent,
    ) -> Result<ContractEvent, ContractCollectionError> {
        let mut enriched = event.clone();
        if let Some(snapshot) = self
            .store
            .observed_embed_context(event.region_id, event.contract.contract_id)
            .await?
        {
            enriched.embed_context.merge_missing_from(&snapshot);
        }
        enriched.embed_context.location.solar_system_id = enriched
            .embed_context
            .location
            .solar_system_id
            .or(enriched.context.solar_system_id);
        enriched.embed_context.location.security_status =
            enriched.embed_context.location.security_status.or(enriched
                .context
                .security_status
                .filter(|value| value.is_finite()));
        enriched.embed_context.location.solar_system_position = enriched
            .embed_context
            .location
            .solar_system_position
            .or(enriched.context.solar_system_position);
        enriched.embed_context.issuer_alliance_id = enriched
            .embed_context
            .issuer_alliance_id
            .or(enriched.context.observed_affiliation_alliance_id);
        Ok(enriched)
    }

    async fn snapshot_observed_contracts(
        &self,
        region_id: i64,
        observed: &[ObservedContract],
        remaining: usize,
    ) -> Result<usize, ContractCollectionError> {
        let mut consumed = 0;
        for observed in observed {
            if self
                .store
                .observed_embed_context(region_id, observed.contract.contract_id)
                .await?
                .is_some()
            {
                self.store
                    .resolve_collection_failures(
                        Some(region_id),
                        Some(observed.contract.contract_id),
                        Some(&embed_context_resource_key(observed.contract.contract_id)),
                        "embed_context_enrichment",
                    )
                    .await?;
                continue;
            }
            if consumed >= remaining {
                break;
            }
            if !self
                .context_request_allowed(observed.contract.contract_id)
                .await?
            {
                break;
            }
            consumed += 1;
            let items = observed
                .manifest
                .offered_items
                .iter()
                .chain(&observed.manifest.requested_items)
                .cloned()
                .collect::<Vec<_>>();
            let limiter = PersistedContractContextLimiter {
                store: self.store.clone(),
            };
            match self
                .record_context_esi_result(
                    observed.contract.contract_id,
                    self.esi
                        .observed_contract_embed_context_limited(
                            &observed.contract,
                            &items,
                            &limiter,
                        )
                        .await,
                )
                .await?
            {
                Some(response) => {
                    self.store
                        .save_observed_embed_context(
                            region_id,
                            observed.contract.contract_id,
                            &response.value.unwrap_or_default(),
                        )
                        .await?;
                }
                None => {
                    self.store
                        .record_failure(
                            Some(region_id),
                            Some(observed.contract.contract_id),
                            Some(&embed_context_resource_key(observed.contract.contract_id)),
                            "embed_context_enrichment",
                            "observation-time contract embed enrichment will be retried",
                            self.store.active_esi_limiter_deadline().await?,
                        )
                        .await?;
                }
            }
            if self.store.active_esi_limiter_deadline().await?.is_some() {
                break;
            }
        }
        Ok(consumed)
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

fn mark_terminal_context_unavailable(
    context: &mut ContractObservationContext,
    requirements: ContractContextRequirements,
) {
    if requirements.solar_system
        && context.solar_system_id.is_none()
        && !matches!(
            context.solar_system_resolution,
            ContractContextResolution::DefinitivelyAbsent
        )
    {
        context.solar_system_resolution = ContractContextResolution::TemporarilyUnavailable;
    }
    if requirements.security_status
        && context.security_status.is_none()
        && !matches!(
            context.security_status_resolution,
            ContractContextResolution::DefinitivelyAbsent
        )
    {
        context.security_status_resolution = ContractContextResolution::TemporarilyUnavailable;
    }
    if requirements.observed_affiliation
        && context.observed_affiliation_alliance_id.is_none()
        && !matches!(
            context.observed_affiliation_alliance_resolution,
            ContractContextResolution::DefinitivelyAbsent
        )
    {
        context.observed_affiliation_alliance_resolution =
            ContractContextResolution::TemporarilyUnavailable;
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
        ContractEventKind::Expired => subscription.event_actions.expired,
        ContractEventKind::ClosedOutcomeUnknown => {
            subscription.event_actions.closed_outcome_unknown
        }
    }
}

fn confirmed_contract_event_kind(
    contract: &PublicContract,
    manifest: &ItemManifest,
) -> Option<ContractEventKind> {
    if manifest.requested_items.is_empty()
        && !manifest.offered_items.is_empty()
        && positive_isk(contract.price)
    {
        return Some(ContractEventKind::SaleConfirmed);
    }
    if manifest.offered_items.is_empty()
        && !manifest.requested_items.is_empty()
        && positive_isk(contract.reward)
    {
        return Some(ContractEventKind::PurchaseConfirmed);
    }
    None
}

fn acceptance_event(
    region_id: i64,
    contract: &PublicContract,
    manifest: &ItemManifest,
    last_public_observed_at: DateTime<Utc>,
    absence_observed_at: DateTime<Utc>,
    evidence_response_at: DateTime<Utc>,
) -> Option<ContractEvent> {
    Some(ContractEvent {
        region_id,
        kind: confirmed_contract_event_kind(contract, manifest)?,
        contract: contract.clone(),
        offered_items: manifest.offered_items.clone(),
        requested_items: manifest.requested_items.clone(),
        context: ContractObservationContext::default(),
        acceptance_evidence: Some(ContractAcceptanceEvidence {
            last_public_observed_at,
            absence_observed_at,
            evidence_response_at,
        }),
        embed_context: ContractEmbedContext::default(),
    })
}

fn nonfinancial_terminal_event(
    resolution: &AwaitingContractResolution,
    state: ContractResolutionState,
) -> Option<ContractEvent> {
    let kind = match state {
        ContractResolutionState::Expired => ContractEventKind::Expired,
        ContractResolutionState::ClosedOutcomeUnknown => ContractEventKind::ClosedOutcomeUnknown,
        ContractResolutionState::AwaitingResolution
        | ContractResolutionState::AcceptanceConfirmed => return None,
    };
    Some(ContractEvent {
        region_id: resolution.region_id,
        kind,
        contract: resolution.contract.clone(),
        offered_items: resolution.manifest.offered_items.clone(),
        requested_items: resolution.manifest.requested_items.clone(),
        context: ContractObservationContext::default(),
        acceptance_evidence: None,
        embed_context: ContractEmbedContext::default(),
    })
}

fn terminal_resolution_event(resolution: &TerminalContractResolution) -> Option<ContractEvent> {
    match resolution.state {
        ContractResolutionState::AcceptanceConfirmed => acceptance_event(
            resolution.region_id,
            &resolution.contract,
            &resolution.manifest,
            resolution.last_public_observed_at,
            resolution.absence_observed_at,
            resolution.acceptance_evidence_at?,
        ),
        ContractResolutionState::Expired | ContractResolutionState::ClosedOutcomeUnknown => {
            nonfinancial_terminal_event(
                &AwaitingContractResolution {
                    region_id: resolution.region_id,
                    contract_id: resolution.contract_id,
                    contract: resolution.contract.clone(),
                    manifest: resolution.manifest.clone(),
                    last_public_observed_at: resolution.last_public_observed_at,
                    absence_observed_at: resolution.absence_observed_at,
                    suppresses_nonfinancial_notification: false,
                },
                resolution.state,
            )
        }
        ContractResolutionState::AwaitingResolution => None,
    }
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
        PrimaryDisplayItem::Resolved {
            item: primary.map(|(item, _)| item),
            strategic_priority: primary.map(|(_, priority)| priority).unwrap_or(usize::MAX),
        }
    }

    fn has_matching_ship_item(&self, direction: ContractItemDirection) -> bool {
        self.matching_ship_items
            .iter()
            .any(|(item_direction, _)| *item_direction == direction)
    }
}

enum PrimaryDisplayItem<'a> {
    Resolved {
        item: Option<&'a PublicContractItem>,
        strategic_priority: usize,
    },
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
                let waits_for_non_context = requirements.is_empty();
                ContractFilterContextPlan {
                    result,
                    requirements,
                    waits_for_non_context,
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
        ContractFilterCondition::LyRangeFrom(ranges) => {
            let Some(event_position) = event
                .context
                .solar_system_position
                .filter(|position| position.is_finite())
            else {
                return ContractFilterMatch::Deferred;
            };
            let mut deferred = false;
            for range in ranges {
                let Some(center_position) = event
                    .context
                    .range_center_positions
                    .get(&range.system_id)
                    .copied()
                    .filter(|position| position.is_finite())
                else {
                    deferred = true;
                    continue;
                };
                if event_position.distance_in_light_years(center_position) <= range.range {
                    return ContractFilterMatch::Matched;
                }
            }
            if deferred {
                ContractFilterMatch::Deferred
            } else {
                ContractFilterMatch::Unmatched
            }
        }
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
    issuer_history: &ContractPartyHistory,
    corporation_history: &ContractPartyHistory,
) -> ContractNotificationMessage {
    let event_label = match event.kind {
        ContractEventKind::Listed => "Listed",
        ContractEventKind::SaleConfirmed => "Sale Confirmed",
        ContractEventKind::PurchaseConfirmed => "Purchase Confirmed",
        ContractEventKind::Expired => "Expired",
        ContractEventKind::ClosedOutcomeUnknown => "Closed — Outcome Unknown",
    };
    let primary_name = primary_item
        .and_then(|item| event.embed_context.item_names.get(&item.type_id))
        .cloned()
        .or_else(|| primary_item.map(|item| format!("Type {}", item.type_id)))
        .unwrap_or_else(|| "Public contract".to_string());
    let title_verb = match event.kind {
        ContractEventKind::Listed => "listed",
        ContractEventKind::SaleConfirmed => "sold",
        ContractEventKind::PurchaseConfirmed => "purchase confirmed",
        ContractEventKind::Expired => "expired",
        ContractEventKind::ClosedOutcomeUnknown => "closed — outcome unknown",
    };
    let title_location = event
        .embed_context
        .location
        .solar_system_name
        .as_deref()
        .map(|system| format!(" in {system}"))
        .unwrap_or_default();
    let mut message = ContractNotificationMessage {
        title: sanitize_discord_text(&format!("{primary_name} {title_verb}{title_location}")),
        description: event
            .contract
            .title
            .as_deref()
            .filter(|title| !title.is_empty())
            .map(sanitize_contract_title)
            .map(|title| format!("Contract title: {title}"))
            .map(|title| bounded_text(&title, 4096)),
        fields: vec![
            ContractEmbedField {
                name: "Event".to_string(),
                value: event_label.to_string(),
                inline: true,
            },
            ContractEmbedField {
                name: "Offered".to_string(),
                value: compact_bundle_summary(&event.offered_items, &event.embed_context),
                inline: false,
            },
            ContractEmbedField {
                name: "Requested".to_string(),
                value: compact_bundle_summary(&event.requested_items, &event.embed_context),
                inline: false,
            },
        ],
        thumbnail_url: primary_item.map(|item| {
            format!(
                "https://images.evetech.net/types/{}/icon?size=64",
                item.type_id
            )
        }),
        footer: Some(format!(
            "Contract {} · issued {}",
            event.contract.contract_id,
            compact_timestamp(event.contract.date_issued)
        )),
    };
    if positive_isk(event.contract.price) {
        message.fields.push(ContractEmbedField {
            name: "Requested ISK".to_string(),
            value: format_compact_isk(event.contract.price),
            inline: true,
        });
    }
    if positive_isk(event.contract.reward) {
        message.fields.push(ContractEmbedField {
            name: "Offered ISK".to_string(),
            value: format_compact_isk(event.contract.reward),
            inline: true,
        });
    }
    message.fields.extend([
        ContractEmbedField {
            name: "Observed Location".to_string(),
            value: format_observed_location(
                &event.embed_context.location,
                event.contract.start_location_id,
            ),
            inline: false,
        },
        ContractEmbedField {
            name: "Issuer".to_string(),
            value: format_identity(
                event.embed_context.issuer_character_name.as_deref(),
                event.contract.issuer_id,
            ),
            inline: false,
        },
    ]);
    let corporation = format_identity(
        event.embed_context.issuer_corporation_name.as_deref(),
        event.contract.issuer_corporation_id,
    );
    let affiliation = event
        .embed_context
        .issuer_alliance_id
        .map(|id| {
            format!(
                "\nAlliance: {}",
                format_identity(event.embed_context.issuer_alliance_name.as_deref(), id)
            )
        })
        .unwrap_or_default();
    message.fields.push(ContractEmbedField {
        name: "Observed Affiliation".to_string(),
        value: format!("At observation: {corporation}{affiliation}"),
        inline: false,
    });
    message.fields.push(ContractEmbedField {
        name: "Issuer History".to_string(),
        value: format_party_history(issuer_history),
        inline: false,
    });
    message.fields.push(ContractEmbedField {
        name: "Corporation History".to_string(),
        value: format_party_history(corporation_history),
        inline: false,
    });
    if let Some(evidence) = &event.acceptance_evidence {
        message.fields.push(ContractEmbedField {
            name: "Acceptance Evidence".to_string(),
            value: format!(
                "Public through {}\nAbsent by {}\n204 received {}",
                compact_timestamp(evidence.last_public_observed_at),
                compact_timestamp(evidence.absence_observed_at),
                compact_timestamp(evidence.evidence_response_at),
            ),
            inline: false,
        });
        let latency_seconds = (Utc::now() - evidence.evidence_response_at)
            .num_seconds()
            .max(0);
        message.fields.push(ContractEmbedField {
            name: "Observed Notification Latency".to_string(),
            value: format!(
                "{} after acceptance evidence",
                compact_duration(latency_seconds)
            ),
            inline: true,
        });
    } else if matches!(
        event.kind,
        ContractEventKind::Expired | ContractEventKind::ClosedOutcomeUnknown
    ) {
        message.fields.push(ContractEmbedField {
            name: "Closure Observation".to_string(),
            value: "Observed no longer public; no exact closure time is available.".to_string(),
            inline: false,
        });
    }
    message.fields.push(ContractEmbedField {
        name: "Contract Address".to_string(),
        value: format!("```\ncontract:0//{}\n```", event.contract.contract_id),
        inline: false,
    });
    bound_embed_message(&mut message);
    message
}

const MAX_EMBED_TITLE_CHARACTERS: usize = 256;
const MAX_EMBED_DESCRIPTION_CHARACTERS: usize = 4096;
const MAX_EMBED_FIELD_NAME_CHARACTERS: usize = 256;
const MAX_EMBED_FIELD_VALUE_CHARACTERS: usize = 1024;
const MAX_EMBED_CHARACTERS: usize = 6000;
const MAX_EMBED_FIELDS: usize = 25;
const MAX_BUNDLE_ITEMS_IN_EMBED: usize = 8;

fn compact_bundle_summary(items: &[PublicContractItem], context: &ContractEmbedContext) -> String {
    let visible = items
        .iter()
        .take(MAX_BUNDLE_ITEMS_IN_EMBED)
        .map(|item| {
            let name = context
                .item_names
                .get(&item.type_id)
                .cloned()
                .unwrap_or_else(|| format!("Type {}", item.type_id));
            format!("{name} ×{}", item.quantity)
        })
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

fn format_compact_isk(value: f64) -> String {
    let (scaled, suffix) = if value >= 1_000_000_000.0 {
        (value / 1_000_000_000.0, "b")
    } else if value >= 1_000_000.0 {
        (value / 1_000_000.0, "m")
    } else if value >= 1_000.0 {
        (value / 1_000.0, "k")
    } else {
        (value, "")
    };
    format!("{}{} ISK", trim_decimal(scaled), suffix)
}

fn trim_decimal(value: f64) -> String {
    let rendered = format!("{value:.2}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn format_observed_location(context: &ContractLocationContext, location_id: i64) -> String {
    let mut lines = Vec::new();
    if let Some(name) = &context.location_name {
        let kind = context.location_kind.as_deref().unwrap_or("Location");
        lines.push(format!("{kind}: {name}"));
    }
    if let Some(name) = &context.solar_system_name {
        let security = context
            .security_status
            .filter(|value| value.is_finite())
            .map(|value| format!(" ({value:.1})"))
            .unwrap_or_default();
        lines.push(format!("System: {name}{security}"));
    } else if let Some(system_id) = context.solar_system_id {
        lines.push(format!("System ID: {system_id}"));
    }
    if let Some(name) = &context.region_name {
        lines.push(format!("Region: {name}"));
    } else if let Some(region_id) = context.region_id {
        lines.push(format!("Region ID: {region_id}"));
    }
    lines.push(format!("Location ID: {location_id}"));
    lines.join("\n")
}

fn format_identity(name: Option<&str>, id: i64) -> String {
    match name {
        Some(name) => format!("{} ({id})", sanitize_discord_text(name)),
        None => id.to_string(),
    }
}

fn format_party_history(history: &ContractPartyHistory) -> String {
    let mut value = format!(
        "90d: {} sales · {} purchases · {} unknown closures",
        history.confirmed_sales, history.confirmed_purchases, history.unknown_closures
    );
    if let Some(latest) = &history.most_recent_confirmed {
        let label = match latest.kind {
            ContractEventKind::SaleConfirmed => "Sale confirmed",
            ContractEventKind::PurchaseConfirmed => "Purchase confirmed",
            _ => "Confirmed",
        };
        value.push_str(&format!(
            "\nLatest: {label} {}",
            compact_timestamp(latest.observed_at)
        ));
    }
    value
}

fn compact_timestamp(value: DateTime<Utc>) -> String {
    value.format("%-d %b %Y %H:%MZ").to_string()
}

fn compact_duration(seconds: i64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 60 * 60 {
        format!("{}m", seconds / 60)
    } else if seconds < 60 * 60 * 24 {
        format!("{}h", seconds / (60 * 60))
    } else {
        format!("{}d", seconds / (60 * 60 * 24))
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
    message.footer = message
        .footer
        .as_deref()
        .map(|footer| bounded_text(footer, 2048));
    if message.fields.len() > MAX_EMBED_FIELDS {
        let contract_address = message
            .fields
            .iter()
            .position(|field| field.name == "Contract Address")
            .map(|index| message.fields.remove(index));
        message
            .fields
            .truncate(MAX_EMBED_FIELDS - usize::from(contract_address.is_some()));
        if let Some(contract_address) = contract_address {
            message.fields.push(contract_address);
        }
    }
    if embed_character_count(message) <= MAX_EMBED_CHARACTERS {
        return;
    }

    message.description = None;
    if embed_character_count(message) <= MAX_EMBED_CHARACTERS {
        return;
    }

    let excess = embed_character_count(message).saturating_sub(MAX_EMBED_CHARACTERS);
    if let Some(footer) = &message.footer {
        let footer_budget = footer.chars().count().saturating_sub(excess);
        message.footer = (footer_budget > 0).then(|| bounded_text(footer, footer_budget));
    }
    while embed_character_count(message) > MAX_EMBED_CHARACTERS {
        let Some(index) = message
            .fields
            .iter()
            .rposition(|field| field.name != "Contract Address")
        else {
            break;
        };
        message.fields.remove(index);
    }
    if embed_character_count(message) > MAX_EMBED_CHARACTERS {
        message.footer = None;
    }
    if embed_character_count(message) > MAX_EMBED_CHARACTERS {
        let title_budget = message
            .title
            .chars()
            .count()
            .saturating_sub(embed_character_count(message) - MAX_EMBED_CHARACTERS);
        message.title = bounded_text(&message.title, title_budget);
    }
}

fn embed_character_count(message: &ContractNotificationMessage) -> usize {
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
        + message
            .fields
            .iter()
            .map(|field| field.name.chars().count() + field.value.chars().count())
            .sum::<usize>()
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

fn sanitize_contract_failure_detail(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_control() || character.is_whitespace() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    bounded_text(
        &sanitize_discord_text(&normalized.split_whitespace().collect::<Vec<_>>().join(" ")),
        512,
    )
}

pub fn sanitize_discord_text(value: &str) -> String {
    value.replace('@', "@\u{200b}")
}

fn sanitize_contract_title(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '@' => sanitized.push_str("@\u{200b}"),
            '\\' | '`' | '*' | '_' | '~' | '>' | '<' | '[' | ']' | '(' | ')' | '#' | '+' | '-'
            | '.' | '!' | '|' | '{' | '}' => sanitized.push(' '),
            character if character.is_whitespace() || character.is_control() => sanitized.push(' '),
            _ => sanitized.push(character),
        }
    }
    sanitized.split_whitespace().collect::<Vec<_>>().join(" ")
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
    ping_limiter: Arc<dyn ContractPingLimiter>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_contract_collection_loop_with_notifications(
        database_url,
        store_handle,
        interval,
        esi_timeout,
        ship_groups,
        delivery,
        ping_limiter,
    ))
}

pub async fn run_contract_collection_loop_with_notifications(
    database_url: String,
    store_handle: ContractStoreHandle,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
) {
    let notifications = ContractNotifications {
        ship_groups,
        delivery,
        ping_limiter,
    };
    let esi = loop {
        match HttpPublicContractEsi::new(esi_timeout) {
            Ok(esi) => break Arc::new(esi),
            Err(error) => {
                warn!("contract collection HTTP client unavailable: {error}");
                tokio::time::sleep(interval).await;
            }
        }
    };
    let mut consecutive_failures = 0_u32;
    loop {
        let mut delay = interval;
        match ContractCollectionStore::connect(&database_url).await {
            Ok(store) => {
                let store = Arc::new(store);
                *store_handle.write().await = Some(store.clone());
                let collector = ContractCollector::new((*store).clone(), esi.clone())
                    .with_notifications_and_ping_limiter(
                        notifications.ship_groups.clone(),
                        notifications.delivery.clone(),
                        notifications.ping_limiter.clone(),
                    )
                    .with_recovery_gap(collection_recovery_gap(interval));
                match collector.collect_cycle().await {
                    Ok(report) => {
                        if report.retry_after.is_some()
                            || report.regions.iter().any(|outcome| {
                                matches!(outcome, CollectionOutcome::Inconclusive { .. })
                            })
                        {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            delay = collection_retry_delay(
                                interval,
                                report.retry_after,
                                consecutive_failures,
                            );
                        } else {
                            consecutive_failures = 0;
                        }
                        info!(
                            regions = report.regions.len(),
                            "contract collection cycle finished"
                        )
                    }
                    Err(error) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        delay = collection_retry_delay(
                            interval,
                            error.retry_after(),
                            consecutive_failures,
                        );
                        warn!("contract collection paused after failure: {error}")
                    }
                }
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                delay = collection_retry_delay(interval, None, consecutive_failures);
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
    let esi = loop {
        match HttpPublicContractEsi::new(esi_timeout) {
            Ok(esi) => break Arc::new(esi),
            Err(error) => {
                warn!("contract collection HTTP client unavailable: {error}");
                tokio::time::sleep(interval).await;
            }
        }
    };
    let mut consecutive_failures = 0_u32;
    loop {
        let mut delay = interval;
        match ContractCollectionStore::connect(&database_url).await {
            Ok(store) => {
                let mut collector = ContractCollector::new(store, esi.clone());
                if let Some(notifications) = &notifications {
                    collector = collector.with_notifications(
                        notifications.ship_groups.clone(),
                        notifications.delivery.clone(),
                    );
                }
                collector = collector.with_recovery_gap(collection_recovery_gap(interval));
                match collector.collect_cycle().await {
                    Ok(report) => {
                        if report.retry_after.is_some()
                            || report.regions.iter().any(|outcome| {
                                matches!(outcome, CollectionOutcome::Inconclusive { .. })
                            })
                        {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            delay = collection_retry_delay(
                                interval,
                                report.retry_after,
                                consecutive_failures,
                            );
                        } else {
                            consecutive_failures = 0;
                        }
                        info!(
                            regions = report.regions.len(),
                            "contract collection cycle finished"
                        )
                    }
                    Err(error) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        delay = collection_retry_delay(
                            interval,
                            error.retry_after(),
                            consecutive_failures,
                        );
                        warn!("contract collection paused after failure: {error}")
                    }
                }
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                delay = collection_retry_delay(interval, None, consecutive_failures);
                warn!("contract collection database unavailable; retrying without an in-memory fallback: {error}")
            }
        }
        tokio::time::sleep(delay).await;
    }
}

pub fn collection_retry_delay(
    interval: Duration,
    retry_after: Option<DateTime<Utc>>,
    consecutive_failures: u32,
) -> Duration {
    const MAX_COLLECTION_RETRY_DELAY: Duration = Duration::from_secs(300);
    let base_delay = interval
        .max(Duration::from_millis(10))
        .min(MAX_COLLECTION_RETRY_DELAY);
    let multiplier = 1_u32 << consecutive_failures.saturating_sub(1).min(16);
    let exponential_delay = base_delay
        .checked_mul(multiplier)
        .unwrap_or(MAX_COLLECTION_RETRY_DELAY)
        .min(MAX_COLLECTION_RETRY_DELAY);
    let retry_delay = retry_after
        .and_then(|retry_after| (retry_after - Utc::now()).to_std().ok())
        .unwrap_or_default();
    exponential_delay.max(retry_delay)
}

fn collection_recovery_gap(interval: Duration) -> ChronoDuration {
    let interval_seconds = i64::try_from(interval.as_secs()).unwrap_or(i64::MAX);
    ChronoDuration::seconds(interval_seconds.saturating_mul(3).max(15 * 60))
}

#[cfg(test)]
mod embed_tests {
    use crate::discord_bot::{contract_notification_embed, DiscordContractDelivery};
    use serenity::http::Http;

    use super::*;

    const MANUAL_CONTRACT_EMBED_NONCE: &str = "ci-manual-contract-check";

    fn contract() -> PublicContract {
        PublicContract {
            contract_id: 45,
            availability: Some("public".to_string()),
            buyout: None,
            collateral: Some(0.0),
            contract_type: "item_exchange".to_string(),
            date_expired: Utc::now() + ChronoDuration::hours(1),
            date_issued: Utc::now() - ChronoDuration::minutes(5),
            days_to_complete: 0,
            end_location_id: None,
            for_corporation: false,
            issuer_corporation_id: 98_000_001,
            issuer_id: 90_000_001,
            price: 77_500_000_000.0,
            reward: 0.0,
            start_location_id: 60_003_760,
            title: Some("@everyone `Hel` **sale**".to_string()),
            volume: 115_000.0,
        }
    }

    fn item(record_id: i64, type_id: i64, included: bool) -> PublicContractItem {
        PublicContractItem {
            record_id,
            type_id,
            quantity: 1,
            is_included: included,
            item_id: None,
            raw_quantity: None,
            is_singleton: None,
            is_blueprint_copy: None,
            material_efficiency: None,
            runs: None,
            time_efficiency: None,
        }
    }

    fn event(kind: ContractEventKind) -> ContractEvent {
        ContractEvent {
            region_id: 10_000_002,
            kind,
            contract: contract(),
            offered_items: vec![item(1, 19_720, true), item(2, 587, true)],
            requested_items: vec![item(3, 34, false)],
            context: ContractObservationContext::default(),
            acceptance_evidence: None,
            embed_context: ContractEmbedContext {
                observed_at: Some(Utc::now()),
                location: ContractLocationContext {
                    location_name: Some(
                        "Jita IV - Moon 4 - Caldari Navy Assembly Plant".to_string(),
                    ),
                    location_kind: Some("Station".to_string()),
                    solar_system_id: Some(30_000_142),
                    solar_system_name: Some("Jita".to_string()),
                    solar_system_position: None,
                    security_status: Some(0.945_913_136_005_401_6),
                    region_id: Some(10_000_002),
                    region_name: Some("The Forge".to_string()),
                },
                issuer_character_name: Some("Issuer Name".to_string()),
                issuer_corporation_name: Some("Issuer Corp".to_string()),
                issuer_alliance_id: Some(99_000_001),
                issuer_alliance_name: Some("Issuer Alliance".to_string()),
                item_names: BTreeMap::from([
                    (19_720, "Ragnarok".to_string()),
                    (587, "Rifter".to_string()),
                    (34, "Tritanium".to_string()),
                ]),
            },
        }
    }

    fn field<'a>(message: &'a ContractNotificationMessage, name: &str) -> Option<&'a str> {
        message
            .fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| field.value.as_str())
    }

    #[test]
    fn renders_every_contract_event_without_a_counterparty() {
        let history = ContractPartyHistory::default();
        for (kind, expected) in [
            (ContractEventKind::Listed, "Listed"),
            (ContractEventKind::SaleConfirmed, "Sale Confirmed"),
            (ContractEventKind::PurchaseConfirmed, "Purchase Confirmed"),
            (ContractEventKind::Expired, "Expired"),
            (
                ContractEventKind::ClosedOutcomeUnknown,
                "Closed — Outcome Unknown",
            ),
        ] {
            let message = contract_notification_message(
                &event(kind),
                Some(&item(1, 19_720, true)),
                &history,
                &history,
            );
            assert_eq!(field(&message, "Event"), Some(expected));
            assert!(message.fields.iter().all(|field| !matches!(
                field.name.as_str(),
                "Buyer" | "Counterparty" | "Accepting Corporation"
            )));
        }
    }

    #[test]
    fn location_degrades_without_dropping_known_facts() {
        let history = ContractPartyHistory::default();
        let full = contract_notification_message(
            &event(ContractEventKind::Listed),
            None,
            &history,
            &history,
        );
        let full_location = field(&full, "Observed Location").expect("location field");
        assert!(full_location.contains("Station: Jita IV - Moon 4 - Caldari Navy Assembly Plant"));
        assert!(full_location.contains("System: Jita (0.9)"));
        assert!(full_location.contains("Region: The Forge"));
        assert!(full_location.ends_with("Location ID: 60003760"));

        let mut fallback = event(ContractEventKind::Listed);
        fallback.embed_context.location = ContractLocationContext {
            solar_system_id: Some(30_000_142),
            ..ContractLocationContext::default()
        };
        let fallback = contract_notification_message(&fallback, None, &history, &history);
        assert_eq!(
            field(&fallback, "Observed Location"),
            Some("System ID: 30000142\nLocation ID: 60003760")
        );

        let mut station_only = event(ContractEventKind::Listed);
        station_only.embed_context.location = ContractLocationContext {
            location_name: Some("Jita IV - Moon 4".to_string()),
            location_kind: Some("Station".to_string()),
            ..ContractLocationContext::default()
        };
        let station_only = contract_notification_message(&station_only, None, &history, &history);
        assert_eq!(
            field(&station_only, "Observed Location"),
            Some("Station: Jita IV - Moon 4\nLocation ID: 60003760")
        );

        let mut region_only = event(ContractEventKind::Listed);
        region_only.embed_context.location = ContractLocationContext {
            region_id: Some(10_000_002),
            ..ContractLocationContext::default()
        };
        let region_only = contract_notification_message(&region_only, None, &history, &history);
        assert_eq!(
            field(&region_only, "Observed Location"),
            Some("Region ID: 10000002\nLocation ID: 60003760")
        );

        let mut raw_only = event(ContractEventKind::Listed);
        raw_only.embed_context.location = ContractLocationContext::default();
        let raw_only = contract_notification_message(&raw_only, None, &history, &history);
        assert_eq!(
            field(&raw_only, "Observed Location"),
            Some("Location ID: 60003760")
        );
    }

    #[test]
    fn ranked_primary_ship_and_complete_bundle_stay_prominent() {
        let history = ContractPartyHistory::default();
        let message = contract_notification_message(
            &event(ContractEventKind::Listed),
            Some(&item(1, 19_720, true)),
            &history,
            &history,
        );
        assert!(message.title.starts_with("Ragnarok listed"));
        assert_eq!(
            message.thumbnail_url.as_deref(),
            Some("https://images.evetech.net/types/19720/icon?size=64")
        );
        let offered = field(&message, "Offered").expect("offered bundle");
        assert!(offered.contains("Ragnarok ×1"));
        assert!(offered.contains("Rifter ×1"));
        assert_eq!(field(&message, "Requested ISK"), Some("77.5b ISK"));
    }

    #[test]
    fn history_evidence_and_contract_address_are_structural_and_bounded() {
        let mut event = event(ContractEventKind::SaleConfirmed);
        event.acceptance_evidence = Some(ContractAcceptanceEvidence {
            last_public_observed_at: Utc::now() - ChronoDuration::minutes(3),
            absence_observed_at: Utc::now() - ChronoDuration::minutes(2),
            evidence_response_at: Utc::now() - ChronoDuration::seconds(90),
        });
        event.contract.title = Some("`@everyone` ".repeat(800));
        event.embed_context.issuer_corporation_name = None;
        event.embed_context.issuer_alliance_id = None;
        event.embed_context.issuer_alliance_name = None;
        let history = ContractPartyHistory {
            confirmed_sales: 4,
            confirmed_purchases: 2,
            unknown_closures: 1,
            most_recent_confirmed: Some(MostRecentConfirmedContractEvent {
                kind: ContractEventKind::SaleConfirmed,
                observed_at: Utc::now() - ChronoDuration::hours(1),
            }),
        };
        let message =
            contract_notification_message(&event, Some(&item(1, 19_720, true)), &history, &history);
        assert!(message
            .description
            .as_deref()
            .expect("title description")
            .contains("@\u{200b}everyone"));
        assert!(
            message
                .description
                .as_deref()
                .expect("title description")
                .chars()
                .count()
                <= 4096
        );
        assert_eq!(
            field(&message, "Observed Affiliation"),
            Some("At observation: 98000001")
        );
        assert!(field(&message, "Issuer History")
            .expect("history")
            .contains("4 sales"));
        assert!(field(&message, "Acceptance Evidence")
            .expect("evidence")
            .contains("204 received"));
        assert_eq!(
            field(&message, "Contract Address"),
            Some("```\ncontract:0//45\n```")
        );
        assert!(message
            .footer
            .as_deref()
            .expect("footer")
            .contains("Contract 45"));
    }

    #[test]
    fn manual_contract_embed_nonce_fits_discord_limit() {
        assert!(MANUAL_CONTRACT_EMBED_NONCE.chars().count() <= 25);
    }

    #[tokio::test]
    #[ignore = "manual visual check; requires DISCORD_BOT_TOKEN and CONTRACT_TEST_DISCORD_CHANNEL_ID and sends one non-pinging embed"]
    async fn manual_contract_embed_visual_delivery() {
        dotenvy::dotenv().ok();
        let token = std::env::var("DISCORD_BOT_TOKEN")
            .expect("DISCORD_BOT_TOKEN is required for the manual contract embed check");
        let channel_id = std::env::var("CONTRACT_TEST_DISCORD_CHANNEL_ID")
            .expect(
                "CONTRACT_TEST_DISCORD_CHANNEL_ID is required for the manual contract embed check",
            )
            .parse()
            .expect("CONTRACT_TEST_DISCORD_CHANNEL_ID must be a Discord snowflake");
        let mut event = event(ContractEventKind::SaleConfirmed);
        event.contract.contract_id = 234_057_619;
        event.contract.title = Some("Manual public Hel sale visual check".to_string());
        event.offered_items = vec![item(1, 22_852, true), item(2, 587, true)];
        event
            .embed_context
            .item_names
            .insert(22_852, "Hel".to_string());
        event.acceptance_evidence = Some(ContractAcceptanceEvidence {
            last_public_observed_at: Utc::now() - ChronoDuration::minutes(3),
            absence_observed_at: Utc::now() - ChronoDuration::minutes(2),
            evidence_response_at: Utc::now() - ChronoDuration::minutes(1),
        });
        let issuer_history = ContractPartyHistory {
            confirmed_sales: 4,
            confirmed_purchases: 1,
            unknown_closures: 0,
            most_recent_confirmed: Some(MostRecentConfirmedContractEvent {
                kind: ContractEventKind::SaleConfirmed,
                observed_at: Utc::now() - ChronoDuration::hours(1),
            }),
        };
        let corporation_history = ContractPartyHistory {
            confirmed_sales: 12,
            confirmed_purchases: 3,
            unknown_closures: 1,
            most_recent_confirmed: Some(MostRecentConfirmedContractEvent {
                kind: ContractEventKind::SaleConfirmed,
                observed_at: Utc::now() - ChronoDuration::minutes(30),
            }),
        };
        let message = contract_notification_message(
            &event,
            Some(&event.offered_items[0]),
            &issuer_history,
            &corporation_history,
        );
        assert_eq!(message.title, "Hel sold in Jita");
        assert_eq!(field(&message, "Requested ISK"), Some("77.5b ISK"));
        assert!(field(&message, "Observed Location")
            .expect("location field")
            .contains("Station: Jita IV - Moon 4 - Caldari Navy Assembly Plant"));
        assert_eq!(field(&message, "Issuer"), Some("Issuer Name (90000001)"));
        assert!(field(&message, "Issuer History")
            .expect("issuer history")
            .contains("4 sales"));
        assert!(field(&message, "Corporation History")
            .expect("corporation history")
            .contains("12 sales"));
        assert_eq!(
            message.thumbnail_url.as_deref(),
            Some("https://images.evetech.net/types/22852/icon?size=64")
        );
        assert_eq!(
            field(&message, "Contract Address"),
            Some("```\ncontract:0//234057619\n```")
        );
        let embed = contract_notification_embed(&message);
        assert_eq!(embed.0["title"].as_str(), Some("Hel sold in Jita"));
        assert_eq!(
            embed.0["thumbnail"]["url"].as_str(),
            Some("https://images.evetech.net/types/22852/icon?size=64")
        );

        let delivery = DiscordContractDelivery::new(Arc::new(Http::new(&token)));
        let message_id = delivery
            .send(PreparedContractDelivery {
                delivery_id: 0,
                guild_id: 0,
                channel_id,
                subscription_id: "manual-contract-embed-check".to_string(),
                contract_id: event.contract.contract_id,
                event_kind: event.kind,
                ping: false,
                ping_type: ContractPingType::Here,
                nonce: MANUAL_CONTRACT_EMBED_NONCE.to_string(),
                enforce_nonce: false,
                message,
            })
            .await
            .expect("send manual contract embed");
        assert!(!message_id.is_empty());
    }

    #[test]
    fn aggregate_embed_limit_trims_optional_content_before_contract_address() {
        let mut message = ContractNotificationMessage {
            title: "T".repeat(MAX_EMBED_TITLE_CHARACTERS),
            description: Some("D".repeat(MAX_EMBED_DESCRIPTION_CHARACTERS)),
            fields: (0..24)
                .map(|index| ContractEmbedField {
                    name: format!("Field {index} {}", "N".repeat(240)),
                    value: "V".repeat(MAX_EMBED_FIELD_VALUE_CHARACTERS),
                    inline: false,
                })
                .chain(std::iter::once(ContractEmbedField {
                    name: "Contract Address".to_string(),
                    value: "```\ncontract:0//45\n```".to_string(),
                    inline: false,
                }))
                .collect(),
            thumbnail_url: None,
            footer: Some("F".repeat(2048)),
        };

        bound_embed_message(&mut message);

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
                + message
                    .fields
                    .iter()
                    .map(|field| field.name.chars().count() + field.value.chars().count())
                    .sum::<usize>()
                <= MAX_EMBED_CHARACTERS
        );
        assert!(message.fields.len() <= MAX_EMBED_FIELDS);
        assert_eq!(
            field(&message, "Contract Address"),
            Some("```\ncontract:0//45\n```")
        );
    }

    #[test]
    fn contract_titles_are_single_line_plain_text_without_discord_markdown() {
        let mut event = event(ContractEventKind::Listed);
        event.contract.title = Some("# heading\n- list ||spoiler|| **bold** <@1234>".to_string());
        let history = ContractPartyHistory::default();

        let message = contract_notification_message(&event, None, &history, &history);
        let description = message.description.expect("sanitized contract title");

        assert!(!description.contains('\n'));
        assert!(!description.contains("# heading"));
        assert!(!description.contains("- list"));
        assert!(!description.contains("||spoiler||"));
        assert!(!description.contains("**bold**"));
        assert!(!description.contains("<@1234>"));
        assert!(description.contains("heading"));
        assert!(description.contains("spoiler"));
        assert!(description.contains("bold"));
    }
}
