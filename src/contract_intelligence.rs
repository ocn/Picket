use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{HeaderMap, ETAG, EXPIRES, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
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

#[derive(Clone, Debug, PartialEq)]
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
pub struct EsiError {
    message: String,
    retry_after: Option<DateTime<Utc>>,
    metadata: CacheMetadata,
}

impl EsiError {
    pub fn retryable(message: impl Into<String>, retry_after: Option<DateTime<Utc>>) -> Self {
        Self {
            message: message.into(),
            retry_after,
            metadata: CacheMetadata {
                retry_after,
                ..CacheMetadata::cached_for_seconds(0)
            },
        }
    }

    fn from_metadata(message: impl Into<String>, metadata: CacheMetadata) -> Self {
        Self {
            message: message.into(),
            retry_after: metadata.retry_after,
            metadata,
        }
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
                metadata,
            ));
        }
        let value = response
            .json()
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?;
        Ok(EsiResponse::fresh(value, metadata))
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractEventKind {
    Listed,
}

impl ContractEventKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Listed => "listed",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractEventActions {
    pub listed: ContractEventAction,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractFilter {
    pub events: Vec<ContractEventKind>,
    pub item_direction: ContractItemDirection,
    pub type_ids: Vec<i64>,
    pub ship_group_ids: Vec<i64>,
}

impl ContractFilter {
    pub fn validate(&self) -> Result<(), String> {
        if !self.events.contains(&ContractEventKind::Listed) {
            return Err("the initial contract filter must include the Listed event".to_string());
        }
        if self.type_ids.is_empty() && self.ship_group_ids.is_empty() {
            return Err("provide at least one ship type ID or ship group ID".to_string());
        }
        Ok(())
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
        self.filter.validate()
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
    async fn group_for_type(&self, type_id: i64) -> Option<i64>;
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

    pub async fn upsert_contract_subscription(
        &self,
        subscription: &ContractSubscription,
    ) -> Result<(), sqlx::Error> {
        subscription
            .validate()
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        sqlx::query("INSERT INTO contract_subscriptions (guild_id, channel_id, subscription_id, description, filter, event_actions) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (guild_id, channel_id, subscription_id) DO UPDATE SET description = EXCLUDED.description, filter = EXCLUDED.filter, event_actions = EXCLUDED.event_actions, updated_at = now()")
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
        sqlx::query("SELECT guild_id, channel_id, subscription_id, description, filter, event_actions FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 ORDER BY subscription_id")
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
        let result = sqlx::query("DELETE FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3")
            .bind(guild_id as i64)
            .bind(channel_id as i64)
            .bind(id)
            .execute(&self.pool)
            .await?;
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
        sqlx::query("SELECT guild_id, channel_id, subscription_id, description, filter, event_actions FROM contract_subscriptions ORDER BY guild_id, channel_id, subscription_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(contract_subscription_from_row)
            .collect()
    }

    async fn prepare_delivery(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        message: &ContractNotificationMessage,
        ping: bool,
    ) -> Result<Option<PreparedContractDelivery>, sqlx::Error> {
        let delivery_id = sqlx::query_scalar::<_, i64>("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'prepared') ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind) DO NOTHING RETURNING id")
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
        sqlx::query("UPDATE contract_outbound_deliveries SET status = 'sent', discord_message_id = $2, sent_at = now() WHERE id = $1 AND status = 'prepared'")
            .bind(delivery_id)
            .bind(discord_message_id)
            .execute(&self.pool)
            .await?;
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ContractEvent {
    pub region_id: i64,
    pub kind: ContractEventKind,
    pub contract: PublicContract,
    pub offered_items: Vec<PublicContractItem>,
    pub requested_items: Vec<PublicContractItem>,
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

    async fn notify(&self, events: &[ContractEvent]) -> Result<(), ContractCollectionError> {
        let Some(notifications) = &self.notifications else {
            return Ok(());
        };
        let subscriptions = self.store.all_contract_subscriptions().await?;
        for event in events {
            for subscription in &subscriptions {
                let action = contract_event_action(subscription, &event.kind);
                if matches!(action, ContractEventAction::Ignore) {
                    continue;
                }
                let matched_items =
                    matching_items(subscription, event, &*notifications.ship_groups).await;
                if matched_items.is_empty() {
                    continue;
                }
                let message =
                    contract_notification_message(event, &subscription.filter, &matched_items);
                let ping = matches!(action, ContractEventAction::PostAndPing);
                let Some(prepared) = self
                    .store
                    .prepare_delivery(subscription, event, &message, ping)
                    .await?
                else {
                    continue;
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
            }
        }
        Ok(())
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

fn contract_event_action(
    subscription: &ContractSubscription,
    event_kind: &ContractEventKind,
) -> ContractEventAction {
    match event_kind {
        ContractEventKind::Listed => subscription.event_actions.listed,
    }
}

async fn matching_items(
    subscription: &ContractSubscription,
    event: &ContractEvent,
    ship_groups: &dyn ShipGroupResolver,
) -> Vec<PublicContractItem> {
    if !subscription.filter.events.contains(&event.kind) {
        return Vec::new();
    }
    let items = match subscription.filter.item_direction {
        ContractItemDirection::Offered => &event.offered_items,
        ContractItemDirection::Requested => &event.requested_items,
    };
    let mut matches = Vec::new();
    for item in items {
        let type_matches = subscription.filter.type_ids.contains(&item.type_id);
        let group_matches = if subscription.filter.ship_group_ids.is_empty() {
            false
        } else {
            ship_groups
                .group_for_type(item.type_id)
                .await
                .map(|group_id| subscription.filter.ship_group_ids.contains(&group_id))
                .unwrap_or(false)
        };
        if type_matches || group_matches {
            matches.push(item.clone());
        }
    }
    matches
}

fn contract_notification_message(
    event: &ContractEvent,
    filter: &ContractFilter,
    matched_items: &[PublicContractItem],
) -> ContractNotificationMessage {
    let item_label = matched_items
        .iter()
        .map(|item| format!("Type {} ×{}", item.type_id, item.quantity))
        .collect::<Vec<_>>()
        .join(", ");
    let (isk_label, isk) = match filter.item_direction {
        ContractItemDirection::Offered => ("Requested ISK", event.contract.price),
        ContractItemDirection::Requested => ("Offered ISK", event.contract.reward),
    };
    let direction = match filter.item_direction {
        ContractItemDirection::Offered => "Offered Item",
        ContractItemDirection::Requested => "Requested Item",
    };
    let location_id = event.contract.start_location_id;
    ContractNotificationMessage {
        title: "Public contract listed".to_string(),
        description: event
            .contract
            .title
            .as_deref()
            .filter(|title| !title.is_empty())
            .map(sanitize_discord_text),
        fields: vec![
            ContractEmbedField {
                name: "Event".to_string(),
                value: "Listed".to_string(),
                inline: true,
            },
            ContractEmbedField {
                name: direction.to_string(),
                value: item_label,
                inline: false,
            },
            ContractEmbedField {
                name: isk_label.to_string(),
                value: format!("{isk:.2} ISK"),
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
    }
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

pub async fn run_contract_collection_loop_with_notifications(
    database_url: String,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
) {
    run_contract_collection_loop_inner(
        database_url,
        interval,
        esi_timeout,
        Some(ContractNotifications {
            ship_groups,
            delivery,
        }),
    )
    .await;
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
