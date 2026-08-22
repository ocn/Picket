use crate::config::{System, SystemRange};
use crate::discord_bot::SHIP_GROUP_PRIORITY;
use crate::feed::{FeedHealthSnapshot, FeedHealthTelemetry};
use crate::location_evidence::{
    lock_location, LocationEvidence, LocationEvidenceClass, LocationEvidenceService,
};
use crate::presentation::{
    compact_location_description, CompactLocation, LocationOn, LocationRange, LocationRegion,
    LocationSystem,
};
use crate::structure_resolver::{
    ResolvedStructure, StructureResolver, StructureResolverError, StructureResolverFailureKind,
    StructureResolverRuntimeStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use reqwest::header::{HeaderMap, ETAG, EXPIRES, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use serde::de::{DeserializeOwned, Error as DeError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    migrate::{Migrate, MigrateError, Migrator},
    postgres::PgPoolOptions,
    PgPool, Postgres, Row, Transaction,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::{info, warn};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
const HEALTH_MIGRATION_VERSIONS: &[i64] = &[20260817000001, 20260817000004];
const CONTRACT_COLLECTOR_USER_AGENT: &str = "killbot-rust Global Public Contract Intelligence";
const MAX_EMBED_CONTEXT_ENRICHMENTS_PER_CYCLE: usize = 8;
const OPTIONAL_EMBED_ENRICHMENT_BUDGET: Duration = Duration::from_secs(30);
const PUBLIC_LOCATION_EVIDENCE_RETRY_BASE_SECONDS: i64 = 30;
const PUBLIC_LOCATION_EVIDENCE_RETRY_MAX_SECONDS: i64 = 300;

fn contract_region_names_from_systems(
    systems: &HashMap<u32, System>,
) -> Result<HashMap<i64, String>, String> {
    let mut region_names = HashMap::new();
    for system in systems.values() {
        let region_id = i64::from(system.region_id);
        let region_name = system.region.trim();
        if region_id <= 0 || region_name.is_empty() {
            continue;
        }
        if let Some(existing) = region_names.get(&region_id) {
            if existing != region_name {
                return Err(format!(
                    "systems catalog has conflicting names for region {region_id}"
                ));
            }
        } else {
            region_names.insert(region_id, region_name.to_string());
        }
    }
    if region_names.is_empty() {
        return Err("systems catalog contains no region names".to_string());
    }
    Ok(region_names)
}

fn configured_contract_region_names() -> Result<HashMap<i64, String>, String> {
    crate::config::load_systems()
        .map_err(|error| format!("cannot load contract region names from systems.json: {error}"))
        .and_then(|systems| contract_region_names_from_systems(&systems))
}

fn configured_contract_system_names() -> HashMap<i64, String> {
    match crate::config::load_systems() {
        Ok(systems) => systems
            .into_values()
            .filter_map(|system| {
                let name = system.name.trim();
                (!name.is_empty()).then_some((i64::from(system.id), name.to_string()))
            })
            .collect(),
        Err(error) => {
            warn!("historical rerender system catalog unavailable; retained system names remain available: {error}");
            HashMap::new()
        }
    }
}

fn runtime_contract_region_names() -> Arc<HashMap<i64, String>> {
    match configured_contract_region_names() {
        Ok(region_names) => Arc::new(region_names),
        Err(error) => {
            warn!("contract region-name catalog unavailable; public ESI fallback remains active: {error}");
            Arc::new(HashMap::new())
        }
    }
}
const MAX_TERMINAL_RESOLUTION_PROBES_PER_COMPLETED_REGION: usize = 8;
const RESOLUTION_PROBE_RETRY_BASE_SECONDS: i64 = 30;
const RESOLUTION_PROBE_RETRY_MAX_SECONDS: i64 = 15 * 60;
pub const DEFAULT_CONTRACT_REGIONAL_CONCURRENCY: usize = 2;
pub const MAX_CONTRACT_REGIONAL_CONCURRENCY: usize = 4;
const DISCORD_NONCE_ENFORCEMENT_WINDOW: ChronoDuration = ChronoDuration::minutes(5);
const CONTRACT_DELIVERY_LEASE: ChronoDuration = ChronoDuration::minutes(2);
const CONTRACT_REPAIR_LEASE: ChronoDuration = ChronoDuration::minutes(2);
const CONTRACT_REPAIR_STALE_COMPLETION_DELAY: ChronoDuration = ChronoDuration::seconds(5);
const MAX_PROXIMITY_EVIDENCE_REFRESHES: usize = 2;
const CONTRACT_REPAIR_RETRY_BASE_SECONDS: i64 = 5;
const CONTRACT_REPAIR_RETRY_MAX_SECONDS: i64 = 5 * 60;
const DEFAULT_COLLECTION_RECOVERY_GAP: ChronoDuration = ChronoDuration::minutes(15);
const REGION_DISCOVERY_RESOURCE_KEY: &str = "esi:regions";
const METERS_PER_LIGHT_YEAR: f64 = 9_460_730_472_580_800.0;
const CONTRACT_ACCEPTED_BY_PLAYER_ERROR: &str =
    "Contract accepted by player, requires authorization";
const CONTRACT_NOT_PUBLIC_ERROR: &str = "Contract not public";
const HEALTH_SNAPSHOT_TRANSITION_LOCK_KEY: i64 = 7_142_300_993_001;
const MAX_HEALTH_REGIONAL_DETAILS: usize = 12;
pub const DEFAULT_HEALTH_CHANNEL_ID: u64 = 1_538_030_920_328_810_536;
pub const DEFAULT_WATCHDOG_CONFIG_REVISION: &str = "1";
const HEALTH_ENVIRONMENT_VARIABLES: &[&str] = &[
    "HEALTH_EVALUATION_INTERVAL_SECS",
    "HEALTH_CHANNEL_ID",
    "HEALTH_HEARTBEAT_DEGRADED_SECS",
    "HEALTH_HEARTBEAT_CRITICAL_SECS",
    "HEALTH_POSTGRES_DEGRADED_FAILURES",
    "HEALTH_POSTGRES_CRITICAL_FAILURES",
    "HEALTH_CONTRACT_PROGRESS_DEGRADED_SECS",
    "HEALTH_CONTRACT_PROGRESS_CRITICAL_SECS",
    "HEALTH_ESI_PROGRESS_DEGRADED_SECS",
    "HEALTH_ESI_PROGRESS_CRITICAL_SECS",
    "HEALTH_PREPARED_DELIVERY_DEGRADED_SECS",
    "HEALTH_PREPARED_DELIVERY_CRITICAL_SECS",
    "HEALTH_BACKLOG_DEGRADED_SECS",
    "HEALTH_BACKLOG_CRITICAL_SECS",
    "HEALTH_R2Z2_PROGRESS_DEGRADED_SECS",
    "HEALTH_R2Z2_PROGRESS_CRITICAL_SECS",
    "HEALTH_FEED_VALIDATION_DEGRADED_FAILURES",
    "HEALTH_FEED_VALIDATION_CRITICAL_FAILURES",
    "WATCHDOG_EVALUATION_INTERVAL_SECS",
    "WATCHDOG_CONFIG_REVISION",
];

pub fn contract_regional_concurrency_from(value: Option<&str>) -> Result<usize, String> {
    let Some(value) = value else {
        return Ok(DEFAULT_CONTRACT_REGIONAL_CONCURRENCY);
    };
    let concurrency = value.parse::<usize>().map_err(|_| {
        format!(
            "CONTRACT_REGIONAL_CONCURRENCY must be an integer from 1 to {MAX_CONTRACT_REGIONAL_CONCURRENCY}"
        )
    })?;
    if !(1..=MAX_CONTRACT_REGIONAL_CONCURRENCY).contains(&concurrency) {
        return Err(format!(
            "CONTRACT_REGIONAL_CONCURRENCY must be from 1 to {MAX_CONTRACT_REGIONAL_CONCURRENCY}"
        ));
    }
    Ok(concurrency)
}

pub fn contract_regional_concurrency_from_environment() -> Result<usize, String> {
    contract_regional_concurrency_from_environment_value(std::env::var(
        "CONTRACT_REGIONAL_CONCURRENCY",
    ))
}

fn contract_regional_concurrency_from_environment_value(
    value: Result<String, std::env::VarError>,
) -> Result<usize, String> {
    match value {
        Ok(value) => contract_regional_concurrency_from(Some(&value)),
        Err(std::env::VarError::NotPresent) => contract_regional_concurrency_from(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "CONTRACT_REGIONAL_CONCURRENCY must be valid Unicode and an integer from 1 to {MAX_CONTRACT_REGIONAL_CONCURRENCY}"
        )),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Critical,
}

impl HealthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }

    fn from_str(value: &str) -> Result<Self, sqlx::Error> {
        match value {
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "critical" => Ok(Self::Critical),
            _ => Err(sqlx::Error::Protocol(format!(
                "unknown health status: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HealthCheck {
    pub key: String,
    pub status: HealthStatus,
    pub observed_at: DateTime<Utc>,
    pub evidence: String,
    pub consecutive_failures: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HealthSnapshot {
    pub status: HealthStatus,
    pub observed_at: DateTime<Utc>,
    pub checks: Vec<HealthCheck>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthView {
    pub channel_id: u64,
    pub message_id: Option<String>,
    pub content: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthDiscordViewIdentity {
    pub channel_id: u64,
    pub message_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HealthWatchdogConfig {
    pub channel_id: u64,
    pub config_revision: String,
    pub thresholds: HealthThresholds,
    pub evaluation_interval: Duration,
}

impl HealthWatchdogConfig {
    pub fn for_channel(channel_id: u64) -> Self {
        Self {
            channel_id,
            config_revision: DEFAULT_WATCHDOG_CONFIG_REVISION.to_string(),
            thresholds: HealthThresholds::default(),
            evaluation_interval: Duration::from_secs(60),
        }
    }

    pub fn from_environment() -> Result<Self, HealthConfigError> {
        Self::from_settings(&health_settings_from_environment()?)
    }

    pub fn from_settings(settings: &HashMap<String, String>) -> Result<Self, HealthConfigError> {
        Self::from_lookup(|name| settings.get(name).cloned())
    }

    fn from_lookup<F>(mut lookup: F) -> Result<Self, HealthConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let health_config = HealthRuntimeConfig::from_lookup(&mut lookup)?;
        let interval = match lookup("WATCHDOG_EVALUATION_INTERVAL_SECS") {
            None => 60,
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value >= 10)
                .ok_or_else(|| {
                    HealthConfigError(
                    "WATCHDOG_EVALUATION_INTERVAL_SECS must be an integer of at least 10 seconds"
                        .to_string(),
                )
                })?,
        };
        let config_revision = lookup("WATCHDOG_CONFIG_REVISION")
            .unwrap_or_else(|| DEFAULT_WATCHDOG_CONFIG_REVISION.to_string());
        if config_revision.trim().is_empty() {
            return Err(HealthConfigError(
                "WATCHDOG_CONFIG_REVISION must not be empty or whitespace".to_string(),
            ));
        }
        Ok(Self {
            channel_id: health_config.channel_id,
            config_revision,
            thresholds: health_config.thresholds,
            evaluation_interval: Duration::from_secs(interval),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HealthPublishError {
    Transient(String),
    UnknownMessage(String),
    RetryAfter {
        detail: String,
        retry_after: ChronoDuration,
    },
    Permanent(String),
}

impl HealthPublishError {
    fn failure_kind(&self) -> &'static str {
        match self {
            Self::Transient(_) | Self::UnknownMessage(_) | Self::RetryAfter { .. } => "transient",
            Self::Permanent(_) => "permanent",
        }
    }

    fn detail(&self) -> &str {
        match self {
            Self::Transient(detail) | Self::UnknownMessage(detail) | Self::Permanent(detail) => {
                detail
            }
            Self::RetryAfter { detail, .. } => detail,
        }
    }

    fn next_attempt_at(&self, observed_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Permanent(_) => None,
            Self::UnknownMessage(_) => Some(observed_at),
            Self::Transient(_) => Some(observed_at + ChronoDuration::minutes(1)),
            Self::RetryAfter { retry_after, .. } => Some(observed_at + *retry_after),
        }
    }

    fn clears_message_identity(&self) -> bool {
        matches!(self, Self::UnknownMessage(_))
    }
}

#[async_trait]
pub trait HealthDiscordPublisher: Send + Sync {
    async fn create_view(
        &self,
        channel_id: u64,
        content: &str,
    ) -> Result<String, HealthPublishError>;
    async fn update_view(
        &self,
        channel_id: u64,
        message_id: &str,
        content: &str,
    ) -> Result<(), HealthPublishError>;
    async fn create_incident(
        &self,
        channel_id: u64,
        content: &str,
        mention_operator_id: Option<u64>,
    ) -> Result<String, HealthPublishError>;
    async fn update_incident(
        &self,
        channel_id: u64,
        message_id: &str,
        content: &str,
    ) -> Result<(), HealthPublishError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HealthDiscordIncidentState {
    channel_id: u64,
    config_revision: String,
    message_id: Option<String>,
    active: bool,
    last_error: Option<String>,
    failure_kind: Option<String>,
    next_attempt_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HealthDiscordViewState {
    channel_id: u64,
    config_revision: String,
    message_id: Option<String>,
    last_error: Option<String>,
    failure_kind: Option<String>,
    next_attempt_at: Option<DateTime<Utc>>,
}

pub struct HealthWatchdog {
    store: ContractCollectionStore,
    clock: Arc<dyn HealthClock>,
    config: HealthWatchdogConfig,
    publisher: Arc<dyn HealthDiscordPublisher>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthWatchdogRun {
    pub status: HealthStatus,
    pub consecutive_postgres_failures: i32,
    pub database_available: bool,
}

pub struct HealthWatchdogRunner {
    database_url: String,
    clock: Arc<dyn HealthClock>,
    config: HealthWatchdogConfig,
    publisher: Arc<dyn HealthDiscordPublisher>,
    last_snapshot: Mutex<Option<HealthSnapshot>>,
    last_persisted_snapshot_at: Mutex<Option<DateTime<Utc>>>,
    last_heartbeat_at: Mutex<Option<DateTime<Utc>>>,
    cached_view: Mutex<Option<HealthDiscordViewState>>,
    cached_incident: Mutex<Option<HealthDiscordIncidentState>>,
    watchdog_started_at: Mutex<Option<DateTime<Utc>>>,
    postgres_failures: Mutex<i32>,
}

impl HealthWatchdog {
    pub fn new(
        store: ContractCollectionStore,
        clock: Arc<dyn HealthClock>,
        config: HealthWatchdogConfig,
        publisher: Arc<dyn HealthDiscordPublisher>,
    ) -> Self {
        Self {
            store,
            clock,
            config,
            publisher,
        }
    }
}

impl HealthWatchdogRunner {
    pub fn new(
        database_url: String,
        clock: Arc<dyn HealthClock>,
        config: HealthWatchdogConfig,
        publisher: Arc<dyn HealthDiscordPublisher>,
    ) -> Self {
        Self {
            database_url,
            clock,
            config,
            publisher,
            last_snapshot: Mutex::new(None),
            last_persisted_snapshot_at: Mutex::new(None),
            last_heartbeat_at: Mutex::new(None),
            cached_view: Mutex::new(None),
            cached_incident: Mutex::new(None),
            watchdog_started_at: Mutex::new(None),
            postgres_failures: Mutex::new(0),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HealthThresholds {
    pub heartbeat_degraded: ChronoDuration,
    pub heartbeat_critical: ChronoDuration,
    pub postgres_degraded_failures: i32,
    pub postgres_critical_failures: i32,
    pub contract_progress_degraded: ChronoDuration,
    pub contract_progress_critical: ChronoDuration,
    pub esi_progress_degraded: ChronoDuration,
    pub esi_progress_critical: ChronoDuration,
    pub prepared_delivery_degraded: ChronoDuration,
    pub prepared_delivery_critical: ChronoDuration,
    pub backlog_degraded: ChronoDuration,
    pub backlog_critical: ChronoDuration,
    pub r2z2_progress_degraded: ChronoDuration,
    pub r2z2_progress_critical: ChronoDuration,
    pub feed_validation_degraded_failures: i32,
    pub feed_validation_critical_failures: i32,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            heartbeat_degraded: ChronoDuration::minutes(5),
            heartbeat_critical: ChronoDuration::minutes(10),
            postgres_degraded_failures: 1,
            postgres_critical_failures: 3,
            contract_progress_degraded: ChronoDuration::minutes(15),
            contract_progress_critical: ChronoDuration::minutes(30),
            esi_progress_degraded: ChronoDuration::minutes(15),
            esi_progress_critical: ChronoDuration::minutes(30),
            prepared_delivery_degraded: ChronoDuration::minutes(5),
            prepared_delivery_critical: ChronoDuration::minutes(15),
            backlog_degraded: ChronoDuration::minutes(30),
            backlog_critical: ChronoDuration::hours(2),
            r2z2_progress_degraded: ChronoDuration::minutes(3),
            r2z2_progress_critical: ChronoDuration::minutes(10),
            feed_validation_degraded_failures: 1,
            feed_validation_critical_failures: 3,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HealthRuntimeConfig {
    pub evaluation_interval: Duration,
    pub channel_id: u64,
    pub thresholds: HealthThresholds,
}

#[derive(Debug, Eq, PartialEq)]
pub struct HealthConfigError(String);

impl Display for HealthConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for HealthConfigError {}

impl HealthRuntimeConfig {
    pub fn from_environment() -> Result<Self, HealthConfigError> {
        Self::from_settings(&health_settings_from_environment()?)
    }

    pub fn from_settings(settings: &HashMap<String, String>) -> Result<Self, HealthConfigError> {
        Self::from_lookup(|name| settings.get(name).cloned())
    }

    fn from_lookup<F>(mut lookup: F) -> Result<Self, HealthConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut thresholds = HealthThresholds::default();
        thresholds.heartbeat_degraded = health_duration_from_settings(
            &mut lookup,
            "HEALTH_HEARTBEAT_DEGRADED_SECS",
            thresholds.heartbeat_degraded,
        )?;
        thresholds.heartbeat_critical = health_duration_from_settings(
            &mut lookup,
            "HEALTH_HEARTBEAT_CRITICAL_SECS",
            thresholds.heartbeat_critical,
        )?;
        thresholds.postgres_degraded_failures = health_failure_count_from_settings(
            &mut lookup,
            "HEALTH_POSTGRES_DEGRADED_FAILURES",
            thresholds.postgres_degraded_failures,
        )?;
        thresholds.postgres_critical_failures = health_failure_count_from_settings(
            &mut lookup,
            "HEALTH_POSTGRES_CRITICAL_FAILURES",
            thresholds.postgres_critical_failures,
        )?;
        thresholds.contract_progress_degraded = health_duration_from_settings(
            &mut lookup,
            "HEALTH_CONTRACT_PROGRESS_DEGRADED_SECS",
            thresholds.contract_progress_degraded,
        )?;
        thresholds.contract_progress_critical = health_duration_from_settings(
            &mut lookup,
            "HEALTH_CONTRACT_PROGRESS_CRITICAL_SECS",
            thresholds.contract_progress_critical,
        )?;
        thresholds.esi_progress_degraded = health_duration_from_settings(
            &mut lookup,
            "HEALTH_ESI_PROGRESS_DEGRADED_SECS",
            thresholds.esi_progress_degraded,
        )?;
        thresholds.esi_progress_critical = health_duration_from_settings(
            &mut lookup,
            "HEALTH_ESI_PROGRESS_CRITICAL_SECS",
            thresholds.esi_progress_critical,
        )?;
        thresholds.prepared_delivery_degraded = health_duration_from_settings(
            &mut lookup,
            "HEALTH_PREPARED_DELIVERY_DEGRADED_SECS",
            thresholds.prepared_delivery_degraded,
        )?;
        thresholds.prepared_delivery_critical = health_duration_from_settings(
            &mut lookup,
            "HEALTH_PREPARED_DELIVERY_CRITICAL_SECS",
            thresholds.prepared_delivery_critical,
        )?;
        thresholds.backlog_degraded = health_duration_from_settings(
            &mut lookup,
            "HEALTH_BACKLOG_DEGRADED_SECS",
            thresholds.backlog_degraded,
        )?;
        thresholds.backlog_critical = health_duration_from_settings(
            &mut lookup,
            "HEALTH_BACKLOG_CRITICAL_SECS",
            thresholds.backlog_critical,
        )?;
        thresholds.r2z2_progress_degraded = health_duration_from_settings(
            &mut lookup,
            "HEALTH_R2Z2_PROGRESS_DEGRADED_SECS",
            thresholds.r2z2_progress_degraded,
        )?;
        thresholds.r2z2_progress_critical = health_duration_from_settings(
            &mut lookup,
            "HEALTH_R2Z2_PROGRESS_CRITICAL_SECS",
            thresholds.r2z2_progress_critical,
        )?;
        thresholds.feed_validation_degraded_failures = health_failure_count_from_settings(
            &mut lookup,
            "HEALTH_FEED_VALIDATION_DEGRADED_FAILURES",
            thresholds.feed_validation_degraded_failures,
        )?;
        thresholds.feed_validation_critical_failures = health_failure_count_from_settings(
            &mut lookup,
            "HEALTH_FEED_VALIDATION_CRITICAL_FAILURES",
            thresholds.feed_validation_critical_failures,
        )?;
        validate_threshold_order(
            "heartbeat",
            thresholds.heartbeat_degraded,
            thresholds.heartbeat_critical,
        )?;
        validate_failure_order(
            "PostgreSQL",
            thresholds.postgres_degraded_failures,
            thresholds.postgres_critical_failures,
        )?;
        validate_threshold_order(
            "contract progress",
            thresholds.contract_progress_degraded,
            thresholds.contract_progress_critical,
        )?;
        validate_threshold_order(
            "ESI progress",
            thresholds.esi_progress_degraded,
            thresholds.esi_progress_critical,
        )?;
        validate_threshold_order(
            "prepared delivery",
            thresholds.prepared_delivery_degraded,
            thresholds.prepared_delivery_critical,
        )?;
        validate_threshold_order(
            "backlog",
            thresholds.backlog_degraded,
            thresholds.backlog_critical,
        )?;
        validate_threshold_order(
            "R2Z2 progress",
            thresholds.r2z2_progress_degraded,
            thresholds.r2z2_progress_critical,
        )?;
        validate_failure_order(
            "feed validation",
            thresholds.feed_validation_degraded_failures,
            thresholds.feed_validation_critical_failures,
        )?;
        Ok(Self {
            evaluation_interval: Duration::from_secs(health_positive_u64_from_settings(
                &mut lookup,
                "HEALTH_EVALUATION_INTERVAL_SECS",
                60,
            )?),
            channel_id: health_channel_id_from_settings(
                &mut lookup,
                "HEALTH_CHANNEL_ID",
                DEFAULT_HEALTH_CHANNEL_ID,
            )?,
            thresholds,
        })
    }
}

fn health_duration_from_settings(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: ChronoDuration,
) -> Result<ChronoDuration, HealthConfigError> {
    match lookup(name) {
        None => Ok(default),
        Some(value) => value
            .parse::<i64>()
            .ok()
            .filter(|seconds| *seconds > 0)
            .and_then(ChronoDuration::try_seconds)
            .ok_or_else(|| {
                HealthConfigError(format!("{name} must be a positive integer seconds value"))
            }),
    }
}

fn health_failure_count_from_settings(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: i32,
) -> Result<i32, HealthConfigError> {
    match lookup(name) {
        None => Ok(default),
        Some(value) => value
            .parse::<i32>()
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| HealthConfigError(format!("{name} must be a positive integer count"))),
    }
}

fn health_positive_u64_from_settings(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64, HealthConfigError> {
    match lookup(name) {
        None => Ok(default),
        Some(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| HealthConfigError(format!("{name} must be a positive integer"))),
    }
}

fn health_channel_id_from_settings(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<u64, HealthConfigError> {
    let value = health_positive_u64_from_settings(lookup, name, default)?;
    if value <= i64::MAX as u64 {
        Ok(value)
    } else {
        Err(HealthConfigError(format!(
            "{name} must fit PostgreSQL BIGINT"
        )))
    }
}

fn health_settings_from_environment() -> Result<HashMap<String, String>, HealthConfigError> {
    health_settings_from_environment_values(|name| std::env::var(name))
}

fn health_settings_from_environment_values<F>(
    mut lookup: F,
) -> Result<HashMap<String, String>, HealthConfigError>
where
    F: FnMut(&str) -> Result<String, std::env::VarError>,
{
    let mut settings = HashMap::new();
    for name in HEALTH_ENVIRONMENT_VARIABLES {
        match lookup(name) {
            Ok(value) => {
                settings.insert((*name).to_string(), value);
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(HealthConfigError(format!("{name} must be valid Unicode")));
            }
        }
    }
    Ok(settings)
}

fn validate_threshold_order(
    name: &str,
    degraded: ChronoDuration,
    critical: ChronoDuration,
) -> Result<(), HealthConfigError> {
    if degraded < critical {
        Ok(())
    } else {
        Err(HealthConfigError(format!(
            "{name} degraded threshold must be lower than its critical threshold"
        )))
    }
}

fn validate_failure_order(
    name: &str,
    degraded: i32,
    critical: i32,
) -> Result<(), HealthConfigError> {
    if degraded < critical {
        Ok(())
    } else {
        Err(HealthConfigError(format!(
            "{name} degraded threshold must be lower than its critical threshold"
        )))
    }
}

pub trait HealthClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

struct SystemHealthClock;

impl HealthClock for SystemHealthClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct HealthCycle {
    store: ContractCollectionStore,
    clock: Arc<dyn HealthClock>,
    thresholds: HealthThresholds,
    feed_telemetry: Option<Arc<FeedHealthTelemetry>>,
    r2z2_enabled: bool,
}

impl HealthCycle {
    pub fn new(store: ContractCollectionStore, clock: Arc<dyn HealthClock>) -> Self {
        Self {
            store,
            clock,
            thresholds: HealthThresholds::default(),
            feed_telemetry: None,
            r2z2_enabled: false,
        }
    }

    pub fn with_thresholds(mut self, thresholds: HealthThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    pub fn with_feed_telemetry(
        mut self,
        feed_telemetry: Arc<FeedHealthTelemetry>,
        r2z2_enabled: bool,
    ) -> Self {
        self.feed_telemetry = Some(feed_telemetry);
        self.r2z2_enabled = r2z2_enabled;
        self
    }
}

fn regional_snapshot_resource_key(region_id: i64) -> String {
    format!("esi:public-contracts:region:{region_id}")
}

fn embed_context_resource_key(contract_id: i64) -> String {
    format!("contract-embed-context:{contract_id}")
}

fn public_location_evidence_resource_key(contract_id: i64) -> String {
    format!("contract-public-location-evidence:{contract_id}")
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractAcceptanceProvenance {
    AcceptedByPlayer,
    NoContent,
}

impl ContractAcceptanceProvenance {
    fn as_str(self) -> &'static str {
        match self {
            Self::AcceptedByPlayer => "accepted_by_player",
            Self::NoContent => "no_content",
        }
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

    fn collection_pause_until_at(&self, observed_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
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
            reset_seconds.map(|seconds| observed_at + ChronoDuration::seconds(seconds))
        })
    }

    fn collection_pacing_until(&self, observed_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        rate_limit_pacing_deadline(
            self.rate_limit_limit.as_deref()?,
            self.rate_limit_remaining?,
            observed_at,
        )
    }
}

pub(crate) fn esi_limiter_deadline_at(
    metadata: &CacheMetadata,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    [
        metadata.collection_pause_until_at(observed_at),
        metadata.collection_pacing_until(observed_at),
    ]
    .into_iter()
    .flatten()
    .max()
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
    AcceptedByPlayer(CacheMetadata),
    NotPublic(CacheMetadata),
    NotFound(CacheMetadata),
}

impl ContractItemProbe {
    fn metadata(&self) -> &CacheMetadata {
        match self {
            Self::Available(response) => &response.metadata,
            Self::NoContent(metadata) => metadata,
            Self::AcceptedByPlayer(metadata) => metadata,
            Self::NotPublic(metadata) => metadata,
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
    fn supports_region_name_lookup(&self) -> bool {
        false
    }
    async fn region_name(
        &self,
        _region_id: i64,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<Option<String>>, EsiError> {
        Ok(EsiResponse::fresh(
            None,
            CacheMetadata::cached_for_seconds(0),
        ))
    }
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

    /// Creates the deadline shared by all optional presentation enrichment in one attempt.
    fn observed_embed_enrichment_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + OPTIONAL_EMBED_ENRICHMENT_BUDGET
    }

    /// Uses a caller-owned deadline so snapshot and notification enrichment cannot renew it.
    async fn observed_contract_embed_context_limited_until(
        &self,
        contract: &PublicContract,
        items: &[PublicContractItem],
        limiter: &dyn ContractContextLimiter,
        _deadline: tokio::time::Instant,
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        self.observed_contract_embed_context_limited(contract, items, limiter)
            .await
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

    /// The public ESI solar-system endpoint also provides the display name used by jump links.
    /// The collector owns durable caching and limiter accounting for these lookups.
    async fn solar_system_name(
        &self,
        _solar_system_id: u32,
        _etag: Option<&str>,
    ) -> Result<EsiResponse<String>, EsiError> {
        Err(EsiError::retryable(
            "solar-system name lookup is unavailable",
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
    fn permits_parallel_enrichment(&self) -> bool {
        false
    }
    async fn wait_for_request_admission(&self) -> Result<(), EsiError> {
        if self.request_allowed().await? {
            Ok(())
        } else {
            Err(EsiError::retryable(
                "persisted global ESI limiter boundary remains active",
                None,
            ))
        }
    }
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
    optional_enrichment_budget: Duration,
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
            optional_enrichment_budget: OPTIONAL_EMBED_ENRICHMENT_BUDGET,
        })
    }

    /// Sets the wall-clock budget for optional public-contract presentation enrichment.
    /// The production constructor retains the thirty-second default.
    pub fn with_optional_enrichment_budget(mut self, budget: Duration) -> Self {
        self.optional_enrichment_budget = budget;
        self
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        etag: Option<&str>,
    ) -> Result<EsiResponse<T>, EsiError> {
        let url = format!("{}{}", self.base_url, path);
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
        let body = response
            .bytes()
            .await
            .map_err(|error| EsiError::from_metadata(error.to_string(), None, metadata.clone()))?;
        let value = serde_json::from_slice(&body).map_err(|error| {
            EsiError::from_metadata(
                format!("error decoding response body: {error}"),
                None,
                metadata.clone(),
            )
        })?;
        Ok(EsiResponse::fresh(value, metadata))
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
            .map_err(|error| EsiError::from_metadata(error.to_string(), None, metadata.clone()))?;
        Ok(EsiResponse::fresh(value, metadata))
    }

    async fn guarded_context_get<T: DeserializeOwned>(
        &self,
        path: &str,
        etag: Option<&str>,
        limiter: &dyn ContractContextLimiter,
    ) -> Result<EsiResponse<T>, EsiError> {
        limiter.wait_for_request_admission().await?;
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
        limiter.wait_for_request_admission().await?;
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
            StatusCode::FORBIDDEN => {
                #[derive(Deserialize)]
                struct ContractProbeError {
                    error: String,
                }

                let error = response
                    .json::<ContractProbeError>()
                    .await
                    .map(|body| body.error)
                    .unwrap_or_default();
                match error.as_str() {
                    CONTRACT_ACCEPTED_BY_PLAYER_ERROR => {
                        Ok(ContractItemProbe::AcceptedByPlayer(metadata))
                    }
                    CONTRACT_NOT_PUBLIC_ERROR => Ok(ContractItemProbe::NotPublic(metadata)),
                    _ => Err(EsiError::from_metadata(
                        "ESI returned 403 Forbidden",
                        Some(StatusCode::FORBIDDEN),
                        metadata,
                    )),
                }
            }
            StatusCode::NOT_FOUND => Ok(ContractItemProbe::NotFound(metadata)),
            StatusCode::NOT_MODIFIED => Ok(ContractItemProbe::Available(
                EsiResponse::not_modified(metadata),
            )),
            status if status.is_success() => {
                let value = response.json().await.map_err(|error| {
                    EsiError::from_metadata(error.to_string(), None, metadata.clone())
                })?;
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

#[derive(Deserialize)]
struct PublicEmbedStation {
    name: String,
    system_id: i64,
}

#[derive(Deserialize)]
struct PublicEmbedSolarSystem {
    name: String,
    security_status: f64,
    constellation_id: i64,
    position: Option<SolarSystemPosition>,
}

#[derive(Deserialize)]
struct PublicEmbedConstellation {
    region_id: i64,
}

#[derive(Deserialize)]
struct PublicEmbedRegion {
    name: String,
}

#[derive(Deserialize)]
struct PublicEmbedAffiliation {
    alliance_id: Option<i64>,
}

#[derive(Deserialize)]
struct PublicEmbedName {
    id: i64,
    name: String,
}

impl HttpPublicContractEsi {
    async fn optional_context_get<T: DeserializeOwned>(
        &self,
        cache_key: &str,
        path: &str,
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> Option<EsiResponse<T>> {
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        match tokio::time::timeout_at(
            deadline,
            self.cached_context_get::<T>(cache_key, path, limiter),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                warn!(
                    error = %error,
                    "optional public contract enrichment request failed before the enrichment deadline"
                );
                None
            }
            Err(_) => None,
        }
    }

    async fn optional_context_post<T: DeserializeOwned, B: Serialize>(
        &self,
        cache_key: &str,
        path: &str,
        body: &B,
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> Option<EsiResponse<T>> {
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        match tokio::time::timeout_at(
            deadline,
            self.cached_context_post::<T, B>(cache_key, path, body, limiter),
        )
        .await
        {
            Ok(Ok(response)) => Some(response),
            Ok(Err(error)) => {
                warn!(
                    error = %error,
                    "optional public contract enrichment request failed before the enrichment deadline"
                );
                None
            }
            Err(_) => None,
        }
    }

    async fn observed_location_context_before_deadline(
        &self,
        location_id: i64,
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> (ContractLocationContext, CacheMetadata) {
        let mut context = ContractLocationContext::default();
        let mut metadata = CacheMetadata::cached_for_seconds(0);
        let Ok(location_id) = u32::try_from(location_id) else {
            return (context, metadata);
        };
        if (30_000_000..33_000_000).contains(&location_id) {
            context.solar_system_id = Some(i64::from(location_id));
        } else {
            let Some(response) = self
                .optional_context_get::<PublicEmbedStation>(
                    &format!("universe/stations/{location_id}"),
                    &format!("universe/stations/{location_id}/"),
                    limiter,
                    deadline,
                )
                .await
            else {
                return (context, metadata);
            };
            metadata = merge_cache_metadata(&metadata, response.metadata);
            let Some(station) = response.value else {
                return (context, metadata);
            };
            context.location_name = Some(station.name);
            context.location_kind = Some("Station".to_string());
            context.solar_system_id = Some(station.system_id);
        }
        let Some(system_id) = context.solar_system_id else {
            return (context, metadata);
        };
        let Some(response) = self
            .optional_context_get::<PublicEmbedSolarSystem>(
                &format!("universe/systems/{system_id}"),
                &format!("universe/systems/{system_id}/"),
                limiter,
                deadline,
            )
            .await
        else {
            return (context, metadata);
        };
        metadata = merge_cache_metadata(&metadata, response.metadata);
        let Some(system) = response.value else {
            return (context, metadata);
        };
        context.solar_system_name = Some(system.name);
        context.security_status = system
            .security_status
            .is_finite()
            .then_some(system.security_status);
        context.solar_system_position = system.position.filter(|position| position.is_finite());
        let Some(response) = self
            .optional_context_get::<PublicEmbedConstellation>(
                &format!("universe/constellations/{}", system.constellation_id),
                &format!("universe/constellations/{}/", system.constellation_id),
                limiter,
                deadline,
            )
            .await
        else {
            return (context, metadata);
        };
        metadata = merge_cache_metadata(&metadata, response.metadata);
        let Some(constellation) = response.value else {
            return (context, metadata);
        };
        context.region_id = Some(constellation.region_id);
        let Some(response) = self
            .optional_context_get::<PublicEmbedRegion>(
                &format!("universe/regions/{}", constellation.region_id),
                &format!("universe/regions/{}/", constellation.region_id),
                limiter,
                deadline,
            )
            .await
        else {
            return (context, metadata);
        };
        metadata = merge_cache_metadata(&metadata, response.metadata);
        if let Some(region) = response.value {
            context.region_name = Some(region.name);
        }
        (context, metadata)
    }

    async fn observed_affiliation_before_deadline(
        &self,
        issuer_id: i64,
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> (Option<i64>, CacheMetadata) {
        let Some(response) = self
            .optional_context_post::<Vec<PublicEmbedAffiliation>, _>(
                &format!("characters/affiliation/{issuer_id}"),
                "characters/affiliation/",
                &vec![issuer_id],
                limiter,
                deadline,
            )
            .await
        else {
            return (None, CacheMetadata::cached_for_seconds(0));
        };
        let alliance_id = response
            .value
            .and_then(|affiliations| affiliations.into_iter().next())
            .and_then(|affiliation| affiliation.alliance_id);
        (alliance_id, response.metadata)
    }

    async fn public_names_before_deadline(
        &self,
        ids: Vec<i64>,
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> (BTreeMap<i64, String>, CacheMetadata) {
        if ids.is_empty() {
            return (BTreeMap::new(), CacheMetadata::cached_for_seconds(0));
        }
        let key = format!(
            "universe/names/{}",
            ids.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );
        let Some(response) = self
            .optional_context_post::<Vec<PublicEmbedName>, _>(
                &key,
                "universe/names/",
                &ids,
                limiter,
                deadline,
            )
            .await
        else {
            return (BTreeMap::new(), CacheMetadata::cached_for_seconds(0));
        };
        (
            response
                .value
                .unwrap_or_default()
                .into_iter()
                .map(|name| (name.id, name.name))
                .collect(),
            response.metadata,
        )
    }

    async fn observed_contract_embed_context_parallel(
        &self,
        contract: &PublicContract,
        items: &[PublicContractItem],
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        let mut names = BTreeSet::new();
        names.insert(contract.issuer_id);
        names.insert(contract.issuer_corporation_id);
        names.extend(items.iter().map(|item| item.type_id));
        let names = names.into_iter().collect::<Vec<_>>();
        let (location, affiliation, names) = tokio::join!(
            self.observed_location_context_before_deadline(
                contract.start_location_id,
                limiter,
                deadline,
            ),
            self.observed_affiliation_before_deadline(contract.issuer_id, limiter, deadline),
            self.public_names_before_deadline(names, limiter, deadline),
        );
        let (location, location_metadata) = location;
        let (issuer_alliance_id, affiliation_metadata) = affiliation;
        let (resolved_names, names_metadata) = names;
        let mut metadata = merge_cache_metadata(&location_metadata, affiliation_metadata);
        metadata = merge_cache_metadata(&metadata, names_metadata);
        let mut context = ContractEmbedContext {
            observed_at: Some(Utc::now()),
            location,
            issuer_alliance_id,
            ..ContractEmbedContext::default()
        };
        for (id, name) in resolved_names {
            if id == contract.issuer_id {
                context.issuer_character_name = Some(name);
            } else if id == contract.issuer_corporation_id {
                context.issuer_corporation_name = Some(name);
            } else {
                context.item_names.insert(id, name);
            }
        }
        if let Some(alliance_id) = context.issuer_alliance_id {
            let (names, alliance_metadata) = self
                .public_names_before_deadline(vec![alliance_id], limiter, deadline)
                .await;
            metadata = merge_cache_metadata(&metadata, alliance_metadata);
            context.issuer_alliance_name = names.get(&alliance_id).cloned();
        }
        if context.issuer_character_name.is_some()
            || context.issuer_corporation_name.is_some()
            || context.issuer_alliance_id.is_some()
            || context.issuer_alliance_name.is_some()
        {
            context.issuer_identity_provenance = Some(IssuerIdentityProvenance::PublicEsi);
        }
        Ok(EsiResponse::fresh(context, metadata))
    }
}

#[async_trait]
impl PublicContractEsi for HttpPublicContractEsi {
    async fn regions(&self, etag: Option<&str>) -> Result<EsiResponse<Vec<i64>>, EsiError> {
        self.get("universe/regions/", etag).await
    }

    fn supports_region_name_lookup(&self) -> bool {
        true
    }

    fn observed_embed_enrichment_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.optional_enrichment_budget
    }

    async fn region_name(
        &self,
        region_id: i64,
        etag: Option<&str>,
    ) -> Result<EsiResponse<Option<String>>, EsiError> {
        #[derive(Deserialize)]
        struct Region {
            name: String,
        }

        let response = self
            .get::<Region>(&format!("universe/regions/{region_id}/"), etag)
            .await?;
        Ok(EsiResponse {
            value: response.value.map(|region| Some(region.name)),
            metadata: response.metadata,
            not_modified: response.not_modified,
        })
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

    async fn solar_system_name(
        &self,
        solar_system_id: u32,
        etag: Option<&str>,
    ) -> Result<EsiResponse<String>, EsiError> {
        #[derive(Deserialize)]
        struct SolarSystem {
            name: String,
        }

        let response = self
            .get::<SolarSystem>(&format!("universe/systems/{solar_system_id}/"), etag)
            .await?;
        Ok(EsiResponse {
            value: response.value.map(|system| system.name),
            metadata: response.metadata,
            not_modified: response.not_modified,
        })
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
            .map_err(|error| EsiError::from_metadata(error.to_string(), None, metadata.clone()))?;
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
        if limiter.permits_parallel_enrichment() {
            return self
                .observed_contract_embed_context_parallel(
                    contract,
                    items,
                    limiter,
                    self.observed_embed_enrichment_deadline(),
                )
                .await;
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
                .cached_context_get::<PublicEmbedStation>(
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
                .cached_context_get::<PublicEmbedSolarSystem>(
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
                        .cached_context_get::<PublicEmbedConstellation>(
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
                                .cached_context_get::<PublicEmbedRegion>(
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
            .cached_context_post::<Vec<PublicEmbedAffiliation>, _>(
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
                .cached_context_post::<Vec<PublicEmbedName>, _>(
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
        if context.issuer_character_name.is_some()
            || context.issuer_corporation_name.is_some()
            || context.issuer_alliance_id.is_some()
            || context.issuer_alliance_name.is_some()
        {
            context.issuer_identity_provenance = Some(IssuerIdentityProvenance::PublicEsi);
        }
        Ok(EsiResponse::fresh(context, metadata))
    }

    async fn observed_contract_embed_context_limited_until(
        &self,
        contract: &PublicContract,
        items: &[PublicContractItem],
        limiter: &dyn ContractContextLimiter,
        deadline: tokio::time::Instant,
    ) -> Result<EsiResponse<ContractEmbedContext>, EsiError> {
        if limiter.permits_parallel_enrichment() {
            return self
                .observed_contract_embed_context_parallel(contract, items, limiter, deadline)
                .await;
        }
        self.observed_contract_embed_context_limited(contract, items, limiter)
            .await
    }
}

pub(crate) fn cache_metadata(headers: &HeaderMap) -> CacheMetadata {
    cache_metadata_at(headers, Utc::now())
}

pub(crate) fn cache_metadata_at(headers: &HeaderMap, observed_at: DateTime<Utc>) -> CacheMetadata {
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
            .and_then(|value| parse_retry_after_at(value, observed_at)),
    };
    if let Some(seconds) = headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .and_then(cache_max_age)
    {
        metadata.expires_at = Some(observed_at + ChronoDuration::seconds(seconds));
    }
    if metadata.retry_after.is_none() && metadata.error_limit_remain.unwrap_or(1) <= 0 {
        metadata.retry_after = metadata
            .error_limit_reset
            .map(|seconds| observed_at + ChronoDuration::seconds(seconds));
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

fn parse_retry_after_at(value: &str, observed_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    value
        .parse::<i64>()
        .ok()
        .map(|seconds| observed_at + ChronoDuration::seconds(seconds))
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
    rate_limit_capacity_and_window_seconds(value).map(|(_, window)| window)
}

fn rate_limit_capacity_and_window_seconds(value: &str) -> Option<(i64, i64)> {
    let (limit, window) = value.split_once('/')?;
    let limit = limit.parse::<i64>().ok()?;
    let (number, unit) = window.split_at(window.len().checked_sub(1)?);
    let count = number.parse::<i64>().ok()?;
    let window_seconds = match unit {
        "s" => Some(count),
        "m" => count.checked_mul(60),
        "h" => count.checked_mul(60 * 60),
        "d" => count.checked_mul(60 * 60 * 24),
        _ => None,
    }?;
    (limit > 0 && window_seconds > 0).then_some((limit, window_seconds))
}

fn rate_limit_pacing_deadline(
    rate_limit: &str,
    remaining: i64,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let (limit, window_seconds) = rate_limit_capacity_and_window_seconds(rate_limit)?;
    if remaining < 0 || remaining.saturating_mul(10) > limit {
        return None;
    }
    let spacing_seconds = (window_seconds + remaining.max(1) - 1) / remaining.max(1);
    Some(observed_at + ChronoDuration::seconds(spacing_seconds.max(1)))
}

#[derive(Clone)]
pub struct ContractCollectionStore {
    pool: PgPool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StructureResolutionAdmission {
    Inactive,
    Attempt {
        generation: i64,
        cached: Option<Box<ResolvedStructure>>,
    },
    WaitUntil(DateTime<Utc>),
    Parked,
}

enum EsiRequestAdmission {
    Granted,
    WaitUntil(DateTime<Utc>),
    PausedUntil(DateTime<Utc>),
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

#[derive(Clone, Debug)]
struct PersistedBacklogMetric {
    due_count: i64,
    peak_due_count: i64,
    growth_detected: bool,
}

#[derive(Clone, Debug)]
struct BacklogMetric {
    due_count: i64,
    peak_due_count: i64,
    growth_detected: bool,
    oldest_due_at: Option<DateTime<Utc>>,
}

struct HealthInputs {
    heartbeat_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    regional_progress: Vec<(i64, Option<DateTime<Utc>>)>,
    esi_progress_at: Option<DateTime<Utc>>,
    esi_pause_until: Option<DateTime<Utc>>,
    due_backlog_count: i64,
    oldest_due_backlog_at: Option<DateTime<Utc>>,
    prior_backlog_metric: Option<PersistedBacklogMetric>,
    oldest_prepared_delivery_at: Option<DateTime<Utc>>,
    permanent_delivery_failures: i64,
    r2z2_progress_at: Option<DateTime<Utc>>,
    feed_validation_at: Option<DateTime<Utc>>,
    feed_validation_failures: i32,
    structure_resolver: Option<(bool, String, Option<String>, DateTime<Utc>)>,
}

impl HealthCycle {
    pub async fn run_once(&self) -> Result<HealthSnapshot, sqlx::Error> {
        self.store.initialize_health_schema().await?;
        let observed_at = self.clock.now();
        let inputs = self.store.health_inputs(observed_at).await?;
        let startup_at = inputs.started_at.unwrap_or(observed_at);
        let feed_snapshot = if let Some(feed_telemetry) = &self.feed_telemetry {
            feed_telemetry
                .hydrate_validation_state(
                    inputs.feed_validation_at,
                    inputs.feed_validation_failures,
                )
                .await;
            Some(feed_telemetry.snapshot().await)
        } else {
            None
        };
        let (backlog_check, backlog_metric) =
            growing_backlog_health_check(&inputs, observed_at, &self.thresholds);
        let mut checks = vec![
            age_health_check(
                "heartbeat",
                inputs.heartbeat_at,
                observed_at,
                self.thresholds.heartbeat_degraded,
                self.thresholds.heartbeat_critical,
                "bot heartbeat",
            ),
            regional_progress_health_check(&inputs, startup_at, observed_at, &self.thresholds),
            esi_progress_health_check(&inputs, startup_at, observed_at, &self.thresholds),
            backlog_check,
            age_health_check(
                "prepared_delivery",
                inputs.oldest_prepared_delivery_at,
                observed_at,
                self.thresholds.prepared_delivery_degraded,
                self.thresholds.prepared_delivery_critical,
                "oldest prepared delivery",
            ),
        ];
        checks.push(permanent_delivery_health_check(
            observed_at,
            inputs.permanent_delivery_failures,
        ));
        if let Some((enabled, status, error, updated_at)) = &inputs.structure_resolver {
            checks.push(structure_resolver_health_check(
                *enabled,
                status,
                error.as_deref(),
                *updated_at,
            ));
        }
        if let Some(feed_snapshot) = &feed_snapshot {
            if self.r2z2_enabled {
                checks.push(progress_health_check(
                    "r2z2_progress",
                    feed_snapshot.r2z2_progress_at.or(inputs.r2z2_progress_at),
                    startup_at,
                    observed_at,
                    self.thresholds.r2z2_progress_degraded,
                    self.thresholds.r2z2_progress_critical,
                    "R2Z2 progress",
                ));
            }
            checks.push(feed_validation_health_check(
                feed_snapshot
                    .validation_success_at
                    .or(inputs.feed_validation_at),
                feed_snapshot.validation_failures,
                startup_at,
                observed_at,
                &self.thresholds,
            ));
        }
        let status = checks
            .iter()
            .map(|check| check.status)
            .max()
            .unwrap_or(HealthStatus::Healthy);
        let snapshot = HealthSnapshot {
            status,
            observed_at,
            checks,
        };

        self.store
            .save_health_snapshot(&snapshot, &backlog_metric, feed_snapshot.as_ref())
            .await?;
        Ok(snapshot)
    }

    pub async fn run_observationally(&self) -> Option<HealthSnapshot> {
        match self.run_once().await {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                warn!("health cycle failed observationally: {error}");
                None
            }
        }
    }
}

pub fn spawn_health_monitor_loop(
    database_url: String,
    config: HealthRuntimeConfig,
    feed_telemetry: Arc<FeedHealthTelemetry>,
    r2z2_enabled: bool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_health_monitor_loop(
        database_url,
        config,
        feed_telemetry,
        r2z2_enabled,
    ))
}

pub async fn run_health_monitor_loop(
    database_url: String,
    config: HealthRuntimeConfig,
    feed_telemetry: Arc<FeedHealthTelemetry>,
    r2z2_enabled: bool,
) {
    let mut session_started = false;
    loop {
        match ContractCollectionStore::connect(&database_url).await {
            Ok(store) => {
                if !session_started {
                    match store.begin_health_monitor_session(Utc::now()).await {
                        Ok(()) => session_started = true,
                        Err(error) => {
                            warn!("health monitor session failed observationally: {error}")
                        }
                    }
                }
                if session_started {
                    HealthCycle::new(store, Arc::new(SystemHealthClock))
                        .with_thresholds(config.thresholds.clone())
                        .with_feed_telemetry(feed_telemetry.clone(), r2z2_enabled)
                        .run_observationally()
                        .await;
                }
            }
            Err(error) => warn!("health store initialization failed observationally: {error}"),
        }
        tokio::time::sleep(config.evaluation_interval).await;
    }
}

fn age_health_check(
    key: &str,
    observed_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    degraded_after: ChronoDuration,
    critical_after: ChronoDuration,
    label: &str,
) -> HealthCheck {
    let (status, evidence) = match observed_at {
        None => (HealthStatus::Healthy, format!("no {label} recorded yet")),
        Some(observed_at) => {
            let age = now
                .signed_duration_since(observed_at)
                .max(ChronoDuration::zero());
            let status = if age >= critical_after {
                HealthStatus::Critical
            } else if age >= degraded_after {
                HealthStatus::Degraded
            } else {
                HealthStatus::Healthy
            };
            (status, format!("{label} is {}s old", age.num_seconds()))
        }
    };
    HealthCheck {
        key: key.to_string(),
        status,
        observed_at: now,
        evidence,
        consecutive_failures: 0,
    }
}

fn watchdog_heartbeat_health_check(
    key: &str,
    heartbeat_at: Option<DateTime<Utc>>,
    watchdog_started_at: DateTime<Utc>,
    now: DateTime<Utc>,
    degraded_after: ChronoDuration,
    critical_after: ChronoDuration,
) -> HealthCheck {
    match heartbeat_at {
        Some(heartbeat_at) => age_health_check(
            key,
            Some(heartbeat_at),
            now,
            degraded_after,
            critical_after,
            "bot heartbeat",
        ),
        None => age_health_check(
            key,
            Some(watchdog_started_at),
            now,
            degraded_after,
            critical_after,
            "bot heartbeat has not been recorded since watchdog start",
        ),
    }
}

fn regional_progress_health_check(
    inputs: &HealthInputs,
    startup_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    thresholds: &HealthThresholds,
) -> HealthCheck {
    if inputs.regional_progress.is_empty() {
        return progress_health_check(
            "contract_progress",
            None,
            startup_at,
            observed_at,
            thresholds.contract_progress_degraded,
            thresholds.contract_progress_critical,
            "regional contract progress",
        );
    }
    let stale_regions = inputs
        .regional_progress
        .iter()
        .map(|(region_id, progress_at)| {
            let check = age_health_check(
                "contract_progress",
                progress_at.or(Some(startup_at)),
                observed_at,
                thresholds.contract_progress_degraded,
                thresholds.contract_progress_critical,
                "regional contract progress",
            );
            (*region_id, check.status, check.evidence)
        })
        .collect::<Vec<_>>();
    let status = stale_regions
        .iter()
        .map(|(_, status, _)| *status)
        .max()
        .unwrap_or(HealthStatus::Healthy);
    let stale_count = stale_regions
        .iter()
        .filter(|(_, region_status, _)| *region_status != HealthStatus::Healthy)
        .count();
    let details = stale_regions
        .iter()
        .filter(|(_, region_status, _)| *region_status != HealthStatus::Healthy)
        .take(MAX_HEALTH_REGIONAL_DETAILS)
        .map(|(region_id, _, evidence)| format!("region {region_id}: {evidence}"))
        .collect::<Vec<_>>();
    HealthCheck {
        key: "contract_progress".to_string(),
        status,
        observed_at,
        evidence: if details.is_empty() {
            format!(
                "{} region(s) have current contract progress",
                stale_regions.len()
            )
        } else {
            let mut evidence = details.join("; ");
            let omitted = stale_count.saturating_sub(details.len());
            if omitted > 0 {
                evidence.push_str(&format!("; {omitted} additional stale region(s) truncated"));
            }
            evidence
        },
        consecutive_failures: 0,
    }
}

fn esi_progress_health_check(
    inputs: &HealthInputs,
    startup_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    thresholds: &HealthThresholds,
) -> HealthCheck {
    if inputs
        .esi_pause_until
        .is_some_and(|until| until > observed_at)
    {
        return HealthCheck {
            key: "esi_progress".to_string(),
            status: HealthStatus::Healthy,
            observed_at,
            evidence: "ESI collection is within its persisted pause window".to_string(),
            consecutive_failures: 0,
        };
    }
    progress_health_check(
        "esi_progress",
        inputs.esi_progress_at,
        startup_at,
        observed_at,
        thresholds.esi_progress_degraded,
        thresholds.esi_progress_critical,
        "ESI progress",
    )
}

fn progress_health_check(
    key: &str,
    progress_at: Option<DateTime<Utc>>,
    startup_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    degraded_after: ChronoDuration,
    critical_after: ChronoDuration,
    label: &str,
) -> HealthCheck {
    let mut check = age_health_check(
        key,
        progress_at.or(Some(startup_at)),
        observed_at,
        degraded_after,
        critical_after,
        label,
    );
    if progress_at.is_none() {
        check.evidence = format!("no {label} recorded since health startup");
    }
    check
}

fn growing_backlog_health_check(
    inputs: &HealthInputs,
    observed_at: DateTime<Utc>,
    thresholds: &HealthThresholds,
) -> (HealthCheck, BacklogMetric) {
    let previous = inputs.prior_backlog_metric.as_ref();
    let (peak_due_count, growth_detected, trend) = match previous {
        None => (
            inputs.due_backlog_count,
            false,
            "awaiting a bounded trend baseline".to_string(),
        ),
        Some(_) if inputs.due_backlog_count == 0 => (0, false, "backlog cleared".to_string()),
        Some(previous) if inputs.due_backlog_count < previous.due_count => (
            inputs.due_backlog_count,
            false,
            format!(
                "backlog shrank from {} to {}",
                previous.due_count, inputs.due_backlog_count
            ),
        ),
        Some(previous) if inputs.due_backlog_count > previous.due_count => (
            previous.peak_due_count.max(inputs.due_backlog_count),
            true,
            format!(
                "backlog grew from {} to {}",
                previous.due_count, inputs.due_backlog_count
            ),
        ),
        Some(previous) => (
            previous.peak_due_count,
            previous.growth_detected,
            if previous.growth_detected {
                format!(
                    "backlog remains at {} after growing to {}",
                    inputs.due_backlog_count, previous.peak_due_count
                )
            } else {
                format!("backlog remains at {}", inputs.due_backlog_count)
            },
        ),
    };
    let should_alert = inputs.due_backlog_count > 0 && growth_detected;
    let age = inputs.oldest_due_backlog_at.map(|due_at| {
        observed_at
            .signed_duration_since(due_at)
            .max(ChronoDuration::zero())
    });
    let status = if !should_alert {
        HealthStatus::Healthy
    } else if age.is_some_and(|age| age >= thresholds.backlog_critical) {
        HealthStatus::Critical
    } else if age.is_some_and(|age| age >= thresholds.backlog_degraded) {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    let evidence = match age {
        Some(age) => format!(
            "{} due manifest or resolution backlog item(s); {trend}; oldest is {}s old",
            inputs.due_backlog_count,
            age.num_seconds()
        ),
        None => "no due manifest or resolution backlog".to_string(),
    };
    (
        HealthCheck {
            key: "deferred_backlog".to_string(),
            status,
            observed_at,
            evidence,
            consecutive_failures: 0,
        },
        BacklogMetric {
            due_count: inputs.due_backlog_count,
            peak_due_count,
            growth_detected,
            oldest_due_at: inputs.oldest_due_backlog_at,
        },
    )
}

fn feed_validation_health_check(
    validation_at: Option<DateTime<Utc>>,
    consecutive_failures: i32,
    startup_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    thresholds: &HealthThresholds,
) -> HealthCheck {
    let mut check = progress_health_check(
        "feed_validation",
        validation_at,
        startup_at,
        observed_at,
        thresholds.r2z2_progress_degraded,
        thresholds.r2z2_progress_critical,
        "successful feed validation",
    );
    check.consecutive_failures = consecutive_failures;
    if consecutive_failures >= thresholds.feed_validation_critical_failures {
        check.status = HealthStatus::Critical;
        check.evidence = format!("{consecutive_failures} consecutive feed validation failure(s)");
    } else if consecutive_failures >= thresholds.feed_validation_degraded_failures {
        check.status = HealthStatus::Degraded;
        check.evidence = format!("{consecutive_failures} consecutive feed validation failure(s)");
    }
    check
}

fn permanent_delivery_health_check(
    observed_at: DateTime<Utc>,
    permanent_delivery_failures: i64,
) -> HealthCheck {
    let status = if permanent_delivery_failures > 0 {
        HealthStatus::Critical
    } else {
        HealthStatus::Healthy
    };
    HealthCheck {
        key: "permanent_delivery_failure".to_string(),
        status,
        observed_at,
        evidence: if permanent_delivery_failures == 0 {
            "no unresolved permanent Discord delivery failures".to_string()
        } else {
            format!(
                "{permanent_delivery_failures} unresolved permanent Discord delivery failure(s)"
            )
        },
        consecutive_failures: permanent_delivery_failures.clamp(0, i64::from(i32::MAX)) as i32,
    }
}

fn structure_resolver_health_check(
    enabled: bool,
    status: &str,
    error: Option<&str>,
    observed_at: DateTime<Utc>,
) -> HealthCheck {
    let status = if status == "ready" {
        HealthStatus::Healthy
    } else {
        HealthStatus::Degraded
    };
    HealthCheck {
        key: "structure_resolver".to_string(),
        status,
        observed_at,
        evidence: if status == HealthStatus::Healthy {
            if enabled {
                "structure resolver ready".to_string()
            } else {
                "structure resolver disabled".to_string()
            }
        } else {
            format!(
                "structure resolver degraded: {}",
                error.unwrap_or("authorization or upstream failure")
            )
        },
        consecutive_failures: (status != HealthStatus::Healthy).into(),
    }
}

pub fn render_health_view(snapshot: &HealthSnapshot) -> String {
    let detail = snapshot
        .checks
        .iter()
        .filter(|check| check.status != HealthStatus::Healthy)
        .map(|check| format!("{}: {}", check.key, check.evidence))
        .collect::<Vec<_>>();
    let summary = if detail.is_empty() {
        "all checks healthy".to_string()
    } else {
        detail.join("\n")
    };
    format!(
        "Health: {}\n{}",
        snapshot.status.as_str(),
        summary.replace('@', "@\u{200b}")
    )
}

impl HealthWatchdog {
    pub async fn run_once(&self) -> Result<HealthSnapshot, sqlx::Error> {
        let observed_at = self.clock.now();
        let mut snapshot = self
            .store
            .health_snapshot_with_watchdog_publication_failures()
            .await?
            .unwrap_or(HealthSnapshot {
                status: HealthStatus::Healthy,
                observed_at,
                checks: Vec::new(),
            });
        let watchdog_started_at = self.store.health_watchdog_started_at(observed_at).await?;
        snapshot.checks.retain(|check| check.key != "heartbeat");
        snapshot.checks.push(watchdog_heartbeat_health_check(
            "heartbeat",
            self.store.bot_heartbeat_at().await?,
            watchdog_started_at,
            observed_at,
            self.config.thresholds.heartbeat_degraded,
            self.config.thresholds.heartbeat_critical,
        ));
        snapshot
            .checks
            .sort_by(|left, right| left.key.cmp(&right.key));
        snapshot.status = snapshot
            .checks
            .iter()
            .map(|check| check.status)
            .max()
            .unwrap_or(HealthStatus::Healthy);
        snapshot.observed_at = observed_at;

        let content = render_watchdog_health_content("Health", &snapshot.checks, snapshot.status);
        let view_state = self.store.health_discord_view().await?;
        let mut view_failure = None;
        if health_discord_publication_is_due(view_state.as_ref(), &self.config, observed_at) {
            let view_result = match view_state
                .as_ref()
                .filter(|state| state.channel_id == self.config.channel_id)
                .and_then(|state| state.message_id.as_deref())
            {
                Some(message_id) => self
                    .publisher
                    .update_view(
                        view_state.as_ref().expect("view state exists").channel_id,
                        message_id,
                        &content,
                    )
                    .await
                    .map(|()| message_id.to_string()),
                None => {
                    self.publisher
                        .create_view(self.config.channel_id, &content)
                        .await
                }
            };
            match view_result {
                Ok(message_id) => {
                    self.store
                        .save_health_discord_view_identity(
                            self.config.channel_id,
                            &self.config.config_revision,
                            &message_id,
                            observed_at,
                        )
                        .await?;
                    view_failure = None;
                }
                Err(error) => {
                    self.store
                        .record_health_discord_view_failure(
                            self.config.channel_id,
                            &self.config.config_revision,
                            &error,
                            observed_at,
                        )
                        .await?;
                    view_failure = Some(health_discord_publication_failure_check_from_error(
                        "discord_health_view",
                        &error,
                        observed_at,
                    ));
                    warn!(
                        "health Discord view publication failed observationally: {}",
                        error.detail()
                    );
                }
            }
        }

        let mut degraded = snapshot
            .checks
            .iter()
            .filter(|check| check.status != HealthStatus::Healthy)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(view_failure) = view_failure {
            degraded.push(view_failure);
        }
        let incident_status = degraded
            .iter()
            .map(|check| check.status)
            .max()
            .unwrap_or(HealthStatus::Healthy);
        if !degraded.is_empty() {
            let content =
                render_watchdog_health_content("Health incident", &degraded, incident_status);
            let incident_state = self.store.health_discord_incident().await?;
            if health_discord_incident_is_due(incident_state.as_ref(), &self.config, observed_at) {
                let incident_result = match incident_state
                    .as_ref()
                    .filter(|incident| incident.channel_id == self.config.channel_id)
                    .and_then(|incident| incident.message_id.as_deref())
                {
                    Some(message_id) => self
                        .publisher
                        .update_incident(
                            incident_state
                                .as_ref()
                                .expect("incident state exists")
                                .channel_id,
                            message_id,
                            &content,
                        )
                        .await
                        .map(|()| message_id.to_string()),
                    None => {
                        self.publisher
                            .create_incident(
                                self.config.channel_id,
                                &content,
                                incident_state
                                    .as_ref()
                                    .filter(|incident| incident.active)
                                    .map(|_| None)
                                    .unwrap_or(Some(crate::commands::health::HEALTH_OPERATOR_ID)),
                            )
                            .await
                    }
                };
                match incident_result {
                    Ok(message_id) => {
                        self.store
                            .save_health_discord_incident(
                                self.config.channel_id,
                                &self.config.config_revision,
                                &message_id,
                                true,
                                &degraded,
                                observed_at,
                            )
                            .await?;
                    }
                    Err(error) => {
                        self.store
                            .record_health_discord_incident_failure(
                                self.config.channel_id,
                                &self.config.config_revision,
                                &error,
                                &degraded,
                                observed_at,
                            )
                            .await?;
                        warn!(
                            "health Discord incident publication failed observationally: {}",
                            error.detail()
                        );
                    }
                }
            }
        } else if let Some(incident) = self.store.health_discord_incident().await? {
            if incident.active
                && incident.channel_id == self.config.channel_id
                && health_discord_incident_is_due(Some(&incident), &self.config, observed_at)
            {
                if let Some(message_id) = incident.message_id.as_deref() {
                    let result = self
                        .publisher
                        .update_incident(
                            incident.channel_id,
                            message_id,
                            "Health incident: resolved\nall checks healthy",
                        )
                        .await;
                    match result {
                        Ok(()) => {
                            self.store
                                .save_health_discord_incident(
                                    incident.channel_id,
                                    &self.config.config_revision,
                                    message_id,
                                    false,
                                    &[],
                                    observed_at,
                                )
                                .await?;
                        }
                        Err(error) => {
                            self.store
                                .record_health_discord_incident_failure(
                                    incident.channel_id,
                                    &self.config.config_revision,
                                    &error,
                                    &[],
                                    observed_at,
                                )
                                .await?;
                            warn!(
                                "health Discord incident recovery publication failed observationally: {}",
                                error.detail()
                            );
                        }
                    }
                }
            }
        }
        Ok(snapshot)
    }
}

impl HealthWatchdogRunner {
    pub async fn run_once(&self) -> HealthWatchdogRun {
        match ContractCollectionStore::connect(&self.database_url).await {
            Ok(store) => {
                if let Err(error) = self.adopt_cached_watchdog_state(&store).await {
                    warn!("watchdog could not adopt cached Discord identities: {error}");
                    return self.run_postgres_outage().await;
                }
                let persisted_snapshot_at = store
                    .health_snapshot()
                    .await
                    .ok()
                    .flatten()
                    .map(|snapshot| snapshot.observed_at);
                let heartbeat_at = store.bot_heartbeat_at().await.ok().flatten();
                let watchdog = HealthWatchdog::new(
                    store.clone(),
                    self.clock.clone(),
                    self.config.clone(),
                    self.publisher.clone(),
                );
                match watchdog.run_once().await {
                    Ok(snapshot) => {
                        *self.last_snapshot.lock().await = Some(snapshot.clone());
                        *self.last_persisted_snapshot_at.lock().await = persisted_snapshot_at;
                        *self.last_heartbeat_at.lock().await = heartbeat_at;
                        *self.cached_view.lock().await =
                            store.health_discord_view().await.ok().flatten();
                        *self.cached_incident.lock().await =
                            store.health_discord_incident().await.ok().flatten();
                        *self.postgres_failures.lock().await = 0;
                        HealthWatchdogRun {
                            status: snapshot.status,
                            consecutive_postgres_failures: 0,
                            database_available: true,
                        }
                    }
                    Err(error) => {
                        warn!("watchdog health database cycle failed observationally: {error}");
                        self.run_postgres_outage().await
                    }
                }
            }
            Err(error) => {
                warn!("watchdog PostgreSQL connection failed observationally: {error}");
                self.run_postgres_outage().await
            }
        }
    }

    async fn run_postgres_outage(&self) -> HealthWatchdogRun {
        let observed_at = self.clock.now();
        let watchdog_started_at = self.watchdog_started_at(observed_at).await;
        let consecutive_postgres_failures = {
            let mut failures = self.postgres_failures.lock().await;
            *failures = failures.saturating_add(1);
            *failures
        };
        let status = health_status_for_failures(
            consecutive_postgres_failures,
            self.config.thresholds.postgres_degraded_failures,
            self.config.thresholds.postgres_critical_failures,
        );
        let mut checks = self
            .last_snapshot
            .lock()
            .await
            .as_ref()
            .map(|snapshot| snapshot.checks.clone())
            .unwrap_or_default();
        checks.retain(|check| {
            check.key != "watchdog_postgres"
                && check.key != "heartbeat"
                && check.key != "watchdog_health_snapshot"
        });
        let heartbeat_at = *self.last_heartbeat_at.lock().await;
        checks.push(watchdog_heartbeat_health_check(
            "heartbeat",
            heartbeat_at,
            watchdog_started_at,
            observed_at,
            self.config.thresholds.heartbeat_degraded,
            self.config.thresholds.heartbeat_critical,
        ));
        let snapshot_at = *self.last_persisted_snapshot_at.lock().await;
        if snapshot_at.is_some() {
            checks.push(age_health_check(
                "watchdog_health_snapshot",
                snapshot_at,
                observed_at,
                self.config.thresholds.heartbeat_degraded,
                self.config.thresholds.heartbeat_critical,
                "cached persisted health snapshot",
            ));
        }
        checks.push(HealthCheck {
            key: "watchdog_postgres".to_string(),
            status,
            observed_at,
            evidence: format!(
                "watchdog PostgreSQL access has failed {consecutive_postgres_failures} consecutive time(s)"
            ),
            consecutive_failures: consecutive_postgres_failures,
        });
        checks.sort_by(|left, right| left.key.cmp(&right.key));
        let overall = checks
            .iter()
            .map(|check| check.status)
            .max()
            .unwrap_or(status);
        self.publish_postgres_outage(&checks, overall, observed_at)
            .await;
        HealthWatchdogRun {
            status: overall,
            consecutive_postgres_failures,
            database_available: false,
        }
    }

    async fn publish_postgres_outage(
        &self,
        checks: &[HealthCheck],
        status: HealthStatus,
        observed_at: DateTime<Utc>,
    ) {
        let view_content = render_watchdog_health_content("Health", checks, status);
        let view = self.cached_view.lock().await.clone();
        if health_discord_publication_is_due(view.as_ref(), &self.config, observed_at) {
            let view_result = match view
                .as_ref()
                .filter(|view| view.channel_id == self.config.channel_id)
                .and_then(|view| view.message_id.as_deref())
            {
                Some(message_id) => self
                    .publisher
                    .update_view(
                        view.as_ref().expect("cached view exists").channel_id,
                        message_id,
                        &view_content,
                    )
                    .await
                    .map(|()| message_id.to_string()),
                None => {
                    self.publisher
                        .create_view(self.config.channel_id, &view_content)
                        .await
                }
            };
            match view_result {
                Ok(message_id) => {
                    *self.cached_view.lock().await = Some(HealthDiscordViewState {
                        channel_id: self.config.channel_id,
                        config_revision: self.config.config_revision.clone(),
                        message_id: Some(message_id),
                        last_error: None,
                        failure_kind: None,
                        next_attempt_at: None,
                    });
                }
                Err(error) => {
                    *self.cached_view.lock().await = Some(HealthDiscordViewState {
                        channel_id: self.config.channel_id,
                        config_revision: self.config.config_revision.clone(),
                        message_id: view.as_ref().and_then(|view| view.message_id.clone()),
                        last_error: Some(error.detail().to_string()),
                        failure_kind: Some(error.failure_kind().to_string()),
                        next_attempt_at: error.next_attempt_at(observed_at),
                    });
                    warn!(
                        "watchdog outage Health View publication failed: {}",
                        error.detail()
                    );
                }
            }
        }

        let incident_content = render_watchdog_health_content("Health incident", checks, status);
        let incident = self.cached_incident.lock().await.clone();
        if health_discord_incident_is_due(incident.as_ref(), &self.config, observed_at) {
            let incident_result = match incident
                .as_ref()
                .filter(|incident| incident.channel_id == self.config.channel_id)
                .and_then(|incident| incident.message_id.as_deref())
            {
                Some(message_id) => self
                    .publisher
                    .update_incident(
                        incident
                            .as_ref()
                            .expect("cached incident exists")
                            .channel_id,
                        message_id,
                        &incident_content,
                    )
                    .await
                    .map(|()| message_id.to_string()),
                None => {
                    self.publisher
                        .create_incident(
                            self.config.channel_id,
                            &incident_content,
                            incident
                                .as_ref()
                                .filter(|incident| incident.active)
                                .map(|_| None)
                                .unwrap_or(Some(crate::commands::health::HEALTH_OPERATOR_ID)),
                        )
                        .await
                }
            };
            match incident_result {
                Ok(message_id) => {
                    *self.cached_incident.lock().await = Some(HealthDiscordIncidentState {
                        channel_id: self.config.channel_id,
                        config_revision: self.config.config_revision.clone(),
                        message_id: Some(message_id),
                        active: true,
                        last_error: None,
                        failure_kind: None,
                        next_attempt_at: None,
                    });
                }
                Err(error) => {
                    *self.cached_incident.lock().await = Some(HealthDiscordIncidentState {
                        channel_id: self.config.channel_id,
                        config_revision: self.config.config_revision.clone(),
                        message_id: incident
                            .as_ref()
                            .and_then(|incident| incident.message_id.clone()),
                        active: incident
                            .as_ref()
                            .map(|incident| incident.active)
                            .unwrap_or(true),
                        last_error: Some(error.detail().to_string()),
                        failure_kind: Some(error.failure_kind().to_string()),
                        next_attempt_at: error.next_attempt_at(observed_at),
                    });
                    warn!(
                        "watchdog outage incident publication failed: {}",
                        error.detail()
                    );
                }
            }
        }
    }

    async fn watchdog_started_at(&self, observed_at: DateTime<Utc>) -> DateTime<Utc> {
        let mut started_at = self.watchdog_started_at.lock().await;
        *started_at.get_or_insert(observed_at)
    }

    async fn adopt_cached_watchdog_state(
        &self,
        store: &ContractCollectionStore,
    ) -> Result<(), sqlx::Error> {
        let observed_at = self.clock.now();
        let cached_started_at = self.watchdog_started_at(observed_at).await;
        let persisted_started_at = store.health_watchdog_started_at(cached_started_at).await?;
        *self.watchdog_started_at.lock().await = Some(persisted_started_at);
        if let Some(view) = self.cached_view.lock().await.clone() {
            store
                .adopt_health_discord_view_state(&view, observed_at)
                .await?;
        }
        if let Some(incident) = self.cached_incident.lock().await.clone() {
            store
                .adopt_health_discord_incident_state(&incident, observed_at)
                .await?;
        }
        Ok(())
    }
}

pub async fn run_health_watchdog_loop(
    database_url: String,
    config: HealthWatchdogConfig,
    publisher: Arc<dyn HealthDiscordPublisher>,
) {
    let interval = config.evaluation_interval;
    let watchdog =
        HealthWatchdogRunner::new(database_url, Arc::new(SystemHealthClock), config, publisher);
    loop {
        watchdog.run_once().await;
        tokio::time::sleep(interval).await;
    }
}

fn health_status_for_failures(
    failures: i32,
    degraded_after: i32,
    critical_after: i32,
) -> HealthStatus {
    if failures >= critical_after {
        HealthStatus::Critical
    } else if failures >= degraded_after {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    }
}

fn render_watchdog_health_content(
    title: &str,
    checks: &[HealthCheck],
    status: HealthStatus,
) -> String {
    let detail = checks
        .iter()
        .filter(|check| check.status != HealthStatus::Healthy)
        .map(|check| format!("{}: {}", check.key, check.evidence))
        .collect::<Vec<_>>();
    let summary = if detail.is_empty() {
        "all checks healthy".to_string()
    } else {
        detail.join("\n")
    };
    format!(
        "{title}: {}\n{}",
        status.as_str(),
        summary.replace('@', "@\u{200b}")
    )
}

fn health_discord_publication_is_due(
    state: Option<&HealthDiscordViewState>,
    config: &HealthWatchdogConfig,
    observed_at: DateTime<Utc>,
) -> bool {
    match state {
        None => true,
        Some(state)
            if state.channel_id != config.channel_id
                || state.config_revision != config.config_revision =>
        {
            true
        }
        Some(state) if state.failure_kind.as_deref() == Some("permanent") => false,
        Some(state) => state
            .next_attempt_at
            .is_none_or(|next_attempt_at| next_attempt_at <= observed_at),
    }
}

fn health_discord_incident_is_due(
    state: Option<&HealthDiscordIncidentState>,
    config: &HealthWatchdogConfig,
    observed_at: DateTime<Utc>,
) -> bool {
    match state {
        None => true,
        Some(state)
            if state.channel_id != config.channel_id
                || state.config_revision != config.config_revision =>
        {
            true
        }
        Some(state) if state.failure_kind.as_deref() == Some("permanent") => false,
        Some(state) => state
            .next_attempt_at
            .is_none_or(|next_attempt_at| next_attempt_at <= observed_at),
    }
}

fn health_discord_publication_failure_check_from_error(
    key: &str,
    error: &HealthPublishError,
    observed_at: DateTime<Utc>,
) -> HealthCheck {
    HealthCheck {
        key: key.to_string(),
        status: if error.failure_kind() == "permanent" {
            HealthStatus::Critical
        } else {
            HealthStatus::Degraded
        },
        observed_at,
        evidence: format!(
            "Health View Discord publication {}: {}",
            error.failure_kind(),
            error.detail()
        ),
        consecutive_failures: 1,
    }
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
    pub acceptance_provenance: Option<ContractAcceptanceProvenance>,
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
    pub location_evidence_id: Option<i64>,
    #[serde(default)]
    pub location_evidence_class: Option<LocationEvidenceClass>,
    #[serde(default)]
    pub range_center_positions: BTreeMap<u32, SolarSystemPosition>,
    #[serde(default)]
    pub range_center_names: BTreeMap<u32, String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verified_at: Option<DateTime<Utc>>,
    pub solar_system_id: Option<i64>,
    pub solar_system_name: Option<String>,
    #[serde(default)]
    pub solar_system_position: Option<SolarSystemPosition>,
    pub security_status: Option<f64>,
    pub region_id: Option<i64>,
    pub region_name: Option<String>,
}

/// Public ESI supplied the human-readable issuer identity fields in the embed context.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssuerIdentityProvenance {
    PublicEsi,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEmbedContext {
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub location: ContractLocationContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_range: Option<ContractMatchedRange>,
    pub issuer_character_name: Option<String>,
    pub issuer_corporation_name: Option<String>,
    pub issuer_alliance_id: Option<i64>,
    pub issuer_alliance_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_identity_provenance: Option<IssuerIdentityProvenance>,
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
        self.location.last_verified_at = self
            .location
            .last_verified_at
            .or(source.location.last_verified_at);
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
        self.matched_range = self
            .matched_range
            .clone()
            .or_else(|| source.matched_range.clone());
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
        self.issuer_identity_provenance = self
            .issuer_identity_provenance
            .or(source.issuer_identity_provenance);
        for (id, name) in &source.item_names {
            self.item_names.entry(*id).or_insert_with(|| name.clone());
        }
    }

    fn merge_terminal_stable_facts_from(&mut self, source: &Self) {
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
        self.location.last_verified_at = self
            .location
            .last_verified_at
            .or(source.location.last_verified_at);
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
        for (id, name) in &source.item_names {
            self.item_names.entry(*id).or_insert_with(|| name.clone());
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContractMatchedRange {
    pub light_years: f64,
    pub reference_system_id: u32,
    pub reference_system_name: String,
    pub destination_system_id: u32,
    pub destination_system_name: String,
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

pub const CONTRACT_NOTIFICATION_PRESENTATION_REVISION: u32 = 5;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractNotificationMessage {
    pub title: String,
    pub description: Option<String>,
    pub fields: Vec<ContractEmbedField>,
    #[serde(default)]
    pub presentation_revision: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
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
    pub(crate) delivery_claim_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContractMessageEdit {
    pub delivery_id: i64,
    pub channel_id: u64,
    pub discord_message_id: String,
    pub contract_id: i64,
    pub event_kind: ContractEventKind,
    repair_revision: i64,
    repair_claim_token: Option<String>,
    repair_attempt_count: i32,
    pub message: ContractNotificationMessage,
}

struct DeliveryOrdering {
    strategic_priority: usize,
    relevant_isk: f64,
    confirmed_at: DateTime<Utc>,
}

enum PrepareDeliveryOutcome {
    Prepared,
    Existing,
    EvidenceBecameAvailable,
    ExistingSent {
        delivery_id: i64,
        message: ContractNotificationMessage,
    },
}

enum PrepareProximityResolutionOutcome {
    Resolved,
    EvidenceChanged,
    Unchanged,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

pub const CONTRACT_DELIVERY_OPERATOR_ACTOR: &str = "discord:146451271497416704";
pub const CONTRACT_DELIVERY_OPERATOR_TOKEN_MAX_BYTES: usize = 128;
pub const CONTRACT_DELIVERY_RERENDER_MAX_LIMIT: usize = 100;

pub struct ContractDeliveryOperatorCapability {
    actor: String,
    digest: [u8; 32],
}
impl ContractDeliveryOperatorCapability {
    pub fn from_config(actor: String, digest_hex: &str) -> Result<Self, String> {
        if digest_hex.len() != 64 || !digest_hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("operator digest must be 64 hex characters".to_string());
        }
        let bytes = (0..64)
            .step_by(2)
            .map(|index| {
                u8::from_str_radix(
                    digest_hex
                        .get(index..index + 2)
                        .ok_or("operator digest must be 64 hex characters")?,
                    16,
                )
                .map_err(|_| "operator digest must be 64 hex characters".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let digest: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "operator digest must be 64 hex characters".to_string())?;
        if actor != CONTRACT_DELIVERY_OPERATOR_ACTOR {
            return Err(format!(
                "operator identity must be {CONTRACT_DELIVERY_OPERATOR_ACTOR}"
            ));
        }
        Ok(Self { actor, digest })
    }
    pub fn verify(&self, token: &[u8]) -> bool {
        self.digest.ct_eq(&Sha256::digest(token)).into()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractRepairStatus {
    None,
    Pending,
    Permanent,
}

fn contract_repair_status_from_str(value: &str) -> Result<ContractRepairStatus, sqlx::Error> {
    match value {
        "none" => Ok(ContractRepairStatus::None),
        "pending" => Ok(ContractRepairStatus::Pending),
        "permanent" => Ok(ContractRepairStatus::Permanent),
        _ => Err(sqlx::Error::Protocol(format!(
            "unknown contract repair status: {value}"
        ))),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractDeliveryAudit {
    pub action: String,
    pub actor: String,
    pub occurred_at: DateTime<Utc>,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContractDeliveryInspection {
    pub delivery_id: i64,
    pub status: DeliveryStatus,
    pub discord_message_id: Option<String>,
    pub ping: bool,
    pub ping_type: ContractPingType,
    pub desired_message: Option<ContractNotificationMessage>,
    pub repair_status: ContractRepairStatus,
    pub repair_attempt_count: i32,
    pub last_repair_attempt_at: Option<DateTime<Utc>>,
    pub next_repair_attempt_at: Option<DateTime<Utc>>,
    pub repair_failure_kind: Option<DeliveryFailureKind>,
    pub repair_last_error: Option<String>,
    pub audit: Vec<ContractDeliveryAudit>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractDeliveryRerenderCandidate {
    pub delivery_id: i64,
    pub contract_id: i64,
    pub event_kind: ContractEventKind,
    pub discord_message_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractDeliveryRerenderExclusion {
    pub delivery_id: i64,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContractDeliveryRerenderReport {
    pub channel_id: u64,
    pub dry_run: bool,
    pub eligible_count: usize,
    pub candidates: Vec<ContractDeliveryRerenderCandidate>,
    pub excluded_count: usize,
    pub excluded: Vec<ContractDeliveryRerenderExclusion>,
}

#[derive(Clone, Debug)]
enum ContractDeliveryRerenderSelector {
    Limit(usize),
    DeliveryIds(Vec<i64>),
}

#[derive(Clone, Debug)]
struct ValidatedContractDeliveryRerenderCandidate {
    candidate: ContractDeliveryRerenderCandidate,
    subscription: ContractSubscription,
    event: ContractEvent,
    stored_message: ContractNotificationMessage,
}

#[derive(Clone, Debug, Default)]
struct ContractDeliveryRerenderSelection {
    candidates: Vec<ValidatedContractDeliveryRerenderCandidate>,
    excluded: Vec<ContractDeliveryRerenderExclusion>,
}

enum ContractDeliveryRerenderRow {
    Candidate(ValidatedContractDeliveryRerenderCandidate),
    Excluded(ContractDeliveryRerenderExclusion),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum OperatorDeliveryCliResult {
    Inspected(ContractDeliveryInspection),
    Requeued(ContractDeliveryInspection),
    Rerendered(ContractDeliveryRerenderReport),
}

#[derive(Clone, Debug)]
struct DeferredContractMatch {
    subscription: ContractSubscription,
    event: ContractEvent,
    proximity_unverified: bool,
}

#[derive(Clone, Debug)]
pub struct ContractDeliveryErrorDetail {
    message: String,
    retry_at: Option<DateTime<Utc>>,
    delivery_scoped: bool,
}

#[derive(Clone, Debug)]
pub enum ContractDeliveryError {
    Transient(ContractDeliveryErrorDetail),
    Ambiguous(ContractDeliveryErrorDetail),
    Permanent(ContractDeliveryErrorDetail),
}

impl ContractDeliveryError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self::Transient(ContractDeliveryErrorDetail {
            message: message.into(),
            retry_at: None,
            delivery_scoped: false,
        })
    }

    pub fn transient_after(message: impl Into<String>, retry_at: DateTime<Utc>) -> Self {
        Self::Transient(ContractDeliveryErrorDetail {
            message: message.into(),
            retry_at: Some(retry_at),
            delivery_scoped: false,
        })
    }

    pub fn ambiguous(message: impl Into<String>) -> Self {
        Self::Ambiguous(ContractDeliveryErrorDetail {
            message: message.into(),
            retry_at: None,
            delivery_scoped: false,
        })
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent(ContractDeliveryErrorDetail {
            message: message.into(),
            retry_at: None,
            delivery_scoped: false,
        })
    }

    pub fn permanent_delivery(message: impl Into<String>) -> Self {
        Self::Permanent(ContractDeliveryErrorDetail {
            message: message.into(),
            retry_at: None,
            delivery_scoped: true,
        })
    }

    fn message(&self) -> &str {
        match self {
            Self::Transient(detail) | Self::Ambiguous(detail) | Self::Permanent(detail) => {
                &detail.message
            }
        }
    }

    fn retry_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Transient(detail) | Self::Ambiguous(detail) => detail.retry_at,
            Self::Permanent(_) => None,
        }
    }

    fn is_delivery_scoped_permanent(&self) -> bool {
        matches!(self, Self::Permanent(detail) if detail.delivery_scoped)
    }
}

impl Display for ContractDeliveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message())
    }
}

impl std::error::Error for ContractDeliveryError {}

pub(crate) fn parse_contract_discord_message_id(value: &str) -> Result<u64, String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid Discord message ID: expected a nonzero decimal snowflake".to_string());
    }
    let message_id = value.parse::<u64>().map_err(|_| {
        "invalid Discord message ID: expected a nonzero decimal snowflake".to_string()
    })?;
    if message_id == 0 {
        return Err("invalid Discord message ID: expected a nonzero decimal snowflake".to_string());
    }
    Ok(message_id)
}

#[async_trait]
pub trait ContractDelivery: Send + Sync {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError>;

    async fn edit(&self, _edit: ContractMessageEdit) -> Result<(), ContractDeliveryError> {
        Err(ContractDeliveryError::permanent(
            "contract message editing is unsupported",
        ))
    }
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
pub trait ContractRequestPacer: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    async fn wait_until(&self, deadline: DateTime<Utc>);
}

struct SystemContractRequestPacer;

#[async_trait]
impl ContractRequestPacer for SystemContractRequestPacer {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn wait_until(&self, deadline: DateTime<Utc>) {
        let delay = (deadline - Utc::now()).to_std().unwrap_or_default();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
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

struct CachedShipGroupResolver {
    groups: HashMap<u32, u32>,
}

impl CachedShipGroupResolver {
    fn new(groups: HashMap<u32, u32>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl ShipGroupResolver for CachedShipGroupResolver {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup {
        let group = u32::try_from(type_id)
            .ok()
            .and_then(|type_id| self.groups.get(&type_id).copied())
            .map(i64::from);
        ShipGroupLookup::Resolved(group)
    }
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

fn non_health_migrator(migrator: &Migrator, health_migration_versions: &[i64]) -> Migrator {
    // The complete source runs first so SQLx can validate every applied version and checksum.
    // This filtered rerun is only for applying later core migrations after the one explicitly
    // allow-listed, transactional health migration failed.
    Migrator {
        migrations: Cow::Owned(
            migrator
                .iter()
                .filter(|migration| !health_migration_versions.contains(&migration.version))
                .cloned()
                .collect(),
        ),
        // The complete migrator already validated the full applied ledger before it
        // failed at an allow-listed health migration. Earlier applied health versions
        // are deliberately absent from this continuation source.
        ignore_missing: true,
        locking: migrator.locking,
        no_tx: migrator.no_tx,
    }
}

async fn run_migrator_releasing_lock_on_error(
    pool: &PgPool,
    migrator: &Migrator,
) -> Result<(), MigrateError> {
    let mut connection = pool.acquire().await?;
    let result = migrator.run_direct(&mut *connection).await;
    if result.is_err() {
        if let Err(unlock_error) = (*connection).unlock().await {
            connection.close_on_drop();
            return Err(unlock_error);
        }
    }
    result
}

impl ContractCollectionStore {
    pub(crate) fn location_evidence_pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn reserve_structure_resolution(
        &self,
        structure_id: i64,
        credential_revision: &str,
        now: DateTime<Utc>,
    ) -> Result<StructureResolutionAdmission, sqlx::Error> {
        if structure_id <= 0 || credential_revision.trim().is_empty() {
            return Err(sqlx::Error::Protocol(
                "structure resolver requires a positive identifier and credential revision"
                    .to_string(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let active_revision = sqlx::query_scalar::<_, String>(
            "SELECT credential_revision FROM structure_resolver_runtime WHERE singleton = TRUE AND enabled = TRUE FOR SHARE",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if active_revision.as_deref() != Some(credential_revision) {
            transaction.commit().await?;
            return Ok(StructureResolutionAdmission::Inactive);
        }
        if let Some(global_deadline) = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT next_attempt_at FROM structure_resolver_backoff WHERE singleton = TRUE AND credential_revision = $1")
            .bind(credential_revision)
            .fetch_optional(&mut *transaction)
            .await?
            .filter(|deadline| *deadline > now)
        {
            transaction.commit().await?;
            return Ok(StructureResolutionAdmission::WaitUntil(global_deadline));
        }
        sqlx::query("INSERT INTO structure_resolution_state (structure_id, credential_revision, next_attempt_at, updated_at) VALUES ($1, $2, $3, $3) ON CONFLICT (structure_id) DO UPDATE SET credential_revision = EXCLUDED.credential_revision, etag = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.etag END, structure_name = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.structure_name END, solar_system_id = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.solar_system_id END, observed_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.observed_at END, cache_expires_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.cache_expires_at END, next_attempt_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN EXCLUDED.next_attempt_at ELSE structure_resolution_state.next_attempt_at END, lease_expires_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.lease_expires_at END, first_denied_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.first_denied_at END, last_denied_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.last_denied_at END, denied_attempts = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN 0 ELSE structure_resolution_state.denied_attempts END, parked_at = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.parked_at END, transient_failures = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN 0 ELSE structure_resolution_state.transient_failures END, last_error = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.last_error END, last_failure_kind = CASE WHEN structure_resolution_state.credential_revision <> EXCLUDED.credential_revision THEN NULL ELSE structure_resolution_state.last_failure_kind END, updated_at = EXCLUDED.updated_at")
            .bind(structure_id)
            .bind(credential_revision)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        let state = sqlx::query("SELECT etag, structure_name, solar_system_id, observed_at, cache_expires_at, next_attempt_at, first_denied_at, parked_at, lease_expires_at FROM structure_resolution_state WHERE structure_id = $1 FOR UPDATE")
            .bind(structure_id)
            .fetch_one(&mut *transaction)
            .await?;
        let parked_at: Option<DateTime<Utc>> = state.get("parked_at");
        let first_denied_at: Option<DateTime<Utc>> = state.get("first_denied_at");
        if parked_at.is_some() {
            transaction.commit().await?;
            return Ok(StructureResolutionAdmission::Parked);
        }
        if first_denied_at.is_some_and(|denied_at| now >= denied_at + ChronoDuration::days(7)) {
            sqlx::query("UPDATE structure_resolution_state SET parked_at = $2, next_attempt_at = NULL, updated_at = $2 WHERE structure_id = $1")
                .bind(structure_id)
                .bind(now)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
            return Ok(StructureResolutionAdmission::Parked);
        }
        let cache_expires_at: Option<DateTime<Utc>> = state.get("cache_expires_at");
        let cached = match (
            state.get::<Option<String>, _>("etag"),
            state.get::<Option<i64>, _>("solar_system_id"),
            state.get::<Option<DateTime<Utc>>, _>("observed_at"),
        ) {
            (etag, Some(solar_system_id), Some(observed_at)) if solar_system_id > 0 => {
                Some(ResolvedStructure {
                    structure_id,
                    name: state.get("structure_name"),
                    solar_system_id,
                    observed_at,
                    expires_at: cache_expires_at,
                    etag,
                    response_metadata: CacheMetadata::cached_for_seconds(0),
                    representation_cacheable: true,
                })
            }
            _ => None,
        };
        let next_attempt_at: Option<DateTime<Utc>> = state.get("next_attempt_at");
        let lease_expires_at: Option<DateTime<Utc>> = state.get("lease_expires_at");
        let deadline = [cache_expires_at, next_attempt_at, lease_expires_at]
            .into_iter()
            .flatten()
            .filter(|deadline| *deadline > now)
            .max();
        if let Some(deadline) = deadline {
            transaction.commit().await?;
            return Ok(StructureResolutionAdmission::WaitUntil(deadline));
        }
        let generation = sqlx::query_scalar::<_, i64>("UPDATE structure_resolution_state SET attempt_generation = attempt_generation + 1, lease_expires_at = $2, updated_at = $1 WHERE structure_id = $3 AND credential_revision = $4 AND (lease_expires_at IS NULL OR lease_expires_at <= $1) RETURNING attempt_generation")
            .bind(now)
            .bind(now + ChronoDuration::minutes(5))
            .bind(structure_id)
            .bind(credential_revision)
            .fetch_optional(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(generation.map_or(
            StructureResolutionAdmission::WaitUntil(now + ChronoDuration::seconds(1)),
            |generation| StructureResolutionAdmission::Attempt {
                generation,
                cached: cached.map(Box::new),
            },
        ))
    }

    pub async fn record_structure_resolution_denial(
        &self,
        structure_id: i64,
        credential_revision: &str,
        generation: i64,
        now: DateTime<Utc>,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<bool, sqlx::Error> {
        let daily_check = now + ChronoDuration::days(1);
        let next_attempt_at =
            retry_after.map_or(daily_check, |retry_after| retry_after.max(daily_check));
        let mut transaction = self.pool.begin().await?;
        if !Self::structure_resolver_runtime_is_active(&mut transaction, credential_revision)
            .await?
        {
            transaction.commit().await?;
            return Ok(false);
        }
        let completion = sqlx::query("UPDATE structure_resolution_state SET next_attempt_at = $1, first_denied_at = COALESCE(first_denied_at, $2), last_denied_at = $2, denied_attempts = denied_attempts + 1, transient_failures = 0, last_error = 'structure resolver access denied', last_failure_kind = 'access_denied', lease_expires_at = NULL, updated_at = $2 WHERE structure_id = $3 AND credential_revision = $4 AND attempt_generation = $5")
            .bind(next_attempt_at)
            .bind(now)
            .bind(structure_id)
            .bind(credential_revision)
            .bind(generation)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(completion.rows_affected() == 1)
    }

    pub async fn record_structure_resolver_backoff(
        &self,
        credential_revision: &str,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let active_revision = sqlx::query_scalar::<_, String>(
            "SELECT credential_revision FROM structure_resolver_runtime WHERE singleton = TRUE AND enabled = TRUE FOR UPDATE",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if active_revision.as_deref() != Some(credential_revision) {
            transaction.commit().await?;
            return Ok(false);
        }
        let update = sqlx::query("INSERT INTO structure_resolver_backoff (singleton, credential_revision, next_attempt_at, updated_at) VALUES (TRUE, $1, $2, $3) ON CONFLICT (singleton) DO UPDATE SET credential_revision = EXCLUDED.credential_revision, next_attempt_at = CASE WHEN structure_resolver_backoff.credential_revision = EXCLUDED.credential_revision THEN GREATEST(structure_resolver_backoff.next_attempt_at, EXCLUDED.next_attempt_at) ELSE EXCLUDED.next_attempt_at END, updated_at = EXCLUDED.updated_at")
            .bind(credential_revision)
            .bind(deadline)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(update.rows_affected() == 1)
    }

    pub async fn record_structure_resolution_success(
        &self,
        resolved: &ResolvedStructure,
        credential_revision: &str,
        generation: i64,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let committed = Self::complete_structure_resolution_success(
            &mut transaction,
            resolved,
            credential_revision,
            generation,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(committed)
    }

    pub async fn discard_structure_resolution_representation(
        &self,
        structure_id: i64,
        credential_revision: &str,
        generation: i64,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        if !Self::structure_resolver_runtime_is_active(&mut transaction, credential_revision)
            .await?
        {
            transaction.commit().await?;
            return Ok(false);
        }
        let update = sqlx::query("UPDATE structure_resolution_state SET etag = NULL, structure_name = NULL, solar_system_id = NULL, observed_at = NULL, cache_expires_at = NULL WHERE structure_id = $1 AND credential_revision = $2 AND attempt_generation = $3")
            .bind(structure_id)
            .bind(credential_revision)
            .bind(generation)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(update.rows_affected() == 1)
    }

    pub async fn record_structure_resolution_success_with_evidence(
        &self,
        resolved: &ResolvedStructure,
        region_id: Option<i64>,
        credential_revision: &str,
        generation: i64,
        resolver_identity: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<LocationEvidence>, sqlx::Error> {
        let Some(expires_at) = resolved.expires_at.filter(|expires_at| *expires_at > now) else {
            return Ok(None);
        };
        let mut transaction = self.pool.begin().await?;
        lock_location(&mut transaction, resolved.structure_id).await?;
        if !Self::complete_structure_resolution_success(
            &mut transaction,
            resolved,
            credential_revision,
            generation,
            now,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(None);
        }
        let evidence = LocationEvidenceService::record_access_qualified_in_transaction(
            &mut transaction,
            resolved.structure_id,
            resolved.solar_system_id,
            region_id,
            resolver_identity,
            resolved.observed_at,
            expires_at,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(Some(evidence))
    }

    async fn complete_structure_resolution_success(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        resolved: &ResolvedStructure,
        credential_revision: &str,
        generation: i64,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        if !Self::structure_resolver_runtime_is_active(transaction, credential_revision).await? {
            return Ok(false);
        }
        let cache_expires_at = resolved.expires_at.unwrap_or(now);
        let completion = sqlx::query("UPDATE structure_resolution_state SET etag = $1, structure_name = $2, solar_system_id = $3, observed_at = $4, cache_expires_at = $5, next_attempt_at = $5, first_denied_at = NULL, last_denied_at = NULL, denied_attempts = 0, parked_at = NULL, transient_failures = 0, last_error = NULL, last_failure_kind = NULL, last_success_at = $6, lease_expires_at = NULL, updated_at = $6 WHERE structure_id = $7 AND credential_revision = $8 AND attempt_generation = $9")
            .bind(&resolved.etag)
            .bind(&resolved.name)
            .bind(resolved.solar_system_id)
            .bind(resolved.observed_at)
            .bind(cache_expires_at)
            .bind(now)
            .bind(resolved.structure_id)
            .bind(credential_revision)
            .bind(generation)
            .execute(&mut **transaction)
            .await?;
        Ok(completion.rows_affected() == 1)
    }

    pub async fn record_structure_resolution_transient_failure(
        &self,
        structure_id: i64,
        credential_revision: &str,
        generation: i64,
        now: DateTime<Utc>,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        if !Self::structure_resolver_runtime_is_active(&mut transaction, credential_revision)
            .await?
        {
            transaction.commit().await?;
            return Ok(false);
        }
        let Some(current_failures) = sqlx::query_scalar::<_, i32>("SELECT transient_failures FROM structure_resolution_state WHERE structure_id = $1 AND credential_revision = $2 AND attempt_generation = $3 FOR UPDATE")
            .bind(structure_id)
            .bind(credential_revision)
            .bind(generation)
            .fetch_optional(&mut *transaction)
            .await?
        else {
            transaction.commit().await?;
            return Ok(false);
        };
        let current_failures = current_failures.clamp(0, 6);
        let backoff = ChronoDuration::seconds(30_i64.saturating_mul(1_i64 << current_failures));
        let next_attempt_at =
            retry_after.map_or(now + backoff, |retry_after| retry_after.max(now + backoff));
        let completion = sqlx::query("UPDATE structure_resolution_state SET next_attempt_at = $1, transient_failures = LEAST(transient_failures + 1, 7), last_error = 'structure resolver temporarily unavailable', last_failure_kind = 'transient', lease_expires_at = NULL, updated_at = $2 WHERE structure_id = $3 AND credential_revision = $4 AND attempt_generation = $5")
            .bind(next_attempt_at)
            .bind(now)
            .bind(structure_id)
            .bind(credential_revision)
            .bind(generation)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(completion.rows_affected() == 1)
    }

    async fn structure_resolver_runtime_is_active(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        credential_revision: &str,
    ) -> Result<bool, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT credential_revision FROM structure_resolver_runtime WHERE singleton = TRUE AND enabled = TRUE FOR SHARE",
        )
        .fetch_optional(&mut **transaction)
        .await?
        .as_deref()
            == Some(credential_revision))
    }

    pub async fn record_structure_resolver_runtime(
        &self,
        enabled: bool,
        credential_revision: &str,
        status: &str,
        last_error: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let active_revision = sqlx::query_scalar::<_, String>(
            "SELECT credential_revision FROM structure_resolver_runtime WHERE singleton = TRUE FOR UPDATE",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if active_revision.as_deref() != Some(credential_revision) {
            transaction.commit().await?;
            return Ok(false);
        }
        let aggregate_degraded = enabled
            && status == "ready"
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM structure_resolution_state WHERE credential_revision = $1 AND (parked_at IS NOT NULL OR last_failure_kind IN ('access_denied', 'degraded')))",
            )
            .bind(credential_revision)
            .fetch_one(&mut *transaction)
            .await?;
        let (status, last_error) = if aggregate_degraded {
            (
                "degraded",
                Some("structure resolver has denied or parked locations"),
            )
        } else {
            (status, last_error)
        };
        let update = sqlx::query("INSERT INTO structure_resolver_runtime (singleton, enabled, credential_revision, status, last_error, updated_at) VALUES (TRUE, $1, $2, $3, $4, $5) ON CONFLICT (singleton) DO UPDATE SET enabled = EXCLUDED.enabled, status = EXCLUDED.status, last_error = EXCLUDED.last_error, updated_at = EXCLUDED.updated_at WHERE structure_resolver_runtime.credential_revision = EXCLUDED.credential_revision")
            .bind(enabled)
            .bind(credential_revision)
            .bind(status)
            .bind(last_error)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(update.rows_affected() == 1)
    }

    pub async fn initialize_structure_resolver_runtime(
        &self,
        runtime: &StructureResolverRuntimeStatus,
        now: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let current = sqlx::query(
            "SELECT enabled, credential_revision FROM structure_resolver_runtime WHERE singleton = TRUE FOR UPDATE",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        let changed = current.as_ref().is_none_or(|current| {
            current.get::<bool, _>("enabled") != runtime.enabled()
                || current.get::<String, _>("credential_revision") != runtime.credential_revision()
        });
        if current.is_none() {
            sqlx::query("INSERT INTO structure_resolver_runtime (singleton, enabled, credential_revision, status, last_error, updated_at) VALUES (TRUE, $1, $2, $3, $4, $5)")
                .bind(runtime.enabled())
                .bind(runtime.credential_revision())
                .bind(runtime.status())
                .bind(runtime.last_error())
                .bind(now)
                .execute(&mut *transaction)
                .await?;
        } else if changed {
            sqlx::query("UPDATE structure_resolver_runtime SET enabled = $1, credential_revision = $2, status = $3, last_error = $4, updated_at = $5 WHERE singleton = TRUE")
                .bind(runtime.enabled())
                .bind(runtime.credential_revision())
                .bind(runtime.status())
                .bind(runtime.last_error())
                .bind(now)
                .execute(&mut *transaction)
                .await?;
        }
        sqlx::query(
            "DELETE FROM structure_resolver_backoff WHERE singleton = TRUE AND credential_revision <> $1",
        )
        .bind(runtime.credential_revision())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await
    }

    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::connect_with_migrator(database_url, &MIGRATOR, HEALTH_MIGRATION_VERSIONS).await
    }

    #[doc(hidden)]
    pub async fn connect_with_migrator_for_test(
        database_url: &str,
        migrator: &Migrator,
        health_migration_versions: &[i64],
    ) -> Result<Self, sqlx::Error> {
        Self::connect_with_migrator(database_url, migrator, health_migration_versions).await
    }

    async fn connect_with_migrator(
        database_url: &str,
        migrator: &Migrator,
        health_migration_versions: &[i64],
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        match run_migrator_releasing_lock_on_error(&pool, migrator).await {
            Ok(()) => {}
            Err(MigrateError::ExecuteMigration(_, version))
                if health_migration_versions.contains(&version) =>
            {
                warn!(
                    health_migration_version = version,
                    "health schema migration failed; applying non-health migrations before continuing contract collection"
                );
                let filtered_non_health_migrator =
                    non_health_migrator(migrator, health_migration_versions);
                run_migrator_releasing_lock_on_error(&pool, &filtered_non_health_migrator)
                    .await
                    .map_err(sqlx::Error::from)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(Self { pool })
    }

    async fn initialize_health_schema(&self) -> Result<(), sqlx::Error> {
        run_migrator_releasing_lock_on_error(&self.pool, &MIGRATOR)
            .await
            .map_err(Into::into)
    }

    async fn health_inputs(&self, observed_at: DateTime<Utc>) -> Result<HealthInputs, sqlx::Error> {
        let heartbeat = sqlx::query(
            "SELECT observed_at, started_at FROM bot_heartbeats WHERE component = 'bot'",
        )
        .fetch_optional(&self.pool)
        .await?;
        let heartbeat_at = heartbeat.as_ref().map(|row| row.get("observed_at"));
        let started_at = heartbeat.as_ref().map(|row| row.get("started_at"));
        let regional_progress = sqlx::query(
            "SELECT region_id, last_complete_at FROM regional_collection_metadata ORDER BY region_id",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| (row.get("region_id"), row.get("last_complete_at")))
        .collect();
        let esi_progress_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT max(updated_at) FROM esi_cache_metadata WHERE resource_key LIKE 'esi:%'",
        )
        .fetch_one(&self.pool)
        .await?;
        let esi_pause_until = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT pause_until FROM esi_collection_limiter_state WHERE limiter_scope = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        let due_backlog = sqlx::query(
            "SELECT count(*)::BIGINT AS due_count, min(due_at) AS oldest_due_at FROM (SELECT last_observed_at AS due_at FROM contract_manifest_pending UNION ALL SELECT next_probe_at AS due_at FROM contract_resolution_cases WHERE state = 'awaiting_resolution' AND next_probe_at <= $1) AS due_backlog",
        )
        .bind(observed_at)
        .fetch_one(&self.pool)
        .await?;
        let due_backlog_count = due_backlog.get("due_count");
        let oldest_due_backlog_at = due_backlog.get("oldest_due_at");
        let prior_backlog_metric = sqlx::query(
            "SELECT due_count, peak_due_count, growth_detected FROM health_backlog_metrics WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| PersistedBacklogMetric {
            due_count: row.get("due_count"),
            peak_due_count: row.get("peak_due_count"),
            growth_detected: row.get("growth_detected"),
        });
        let oldest_prepared_delivery_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT min(prepared_at) FROM contract_outbound_deliveries WHERE status = 'prepared'",
        )
        .fetch_one(&self.pool)
        .await?;
        let permanent_delivery_failures = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM contract_outbound_deliveries WHERE (status = 'failed' AND failure_kind = 'permanent' AND failure_resolved_at IS NULL) OR (repair_status = 'permanent' AND repair_failure_kind = 'permanent' AND repair_failure_resolved_at IS NULL)",
        )
        .fetch_one(&self.pool)
        .await?;
        let structure_resolver = sqlx::query(
            "SELECT enabled, status, last_error, updated_at FROM structure_resolver_runtime WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            (
                row.get("enabled"),
                row.get("status"),
                row.get("last_error"),
                row.get("updated_at"),
            )
        });
        let r2z2_telemetry = sqlx::query(
            "SELECT last_progress_at FROM health_feed_telemetry WHERE source = 'r2z2_progress'",
        )
        .fetch_optional(&self.pool)
        .await?;
        let feed_validation_telemetry = sqlx::query(
            "SELECT last_progress_at, consecutive_failures FROM health_feed_telemetry WHERE source = 'feed_validation'",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(HealthInputs {
            heartbeat_at,
            started_at,
            regional_progress,
            esi_progress_at,
            esi_pause_until,
            due_backlog_count,
            oldest_due_backlog_at,
            prior_backlog_metric,
            oldest_prepared_delivery_at,
            permanent_delivery_failures,
            r2z2_progress_at: r2z2_telemetry
                .as_ref()
                .and_then(|row| row.get::<Option<DateTime<Utc>>, _>("last_progress_at")),
            feed_validation_at: feed_validation_telemetry
                .as_ref()
                .and_then(|row| row.get::<Option<DateTime<Utc>>, _>("last_progress_at")),
            feed_validation_failures: feed_validation_telemetry
                .as_ref()
                .map(|row| row.get("consecutive_failures"))
                .unwrap_or(0),
            structure_resolver,
        })
    }

    pub async fn begin_health_monitor_session(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        self.initialize_health_schema().await?;
        sqlx::query(
            "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1) ON CONFLICT (component) DO UPDATE SET observed_at = GREATEST(bot_heartbeats.observed_at, EXCLUDED.observed_at)",
        )
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn save_health_snapshot(
        &self,
        snapshot: &HealthSnapshot,
        backlog_metric: &BacklogMetric,
        feed_snapshot: Option<&FeedHealthSnapshot>,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1::BIGINT)")
            .bind(HEALTH_SNAPSHOT_TRANSITION_LOCK_KEY)
            .execute(&mut *transaction)
            .await?;
        let newest_snapshot_at = sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT observed_at FROM health_snapshots WHERE singleton = TRUE",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if newest_snapshot_at.is_some_and(|newest_at| newest_at >= snapshot.observed_at) {
            return transaction.commit().await;
        }
        sqlx::query(
            "INSERT INTO bot_heartbeats (component, observed_at, started_at) VALUES ('bot', $1, $1) ON CONFLICT (component) DO UPDATE SET observed_at = GREATEST(bot_heartbeats.observed_at, EXCLUDED.observed_at)",
        )
        .bind(snapshot.observed_at)
        .execute(&mut *transaction)
        .await?;
        for check in &snapshot.checks {
            let prior_status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM health_check_results WHERE check_key = $1",
            )
            .bind(&check.key)
            .fetch_optional(&mut *transaction)
            .await?;
            if prior_status.as_deref() != Some(check.status.as_str()) {
                let transition_identity = format!(
                    "{}:{}:{}",
                    check.key,
                    check.status.as_str(),
                    check
                        .observed_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
                );
                sqlx::query(
                    "INSERT INTO health_transitions (transition_identity, check_key, prior_status, status, observed_at, evidence) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (transition_identity) DO NOTHING",
                )
                .bind(transition_identity)
                .bind(&check.key)
                .bind(prior_status)
                .bind(check.status.as_str())
                .bind(check.observed_at)
                .bind(&check.evidence)
                .execute(&mut *transaction)
                .await?;
            }
            sqlx::query(
                "INSERT INTO health_check_results (check_key, status, observed_at, evidence, consecutive_failures) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (check_key) DO UPDATE SET status = EXCLUDED.status, observed_at = EXCLUDED.observed_at, evidence = EXCLUDED.evidence, consecutive_failures = EXCLUDED.consecutive_failures",
            )
            .bind(&check.key)
            .bind(check.status.as_str())
            .bind(check.observed_at)
            .bind(&check.evidence)
            .bind(check.consecutive_failures)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "INSERT INTO health_snapshots (singleton, status, observed_at, evidence) VALUES (TRUE,$1,$2,$3) ON CONFLICT (singleton) DO UPDATE SET status = EXCLUDED.status, observed_at = EXCLUDED.observed_at, evidence = EXCLUDED.evidence",
        )
        .bind(snapshot.status.as_str())
        .bind(snapshot.observed_at)
        .bind(serde_json::to_value(&snapshot.checks).map_err(json_to_sqlx)?)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO health_backlog_metrics (singleton, due_count, peak_due_count, growth_detected, oldest_due_at, observed_at) VALUES (TRUE,$1,$2,$3,$4,$5) ON CONFLICT (singleton) DO UPDATE SET due_count = EXCLUDED.due_count, peak_due_count = EXCLUDED.peak_due_count, growth_detected = EXCLUDED.growth_detected, oldest_due_at = EXCLUDED.oldest_due_at, observed_at = EXCLUDED.observed_at",
        )
        .bind(backlog_metric.due_count)
        .bind(backlog_metric.peak_due_count)
        .bind(backlog_metric.growth_detected)
        .bind(backlog_metric.oldest_due_at)
        .bind(snapshot.observed_at)
        .execute(&mut *transaction)
        .await?;
        if let Some(feed_snapshot) = feed_snapshot {
            sqlx::query(
                "INSERT INTO health_feed_telemetry (source, last_progress_at, consecutive_failures, updated_at) VALUES ('feed_validation',$1,$2,$3) ON CONFLICT (source) DO UPDATE SET last_progress_at = EXCLUDED.last_progress_at, consecutive_failures = EXCLUDED.consecutive_failures, updated_at = EXCLUDED.updated_at",
            )
            .bind(feed_snapshot.validation_success_at)
            .bind(feed_snapshot.validation_failures)
            .bind(snapshot.observed_at)
            .execute(&mut *transaction)
            .await?;
            if let Some(r2z2_progress_at) = feed_snapshot.r2z2_progress_at {
                sqlx::query(
                    "INSERT INTO health_feed_telemetry (source, last_progress_at, consecutive_failures, updated_at) VALUES ('r2z2_progress',$1,0,$2) ON CONFLICT (source) DO UPDATE SET last_progress_at = EXCLUDED.last_progress_at, consecutive_failures = 0, updated_at = EXCLUDED.updated_at",
                )
                .bind(r2z2_progress_at)
                .bind(snapshot.observed_at)
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await
    }

    pub async fn health_snapshot(&self) -> Result<Option<HealthSnapshot>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT status, observed_at, evidence FROM health_snapshots WHERE singleton = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(HealthSnapshot {
                status: HealthStatus::from_str(&row.get::<String, _>("status"))?,
                observed_at: row.get("observed_at"),
                checks: serde_json::from_value(row.get("evidence")).map_err(json_to_sqlx)?,
            })
        })
        .transpose()
    }

    pub async fn health_snapshot_with_watchdog_publication_failures(
        &self,
    ) -> Result<Option<HealthSnapshot>, sqlx::Error> {
        let base_snapshot = self.health_snapshot().await?;
        let publication_failures = sqlx::query(
            "SELECT 'discord_health_view' AS check_key, last_error, failure_kind, updated_at FROM health_discord_views WHERE view_key = 'primary' AND failure_kind IS NOT NULL UNION ALL SELECT 'discord_health_incident' AS check_key, last_error, failure_kind, updated_at FROM health_discord_incidents WHERE incident_key = 'current' AND failure_kind IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;
        let Some(mut snapshot) = base_snapshot.or_else(|| {
            publication_failures
                .iter()
                .map(|failure| failure.get::<DateTime<Utc>, _>("updated_at"))
                .max()
                .map(|observed_at| HealthSnapshot {
                    status: HealthStatus::Healthy,
                    observed_at,
                    checks: Vec::new(),
                })
        }) else {
            return Ok(None);
        };
        for failure in publication_failures {
            let key: String = failure.get("check_key");
            snapshot.checks.retain(|check| check.key != key);
            let failure_kind: String = failure.get("failure_kind");
            let detail: Option<String> = failure.get("last_error");
            let status = if failure_kind == "permanent" {
                HealthStatus::Critical
            } else {
                HealthStatus::Degraded
            };
            snapshot.checks.push(HealthCheck {
                key,
                status,
                observed_at: failure.get("updated_at"),
                evidence: format!(
                    "watchdog Discord publication {failure_kind}: {}",
                    detail.unwrap_or_else(|| "unknown error".to_string())
                ),
                consecutive_failures: 1,
            });
        }
        snapshot
            .checks
            .sort_by(|left, right| left.key.cmp(&right.key));
        snapshot.status = snapshot
            .checks
            .iter()
            .map(|check| check.status)
            .max()
            .unwrap_or(HealthStatus::Healthy);
        Ok(Some(snapshot))
    }

    pub async fn health_discord_view_identity(
        &self,
    ) -> Result<Option<HealthDiscordViewIdentity>, sqlx::Error> {
        sqlx::query(
            "SELECT channel_id, message_id FROM health_discord_views WHERE view_key = 'primary'",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            let channel_id: i64 = row.get("channel_id");
            let channel_id = u64::try_from(channel_id).map_err(|_| {
                sqlx::Error::Protocol("health view channel ID cannot be negative".to_string())
            })?;
            Ok(HealthDiscordViewIdentity {
                channel_id,
                message_id: row.get("message_id"),
            })
        })
        .transpose()
    }

    async fn health_discord_view(&self) -> Result<Option<HealthDiscordViewState>, sqlx::Error> {
        sqlx::query(
            "SELECT channel_id, config_revision, message_id, last_error, failure_kind, next_attempt_at FROM health_discord_views WHERE view_key = 'primary'",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            let channel_id: i64 = row.get("channel_id");
            let channel_id = u64::try_from(channel_id).map_err(|_| {
                sqlx::Error::Protocol("health view channel ID cannot be negative".to_string())
            })?;
            Ok(HealthDiscordViewState {
                channel_id,
                config_revision: row.get("config_revision"),
                message_id: row.get("message_id"),
                last_error: row.get("last_error"),
                failure_kind: row.get("failure_kind"),
                next_attempt_at: row.get("next_attempt_at"),
            })
        })
        .transpose()
    }

    async fn bot_heartbeat_at(&self) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar("SELECT observed_at FROM bot_heartbeats WHERE component = 'bot'")
            .fetch_optional(&self.pool)
            .await
    }

    async fn health_watchdog_started_at(
        &self,
        candidate_started_at: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, sqlx::Error> {
        sqlx::query_scalar(
            "INSERT INTO health_watchdog_runtime (singleton, started_at) VALUES (TRUE, $1) ON CONFLICT (singleton) DO UPDATE SET started_at = health_watchdog_runtime.started_at RETURNING started_at",
        )
        .bind(candidate_started_at)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn save_health_discord_view_identity(
        &self,
        channel_id: u64,
        config_revision: &str,
        message_id: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let channel_id = i64::try_from(channel_id).map_err(|_| {
            sqlx::Error::Protocol("health view channel ID exceeds PostgreSQL BIGINT".to_string())
        })?;
        sqlx::query(
            "INSERT INTO health_discord_views (view_key, channel_id, config_revision, message_id, updated_at) VALUES ('primary',$1,$2,$3,$4) ON CONFLICT (view_key) DO UPDATE SET channel_id = EXCLUDED.channel_id, config_revision = EXCLUDED.config_revision, message_id = EXCLUDED.message_id, last_error = NULL, failure_kind = NULL, next_attempt_at = NULL, attempt_count = 0, updated_at = EXCLUDED.updated_at",
        )
        .bind(channel_id)
        .bind(config_revision)
        .bind(message_id)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_health_discord_view_failure(
        &self,
        channel_id: u64,
        config_revision: &str,
        error: &HealthPublishError,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let channel_id = i64::try_from(channel_id).map_err(|_| {
            sqlx::Error::Protocol("health view channel ID exceeds PostgreSQL BIGINT".to_string())
        })?;
        sqlx::query(
            "INSERT INTO health_discord_views (view_key, channel_id, config_revision, message_id, last_error, failure_kind, next_attempt_at, attempt_count, updated_at) VALUES ('primary',$1,$2,NULL,$3,$4,$5,1,$6) ON CONFLICT (view_key) DO UPDATE SET channel_id = EXCLUDED.channel_id, config_revision = EXCLUDED.config_revision, message_id = CASE WHEN $7 THEN NULL ELSE health_discord_views.message_id END, last_error = EXCLUDED.last_error, failure_kind = EXCLUDED.failure_kind, next_attempt_at = EXCLUDED.next_attempt_at, attempt_count = health_discord_views.attempt_count + 1, updated_at = EXCLUDED.updated_at",
        )
        .bind(channel_id)
        .bind(config_revision)
        .bind(error.detail())
        .bind(error.failure_kind())
        .bind(error.next_attempt_at(observed_at))
        .bind(observed_at)
        .bind(error.clears_message_identity())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn adopt_health_discord_view_state(
        &self,
        state: &HealthDiscordViewState,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let channel_id = i64::try_from(state.channel_id).map_err(|_| {
            sqlx::Error::Protocol("health view channel ID exceeds PostgreSQL BIGINT".to_string())
        })?;
        sqlx::query(
            "INSERT INTO health_discord_views (view_key, channel_id, config_revision, message_id, last_error, failure_kind, next_attempt_at, attempt_count, updated_at) VALUES ('primary',$1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (view_key) DO UPDATE SET channel_id = EXCLUDED.channel_id, config_revision = EXCLUDED.config_revision, message_id = EXCLUDED.message_id, last_error = EXCLUDED.last_error, failure_kind = EXCLUDED.failure_kind, next_attempt_at = EXCLUDED.next_attempt_at, attempt_count = EXCLUDED.attempt_count, updated_at = EXCLUDED.updated_at",
        )
        .bind(channel_id)
        .bind(&state.config_revision)
        .bind(&state.message_id)
        .bind(&state.last_error)
        .bind(&state.failure_kind)
        .bind(state.next_attempt_at)
        .bind(i32::from(state.failure_kind.is_some()))
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn health_discord_incident(
        &self,
    ) -> Result<Option<HealthDiscordIncidentState>, sqlx::Error> {
        sqlx::query(
            "SELECT channel_id, config_revision, message_id, active, last_error, failure_kind, next_attempt_at FROM health_discord_incidents WHERE incident_key = 'current'",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            let channel_id: i64 = row.get("channel_id");
            let channel_id = u64::try_from(channel_id).map_err(|_| {
                sqlx::Error::Protocol("health incident channel ID cannot be negative".to_string())
            })?;
            Ok(HealthDiscordIncidentState {
                channel_id,
                config_revision: row.get("config_revision"),
                message_id: row.get("message_id"),
                active: row.get("active"),
                last_error: row.get("last_error"),
                failure_kind: row.get("failure_kind"),
                next_attempt_at: row.get("next_attempt_at"),
            })
        })
        .transpose()
    }

    async fn save_health_discord_incident(
        &self,
        channel_id: u64,
        config_revision: &str,
        message_id: &str,
        active: bool,
        checks: &[HealthCheck],
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let channel_id = i64::try_from(channel_id).map_err(|_| {
            sqlx::Error::Protocol(
                "health incident channel ID exceeds PostgreSQL BIGINT".to_string(),
            )
        })?;
        sqlx::query(
            "INSERT INTO health_discord_incidents (incident_key, channel_id, config_revision, message_id, active, degradation, updated_at) VALUES ('current',$1,$2,$3,$4,$5,$6) ON CONFLICT (incident_key) DO UPDATE SET channel_id = EXCLUDED.channel_id, config_revision = EXCLUDED.config_revision, message_id = EXCLUDED.message_id, active = EXCLUDED.active, degradation = EXCLUDED.degradation, last_error = NULL, failure_kind = NULL, next_attempt_at = NULL, attempt_count = 0, updated_at = EXCLUDED.updated_at",
        )
        .bind(channel_id)
        .bind(config_revision)
        .bind(message_id)
        .bind(active)
        .bind(serde_json::to_value(checks).map_err(json_to_sqlx)?)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_health_discord_incident_failure(
        &self,
        channel_id: u64,
        config_revision: &str,
        error: &HealthPublishError,
        checks: &[HealthCheck],
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let channel_id = i64::try_from(channel_id).map_err(|_| {
            sqlx::Error::Protocol(
                "health incident channel ID exceeds PostgreSQL BIGINT".to_string(),
            )
        })?;
        sqlx::query(
            "INSERT INTO health_discord_incidents (incident_key, channel_id, config_revision, message_id, active, degradation, last_error, failure_kind, next_attempt_at, attempt_count, updated_at) VALUES ('current',$1,$2,NULL,TRUE,$3,$4,$5,$6,1,$7) ON CONFLICT (incident_key) DO UPDATE SET channel_id = EXCLUDED.channel_id, config_revision = EXCLUDED.config_revision, message_id = CASE WHEN $8 THEN NULL ELSE health_discord_incidents.message_id END, active = TRUE, degradation = EXCLUDED.degradation, last_error = EXCLUDED.last_error, failure_kind = EXCLUDED.failure_kind, next_attempt_at = EXCLUDED.next_attempt_at, attempt_count = health_discord_incidents.attempt_count + 1, updated_at = EXCLUDED.updated_at",
        )
        .bind(channel_id)
        .bind(config_revision)
        .bind(serde_json::to_value(checks).map_err(json_to_sqlx)?)
        .bind(error.detail())
        .bind(error.failure_kind())
        .bind(error.next_attempt_at(observed_at))
        .bind(observed_at)
        .bind(error.clears_message_identity())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn adopt_health_discord_incident_state(
        &self,
        state: &HealthDiscordIncidentState,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let channel_id = i64::try_from(state.channel_id).map_err(|_| {
            sqlx::Error::Protocol(
                "health incident channel ID exceeds PostgreSQL BIGINT".to_string(),
            )
        })?;
        sqlx::query(
            "INSERT INTO health_discord_incidents (incident_key, channel_id, config_revision, message_id, active, degradation, last_error, failure_kind, next_attempt_at, attempt_count, updated_at) VALUES ('current',$1,$2,$3,$4,'[]'::jsonb,$5,$6,$7,$8,$9) ON CONFLICT (incident_key) DO UPDATE SET channel_id = EXCLUDED.channel_id, config_revision = EXCLUDED.config_revision, message_id = EXCLUDED.message_id, active = EXCLUDED.active, last_error = EXCLUDED.last_error, failure_kind = EXCLUDED.failure_kind, next_attempt_at = EXCLUDED.next_attempt_at, attempt_count = EXCLUDED.attempt_count, updated_at = EXCLUDED.updated_at",
        )
        .bind(channel_id)
        .bind(&state.config_revision)
        .bind(&state.message_id)
        .bind(state.active)
        .bind(&state.last_error)
        .bind(&state.failure_kind)
        .bind(state.next_attempt_at)
        .bind(i32::from(state.failure_kind.is_some()))
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
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
        let persisted = sqlx::query_scalar::<_, Value>(
            "SELECT context FROM contract_observed_embed_contexts WHERE region_id = $1 AND contract_id = $2 FOR UPDATE",
        )
        .bind(region_id)
        .bind(contract_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(persisted) = persisted {
            let mut persisted =
                serde_json::from_value::<ContractEmbedContext>(persisted).map_err(json_to_sqlx)?;
            if persisted.location.solar_system_id.is_some()
                && context.location.solar_system_id.is_some()
                && persisted.location.solar_system_id != context.location.solar_system_id
            {
                persisted.location = context.location.clone();
            }
            persisted.merge_missing_from(&context);
            sqlx::query("UPDATE contract_observed_embed_contexts SET context = $3, updated_at = now() WHERE region_id = $1 AND contract_id = $2")
                .bind(region_id)
                .bind(contract_id)
                .bind(serde_json::to_value(persisted).map_err(json_to_sqlx)?)
                .execute(&mut *transaction)
                .await?;
        } else {
            sqlx::query("INSERT INTO contract_observed_embed_contexts (region_id, contract_id, context, observed_at) VALUES ($1,$2,$3,$4)")
                .bind(region_id)
                .bind(contract_id)
                .bind(serde_json::to_value(context).map_err(json_to_sqlx)?)
                .bind(observed_at)
                .execute(&mut *transaction)
                .await?;
        }
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
                        AND acceptance_provenance = 'accepted_by_player' \
                        AND jsonb_array_length(COALESCE(manifest->'requested_items', '[]'::jsonb)) = 0 \
                        AND COALESCE((contract->>'price')::double precision, 0) > 0) AS confirmed_sales, \
                    count(*) FILTER (WHERE state = 'acceptance_confirmed' \
                        AND acceptance_provenance = 'accepted_by_player' \
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
                   AND acceptance_provenance = 'accepted_by_player' \
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
        sqlx::query("SELECT region_id, contract_id, state, last_public_observed_at, absence_observed_at, acceptance_evidence_at, acceptance_provenance FROM contract_resolution_cases ORDER BY region_id, contract_id")
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

    pub async fn prepare_delivery_repair(
        &self,
        delivery_id: i64,
        desired_message: ContractNotificationMessage,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET desired_message = $2, repair_status = 'pending', repair_revision = CASE WHEN repair_status <> 'pending' OR desired_message IS DISTINCT FROM $2 THEN repair_revision + 1 ELSE repair_revision END, repair_prepared_at = CASE WHEN repair_status = 'pending' THEN repair_prepared_at ELSE now() END, repair_next_attempt_at = CASE WHEN repair_status = 'pending' THEN repair_next_attempt_at ELSE NULL END, repair_failure_kind = CASE WHEN repair_status = 'pending' THEN repair_failure_kind ELSE NULL END, repair_last_error = CASE WHEN repair_status = 'pending' THEN repair_last_error ELSE NULL END, repair_failed_at = CASE WHEN repair_status = 'pending' THEN repair_failed_at ELSE NULL END, repair_failure_resolved_at = CASE WHEN repair_status = 'pending' THEN repair_failure_resolved_at ELSE NULL END WHERE id = $1 AND status = 'sent' AND discord_message_id IS NOT NULL")
            .bind(delivery_id)
            .bind(serde_json::to_value(desired_message).map_err(json_to_sqlx)?)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    async fn rerender_contract_delivery_candidates(
        &self,
        channel_id: u64,
        selector: &ContractDeliveryRerenderSelector,
    ) -> Result<ContractDeliveryRerenderSelection, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let selection = self
            .select_contract_delivery_rerender_candidates(
                &mut transaction,
                channel_id,
                selector,
                false,
            )
            .await?;
        transaction.commit().await?;
        Ok(selection)
    }

    async fn select_contract_delivery_rerender_candidates(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        channel_id: u64,
        selector: &ContractDeliveryRerenderSelector,
        lock_for_queue: bool,
    ) -> Result<ContractDeliveryRerenderSelection, sqlx::Error> {
        const RERENDER_SELECTION_PAGE_SIZE: i64 = 64;
        const RERENDER_SELECTION_MAX_SCANNED_ROWS: usize = 4096;
        const RERENDER_DELIVERY_COLUMNS: &str = "SELECT deliveries.id, deliveries.contract_id, deliveries.event_kind, deliveries.status, deliveries.discord_message_id, deliveries.repair_status, deliveries.failure_kind, deliveries.repair_failure_kind, deliveries.event, deliveries.message, subscriptions.guild_id AS retained_subscription_guild_id, subscriptions.channel_id AS retained_subscription_channel_id, subscriptions.subscription_id AS retained_subscription_id, subscriptions.description AS retained_subscription_description, subscriptions.filter AS retained_subscription_filter, subscriptions.event_actions AS retained_subscription_event_actions, subscriptions.deleted_at AS retained_subscription_deleted_at FROM contract_outbound_deliveries AS deliveries LEFT JOIN contract_subscriptions AS subscriptions ON subscriptions.guild_id = deliveries.guild_id AND subscriptions.channel_id = deliveries.channel_id AND subscriptions.subscription_id = deliveries.subscription_id";
        const OUTDATED_PRESENTATION: &str = "(CASE WHEN jsonb_typeof(deliveries.message -> 'presentation_revision') = 'number' AND length(deliveries.message ->> 'presentation_revision') <= 9 AND (deliveries.message ->> 'presentation_revision') ~ '^[0-9]+$' THEN (deliveries.message ->> 'presentation_revision')::INTEGER ELSE 0 END) < $2";
        let lock_clause = if lock_for_queue {
            " FOR UPDATE OF deliveries SKIP LOCKED"
        } else {
            ""
        };
        let mut selection = ContractDeliveryRerenderSelection::default();
        let mut selected_ids = BTreeSet::new();
        match selector {
            ContractDeliveryRerenderSelector::Limit(limit) => {
                let scan_limit = limit
                    .saturating_mul(RERENDER_SELECTION_PAGE_SIZE as usize)
                    .clamp(
                        RERENDER_SELECTION_PAGE_SIZE as usize,
                        RERENDER_SELECTION_MAX_SCANNED_ROWS,
                    );
                let mut after_delivery_id = 0_i64;
                let mut scanned_rows = 0_usize;
                while selection.candidates.len() < *limit && scanned_rows < scan_limit {
                    let page_size = (scan_limit - scanned_rows)
                        .min(RERENDER_SELECTION_PAGE_SIZE as usize)
                        as i64;
                    let query = format!(
                        "{RERENDER_DELIVERY_COLUMNS} WHERE deliveries.channel_id = $1 AND {OUTDATED_PRESENTATION} AND deliveries.id > $3 ORDER BY deliveries.id LIMIT $4{lock_clause}"
                    );
                    let rows = sqlx::query(&query)
                        .bind(channel_id as i64)
                        .bind(CONTRACT_NOTIFICATION_PRESENTATION_REVISION as i32)
                        .bind(after_delivery_id)
                        .bind(page_size)
                        .fetch_all(&mut **transaction)
                        .await?;
                    let Some(last_row) = rows.last() else {
                        break;
                    };
                    after_delivery_id = last_row.get("id");
                    scanned_rows += rows.len();
                    for row in rows {
                        match contract_delivery_rerender_row_from_row(row)? {
                            ContractDeliveryRerenderRow::Candidate(candidate) => {
                                selection.candidates.push(candidate);
                                if selection.candidates.len() == *limit {
                                    break;
                                }
                            }
                            ContractDeliveryRerenderRow::Excluded(exclusion) => {
                                selection.excluded.push(exclusion);
                            }
                        }
                    }
                }
                if selection.candidates.len() < *limit && scanned_rows == scan_limit {
                    let more_outdated_rows: bool = sqlx::query_scalar(&format!(
                        "SELECT EXISTS(SELECT 1 FROM contract_outbound_deliveries AS deliveries WHERE deliveries.channel_id = $1 AND {OUTDATED_PRESENTATION} AND deliveries.id > $3)"
                    ))
                    .bind(channel_id as i64)
                    .bind(CONTRACT_NOTIFICATION_PRESENTATION_REVISION as i32)
                    .bind(after_delivery_id)
                    .fetch_one(&mut **transaction)
                    .await?;
                    if more_outdated_rows {
                        return Err(sqlx::Error::Protocol(format!(
                            "rerender scan limit reached after examining {scan_limit} outdated deliveries for channel {channel_id}; rerun with --delivery-ids for explicit retained delivery IDs"
                        )));
                    }
                }
            }
            ContractDeliveryRerenderSelector::DeliveryIds(delivery_ids) => {
                let query = format!(
                    "{RERENDER_DELIVERY_COLUMNS} WHERE deliveries.channel_id = $1 AND {OUTDATED_PRESENTATION} AND deliveries.id = ANY($3) ORDER BY array_position($3::BIGINT[], deliveries.id){lock_clause}"
                );
                for row in sqlx::query(&query)
                    .bind(channel_id as i64)
                    .bind(CONTRACT_NOTIFICATION_PRESENTATION_REVISION as i32)
                    .bind(delivery_ids)
                    .fetch_all(&mut **transaction)
                    .await?
                {
                    let delivery_id: i64 = row.get("id");
                    selected_ids.insert(delivery_id);
                    match contract_delivery_rerender_row_from_row(row)? {
                        ContractDeliveryRerenderRow::Candidate(candidate) => {
                            selection.candidates.push(candidate);
                        }
                        ContractDeliveryRerenderRow::Excluded(exclusion) => {
                            selection.excluded.push(exclusion);
                        }
                    }
                }
            }
        }
        if let ContractDeliveryRerenderSelector::DeliveryIds(delivery_ids) = selector {
            let missing = delivery_ids
                .iter()
                .filter(|delivery_id| !selected_ids.contains(delivery_id))
                .map(i64::to_string)
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(sqlx::Error::Protocol(format!(
                    "selected deliveries are not eligible outdated retained deliveries for channel {channel_id}: {}",
                    missing.join(",")
                )));
            }
            if !selection.excluded.is_empty() {
                return Err(sqlx::Error::Protocol(format!(
                    "selected deliveries have malformed retained data: {}",
                    selection
                        .excluded
                        .iter()
                        .map(|excluded| format!("{} ({})", excluded.delivery_id, excluded.reason))
                        .collect::<Vec<_>>()
                        .join(",")
                )));
            }
        }
        Ok(selection)
    }

    async fn queue_contract_delivery_rerenders(
        &self,
        channel_id: u64,
        selector: &ContractDeliveryRerenderSelector,
        actor: &str,
        occurred_at: DateTime<Utc>,
        region_names: &HashMap<i64, String>,
        region_name_esi: Option<&dyn PublicContractEsi>,
        ship_groups: &dyn ShipGroupResolver,
    ) -> Result<ContractDeliveryRerenderSelection, sqlx::Error> {
        if actor != CONTRACT_DELIVERY_OPERATOR_ACTOR {
            return Err(sqlx::Error::Protocol(
                "delivery rerender actor is not authorized".to_string(),
            ));
        }
        let selected = self
            .rerender_contract_delivery_candidates(channel_id, selector)
            .await?;
        if selected.candidates.is_empty() {
            return Ok(selected);
        }
        let system_names = configured_contract_system_names();
        let mut resolved_region_names = region_names.clone();
        for candidate in &selected.candidates {
            let region_id = candidate.event.region_id;
            if resolved_region_names
                .get(&region_id)
                .is_some_and(|name| !name.trim().is_empty())
            {
                continue;
            }
            if let Some(name) = candidate
                .event
                .embed_context
                .location
                .region_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
            {
                resolved_region_names.insert(region_id, name.to_string());
                continue;
            }
            if let Some(name) = self
                .observed_embed_context(region_id, candidate.event.contract.contract_id)
                .await?
                .and_then(|context| context.location.region_name)
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty())
            {
                resolved_region_names.insert(region_id, name);
                continue;
            }
            if let Some(name) = self.retained_region_name(region_id).await? {
                resolved_region_names.insert(region_id, name);
                continue;
            }
            let Some(esi) = region_name_esi.filter(|esi| esi.supports_region_name_lookup()) else {
                return Err(sqlx::Error::Protocol(format!(
                    "human-readable region name is unavailable for region {region_id}"
                )));
            };
            let response = esi.region_name(region_id, None).await.map_err(|error| {
                sqlx::Error::Protocol(format!(
                    "public region name lookup failed for region {region_id}: {error}"
                ))
            })?;
            let Some(name) = response
                .value
                .flatten()
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty())
            else {
                return Err(sqlx::Error::Protocol(format!(
                    "public region name lookup returned no name for region {region_id}"
                )));
            };
            resolved_region_names.insert(region_id, name);
        }
        let delivery_ids = selected
            .candidates
            .iter()
            .map(|candidate| candidate.candidate.delivery_id)
            .collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        let locked = self
            .select_contract_delivery_rerender_candidates(
                &mut transaction,
                channel_id,
                &ContractDeliveryRerenderSelector::DeliveryIds(delivery_ids),
                true,
            )
            .await?;
        for candidate in &locked.candidates {
            let event = self
                .reconstruct_historical_matched_range(
                    candidate,
                    &resolved_region_names,
                    &system_names,
                    occurred_at,
                )
                .await?;
            let (issuer_history, corporation_history) = self
                .contract_party_history(
                    event.contract.issuer_id,
                    event.contract.issuer_corporation_id,
                )
                .await?;
            let regenerated = contract_notification_message(
                &event,
                historical_contract_primary_item(&event, ship_groups).await?,
                &issuer_history,
                &corporation_history,
                false,
            );
            let desired_message = repaired_contract_message(
                &candidate.stored_message,
                &regenerated,
                candidate.event.kind,
            );
            sqlx::query("UPDATE contract_outbound_deliveries SET event = $2, desired_message = $3, repair_status = 'pending', repair_revision = repair_revision + 1, repair_prepared_at = now(), repair_next_attempt_at = NULL, repair_failure_kind = NULL, repair_last_error = NULL, repair_failed_at = NULL, repair_failure_resolved_at = NULL WHERE id = $1")
                .bind(candidate.candidate.delivery_id)
                .bind(serde_json::to_value(event).map_err(json_to_sqlx)?)
                .bind(serde_json::to_value(desired_message).map_err(json_to_sqlx)?)
                .execute(&mut *transaction)
                .await?;
            sqlx::query("INSERT INTO contract_delivery_audit (delivery_id, action, actor, occurred_at, detail) VALUES ($1, 'rerendered', $2, $3, 'operator queued historical contract format rerender')")
                .bind(candidate.candidate.delivery_id)
                .bind(actor)
                .bind(occurred_at)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(ContractDeliveryRerenderSelection {
            candidates: locked.candidates,
            excluded: selected.excluded,
        })
    }

    async fn historical_retained_location(
        &self,
        event: &ContractEvent,
        system_names: &HashMap<i64, String>,
        evidence_now: DateTime<Utc>,
    ) -> Result<Option<ContractLocationContext>, sqlx::Error> {
        let cached_structure = sqlx::query("SELECT evidence.solar_system_id, evidence.region_id, state.structure_name FROM location_evidence AS evidence JOIN structure_resolution_state AS state ON state.structure_id = evidence.structure_id AND state.solar_system_id = evidence.solar_system_id WHERE evidence.location_id = $1 AND evidence.evidence_class = 'access_qualified' AND evidence.observed_at <= $2 AND evidence.superseded_at IS NULL AND evidence.expired_at IS NULL AND evidence.expires_at > $2 AND state.cache_expires_at > $2 ORDER BY evidence.observed_at DESC, evidence.id DESC LIMIT 1")
            .bind(event.contract.start_location_id)
            .bind(evidence_now)
            .fetch_optional(&self.pool)
            .await?;
        if let Some(cached_structure) = cached_structure {
            let structure_name = cached_structure
                .get::<Option<String>, _>("structure_name")
                .map(|name| sanitize_contract_text(&name))
                .filter(|name| !name.is_empty());
            if let Some(structure_name) = structure_name {
                let solar_system_id: i64 = cached_structure.get("solar_system_id");
                return Ok(Some(ContractLocationContext {
                    location_name: Some(structure_name),
                    location_kind: Some("Player-owned structure".to_string()),
                    solar_system_id: Some(solar_system_id),
                    solar_system_name: system_names.get(&solar_system_id).cloned().or(self
                        .retained_range_center_name(u32::try_from(solar_system_id).map_err(
                            |_| {
                                sqlx::Error::Protocol(format!(
                                    "retained solar-system ID is invalid: {solar_system_id}"
                                ))
                            },
                        )?)
                        .await?),
                    region_id: cached_structure
                        .get::<Option<i64>, _>("region_id")
                        .or(Some(event.region_id)),
                    ..ContractLocationContext::default()
                }));
            }
        }
        let Some(evidence) = LocationEvidenceService::new(self)
            .last_verified_access_qualified(event.contract.start_location_id, evidence_now)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(ContractLocationContext {
            location_kind: Some("Player-owned structure".to_string()),
            last_verified_at: Some(evidence.observed_at),
            solar_system_id: Some(evidence.solar_system_id),
            solar_system_name: system_names.get(&evidence.solar_system_id).cloned().or(self
                .retained_range_center_name(u32::try_from(evidence.solar_system_id).map_err(
                    |_| {
                        sqlx::Error::Protocol(format!(
                            "retained solar-system ID is invalid: {}",
                            evidence.solar_system_id
                        ))
                    },
                )?)
                .await?),
            region_id: evidence.region_id.or(Some(event.region_id)),
            ..ContractLocationContext::default()
        }))
    }

    async fn reconstruct_historical_matched_range(
        &self,
        candidate: &ValidatedContractDeliveryRerenderCandidate,
        region_names: &HashMap<i64, String>,
        system_names: &HashMap<i64, String>,
        evidence_now: DateTime<Utc>,
    ) -> Result<ContractEvent, sqlx::Error> {
        let mut event = candidate.event.clone();
        if let Some(snapshot) = self
            .observed_embed_context(event.region_id, event.contract.contract_id)
            .await?
        {
            event.embed_context.merge_missing_from(&snapshot);
        }
        if let Some(location) = self
            .historical_retained_location(&event, system_names, evidence_now)
            .await?
        {
            let retained = &mut event.embed_context.location;
            let compatible_system = retained
                .solar_system_id
                .is_none_or(|system_id| Some(system_id) == location.solar_system_id);
            let compatible_region = retained
                .region_id
                .is_none_or(|region_id| Some(region_id) == location.region_id);
            let compatible_kind = retained
                .location_kind
                .as_deref()
                .is_none_or(|kind| kind == "Player-owned structure");
            if retained.location_name.is_none()
                && compatible_system
                && compatible_region
                && compatible_kind
            {
                if retained.location_kind.is_none() {
                    retained.location_kind = location.location_kind;
                }
                if location.location_name.is_some() {
                    retained.last_verified_at = None;
                } else if retained.last_verified_at.is_none() {
                    retained.last_verified_at = location.last_verified_at;
                }
                if retained.solar_system_id.is_none() {
                    retained.solar_system_id = location.solar_system_id;
                }
                if retained.solar_system_name.is_none() {
                    retained.solar_system_name = location.solar_system_name;
                }
                if retained.region_id.is_none() {
                    retained.region_id = location.region_id;
                }
                if retained.region_name.is_none() {
                    retained.region_name = location.region_name;
                }
                retained.location_name = location.location_name;
            }
        }
        event.embed_context.location.region_id = Some(event.region_id);
        if event
            .embed_context
            .location
            .region_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            event.embed_context.location.region_name =
                if let Some(region_name) = region_names.get(&event.region_id).cloned() {
                    Some(region_name)
                } else {
                    self.retained_region_name(event.region_id).await?
                };
        }
        if event
            .embed_context
            .location
            .region_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            return Err(sqlx::Error::Protocol(format!(
                "human-readable region name is unavailable for region {}",
                event.region_id
            )));
        }
        if event.embed_context.matched_range.is_some() {
            return Ok(event);
        }
        let Some(range) = retained_direct_positive_range(&event, &candidate.subscription.filter)
        else {
            return Ok(event);
        };
        let reference_system_name = event
            .context
            .range_center_names
            .get(&range.reference_system_id)
            .cloned()
            .or(self
                .retained_range_center_name(range.reference_system_id)
                .await?)
            .map(|name| sanitize_contract_text(&name))
            .filter(|name| !name.is_empty());
        let Some(reference_system_name) = reference_system_name else {
            return Ok(event);
        };
        event.embed_context.matched_range = contract_matched_range_for_embed(
            MatchedContractRange {
                light_years: range.light_years,
                reference_system_id: range.reference_system_id,
                reference_system_name,
            },
            &event.embed_context.location,
        );
        Ok(event)
    }

    async fn retained_range_center_name(
        &self,
        system_id: u32,
    ) -> Result<Option<String>, sqlx::Error> {
        let key = format!("universe/systems/{system_id}/range-center-name");
        Ok(self
            .cache(&key)
            .await?
            .and_then(|cached| serde_json::from_value::<String>(cached.response).ok()))
    }

    async fn retained_region_name(&self, region_id: i64) -> Result<Option<String>, sqlx::Error> {
        let key = format!("universe/regions/{region_id}/contract-region-name");
        Ok(self
            .cache(&key)
            .await?
            .and_then(|cached| serde_json::from_value::<Option<String>>(cached.response).ok())
            .flatten()
            .filter(|name| !name.trim().is_empty()))
    }

    pub async fn inspect_contract_delivery(
        &self,
        delivery_id: i64,
    ) -> Result<Option<ContractDeliveryInspection>, sqlx::Error> {
        let row = sqlx::query("SELECT id, status, discord_message_id, ping, ping_type, desired_message, repair_status, repair_attempt_count, last_repair_attempt_at, repair_next_attempt_at, repair_failure_kind, repair_last_error FROM contract_outbound_deliveries WHERE id = $1")
            .bind(delivery_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let audit = sqlx::query("SELECT action, actor, occurred_at, detail FROM contract_delivery_audit WHERE delivery_id = $1 ORDER BY id")
            .bind(delivery_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| ContractDeliveryAudit {
                action: row.get("action"),
                actor: row.get("actor"),
                occurred_at: row.get("occurred_at"),
                detail: row.get("detail"),
            })
            .collect();
        Ok(Some(ContractDeliveryInspection {
            delivery_id: row.get("id"),
            status: delivery_status_from_str(&row.get::<String, _>("status"))?,
            discord_message_id: row.get("discord_message_id"),
            ping: row.get("ping"),
            ping_type: contract_ping_type_from_str(&row.get::<String, _>("ping_type"))?,
            desired_message: row
                .get::<Option<Value>, _>("desired_message")
                .map(|message| serde_json::from_value(message).map_err(json_to_sqlx))
                .transpose()?,
            repair_status: contract_repair_status_from_str(&row.get::<String, _>("repair_status"))?,
            repair_attempt_count: row.get("repair_attempt_count"),
            last_repair_attempt_at: row.get("last_repair_attempt_at"),
            next_repair_attempt_at: row.get("repair_next_attempt_at"),
            repair_failure_kind: row
                .get::<Option<String>, _>("repair_failure_kind")
                .map(|kind| delivery_failure_kind_from_str(&kind))
                .transpose()?,
            repair_last_error: row.get("repair_last_error"),
            audit,
        }))
    }

    async fn requeue_permanent_contract_delivery(
        &self,
        delivery_id: i64,
        actor: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<ContractDeliveryInspection, sqlx::Error> {
        if actor != CONTRACT_DELIVERY_OPERATOR_ACTOR {
            return Err(sqlx::Error::Protocol(
                "delivery requeue actor is not authorized".to_string(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT status, failure_kind, failure_resolved_at, repair_status, desired_message FROM contract_outbound_deliveries WHERE id = $1 FOR UPDATE")
            .bind(delivery_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| sqlx::Error::Protocol(format!("contract delivery {delivery_id} does not exist")))?;
        if row.get::<String, _>("status") != "failed"
            || row.get::<Option<String>, _>("failure_kind").as_deref() != Some("permanent")
            || row
                .get::<Option<DateTime<Utc>>, _>("failure_resolved_at")
                .is_some()
        {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {delivery_id} is not an unresolved permanent failure"
            )));
        }
        let requeue_repair = row.get::<String, _>("repair_status") == "permanent"
            && row.get::<Option<Value>, _>("desired_message").is_some();
        let (status, repair_status, detail) = if requeue_repair {
            (
                "sent",
                "pending",
                "operator requeued permanent Discord repair",
            )
        } else {
            (
                "prepared",
                "none",
                "operator requeued permanent Discord delivery",
            )
        };
        sqlx::query("UPDATE contract_outbound_deliveries SET status = $2, repair_status = $3, repair_next_attempt_at = NULL, failure_resolved_at = $4, repair_failure_resolved_at = CASE WHEN $3 = 'pending' THEN $4 ELSE repair_failure_resolved_at END WHERE id = $1")
            .bind(delivery_id)
            .bind(status)
            .bind(repair_status)
            .bind(occurred_at)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO contract_delivery_audit (delivery_id, action, actor, occurred_at, detail) VALUES ($1, 'requeued', $2, $3, $4)")
            .bind(delivery_id)
            .bind(actor)
            .bind(occurred_at)
            .bind(detail)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        self.inspect_contract_delivery(delivery_id)
            .await?
            .ok_or_else(|| {
                sqlx::Error::Protocol(format!(
                    "contract delivery {delivery_id} disappeared after requeue"
                ))
            })
    }

    async fn deferred_contract_matches(&self) -> Result<Vec<DeferredContractMatch>, sqlx::Error> {
        sqlx::query("SELECT contract_subscriptions.guild_id, contract_subscriptions.channel_id, contract_subscriptions.subscription_id, contract_subscriptions.description, contract_subscriptions.filter, contract_subscriptions.event_actions, contract_deferred_subscription_matches.event, contract_deferred_subscription_matches.proximity_unverified FROM contract_deferred_subscription_matches JOIN contract_subscriptions USING (guild_id, channel_id, subscription_id) WHERE contract_subscriptions.deleted_at IS NULL AND NOT contract_deferred_subscription_matches.proximity_unverified ORDER BY contract_deferred_subscription_matches.created_at, contract_subscriptions.guild_id, contract_subscriptions.channel_id, contract_subscriptions.subscription_id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let event = serde_json::from_value(row.get("event")).map_err(json_to_sqlx)?;
                let proximity_unverified = row.get("proximity_unverified");
                Ok(DeferredContractMatch {
                    subscription: contract_subscription_from_row(row)?,
                    event,
                    proximity_unverified,
                })
            })
            .collect()
    }

    async fn proximity_unverified_matches_for_location(
        &self,
        location_id: i64,
    ) -> Result<Vec<DeferredContractMatch>, sqlx::Error> {
        sqlx::query("SELECT contract_subscriptions.guild_id, contract_subscriptions.channel_id, contract_subscriptions.subscription_id, contract_subscriptions.description, contract_subscriptions.filter, contract_subscriptions.event_actions, contract_deferred_subscription_matches.event FROM contract_deferred_subscription_matches JOIN contract_subscriptions USING (guild_id, channel_id, subscription_id) WHERE contract_subscriptions.deleted_at IS NULL AND contract_deferred_subscription_matches.proximity_unverified AND contract_deferred_subscription_matches.event #>> '{contract,start_location_id}' = $1 ORDER BY contract_deferred_subscription_matches.created_at, contract_subscriptions.guild_id, contract_subscriptions.channel_id, contract_subscriptions.subscription_id")
            .bind(location_id.to_string())
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let event = serde_json::from_value(row.get("event")).map_err(json_to_sqlx)?;
                Ok(DeferredContractMatch {
                    subscription: contract_subscription_from_row(row)?,
                    event,
                    proximity_unverified: true,
                })
            })
            .collect()
    }

    async fn proximity_unverified_locations_ready_for_reconciliation(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar("SELECT DISTINCT (matches.event #>> '{contract,start_location_id}')::BIGINT FROM contract_deferred_subscription_matches AS matches WHERE matches.proximity_unverified AND (EXISTS (SELECT 1 FROM (SELECT evidence.id FROM location_evidence AS evidence WHERE evidence.location_id = (matches.event #>> '{contract,start_location_id}')::BIGINT AND evidence.observed_at <= $1 AND evidence.superseded_at IS NULL AND evidence.expired_at IS NULL AND (evidence.expires_at IS NULL OR evidence.expires_at > $1) ORDER BY CASE evidence.evidence_class WHEN 'public_npc' THEN 0 WHEN 'access_qualified' THEN 1 WHEN 'operator' THEN 2 ELSE 3 END, evidence.observed_at DESC, evidence.id DESC LIMIT 1) AS selected WHERE selected.id IS DISTINCT FROM NULLIF(matches.event #>> '{context,location_evidence_id}', '')::BIGINT) OR (NOT EXISTS (SELECT 1 FROM location_evidence AS evidence WHERE evidence.location_id = (matches.event #>> '{contract,start_location_id}')::BIGINT AND evidence.observed_at <= $1 AND evidence.superseded_at IS NULL AND evidence.expired_at IS NULL AND (evidence.expires_at IS NULL OR evidence.expires_at > $1)) AND EXISTS (SELECT 1 FROM structure_resolution_state AS state WHERE state.structure_id = (matches.event #>> '{contract,start_location_id}')::BIGINT AND state.parked_at IS NULL AND state.next_attempt_at IS NOT NULL AND state.next_attempt_at <= $1 AND (state.lease_expires_at IS NULL OR state.lease_expires_at <= $1)))) ORDER BY 1")
            .bind(now)
            .fetch_all(&self.pool)
            .await
    }

    async fn defer_contract_match(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        proximity_unverified: bool,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        self.defer_contract_match_in_transaction(
            &mut transaction,
            subscription,
            event,
            proximity_unverified,
        )
        .await?;
        transaction.commit().await
    }

    async fn defer_contract_match_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        proximity_unverified: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO contract_deferred_subscription_matches (guild_id, channel_id, subscription_id, contract_id, event_kind, event, proximity_unverified) SELECT $1,$2,$3,$4,$5,$6,$7 WHERE EXISTS (SELECT 1 FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND deleted_at IS NULL) ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind) DO UPDATE SET event = EXCLUDED.event, proximity_unverified = contract_deferred_subscription_matches.proximity_unverified OR EXCLUDED.proximity_unverified, updated_at = now()")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .bind(serde_json::to_value(event).map_err(json_to_sqlx)?)
            .bind(proximity_unverified)
            .execute(&mut **transaction)
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

    async fn prepare_proximity_resolution(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        in_range_message: Option<&ContractNotificationMessage>,
        ordering: Option<DeliveryOrdering>,
    ) -> Result<PrepareProximityResolutionOutcome, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_location(&mut transaction, event.contract.start_location_id).await?;
        let selected_evidence = LocationEvidenceService::resolve_in_transaction(
            &mut transaction,
            event.contract.start_location_id,
            Utc::now(),
        )
        .await?;
        let Some(selected_evidence) = selected_evidence else {
            transaction.commit().await?;
            return Ok(PrepareProximityResolutionOutcome::Unchanged);
        };
        if event
            .context
            .location_evidence_id
            .is_some_and(|id| id != selected_evidence.id)
        {
            transaction.commit().await?;
            return Ok(PrepareProximityResolutionOutcome::EvidenceChanged);
        }
        let unresolved = sqlx::query_scalar::<_, i64>("SELECT 1::BIGINT FROM contract_deferred_subscription_matches WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND contract_id = $4 AND event_kind = $5 AND proximity_unverified FOR UPDATE")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .fetch_optional(&mut *transaction)
            .await?;
        if unresolved.is_none() {
            transaction.commit().await?;
            return Ok(PrepareProximityResolutionOutcome::Unchanged);
        }
        let original = sqlx::query("SELECT id, status, discord_message_id, message, ping_type, proximity_promotion_ping FROM contract_outbound_deliveries WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND contract_id = $4 AND event_kind = $5 AND delivery_kind = 'event' FOR UPDATE")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(original) = original else {
            transaction.commit().await?;
            return Ok(PrepareProximityResolutionOutcome::Unchanged);
        };
        if original.get::<String, _>("status") != "sent"
            || original
                .get::<Option<String>, _>("discord_message_id")
                .is_none()
        {
            transaction.commit().await?;
            return Ok(PrepareProximityResolutionOutcome::Unchanged);
        }
        let stored_message: ContractNotificationMessage =
            serde_json::from_value(original.get("message")).map_err(json_to_sqlx)?;
        let desired_message = in_range_message
            .cloned()
            .unwrap_or_else(|| proximity_out_of_range_contract_message(&stored_message));
        let desired_value = serde_json::to_value(&desired_message).map_err(json_to_sqlx)?;
        sqlx::query("UPDATE contract_outbound_deliveries SET desired_message = $2, repair_status = 'pending', repair_revision = CASE WHEN repair_status <> 'pending' OR desired_message IS DISTINCT FROM $2 THEN repair_revision + 1 ELSE repair_revision END, repair_prepared_at = CASE WHEN repair_status = 'pending' THEN repair_prepared_at ELSE now() END, repair_next_attempt_at = CASE WHEN repair_status = 'pending' THEN repair_next_attempt_at ELSE NULL END, repair_failure_kind = CASE WHEN repair_status = 'pending' THEN repair_failure_kind ELSE NULL END, repair_last_error = CASE WHEN repair_status = 'pending' THEN repair_last_error ELSE NULL END, repair_failed_at = CASE WHEN repair_status = 'pending' THEN repair_failed_at ELSE NULL END, repair_failure_resolved_at = CASE WHEN repair_status = 'pending' THEN repair_failure_resolved_at ELSE NULL END WHERE id = $1 AND status = 'sent' AND discord_message_id IS NOT NULL AND (message IS DISTINCT FROM $2 OR desired_message IS DISTINCT FROM $2)")
            .bind(original.get::<i64, _>("id"))
            .bind(desired_value)
            .execute(&mut *transaction)
            .await?;
        if let Some(ordering) = ordering {
            let promotion_id = sqlx::query_scalar::<_, i64>("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, delivery_kind, event, message, ping, ping_type, status, delivery_nonce, strategic_priority, relevant_isk, confirmed_at) VALUES ($1,$2,$3,$4,$5,'proximity_promotion',$6,$7,$8,$9,'prepared',$10,$11,$12,$13) ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind, delivery_kind) DO NOTHING RETURNING id")
                .bind(subscription.guild_id as i64)
                .bind(subscription.channel_id as i64)
                .bind(&subscription.id)
                .bind(event.contract.contract_id)
                .bind(event.kind.as_str())
                .bind(serde_json::to_value(event).map_err(json_to_sqlx)?)
                .bind(serde_json::to_value(&desired_message).map_err(json_to_sqlx)?)
                .bind(original.get::<bool, _>("proximity_promotion_ping"))
                .bind(original.get::<String, _>("ping_type"))
                .bind("pending")
                .bind(i64::try_from(ordering.strategic_priority).unwrap_or(i64::MAX))
                .bind(ordering.relevant_isk)
                .bind(ordering.confirmed_at)
                .fetch_optional(&mut *transaction)
                .await?;
            if let Some(promotion_id) = promotion_id {
                sqlx::query(
                    "UPDATE contract_outbound_deliveries SET delivery_nonce = $2 WHERE id = $1",
                )
                .bind(promotion_id)
                .bind(format!("ci-{promotion_id}"))
                .execute(&mut *transaction)
                .await?;
            }
        }
        sqlx::query("DELETE FROM contract_deferred_subscription_matches WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND contract_id = $4 AND event_kind = $5 AND proximity_unverified")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(PrepareProximityResolutionOutcome::Resolved)
    }

    async fn prepare_delivery(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        message: &ContractNotificationMessage,
        ping: bool,
        ping_type: ContractPingType,
        ordering: DeliveryOrdering,
    ) -> Result<PrepareDeliveryOutcome, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_location(&mut transaction, event.contract.start_location_id).await?;
        let outcome = self
            .prepare_delivery_in_transaction(
                &mut transaction,
                subscription,
                event,
                message,
                ping,
                ping_type,
                ordering,
            )
            .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    async fn prepare_proximity_unverified_delivery(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        message: &ContractNotificationMessage,
        promotion_ping: bool,
        ping_type: ContractPingType,
        ordering: DeliveryOrdering,
    ) -> Result<PrepareDeliveryOutcome, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_location(&mut transaction, event.contract.start_location_id).await?;
        let selected_evidence = LocationEvidenceService::resolve_in_transaction(
            &mut transaction,
            event.contract.start_location_id,
            Utc::now(),
        )
        .await?;
        if selected_evidence
            .as_ref()
            .is_some_and(|evidence| event.context.location_evidence_id != Some(evidence.id))
        {
            transaction.commit().await?;
            return Ok(PrepareDeliveryOutcome::EvidenceBecameAvailable);
        }
        let outcome = self
            .prepare_delivery_in_transaction(
                &mut transaction,
                subscription,
                event,
                message,
                false,
                ping_type,
                ordering,
            )
            .await?;
        if matches!(&outcome, PrepareDeliveryOutcome::Prepared) {
            sqlx::query("UPDATE contract_outbound_deliveries SET proximity_promotion_ping = $1 WHERE guild_id = $2 AND channel_id = $3 AND subscription_id = $4 AND contract_id = $5 AND event_kind = $6 AND delivery_kind = 'event'")
                .bind(promotion_ping)
                .bind(subscription.guild_id as i64)
                .bind(subscription.channel_id as i64)
                .bind(&subscription.id)
                .bind(event.contract.contract_id)
                .bind(event.kind.as_str())
                .execute(&mut *transaction)
                .await?;
        }
        self.defer_contract_match_in_transaction(&mut transaction, subscription, event, true)
            .await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    async fn prepare_delivery_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        message: &ContractNotificationMessage,
        ping: bool,
        ping_type: ContractPingType,
        ordering: DeliveryOrdering,
    ) -> Result<PrepareDeliveryOutcome, sqlx::Error> {
        let delivery_id = sqlx::query_scalar::<_, i64>("INSERT INTO contract_outbound_deliveries (guild_id, channel_id, subscription_id, contract_id, event_kind, event, message, ping, ping_type, status, delivery_nonce, strategic_priority, relevant_isk, confirmed_at) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,'prepared',$10,$11,$12,$13 WHERE EXISTS (SELECT 1 FROM contract_subscriptions WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND deleted_at IS NULL) ON CONFLICT (guild_id, channel_id, subscription_id, contract_id, event_kind, delivery_kind) DO NOTHING RETURNING id")
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
            .fetch_optional(&mut **transaction)
            .await?;
        if let Some(delivery_id) = delivery_id {
            sqlx::query(
                "UPDATE contract_outbound_deliveries SET delivery_nonce = $2 WHERE id = $1",
            )
            .bind(delivery_id)
            .bind(format!("ci-{delivery_id}"))
            .execute(&mut **transaction)
            .await?;
            return Ok(PrepareDeliveryOutcome::Prepared);
        }
        let existing = sqlx::query("SELECT id, status, message FROM contract_outbound_deliveries WHERE guild_id = $1 AND channel_id = $2 AND subscription_id = $3 AND contract_id = $4 AND event_kind = $5 AND delivery_kind = 'event'")
            .bind(subscription.guild_id as i64)
            .bind(subscription.channel_id as i64)
            .bind(&subscription.id)
            .bind(event.contract.contract_id)
            .bind(event.kind.as_str())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| sqlx::Error::Protocol("contract delivery disappeared after its identity conflict".to_string()))?;
        if existing.get::<String, _>("status") == "sent" {
            return Ok(PrepareDeliveryOutcome::ExistingSent {
                delivery_id: existing.get("id"),
                message: serde_json::from_value(existing.get("message")).map_err(json_to_sqlx)?,
            });
        }
        Ok(PrepareDeliveryOutcome::Existing)
    }

    async fn prepared_deliveries(&self) -> Result<Vec<PreparedContractDelivery>, sqlx::Error> {
        sqlx::query("SELECT id, guild_id, channel_id, subscription_id, contract_id, event_kind, message, ping, ping_type, delivery_nonce, nonce_window_until FROM contract_outbound_deliveries WHERE status = 'prepared' ORDER BY strategic_priority ASC, relevant_isk DESC, confirmed_at ASC, id ASC")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(prepared_contract_delivery_from_row)
            .collect()
    }

    async fn pending_repairs(
        &self,
        attempted_at: DateTime<Utc>,
    ) -> Result<Vec<ContractMessageEdit>, sqlx::Error> {
        sqlx::query("SELECT id, channel_id, discord_message_id, contract_id, event_kind, desired_message, repair_revision, repair_claim_token, repair_attempt_count FROM contract_outbound_deliveries WHERE status = 'sent' AND repair_status = 'pending' AND desired_message IS NOT NULL AND (repair_next_attempt_at IS NULL OR repair_next_attempt_at <= $1) AND (repair_lease_until IS NULL OR repair_lease_until <= $1) AND NOT EXISTS (SELECT 1 FROM contract_outbound_deliveries AS deferred WHERE deferred.status = 'sent' AND deferred.repair_status = 'pending' AND deferred.repair_next_attempt_at > $1) ORDER BY repair_prepared_at, id")
            .bind(attempted_at)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(contract_message_edit_from_row)
            .collect()
    }

    async fn begin_delivery_attempt(
        &self,
        delivery_id: i64,
        attempted_at: DateTime<Utc>,
    ) -> Result<Option<PreparedContractDelivery>, sqlx::Error> {
        let nonce_window_until = attempted_at + DISCORD_NONCE_ENFORCEMENT_WINDOW;
        let claim_token = format!("delivery-{:032x}", rand::thread_rng().gen::<u128>());
        let lease_until = attempted_at + CONTRACT_DELIVERY_LEASE;
        let row = sqlx::query("UPDATE contract_outbound_deliveries SET attempt_count = attempt_count + 1, first_attempt_at = COALESCE(first_attempt_at, $2), last_attempt_at = $2, nonce_window_until = COALESCE(nonce_window_until, $3), delivery_claim_token = $4, delivery_claimed_at = $2, delivery_lease_until = $5 WHERE id = $1 AND status = 'prepared' AND (delivery_lease_until IS NULL OR delivery_lease_until <= $2) RETURNING id, guild_id, channel_id, subscription_id, contract_id, event_kind, message, ping, ping_type, delivery_nonce, nonce_window_until, delivery_claim_token")
            .bind(delivery_id)
            .bind(attempted_at)
            .bind(nonce_window_until)
            .bind(&claim_token)
            .bind(lease_until)
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

    async fn begin_repair_attempt(
        &self,
        delivery_id: i64,
        attempted_at: DateTime<Utc>,
    ) -> Result<Option<ContractMessageEdit>, sqlx::Error> {
        let claim_token = format!("repair-{:032x}", rand::thread_rng().gen::<u128>());
        let lease_until = attempted_at + CONTRACT_REPAIR_LEASE;
        let row = sqlx::query("UPDATE contract_outbound_deliveries SET repair_attempt_count = repair_attempt_count + 1, first_repair_attempt_at = COALESCE(first_repair_attempt_at, $2), last_repair_attempt_at = $2, repair_claim_token = $3, repair_claimed_revision = repair_revision, repair_claimed_at = $2, repair_lease_until = $4 WHERE id = $1 AND status = 'sent' AND repair_status = 'pending' AND desired_message IS NOT NULL AND (repair_next_attempt_at IS NULL OR repair_next_attempt_at <= $2) AND (repair_lease_until IS NULL OR repair_lease_until <= $2) RETURNING id, channel_id, discord_message_id, contract_id, event_kind, desired_message, repair_revision, repair_claim_token, repair_attempt_count")
            .bind(delivery_id)
            .bind(attempted_at)
            .bind(&claim_token)
            .bind(lease_until)
            .fetch_optional(&self.pool)
            .await?;
        row.map(contract_message_edit_from_row).transpose()
    }

    async fn mark_delivery_sent(
        &self,
        delivery: &PreparedContractDelivery,
        discord_message_id: &str,
    ) -> Result<(), sqlx::Error> {
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("delivery completion is missing its claim token".to_string())
        })?;
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET status = 'sent', discord_message_id = $2, sent_at = now(), failure_resolved_at = CASE WHEN failure_kind IS NULL THEN failure_resolved_at ELSE now() END, delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $3")
            .bind(delivery.delivery_id)
            .bind(discord_message_id)
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {} was not claimed when Discord returned success",
                delivery.delivery_id
            )));
        }
        Ok(())
    }

    async fn mark_repair_succeeded(
        &self,
        edit: &ContractMessageEdit,
        completed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let claim_token = edit.repair_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("repair completion is missing its claim token".to_string())
        })?;
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET message = desired_message, desired_message = NULL, repair_status = 'none', repair_next_attempt_at = NULL, repair_failure_resolved_at = CASE WHEN repair_failure_kind IS NOT NULL THEN $5 ELSE repair_failure_resolved_at END, repair_claim_token = NULL, repair_claimed_revision = NULL, repair_claimed_at = NULL, repair_lease_until = NULL WHERE id = $1 AND repair_claim_token = $3 AND repair_revision = $2")
            .bind(edit.delivery_id)
            .bind(edit.repair_revision)
            .bind(claim_token)
            .bind(completed_at + CONTRACT_REPAIR_STALE_COMPLETION_DELAY)
            .bind(completed_at)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            let recovery = sqlx::query("UPDATE contract_outbound_deliveries SET desired_message = COALESCE(desired_message, message), repair_status = 'pending', repair_revision = CASE WHEN desired_message IS NULL THEN repair_revision + 1 ELSE repair_revision END, repair_prepared_at = CASE WHEN desired_message IS NULL THEN $2 ELSE repair_prepared_at END, repair_next_attempt_at = GREATEST(COALESCE(repair_next_attempt_at, $3), $3), repair_failure_kind = CASE WHEN desired_message IS NULL THEN NULL ELSE repair_failure_kind END, repair_last_error = CASE WHEN desired_message IS NULL THEN NULL ELSE repair_last_error END, repair_failed_at = CASE WHEN desired_message IS NULL THEN NULL ELSE repair_failed_at END, repair_failure_resolved_at = CASE WHEN desired_message IS NULL THEN NULL ELSE repair_failure_resolved_at END WHERE id = $1 AND status = 'sent' AND discord_message_id IS NOT NULL AND repair_status <> 'permanent'")
                .bind(edit.delivery_id)
                .bind(completed_at)
                .bind(completed_at + CONTRACT_REPAIR_STALE_COMPLETION_DELAY)
                .execute(&self.pool)
                .await?;
            if recovery.rows_affected() != 1 {
                return Err(sqlx::Error::Protocol(format!(
                    "contract delivery {} was not claimed when Discord acknowledged its repair",
                    edit.delivery_id
                )));
            }
        }
        Ok(())
    }

    async fn mark_repair_failed(
        &self,
        edit: &ContractMessageEdit,
        error: &ContractDeliveryError,
        failed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let claim_token = edit.repair_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("repair failure is missing its claim token".to_string())
        })?;
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET status = CASE WHEN repair_revision = $2 OR $6 THEN 'failed' ELSE status END, failure_kind = CASE WHEN repair_revision = $2 OR $6 THEN 'permanent' ELSE failure_kind END, last_error = CASE WHEN repair_revision = $2 OR $6 THEN $4 ELSE last_error END, failed_at = CASE WHEN repair_revision = $2 OR $6 THEN $3 ELSE failed_at END, failure_resolved_at = CASE WHEN repair_revision = $2 OR $6 THEN NULL ELSE failure_resolved_at END, repair_status = CASE WHEN repair_revision = $2 OR $6 THEN 'permanent' ELSE repair_status END, repair_next_attempt_at = CASE WHEN repair_revision = $2 OR $6 THEN NULL ELSE repair_next_attempt_at END, repair_failure_kind = CASE WHEN repair_revision = $2 OR $6 THEN 'permanent' ELSE repair_failure_kind END, repair_last_error = CASE WHEN repair_revision = $2 OR $6 THEN $4 ELSE repair_last_error END, repair_failed_at = CASE WHEN repair_revision = $2 OR $6 THEN $3 ELSE repair_failed_at END, repair_failure_resolved_at = CASE WHEN repair_revision = $2 OR $6 THEN NULL ELSE repair_failure_resolved_at END, repair_claim_token = NULL, repair_claimed_revision = NULL, repair_claimed_at = NULL, repair_lease_until = NULL WHERE id = $1 AND repair_claim_token = $5")
            .bind(edit.delivery_id)
            .bind(edit.repair_revision)
            .bind(failed_at)
            .bind(sanitize_contract_failure_detail(error.message()))
            .bind(claim_token)
            .bind(error.is_delivery_scoped_permanent())
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {} was not claimed when Discord returned a permanent repair failure",
                edit.delivery_id
            )));
        }
        Ok(())
    }

    async fn mark_repair_retryable(
        &self,
        edit: &ContractMessageEdit,
        error: &ContractDeliveryError,
        retry_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let claim_token = edit.repair_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("repair retry is missing its claim token".to_string())
        })?;
        let failure_kind = match error {
            ContractDeliveryError::Transient(_) => "transient",
            ContractDeliveryError::Ambiguous(_) => "ambiguous",
            ContractDeliveryError::Permanent(_) => {
                return Err(sqlx::Error::Protocol(
                    "permanent contract repair failure cannot remain pending".to_string(),
                ))
            }
        };
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET repair_failure_kind = CASE WHEN repair_revision = $2 THEN $4 ELSE repair_failure_kind END, repair_last_error = CASE WHEN repair_revision = $2 THEN $5 ELSE repair_last_error END, repair_failure_resolved_at = CASE WHEN repair_revision = $2 THEN NULL ELSE repair_failure_resolved_at END, repair_next_attempt_at = GREATEST(COALESCE(repair_next_attempt_at, $3), $3), repair_claim_token = NULL, repair_claimed_revision = NULL, repair_claimed_at = NULL, repair_lease_until = NULL WHERE id = $1 AND repair_claim_token = $6")
            .bind(edit.delivery_id)
            .bind(edit.repair_revision)
            .bind(retry_at)
            .bind(failure_kind)
            .bind(sanitize_contract_failure_detail(error.message()))
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {} was not claimed when Discord returned a retryable repair failure",
                edit.delivery_id
            )));
        }
        Ok(())
    }

    async fn mark_delivery_failed(
        &self,
        delivery: &PreparedContractDelivery,
        error: &ContractDeliveryError,
        failed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("delivery failure is missing its claim token".to_string())
        })?;
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET status = 'failed', failure_kind = 'permanent', last_error = $2, failed_at = $3, failure_resolved_at = NULL, delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $4")
            .bind(delivery.delivery_id)
            .bind(sanitize_contract_failure_detail(error.message()))
            .bind(failed_at)
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {} was not claimed when Discord returned a permanent failure",
                delivery.delivery_id
            )));
        }
        Ok(())
    }

    async fn mark_delivery_retryable(
        &self,
        delivery: &PreparedContractDelivery,
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
        let claim_token = delivery.delivery_claim_token.as_deref().ok_or_else(|| {
            sqlx::Error::Protocol("delivery retry is missing its claim token".to_string())
        })?;
        let result = sqlx::query("UPDATE contract_outbound_deliveries SET failure_kind = $2, last_error = $3, failure_resolved_at = NULL, delivery_claim_token = NULL, delivery_claimed_at = NULL, delivery_lease_until = NULL WHERE id = $1 AND status = 'prepared' AND delivery_claim_token = $4")
            .bind(delivery.delivery_id)
            .bind(failure_kind)
            .bind(sanitize_contract_failure_detail(error.message()))
            .bind(claim_token)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(format!(
                "contract delivery {} was not claimed when Discord returned a retryable failure",
                delivery.delivery_id
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

    pub(crate) async fn record_esi_limiter_at(
        &self,
        metadata: &CacheMetadata,
        observed_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let pause_until = metadata.collection_pause_until_at(observed_at);
        let next_request_at = metadata.collection_pacing_until(observed_at);
        let pacing_active = next_request_at.is_some();
        sqlx::query(
            r#"
INSERT INTO esi_collection_limiter_state (
    limiter_scope, pause_until, error_limit_remain, error_limit_reset,
    rate_limit_group, rate_limit_limit, rate_limit_remaining, rate_limit_used,
    next_request_at, pacing_active, updated_at
) VALUES (TRUE,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (limiter_scope) DO UPDATE SET
    pause_until = CASE
        WHEN EXCLUDED.pause_until IS NULL THEN esi_collection_limiter_state.pause_until
        WHEN esi_collection_limiter_state.pause_until IS NULL
          OR esi_collection_limiter_state.pause_until < EXCLUDED.pause_until
            THEN EXCLUDED.pause_until
        ELSE esi_collection_limiter_state.pause_until
    END,
    error_limit_remain = CASE
        WHEN EXCLUDED.error_limit_remain IS NULL THEN esi_collection_limiter_state.error_limit_remain
        WHEN esi_collection_limiter_state.error_limit_remain IS NULL
          OR EXCLUDED.error_limit_remain < esi_collection_limiter_state.error_limit_remain
            THEN EXCLUDED.error_limit_remain
        ELSE esi_collection_limiter_state.error_limit_remain
    END,
    error_limit_reset = CASE
        WHEN EXCLUDED.error_limit_remain IS NULL THEN esi_collection_limiter_state.error_limit_reset
        WHEN esi_collection_limiter_state.error_limit_remain IS NULL
          OR EXCLUDED.error_limit_remain < esi_collection_limiter_state.error_limit_remain
            THEN EXCLUDED.error_limit_reset
        WHEN EXCLUDED.error_limit_remain = esi_collection_limiter_state.error_limit_remain
            THEN GREATEST(esi_collection_limiter_state.error_limit_reset, EXCLUDED.error_limit_reset)
        ELSE esi_collection_limiter_state.error_limit_reset
    END,
    rate_limit_group = CASE
        WHEN EXCLUDED.rate_limit_group IS NULL
          AND EXCLUDED.rate_limit_limit IS NULL
          AND EXCLUDED.rate_limit_remaining IS NULL
          AND EXCLUDED.rate_limit_used IS NULL
            THEN esi_collection_limiter_state.rate_limit_group
        WHEN esi_collection_limiter_state.pacing_active AND (
        EXCLUDED.next_request_at IS NULL OR (
            EXCLUDED.rate_limit_group IS DISTINCT FROM esi_collection_limiter_state.rate_limit_group
            AND esi_collection_limiter_state.next_request_at >= EXCLUDED.next_request_at
        )
    ) THEN esi_collection_limiter_state.rate_limit_group ELSE COALESCE(EXCLUDED.rate_limit_group, esi_collection_limiter_state.rate_limit_group) END,
    rate_limit_limit = CASE
        WHEN EXCLUDED.rate_limit_group IS NULL
          AND EXCLUDED.rate_limit_limit IS NULL
          AND EXCLUDED.rate_limit_remaining IS NULL
          AND EXCLUDED.rate_limit_used IS NULL
            THEN esi_collection_limiter_state.rate_limit_limit
        WHEN esi_collection_limiter_state.pacing_active AND (
        EXCLUDED.next_request_at IS NULL OR (
            EXCLUDED.rate_limit_group IS DISTINCT FROM esi_collection_limiter_state.rate_limit_group
            AND esi_collection_limiter_state.next_request_at >= EXCLUDED.next_request_at
        )
    ) THEN esi_collection_limiter_state.rate_limit_limit ELSE COALESCE(EXCLUDED.rate_limit_limit, esi_collection_limiter_state.rate_limit_limit) END,
    rate_limit_remaining = CASE
        WHEN EXCLUDED.rate_limit_group IS NULL
          AND EXCLUDED.rate_limit_limit IS NULL
          AND EXCLUDED.rate_limit_remaining IS NULL
          AND EXCLUDED.rate_limit_used IS NULL
            THEN esi_collection_limiter_state.rate_limit_remaining
        WHEN esi_collection_limiter_state.pacing_active AND (
        EXCLUDED.next_request_at IS NULL OR (
            EXCLUDED.rate_limit_group IS DISTINCT FROM esi_collection_limiter_state.rate_limit_group
            AND esi_collection_limiter_state.next_request_at >= EXCLUDED.next_request_at
        )
    ) THEN esi_collection_limiter_state.rate_limit_remaining ELSE COALESCE(EXCLUDED.rate_limit_remaining, esi_collection_limiter_state.rate_limit_remaining) END,
    rate_limit_used = CASE
        WHEN EXCLUDED.rate_limit_group IS NULL
          AND EXCLUDED.rate_limit_limit IS NULL
          AND EXCLUDED.rate_limit_remaining IS NULL
          AND EXCLUDED.rate_limit_used IS NULL
            THEN esi_collection_limiter_state.rate_limit_used
        WHEN esi_collection_limiter_state.pacing_active AND (
        EXCLUDED.next_request_at IS NULL OR (
            EXCLUDED.rate_limit_group IS DISTINCT FROM esi_collection_limiter_state.rate_limit_group
            AND esi_collection_limiter_state.next_request_at >= EXCLUDED.next_request_at
        )
    ) THEN esi_collection_limiter_state.rate_limit_used ELSE COALESCE(EXCLUDED.rate_limit_used, esi_collection_limiter_state.rate_limit_used) END,
    next_request_at = CASE
        WHEN EXCLUDED.rate_limit_group IS NULL
          AND EXCLUDED.rate_limit_limit IS NULL
          AND EXCLUDED.rate_limit_remaining IS NULL
          AND EXCLUDED.rate_limit_used IS NULL
            THEN esi_collection_limiter_state.next_request_at
        WHEN esi_collection_limiter_state.pacing_active AND (
            EXCLUDED.next_request_at IS NULL
            OR (
                EXCLUDED.rate_limit_group IS DISTINCT FROM esi_collection_limiter_state.rate_limit_group
                AND esi_collection_limiter_state.next_request_at >= EXCLUDED.next_request_at
            )
        ) THEN esi_collection_limiter_state.next_request_at
        WHEN EXCLUDED.next_request_at IS NULL THEN NULL
        WHEN esi_collection_limiter_state.next_request_at IS NULL
          OR esi_collection_limiter_state.next_request_at < EXCLUDED.next_request_at
            THEN EXCLUDED.next_request_at
        ELSE esi_collection_limiter_state.next_request_at
    END,
    pacing_active = CASE
        WHEN EXCLUDED.rate_limit_group IS NULL
          AND EXCLUDED.rate_limit_limit IS NULL
          AND EXCLUDED.rate_limit_remaining IS NULL
          AND EXCLUDED.rate_limit_used IS NULL
            THEN esi_collection_limiter_state.pacing_active
        WHEN esi_collection_limiter_state.pacing_active AND (
        EXCLUDED.next_request_at IS NULL OR (
            EXCLUDED.rate_limit_group IS DISTINCT FROM esi_collection_limiter_state.rate_limit_group
            AND esi_collection_limiter_state.next_request_at >= EXCLUDED.next_request_at
        )
    ) THEN TRUE ELSE EXCLUDED.pacing_active END,
    updated_at = GREATEST(esi_collection_limiter_state.updated_at, EXCLUDED.updated_at)
"#,
        )
            .bind(pause_until)
            .bind(metadata.error_limit_remain)
            .bind(metadata.error_limit_reset)
            .bind(&metadata.rate_limit_group)
            .bind(&metadata.rate_limit_limit)
            .bind(metadata.rate_limit_remaining)
            .bind(metadata.rate_limit_used)
            .bind(next_request_at)
            .bind(pacing_active)
            .bind(observed_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn reserve_esi_request(
        &self,
        requested_at: DateTime<Utc>,
    ) -> Result<EsiRequestAdmission, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let limiter = sqlx::query("SELECT pause_until, next_request_at, rate_limit_limit, rate_limit_remaining, pacing_active FROM esi_collection_limiter_state WHERE limiter_scope = TRUE FOR UPDATE")
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(limiter) = limiter else {
            transaction.commit().await?;
            return Ok(EsiRequestAdmission::Granted);
        };
        let pause_until: Option<DateTime<Utc>> = limiter.get("pause_until");
        if let Some(pause_until) = pause_until.filter(|deadline| *deadline > requested_at) {
            transaction.commit().await?;
            return Ok(EsiRequestAdmission::PausedUntil(pause_until));
        }
        let next_request_at: Option<DateTime<Utc>> = limiter.get("next_request_at");
        if let Some(next_request_at) = next_request_at.filter(|deadline| *deadline > requested_at) {
            transaction.commit().await?;
            return Ok(EsiRequestAdmission::WaitUntil(next_request_at));
        }
        let rate_limit: Option<String> = limiter.get("rate_limit_limit");
        let remaining: Option<i64> = limiter.get("rate_limit_remaining");
        let pacing_active: bool = limiter.get("pacing_active");
        if pacing_active && remaining.is_some_and(|remaining| remaining <= 0) {
            sqlx::query("UPDATE esi_collection_limiter_state SET rate_limit_group = NULL, rate_limit_limit = NULL, rate_limit_remaining = NULL, rate_limit_used = NULL, next_request_at = NULL, pacing_active = FALSE, updated_at = $1 WHERE limiter_scope = TRUE")
                .bind(requested_at)
                .execute(&mut *transaction)
                .await?;
        } else if let (Some(rate_limit), Some(remaining)) = (rate_limit, remaining) {
            let reserved_remaining = remaining.saturating_sub(1);
            if let Some(next_request_at) =
                rate_limit_pacing_deadline(&rate_limit, reserved_remaining, requested_at)
            {
                sqlx::query("UPDATE esi_collection_limiter_state SET rate_limit_remaining = $1, next_request_at = $2, pacing_active = TRUE, updated_at = $3 WHERE limiter_scope = TRUE")
                    .bind(reserved_remaining)
                    .bind(next_request_at)
                    .bind(requested_at)
                    .execute(&mut *transaction)
                    .await?;
            }
        }
        transaction.commit().await?;
        Ok(EsiRequestAdmission::Granted)
    }

    async fn record_complete(
        &self,
        region_id: i64,
        observation: CompleteRegionalObservation<'_>,
        recovery_gap: ChronoDuration,
    ) -> Result<RecordComplete, sqlx::Error> {
        let CompleteRegionalObservation {
            evidence,
            summaries,
            resolved_contracts,
            manifest_failures,
        } = observation;
        let expected_pages = evidence.expected_pages.ok_or_else(|| {
            sqlx::Error::Protocol(
                "complete regional observation omitted expected pages".to_string(),
            )
        })?;
        let mut transaction = self.pool.begin().await?;
        let now = Utc::now();
        sqlx::query("INSERT INTO regional_collection_metadata (region_id) VALUES ($1) ON CONFLICT (region_id) DO NOTHING").bind(region_id).execute(&mut *transaction).await?;
        let regional_metadata = sqlx::query(
            "SELECT baseline_at, current_recovery_epoch_id, requires_silent_baseline FROM regional_collection_metadata WHERE region_id = $1 FOR UPDATE",
        )
        .bind(region_id)
        .fetch_one(&mut *transaction)
        .await?;
        let baseline_at: Option<DateTime<Utc>> = regional_metadata.get("baseline_at");
        let current_recovery_epoch_id: Option<i64> =
            regional_metadata.get("current_recovery_epoch_id");
        let requires_silent_baseline: bool = regional_metadata.get("requires_silent_baseline");
        let epoch_last_complete_at = if let Some(recovery_epoch_id) = current_recovery_epoch_id {
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT max(completed_at) FROM regional_observation_batches WHERE region_id = $1 AND recovery_epoch_id = $2 AND outcome = 'complete'",
            )
            .bind(region_id)
            .bind(recovery_epoch_id)
            .fetch_one(&mut *transaction)
            .await?
        } else {
            None
        };
        let epoch_initial_complete_at = if let Some(recovery_epoch_id) = current_recovery_epoch_id {
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT initial_complete_at FROM regional_recovery_epochs WHERE id = $1",
            )
            .bind(recovery_epoch_id)
            .fetch_one(&mut *transaction)
            .await?
        } else {
            None
        };
        let continuity_complete_at = epoch_last_complete_at.or(epoch_initial_complete_at);
        let recovery_baseline = baseline_at.is_some()
            && (requires_silent_baseline
                || current_recovery_epoch_id.is_none()
                || continuity_complete_at.is_some_and(|complete_at| {
                    now.signed_duration_since(complete_at) > recovery_gap
                }));
        let recovery_epoch_id = match (current_recovery_epoch_id, recovery_baseline) {
            (Some(recovery_epoch_id), false) => recovery_epoch_id,
            _ => {
                let initialization_source = if baseline_at.is_none() {
                    "initial_baseline"
                } else if requires_silent_baseline || current_recovery_epoch_id.is_none() {
                    "migration_silent_baseline"
                } else {
                    "recovery_gap"
                };
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO regional_recovery_epochs (region_id, started_at, initialization_source) VALUES ($1,$2,$3) RETURNING id",
                )
                .bind(region_id)
                .bind(now)
                .bind(initialization_source)
                .fetch_one(&mut *transaction)
                .await?
            }
        };
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
        for summary in summaries {
            sqlx::query("INSERT INTO regional_epoch_contract_presence (recovery_epoch_id, region_id, contract_id, last_observed_at) VALUES ($1,$2,$3,$4) ON CONFLICT (recovery_epoch_id, region_id, contract_id) DO UPDATE SET last_observed_at = EXCLUDED.last_observed_at")
                .bind(recovery_epoch_id)
                .bind(region_id)
                .bind(summary.contract.contract_id)
                .bind(now)
                .execute(&mut *transaction)
                .await?;
        }
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
        let observed_in_current_epoch = sqlx::query_scalar::<_, i64>(
            "SELECT contract_id FROM regional_epoch_contract_presence WHERE recovery_epoch_id = $1 AND region_id = $2",
        )
        .bind(recovery_epoch_id)
        .bind(region_id)
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
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
            let observed_in_epoch = observed_in_current_epoch.contains(&contract_id);
            if !recovery_baseline && !observed_in_epoch {
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
            if inserted.is_some() && observed_in_epoch {
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
        let retry_after = manifest_failures
            .iter()
            .filter_map(|failure| failure.retry_after)
            .max();
        let failure_classification =
            (!manifest_failures.is_empty()).then_some("manifest_collection");
        let failure_detail = (!manifest_failures.is_empty()).then(|| {
            let first_detail = manifest_failures
                .first()
                .map(|failure| sanitize_contract_failure_detail(&failure.detail))
                .unwrap_or_default();
            format!(
                "{} manifest failures; first: {first_detail}",
                manifest_failures.len()
            )
        });
        sqlx::query("INSERT INTO regional_observation_batches (region_id, recovery_epoch_id, attempt_started_at, completed_at, outcome, expected_pages, pages_attempted, pages_observed, observed_contract_count, resolved_contract_count, manifest_failure_count, consistency_evidence, failure_classification, failure_detail, retry_after) VALUES ($1,$2,$3,$4,'complete',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(region_id)
            .bind(recovery_epoch_id)
            .bind(evidence.attempt_started_at)
            .bind(now)
            .bind(expected_pages as i32)
            .bind(evidence.pages_attempted as i32)
            .bind(evidence.pages_observed as i32)
            .bind(i64::try_from(evidence.observed_contract_count).unwrap_or(i64::MAX))
            .bind(i64::try_from(evidence.resolved_contract_count).unwrap_or(i64::MAX))
            .bind(i64::try_from(evidence.manifest_failure_count).unwrap_or(i64::MAX))
            .bind(evidence.consistency_evidence())
            .bind(failure_classification)
            .bind(failure_detail)
            .bind(retry_after)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE contract_collection_failures SET resolved_at = $2 WHERE region_id = $1 AND resource_key = $3 AND failure_kind = 'inconclusive_observation' AND resolved_at IS NULL AND classification IS NULL")
            .bind(region_id)
            .bind(now)
            .bind(regional_snapshot_resource_key(region_id))
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE regional_collection_metadata SET baseline_at = COALESCE(baseline_at, $2), complete_observations = complete_observations + 1, last_complete_at = $2, recovery_baseline_at = CASE WHEN $3 THEN $2 ELSE recovery_baseline_at END, current_recovery_epoch_id = $4, requires_silent_baseline = FALSE WHERE region_id = $1")
            .bind(region_id)
            .bind(now)
            .bind(recovery_baseline)
            .bind(recovery_epoch_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(RecordComplete {
            baseline_established: baseline_at.is_none(),
            recovery_baseline,
            observed_contracts: summaries.len(),
            newly_observed,
            observed: resolved_contracts.to_vec(),
            retry_after,
        })
    }

    async fn record_inconclusive(
        &self,
        region_id: i64,
        evidence: &RegionalAttemptEvidence,
        failure_classification: &str,
        detail: &str,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let now = Utc::now();
        let recovery_epoch_id = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT current_recovery_epoch_id FROM regional_collection_metadata WHERE region_id = $1",
        )
        .bind(region_id)
        .fetch_optional(&mut *transaction)
        .await?
        .flatten();
        let detail = sanitize_contract_failure_detail(detail);
        sqlx::query("INSERT INTO regional_observations (region_id, observed_at, outcome, detail) VALUES ($1,$2,'inconclusive',$3)")
            .bind(region_id)
            .bind(now)
            .bind(&detail)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO regional_observation_batches (region_id, recovery_epoch_id, attempt_started_at, completed_at, outcome, expected_pages, pages_attempted, pages_observed, observed_contract_count, resolved_contract_count, manifest_failure_count, consistency_evidence, failure_classification, failure_detail, retry_after) VALUES ($1,$2,$3,$4,'inconclusive',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(region_id)
            .bind(recovery_epoch_id)
            .bind(evidence.attempt_started_at)
            .bind(now)
            .bind(evidence.expected_pages.map(|pages| pages as i32))
            .bind(evidence.pages_attempted as i32)
            .bind(evidence.pages_observed as i32)
            .bind(i64::try_from(evidence.observed_contract_count).unwrap_or(i64::MAX))
            .bind(i64::try_from(evidence.resolved_contract_count).unwrap_or(i64::MAX))
            .bind(i64::try_from(evidence.manifest_failure_count).unwrap_or(i64::MAX))
            .bind(evidence.consistency_evidence())
            .bind(failure_classification)
            .bind(&detail)
            .bind(retry_after)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail, retry_after) VALUES ($1,$2,$3,$4,$5,$6,$7)")
            .bind(Some(region_id))
            .bind(Option::<i64>::None)
            .bind(Some(regional_snapshot_resource_key(region_id)))
            .bind(now)
            .bind("inconclusive_observation")
            .bind(&detail)
            .bind(retry_after)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await
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

    async fn public_location_evidence_retry_is_due(
        &self,
        region_id: i64,
        contract_id: i64,
        now: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let (failure_count, retry_after): (i64, Option<DateTime<Utc>>) = sqlx::query_as(
            "SELECT count(*), max(retry_after) FROM contract_collection_failures WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'embed_context_enrichment' AND resolved_at IS NULL AND classification IS NULL",
        )
        .bind(region_id)
        .bind(contract_id)
        .bind(public_location_evidence_resource_key(contract_id))
        .fetch_one(&self.pool)
        .await?;
        Ok(failure_count > 0 && retry_after.is_none_or(|retry_after| retry_after <= now))
    }

    async fn record_public_location_evidence_failure(
        &self,
        region_id: i64,
        contract_id: i64,
        detail: &str,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        let resource_key = public_location_evidence_resource_key(contract_id);
        let mut transaction = self.pool.begin().await?;
        let previous_failures: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM contract_collection_failures WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'embed_context_enrichment' AND resolved_at IS NULL AND classification IS NULL",
        )
        .bind(region_id)
        .bind(contract_id)
        .bind(&resource_key)
        .fetch_one(&mut *transaction)
        .await?;
        let shift = u32::try_from(previous_failures.clamp(0, 4)).unwrap_or(4);
        let delay_seconds = PUBLIC_LOCATION_EVIDENCE_RETRY_BASE_SECONDS
            .saturating_mul(1_i64 << shift)
            .min(PUBLIC_LOCATION_EVIDENCE_RETRY_MAX_SECONDS);
        let fallback_retry_at = now + ChronoDuration::seconds(delay_seconds);
        let next_retry_at = retry_after
            .map(|deadline| deadline.max(fallback_retry_at))
            .unwrap_or(fallback_retry_at);
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail, retry_after) VALUES ($1,$2,$3,$4,'embed_context_enrichment',$5,$6)")
            .bind(region_id)
            .bind(contract_id)
            .bind(&resource_key)
            .bind(now)
            .bind(sanitize_contract_failure_detail(detail))
            .bind(next_retry_at)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await
    }

    async fn record_resolution_probe_failure(
        &self,
        resolution: &AwaitingContractResolution,
        detail: &str,
        retry_after: Option<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let resource_key = format!("contracts/public/items/{}", resolution.contract_id);
        let mut transaction = self.pool.begin().await?;
        let previous_failures: i64 = sqlx::query_scalar("SELECT count(*) FROM contract_collection_failures WHERE region_id = $1 AND contract_id = $2 AND resource_key = $3 AND failure_kind = 'resolution_probe' AND resolved_at IS NULL AND classification IS NULL")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(&resource_key)
            .fetch_one(&mut *transaction)
            .await?;
        let shift = u32::try_from(previous_failures.clamp(0, 5)).unwrap_or(5);
        let delay_seconds = RESOLUTION_PROBE_RETRY_BASE_SECONDS
            .saturating_mul(1_i64 << shift)
            .min(RESOLUTION_PROBE_RETRY_MAX_SECONDS);
        let fallback_retry_at = Utc::now() + ChronoDuration::seconds(delay_seconds);
        let next_probe_at = retry_after
            .map(|deadline| std::cmp::max(deadline, fallback_retry_at))
            .unwrap_or(fallback_retry_at);
        sqlx::query("INSERT INTO contract_collection_failures (region_id, contract_id, resource_key, observed_at, failure_kind, detail, retry_after) VALUES ($1,$2,$3,$4,'resolution_probe',$5,$6)")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(&resource_key)
            .bind(Utc::now())
            .bind(sanitize_contract_failure_detail(detail))
            .bind(retry_after)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE contract_resolution_cases SET next_probe_at = GREATEST(next_probe_at, $3), updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution'")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(next_probe_at)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await
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

    async fn awaiting_resolution_cases_for_region(
        &self,
        region_id: i64,
        limit: usize,
    ) -> Result<Vec<AwaitingContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, suppresses_nonfinancial_notification FROM contract_resolution_cases WHERE state = 'awaiting_resolution' AND next_probe_at <= now() AND region_id = $1 ORDER BY contract_id LIMIT $2")
            .bind(region_id)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
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

    async fn awaiting_resolution_cases_excluding_regions(
        &self,
        excluded_region_ids: &[i64],
    ) -> Result<Vec<AwaitingContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, suppresses_nonfinancial_notification FROM contract_resolution_cases WHERE state = 'awaiting_resolution' AND next_probe_at <= now() AND NOT (region_id = ANY($1)) ORDER BY region_id, contract_id")
            .bind(excluded_region_ids)
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
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, acceptance_evidence_at, acceptance_provenance, state FROM contract_resolution_cases WHERE state <> 'awaiting_resolution' AND notification_pending = TRUE ORDER BY region_id, contract_id")
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
                    acceptance_provenance: contract_acceptance_provenance_from_optional_str(
                        row.get::<Option<String>, _>("acceptance_provenance").as_deref(),
                    )?,
                    state: contract_resolution_state_from_str(&row.get::<String, _>("state"))?,
                })
            })
            .collect()
    }

    async fn pending_terminal_notifications_for_region(
        &self,
        region_id: i64,
    ) -> Result<Vec<TerminalContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, acceptance_evidence_at, acceptance_provenance, state FROM contract_resolution_cases WHERE state <> 'awaiting_resolution' AND notification_pending = TRUE AND region_id = $1 ORDER BY contract_id")
            .bind(region_id)
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
                    acceptance_provenance: contract_acceptance_provenance_from_optional_str(
                        row.get::<Option<String>, _>("acceptance_provenance").as_deref(),
                    )?,
                    state: contract_resolution_state_from_str(&row.get::<String, _>("state"))?,
                })
            })
            .collect()
    }

    async fn pending_terminal_notifications_excluding_regions(
        &self,
        excluded_region_ids: &[i64],
    ) -> Result<Vec<TerminalContractResolution>, sqlx::Error> {
        sqlx::query("SELECT region_id, contract_id, contract, manifest, last_public_observed_at, absence_observed_at, acceptance_evidence_at, acceptance_provenance, state FROM contract_resolution_cases WHERE state <> 'awaiting_resolution' AND notification_pending = TRUE AND NOT (region_id = ANY($1)) ORDER BY region_id, contract_id")
            .bind(excluded_region_ids)
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
                    acceptance_provenance: contract_acceptance_provenance_from_optional_str(
                        row.get::<Option<String>, _>("acceptance_provenance").as_deref(),
                    )?,
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

    async fn resolve_terminal_resolution_failures(
        &self,
        region_id: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE contract_collection_failures AS failure SET resolved_at = now() FROM contract_resolution_cases AS resolution WHERE resolution.state <> 'awaiting_resolution' AND failure.region_id = resolution.region_id AND failure.contract_id = resolution.contract_id AND failure.resource_key = 'contracts/public/items/' || resolution.contract_id::text AND failure.failure_kind = 'resolution_probe' AND failure.resolved_at IS NULL AND failure.classification IS NULL AND ($1::bigint IS NULL OR resolution.region_id = $1)")
            .bind(region_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn resolve_terminal_resolution_failures_excluding_regions(
        &self,
        excluded_region_ids: &[i64],
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE contract_collection_failures AS failure SET resolved_at = now() FROM contract_resolution_cases AS resolution WHERE resolution.state <> 'awaiting_resolution' AND failure.region_id = resolution.region_id AND failure.contract_id = resolution.contract_id AND failure.resource_key = 'contracts/public/items/' || resolution.contract_id::text AND failure.failure_kind = 'resolution_probe' AND failure.resolved_at IS NULL AND failure.classification IS NULL AND NOT (resolution.region_id = ANY($1))")
            .bind(excluded_region_ids)
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
        let confirmed = sqlx::query_scalar::<_, i64>("UPDATE contract_resolution_cases SET state = 'acceptance_confirmed', acceptance_evidence_at = $3, acceptance_response_metadata = $4, acceptance_provenance = 'accepted_by_player', notification_pending = $5, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution' RETURNING contract_id")
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
                    "provenance": ContractAcceptanceProvenance::AcceptedByPlayer.as_str(),
                    "response": "403_accepted_by_player",
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
        provenance: Option<ContractAcceptanceProvenance>,
        metadata: Option<&CacheMetadata>,
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
        let resolved = sqlx::query_scalar::<_, i64>("UPDATE contract_resolution_cases SET state = $3, acceptance_provenance = $4, acceptance_response_metadata = $5, acceptance_evidence_at = $6, notification_pending = NOT suppresses_nonfinancial_notification, updated_at = now() WHERE region_id = $1 AND contract_id = $2 AND state = 'awaiting_resolution' RETURNING contract_id")
            .bind(resolution.region_id)
            .bind(resolution.contract_id)
            .bind(state_name)
            .bind(provenance.map(ContractAcceptanceProvenance::as_str))
            .bind(metadata.map(serde_json::to_value).transpose().map_err(json_to_sqlx)?)
            .bind(observed_at)
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
                    "evidence_response_at": observed_at,
                    "provenance": provenance.map(ContractAcceptanceProvenance::as_str),
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

pub async fn run_operator_delivery_cli_from_process_args(arguments: &[String]) -> Option<i32> {
    let (command, values) = arguments.split_first()?;
    if command != "contract-delivery" {
        return None;
    }
    let database_url = match std::env::var("CONTRACT_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("contract-delivery requires CONTRACT_DATABASE_URL");
            return Some(2);
        }
    };
    let store = match ContractCollectionStore::connect(&database_url).await {
        Ok(store) => store,
        Err(error) => {
            eprintln!("cannot open contract-delivery store: {error}");
            return Some(2);
        }
    };
    let values = values.iter().map(String::as_str).collect::<Vec<_>>();
    let actor = match std::env::var("CONTRACT_DELIVERY_OPERATOR_ID")
        .ok()
        .zip(std::env::var("CONTRACT_DELIVERY_OPERATOR_TOKEN_SHA256").ok())
        .map(|(actor, digest)| ContractDeliveryOperatorCapability::from_config(actor, &digest))
    {
        Some(Ok(capability)) => capability,
        _ => {
            eprintln!("contract-delivery requires configured operator capability");
            return Some(2);
        }
    };
    let mut token = Vec::with_capacity(CONTRACT_DELIVERY_OPERATOR_TOKEN_MAX_BYTES + 1);
    let mut stdin = std::io::stdin();
    if std::io::Read::take(
        std::io::Read::by_ref(&mut stdin),
        (CONTRACT_DELIVERY_OPERATOR_TOKEN_MAX_BYTES + 1) as u64,
    )
    .read_to_end(&mut token)
    .is_err()
        || token.len() > CONTRACT_DELIVERY_OPERATOR_TOKEN_MAX_BYTES
        || !actor.verify(token.strip_suffix(b"\n").unwrap_or(&token))
    {
        eprintln!("contract-delivery capability rejected");
        return Some(2);
    }
    if values.contains(&"--actor") {
        eprintln!("contract-delivery does not accept --actor");
        return Some(2);
    }
    let values = [values, vec!["--actor", actor.actor.as_str()]].concat();
    let region_names = if values.first().copied() == Some("rerender") && values.contains(&"--queue")
    {
        match configured_contract_region_names() {
            Ok(region_names) => region_names,
            Err(error) => {
                eprintln!("contract-delivery rerender cannot resolve region names: {error}");
                return Some(2);
            }
        }
    } else {
        HashMap::new()
    };
    let region_name_esi = if values.first().copied() == Some("rerender")
        && values.contains(&"--queue")
    {
        match HttpPublicContractEsi::new(Duration::from_secs(15)) {
            Ok(esi) => Some(esi),
            Err(error) => {
                eprintln!("contract-delivery rerender cannot create public ESI client: {error}");
                return Some(2);
            }
        }
    } else {
        None
    };
    match execute_operator_delivery_cli_with_region_name_lookup(
        &store,
        &values,
        Utc::now(),
        &region_names,
        region_name_esi
            .as_ref()
            .map(|esi| esi as &dyn PublicContractEsi),
    )
    .await
    {
        Ok(result) => match serde_json::to_string(&result) {
            Ok(output) => {
                println!("{output}");
                Some(0)
            }
            Err(error) => {
                eprintln!("cannot render contract-delivery result: {error}");
                Some(2)
            }
        },
        Err(error) => {
            eprintln!("contract-delivery command failed: {error}");
            Some(2)
        }
    }
}

pub async fn execute_operator_delivery_cli(
    store: &ContractCollectionStore,
    arguments: &[&str],
    now: DateTime<Utc>,
) -> Result<OperatorDeliveryCliResult, String> {
    execute_operator_delivery_cli_with_region_name_lookup(
        store,
        arguments,
        now,
        &HashMap::new(),
        None,
    )
    .await
}

#[doc(hidden)]
pub async fn execute_operator_delivery_cli_with_region_name_lookup(
    store: &ContractCollectionStore,
    arguments: &[&str],
    now: DateTime<Utc>,
    region_names: &HashMap<i64, String>,
    region_name_esi: Option<&dyn PublicContractEsi>,
) -> Result<OperatorDeliveryCliResult, String> {
    execute_operator_delivery_cli_with_region_name_lookup_inner(
        store,
        arguments,
        now,
        region_names,
        region_name_esi,
        None,
    )
    .await
}

#[doc(hidden)]
pub async fn execute_operator_delivery_cli_with_region_name_lookup_and_ship_groups(
    store: &ContractCollectionStore,
    arguments: &[&str],
    now: DateTime<Utc>,
    region_names: &HashMap<i64, String>,
    region_name_esi: Option<&dyn PublicContractEsi>,
    ship_groups: &dyn ShipGroupResolver,
) -> Result<OperatorDeliveryCliResult, String> {
    execute_operator_delivery_cli_with_region_name_lookup_inner(
        store,
        arguments,
        now,
        region_names,
        region_name_esi,
        Some(ship_groups),
    )
    .await
}

async fn execute_operator_delivery_cli_with_region_name_lookup_inner(
    store: &ContractCollectionStore,
    arguments: &[&str],
    now: DateTime<Utc>,
    region_names: &HashMap<i64, String>,
    region_name_esi: Option<&dyn PublicContractEsi>,
    supplied_ship_groups: Option<&dyn ShipGroupResolver>,
) -> Result<OperatorDeliveryCliResult, String> {
    let (command, flags) = parse_operator_delivery_command(arguments)?;
    let actor = required_operator_delivery_option(&flags, "--actor")?;
    if actor != CONTRACT_DELIVERY_OPERATOR_ACTOR {
        return Err("delivery requeue actor is not authorized".to_string());
    }
    match command {
        "inspect" | "requeue" => {
            reject_unknown_operator_delivery_options(&flags, &["--id", "--actor"])?;
            let delivery_id = required_positive_operator_delivery_id(&flags, "--id")?;
            if command == "inspect" {
                store
                    .inspect_contract_delivery(delivery_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .map(OperatorDeliveryCliResult::Inspected)
                    .ok_or_else(|| format!("contract delivery {delivery_id} does not exist"))
            } else {
                store
                    .requeue_permanent_contract_delivery(delivery_id, actor, now)
                    .await
                    .map(OperatorDeliveryCliResult::Requeued)
                    .map_err(|error| error.to_string())
            }
        }
        "rerender" => {
            reject_unknown_operator_delivery_options(
                &flags,
                &[
                    "--channel",
                    "--limit",
                    "--delivery-ids",
                    "--dry-run",
                    "--queue",
                    "--actor",
                ],
            )?;
            let dry_run = flags.contains_key("--dry-run");
            let queue = flags.contains_key("--queue");
            if dry_run == queue {
                return Err("rerender requires exactly one of --dry-run or --queue".to_string());
            }
            let channel_id = required_positive_operator_delivery_id(&flags, "--channel")? as u64;
            let selector = if flags.contains_key("--delivery-ids") {
                if flags.contains_key("--limit") {
                    return Err("--delivery-ids conflicts with --limit".to_string());
                }
                ContractDeliveryRerenderSelector::DeliveryIds(required_operator_delivery_ids(
                    &flags,
                    "--delivery-ids",
                )?)
            } else {
                ContractDeliveryRerenderSelector::Limit(required_operator_delivery_limit(&flags)?)
            };
            let selection = if dry_run {
                store
                    .rerender_contract_delivery_candidates(channel_id, &selector)
                    .await
                    .map_err(|error| error.to_string())?
            } else if let Some(ship_groups) = supplied_ship_groups {
                store
                    .queue_contract_delivery_rerenders(
                        channel_id,
                        &selector,
                        actor,
                        now,
                        region_names,
                        region_name_esi,
                        ship_groups,
                    )
                    .await
                    .map_err(|error| error.to_string())?
            } else {
                let ship_groups =
                    CachedShipGroupResolver::new(crate::config::load_ships().map_err(|error| {
                        format!("load cached ship groups for rerender: {error}")
                    })?);
                store
                    .queue_contract_delivery_rerenders(
                        channel_id,
                        &selector,
                        actor,
                        now,
                        region_names,
                        region_name_esi,
                        &ship_groups,
                    )
                    .await
                    .map_err(|error| error.to_string())?
            };
            let excluded_count = selection.excluded.len();
            let candidates = selection
                .candidates
                .into_iter()
                .map(|candidate| candidate.candidate)
                .collect::<Vec<_>>();
            Ok(OperatorDeliveryCliResult::Rerendered(
                ContractDeliveryRerenderReport {
                    channel_id,
                    dry_run,
                    eligible_count: candidates.len(),
                    candidates,
                    excluded_count,
                    excluded: selection.excluded,
                },
            ))
        }
        _ => Err("contract-delivery command must be inspect, requeue, or rerender".to_string()),
    }
}

fn parse_operator_delivery_command<'a>(
    arguments: &'a [&'a str],
) -> Result<(&'a str, BTreeMap<&'a str, Option<&'a str>>), String> {
    let Some((command, values)) = arguments.split_first() else {
        return Err("contract-delivery command is required".to_string());
    };
    let mut flags = BTreeMap::new();
    let mut values = values.iter().peekable();
    while let Some(flag) = values.next() {
        if !flag.starts_with("--") || flags.contains_key(flag) {
            return Err(format!(
                "invalid or duplicate contract-delivery option: {flag}"
            ));
        }
        let value = match values.peek() {
            Some(value) if !value.starts_with("--") => values.next().copied(),
            _ => None,
        };
        flags.insert(*flag, value);
    }
    Ok((command, flags))
}

fn reject_unknown_operator_delivery_options(
    flags: &BTreeMap<&str, Option<&str>>,
    allowed: &[&str],
) -> Result<(), String> {
    flags
        .keys()
        .find(|flag| !allowed.contains(flag))
        .map(|flag| Err(format!("unknown contract-delivery option: {flag}")))
        .unwrap_or(Ok(()))
}

fn required_operator_delivery_option<'a>(
    flags: &BTreeMap<&str, Option<&'a str>>,
    name: &str,
) -> Result<&'a str, String> {
    flags
        .get(name)
        .copied()
        .flatten()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

fn required_positive_operator_delivery_id(
    flags: &BTreeMap<&str, Option<&str>>,
    name: &str,
) -> Result<i64, String> {
    let value = required_operator_delivery_option(flags, name)?
        .parse::<i64>()
        .map_err(|_| format!("{name} must be an integer"))?;
    if value <= 0 {
        return Err(format!("{name} must be a positive integer"));
    }
    Ok(value)
}

fn required_operator_delivery_limit(flags: &BTreeMap<&str, Option<&str>>) -> Result<usize, String> {
    let limit = required_operator_delivery_option(flags, "--limit")?
        .parse::<usize>()
        .map_err(|_| {
            format!(
                "--limit must be an integer between 1 and {CONTRACT_DELIVERY_RERENDER_MAX_LIMIT}"
            )
        })?;
    if !(1..=CONTRACT_DELIVERY_RERENDER_MAX_LIMIT).contains(&limit) {
        return Err(format!(
            "--limit must be an integer between 1 and {CONTRACT_DELIVERY_RERENDER_MAX_LIMIT}"
        ));
    }
    Ok(limit)
}

fn required_operator_delivery_ids(
    flags: &BTreeMap<&str, Option<&str>>,
    name: &str,
) -> Result<Vec<i64>, String> {
    let values = required_operator_delivery_option(flags, name)?
        .split(',')
        .map(str::trim)
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| format!("{name} must be comma-separated positive integers"))
                .and_then(|value| {
                    (value > 0)
                        .then_some(value)
                        .ok_or_else(|| format!("{name} must be comma-separated positive integers"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.len() > CONTRACT_DELIVERY_RERENDER_MAX_LIMIT {
        return Err(format!(
            "{name} must contain between 1 and {CONTRACT_DELIVERY_RERENDER_MAX_LIMIT} IDs"
        ));
    }
    let unique = values.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != values.len() {
        return Err(format!("{name} must not contain duplicate IDs"));
    }
    Ok(values)
}

fn contract_delivery_rerender_row_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ContractDeliveryRerenderRow, sqlx::Error> {
    let delivery_id: i64 = row.get("id");
    let excluded = |reason: &str| {
        ContractDeliveryRerenderRow::Excluded(ContractDeliveryRerenderExclusion {
            delivery_id,
            reason: reason.to_string(),
        })
    };
    let repair_status: String = row.get("repair_status");
    let repair_failure_kind: Option<String> = row.get("repair_failure_kind");
    let failure_kind: Option<String> = row.get("failure_kind");
    if repair_status == "permanent"
        || repair_failure_kind.as_deref() == Some("permanent")
        || failure_kind.as_deref() == Some("permanent")
    {
        return Ok(excluded("permanent_failure"));
    }
    if row
        .get::<Option<DateTime<Utc>>, _>("retained_subscription_deleted_at")
        .is_some()
    {
        return Ok(excluded("retired_subscription"));
    }
    match row.get::<String, _>("status").as_str() {
        "sent" => {}
        "prepared" => return Ok(excluded("prepared_delivery")),
        _ => return Ok(excluded("unsent_delivery")),
    }
    let Some(discord_message_id) = row.get::<Option<String>, _>("discord_message_id") else {
        return Ok(excluded("missing_discord_message_identity"));
    };
    if parse_contract_discord_message_id(&discord_message_id).is_err() {
        return Ok(excluded("invalid_discord_message_identity"));
    }
    if repair_status != "none" {
        return Ok(excluded("active_repair"));
    }
    let Some(guild_id) = row.get::<Option<i64>, _>("retained_subscription_guild_id") else {
        return Ok(excluded("missing_retained_subscription"));
    };
    let event_kind = contract_delivery_event_kind_from_str(&row.get::<String, _>("event_kind"));
    let event = serde_json::from_value::<ContractEvent>(row.get("event"));
    let stored_message = serde_json::from_value::<ContractNotificationMessage>(row.get("message"));
    let subscription: Result<ContractSubscription, sqlx::Error> = (|| {
        Ok(ContractSubscription {
            guild_id: guild_id as u64,
            channel_id: row
                .get::<Option<i64>, _>("retained_subscription_channel_id")
                .ok_or_else(|| {
                    sqlx::Error::Protocol("retained subscription channel is missing".to_string())
                })? as u64,
            id: row
                .get::<Option<String>, _>("retained_subscription_id")
                .ok_or_else(|| {
                    sqlx::Error::Protocol("retained subscription ID is missing".to_string())
                })?,
            description: row
                .get::<Option<String>, _>("retained_subscription_description")
                .ok_or_else(|| {
                    sqlx::Error::Protocol(
                        "retained subscription description is missing".to_string(),
                    )
                })?,
            filter: serde_json::from_value(
                row.get::<Option<Value>, _>("retained_subscription_filter")
                    .ok_or_else(|| {
                        sqlx::Error::Protocol("retained subscription filter is missing".to_string())
                    })?,
            )
            .map_err(json_to_sqlx)?,
            event_actions: serde_json::from_value(
                row.get::<Option<Value>, _>("retained_subscription_event_actions")
                    .ok_or_else(|| {
                        sqlx::Error::Protocol(
                            "retained subscription event actions are missing".to_string(),
                        )
                    })?,
            )
            .map_err(json_to_sqlx)?,
        })
    })()
    .and_then(|subscription| {
        subscription
            .validate()
            .map(|()| subscription)
            .map_err(sqlx::Error::Protocol)
    });
    let contract_id: i64 = row.get("contract_id");
    let malformed_reason = match (&event_kind, &event, &stored_message, &subscription) {
        (Err(_), _, _, _) => Some("invalid_delivery_event_kind"),
        (_, Err(_), _, _) => Some("invalid_retained_event"),
        (_, _, Err(_), _) => Some("invalid_retained_message"),
        (_, _, _, Err(_)) => Some("invalid_retained_subscription"),
        (Ok(_), Ok(event), Ok(_), Ok(_)) if event.contract.contract_id != contract_id => {
            Some("retained_event_identity_mismatch")
        }
        (Ok(event_kind), Ok(event), Ok(_), Ok(_)) if *event_kind != event.kind => {
            Some("retained_event_kind_mismatch")
        }
        _ => None,
    };
    if let Some(reason) = malformed_reason {
        return Ok(excluded(reason));
    }
    Ok(ContractDeliveryRerenderRow::Candidate(
        ValidatedContractDeliveryRerenderCandidate {
            candidate: ContractDeliveryRerenderCandidate {
                delivery_id,
                contract_id,
                event_kind: event_kind.expect("validated event kind"),
                discord_message_id,
            },
            subscription: subscription.expect("validated retained subscription"),
            event: event.expect("validated retained event"),
            stored_message: stored_message.expect("validated retained message"),
        },
    ))
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
    let event_kind = contract_delivery_event_kind_from_str(&row.get::<String, _>("event_kind"))?;
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
        delivery_claim_token: row.try_get("delivery_claim_token").ok(),
    })
}

fn contract_message_edit_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ContractMessageEdit, sqlx::Error> {
    Ok(ContractMessageEdit {
        delivery_id: row.get("id"),
        channel_id: row.get::<i64, _>("channel_id") as u64,
        discord_message_id: row
            .get::<Option<String>, _>("discord_message_id")
            .ok_or_else(|| {
                sqlx::Error::Protocol(
                    "pending repair lacks an original Discord message ID".to_string(),
                )
            })?,
        contract_id: row.get("contract_id"),
        event_kind: contract_delivery_event_kind_from_str(&row.get::<String, _>("event_kind"))?,
        repair_revision: row.get("repair_revision"),
        repair_claim_token: row.get("repair_claim_token"),
        repair_attempt_count: row.get("repair_attempt_count"),
        message: serde_json::from_value(row.get("desired_message")).map_err(json_to_sqlx)?,
    })
}

fn contract_repair_retry_at(
    error: &ContractDeliveryError,
    attempted_at: DateTime<Utc>,
    attempt_count: i32,
) -> DateTime<Utc> {
    let exponent = (attempt_count.saturating_sub(1) as u32).min(16);
    let delay = CONTRACT_REPAIR_RETRY_BASE_SECONDS
        .saturating_mul(1_i64 << exponent)
        .min(CONTRACT_REPAIR_RETRY_MAX_SECONDS);
    let backoff = attempted_at + ChronoDuration::seconds(delay);
    error
        .retry_at()
        .map_or(backoff, |deadline| deadline.max(backoff))
}

fn contract_delivery_event_kind_from_str(value: &str) -> Result<ContractEventKind, sqlx::Error> {
    Ok(match value {
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
        acceptance_provenance: contract_acceptance_provenance_from_optional_str(
            row.get::<Option<String>, _>("acceptance_provenance")
                .as_deref(),
        )?,
    })
}

fn contract_acceptance_provenance_from_optional_str(
    value: Option<&str>,
) -> Result<Option<ContractAcceptanceProvenance>, sqlx::Error> {
    value
        .map(|value| match value {
            "accepted_by_player" => Ok(ContractAcceptanceProvenance::AcceptedByPlayer),
            "no_content" => Ok(ContractAcceptanceProvenance::NoContent),
            value => Err(sqlx::Error::Protocol(format!(
                "unknown contract acceptance provenance: {value}"
            ))),
        })
        .transpose()
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

struct CompleteRegionalObservation<'a> {
    evidence: &'a RegionalAttemptEvidence,
    summaries: &'a [SummaryObservedContract],
    resolved_contracts: &'a [ObservedContract],
    manifest_failures: &'a [ManifestCollectionFailure],
}

#[derive(Clone, Debug)]
struct RegionalAttemptEvidence {
    attempt_started_at: DateTime<Utc>,
    expected_pages: Option<u32>,
    pages_attempted: u32,
    pages_observed: u32,
    observed_contract_count: usize,
    resolved_contract_count: usize,
    manifest_failure_count: usize,
    first_page: Option<CacheMetadata>,
    last_page: Option<CacheMetadata>,
    expected_pages_matches_first: Option<bool>,
    last_modified_matches_first: Option<bool>,
    duplicate_contract_count: usize,
    failure_response: Option<CacheMetadata>,
}

impl RegionalAttemptEvidence {
    fn new(attempt_started_at: DateTime<Utc>) -> Self {
        Self {
            attempt_started_at,
            expected_pages: None,
            pages_attempted: 0,
            pages_observed: 0,
            observed_contract_count: 0,
            resolved_contract_count: 0,
            manifest_failure_count: 0,
            first_page: None,
            last_page: None,
            expected_pages_matches_first: None,
            last_modified_matches_first: None,
            duplicate_contract_count: 0,
            failure_response: None,
        }
    }

    fn capture_failure(&mut self, error: &ContractCollectionError) {
        if let ContractCollectionError::Esi(error) = error {
            self.failure_response = Some(error.metadata.clone());
        }
    }

    fn consistency_evidence(&self) -> Value {
        serde_json::json!({
            "pages_attempted": self.pages_attempted,
            "pages_observed": self.pages_observed,
            "expected_pages": self.expected_pages,
            "observed_summary_count": self.observed_contract_count,
            "resolved_contract_count": self.resolved_contract_count,
            "manifest_failure_count": self.manifest_failure_count,
            "first_page": self.first_page.as_ref().map(compact_cache_metadata),
            "last_page": self.last_page.as_ref().map(compact_cache_metadata),
            "failure_response": self.failure_response.as_ref().map(compact_cache_metadata),
            "consistency": {
                "expected_pages_matches_first": self.expected_pages_matches_first,
                "last_modified_matches_first": self.last_modified_matches_first,
                "duplicate_contract_count": self.duplicate_contract_count,
            },
        })
    }
}

#[derive(Debug)]
struct RegionalCollectionAttemptError {
    error: ContractCollectionError,
    evidence: RegionalAttemptEvidence,
}

fn compact_cache_metadata(metadata: &CacheMetadata) -> Value {
    serde_json::json!({
        "etag": metadata.etag,
        "last_modified": metadata.last_modified,
        "expires_at": metadata.expires_at,
        "expected_pages": metadata.expected_pages,
        "error_limit_remain": metadata.error_limit_remain,
        "error_limit_reset": metadata.error_limit_reset,
        "rate_limit_group": metadata.rate_limit_group,
        "rate_limit_limit": metadata.rate_limit_limit,
        "rate_limit_remaining": metadata.rate_limit_remaining,
        "rate_limit_used": metadata.rate_limit_used,
        "retry_after": metadata.retry_after,
    })
}

fn regional_attempt_error(
    error: ContractCollectionError,
    evidence: &mut RegionalAttemptEvidence,
) -> RegionalCollectionAttemptError {
    evidence.capture_failure(&error);
    RegionalCollectionAttemptError {
        error,
        evidence: evidence.clone(),
    }
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
    acceptance_provenance: Option<ContractAcceptanceProvenance>,
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

type RegionalAttemptTaskResult = (
    usize,
    i64,
    Result<RecordComplete, RegionalCollectionAttemptError>,
);

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

fn collection_failure_classification(error: &ContractCollectionError) -> &'static str {
    match error {
        ContractCollectionError::Esi(_) => "esi",
        ContractCollectionError::Cache(_) => "consistency",
        ContractCollectionError::Database(_) => "database",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<ContractAcceptanceProvenance>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CollectionReport {
    pub regions: Vec<CollectionOutcome>,
    pub events: Vec<ContractEvent>,
    pub retry_after: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct ContractCollector {
    store: ContractCollectionStore,
    esi: Arc<dyn PublicContractEsi>,
    region_names: Arc<HashMap<i64, String>>,
    structure_resolver: Option<Arc<dyn StructureResolver>>,
    notifications: Option<ContractNotifications>,
    delivery_clock: Arc<dyn ContractDeliveryClock>,
    request_pacer: Arc<dyn ContractRequestPacer>,
    recovery_gap: ChronoDuration,
    max_concurrent_regions: usize,
}

#[derive(Clone)]
struct ContractNotifications {
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
}

enum NotificationResolution {
    Complete,
    Deferred,
    ProximityUnverified,
    ProximityResolved,
    EvidenceChanged,
}

#[derive(Default)]
struct ResolutionBatch {
    events: Vec<ContractEvent>,
    retry_after: Option<DateTime<Utc>>,
}

struct CompletedRegionalPostprocessing {
    outcome: CollectionOutcome,
    events: Vec<ContractEvent>,
    consumed_embed_context_enrichments: usize,
    retry_after: Option<DateTime<Utc>>,
}

struct PersistedContractContextLimiter {
    store: ContractCollectionStore,
    request_pacer: Arc<dyn ContractRequestPacer>,
}

async fn wait_for_esi_request_admission(
    store: &ContractCollectionStore,
    request_pacer: &dyn ContractRequestPacer,
) -> Result<(), EsiError> {
    loop {
        match store
            .reserve_esi_request(request_pacer.now())
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))?
        {
            EsiRequestAdmission::Granted => return Ok(()),
            EsiRequestAdmission::WaitUntil(deadline) => request_pacer.wait_until(deadline).await,
            EsiRequestAdmission::PausedUntil(deadline) => {
                return Err(EsiError::retryable(
                    "persisted global ESI limiter boundary remains active",
                    Some(deadline),
                ));
            }
        }
    }
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

    fn permits_parallel_enrichment(&self) -> bool {
        true
    }

    async fn wait_for_request_admission(&self) -> Result<(), EsiError> {
        wait_for_esi_request_admission(&self.store, &*self.request_pacer).await
    }

    async fn record_response(&self, metadata: &CacheMetadata) -> Result<(), EsiError> {
        self.store
            .record_esi_limiter_at(metadata, self.request_pacer.now())
            .await
            .map_err(|error| EsiError::retryable(error.to_string(), None))
    }
}

impl ContractCollector {
    pub fn new(store: ContractCollectionStore, esi: Arc<dyn PublicContractEsi>) -> Self {
        Self {
            store,
            esi,
            region_names: Arc::new(HashMap::new()),
            structure_resolver: None,
            notifications: None,
            delivery_clock: Arc::new(SystemContractDeliveryClock),
            request_pacer: Arc::new(SystemContractRequestPacer),
            recovery_gap: DEFAULT_COLLECTION_RECOVERY_GAP,
            max_concurrent_regions: 1,
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

    pub fn with_structure_resolver(mut self, resolver: Arc<dyn StructureResolver>) -> Self {
        self.structure_resolver = Some(resolver);
        self
    }

    pub fn with_region_names(mut self, region_names: Arc<HashMap<i64, String>>) -> Self {
        self.region_names = region_names;
        self
    }

    fn with_optional_structure_resolver(
        mut self,
        resolver: Option<Arc<dyn StructureResolver>>,
    ) -> Self {
        self.structure_resolver = resolver;
        self
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

    pub fn with_request_pacer(mut self, request_pacer: Arc<dyn ContractRequestPacer>) -> Self {
        self.request_pacer = request_pacer;
        self
    }

    pub fn with_recovery_gap(mut self, recovery_gap: ChronoDuration) -> Self {
        self.recovery_gap = recovery_gap.max(ChronoDuration::zero());
        self
    }

    pub fn with_max_concurrent_regions(mut self, max_concurrent_regions: usize) -> Self {
        self.max_concurrent_regions =
            max_concurrent_regions.clamp(1, MAX_CONTRACT_REGIONAL_CONCURRENCY);
        self
    }

    fn start_regional_attempt(
        &self,
        attempts: &mut JoinSet<RegionalAttemptTaskResult>,
        region_index: usize,
        region_id: i64,
    ) {
        let collector = self.clone();
        attempts.spawn(async move {
            (
                region_index,
                region_id,
                collector.collect_region(region_id, Utc::now()).await,
            )
        });
    }

    async fn postprocess_completed_region(
        &self,
        region_id: i64,
        recorded: RecordComplete,
        remaining_embed_context_enrichments: usize,
    ) -> Result<CompletedRegionalPostprocessing, ContractCollectionError> {
        let observed_contracts = recorded.observed_contracts;
        let mut attempted_embed_enrichment_deadlines = HashMap::new();
        let consumed = self
            .snapshot_observed_contracts(
                region_id,
                &recorded.observed,
                remaining_embed_context_enrichments,
                &mut attempted_embed_enrichment_deadlines,
            )
            .await?;
        let mut events = recorded
            .newly_observed
            .into_iter()
            .map(|observed| ContractEvent {
                region_id,
                kind: ContractEventKind::Listed,
                contract: observed.contract,
                offered_items: observed.manifest.offered_items,
                requested_items: observed.manifest.requested_items,
                context: ContractObservationContext::default(),
                acceptance_evidence: None,
                embed_context: ContractEmbedContext::default(),
            })
            .collect::<Vec<_>>();
        if !events.is_empty() {
            self.notify_fresh_events_with_deadlines(&events, &attempted_embed_enrichment_deadlines)
                .await?;
        }
        let resolution_batch = self
            .resolve_awaiting_resolutions_for_region(region_id)
            .await?;
        events.extend(resolution_batch.events);
        let outcome = if recorded.baseline_established {
            CollectionOutcome::BaselineEstablished { region_id }
        } else if recorded.recovery_baseline {
            CollectionOutcome::RecoveryBaselineEstablished { region_id }
        } else {
            CollectionOutcome::Complete {
                region_id,
                observed_contracts,
            }
        };
        Ok(CompletedRegionalPostprocessing {
            outcome,
            events,
            consumed_embed_context_enrichments: consumed,
            retry_after: std::cmp::max(recorded.retry_after, resolution_batch.retry_after),
        })
    }

    pub async fn collect_cycle(&self) -> Result<CollectionReport, ContractCollectionError> {
        if let Some(notifications) = &self.notifications {
            self.deliver_prepared_notifications(notifications).await?;
        }
        self.notify(&[]).await?;
        if let Some(deadline) = self.store.active_esi_limiter_deadline().await? {
            return Err(EsiError::retryable(
                "persisted global ESI limiter boundary remains active",
                Some(deadline),
            )
            .into());
        }
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
        let region_order = regions
            .iter()
            .enumerate()
            .map(|(index, region_id)| (*region_id, index))
            .collect::<HashMap<_, _>>();
        let mut outcomes = (0..regions.len()).map(|_| None).collect::<Vec<_>>();
        let mut events = (0..regions.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut retry_after = self.store.active_esi_limiter_deadline().await?;
        let mut remaining_embed_context_enrichments = MAX_EMBED_CONTEXT_ENRICHMENTS_PER_CYCLE;
        let mut next_region = 0;
        let mut accepting_regions = true;
        let mut parent_error = None;
        let mut attempts = JoinSet::new();

        loop {
            while accepting_regions
                && next_region < regions.len()
                && attempts.len() < self.max_concurrent_regions
            {
                match self.store.active_esi_limiter_deadline().await {
                    Ok(Some(deadline)) => {
                        retry_after = std::cmp::max(retry_after, Some(deadline));
                        accepting_regions = false;
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        parent_error.get_or_insert_with(|| error.into());
                        accepting_regions = false;
                        break;
                    }
                }
                let region_index = next_region;
                let region_id = regions[region_index];
                next_region += 1;
                self.start_regional_attempt(&mut attempts, region_index, region_id);
            }

            let Some(joined) = attempts.join_next().await else {
                break;
            };
            let (region_index, region_id, result) = match joined {
                Ok(result) => result,
                Err(error) => {
                    parent_error.get_or_insert_with(|| {
                        ContractCollectionError::Cache(format!(
                            "started regional attempt task failed before a batch could be retained: {error}"
                        ))
                    });
                    accepting_regions = false;
                    continue;
                }
            };
            match result {
                Ok(recorded) => {
                    retry_after = std::cmp::max(retry_after, recorded.retry_after);
                    match self
                        .postprocess_completed_region(
                            region_id,
                            recorded,
                            remaining_embed_context_enrichments,
                        )
                        .await
                    {
                        Ok(postprocessed) => {
                            remaining_embed_context_enrichments =
                                remaining_embed_context_enrichments.saturating_sub(
                                    postprocessed.consumed_embed_context_enrichments,
                                );
                            retry_after = std::cmp::max(retry_after, postprocessed.retry_after);
                            outcomes[region_index] = Some(postprocessed.outcome);
                            events[region_index].extend(postprocessed.events);
                        }
                        Err(error) => {
                            parent_error.get_or_insert(error);
                            accepting_regions = false;
                        }
                    }
                }
                Err(error) => {
                    let failure_classification = collection_failure_classification(&error.error);
                    let detail = error.error.to_string();
                    let regional_retry_after = error.error.retry_after();
                    match self
                        .store
                        .record_inconclusive(
                            region_id,
                            &error.evidence,
                            failure_classification,
                            &detail,
                            regional_retry_after,
                        )
                        .await
                    {
                        Ok(()) => {
                            retry_after = std::cmp::max(retry_after, regional_retry_after);
                            outcomes[region_index] = Some(CollectionOutcome::Inconclusive {
                                region_id,
                                reason: detail,
                            });
                        }
                        Err(record_error) => {
                            parent_error.get_or_insert(record_error.into());
                            accepting_regions = false;
                        }
                    }
                }
            }
        }
        if let Some(error) = parent_error {
            return Err(error);
        }
        let completed_region_ids = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                Some(CollectionOutcome::BaselineEstablished { region_id })
                | Some(CollectionOutcome::RecoveryBaselineEstablished { region_id })
                | Some(CollectionOutcome::Complete { region_id, .. }) => Some(*region_id),
                Some(CollectionOutcome::Inconclusive { .. }) | None => None,
            })
            .collect::<Vec<_>>();
        let mut events = events.into_iter().flatten().collect::<Vec<_>>();
        let resolution_batch = if self.store.active_esi_limiter_deadline().await?.is_some() {
            ResolutionBatch::default()
        } else if completed_region_ids.is_empty() {
            self.resolve_awaiting_resolutions().await?
        } else {
            self.resolve_awaiting_resolutions_excluding_regions(&completed_region_ids)
                .await?
        };
        retry_after = std::cmp::max(retry_after, resolution_batch.retry_after);
        events.extend(resolution_batch.events);
        events.sort_by_key(|event| {
            region_order
                .get(&event.region_id)
                .copied()
                .unwrap_or(usize::MAX)
        });
        Ok(CollectionReport {
            regions: outcomes.into_iter().flatten().collect(),
            events,
            retry_after,
        })
    }

    async fn collect_region(
        &self,
        region_id: i64,
        attempt_started_at: DateTime<Utc>,
    ) -> Result<RecordComplete, RegionalCollectionAttemptError> {
        let mut evidence = RegionalAttemptEvidence::new(attempt_started_at);
        evidence.pages_attempted += 1;
        let (mut contracts, first_page_metadata) = match self.contract_page(region_id, 1).await {
            Ok(page) => page,
            Err(error) => return Err(regional_attempt_error(error, &mut evidence)),
        };
        evidence.pages_observed += 1;
        evidence.first_page = Some(first_page_metadata.clone());
        evidence.last_page = Some(first_page_metadata.clone());
        evidence.expected_pages = first_page_metadata.expected_pages;
        evidence.observed_contract_count = contracts
            .iter()
            .filter(|contract| contract.is_public_item_exchange())
            .count();
        let expected_pages = match first_page_metadata.expected_pages {
            Some(expected_pages) => expected_pages,
            None => {
                return Err(regional_attempt_error(
                    ContractCollectionError::Cache("page 1 omitted X-Pages".to_string()),
                    &mut evidence,
                ));
            }
        };
        if expected_pages == 0 {
            return Err(regional_attempt_error(
                ContractCollectionError::Cache("X-Pages must be at least 1".to_string()),
                &mut evidence,
            ));
        }
        for page in 2..=expected_pages {
            evidence.pages_attempted += 1;
            let (mut page_contracts, metadata) = match self.contract_page(region_id, page).await {
                Ok(page) => page,
                Err(error) => return Err(regional_attempt_error(error, &mut evidence)),
            };
            evidence.pages_observed += 1;
            evidence.last_page = Some(metadata.clone());
            evidence.observed_contract_count += page_contracts
                .iter()
                .filter(|contract| contract.is_public_item_exchange())
                .count();
            evidence.expected_pages_matches_first =
                Some(metadata.expected_pages == Some(expected_pages));
            if metadata.expected_pages != Some(expected_pages) {
                return Err(regional_attempt_error(
                    ContractCollectionError::Cache(format!(
                        "page {page} reported inconsistent X-Pages"
                    )),
                    &mut evidence,
                ));
            }
            evidence.last_modified_matches_first =
                Some(metadata.last_modified == first_page_metadata.last_modified);
            if metadata.last_modified != first_page_metadata.last_modified {
                return Err(regional_attempt_error(
                    ContractCollectionError::Cache(format!(
                        "page {page} reported inconsistent Last-Modified"
                    )),
                    &mut evidence,
                ));
            }
            contracts.append(&mut page_contracts);
        }
        evidence.expected_pages_matches_first.get_or_insert(true);
        evidence.last_modified_matches_first.get_or_insert(true);
        let contract_ids = contracts
            .iter()
            .map(|contract| contract.contract_id)
            .collect::<HashSet<_>>();
        evidence.duplicate_contract_count = contracts.len().saturating_sub(contract_ids.len());
        if evidence.duplicate_contract_count > 0 {
            return Err(regional_attempt_error(
                ContractCollectionError::Cache(
                    "a contract appeared on more than one page".to_string(),
                ),
                &mut evidence,
            ));
        }
        let public_contracts: Vec<_> = contracts
            .into_iter()
            .filter(PublicContract::is_public_item_exchange)
            .collect();
        evidence.observed_contract_count = public_contracts.len();
        let summaries = match public_contracts
            .into_iter()
            .map(|contract| {
                Ok(SummaryObservedContract {
                    fact_hash: content_hash(&contract)?,
                    contract,
                })
            })
            .collect::<Result<Vec<_>, ContractCollectionError>>()
        {
            Ok(summaries) => summaries,
            Err(error) => return Err(regional_attempt_error(error, &mut evidence)),
        };
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
                    let manifest_hash = match content_hash(&manifest) {
                        Ok(manifest_hash) => manifest_hash,
                        Err(error) => return Err(regional_attempt_error(error, &mut evidence)),
                    };
                    observed.push(ObservedContract {
                        contract: summary.contract.clone(),
                        manifest,
                        fact_hash: summary.fact_hash.clone(),
                        manifest_hash,
                    });
                    evidence.resolved_contract_count = observed.len();
                }
                Err(error @ ContractCollectionError::Database(_)) => {
                    evidence.manifest_failure_count = manifest_failures.len();
                    return Err(regional_attempt_error(error, &mut evidence));
                }
                Err(error) => {
                    evidence.capture_failure(&error);
                    manifest_failures.push(ManifestCollectionFailure {
                        contract_id: summary.contract.contract_id,
                        detail: error.to_string(),
                        retry_after: error.retry_after(),
                    });
                    evidence.manifest_failure_count = manifest_failures.len();
                    limiter_active = match self.store.active_esi_limiter_deadline().await {
                        Ok(deadline) => deadline.is_some(),
                        Err(error) => {
                            return Err(regional_attempt_error(error.into(), &mut evidence));
                        }
                    };
                }
            }
        }
        evidence.resolved_contract_count = observed.len();
        evidence.manifest_failure_count = manifest_failures.len();
        self.store
            .record_complete(
                region_id,
                CompleteRegionalObservation {
                    evidence: &evidence,
                    summaries: &summaries,
                    resolved_contracts: &observed,
                    manifest_failures: &manifest_failures,
                },
                self.recovery_gap,
            )
            .await
            .map_err(|error| regional_attempt_error(error.into(), &mut evidence))
    }

    async fn resolve_awaiting_resolutions(
        &self,
    ) -> Result<ResolutionBatch, ContractCollectionError> {
        self.store
            .resolve_terminal_resolution_failures(None)
            .await?;
        self.resolve_resolution_batch(
            self.store.pending_terminal_notifications().await?,
            self.store.awaiting_resolution_cases().await?,
        )
        .await
    }

    async fn resolve_awaiting_resolutions_excluding_regions(
        &self,
        excluded_region_ids: &[i64],
    ) -> Result<ResolutionBatch, ContractCollectionError> {
        self.store
            .resolve_terminal_resolution_failures_excluding_regions(excluded_region_ids)
            .await?;
        self.resolve_resolution_batch(
            self.store
                .pending_terminal_notifications_excluding_regions(excluded_region_ids)
                .await?,
            self.store
                .awaiting_resolution_cases_excluding_regions(excluded_region_ids)
                .await?,
        )
        .await
    }

    async fn resolve_awaiting_resolutions_for_region(
        &self,
        region_id: i64,
    ) -> Result<ResolutionBatch, ContractCollectionError> {
        self.store
            .resolve_terminal_resolution_failures(Some(region_id))
            .await?;
        let awaiting_resolutions = if self.store.active_esi_limiter_deadline().await?.is_some() {
            Vec::new()
        } else {
            self.store
                .awaiting_resolution_cases_for_region(
                    region_id,
                    MAX_TERMINAL_RESOLUTION_PROBES_PER_COMPLETED_REGION,
                )
                .await?
        };
        self.resolve_resolution_batch(
            self.store
                .pending_terminal_notifications_for_region(region_id)
                .await?,
            awaiting_resolutions,
        )
        .await
    }

    async fn resolve_resolution_batch(
        &self,
        pending_notifications: Vec<TerminalContractResolution>,
        awaiting_resolutions: Vec<AwaitingContractResolution>,
    ) -> Result<ResolutionBatch, ContractCollectionError> {
        let mut batch = ResolutionBatch::default();
        for resolution in pending_notifications {
            if let Some(event) = terminal_resolution_event(&resolution) {
                let resolution_key = ResolutionCaseKey {
                    region_id: resolution.region_id,
                    contract_id: resolution.contract_id,
                };
                self.notify_terminal_event(&event, &resolution_key).await?;
                batch.events.push(event);
            } else {
                warn!(
                    region_id = resolution.region_id,
                    contract_id = resolution.contract_id,
                    "terminal notification recovery found an unreportable resolution"
                );
            }
        }
        for resolution in awaiting_resolutions {
            let resolution_key = ResolutionCaseKey {
                region_id: resolution.region_id,
                contract_id: resolution.contract_id,
            };
            match self.resolve_awaiting_resolution(&resolution).await {
                Ok(Some(event)) => {
                    self.notify_terminal_event(&event, &resolution_key).await?;
                    batch.events.push(event);
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
                        .record_resolution_probe_failure(
                            &resolution,
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

    async fn notify_terminal_event(
        &self,
        event: &ContractEvent,
        resolution_key: &ResolutionCaseKey,
    ) -> Result<(), ContractCollectionError> {
        self.notify_fresh_events(std::slice::from_ref(event))
            .await?;
        if self.notifications.is_some() {
            self.store
                .mark_terminal_notifications_reported(std::slice::from_ref(resolution_key))
                .await?;
        }
        Ok(())
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
                            None,
                            None,
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
                            None,
                            None,
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
            ContractItemProbe::AcceptedByPlayer(metadata) => {
                let evidence_response_at = Utc::now();
                let event = acceptance_event(
                    resolution.region_id,
                    &resolution.contract,
                    &resolution.manifest,
                    resolution.last_public_observed_at,
                    resolution.absence_observed_at,
                    evidence_response_at,
                    ContractAcceptanceProvenance::AcceptedByPlayer,
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
            ContractItemProbe::NoContent(metadata) => {
                let observed_at = Utc::now();
                let state = if observed_at >= resolution.contract.date_expired {
                    ContractResolutionState::Expired
                } else {
                    ContractResolutionState::ClosedOutcomeUnknown
                };
                self.resolve_nonfinancial_terminal(
                    resolution,
                    state,
                    observed_at,
                    Some(ContractAcceptanceProvenance::NoContent),
                    Some(&metadata),
                )
                .await
            }
            ContractItemProbe::NotPublic(_) | ContractItemProbe::NotFound(_) => {
                let observed_at = Utc::now();
                let state = if observed_at >= resolution.contract.date_expired {
                    ContractResolutionState::Expired
                } else {
                    ContractResolutionState::ClosedOutcomeUnknown
                };
                self.resolve_nonfinancial_terminal(resolution, state, observed_at, None, None)
                    .await
            }
        }
    }

    async fn resolve_nonfinancial_terminal(
        &self,
        resolution: &AwaitingContractResolution,
        state: ContractResolutionState,
        observed_at: DateTime<Utc>,
        provenance: Option<ContractAcceptanceProvenance>,
        metadata: Option<&CacheMetadata>,
    ) -> Result<Option<ContractEvent>, ContractCollectionError> {
        let acceptance_evidence = Some(ContractAcceptanceEvidence {
            last_public_observed_at: resolution.last_public_observed_at,
            absence_observed_at: resolution.absence_observed_at,
            evidence_response_at: observed_at,
            provenance,
        });
        if self
            .store
            .resolve_nonfinancial_terminal(resolution, state, observed_at, provenance, metadata)
            .await?
            && !resolution.suppresses_nonfinancial_notification
        {
            return Ok(nonfinancial_terminal_event(
                resolution,
                state,
                acceptance_evidence,
            ));
        }
        Ok(None)
    }

    async fn notify(&self, events: &[ContractEvent]) -> Result<(), ContractCollectionError> {
        let Some(notifications) = &self.notifications else {
            return Ok(());
        };
        let ship_groups = CycleShipGroupResolver::new(&*notifications.ship_groups);
        self.reconcile_proximity_evidence(&ship_groups).await?;
        let deferred_matches = self.store.deferred_contract_matches().await?;
        for deferred in deferred_matches {
            let mut embed_enrichment_deadline =
                (!matches!(deferred.event.kind, ContractEventKind::Listed))
                    .then(|| self.esi.observed_embed_enrichment_deadline());
            let event = self
                .event_with_observed_context_before_enrichment_deadline(
                    &deferred.event,
                    std::slice::from_ref(&deferred.subscription),
                    &ship_groups,
                    embed_enrichment_deadline,
                )
                .await?;
            match self
                .notify_subscription(
                    &deferred.subscription,
                    &event,
                    &ship_groups,
                    false,
                    &mut embed_enrichment_deadline,
                )
                .await?
            {
                NotificationResolution::Complete if !deferred.proximity_unverified => {
                    self.store
                        .resolve_deferred_contract_match(&deferred.subscription, &deferred.event)
                        .await?;
                }
                NotificationResolution::Complete
                | NotificationResolution::Deferred
                | NotificationResolution::ProximityUnverified
                | NotificationResolution::ProximityResolved
                | NotificationResolution::EvidenceChanged => {}
            }
        }
        self.notify_events(events, notifications, &ship_groups)
            .await?;
        self.reconcile_proximity_evidence(&ship_groups).await?;
        self.deliver_prepared_notifications(notifications).await
    }

    pub async fn reconcile_proximity_notifications(&self) -> Result<(), ContractCollectionError> {
        let Some(notifications) = &self.notifications else {
            return Ok(());
        };
        let ship_groups = CycleShipGroupResolver::new(&*notifications.ship_groups);
        self.reconcile_proximity_evidence(&ship_groups).await?;
        self.deliver_prepared_notifications(notifications).await
    }

    async fn reconcile_proximity_evidence(
        &self,
        ship_groups: &dyn ShipGroupResolver,
    ) -> Result<(), ContractCollectionError> {
        let now = self.delivery_clock.now();
        for location_id in self
            .store
            .proximity_unverified_locations_ready_for_reconciliation(now)
            .await?
        {
            let matches = self
                .store
                .proximity_unverified_matches_for_location(location_id)
                .await?;
            for deferred in matches {
                let mut retained_context_subscription = deferred.subscription.clone();
                match deferred.event.kind {
                    ContractEventKind::Listed => {
                        retained_context_subscription.event_actions.listed =
                            ContractEventAction::Post
                    }
                    ContractEventKind::SaleConfirmed => {
                        retained_context_subscription.event_actions.sale_confirmed =
                            ContractEventAction::Post
                    }
                    ContractEventKind::PurchaseConfirmed => {
                        retained_context_subscription
                            .event_actions
                            .purchase_confirmed = ContractEventAction::Post
                    }
                    ContractEventKind::Expired => {
                        retained_context_subscription.event_actions.expired =
                            ContractEventAction::Post
                    }
                    ContractEventKind::ClosedOutcomeUnknown => {
                        retained_context_subscription
                            .event_actions
                            .closed_outcome_unknown = ContractEventAction::Post
                    }
                }
                let mut embed_enrichment_deadline =
                    (!matches!(deferred.event.kind, ContractEventKind::Listed))
                        .then(|| self.esi.observed_embed_enrichment_deadline());
                let event = self
                    .event_with_observed_context_before_enrichment_deadline(
                        &deferred.event,
                        std::slice::from_ref(&retained_context_subscription),
                        ship_groups,
                        embed_enrichment_deadline,
                    )
                    .await?;
                match self
                    .notify_subscription(
                        &deferred.subscription,
                        &event,
                        ship_groups,
                        true,
                        &mut embed_enrichment_deadline,
                    )
                    .await?
                {
                    NotificationResolution::ProximityResolved => {}
                    NotificationResolution::Complete
                    | NotificationResolution::Deferred
                    | NotificationResolution::ProximityUnverified
                    | NotificationResolution::EvidenceChanged => {}
                }
            }
        }
        Ok(())
    }

    async fn notify_fresh_events(
        &self,
        events: &[ContractEvent],
    ) -> Result<(), ContractCollectionError> {
        self.notify_fresh_events_with_deadlines(events, &HashMap::new())
            .await
    }

    async fn notify_fresh_events_with_deadlines(
        &self,
        events: &[ContractEvent],
        attempted_embed_enrichment_deadlines: &HashMap<i64, tokio::time::Instant>,
    ) -> Result<(), ContractCollectionError> {
        let Some(notifications) = &self.notifications else {
            return Ok(());
        };
        let ship_groups = CycleShipGroupResolver::new(&*notifications.ship_groups);
        self.notify_events_with_deadlines(
            events,
            notifications,
            &ship_groups,
            attempted_embed_enrichment_deadlines,
        )
        .await
    }

    async fn notify_events(
        &self,
        events: &[ContractEvent],
        notifications: &ContractNotifications,
        ship_groups: &dyn ShipGroupResolver,
    ) -> Result<(), ContractCollectionError> {
        self.notify_events_with_deadlines(events, notifications, ship_groups, &HashMap::new())
            .await
    }

    async fn notify_events_with_deadlines(
        &self,
        events: &[ContractEvent],
        notifications: &ContractNotifications,
        ship_groups: &dyn ShipGroupResolver,
        attempted_embed_enrichment_deadlines: &HashMap<i64, tokio::time::Instant>,
    ) -> Result<(), ContractCollectionError> {
        let subscriptions = self.store.all_contract_subscriptions().await?;
        for event in events {
            let mut embed_enrichment_deadline = attempted_embed_enrichment_deadlines
                .get(&event.contract.contract_id)
                .copied()
                .or_else(|| {
                    (!matches!(event.kind, ContractEventKind::Listed))
                        .then(|| self.esi.observed_embed_enrichment_deadline())
                });
            let event = self
                .event_with_observed_context_before_enrichment_deadline(
                    event,
                    &subscriptions,
                    ship_groups,
                    embed_enrichment_deadline,
                )
                .await?;
            for subscription in &subscriptions {
                match self
                    .notify_subscription(
                        subscription,
                        &event,
                        ship_groups,
                        false,
                        &mut embed_enrichment_deadline,
                    )
                    .await?
                {
                    NotificationResolution::Complete => {}
                    NotificationResolution::Deferred => {
                        self.store
                            .defer_contract_match(subscription, &event, false)
                            .await?;
                    }
                    NotificationResolution::ProximityUnverified => {}
                    NotificationResolution::ProximityResolved => {}
                    NotificationResolution::EvidenceChanged => {}
                }
            }
        }
        self.deliver_prepared_notifications(notifications).await?;
        Ok(())
    }

    async fn event_with_observed_context_before_enrichment_deadline(
        &self,
        event: &ContractEvent,
        subscriptions: &[ContractSubscription],
        ship_groups: &dyn ShipGroupResolver,
        enrichment_deadline: Option<tokio::time::Instant>,
    ) -> Result<ContractEvent, ContractCollectionError> {
        let Some(deadline) = enrichment_deadline else {
            return self
                .event_with_observed_context(event, subscriptions, ship_groups)
                .await;
        };
        if tokio::time::Instant::now() >= deadline {
            return self.event_with_persisted_observation_context(event).await;
        }
        match tokio::time::timeout_at(
            deadline,
            self.event_with_observed_context(event, subscriptions, ship_groups),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => self.event_with_persisted_observation_context(event).await,
        }
    }

    async fn event_with_observed_context(
        &self,
        event: &ContractEvent,
        subscriptions: &[ContractSubscription],
        ship_groups: &dyn ShipGroupResolver,
    ) -> Result<ContractEvent, ContractCollectionError> {
        let evidence_now = self.delivery_clock.now();
        let mut event = self.event_with_persisted_observation_context(event).await?;
        let retained_evidence_requires_public_refresh = matches!(
            event.context.location_evidence_class,
            Some(LocationEvidenceClass::AccessQualified | LocationEvidenceClass::Operator)
        );
        let selected_evidence = if event.context.location_evidence_id.is_some() {
            LocationEvidenceService::new(&self.store)
                .resolve(event.contract.start_location_id, evidence_now)
                .await?
        } else {
            None
        };
        let authoritative_evidence_changed =
            event
                .context
                .location_evidence_id
                .is_some_and(|retained_id| {
                    selected_evidence.as_ref().is_some_and(|evidence| {
                        evidence.id != retained_id
                            || event.context.solar_system_id != Some(evidence.solar_system_id)
                    })
                });
        let retained_observation_context = (matches!(event.kind, ContractEventKind::Listed)
            && retained_evidence_requires_public_refresh
            && selected_evidence.is_none())
        .then(|| (event.context.clone(), event.embed_context.location.clone()));
        if matches!(event.kind, ContractEventKind::Listed)
            && (retained_evidence_requires_public_refresh || authoritative_evidence_changed)
        {
            clear_location_evidence_context(&mut event);
            event.context.location_evidence_id = None;
            event.context.location_evidence_class = None;
        }
        let mut requirements = ContractContextRequirements::default();
        let mut presentation_location_required = false;
        for subscription in subscriptions {
            if !matches!(
                contract_event_action(subscription, &event.kind),
                ContractEventAction::Ignore
            ) {
                let mut evaluator = ContractFilterEvaluator::new(&event, ship_groups);
                let plan =
                    plan_contract_filter_context(&subscription.filter.root, &mut evaluator, true)
                        .await;
                presentation_location_required |=
                    matches!(plan.result, ContractFilterMatch::Matched);
                requirements = requirements.union(plan.requirements);
            }
            if matches!(event.kind, ContractEventKind::Listed) {
                for terminal_kind in [
                    ContractEventKind::SaleConfirmed,
                    ContractEventKind::PurchaseConfirmed,
                    ContractEventKind::Expired,
                    ContractEventKind::ClosedOutcomeUnknown,
                ] {
                    if matches!(
                        contract_event_action(subscription, &terminal_kind),
                        ContractEventAction::Ignore
                    ) {
                        continue;
                    }
                    let mut terminal_event = event.clone();
                    terminal_event.kind = terminal_kind;
                    let mut evaluator = ContractFilterEvaluator::new(&terminal_event, ship_groups);
                    let plan = plan_contract_filter_context(
                        &subscription.filter.root,
                        &mut evaluator,
                        true,
                    )
                    .await;
                    presentation_location_required |=
                        matches!(plan.result, ContractFilterMatch::Matched);
                    requirements = requirements.union(plan.requirements);
                }
            }
        }
        if presentation_location_required {
            requirements.solar_system = true;
        }
        if requirements.is_empty() {
            return Ok(event);
        }
        event.context.normalize_resolutions();

        if !matches!(event.kind, ContractEventKind::Listed) {
            let missing_current_structure_evidence = (requirements.solar_system
                || requirements.security_status
                || !requirements.ly_ranges.is_empty())
                && u32::try_from(event.contract.start_location_id).is_err()
                && LocationEvidenceService::new(&self.store)
                    .resolve(event.contract.start_location_id, evidence_now)
                    .await?
                    .is_none();
            if missing_current_structure_evidence {
                self.resolve_structure_location_evidence(&mut event, evidence_now)
                    .await?;
            }
            if requirements.solar_system
                || requirements.security_status
                || !requirements.ly_ranges.is_empty()
            {
                if let Some(evidence) = LocationEvidenceService::new(&self.store)
                    .resolve(event.contract.start_location_id, evidence_now)
                    .await?
                {
                    if event.context.location_evidence_id != Some(evidence.id) {
                        clear_location_evidence_context(&mut event);
                    }
                    event.context.solar_system_id = Some(evidence.solar_system_id);
                    event.context.solar_system_resolution = ContractContextResolution::Resolved;
                    event.context.location_evidence_id = Some(evidence.id);
                    event.context.location_evidence_class = Some(evidence.evidence_class);
                }
            }
            if !requirements.ly_ranges.is_empty() && event.context.solar_system_position.is_none() {
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
                    }
                }
            }
            self.load_range_center_positions(&mut event, &requirements)
                .await?;
            mark_terminal_context_unavailable(&mut event.context, requirements);
            return Ok(event);
        }

        if (requirements.solar_system || requirements.security_status)
            && event.context.solar_system_id.is_none()
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
                let public_system_id =
                    response.as_ref().and_then(|response| match response.value {
                        Some(ContractContextValue::Resolved(system_id)) => Some(system_id),
                        Some(ContractContextValue::DefinitivelyAbsent)
                        | Some(ContractContextValue::Indeterminate)
                        | None => None,
                    });
                apply_solar_system_context(&mut event.context, response);
                if let Some((station_id, solar_system_id)) =
                    public_system_id.and_then(|solar_system_id| {
                        public_location_facts(
                            event.contract.start_location_id,
                            solar_system_id,
                            None,
                        )
                    })
                {
                    match LocationEvidenceService::new(&self.store)
                        .record_public_npc(
                            event.contract.start_location_id,
                            station_id,
                            solar_system_id,
                            None,
                            evidence_now,
                            evidence_now,
                        )
                        .await
                    {
                        Ok(evidence) => {
                            event.context.location_evidence_id = Some(evidence.id);
                            event.context.location_evidence_class =
                                Some(LocationEvidenceClass::PublicNpc);
                        }
                        Err(error) => warn!(
                            contract_id = event.contract.contract_id,
                            location_id = event.contract.start_location_id,
                            "could not retain optional public location evidence: {error}"
                        ),
                    }
                }
            } else {
                event.context.solar_system_resolution =
                    ContractContextResolution::TemporarilyUnavailable;
            }
        }

        if (requirements.solar_system || requirements.security_status)
            && event.context.solar_system_id.is_none()
        {
            if let Some(evidence) = LocationEvidenceService::new(&self.store)
                .resolve(event.contract.start_location_id, evidence_now)
                .await?
            {
                event.context.solar_system_id = Some(evidence.solar_system_id);
                event.context.solar_system_resolution = ContractContextResolution::Resolved;
                event.context.location_evidence_id = Some(evidence.id);
                event.context.location_evidence_class = Some(evidence.evidence_class);
            }
        }

        if (requirements.solar_system || requirements.security_status)
            && (event.context.solar_system_id.is_none()
                || u32::try_from(event.contract.start_location_id).is_err())
        {
            self.resolve_structure_location_evidence(&mut event, evidence_now)
                .await?;
        }

        if event.context.solar_system_id.is_none() {
            if let Some((context, location)) = retained_observation_context {
                event.context = context;
                event.embed_context.location = location;
            }
        }

        if (requirements.solar_system || requirements.security_status)
            && event.context.solar_system_id.is_none()
        {
            if let Some(evidence) = LocationEvidenceService::new(&self.store)
                .last_verified_access_qualified(event.contract.start_location_id, evidence_now)
                .await?
            {
                event.context.solar_system_id = Some(evidence.solar_system_id);
                event.context.solar_system_resolution = ContractContextResolution::Resolved;
                event.context.location_evidence_id = Some(evidence.id);
                event.context.location_evidence_class = Some(evidence.evidence_class);
                event.embed_context.location.location_name = None;
                event.embed_context.location.location_kind =
                    Some("Player-owned structure".to_string());
                event.embed_context.location.last_verified_at = Some(evidence.observed_at);
                event.embed_context.location.solar_system_id = Some(evidence.solar_system_id);
                event.embed_context.location.region_id =
                    evidence.region_id.or(Some(event.region_id));
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
        if requirements.solar_system {
            if let Some(solar_system_id) = event
                .context
                .solar_system_id
                .and_then(|solar_system_id| u32::try_from(solar_system_id).ok())
            {
                if event.embed_context.location.solar_system_id != Some(i64::from(solar_system_id))
                {
                    event.embed_context.location.location_name = None;
                    event.embed_context.location.location_kind = None;
                    event.embed_context.location.solar_system_name = None;
                    event.embed_context.location.solar_system_position = None;
                    event.embed_context.location.security_status = None;
                }
                event.embed_context.location.solar_system_id = Some(i64::from(solar_system_id));
                event.embed_context.location.region_id = Some(event.region_id);
                if event.embed_context.location.solar_system_name.is_none() {
                    event.embed_context.location.solar_system_name = self
                        .cached_solar_system_name(event.contract.contract_id, solar_system_id)
                        .await?;
                }
            }
        }
        self.persist_listing_observation_context(&event).await?;
        Ok(event)
    }

    async fn persist_listing_observation_context(
        &self,
        event: &ContractEvent,
    ) -> Result<(), ContractCollectionError> {
        let mut context = event.embed_context.clone();
        context.location.solar_system_id = context
            .location
            .solar_system_id
            .or(event.context.solar_system_id);
        context.location.solar_system_position = context
            .location
            .solar_system_position
            .or(event.context.solar_system_position);
        context.location.security_status = context.location.security_status.or(event
            .context
            .security_status
            .filter(|value| value.is_finite()));
        context.issuer_alliance_id = context
            .issuer_alliance_id
            .or(event.context.observed_affiliation_alliance_id);
        if context.location.solar_system_id.is_none()
            && context.location.solar_system_position.is_none()
            && context.location.security_status.is_none()
            && context.issuer_alliance_id.is_none()
        {
            return Ok(());
        }
        self.ensure_region_context(event.contract.contract_id, event.region_id, &mut context)
            .await?;
        context.observed_at = context.observed_at.or(Some(self.delivery_clock.now()));
        self.store
            .save_observed_embed_context(event.region_id, event.contract.contract_id, &context)
            .await?;
        Ok(())
    }

    async fn resolve_structure_location_evidence(
        &self,
        event: &mut ContractEvent,
        now: DateTime<Utc>,
    ) -> Result<(), ContractCollectionError> {
        let Some(resolver) = &self.structure_resolver else {
            return Ok(());
        };
        let structure_id = event.contract.start_location_id;
        if !self
            .context_request_allowed(event.contract.contract_id)
            .await?
        {
            return Ok(());
        }
        let admission = match self
            .store
            .reserve_structure_resolution(structure_id, resolver.credential_revision(), now)
            .await
        {
            Ok(admission) => admission,
            Err(error) => {
                warn!(
                    contract_id = event.contract.contract_id,
                    structure_id, "structure resolver state unavailable: {error}"
                );
                return Ok(());
            }
        };
        let (generation, cached) = match admission {
            StructureResolutionAdmission::Inactive
            | StructureResolutionAdmission::WaitUntil(_)
            | StructureResolutionAdmission::Parked => return Ok(()),
            StructureResolutionAdmission::Attempt { generation, cached } => {
                (generation, cached.map(|cached| *cached))
            }
        };
        let resolution = resolver
            .resolve_structure_revalidating(structure_id, cached, now)
            .await;
        let response_from_structure_esi = match &resolution {
            Ok(_) => true,
            Err(error) => error.has_structure_esi_response(),
        };
        let response_metadata = match &resolution {
            Ok(resolved) => &resolved.response_metadata,
            Err(error) => error.response_metadata(),
        };
        let completion_at = resolution
            .as_ref()
            .ok()
            .map(|resolved| self.request_pacer.now().max(resolved.observed_at))
            .unwrap_or(now);
        if response_from_structure_esi {
            if let Err(error) = self
                .store
                .record_esi_limiter_at(response_metadata, self.request_pacer.now())
                .await
            {
                warn!(
                    contract_id = event.contract.contract_id,
                    structure_id, "could not persist structure resolver ESI limiter state: {error}"
                );
                return Ok(());
            }
        }
        match resolution {
            Ok(resolved)
                if resolved
                    .expires_at
                    .is_some_and(|expires_at| expires_at > completion_at)
                    && resolved.representation_cacheable =>
            {
                let evidence = match self
                    .store
                    .record_structure_resolution_success_with_evidence(
                        &resolved,
                        Some(event.region_id),
                        resolver.credential_revision(),
                        generation,
                        resolver.resolver_identity(),
                        completion_at,
                    )
                    .await
                {
                    Ok(Some(evidence)) => evidence,
                    Ok(None) => return Ok(()),
                    Err(error) => {
                        warn!(
                            contract_id = event.contract.contract_id,
                            structure_id,
                            "could not retain access-qualified structure evidence: {error}"
                        );
                        return Ok(());
                    }
                };
                if let Err(error) = self
                    .store
                    .record_structure_resolver_runtime(
                        true,
                        resolver.credential_revision(),
                        "ready",
                        None,
                        completion_at,
                    )
                    .await
                {
                    warn!(
                        contract_id = event.contract.contract_id,
                        structure_id, "could not persist resolver health: {error}"
                    );
                }
                event.context.solar_system_id = Some(evidence.solar_system_id);
                event.context.solar_system_resolution = ContractContextResolution::Resolved;
                event.context.location_evidence_id = Some(evidence.id);
                event.context.location_evidence_class = Some(evidence.evidence_class);
                event.embed_context.location.last_verified_at = None;
                if let Some(name) = resolved.name {
                    event.embed_context.location.solar_system_id = Some(evidence.solar_system_id);
                    event.embed_context.location.region_id = Some(event.region_id);
                    event.embed_context.location.location_name = Some(name);
                    event.embed_context.location.location_kind =
                        Some("Player-owned structure".to_string());
                }
            }
            Ok(resolved) => {
                if !resolved.representation_cacheable {
                    if let Err(error) = self
                        .store
                        .discard_structure_resolution_representation(
                            structure_id,
                            resolver.credential_revision(),
                            generation,
                        )
                        .await
                    {
                        warn!(
                            contract_id = event.contract.contract_id,
                            structure_id,
                            "could not discard no-store structure representation: {error}"
                        );
                    }
                }
                if let Err(error) = self
                    .store
                    .record_structure_resolution_transient_failure(
                        structure_id,
                        resolver.credential_revision(),
                        generation,
                        completion_at,
                        Self::structure_resolution_retry_deadline(
                            &resolved.response_metadata,
                            resolved.response_metadata.retry_after,
                            completion_at,
                        ),
                    )
                    .await
                {
                    warn!(
                        contract_id = event.contract.contract_id,
                        structure_id,
                        "could not persist structure resolver cache boundary: {error}"
                    );
                }
            }
            Err(error) if error.kind() == StructureResolverFailureKind::AccessDenied => {
                if error.response_disallows_representation_storage() {
                    if let Err(discard_error) = self
                        .store
                        .discard_structure_resolution_representation(
                            structure_id,
                            resolver.credential_revision(),
                            generation,
                        )
                        .await
                    {
                        warn!(
                            contract_id = event.contract.contract_id,
                            structure_id,
                            "could not discard no-store structure representation: {discard_error}"
                        );
                    }
                }
                match self
                    .store
                    .record_structure_resolution_denial(
                        structure_id,
                        resolver.credential_revision(),
                        generation,
                        now,
                        error.retry_after(),
                    )
                    .await
                {
                    Ok(true) => {
                        if let Err(record_error) = self
                            .store
                            .record_structure_resolver_runtime(
                                true,
                                resolver.credential_revision(),
                                "degraded",
                                Some("structure resolver access denied"),
                                now,
                            )
                            .await
                        {
                            warn!(
                                contract_id = event.contract.contract_id,
                                structure_id, "could not persist resolver health: {record_error}"
                            );
                        }
                    }
                    Ok(false) => {}
                    Err(record_error) => {
                        warn!(
                            contract_id = event.contract.contract_id,
                            structure_id,
                            "could not persist structure resolver denial: {record_error}"
                        );
                    }
                }
            }
            Err(error) => {
                if error.response_disallows_representation_storage() {
                    if let Err(discard_error) = self
                        .store
                        .discard_structure_resolution_representation(
                            structure_id,
                            resolver.credential_revision(),
                            generation,
                        )
                        .await
                    {
                        warn!(
                            contract_id = event.contract.contract_id,
                            structure_id,
                            "could not discard no-store structure representation: {discard_error}"
                        );
                    }
                }
                if error.is_global_auth_failure() {
                    let deadline = error
                        .retry_after()
                        .unwrap_or(now + ChronoDuration::seconds(30));
                    match self
                        .store
                        .record_structure_resolver_backoff(
                            resolver.credential_revision(),
                            deadline,
                            now,
                        )
                        .await
                    {
                        Ok(true) => {
                            if let Err(record_error) = self
                                .store
                                .record_structure_resolver_runtime(
                                    true,
                                    resolver.credential_revision(),
                                    "degraded",
                                    Some("structure resolver authorization or upstream failure"),
                                    now,
                                )
                                .await
                            {
                                warn!(
                                    structure_id,
                                    "could not persist resolver health: {record_error}"
                                );
                            }
                        }
                        Ok(false) => {}
                        Err(record_error) => {
                            warn!(
                                structure_id,
                                "could not persist resolver-wide backoff: {record_error}"
                            );
                        }
                    }
                    return Ok(());
                }
                match self
                    .store
                    .record_structure_resolution_transient_failure(
                        structure_id,
                        resolver.credential_revision(),
                        generation,
                        now,
                        Self::transient_structure_resolution_retry_deadline(&error, now),
                    )
                    .await
                {
                    Ok(true) => {
                        if let Err(record_error) = self
                            .store
                            .record_structure_resolver_runtime(
                                true,
                                resolver.credential_revision(),
                                "degraded",
                                Some("structure resolver authorization or upstream failure"),
                                now,
                            )
                            .await
                        {
                            warn!(
                                contract_id = event.contract.contract_id,
                                structure_id, "could not persist resolver health: {record_error}"
                            );
                        }
                    }
                    Ok(false) => {}
                    Err(record_error) => {
                        warn!(
                            contract_id = event.contract.contract_id,
                            structure_id,
                            "could not persist structure resolver retry: {record_error}"
                        );
                    }
                }
                warn!(
                    contract_id = event.contract.contract_id,
                    structure_id, "structure resolver degraded: {error}"
                );
            }
        }
        Ok(())
    }

    fn transient_structure_resolution_retry_deadline(
        error: &StructureResolverError,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        Self::structure_resolution_retry_deadline(
            error.response_metadata(),
            error.retry_after(),
            now,
        )
    }

    fn structure_resolution_retry_deadline(
        response_metadata: &CacheMetadata,
        retry_after: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        [retry_after, response_metadata.expires_at]
            .into_iter()
            .flatten()
            .filter(|deadline| *deadline > now)
            .max()
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
            if !event
                .context
                .range_center_positions
                .contains_key(&range.system_id)
            {
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
            if event
                .context
                .range_center_names
                .contains_key(&range.system_id)
            {
                continue;
            }
            if let Some(name) = self
                .cached_solar_system_name(event.contract.contract_id, range.system_id)
                .await?
            {
                event
                    .context
                    .range_center_names
                    .insert(range.system_id, name);
            }
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

    async fn cached_solar_system_name(
        &self,
        contract_id: i64,
        solar_system_id: u32,
    ) -> Result<Option<String>, ContractCollectionError> {
        let key = format!("universe/systems/{solar_system_id}/range-center-name");
        let cached = self.store.cache(&key).await?;
        if let Some((name, _)) = self.fresh_cached::<String>(&key, &cached)? {
            return Ok((!name.trim().is_empty()).then_some(name));
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
                    .solar_system_name(solar_system_id, etag.as_deref())
                    .await,
            )
            .await?
        else {
            return Ok(None);
        };
        let (name, _) = self.resolve_response(&key, cached, response).await?;
        Ok((!name.trim().is_empty()).then_some(name))
    }

    async fn cached_region_name(
        &self,
        contract_id: i64,
        region_id: i64,
    ) -> Result<Option<String>, ContractCollectionError> {
        if !self.esi.supports_region_name_lookup() {
            return Ok(None);
        }
        let key = format!("universe/regions/{region_id}/contract-region-name");
        let cached = self.store.cache(&key).await?;
        if let Some((name, _)) = self.fresh_cached::<Option<String>>(&key, &cached)? {
            return Ok(name.filter(|name| !name.trim().is_empty()));
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
                self.esi.region_name(region_id, etag.as_deref()).await,
            )
            .await?
        else {
            return Ok(None);
        };
        let (name, _) = self.resolve_response(&key, cached, response).await?;
        Ok(name.filter(|name| !name.trim().is_empty()))
    }

    async fn ensure_region_context(
        &self,
        contract_id: i64,
        region_id: i64,
        context: &mut ContractEmbedContext,
    ) -> Result<(), ContractCollectionError> {
        context.location.region_id = Some(region_id);
        if context
            .location
            .region_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            context.location.region_name =
                if let Some(region_name) = self.region_names.get(&region_id).cloned() {
                    Some(region_name)
                } else {
                    self.cached_region_name(contract_id, region_id).await?
                };
        }
        Ok(())
    }

    /// Completes the required region presentation fact from sources that are already
    /// retained locally. Live public lookups belong to the bounded enrichment barrier.
    async fn ensure_region_context_from_retained_sources(
        &self,
        region_id: i64,
        context: &mut ContractEmbedContext,
    ) -> Result<(), ContractCollectionError> {
        context.location.region_id = Some(region_id);
        if context
            .location
            .region_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            context.location.region_name = self
                .region_names
                .get(&region_id)
                .cloned()
                .or(self.store.retained_region_name(region_id).await?);
        }
        Ok(())
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
                self.store
                    .record_esi_limiter_at(&response.metadata, self.request_pacer.now())
                    .await?;
                Ok(Some(response))
            }
            Err(error) => {
                self.store
                    .record_esi_limiter_at(&error.metadata, self.request_pacer.now())
                    .await?;
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
        resolving_proximity_unverified: bool,
        embed_enrichment_deadline: &mut Option<tokio::time::Instant>,
    ) -> Result<NotificationResolution, ContractCollectionError> {
        let mut refreshed = event.clone();
        for refresh in 0..=MAX_PROXIMITY_EVIDENCE_REFRESHES {
            match self
                .notify_subscription_once(
                    subscription,
                    &refreshed,
                    ship_groups,
                    resolving_proximity_unverified,
                    embed_enrichment_deadline,
                )
                .await?
            {
                NotificationResolution::EvidenceChanged
                    if refresh < MAX_PROXIMITY_EVIDENCE_REFRESHES =>
                {
                    refreshed = self
                        .event_with_observed_context_before_enrichment_deadline(
                            &refreshed,
                            std::slice::from_ref(subscription),
                            ship_groups,
                            *embed_enrichment_deadline,
                        )
                        .await?;
                }
                NotificationResolution::EvidenceChanged => {
                    return Ok(NotificationResolution::Deferred);
                }
                resolution => return Ok(resolution),
            }
        }
        Ok(NotificationResolution::Deferred)
    }

    async fn notify_subscription_once(
        &self,
        subscription: &ContractSubscription,
        event: &ContractEvent,
        ship_groups: &dyn ShipGroupResolver,
        resolving_proximity_unverified: bool,
        embed_enrichment_deadline: &mut Option<tokio::time::Instant>,
    ) -> Result<NotificationResolution, ContractCollectionError> {
        if event.kind == ContractEventKind::Listed && is_want_to_buy_listing(event) {
            return Ok(NotificationResolution::Complete);
        }
        let action = contract_event_action(subscription, &event.kind);
        if matches!(action, ContractEventAction::Ignore) && !resolving_proximity_unverified {
            return Ok(NotificationResolution::Complete);
        }
        let mut evaluator = ContractFilterEvaluator::new(event, ship_groups);
        let proximity_unverified = match evaluator.matches(&subscription.filter.root).await {
            ContractFilterMatch::Matched => false,
            ContractFilterMatch::Unmatched => {
                return if resolving_proximity_unverified {
                    match self
                        .store
                        .prepare_proximity_resolution(subscription, event, None, None)
                        .await?
                    {
                        PrepareProximityResolutionOutcome::Resolved => {
                            Ok(NotificationResolution::ProximityResolved)
                        }
                        PrepareProximityResolutionOutcome::EvidenceChanged => {
                            Ok(NotificationResolution::EvidenceChanged)
                        }
                        PrepareProximityResolutionOutcome::Unchanged => {
                            Ok(NotificationResolution::Deferred)
                        }
                    }
                } else {
                    Ok(NotificationResolution::Complete)
                };
            }
            ContractFilterMatch::Deferred => return Ok(NotificationResolution::Deferred),
            ContractFilterMatch::UnresolvedProximity => true,
        };
        let required_direction = contract_presentation_direction(event.kind);
        if required_direction
            .is_some_and(|direction| !event_supports_presentation_direction(event, direction))
        {
            return Ok(NotificationResolution::Complete);
        }
        let (primary_item, strategic_priority) = if proximity_unverified {
            let mut selection_evaluator = ContractFilterEvaluator::new(event, ship_groups);
            let selection = plan_contract_filter_context(
                &subscription.filter.root,
                &mut selection_evaluator,
                true,
            )
            .await;
            if selection.waits_for_non_context
                || required_direction.is_some_and(|direction| {
                    selection.has_explicit_ship_items()
                        && !selection.has_ship_item_in_direction(direction)
                })
            {
                return Ok(NotificationResolution::Deferred);
            }
            match selection_evaluator
                .primary_display_item(required_direction)
                .await
            {
                PrimaryDisplayItem::Resolved {
                    item,
                    strategic_priority,
                } => (item, strategic_priority),
                PrimaryDisplayItem::Deferred => return Ok(NotificationResolution::Deferred),
            }
        } else {
            if required_direction.is_some_and(|direction| {
                evaluator.has_matching_ship_items() && !evaluator.has_matching_ship_item(direction)
            }) {
                return Ok(NotificationResolution::Complete);
            }
            match evaluator.primary_display_item(required_direction).await {
                PrimaryDisplayItem::Resolved {
                    item,
                    strategic_priority,
                } => (item, strategic_priority),
                PrimaryDisplayItem::Deferred => return Ok(NotificationResolution::Deferred),
            }
        };
        let Some(mut event) = self
            .enrich_event_for_embed(event, primary_item, embed_enrichment_deadline)
            .await?
        else {
            return Ok(NotificationResolution::Deferred);
        };
        event.embed_context.matched_range = evaluator.matched_range().and_then(|range| {
            contract_matched_range_for_embed(range, &event.embed_context.location)
        });
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
            proximity_unverified,
        );
        let ping_type = action.ping_type();
        let ping = !proximity_unverified && ping_type.is_some();
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
        let ordering = DeliveryOrdering {
            strategic_priority,
            relevant_isk,
            confirmed_at,
        };
        let preparation = if proximity_unverified {
            self.store
                .prepare_proximity_unverified_delivery(
                    subscription,
                    &event,
                    &message,
                    ping_type.is_some(),
                    ping_type.unwrap_or_default(),
                    ordering,
                )
                .await?
        } else if resolving_proximity_unverified {
            return match self
                .store
                .prepare_proximity_resolution(subscription, &event, Some(&message), Some(ordering))
                .await?
            {
                PrepareProximityResolutionOutcome::Resolved => {
                    Ok(NotificationResolution::ProximityResolved)
                }
                PrepareProximityResolutionOutcome::EvidenceChanged => {
                    Ok(NotificationResolution::EvidenceChanged)
                }
                PrepareProximityResolutionOutcome::Unchanged => {
                    Ok(NotificationResolution::Deferred)
                }
            };
        } else {
            self.store
                .prepare_delivery(
                    subscription,
                    &event,
                    &message,
                    ping,
                    ping_type.unwrap_or_default(),
                    ordering,
                )
                .await?
        };
        if matches!(preparation, PrepareDeliveryOutcome::EvidenceBecameAvailable) {
            return Ok(NotificationResolution::EvidenceChanged);
        }
        if let PrepareDeliveryOutcome::ExistingSent {
            delivery_id,
            message: stored_message,
        } = preparation
        {
            if event.kind == ContractEventKind::Listed {
                let desired_message =
                    repaired_contract_message(&stored_message, &message, event.kind);
                if desired_message != stored_message {
                    self.store
                        .prepare_delivery_repair(delivery_id, desired_message)
                        .await?;
                }
            }
        }
        Ok(if proximity_unverified {
            NotificationResolution::ProximityUnverified
        } else {
            NotificationResolution::Complete
        })
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
        for pending in self
            .store
            .pending_repairs(self.delivery_clock.now())
            .await?
        {
            let stop_batch = self.deliver_pending_repair(&pending, notifications).await?;
            if stop_batch {
                break;
            }
        }
        Ok(())
    }

    async fn deliver_pending_repair(
        &self,
        pending: &ContractMessageEdit,
        notifications: &ContractNotifications,
    ) -> Result<bool, ContractCollectionError> {
        let attempted_at = self.delivery_clock.now();
        let Some(edit) = self
            .store
            .begin_repair_attempt(pending.delivery_id, attempted_at)
            .await?
        else {
            return Ok(false);
        };
        match notifications.delivery.edit(edit.clone()).await {
            Ok(()) => {
                let completed_at = self.delivery_clock.now();
                self.store
                    .mark_repair_succeeded(&edit, completed_at)
                    .await?;
                Ok(false)
            }
            Err(ContractDeliveryError::Permanent(error)) => {
                let completed_at = self.delivery_clock.now();
                let error = ContractDeliveryError::Permanent(error);
                self.store
                    .mark_repair_failed(&edit, &error, completed_at)
                    .await?;
                warn!(
                    delivery_id = edit.delivery_id,
                    contract_id = edit.contract_id,
                    "contract message repair failed permanently: {error}"
                );
                Ok(true)
            }
            Err(
                error @ (ContractDeliveryError::Transient(_) | ContractDeliveryError::Ambiguous(_)),
            ) => {
                let completed_at = self.delivery_clock.now();
                let retry_at =
                    contract_repair_retry_at(&error, completed_at, edit.repair_attempt_count);
                self.store
                    .mark_repair_retryable(&edit, &error, retry_at)
                    .await?;
                warn!(
                    delivery_id = edit.delivery_id,
                    contract_id = edit.contract_id,
                    "contract message repair remains pending after retryable Discord failure: {error}"
                );
                Ok(true)
            }
        }
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
                    .mark_delivery_sent(&prepared, &message_id)
                    .await?;
                Ok(false)
            }
            Err(ContractDeliveryError::Permanent(error)) => {
                let error = ContractDeliveryError::Permanent(error);
                self.store
                    .mark_delivery_failed(&prepared, &error, attempted_at)
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
                    .mark_delivery_retryable(&prepared, &error)
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
        primary_item: Option<&PublicContractItem>,
        embed_enrichment_deadline: &mut Option<tokio::time::Instant>,
    ) -> Result<Option<ContractEvent>, ContractCollectionError> {
        let mut enriched = event.clone();
        let selected_last_verified = enriched.embed_context.location.last_verified_at.is_some();
        let snapshot = self
            .store
            .observed_embed_context(event.region_id, event.contract.contract_id)
            .await?;
        if let Some(snapshot) = snapshot.as_ref() {
            enriched.embed_context.merge_missing_from(&snapshot);
        }
        if enriched.context.location_evidence_id.is_some() && !selected_last_verified {
            enriched.embed_context.location.last_verified_at = None;
        }
        let authoritative_location_changed = enriched.context.location_evidence_id.is_some()
            && enriched.context.solar_system_id.is_some()
            && enriched.embed_context.location.solar_system_id != enriched.context.solar_system_id;
        if authoritative_location_changed {
            enriched.embed_context.location.location_name = None;
            enriched.embed_context.location.location_kind = None;
            enriched.embed_context.location.solar_system_name = None;
            enriched.embed_context.location.region_id = None;
            enriched.embed_context.location.region_name = None;
        }
        if enriched.embed_context.location.last_verified_at.is_some() {
            enriched.embed_context.location.location_name = None;
            enriched.embed_context.location.location_kind =
                Some("Player-owned structure".to_string());
        }
        if enriched.context.location_evidence_id.is_some() {
            enriched.embed_context.location.solar_system_id = enriched.context.solar_system_id;
            enriched.embed_context.location.security_status = enriched
                .context
                .security_status
                .filter(|value| value.is_finite());
            enriched.embed_context.location.solar_system_position =
                enriched.context.solar_system_position;
        } else {
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
        }
        enriched.embed_context.issuer_alliance_id = enriched
            .embed_context
            .issuer_alliance_id
            .or(enriched.context.observed_affiliation_alliance_id);
        let primary_name_missing = primary_item.is_some_and(|item| {
            !enriched
                .embed_context
                .item_names
                .contains_key(&item.type_id)
        });
        let incomplete_station_listing = event.kind == ContractEventKind::Listed
            && u32::try_from(event.contract.start_location_id)
                .ok()
                .is_some_and(|location_id| !(30_000_000..33_000_000).contains(&location_id))
            && enriched
                .embed_context
                .location
                .location_name
                .as_deref()
                .is_none_or(|name| name.trim().is_empty());
        let incomplete_authoritative_listing_location = event.kind == ContractEventKind::Listed
            && enriched.context.location_evidence_id.is_some()
            && enriched
                .embed_context
                .location
                .solar_system_name
                .as_deref()
                .is_none_or(|name| name.trim().is_empty());
        let location_is_solar_system = u32::try_from(event.contract.start_location_id)
            .ok()
            .is_some_and(|location_id| (30_000_000..33_000_000).contains(&location_id));
        let incomplete_terminal_location = event.kind != ContractEventKind::Listed
            && (enriched.embed_context.location.solar_system_id.is_none()
                || enriched
                    .embed_context
                    .location
                    .solar_system_name
                    .as_deref()
                    .is_none_or(|name| name.trim().is_empty())
                || (!location_is_solar_system
                    && enriched
                        .embed_context
                        .location
                        .location_name
                        .as_deref()
                        .is_none_or(|name| name.trim().is_empty())));
        let required_region_missing = self.esi.supports_region_name_lookup()
            && enriched
                .embed_context
                .location
                .region_name
                .as_deref()
                .is_none_or(|name| name.trim().is_empty());
        let needs_live_enrichment = (snapshot.is_none()
            && (event.kind == ContractEventKind::Listed || primary_name_missing))
            || incomplete_station_listing
            || incomplete_authoritative_listing_location
            || incomplete_terminal_location
            || required_region_missing;
        if needs_live_enrichment
            && self
                .context_request_allowed(event.contract.contract_id)
                .await?
        {
            let items = event
                .offered_items
                .iter()
                .chain(&event.requested_items)
                .cloned()
                .collect::<Vec<_>>();
            let limiter = PersistedContractContextLimiter {
                store: self.store.clone(),
                request_pacer: self.request_pacer.clone(),
            };
            let deadline = embed_enrichment_deadline
                .get_or_insert_with(|| self.esi.observed_embed_enrichment_deadline());
            match self
                .record_context_esi_result(
                    event.contract.contract_id,
                    self.esi
                        .observed_contract_embed_context_limited_until(
                            &event.contract,
                            &items,
                            &limiter,
                            *deadline,
                        )
                        .await,
                )
                .await?
            {
                Some(response) => {
                    let response = response.value.unwrap_or_default();
                    if event.kind == ContractEventKind::Listed {
                        enriched.embed_context.merge_missing_from(&response);
                    } else {
                        enriched
                            .embed_context
                            .merge_terminal_stable_facts_from(&response);
                    }
                    self.store
                        .save_observed_embed_context(
                            event.region_id,
                            event.contract.contract_id,
                            &enriched.embed_context,
                        )
                        .await?;
                }
                None => {
                    self.store
                        .record_failure(
                            Some(event.region_id),
                            Some(event.contract.contract_id),
                            Some(&embed_context_resource_key(event.contract.contract_id)),
                            "embed_context_enrichment",
                            "notification embed enrichment will be retried",
                            self.store.active_esi_limiter_deadline().await?,
                        )
                        .await?;
                }
            }
        }
        self.ensure_region_context_from_retained_sources(
            event.region_id,
            &mut enriched.embed_context,
        )
        .await?;
        if self.esi.supports_region_name_lookup()
            && enriched
                .embed_context
                .location
                .region_name
                .as_deref()
                .is_none_or(|name| name.trim().is_empty())
        {
            return Ok(None);
        }
        Ok(Some(enriched))
    }

    async fn snapshot_observed_contracts(
        &self,
        region_id: i64,
        observed: &[ObservedContract],
        remaining: usize,
        attempted_embed_enrichment_deadlines: &mut HashMap<i64, tokio::time::Instant>,
    ) -> Result<usize, ContractCollectionError> {
        let evidence_now = self.delivery_clock.now();
        let mut consumed = 0;
        for observed in observed {
            if let Some(mut context) = self
                .store
                .observed_embed_context(region_id, observed.contract.contract_id)
                .await?
            {
                let prior_location = context.location.clone();
                self.ensure_region_context_from_retained_sources(region_id, &mut context)
                    .await?;
                let required_region_missing = self.esi.supports_region_name_lookup()
                    && context
                        .location
                        .region_name
                        .as_deref()
                        .is_none_or(|name| name.trim().is_empty());
                let public_location =
                    context
                        .location
                        .solar_system_id
                        .and_then(|solar_system_id| {
                            public_location_facts(
                                observed.contract.start_location_id,
                                solar_system_id,
                                context.location.location_kind.as_deref(),
                            )
                        });
                let mut public_location_evidence_retained =
                    LocationEvidenceService::new(&self.store)
                        .current_public_npc(observed.contract.start_location_id, evidence_now)
                        .await?
                        .is_some();
                let retry_public_location_evidence = !public_location_evidence_retained
                    && public_location.is_some()
                    && self
                        .store
                        .public_location_evidence_retry_is_due(
                            region_id,
                            observed.contract.contract_id,
                            Utc::now(),
                        )
                        .await?;
                if !required_region_missing && retry_public_location_evidence {
                    if consumed >= remaining {
                        break;
                    }
                    consumed += 1;
                    let observed_at = context
                        .observed_at
                        .unwrap_or(evidence_now)
                        .min(evidence_now);
                    if let Some((station_id, solar_system_id)) = public_location {
                        match LocationEvidenceService::new(&self.store)
                            .record_public_npc(
                                observed.contract.start_location_id,
                                station_id,
                                solar_system_id,
                                context.location.region_id,
                                observed_at,
                                evidence_now,
                            )
                            .await
                        {
                            Ok(_) => {
                                public_location_evidence_retained = true;
                                self.store
                                    .resolve_collection_failures(
                                        Some(region_id),
                                        Some(observed.contract.contract_id),
                                        Some(&public_location_evidence_resource_key(
                                            observed.contract.contract_id,
                                        )),
                                        "embed_context_enrichment",
                                    )
                                    .await?;
                            }
                            Err(error) => {
                                warn!(
                                    region_id,
                                    contract_id = observed.contract.contract_id,
                                    location_id = observed.contract.start_location_id,
                                    "could not retry retained public location evidence: {error}"
                                );
                                self.store
                                    .record_public_location_evidence_failure(
                                        region_id,
                                        observed.contract.contract_id,
                                        "public location evidence retention will be retried",
                                        None,
                                    )
                                    .await?;
                            }
                        }
                    }
                }
                if !required_region_missing {
                    if context.location != prior_location {
                        self.store
                            .save_observed_embed_context(
                                region_id,
                                observed.contract.contract_id,
                                &context,
                            )
                            .await?;
                    }
                    self.store
                        .resolve_collection_failures(
                            Some(region_id),
                            Some(observed.contract.contract_id),
                            Some(&embed_context_resource_key(observed.contract.contract_id)),
                            "embed_context_enrichment",
                        )
                        .await?;
                    if public_location_evidence_retained {
                        self.store
                            .resolve_collection_failures(
                                Some(region_id),
                                Some(observed.contract.contract_id),
                                Some(&public_location_evidence_resource_key(
                                    observed.contract.contract_id,
                                )),
                                "embed_context_enrichment",
                            )
                            .await?;
                    }
                    continue;
                }
            }
            if consumed >= remaining {
                break;
            }
            if self.store.active_esi_limiter_deadline().await?.is_some() {
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
                request_pacer: self.request_pacer.clone(),
            };
            let deadline = *attempted_embed_enrichment_deadlines
                .entry(observed.contract.contract_id)
                .or_insert_with(|| self.esi.observed_embed_enrichment_deadline());
            match self
                .record_context_esi_result(
                    observed.contract.contract_id,
                    self.esi
                        .observed_contract_embed_context_limited_until(
                            &observed.contract,
                            &items,
                            &limiter,
                            deadline,
                        )
                        .await,
                )
                .await?
            {
                Some(response) => {
                    let mut context = response.value.unwrap_or_default();
                    self.ensure_region_context_from_retained_sources(region_id, &mut context)
                        .await?;
                    let mut public_location_evidence_retained = false;
                    if let Some((station_id, solar_system_id)) = context
                        .location
                        .solar_system_id
                        .and_then(|solar_system_id| {
                            public_location_facts(
                                observed.contract.start_location_id,
                                solar_system_id,
                                context.location.location_kind.as_deref(),
                            )
                        })
                    {
                        let observed_at = context
                            .observed_at
                            .unwrap_or(evidence_now)
                            .min(evidence_now);
                        match LocationEvidenceService::new(&self.store)
                            .record_public_npc(
                                observed.contract.start_location_id,
                                station_id,
                                solar_system_id,
                                context.location.region_id,
                                observed_at,
                                evidence_now,
                            )
                            .await
                        {
                            Ok(_) => public_location_evidence_retained = true,
                            Err(error) => {
                                warn!(
                                    region_id,
                                    contract_id = observed.contract.contract_id,
                                    location_id = observed.contract.start_location_id,
                                    "could not retain public location evidence before saving the embed snapshot: {error}"
                                );
                                if let Err(record_error) = self
                                    .store
                                    .record_public_location_evidence_failure(
                                        region_id,
                                        observed.contract.contract_id,
                                        "public location evidence retention will be retried",
                                        None,
                                    )
                                    .await
                                {
                                    warn!(
                                        region_id,
                                        contract_id = observed.contract.contract_id,
                                        "could not record public location retention failure: {record_error}"
                                    );
                                }
                            }
                        }
                    }
                    self.store
                        .save_observed_embed_context(
                            region_id,
                            observed.contract.contract_id,
                            &context,
                        )
                        .await?;
                    if public_location_evidence_retained
                        || LocationEvidenceService::new(&self.store)
                            .current_public_npc(observed.contract.start_location_id, evidence_now)
                            .await?
                            .is_some()
                    {
                        self.store
                            .resolve_collection_failures(
                                Some(region_id),
                                Some(observed.contract.contract_id),
                                Some(&public_location_evidence_resource_key(
                                    observed.contract.contract_id,
                                )),
                                "embed_context_enrichment",
                            )
                            .await?;
                    }
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
        wait_for_esi_request_admission(&self.store, &*self.request_pacer)
            .await
            .map_err(Into::into)
    }

    async fn record_esi_result<T>(
        &self,
        result: Result<EsiResponse<T>, EsiError>,
    ) -> Result<EsiResponse<T>, ContractCollectionError> {
        match result {
            Ok(response) => {
                self.store
                    .record_esi_limiter_at(&response.metadata, self.request_pacer.now())
                    .await?;
                Ok(response)
            }
            Err(error) => {
                self.store
                    .record_esi_limiter_at(&error.metadata, self.request_pacer.now())
                    .await?;
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
                self.store
                    .record_esi_limiter_at(probe.metadata(), self.request_pacer.now())
                    .await?;
                Ok(probe)
            }
            Err(error) => {
                self.store
                    .record_esi_limiter_at(&error.metadata, self.request_pacer.now())
                    .await?;
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

fn clear_location_evidence_context(event: &mut ContractEvent) {
    event.context.solar_system_id = None;
    event.context.solar_system_position = None;
    event.context.solar_system_resolution = ContractContextResolution::Indeterminate;
    event.context.solar_system_position_resolution = ContractContextResolution::Indeterminate;
    event.context.range_center_positions.clear();
    event.context.security_status = None;
    event.context.security_status_resolution = ContractContextResolution::Indeterminate;
    event.embed_context.location.location_name = None;
    event.embed_context.location.location_kind = None;
    event.embed_context.location.last_verified_at = None;
    event.embed_context.location.solar_system_id = None;
    event.embed_context.location.solar_system_name = None;
    event.embed_context.location.solar_system_position = None;
    event.embed_context.location.security_status = None;
    event.embed_context.location.region_id = None;
    event.embed_context.location.region_name = None;
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

fn is_want_to_buy_listing(event: &ContractEvent) -> bool {
    event.offered_items.is_empty()
        && !event.requested_items.is_empty()
        && positive_isk(event.contract.reward)
}

fn acceptance_event(
    region_id: i64,
    contract: &PublicContract,
    manifest: &ItemManifest,
    last_public_observed_at: DateTime<Utc>,
    absence_observed_at: DateTime<Utc>,
    evidence_response_at: DateTime<Utc>,
    provenance: ContractAcceptanceProvenance,
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
            provenance: Some(provenance),
        }),
        embed_context: ContractEmbedContext::default(),
    })
}

fn nonfinancial_terminal_event(
    resolution: &AwaitingContractResolution,
    state: ContractResolutionState,
    acceptance_evidence: Option<ContractAcceptanceEvidence>,
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
        acceptance_evidence,
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
            resolution.acceptance_provenance?,
        ),
        ContractResolutionState::Expired | ContractResolutionState::ClosedOutcomeUnknown => {
            let acceptance_evidence =
                resolution
                    .acceptance_evidence_at
                    .map(|evidence_response_at| ContractAcceptanceEvidence {
                        last_public_observed_at: resolution.last_public_observed_at,
                        absence_observed_at: resolution.absence_observed_at,
                        evidence_response_at,
                        provenance: resolution.acceptance_provenance,
                    });
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
                acceptance_evidence,
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
    UnresolvedProximity,
}

struct ContractFilterEvaluator<'a> {
    event: &'a ContractEvent,
    ship_groups: &'a dyn ShipGroupResolver,
    group_cache: HashMap<i64, ShipGroupLookup>,
    matching_ship_items: HashSet<(ContractItemDirection, i64)>,
    matching_ship_items_deferred: bool,
    matched_range: Option<MatchedContractRange>,
}

#[derive(Clone)]
struct MatchedContractRange {
    light_years: f64,
    reference_system_id: u32,
    reference_system_name: String,
}

fn prefer_nearer_contract_range(
    current: Option<MatchedContractRange>,
    candidate: Option<MatchedContractRange>,
) -> Option<MatchedContractRange> {
    match (current, candidate) {
        (Some(current), Some(candidate))
            if candidate.light_years < current.light_years
                || (candidate.light_years == current.light_years
                    && candidate.reference_system_id < current.reference_system_id) =>
        {
            Some(candidate)
        }
        (Some(current), _) => Some(current),
        (None, candidate) => candidate,
    }
}

impl<'a> ContractFilterEvaluator<'a> {
    fn new(event: &'a ContractEvent, ship_groups: &'a dyn ShipGroupResolver) -> Self {
        Self {
            event,
            ship_groups,
            group_cache: HashMap::new(),
            matching_ship_items: HashSet::new(),
            matching_ship_items_deferred: false,
            matched_range: None,
        }
    }

    async fn matches(&mut self, node: &ContractFilterNode) -> ContractFilterMatch {
        self.matching_ship_items.clear();
        self.matching_ship_items_deferred = false;
        self.matched_range = None;
        evaluate_contract_filter_node(node.clone(), self, true).await
    }

    fn matched_range(&self) -> Option<MatchedContractRange> {
        self.matched_range.clone()
    }

    async fn group_for_type(&mut self, type_id: i64) -> ShipGroupLookup {
        if let Some(group) = self.group_cache.get(&type_id).copied() {
            return group;
        }
        let group = self.ship_groups.group_for_type(type_id).await;
        self.group_cache.insert(type_id, group);
        group
    }

    async fn primary_display_item(
        &mut self,
        required_direction: Option<ContractItemDirection>,
    ) -> PrimaryDisplayItem<'a> {
        let presentation_ship_items = contract_presentation_items(self.event, required_direction)
            .map(|(direction, item)| (direction, item.record_id))
            .collect::<HashSet<_>>();
        self.primary_display_item_for(&presentation_ship_items)
            .await
    }

    async fn primary_display_item_for(
        &mut self,
        matching_ship_items: &HashSet<(ContractItemDirection, i64)>,
    ) -> PrimaryDisplayItem<'a> {
        if self.matching_ship_items_deferred {
            return PrimaryDisplayItem::Deferred;
        }
        let mut resolved_items = Vec::new();
        for (direction, item) in contract_items_with_direction(self.event) {
            if !matching_ship_items.contains(&(direction, item.record_id)) {
                continue;
            }
            let priority = match self.group_for_type(item.type_id).await {
                ShipGroupLookup::Resolved(Some(group_id)) => {
                    strategic_ship_group_priority(group_id)
                }
                ShipGroupLookup::Resolved(None) => continue,
                ShipGroupLookup::TemporarilyUnavailable => return PrimaryDisplayItem::Deferred,
            };
            resolved_items.push((item, priority));
        }
        let primary = canonical_contract_primary_item(resolved_items);
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

    fn has_matching_ship_items(&self) -> bool {
        !self.matching_ship_items.is_empty()
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

    fn has_explicit_ship_items(&self) -> bool {
        !self.confirmed_ship_items.is_empty() || !self.potential_ship_items.is_empty()
    }

    fn has_ship_item_in_direction(&self, direction: ContractItemDirection) -> bool {
        self.confirmed_ship_items
            .iter()
            .chain(&self.potential_ship_items)
            .any(|(item_direction, _)| *item_direction == direction)
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
                if matches!(result, ContractFilterMatch::Matched)
                    && matches!(condition, ContractFilterCondition::LyRangeFrom(_))
                {
                    return ContractFilterContextPlan {
                        result,
                        requirements: condition.context_requirements(),
                        waits_for_non_context: false,
                        confirmed_ship_items: HashSet::new(),
                        potential_ship_items: HashSet::new(),
                    };
                }
                if !matches!(
                    result,
                    ContractFilterMatch::Deferred | ContractFilterMatch::UnresolvedProximity
                ) {
                    return ContractFilterContextPlan::resolved(result);
                }
                let requirements = condition.context_requirements();
                let waits_for_non_context = requirements.is_empty();
                ContractFilterContextPlan {
                    result: ContractFilterMatch::Deferred,
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
                        ContractFilterMatch::UnresolvedProximity => {
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
                        ContractFilterMatch::Deferred
                        | ContractFilterMatch::UnresolvedProximity => deferred_plans.push(plan),
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
                        ContractFilterMatch::UnresolvedProximity => ContractFilterMatch::Deferred,
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
                let initial_range = evaluator.matched_range.clone();
                let mut deferred = false;
                let mut unresolved_proximity = false;
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
                            evaluator.matched_range = initial_range.clone();
                            return ContractFilterMatch::Unmatched;
                        }
                        ContractFilterMatch::Deferred => deferred = true,
                        ContractFilterMatch::UnresolvedProximity => unresolved_proximity = true,
                    }
                }
                if deferred || unresolved_proximity {
                    let branch_may_add_ship_items = collect_matching_ship_items
                        && evaluator.matching_ship_items != initial_matches;
                    evaluator.matching_ship_items = initial_matches;
                    evaluator.matching_ship_items_deferred = initial_matches_deferred
                        || evaluator.matching_ship_items_deferred
                        || branch_may_add_ship_items;
                    evaluator.matched_range = initial_range;
                    if deferred {
                        ContractFilterMatch::Deferred
                    } else {
                        ContractFilterMatch::UnresolvedProximity
                    }
                } else {
                    ContractFilterMatch::Matched
                }
            }
            ContractFilterNode::Or(nodes) => {
                let initial_matches = evaluator.matching_ship_items.clone();
                let initial_matches_deferred = evaluator.matching_ship_items_deferred;
                let initial_range = evaluator.matched_range.clone();
                let mut matching_ship_items = initial_matches.clone();
                let mut matching_ship_items_deferred = initial_matches_deferred;
                let mut matched_range = initial_range.clone();
                let mut unresolved_ship_items = false;
                let mut matched = false;
                let mut deferred = false;
                let mut unresolved_proximity = false;
                for node in nodes {
                    evaluator.matching_ship_items = initial_matches.clone();
                    evaluator.matching_ship_items_deferred = initial_matches_deferred;
                    evaluator.matched_range = initial_range.clone();
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
                            matched_range = prefer_nearer_contract_range(
                                matched_range,
                                evaluator.matched_range.clone(),
                            );
                        }
                        ContractFilterMatch::Unmatched => {}
                        ContractFilterMatch::Deferred => {
                            deferred = true;
                            unresolved_ship_items |= evaluator.matching_ship_items_deferred
                                || (collect_matching_ship_items
                                    && evaluator.matching_ship_items != initial_matches);
                        }
                        ContractFilterMatch::UnresolvedProximity => {
                            unresolved_proximity = true;
                            unresolved_ship_items |= evaluator.matching_ship_items_deferred
                                || (collect_matching_ship_items
                                    && evaluator.matching_ship_items != initial_matches);
                        }
                    }
                }
                if matched {
                    evaluator.matching_ship_items = matching_ship_items;
                    evaluator.matching_ship_items_deferred = matching_ship_items_deferred;
                    evaluator.matched_range = matched_range;
                    ContractFilterMatch::Matched
                } else if deferred || unresolved_proximity {
                    evaluator.matching_ship_items = initial_matches;
                    evaluator.matching_ship_items_deferred =
                        matching_ship_items_deferred || unresolved_ship_items;
                    evaluator.matched_range = initial_range;
                    if deferred {
                        ContractFilterMatch::Deferred
                    } else {
                        ContractFilterMatch::UnresolvedProximity
                    }
                } else {
                    evaluator.matching_ship_items = initial_matches;
                    evaluator.matching_ship_items_deferred = initial_matches_deferred;
                    evaluator.matched_range = initial_range;
                    ContractFilterMatch::Unmatched
                }
            }
            ContractFilterNode::Not(node) => {
                let initial_range = evaluator.matched_range.clone();
                let result = match evaluate_contract_filter_node(*node, evaluator, false).await {
                    ContractFilterMatch::Matched => ContractFilterMatch::Unmatched,
                    ContractFilterMatch::Unmatched => ContractFilterMatch::Matched,
                    ContractFilterMatch::Deferred => ContractFilterMatch::Deferred,
                    ContractFilterMatch::UnresolvedProximity => {
                        ContractFilterMatch::UnresolvedProximity
                    }
                };
                evaluator.matched_range = initial_range;
                result
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
        ContractFilterCondition::SolarSystems(ids) => proximity_context_match(
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
                return ContractFilterMatch::UnresolvedProximity;
            };
            let mut deferred = false;
            let mut matched = false;
            let mut matched_range: Option<MatchedContractRange> = None;
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
                let light_years = event_position.distance_in_light_years(center_position);
                if light_years <= range.range {
                    matched = true;
                    if let Some(reference_system_name) = event
                        .context
                        .range_center_names
                        .get(&range.system_id)
                        .map(|name| sanitize_contract_text(name))
                        .filter(|name| !name.is_empty())
                    {
                        let candidate = MatchedContractRange {
                            light_years,
                            reference_system_id: range.system_id,
                            reference_system_name,
                        };
                        if matched_range.as_ref().is_none_or(|current| {
                            candidate.light_years < current.light_years
                                || (candidate.light_years == current.light_years
                                    && candidate.reference_system_id < current.reference_system_id)
                        }) {
                            matched_range = Some(candidate);
                        }
                    }
                }
            }
            if matched {
                if let Some(matched_range) = matched_range {
                    evaluator.matched_range = prefer_nearer_contract_range(
                        evaluator.matched_range.clone(),
                        Some(matched_range),
                    );
                }
                ContractFilterMatch::Matched
            } else if deferred {
                ContractFilterMatch::UnresolvedProximity
            } else {
                ContractFilterMatch::Unmatched
            }
        }
        ContractFilterCondition::SecurityRange { min, max } => proximity_context_match(
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

fn proximity_context_match<T>(
    value: Option<T>,
    resolution: ContractContextResolution,
    predicate: impl FnOnce(T) -> bool,
) -> ContractFilterMatch {
    match value {
        Some(value) => bool_match(predicate(value)),
        None if matches!(resolution, ContractContextResolution::DefinitivelyAbsent) => {
            ContractFilterMatch::Unmatched
        }
        None => ContractFilterMatch::UnresolvedProximity,
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

fn contract_presentation_direction(kind: ContractEventKind) -> Option<ContractItemDirection> {
    match kind {
        ContractEventKind::SaleConfirmed => Some(ContractItemDirection::Offered),
        ContractEventKind::PurchaseConfirmed => Some(ContractItemDirection::Requested),
        ContractEventKind::Listed
        | ContractEventKind::Expired
        | ContractEventKind::ClosedOutcomeUnknown => None,
    }
}

fn contract_presentation_items(
    event: &ContractEvent,
    required_direction: Option<ContractItemDirection>,
) -> impl Iterator<Item = (ContractItemDirection, &PublicContractItem)> {
    contract_items_with_direction(event).filter(move |(direction, _)| {
        required_direction.is_none_or(|required| *direction == required)
    })
}

fn public_location_facts(
    location_id: i64,
    solar_system_id: i64,
    location_kind: Option<&str>,
) -> Option<(Option<i64>, i64)> {
    if location_id == solar_system_id {
        return Some((None, solar_system_id));
    }
    (location_kind.is_none_or(|kind| kind == "Station"))
        .then_some((Some(location_id), solar_system_id))
}

fn event_supports_presentation_direction(
    event: &ContractEvent,
    direction: ContractItemDirection,
) -> bool {
    !items_for_direction(event, direction).is_empty()
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
    proximity_unverified: bool,
) -> ContractNotificationMessage {
    let primary_name = primary_item
        .and_then(|item| event.embed_context.item_names.get(&item.type_id))
        .map(|name| sanitize_contract_text(name))
        .or_else(|| primary_item.map(|_| "Public contract".to_string()))
        .unwrap_or_else(|| "Public contract".to_string());
    let title_location = contract_title_location(&event.embed_context.location);
    let title_isk = title_isk(event, primary_item);
    let title = contract_embed_title(
        &primary_name,
        event.kind,
        title_isk.as_deref(),
        &title_location,
    );
    let has_history = has_party_history(issuer_history) || has_party_history(corporation_history);
    let history_suffix = if has_history {
        " • 90-day history"
    } else {
        ""
    };
    let footer = format!("Contract #{}{history_suffix}", event.contract.contract_id);
    let mut description = vec![contract_address(
        event.contract.contract_id,
        &primary_name,
        contract_link_place(
            &event.embed_context.location,
            event.contract.start_location_id,
        )
        .as_str(),
    )];
    if let Some(location) = compact_contract_location_description(
        &event.embed_context.location,
        event.contract.start_location_id,
        event.embed_context.matched_range.as_ref(),
    ) {
        description.push(location);
    }
    let mut message = ContractNotificationMessage {
        title,
        description: Some(bounded_text(
            &description.join("\n"),
            MAX_EMBED_DESCRIPTION_CHARACTERS,
        )),
        fields: Vec::new(),
        presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
        author: Some(
            compact_issuer(&event.embed_context)
                .map(|issuer| format!("Public Contract\n{issuer}"))
                .unwrap_or_else(|| "Public Contract".to_string()),
        ),
        thumbnail_url: primary_item.map(|item| {
            format!(
                "https://images.evetech.net/types/{}/icon?size=64",
                item.type_id
            )
        }),
        footer: Some(footer),
        timestamp: match event.kind {
            ContractEventKind::Listed => Some(event.contract.date_issued),
            ContractEventKind::SaleConfirmed | ContractEventKind::PurchaseConfirmed => event
                .acceptance_evidence
                .as_ref()
                .map(|evidence| evidence.evidence_response_at),
            ContractEventKind::Expired | ContractEventKind::ClosedOutcomeUnknown => event
                .acceptance_evidence
                .as_ref()
                .map(|evidence| evidence.absence_observed_at),
        },
    };
    if matches!(
        event.kind,
        ContractEventKind::SaleConfirmed | ContractEventKind::PurchaseConfirmed
    ) {
        message.fields.push(ContractEmbedField {
            name: "Listing time".to_string(),
            value: discord_absolute_timestamp(event.contract.date_issued),
            inline: false,
        });
        if let Some(evidence) = &event.acceptance_evidence {
            message.fields.push(ContractEmbedField {
                name: "Observed acceptance window".to_string(),
                value: format!(
                    "**after** {}\n**before** {}",
                    discord_absolute_timestamp(evidence.last_public_observed_at),
                    discord_absolute_timestamp(evidence.absence_observed_at),
                ),
                inline: false,
            });
            message.fields.push(ContractEmbedField {
                name: "ESI result".to_string(),
                value: format!(
                    "Contract accepted by another player\nESI confirmed {}",
                    discord_relative_timestamp(evidence.evidence_response_at),
                ),
                inline: false,
            });
        }
    }
    if matches!(
        event.kind,
        ContractEventKind::Expired | ContractEventKind::ClosedOutcomeUnknown
    ) {
        if let Some(evidence) = event.acceptance_evidence.as_ref() {
            let window_name = match event.kind {
                ContractEventKind::ClosedOutcomeUnknown => "Observed closure window",
                ContractEventKind::Expired => "Observed expiry window",
                _ => unreachable!("only nonfinancial terminal events reach this branch"),
            };
            message.fields.push(ContractEmbedField {
                name: "Listing time".to_string(),
                value: discord_absolute_relative_timestamps(event.contract.date_issued),
                inline: false,
            });
            message.fields.push(ContractEmbedField {
                name: window_name.to_string(),
                value: format!(
                    "**after** {}\n**before** {}",
                    discord_absolute_relative_timestamps(evidence.last_public_observed_at),
                    discord_absolute_relative_timestamps(evidence.absence_observed_at),
                ),
                inline: false,
            });
            if evidence.provenance == Some(ContractAcceptanceProvenance::NoContent) {
                let semantic_esi_result = match event.kind {
                    ContractEventKind::ClosedOutcomeUnknown => {
                        "Public listing absent before expiry boundary"
                    }
                    ContractEventKind::Expired => "Public listing absent after expiry boundary",
                    _ => unreachable!("only nonfinancial terminal events reach this branch"),
                };
                message.fields.push(ContractEmbedField {
                    name: "ESI result".to_string(),
                    value: semantic_esi_result.to_string(),
                    inline: false,
                });
            }
        }
    }
    if has_history {
        let mut history = Vec::new();
        if has_party_history(issuer_history) {
            history.push(format!("Issuer: {}", format_party_history(issuer_history)));
        }
        if has_party_history(corporation_history) {
            history.push(format!(
                "Corp: {}",
                format_party_history(corporation_history)
            ));
        }
        message.fields.push(ContractEmbedField {
            name: "History".to_string(),
            value: history.join("\n"),
            inline: false,
        });
    }
    if proximity_unverified {
        message.fields.push(ContractEmbedField {
            name: "Alert".to_string(),
            value: "Proximity-Unverified".to_string(),
            inline: false,
        });
    }
    if event.acceptance_evidence.is_none()
        && matches!(
            event.kind,
            ContractEventKind::Expired | ContractEventKind::ClosedOutcomeUnknown
        )
    {
        message.fields.push(ContractEmbedField {
            name: "Evidence".to_string(),
            value: match event.kind {
                ContractEventKind::Expired => {
                    format!(
                        "Expired {}",
                        discord_absolute_timestamp(event.contract.date_expired)
                    )
                }
                ContractEventKind::ClosedOutcomeUnknown => {
                    "No longer public; closure time unknown".to_string()
                }
                _ => unreachable!("only terminal events without acceptance evidence reach here"),
            },
            inline: false,
        });
    }
    if event.kind == ContractEventKind::Listed {
        bound_listing_embed_message(&mut message);
    } else {
        bound_terminal_embed_message(&mut message);
    }
    bound_embed_message(&mut message);
    message
}

fn repaired_contract_message(
    stored: &ContractNotificationMessage,
    regenerated: &ContractNotificationMessage,
    event_kind: ContractEventKind,
) -> ContractNotificationMessage {
    let proximity = stored
        .fields
        .iter()
        .find(|field| {
            field.name == "Alert"
                && matches!(
                    field.value.as_str(),
                    "Proximity-Unverified" | "Proximity-Verified: Out of Range"
                )
        })
        .cloned();
    let mut message = regenerated.clone();
    if let Some(proximity) = proximity {
        message.fields.retain(|field| field.name != "Alert");
        message.fields.push(proximity);
    }
    if event_kind == ContractEventKind::Listed {
        bound_listing_embed_message(&mut message);
    } else {
        bound_terminal_embed_message(&mut message);
    }
    bound_embed_message(&mut message);
    message
}

fn contract_matched_range_for_embed(
    range: MatchedContractRange,
    location: &ContractLocationContext,
) -> Option<ContractMatchedRange> {
    let destination_system_id = location
        .solar_system_id
        .and_then(|id| u32::try_from(id).ok())?;
    let destination_system_name = location
        .solar_system_name
        .as_deref()
        .map(sanitize_contract_text)
        .filter(|name| !name.is_empty())?;
    Some(ContractMatchedRange {
        light_years: range.light_years,
        reference_system_id: range.reference_system_id,
        reference_system_name: range.reference_system_name,
        destination_system_id,
        destination_system_name,
    })
}

fn canonical_contract_primary_item<'a>(
    candidates: impl IntoIterator<Item = (&'a PublicContractItem, usize)>,
) -> Option<(&'a PublicContractItem, usize)> {
    candidates
        .into_iter()
        .min_by_key(|(item, priority)| (*priority, item.type_id, item.record_id))
}

async fn historical_contract_primary_item<'a>(
    event: &'a ContractEvent,
    ship_groups: &dyn ShipGroupResolver,
) -> Result<Option<&'a PublicContractItem>, sqlx::Error> {
    let mut resolved_items = Vec::new();
    for (_, item) in contract_presentation_items(event, contract_presentation_direction(event.kind))
    {
        let group_id = match ship_groups.group_for_type(item.type_id).await {
            ShipGroupLookup::Resolved(Some(group_id)) => group_id,
            ShipGroupLookup::Resolved(None) => {
                return Err(sqlx::Error::Protocol(format!(
                    "historical contract presentation lacks cached ship group for type {}",
                    item.type_id
                )));
            }
            ShipGroupLookup::TemporarilyUnavailable => {
                return Err(sqlx::Error::Protocol(format!(
                    "historical contract presentation ship group is unavailable for type {}",
                    item.type_id
                )));
            }
        };
        resolved_items.push((item, strategic_ship_group_priority(group_id)));
    }
    Ok(canonical_contract_primary_item(resolved_items).map(|(item, _)| item))
}

enum RetainedRangeMatch {
    Matched(Option<RetainedMatchedRange>),
    Unmatched,
    Unresolved,
}

struct RetainedMatchedRange {
    light_years: f64,
    reference_system_id: u32,
}

fn retained_direct_positive_range(
    event: &ContractEvent,
    filter: &ContractFilter,
) -> Option<RetainedMatchedRange> {
    let ContractFilterNode::And(nodes) = &filter.root else {
        return None;
    };
    let mut matched_range: Option<RetainedMatchedRange> = None;
    let mut has_direct_range = false;
    for node in nodes {
        let ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(ranges)) = node
        else {
            continue;
        };
        has_direct_range = true;
        match retained_light_year_range_match(event, ranges) {
            RetainedRangeMatch::Matched(Some(range)) => {
                matched_range = match matched_range {
                    Some(current)
                        if range.light_years > current.light_years
                            || (range.light_years == current.light_years
                                && range.reference_system_id >= current.reference_system_id) =>
                    {
                        Some(current)
                    }
                    _ => Some(range),
                };
            }
            RetainedRangeMatch::Matched(None) => {}
            RetainedRangeMatch::Unmatched | RetainedRangeMatch::Unresolved => return None,
        }
    }
    has_direct_range.then_some(matched_range).flatten()
}

fn retained_light_year_range_match(
    event: &ContractEvent,
    ranges: &[SystemRange],
) -> RetainedRangeMatch {
    let Some(event_position) = event
        .context
        .solar_system_position
        .filter(|position| position.is_finite())
    else {
        return RetainedRangeMatch::Unresolved;
    };
    let mut matched = false;
    let mut unresolved = false;
    let mut matched_range: Option<RetainedMatchedRange> = None;
    for range in ranges {
        let Some(center_position) = event
            .context
            .range_center_positions
            .get(&range.system_id)
            .copied()
            .filter(|position| position.is_finite())
        else {
            unresolved = true;
            continue;
        };
        let light_years = event_position.distance_in_light_years(center_position);
        if light_years > range.range {
            continue;
        }
        matched = true;
        let candidate = RetainedMatchedRange {
            light_years,
            reference_system_id: range.system_id,
        };
        matched_range = match matched_range {
            Some(current)
                if candidate.light_years > current.light_years
                    || (candidate.light_years == current.light_years
                        && candidate.reference_system_id >= current.reference_system_id) =>
            {
                Some(current)
            }
            _ => Some(candidate),
        };
    }
    if matched {
        RetainedRangeMatch::Matched(matched_range)
    } else if unresolved {
        RetainedRangeMatch::Unresolved
    } else {
        RetainedRangeMatch::Unmatched
    }
}

fn proximity_out_of_range_contract_message(
    stored: &ContractNotificationMessage,
) -> ContractNotificationMessage {
    let mut message = stored.clone();
    message.fields.retain(|field| field.name != "Alert");
    message.fields.push(ContractEmbedField {
        name: "Alert".to_string(),
        value: "Proximity-Verified: Out of Range".to_string(),
        inline: false,
    });
    bound_embed_message(&mut message);
    message
}

const MAX_EMBED_TITLE_CHARACTERS: usize = 256;
const MAX_EMBED_DESCRIPTION_CHARACTERS: usize = 4096;
const MAX_EMBED_AUTHOR_NAME_CHARACTERS: usize = 256;
const MAX_EMBED_FIELD_NAME_CHARACTERS: usize = 256;
const MAX_EMBED_FIELD_VALUE_CHARACTERS: usize = 1024;
const MAX_EMBED_CHARACTERS: usize = 6000;
const MAX_EMBED_FIELDS: usize = 25;
const MAX_LISTING_EMBED_FIELDS: usize = 3;
const MAX_LISTING_CONTENT_LINES: usize = 7;
const MAX_TERMINAL_EMBED_FIELDS: usize = 5;
const MAX_TERMINAL_CONTENT_LINES: usize = 12;

fn contract_embed_title(
    primary_name: &str,
    kind: ContractEventKind,
    title_isk: Option<&str>,
    location: &str,
) -> String {
    let definitive_title = match kind {
        ContractEventKind::SaleConfirmed => {
            title_isk.map(|isk| format!("{primary_name} contract accepted • {isk}"))
        }
        ContractEventKind::PurchaseConfirmed => {
            title_isk.map(|isk| format!("{primary_name} buy contract accepted • {isk}"))
        }
        ContractEventKind::Expired => Some(format!("{primary_name} contract expired")),
        ContractEventKind::ClosedOutcomeUnknown => {
            Some(format!("{primary_name} contract closed • outcome unknown"))
        }
        ContractEventKind::Listed => None,
    };
    if let Some(title) =
        definitive_title.filter(|title| title.chars().count() <= MAX_EMBED_TITLE_CHARACTERS)
    {
        return title;
    }
    let action = match kind {
        ContractEventKind::Listed => "listed",
        ContractEventKind::SaleConfirmed => "contract accepted",
        ContractEventKind::PurchaseConfirmed => "buy contract accepted",
        ContractEventKind::Expired => "contract expired",
        ContractEventKind::ClosedOutcomeUnknown => "contract closed • outcome unknown",
    };
    let with_isk = title_isk.map(|isk| format!("{primary_name} {action} for {isk}{location}"));
    if let Some(title) =
        with_isk.filter(|title| title.chars().count() <= MAX_EMBED_TITLE_CHARACTERS)
    {
        return title;
    }
    bounded_text(
        &format!("{primary_name} {action}{location}"),
        MAX_EMBED_TITLE_CHARACTERS,
    )
}

fn title_isk(event: &ContractEvent, primary_item: Option<&PublicContractItem>) -> Option<String> {
    if !matches!(
        event.kind,
        ContractEventKind::Listed
            | ContractEventKind::SaleConfirmed
            | ContractEventKind::PurchaseConfirmed
    ) {
        return None;
    }
    let value = contract_display_isk(event, primary_item)?;
    positive_isk(value).then(|| format_compact_isk(value))
}

fn contract_display_isk(
    event: &ContractEvent,
    primary_item: Option<&PublicContractItem>,
) -> Option<f64> {
    let value = match primary_item {
        Some(item) if item.is_included => event.contract.price,
        Some(_) => event.contract.reward,
        None => event.contract.price.max(event.contract.reward),
    };
    positive_isk(value).then_some(value)
}

fn contract_title_location(context: &ContractLocationContext) -> String {
    match (
        context.solar_system_name.as_deref(),
        context.region_name.as_deref(),
    ) {
        (Some(system), Some(region)) => {
            format!(
                " in {} ({})",
                sanitize_contract_text(system),
                sanitize_contract_text(region)
            )
        }
        (Some(system), None) => format!(" in {}", sanitize_contract_text(system)),
        (None, Some(region)) => format!(" in {}", sanitize_contract_text(region)),
        (None, None) => String::new(),
    }
}

fn compact_contract_location_description(
    context: &ContractLocationContext,
    location_id: i64,
    matched_range: Option<&ContractMatchedRange>,
) -> Option<String> {
    let system_name = context
        .solar_system_name
        .as_deref()
        .map(sanitize_contract_text)
        .filter(|name| !name.is_empty());
    let system = system_name
        .as_deref()
        .zip(
            context
                .solar_system_id
                .and_then(|id| u32::try_from(id).ok()),
        )
        .map(|(name, id)| LocationSystem { name, id });
    let region_name = contract_region_display_name(context);
    let region = region_name
        .as_deref()
        .zip(context.region_id.and_then(|id| u32::try_from(id).ok()))
        .map(|(name, id)| LocationRegion { name, id });
    let location_name = context
        .location_name
        .as_deref()
        .map(sanitize_contract_text)
        .filter(|name| !name.is_empty());
    let on = location_name
        .as_deref()
        .zip(u64::try_from(location_id).ok())
        .map(|(name, id)| LocationOn {
            name,
            id,
            suffix: None,
        });
    let range = matched_range.map(|range| LocationRange {
        light_years: range.light_years,
        reference_system: &range.reference_system_name,
        destination_system: &range.destination_system_name,
    });
    let description = compact_location_description(CompactLocation {
        system,
        region,
        on,
        range,
    });
    let mut description = (!description.is_empty()).then_some(description)?;
    if let Some(verified_at) = context.last_verified_at {
        description.push_str("\n**on:** Player-owned structure");
        description.push_str(&format!(
            "\n**location verified:** {}",
            verified_at.format("%Y-%m-%d")
        ));
    }
    Some(description)
}

fn contract_link_place(context: &ContractLocationContext, location_id: i64) -> String {
    context
        .solar_system_name
        .as_deref()
        .map(sanitize_contract_text)
        .filter(|place| !place.is_empty())
        .or_else(|| {
            context
                .location_name
                .as_deref()
                .map(sanitize_contract_text)
                .filter(|place| !place.is_empty())
        })
        .or_else(|| {
            context
                .region_name
                .as_deref()
                .map(sanitize_contract_text)
                .filter(|place| !place.is_empty())
        })
        .unwrap_or_else(|| unresolved_contract_location(location_id).to_string())
}

fn contract_region_display_name(context: &ContractLocationContext) -> Option<String> {
    context
        .region_name
        .as_deref()
        .map(sanitize_contract_text)
        .filter(|name| !name.is_empty())
}

fn unresolved_contract_location(location_id: i64) -> &'static str {
    if u32::try_from(location_id).is_err() {
        "Player-owned structure"
    } else {
        "Unknown location"
    }
}

fn contract_address(contract_id: i64, label: &str, place: &str) -> String {
    let label = bounded_text(label, 160);
    let place = bounded_text(place, 160);
    format!("`<url=\"contract:0//{contract_id}\">{label} - {place}</url>`")
}

fn compact_issuer(context: &ContractEmbedContext) -> Option<String> {
    let character = context
        .issuer_character_name
        .as_deref()
        .map(sanitize_contract_identity)
        .filter(|name| !name.is_empty())?;
    let affiliation = context
        .issuer_alliance_name
        .as_deref()
        .map(sanitize_contract_identity)
        .filter(|name| !name.is_empty())
        .or_else(|| {
            context
                .issuer_corporation_name
                .as_deref()
                .map(sanitize_contract_identity)
                .filter(|name| !name.is_empty())
        });
    let identity = affiliation
        .map(|affiliation| format!("[{affiliation}] {character}"))
        .unwrap_or(character);
    Some(format!("issuer: {identity}"))
}

fn has_party_history(history: &ContractPartyHistory) -> bool {
    history.confirmed_sales > 0
        || history.confirmed_purchases > 0
        || history.unknown_closures > 0
        || history.most_recent_confirmed.is_some()
}

fn format_compact_isk(value: f64) -> String {
    format!("{} ISK", format_compact_isk_short(value))
}

fn format_compact_isk_short(value: f64) -> String {
    let (scaled, suffix) = if value >= 1_000_000_000.0 {
        (value / 1_000_000_000.0, "B")
    } else if value >= 1_000_000.0 {
        (value / 1_000_000.0, "M")
    } else if value >= 1_000.0 {
        (value / 1_000.0, "K")
    } else {
        (value, "")
    };
    format!("{}{}", trim_decimal(scaled), suffix)
}

fn trim_decimal(value: f64) -> String {
    let rendered = format!("{value:.2}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn format_party_history(history: &ContractPartyHistory) -> String {
    let mut entries = Vec::new();
    if history.confirmed_sales > 0 {
        entries.push(format!("{} sales", history.confirmed_sales));
    }
    if history.confirmed_purchases > 0 {
        entries.push(format!("{} purchases", history.confirmed_purchases));
    }
    if history.unknown_closures > 0 {
        entries.push(format!("{} unknown", history.unknown_closures));
    }
    if let Some(latest) = &history.most_recent_confirmed {
        entries.push(format!(
            "Latest confirmation {}",
            discord_short_date_timestamp(latest.observed_at)
        ));
    }
    entries.join(" • ")
}

fn discord_absolute_timestamp(value: DateTime<Utc>) -> String {
    let timestamp = value.timestamp();
    format!("<t:{timestamp}:F>")
}

fn discord_relative_timestamp(value: DateTime<Utc>) -> String {
    let timestamp = value.timestamp();
    format!("<t:{timestamp}:R>")
}

fn discord_absolute_relative_timestamps(value: DateTime<Utc>) -> String {
    format!(
        "{}\n{}",
        discord_absolute_timestamp(value),
        discord_relative_timestamp(value)
    )
}

fn discord_short_date_timestamp(value: DateTime<Utc>) -> String {
    let timestamp = value.timestamp();
    format!("<t:{timestamp}:d>")
}

fn bound_listing_embed_message(message: &mut ContractNotificationMessage) {
    message
        .fields
        .retain(|field| !field.value.trim().is_empty());
    message.fields.truncate(MAX_LISTING_EMBED_FIELDS);
    while listing_content_lines(message) > MAX_LISTING_CONTENT_LINES {
        if message.fields.pop().is_none() {
            break;
        }
    }
}

fn listing_content_lines(message: &ContractNotificationMessage) -> usize {
    message
        .description
        .as_deref()
        .map(str::lines)
        .map(Iterator::count)
        .unwrap_or(0)
        + message
            .fields
            .iter()
            .map(|field| field.value.lines().count())
            .sum::<usize>()
}

fn bound_terminal_embed_message(message: &mut ContractNotificationMessage) {
    message
        .fields
        .retain(|field| !field.value.trim().is_empty());
    message.fields.truncate(MAX_TERMINAL_EMBED_FIELDS);
    while listing_content_lines(message) > MAX_TERMINAL_CONTENT_LINES {
        let Some(history) = message
            .fields
            .iter_mut()
            .find(|field| field.name == "History")
        else {
            break;
        };
        if history.value.contains('\n') {
            history.value = history.value.replace('\n', " • ");
        } else {
            message.fields.retain(|field| field.name != "History");
        }
    }
}

fn bound_embed_message(message: &mut ContractNotificationMessage) {
    message.title = bounded_text(&message.title, MAX_EMBED_TITLE_CHARACTERS);
    message.description = message
        .description
        .as_deref()
        .map(|description| bounded_text(description, MAX_EMBED_DESCRIPTION_CHARACTERS));
    message.author = message
        .author
        .as_deref()
        .map(|author| bounded_text(author, MAX_EMBED_AUTHOR_NAME_CHARACTERS));
    for field in &mut message.fields {
        field.name = bounded_text(&field.name, MAX_EMBED_FIELD_NAME_CHARACTERS);
        field.value = bounded_text(&field.value, MAX_EMBED_FIELD_VALUE_CHARACTERS);
    }
    message.footer = message
        .footer
        .as_deref()
        .map(|footer| bounded_text(footer, 2048));
    message.fields.truncate(MAX_EMBED_FIELDS);
    if embed_character_count(message) <= MAX_EMBED_CHARACTERS {
        return;
    }

    while embed_character_count(message) > MAX_EMBED_CHARACTERS {
        if message.fields.pop().is_none() {
            break;
        }
    }
    if embed_character_count(message) > MAX_EMBED_CHARACTERS {
        let excess = embed_character_count(message) - MAX_EMBED_CHARACTERS;
        if let Some(footer) = &message.footer {
            let footer_budget = footer.chars().count().saturating_sub(excess);
            message.footer = (footer_budget > 0).then(|| bounded_text(footer, footer_budget));
        }
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
            .author
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

fn sanitize_contract_text(value: &str) -> String {
    sanitize_contract_visible_text(value, false)
}

fn sanitize_contract_identity(value: &str) -> String {
    sanitize_contract_visible_text(value, true)
}

fn sanitize_contract_visible_text(value: &str, preserve_name_punctuation: bool) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '@' => sanitized.push_str("@\u{200b}"),
            '-' | '.' if preserve_name_punctuation => sanitized.push(character),
            '\\' | '`' | '*' | '_' | '~' | '>' | '<' | '[' | ']' | '(' | ')' | '#' | '+' | '-'
            | '.' | '!' | '|' | '{' | '}' | '"' => sanitized.push(' '),
            character if character.is_whitespace() || character.is_control() => sanitized.push(' '),
            _ => sanitized.push(character),
        }
    }
    sanitized.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn merge_cache_metadata(
    cached: &CacheMetadata,
    response: CacheMetadata,
) -> CacheMetadata {
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

pub const DEFAULT_PROXIMITY_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
pub const MAX_PROXIMITY_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);

pub fn proximity_reconciliation_interval_from(value: Option<&str>) -> Result<Duration, String> {
    let Some(value) = value else {
        return Ok(DEFAULT_PROXIMITY_RECONCILIATION_INTERVAL);
    };
    let seconds = value.parse::<u64>().map_err(|_| {
        "CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS must be an integer from 1 through 30"
            .to_string()
    })?;
    if !(1..=MAX_PROXIMITY_RECONCILIATION_INTERVAL.as_secs()).contains(&seconds) {
        return Err(
            "CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS must be an integer from 1 through 30"
                .to_string(),
        );
    }
    Ok(Duration::from_secs(seconds))
}

pub fn proximity_reconciliation_interval_from_environment() -> Result<Duration, String> {
    proximity_reconciliation_interval_from_environment_value(std::env::var(
        "CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS",
    ))
}

fn proximity_reconciliation_interval_from_environment_value(
    value: Result<String, std::env::VarError>,
) -> Result<Duration, String> {
    match value {
        Ok(value) => proximity_reconciliation_interval_from(Some(&value)),
        Err(std::env::VarError::NotPresent) => proximity_reconciliation_interval_from(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(
                "CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS must be valid Unicode".to_string(),
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_proximity_reconciliation_loop(
    database_url: String,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
    structure_resolver: Option<Arc<dyn StructureResolver>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let esi = loop {
            match HttpPublicContractEsi::new(esi_timeout) {
                Ok(esi) => break Arc::new(esi),
                Err(error) => {
                    warn!("proximity reconciliation HTTP client unavailable: {error}");
                    tokio::time::sleep(interval).await;
                }
            }
        };
        run_proximity_reconciliation_loop(
            database_url,
            interval,
            esi,
            ship_groups,
            delivery,
            ping_limiter,
            structure_resolver,
        )
        .await;
    })
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn spawn_proximity_reconciliation_loop_with_esi(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn PublicContractEsi>,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
    structure_resolver: Option<Arc<dyn StructureResolver>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_proximity_reconciliation_loop(
        database_url,
        interval,
        esi,
        ship_groups,
        delivery,
        ping_limiter,
        structure_resolver,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_proximity_reconciliation_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn PublicContractEsi>,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
    structure_resolver: Option<Arc<dyn StructureResolver>>,
) {
    let region_names = runtime_contract_region_names();
    let mut cadence = tokio::time::interval_at(tokio::time::Instant::now(), interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        match ContractCollectionStore::connect(&database_url).await {
            Ok(store) => {
                let collector = ContractCollector::new(store, esi.clone())
                    .with_region_names(region_names.clone())
                    .with_notifications_and_ping_limiter(
                        ship_groups.clone(),
                        delivery.clone(),
                        ping_limiter.clone(),
                    )
                    .with_optional_structure_resolver(structure_resolver.clone());
                if let Err(error) = collector.reconcile_proximity_notifications().await {
                    warn!("proximity reconciliation cycle failed: {error}");
                }
            }
            Err(error) => warn!("proximity reconciliation database unavailable: {error}"),
        }
    }
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
    spawn_contract_collection_loop_with_notifications_and_region_concurrency(
        database_url,
        store_handle,
        interval,
        esi_timeout,
        ship_groups,
        delivery,
        ping_limiter,
        DEFAULT_CONTRACT_REGIONAL_CONCURRENCY,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_contract_collection_loop_with_notifications_and_region_concurrency(
    database_url: String,
    store_handle: ContractStoreHandle,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
    max_concurrent_regions: usize,
) -> tokio::task::JoinHandle<()> {
    spawn_contract_collection_loop_with_notifications_structure_resolver_and_region_concurrency(
        database_url,
        store_handle,
        interval,
        esi_timeout,
        ship_groups,
        delivery,
        ping_limiter,
        None,
        None,
        max_concurrent_regions,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_contract_collection_loop_with_notifications_structure_resolver_and_region_concurrency(
    database_url: String,
    store_handle: ContractStoreHandle,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
    structure_resolver: Option<Arc<dyn StructureResolver>>,
    structure_resolver_runtime: Option<StructureResolverRuntimeStatus>,
    max_concurrent_regions: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        run_contract_collection_loop_with_notifications_and_region_concurrency(
            database_url,
            store_handle,
            interval,
            esi_timeout,
            ship_groups,
            delivery,
            ping_limiter,
            structure_resolver,
            structure_resolver_runtime,
            max_concurrent_regions,
        ),
    )
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
    run_contract_collection_loop_with_notifications_and_region_concurrency(
        database_url,
        store_handle,
        interval,
        esi_timeout,
        ship_groups,
        delivery,
        ping_limiter,
        None,
        None,
        DEFAULT_CONTRACT_REGIONAL_CONCURRENCY,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_contract_collection_loop_with_notifications_and_region_concurrency(
    database_url: String,
    store_handle: ContractStoreHandle,
    interval: Duration,
    esi_timeout: Duration,
    ship_groups: Arc<dyn ShipGroupResolver>,
    delivery: Arc<dyn ContractDelivery>,
    ping_limiter: Arc<dyn ContractPingLimiter>,
    structure_resolver: Option<Arc<dyn StructureResolver>>,
    structure_resolver_runtime: Option<StructureResolverRuntimeStatus>,
    max_concurrent_regions: usize,
) {
    let region_names = runtime_contract_region_names();
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
                if let Some(runtime) = &structure_resolver_runtime {
                    if let Err(error) = store
                        .initialize_structure_resolver_runtime(runtime, Utc::now())
                        .await
                    {
                        warn!("could not persist structure resolver runtime state: {error}");
                    }
                }
                let collector = ContractCollector::new((*store).clone(), esi.clone())
                    .with_region_names(region_names.clone())
                    .with_notifications_and_ping_limiter(
                        notifications.ship_groups.clone(),
                        notifications.delivery.clone(),
                        notifications.ping_limiter.clone(),
                    )
                    .with_optional_structure_resolver(structure_resolver.clone())
                    .with_recovery_gap(collection_recovery_gap(interval))
                    .with_max_concurrent_regions(max_concurrent_regions);
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
    let region_names = runtime_contract_region_names();
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
                let mut collector = ContractCollector::new(store, esi.clone())
                    .with_region_names(region_names.clone())
                    .with_max_concurrent_regions(DEFAULT_CONTRACT_REGIONAL_CONCURRENCY);
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
    use chrono::TimeZone;
    use serenity::http::Http;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::thread::JoinHandle;
    use std::time::Duration;

    use super::*;

    const MANUAL_CONTRACT_EMBED_NONCE: &str = "ci-manual-contract-check";

    #[cfg(unix)]
    #[test]
    fn non_unicode_regional_concurrency_is_rejected() {
        use std::os::unix::ffi::OsStringExt;

        let error = contract_regional_concurrency_from_environment_value(Err(
            std::env::VarError::NotUnicode(std::ffi::OsString::from_vec(vec![0x80])),
        ))
        .expect_err("a configured non-Unicode value must not become the default");
        assert!(error.contains("valid Unicode"));
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_proximity_reconciliation_cadence_is_rejected() {
        use std::os::unix::ffi::OsStringExt;

        let error = proximity_reconciliation_interval_from_environment_value(Err(
            std::env::VarError::NotUnicode(std::ffi::OsString::from_vec(vec![0x80])),
        ))
        .expect_err("a non-Unicode cadence must not become the default");
        assert!(error.contains("valid Unicode"));
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_watchdog_configuration_is_rejected() {
        use std::os::unix::ffi::OsStringExt;

        let error = health_settings_from_environment_values(|name| {
            if name == "HEALTH_CHANNEL_ID" {
                Err(std::env::VarError::NotUnicode(
                    std::ffi::OsString::from_vec(vec![0x80]),
                ))
            } else {
                Err(std::env::VarError::NotPresent)
            }
        })
        .expect_err("a non-Unicode watchdog setting must not become a default");
        assert_eq!(error.to_string(), "HEALTH_CHANNEL_ID must be valid Unicode");
    }

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
                    last_verified_at: None,
                },
                matched_range: None,
                issuer_character_name: Some("Issuer Name".to_string()),
                issuer_corporation_name: Some("Issuer Corp".to_string()),
                issuer_alliance_id: Some(99_000_001),
                issuer_alliance_name: Some("Issuer Alliance".to_string()),
                issuer_identity_provenance: None,
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

    #[derive(Clone)]
    struct DiscordWireReply {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    }

    struct DiscordEditWireServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        handle: JoinHandle<()>,
    }

    impl DiscordEditWireServer {
        fn start(replies: Vec<DiscordWireReply>) -> Self {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("bind controlled Discord HTTP listener");
            listener
                .set_nonblocking(true)
                .expect("make controlled Discord listener nonblocking");
            let address = listener
                .local_addr()
                .expect("controlled Discord listener address");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded_requests = requests.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = stop.clone();
            let handle = std::thread::spawn(move || {
                let mut replies = replies.into_iter();
                while !stopped.load(Ordering::Relaxed) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(connection) => connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                        Err(error) => panic!("accept controlled Discord request: {error}"),
                    };
                    stream
                        .set_nonblocking(false)
                        .expect("make controlled Discord connection blocking");
                    let request = read_discord_edit_request(&mut stream);
                    recorded_requests.lock().unwrap().push(request);
                    let reply = replies.next().unwrap_or(DiscordWireReply {
                        status: 500,
                        headers: vec![],
                        body: r#"{"code":0,"message":"unexpected retry"}"#.to_string(),
                    });
                    let headers = reply
                        .headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .collect::<String>();
                    let response = format!(
                        "HTTP/1.1 {} controlled\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                        reply.status,
                        reply.body.len(),
                        headers,
                        reply.body,
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write controlled Discord response");
                }
            });
            Self {
                base_url: format!("http://{address}"),
                requests,
                stop,
                handle,
            }
        }

        fn finish(self) -> Vec<String> {
            self.stop.store(true, Ordering::Relaxed);
            self.handle
                .join()
                .expect("join controlled Discord listener");
            self.requests.lock().unwrap().clone()
        }
    }

    fn read_discord_edit_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set controlled Discord request timeout");
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0_u8; 1024];
            let read = stream
                .read(&mut chunk)
                .expect("read controlled Discord request");
            assert!(read > 0, "controlled Discord request ended before headers");
            bytes.extend_from_slice(&chunk[..read]);
            let Some(headers_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..headers_end])
                .expect("controlled Discord request headers are UTF-8");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':').and_then(|(name, value)| {
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().expect("content length"))
                    })
                })
                .unwrap_or(0);
            if bytes.len() >= headers_end + 4 + content_length {
                return String::from_utf8(bytes).expect("controlled Discord request is UTF-8");
            }
        }
    }

    fn contract_edit_for_wire(message: ContractNotificationMessage) -> ContractMessageEdit {
        ContractMessageEdit {
            delivery_id: 13,
            channel_id: 77,
            discord_message_id: "9001".to_string(),
            contract_id: 45,
            event_kind: ContractEventKind::Listed,
            repair_revision: 1,
            repair_claim_token: Some("controlled-claim".to_string()),
            repair_attempt_count: 1,
            message,
        }
    }

    #[test]
    fn contract_delivery_operator_capability_requires_the_configured_operator_id() {
        let digest = "00".repeat(32);
        assert!(ContractDeliveryOperatorCapability::from_config(
            CONTRACT_DELIVERY_OPERATOR_ACTOR.to_string(),
            &digest,
        )
        .is_ok());
        assert!(ContractDeliveryOperatorCapability::from_config(
            "discord:000000000000000001".to_string(),
            &digest,
        )
        .is_err());
    }

    #[test]
    fn contract_delivery_operator_capability_requires_exactly_64_ascii_hex_digest_characters() {
        let actor = CONTRACT_DELIVERY_OPERATOR_ACTOR.to_string();
        assert!(
            ContractDeliveryOperatorCapability::from_config(actor.clone(), &"aB".repeat(32))
                .is_ok()
        );
        for digest in [
            "0".repeat(63),
            "0".repeat(65),
            format!("{}g", "0".repeat(63)),
            format!("{}é", "0".repeat(62)),
        ] {
            assert!(
                ContractDeliveryOperatorCapability::from_config(actor.clone(), &digest).is_err(),
                "digest must be exactly 64 ASCII hexadecimal characters: {digest:?}"
            );
        }
    }

    #[tokio::test]
    async fn discord_contract_edit_patches_the_exact_message_with_embed_and_mentions_suppressed() {
        let server = DiscordEditWireServer::start(vec![DiscordWireReply {
            status: 200,
            headers: vec![],
            body: "{}".to_string(),
        }]);
        let token = "controlled-auth-token";
        let message = ContractNotificationMessage {
            title: "Corrected public contract".to_string(),
            description: Some("@everyone remains inert".to_string()),
            fields: vec![ContractEmbedField {
                name: "Contract Address".to_string(),
                value: "contract:0//45".to_string(),
                inline: false,
            }],
            presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
            author: None,
            thumbnail_url: Some("https://images.example.test/rifter.png".to_string()),
            footer: Some("corrected".to_string()),
            timestamp: None,
        };
        let expected_embed = contract_notification_embed(&message).0;
        let delivery = DiscordContractDelivery::new_with_edit_api_base(
            Arc::new(Http::new(token)),
            server.base_url.clone(),
        );

        delivery
            .edit(contract_edit_for_wire(message))
            .await
            .expect("Discord accepts the contract repair PATCH");
        let requests = server.finish();

        assert_eq!(requests.len(), 1, "an edit never issues a replacement POST");
        let request = &requests[0];
        assert!(request.starts_with("PATCH /channels/77/messages/9001 HTTP/1.1\r\n"));
        assert!(request
            .lines()
            .any(|line| { line.eq_ignore_ascii_case(&format!("authorization: Bot {token}")) }));
        let body = request
            .split_once("\r\n\r\n")
            .map(|(_, body)| serde_json::from_str::<Value>(body).expect("edit JSON body"))
            .expect("edit HTTP body");
        assert_eq!(body["embeds"], serde_json::json!([expected_embed]));
        assert_eq!(body["allowed_mentions"]["parse"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn discord_contract_edit_rejects_invalid_message_identities_without_a_patch() {
        for message_id in ["", "not-a-snowflake", "0", "18446744073709551616"] {
            let server = DiscordEditWireServer::start(vec![]);
            let delivery = DiscordContractDelivery::new_with_edit_api_base(
                Arc::new(Http::new("controlled-auth-token")),
                server.base_url.clone(),
            );
            let mut edit = contract_edit_for_wire(ContractNotificationMessage {
                title: "Invalid identity correction".to_string(),
                description: None,
                fields: vec![],
                presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
                author: None,
                thumbnail_url: None,
                footer: None,
                timestamp: None,
            });
            edit.discord_message_id = message_id.to_string();

            let error = delivery
                .edit(edit)
                .await
                .expect_err("invalid persisted Discord message identities are permanent");
            assert!(matches!(error, ContractDeliveryError::Permanent(_)));
            assert!(error
                .to_string()
                .contains("expected a nonzero decimal snowflake"));
            assert!(
                server.finish().is_empty(),
                "invalid message identity {message_id:?} must not issue a PATCH"
            );
        }
    }

    #[tokio::test]
    async fn discord_contract_edit_uses_platform_retry_after_without_retrying_internally() {
        for (headers, body, expected_delay) in [
            (
                vec![("Retry-After".to_string(), "1.5".to_string())],
                r#"{"retry_after":9.0}"#.to_string(),
                ChronoDuration::milliseconds(1500),
            ),
            (
                vec![],
                r#"{"retry_after":2.25}"#.to_string(),
                ChronoDuration::milliseconds(2250),
            ),
        ] {
            let server = DiscordEditWireServer::start(vec![DiscordWireReply {
                status: 429,
                headers,
                body,
            }]);
            let delivery = DiscordContractDelivery::new_with_edit_api_base(
                Arc::new(Http::new("controlled-auth-token")),
                server.base_url.clone(),
            );
            let before = Utc::now();
            let error = delivery
                .edit(contract_edit_for_wire(ContractNotificationMessage {
                    title: "Rate limited correction".to_string(),
                    description: None,
                    fields: vec![],
                    presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
                    author: None,
                    thumbnail_url: None,
                    footer: None,
                    timestamp: None,
                }))
                .await
                .expect_err("the retry deadline is returned to the durable dispatcher");
            let after = Utc::now();
            let requests = server.finish();

            let retry_at = error.retry_at().expect("platform retry deadline");
            assert!(matches!(error, ContractDeliveryError::Transient(_)));
            assert!(retry_at >= before + expected_delay);
            assert!(retry_at <= after + expected_delay);
            assert_eq!(
                requests.len(),
                1,
                "the delivery adapter never retries 429 itself"
            );
            assert!(requests.iter().all(|request| request.starts_with("PATCH ")));
        }
    }

    #[tokio::test]
    async fn discord_code_10008_edit_is_permanent_without_replacement_or_token_leakage() {
        let server = DiscordEditWireServer::start(vec![DiscordWireReply {
            status: 500,
            headers: vec![],
            body: r#"{"code":10008,"message":"Unknown Message"}"#.to_string(),
        }]);
        let token = "controlled-auth-token";
        let delivery = DiscordContractDelivery::new_with_edit_api_base(
            Arc::new(Http::new(token)),
            server.base_url.clone(),
        );
        let error = delivery
            .edit(contract_edit_for_wire(ContractNotificationMessage {
                title: "Missing message correction".to_string(),
                description: None,
                fields: vec![],
                presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
                author: None,
                thumbnail_url: None,
                footer: None,
                timestamp: None,
            }))
            .await
            .expect_err("an unknown Discord message is a permanent repair failure");
        let requests = server.finish();

        assert!(error.to_string().contains("JSON code 10008"));
        assert!(!error.to_string().contains(token));
        assert!(!format!("{error:?}").contains(token));
        assert!(matches!(error, ContractDeliveryError::Permanent(_)));
        assert_eq!(
            requests.len(),
            1,
            "unknown messages never create replacement posts"
        );
        assert!(requests[0].starts_with("PATCH /channels/77/messages/9001 HTTP/1.1\r\n"));
    }

    #[tokio::test]
    async fn discord_contract_edit_classifies_temporary_codes_before_delivery_scoped_405() {
        for (status, code, expected_temporary, expected_delivery_scoped) in [
            (400, 40_004, true, false),
            (400, 0, false, false),
            (405, 0, false, true),
            (500, 10_008, false, true),
        ] {
            let server = DiscordEditWireServer::start(vec![DiscordWireReply {
                status,
                headers: vec![],
                body: format!(r#"{{"code":{code},"message":"controlled"}}"#),
            }]);
            let delivery = DiscordContractDelivery::new_with_edit_api_base(
                Arc::new(Http::new("controlled-auth-token")),
                server.base_url.clone(),
            );
            let error = delivery
                .edit(contract_edit_for_wire(ContractNotificationMessage {
                    title: "classification".to_string(),
                    description: None,
                    fields: vec![],
                    presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
                    author: None,
                    thumbnail_url: None,
                    footer: None,
                    timestamp: None,
                }))
                .await
                .expect_err("controlled Discord error");
            let requests = server.finish();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                matches!(error, ContractDeliveryError::Transient(_)),
                expected_temporary,
                "Discord HTTP {status}, code {code}"
            );
            if !expected_temporary {
                assert!(matches!(error, ContractDeliveryError::Permanent(_)));
                assert_eq!(
                    error.is_delivery_scoped_permanent(),
                    expected_delivery_scoped,
                    "Discord HTTP {status}, code {code}"
                );
            }
        }
    }

    #[test]
    fn renders_every_contract_event_without_a_counterparty() {
        let history = ContractPartyHistory::default();
        for (kind, expected) in [
            (ContractEventKind::Listed, "listed"),
            (ContractEventKind::SaleConfirmed, "contract accepted"),
            (
                ContractEventKind::PurchaseConfirmed,
                "buy contract accepted",
            ),
            (ContractEventKind::Expired, "contract expired"),
            (
                ContractEventKind::ClosedOutcomeUnknown,
                "contract closed • outcome unknown",
            ),
        ] {
            let message = contract_notification_message(
                &event(kind),
                Some(&item(1, 19_720, true)),
                &history,
                &history,
                false,
            );
            assert!(message.title.contains(expected));
            assert_eq!(field(&message, "Event"), None);
            assert_eq!(field(&message, "History"), None);
            assert!(message.fields.iter().all(|field| !matches!(
                field.name.as_str(),
                "Buyer" | "Counterparty" | "Accepting Corporation"
            )));
        }
    }

    #[test]
    fn contract_acceptance_fields_use_separate_native_absolute_and_relative_times() {
        let listed_at = DateTime::parse_from_rfc3339("2026-08-17T19:43:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let confirmed_at = DateTime::parse_from_rfc3339("2026-08-18T22:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let history = ContractPartyHistory::default();

        let mut listed = event(ContractEventKind::Listed);
        listed.contract.date_issued = listed_at;
        let listed_message = contract_notification_message(
            &listed,
            Some(&listed.offered_items[0]),
            &history,
            &history,
            false,
        );
        assert_eq!(field(&listed_message, "Timeline"), None);
        assert_eq!(listed_message.timestamp, Some(listed_at));

        let mut sold = event(ContractEventKind::SaleConfirmed);
        sold.contract.date_issued = listed_at;
        sold.acceptance_evidence = Some(ContractAcceptanceEvidence {
            last_public_observed_at: confirmed_at - ChronoDuration::minutes(2),
            absence_observed_at: confirmed_at - ChronoDuration::minutes(1),
            evidence_response_at: confirmed_at,
            provenance: Some(ContractAcceptanceProvenance::AcceptedByPlayer),
        });
        let sold_message = contract_notification_message(
            &sold,
            Some(&sold.offered_items[0]),
            &history,
            &history,
            false,
        );
        assert_eq!(
            field(&sold_message, "Listing time"),
            Some("<t:1786995780:F>")
        );
        assert_eq!(
            field(&sold_message, "Observed acceptance window"),
            Some("**after** <t:1787090340:F>\n**before** <t:1787090400:F>")
        );
        assert_eq!(
            field(&sold_message, "ESI result"),
            Some("Contract accepted by another player\nESI confirmed <t:1787090460:R>")
        );
        assert_eq!(sold_message.timestamp, Some(confirmed_at));

        let mut purchased = event(ContractEventKind::PurchaseConfirmed);
        purchased.contract.date_issued = listed_at;
        purchased.acceptance_evidence = sold.acceptance_evidence;
        let purchased_message = contract_notification_message(
            &purchased,
            Some(&purchased.requested_items[0]),
            &history,
            &history,
            false,
        );
        assert_eq!(
            field(&purchased_message, "Listing time"),
            Some("<t:1786995780:F>")
        );
        assert_eq!(purchased_message.timestamp, Some(confirmed_at));
    }

    #[test]
    fn terminal_no_content_embed_trims_history_before_proximity_alert() {
        let observed_at = Utc
            .with_ymd_and_hms(2026, 8, 18, 22, 1, 0)
            .single()
            .expect("fixed observation time");
        let mut event = event(ContractEventKind::ClosedOutcomeUnknown);
        event.acceptance_evidence = Some(ContractAcceptanceEvidence {
            last_public_observed_at: observed_at - ChronoDuration::minutes(2),
            absence_observed_at: observed_at - ChronoDuration::minutes(1),
            evidence_response_at: observed_at,
            provenance: Some(ContractAcceptanceProvenance::NoContent),
        });
        let history = ContractPartyHistory {
            confirmed_sales: 4,
            confirmed_purchases: 2,
            unknown_closures: 1,
            most_recent_confirmed: Some(MostRecentConfirmedContractEvent {
                kind: ContractEventKind::SaleConfirmed,
                observed_at: observed_at - ChronoDuration::hours(1),
            }),
        };

        assert!(3 + 2 + 4 + 1 + 2 + 1 > MAX_TERMINAL_CONTENT_LINES);
        let message = contract_notification_message(
            &event,
            Some(&event.offered_items[0]),
            &history,
            &history,
            true,
        );

        let description = message.description.as_deref().expect("contract context");
        assert_eq!(description.lines().count(), 3);
        assert!(description.contains("contract:0//45"));
        assert!(description.contains("**in:**"));
        assert!(description.contains("**on:**"));
        assert!(field(&message, "Listing time").is_some());
        assert!(field(&message, "Observed closure window").is_some());
        assert_eq!(
            field(&message, "ESI result"),
            Some("Public listing absent before expiry boundary")
        );
        assert!(field(&message, "History").is_some_and(|history| {
            history.contains("Issuer: 4 sales")
                && history.contains("Corp: 4 sales")
                && !history.contains('\n')
        }));
        assert_eq!(field(&message, "Alert"), Some("Proximity-Unverified"));
        assert!(message.fields.len() <= MAX_TERMINAL_EMBED_FIELDS);
        assert!(listing_content_lines(&message) <= MAX_TERMINAL_CONTENT_LINES);
        assert!(message.title.chars().count() <= MAX_EMBED_TITLE_CHARACTERS);
        assert!(message
            .description
            .as_deref()
            .is_none_or(
                |description| description.chars().count() <= MAX_EMBED_DESCRIPTION_CHARACTERS
            ));
        assert!(message
            .author
            .as_deref()
            .is_none_or(|author| author.chars().count() <= MAX_EMBED_AUTHOR_NAME_CHARACTERS));
        assert!(message
            .footer
            .as_deref()
            .is_none_or(|footer| footer.chars().count() <= 2048));
        assert!(message.fields.iter().all(|field| {
            field.name.chars().count() <= MAX_EMBED_FIELD_NAME_CHARACTERS
                && field.value.chars().count() <= MAX_EMBED_FIELD_VALUE_CHARACTERS
        }));
        assert!(embed_character_count(&message) <= MAX_EMBED_CHARACTERS);
    }

    #[test]
    fn contract_history_uses_a_native_confirmation_date() {
        let confirmed_at = Utc
            .with_ymd_and_hms(2026, 8, 18, 12, 1, 0)
            .single()
            .expect("fixed confirmation time");
        let history = ContractPartyHistory {
            confirmed_sales: 4,
            confirmed_purchases: 2,
            unknown_closures: 1,
            most_recent_confirmed: Some(MostRecentConfirmedContractEvent {
                kind: ContractEventKind::SaleConfirmed,
                observed_at: confirmed_at,
            }),
        };

        assert_eq!(
            format_party_history(&history),
            "4 sales • 2 purchases • 1 unknown • Latest confirmation <t:1787054460:d>"
        );
    }

    #[test]
    fn location_degrades_without_dropping_known_facts() {
        let history = ContractPartyHistory::default();
        let full = contract_notification_message(
            &event(ContractEventKind::Listed),
            None,
            &history,
            &history,
            false,
        );
        assert_eq!(
            full.description.as_deref(),
            Some(
                "`<url=\"contract:0//45\">Public contract - Jita</url>`\n**in:** [Jita](http://evemaps.dotlan.net/system/30000142) ([The Forge](http://evemaps.dotlan.net/region/10000002))\n**on:** [Jita IV Moon 4 Caldari Navy Assembly Plant](https://zkillboard.com/location/60003760/)"
            )
        );

        let mut fallback = event(ContractEventKind::Listed);
        fallback.embed_context.location = ContractLocationContext {
            solar_system_id: Some(30_000_142),
            ..ContractLocationContext::default()
        };
        let fallback = contract_notification_message(&fallback, None, &history, &history, false);
        assert_eq!(
            fallback.description.as_deref(),
            Some("`<url=\"contract:0//45\">Public contract - Unknown location</url>`")
        );

        let mut station_only = event(ContractEventKind::Listed);
        station_only.embed_context.location = ContractLocationContext {
            location_name: Some("Jita IV - Moon 4".to_string()),
            location_kind: Some("Station".to_string()),
            ..ContractLocationContext::default()
        };
        let station_only =
            contract_notification_message(&station_only, None, &history, &history, false);
        assert_eq!(
            station_only.description.as_deref(),
            Some(
                "`<url=\"contract:0//45\">Public contract - Jita IV Moon 4</url>`\n**on:** [Jita IV Moon 4](https://zkillboard.com/location/60003760/)"
            )
        );

        let mut region_only = event(ContractEventKind::Listed);
        region_only.embed_context.location = ContractLocationContext {
            region_id: Some(10_000_002),
            ..ContractLocationContext::default()
        };
        let region_only =
            contract_notification_message(&region_only, None, &history, &history, false);
        assert_eq!(
            region_only.description.as_deref(),
            Some("`<url=\"contract:0//45\">Public contract - Unknown location</url>`")
        );

        let mut raw_only = event(ContractEventKind::Listed);
        raw_only.embed_context.location = ContractLocationContext::default();
        let raw_only = contract_notification_message(&raw_only, None, &history, &history, false);
        assert_eq!(
            raw_only.description.as_deref(),
            Some("`<url=\"contract:0//45\">Public contract - Unknown location</url>`")
        );
    }

    #[test]
    fn listing_omits_unresolved_player_structure_ids() {
        let history = ContractPartyHistory::default();
        let mut listed = event(ContractEventKind::Listed);
        listed.contract.start_location_id = 1_035_466_617_946;
        listed.embed_context.location = ContractLocationContext::default();

        let message = contract_notification_message(
            &listed,
            Some(&listed.offered_items[0]),
            &history,
            &history,
            false,
        );
        let description = message.description.expect("listing description");

        assert_eq!(
            description,
            "`<url=\"contract:0//45\">Ragnarok - Player-owned structure</url>`"
        );
        assert!(!description.contains(&listed.contract.start_location_id.to_string()));
    }

    #[test]
    fn ranked_primary_ship_and_complete_bundle_stay_prominent() {
        let history = ContractPartyHistory::default();
        let message = contract_notification_message(
            &event(ContractEventKind::Listed),
            Some(&item(1, 19_720, true)),
            &history,
            &history,
            false,
        );
        assert!(message.title.starts_with("Ragnarok listed"));
        assert_eq!(
            message.thumbnail_url.as_deref(),
            Some("https://images.evetech.net/types/19720/icon?size=64")
        );
        assert_eq!(
            message
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
        let description = message.description.expect("compact contract description");
        assert!(description.contains("Ragnarok - Jita"));
        assert!(!description.contains("Rifter"));
        assert!(!description.contains("Tritanium"));
    }

    #[test]
    fn issuer_affiliations_render_progressively_without_ids() {
        let mut context = ContractEmbedContext {
            issuer_character_name: Some("Issuer-Name".to_string()),
            ..ContractEmbedContext::default()
        };
        assert_eq!(
            compact_issuer(&context).as_deref(),
            Some("issuer: Issuer-Name")
        );

        context.issuer_corporation_name = Some("Issuer Corp".to_string());
        assert_eq!(
            compact_issuer(&context).as_deref(),
            Some("issuer: [Issuer Corp] Issuer-Name")
        );

        context.issuer_alliance_name = Some("Issuer Alliance".to_string());
        assert_eq!(
            compact_issuer(&context).as_deref(),
            Some("issuer: [Issuer Alliance] Issuer-Name")
        );

        context.issuer_character_name = None;
        assert_eq!(compact_issuer(&context).as_deref(), None);
    }

    #[test]
    fn compact_listing_embed_keeps_only_the_matched_ship_and_its_contract_link() {
        let history = ContractPartyHistory::default();
        let message = contract_notification_message(
            &event(ContractEventKind::Listed),
            Some(&item(1, 19_720, true)),
            &history,
            &history,
            false,
        );

        assert_eq!(
            message.title,
            "Ragnarok listed for 77.5B ISK in Jita (The Forge)"
        );
        assert_eq!(
            message.description.as_deref(),
            Some(
                "`<url=\"contract:0//45\">Ragnarok - Jita</url>`\n**in:** [Jita](http://evemaps.dotlan.net/system/30000142) ([The Forge](http://evemaps.dotlan.net/region/10000002))\n**on:** [Jita IV Moon 4 Caldari Navy Assembly Plant](https://zkillboard.com/location/60003760/)"
            )
        );
        assert_eq!(message.footer.as_deref(), Some("Contract #45"));
        assert!(message.timestamp.is_some());
        assert_eq!(
            message.author.as_deref(),
            Some("Public Contract\nissuer: [Issuer Alliance] Issuer Name")
        );
        assert!(message.fields.iter().all(|field| {
            !matches!(
                field.name.as_str(),
                "Event"
                    | "Contract"
                    | "Terms"
                    | "Issuer"
                    | "Location"
                    | "Offered"
                    | "Requested"
                    | "Requested ISK"
                    | "Offered ISK"
                    | "Observed Location"
                    | "Observed Affiliation"
                    | "Issuer History"
                    | "Corporation History"
            )
        }));
        let values = message
            .fields
            .iter()
            .map(|field| field.value.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!values.contains("Rifter"));
        assert!(!values.contains("Tritanium"));
    }

    #[test]
    fn history_evidence_and_contract_link_are_structural_and_bounded() {
        let mut event = event(ContractEventKind::SaleConfirmed);
        event.acceptance_evidence = Some(ContractAcceptanceEvidence {
            last_public_observed_at: Utc::now() - ChronoDuration::minutes(3),
            absence_observed_at: Utc::now() - ChronoDuration::minutes(2),
            evidence_response_at: Utc::now() - ChronoDuration::seconds(90),
            provenance: Some(ContractAcceptanceProvenance::AcceptedByPlayer),
        });
        event.embed_context.item_names.insert(
            19_720,
            format!("`@everyone` <url=\"bad\">{}</url>", "R".repeat(400)),
        );
        event.embed_context.issuer_character_name = Some("Issuer-Name".to_string());
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
        let message = contract_notification_message(
            &event,
            Some(&item(1, 19_720, true)),
            &history,
            &history,
            false,
        );
        let description = message
            .description
            .as_deref()
            .expect("contract description");
        let contract_link = description.lines().next().expect("contract link");
        assert!(contract_link.starts_with("`<url=\"contract:0//45\">"));
        assert!(contract_link.ends_with(" - Jita</url>`"));
        assert!(!contract_link.contains("<url=\"bad\">"));
        assert!(!description.contains("issuer: Issuer-Name"));
        assert!(!description.contains("90000001"));
        assert!(!description.contains("98000001"));
        assert!(description.chars().count() <= MAX_EMBED_DESCRIPTION_CHARACTERS);
        assert_eq!(
            message.author.as_deref(),
            Some("Public Contract\nissuer: Issuer-Name")
        );
        assert!(field(&message, "History")
            .expect("history")
            .contains("4 sales"));
        assert_eq!(field(&message, "Evidence"), None);
        assert_eq!(field(&message, "Delay"), None);
        assert!(field(&message, "ESI result")
            .expect("ESI result")
            .contains("accepted by another player"));
        assert_eq!(field(&message, "Observed Affiliation"), None);
        assert_eq!(field(&message, "Contract Address"), None);
        let footer = message.footer.as_deref().expect("footer");
        assert_eq!(footer, "Contract #45 • 90-day history");
        assert!(message.fields.len() <= MAX_TERMINAL_EMBED_FIELDS);
        assert!(listing_content_lines(&message) <= MAX_TERMINAL_CONTENT_LINES);
        assert!(embed_character_count(&message) <= MAX_EMBED_CHARACTERS);
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
            provenance: Some(ContractAcceptanceProvenance::AcceptedByPlayer),
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
            false,
        );
        assert_eq!(message.title, "Hel sold for 77.5B in Jita (The Forge)");
        assert!(message
            .description
            .as_deref()
            .is_some_and(|description| description.starts_with(
                "`<url=\"contract:0//234057619\">Hel - Jita</url>`\nissuer: [Issuer Alliance] Issuer Name"
            )));
        assert!(field(&message, "History")
            .expect("issuer history")
            .contains("4 sales"));
        assert!(field(&message, "History")
            .expect("corporation history")
            .contains("12 sales"));
        assert_eq!(
            message.thumbnail_url.as_deref(),
            Some("https://images.evetech.net/types/22852/icon?size=64")
        );
        let embed = contract_notification_embed(&message);
        assert_eq!(
            embed.0["title"].as_str(),
            Some("Hel sold for 77.5B in Jita (The Forge)")
        );
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
                delivery_claim_token: None,
            })
            .await
            .expect("send manual contract embed");
        assert!(!message_id.is_empty());
    }

    #[test]
    fn aggregate_embed_limit_preserves_the_contract_link() {
        let mut message = ContractNotificationMessage {
            title: "T".repeat(MAX_EMBED_TITLE_CHARACTERS),
            description: Some(format!(
                "`<url=\"contract:0//45\">Ragnarok - Jita</url>`\n{}",
                "D".repeat(4_000)
            )),
            fields: (0..25)
                .map(|index| ContractEmbedField {
                    name: format!("Field {index} {}", "N".repeat(240)),
                    value: "V".repeat(MAX_EMBED_FIELD_VALUE_CHARACTERS),
                    inline: false,
                })
                .collect(),
            presentation_revision: CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
            author: Some("A".repeat(257)),
            thumbnail_url: None,
            footer: Some("F".repeat(2048)),
            timestamp: None,
        };

        bound_embed_message(&mut message);

        assert_eq!(
            message
                .author
                .as_deref()
                .expect("bounded author")
                .chars()
                .count(),
            256
        );
        assert!(
            message.title.chars().count()
                + message
                    .description
                    .as_deref()
                    .map(str::chars)
                    .map(Iterator::count)
                    .unwrap_or(0)
                + message
                    .author
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
        assert!(message
            .description
            .as_deref()
            .is_some_and(|description| description
                .starts_with("`<url=\"contract:0//45\">Ragnarok - Jita</url>`")));
    }

    #[test]
    fn contract_embed_text_is_single_line_plain_text_without_discord_markdown() {
        let mut event = event(ContractEventKind::Listed);
        event.embed_context.item_names.insert(
            19_720,
            "# heading\n- list ||spoiler|| **bold** <@1234>".to_string(),
        );
        let history = ContractPartyHistory::default();

        let message = contract_notification_message(
            &event,
            Some(&event.offered_items[0]),
            &history,
            &history,
            false,
        );
        let description = message.description.expect("sanitized contract description");
        let contract_link = description.lines().next().expect("contract link");

        assert!(!contract_link.contains('\n'));
        assert!(!contract_link.contains("# heading"));
        assert!(!contract_link.contains("- list"));
        assert!(!contract_link.contains("||spoiler||"));
        assert!(!contract_link.contains("**bold**"));
        assert!(!contract_link.contains("<@1234>"));
        assert!(contract_link.contains("heading"));
        assert!(contract_link.contains("spoiler"));
        assert!(contract_link.contains("bold"));
        assert!(contract_link.ends_with(" - Jita</url>`"));
    }
}
