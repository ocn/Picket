use crate::esi::{Celestial, EsiClient};
use config::{Config, ConfigError, Environment, File};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use serenity::model::id::{GuildId, UserId};
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Formatter;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{error, info, warn};

// --- Data Models ---

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct System {
    pub id: u32,
    #[serde(rename = "systemName")]
    pub name: String,
    #[serde(rename = "securityStatus")]
    pub security_status: f64,
    #[serde(rename = "regionId")]
    pub region_id: u32,
    #[serde(rename = "regionName")]
    pub region: String,
    #[serde(default)]
    pub x: f64,
    #[serde(default)]
    pub y: f64,
    #[serde(default)]
    pub z: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SystemRange {
    pub system_id: u32,
    pub range: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Default)]
pub enum Target {
    #[default]
    Any,
    Attacker,
    Victim,
}

impl Target {
    pub fn is_attacker(&self) -> bool {
        matches!(self, Target::Attacker | Target::Any)
    }

    pub fn is_victim(&self) -> bool {
        matches!(self, Target::Victim | Target::Any)
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Target::Any => "Any",
            Target::Attacker => "Attacker",
            Target::Victim => "Victim",
        };
        write!(f, "{}", s)
    }
}

// Filter conditions that can be targeted to either a victim or attacker
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum TargetableCondition {
    Alliance(Vec<u64>),
    Corporation(Vec<u64>),
    Character(Vec<u64>),
    ShipType(Vec<u32>),
    ShipGroup(Vec<u32>),
    NameFragment(String),
}

// Combines a targetable condition with a target
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct TargetedFilter {
    pub condition: TargetableCondition,
    #[serde(default)]
    pub target: Target,
}

impl TargetedFilter {
    pub fn name(&self) -> String {
        match &self.condition {
            TargetableCondition::Alliance(ids) => {
                format!(
                    "Alliance(ids: [{}], target: {})",
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.target
                )
            }
            TargetableCondition::Corporation(ids) => {
                format!(
                    "Corporation(ids: [{}], target: {})",
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.target
                )
            }
            TargetableCondition::Character(ids) => {
                format!(
                    "Character(ids: [{}], target: {})",
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.target
                )
            }
            TargetableCondition::ShipType(ids) => {
                format!(
                    "ShipType(ids: [{}], target: {})",
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.target
                )
            }
            TargetableCondition::ShipGroup(ids) => {
                format!(
                    "ShipGroup(ids: [{}], target: {})",
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.target
                )
            }
            TargetableCondition::NameFragment(s) => {
                format!("NameFragment(fragment: \"{}\", target: {})", s, self.target)
            }
        }
    }
}

// Simple, non-targeted conditions
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum SimpleFilter {
    TotalValue {
        min: Option<u64>,
        max: Option<u64>,
    },
    DroppedValue {
        min: Option<u64>,
        max: Option<u64>,
    },
    Region(Vec<u32>),
    System(Vec<u32>),
    Security(String),
    LyRangeFrom(Vec<SystemRange>),
    IsNpc(bool),
    IsSolo(bool),
    Pilots {
        min: Option<u32>,
        max: Option<u32>,
    },
    TimeRange {
        start: u32,
        end: u32,
    },
    IgnoreHighStanding {
        synched_by_user_id: u64,
        source: StandingSource,
        source_entity_id: u64,
    },
}

impl SimpleFilter {
    pub fn name(&self) -> String {
        match &self {
            SimpleFilter::TotalValue { min, max } => format!(
                "TotalValue(min: {}, max: {})",
                min.map_or("any".to_string(), |v| v.to_string()),
                max.map_or("any".to_string(), |v| v.to_string())
            ),
            SimpleFilter::DroppedValue { min, max } => format!(
                "DroppedValue(min: {}, max: {})",
                min.map_or("any".to_string(), |v| v.to_string()),
                max.map_or("any".to_string(), |v| v.to_string())
            ),
            SimpleFilter::Region(ids) => format!(
                "Region({})",
                ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            SimpleFilter::System(ids) => format!(
                "System({})",
                ids.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            SimpleFilter::Security(range) => format!("Security({})", range),
            SimpleFilter::LyRangeFrom(system_ranges) => {
                let parts: Vec<String> = system_ranges
                    .iter()
                    .map(|sr| format!("{}:{}ly", sr.system_id, sr.range))
                    .collect();
                format!("LyRangeFrom({})", parts.join(", "))
            }
            SimpleFilter::IsNpc(b) => format!("IsNpc({})", b),
            SimpleFilter::IsSolo(b) => format!("IsSolo({})", b),
            SimpleFilter::Pilots { min, max } => format!(
                "Pilots(min: {}, max: {})",
                min.map_or("any".to_string(), |v| v.to_string()),
                max.map_or("any".to_string(), |v| v.to_string())
            ),
            SimpleFilter::TimeRange { start, end } => format!("TimeRange({}:00-{}:00)", start, end),
            SimpleFilter::IgnoreHighStanding {
                synched_by_user_id,
                source,
                source_entity_id,
            } => {
                format!(
                    "IgnoreHighStanding(synched_by: {}, source: {:?}, source_id: {})",
                    synched_by_user_id, source, source_entity_id
                )
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Filter {
    Simple(SimpleFilter),
    Targeted(TargetedFilter),
}

impl Filter {
    /// Creates a human-readable name for the filter and its configuration.
    pub fn name(&self) -> String {
        match self {
            Filter::Simple(sf) => sf.name(),
            Filter::Targeted(tf) => tf.name(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum FilterNode {
    Condition(Filter),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}

impl FilterNode {
    /// Recursively creates a human-readable name for the filter node and its children.
    pub fn name(&self) -> String {
        match self {
            FilterNode::Condition(condition) => condition.name(),
            FilterNode::And(nodes) => {
                let children = nodes
                    .iter()
                    .map(FilterNode::name)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("And({})", children)
            }
            FilterNode::Or(nodes) => {
                let children = nodes
                    .iter()
                    .map(FilterNode::name)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Or({})", children)
            }
            FilterNode::Not(node) => {
                format!("Not({})", node.name())
            }
        }
    }

    /// Checks if this filter tree contains any ShipType or ShipGroup conditions.
    /// Used to determine if we're tracking specific ships vs entities (alliance/corp).
    pub fn contains_ship_filter(&self) -> bool {
        match self {
            FilterNode::Condition(filter) => match filter {
                Filter::Targeted(tf) => matches!(
                    tf.condition,
                    TargetableCondition::ShipType(_) | TargetableCondition::ShipGroup(_)
                ),
                Filter::Simple(_) => false,
            },
            FilterNode::And(nodes) | FilterNode::Or(nodes) => {
                nodes.iter().any(|n| n.contains_ship_filter())
            }
            FilterNode::Not(node) => node.contains_ship_filter(),
        }
    }

    /// Collects the union of all ShipType ID lists in this filter tree.
    /// Used to determine which attackers are Type-Tracked Ships for display:
    /// an attacker whose flown hull's type ID is in this set is displayed by
    /// its Ship Type name instead of its Ship Group name.
    pub fn ship_type_ids(&self) -> HashSet<u32> {
        let mut ids = HashSet::new();
        self.collect_ship_type_ids(&mut ids);
        ids
    }

    fn collect_ship_type_ids(&self, ids: &mut HashSet<u32>) {
        match self {
            FilterNode::Condition(Filter::Targeted(tf)) => {
                if let TargetableCondition::ShipType(type_ids) = &tf.condition {
                    ids.extend(type_ids.iter().copied());
                }
            }
            FilterNode::Condition(Filter::Simple(_)) => {}
            FilterNode::And(nodes) | FilterNode::Or(nodes) => {
                for node in nodes {
                    node.collect_ship_type_ids(ids);
                }
            }
            FilterNode::Not(node) => node.collect_ship_type_ids(ids),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum PingType {
    Here {
        max_ping_delay_minutes: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ping_cooldown_minutes: Option<u32>,
    },
    Everyone {
        max_ping_delay_minutes: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ping_cooldown_minutes: Option<u32>,
    },
    Role {
        role_id: u64,
        max_ping_delay_minutes: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ping_cooldown_minutes: Option<u32>,
    },
}

impl PingType {
    pub fn max_ping_delay_in_minutes(&self) -> Option<u32> {
        match self {
            PingType::Here {
                max_ping_delay_minutes,
                ..
            } => *max_ping_delay_minutes,
            PingType::Everyone {
                max_ping_delay_minutes,
                ..
            } => *max_ping_delay_minutes,
            PingType::Role {
                max_ping_delay_minutes,
                ..
            } => *max_ping_delay_minutes,
        }
    }

    pub fn ping_cooldown_in_minutes(&self) -> u32 {
        match self {
            PingType::Here {
                ping_cooldown_minutes,
                ..
            }
            | PingType::Everyone {
                ping_cooldown_minutes,
                ..
            }
            | PingType::Role {
                ping_cooldown_minutes,
                ..
            } => ping_cooldown_minutes.unwrap_or(DEFAULT_CHANNEL_PING_COOLDOWN_MINUTES),
        }
    }

    pub fn configured_ping_cooldown_in_minutes(&self) -> Option<u32> {
        match self {
            PingType::Here {
                ping_cooldown_minutes,
                ..
            }
            | PingType::Everyone {
                ping_cooldown_minutes,
                ..
            }
            | PingType::Role {
                ping_cooldown_minutes,
                ..
            } => *ping_cooldown_minutes,
        }
    }

    pub fn channel_ping_cooldown(&self) -> Duration {
        Duration::from_secs(u64::from(self.ping_cooldown_in_minutes()) * 60)
    }

    pub fn name(&self) -> String {
        match self {
            PingType::Here {
                max_ping_delay_minutes,
                ..
            } => {
                format!(
                    "Here (max delay: {} min)",
                    max_ping_delay_minutes.unwrap_or(0)
                )
            }
            PingType::Everyone {
                max_ping_delay_minutes,
                ..
            } => {
                format!(
                    "Everyone (max delay: {} min)",
                    max_ping_delay_minutes.unwrap_or(0)
                )
            }
            PingType::Role {
                role_id,
                max_ping_delay_minutes,
                ..
            } => {
                format!(
                    "Role <@&{}> (max delay: {} min)",
                    role_id,
                    max_ping_delay_minutes.unwrap_or(0)
                )
            }
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
pub struct Action {
    pub channel_id: String,
    pub ping_type: Option<PingType>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Subscription {
    pub id: String,
    pub description: String,
    #[serde(rename = "filter")]
    pub root_filter: FilterNode,
    pub action: Action,
}

impl Subscription {
    pub fn filter_name(&self) -> String {
        self.root_filter.name()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Default)]
pub enum StandingSource {
    #[default]
    Character,
    Corporation,
    Alliance,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EveAuthToken {
    pub character_id: u64,
    pub character_name: String,
    pub corporation_id: u64,
    pub alliance_id: Option<u64>,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: u64, // Store as a Unix timestamp
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StandingContact {
    pub contact_id: u64,
    pub standing: f32,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct UserContactLists {
    // Key: The EVE entity ID (char, corp, or alliance) whose contacts these are
    pub contacts: HashMap<u64, Vec<StandingContact>>,
}

// Helper struct for the map value
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct UserStandings {
    #[serde(default)]
    pub tokens: Vec<EveAuthToken>,
    #[serde(default)]
    pub contact_lists: UserContactLists,
}

// Helper struct for tracking the SSO state
pub struct SsoState {
    pub discord_user_id: UserId,
    pub subscription_id: String,
    pub standing_source: StandingSource,
    pub original_interaction: ApplicationCommandInteraction, // To respond later
}

// --- App Configuration & State ---

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FeedProvider {
    #[default]
    Redisq,
    R2z2,
}

impl fmt::Display for FeedProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FeedProvider::Redisq => write!(f, "redisq"),
            FeedProvider::R2z2 => write!(f, "r2z2"),
        }
    }
}

fn default_4() -> usize {
    4
}
fn default_6() -> u64 {
    6
}
fn default_10() -> u64 {
    10
}
fn default_15() -> u64 {
    15
}
fn default_60() -> u64 {
    60
}
fn default_300() -> u64 {
    300
}
fn default_512() -> usize {
    512
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub discord_bot_token: String,
    pub discord_client_id: u64,
    pub eve_client_id: String,
    pub eve_client_secret: String,

    #[serde(default = "default_15")]
    pub esi_http_timeout_secs: u64,
    #[serde(default = "default_60")]
    pub killmail_process_timeout_secs: u64,
    #[serde(default = "default_10")]
    pub redisq_connect_timeout_secs: u64,
    #[serde(default = "default_60")]
    pub redisq_request_timeout_secs: u64,

    #[serde(default = "default_10")]
    pub r2z2_connect_timeout_secs: u64,
    #[serde(default = "default_15")]
    pub r2z2_request_timeout_secs: u64,
    #[serde(default = "default_6")]
    pub r2z2_poll_interval_secs: u64,
    #[serde(default = "default_10")]
    pub r2z2_max_consecutive_404s: u64,
    #[serde(default = "default_300")]
    pub r2z2_resync_timeout_secs: u64,

    #[serde(default)]
    pub killmail_feed_provider: FeedProvider,

    #[serde(default)]
    pub killmail_post_process_sleep_ms: u64,

    #[serde(default = "default_4")]
    pub killmail_workers: usize,

    #[serde(default = "default_512")]
    pub killmail_queue_size: usize,
}

pub struct AppState {
    pub systems: Arc<RwLock<HashMap<u32, System>>>,
    pub ships: Arc<RwLock<HashMap<u32, u32>>>,
    pub names: Arc<RwLock<HashMap<u64, String>>>,
    pub tickers: Arc<RwLock<HashMap<u64, String>>>,
    pub group_names: Arc<RwLock<HashMap<u32, String>>>,
    pub subscriptions: Arc<RwLock<HashMap<GuildId, Vec<Subscription>>>>,
    pub app_config: Arc<AppConfig>,
    pub esi_client: EsiClient,
    pub celestial_cache: Cache<u32, Arc<Celestial>>,
    pub systems_file_lock: Mutex<()>,
    pub ships_file_lock: Mutex<()>,
    pub names_file_lock: Mutex<()>,
    pub tickers_file_lock: Mutex<()>,
    pub group_names_file_lock: Mutex<()>,
    pub subscriptions_file_lock: Mutex<()>,
    pub last_ping_times: Mutex<HashMap<u64, Instant>>,
    pub user_standings: Arc<RwLock<HashMap<UserId, UserStandings>>>,
    pub user_standings_file_lock: Mutex<()>,
    pub sso_states: Arc<Mutex<HashMap<String, SsoState>>>, // For tracking the SSO flow
}

pub const DEFAULT_CHANNEL_PING_COOLDOWN_MINUTES: u32 = 5;
pub const CHANNEL_PING_COOLDOWN: Duration =
    Duration::from_secs(DEFAULT_CHANNEL_PING_COOLDOWN_MINUTES as u64 * 60);

pub async fn try_acquire_channel_ping(
    last_ping_times: &Mutex<HashMap<u64, Instant>>,
    channel_id: u64,
) -> bool {
    try_acquire_channel_ping_with_cooldown(last_ping_times, channel_id, CHANNEL_PING_COOLDOWN).await
}

pub async fn try_acquire_channel_ping_with_cooldown(
    last_ping_times: &Mutex<HashMap<u64, Instant>>,
    channel_id: u64,
    cooldown: Duration,
) -> bool {
    try_acquire_channel_ping_with_cooldown_at(last_ping_times, channel_id, cooldown, Instant::now())
        .await
}

pub async fn try_acquire_channel_ping_at(
    last_ping_times: &Mutex<HashMap<u64, Instant>>,
    channel_id: u64,
    now: Instant,
) -> bool {
    try_acquire_channel_ping_with_cooldown_at(
        last_ping_times,
        channel_id,
        CHANNEL_PING_COOLDOWN,
        now,
    )
    .await
}

pub async fn try_acquire_channel_ping_with_cooldown_at(
    last_ping_times: &Mutex<HashMap<u64, Instant>>,
    channel_id: u64,
    cooldown: Duration,
    now: Instant,
) -> bool {
    let mut ping_times = last_ping_times.lock().await;
    match ping_times.get_mut(&channel_id) {
        Some(last_ping) if now.duration_since(*last_ping) < cooldown => false,
        Some(last_ping) => {
            *last_ping = now;
            true
        }
        None => {
            ping_times.insert(channel_id, now);
            true
        }
    }
}

impl AppState {
    pub fn new(
        app_config: AppConfig,
        systems: HashMap<u32, System>,
        ships: HashMap<u32, u32>,
        names: HashMap<u64, String>,
        tickers: HashMap<u64, String>,
        group_names: HashMap<u32, String>,
        subscriptions: HashMap<GuildId, Vec<Subscription>>,
        user_standings: HashMap<UserId, UserStandings>,
    ) -> Self {
        AppState {
            systems: Arc::new(RwLock::new(systems)),
            ships: Arc::new(RwLock::new(ships)),
            names: Arc::new(RwLock::new(names)),
            tickers: Arc::new(RwLock::new(tickers)),
            group_names: Arc::new(RwLock::new(group_names)),
            subscriptions: Arc::new(RwLock::new(subscriptions)),
            esi_client: EsiClient::new(Duration::from_secs(app_config.esi_http_timeout_secs)),
            app_config: Arc::new(app_config),
            celestial_cache: Cache::new(10_000),
            systems_file_lock: Mutex::new(()),
            ships_file_lock: Mutex::new(()),
            names_file_lock: Mutex::new(()),
            tickers_file_lock: Mutex::new(()),
            group_names_file_lock: Mutex::new(()),
            subscriptions_file_lock: Mutex::new(()),
            last_ping_times: Mutex::new(HashMap::new()),
            user_standings: Arc::new(RwLock::new(user_standings)),
            user_standings_file_lock: Mutex::new(()),
            sso_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

// --- Configuration Loading & Saving ---

// Correctly parses a file containing a JSON array using serde_json
fn load_vec_from_json_file<T: for<'de> Deserialize<'de>>(
    file_path: &Path,
) -> Result<Vec<T>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(file_path)?;
    let data = serde_json::from_str(&content)?;
    Ok(data)
}

fn load_map_from_json_file<K, V>(file_path: &Path) -> Result<HashMap<K, V>, ConfigError>
where
    K: Eq + std::hash::Hash + for<'de> Deserialize<'de>,
    V: for<'de> Deserialize<'de>,
{
    Config::builder()
        .add_source(File::from(file_path))
        .build()?
        .try_deserialize()
}

fn save_to_json_file<T: Serialize>(file_path: &str, data: &T) {
    match serde_json::to_string_pretty(data) {
        Ok(json_string) => {
            if let Err(e) = fs::write(file_path, json_string) {
                error!("Failed to write to {}: {}", file_path, e);
            }
        }
        Err(e) => error!("Failed to serialize data for {}: {}", file_path, e),
    }
}

pub fn save_systems(systems: &HashMap<u32, System>) {
    save_to_json_file("config/systems.json", systems);
}

pub fn save_ships(ships: &HashMap<u32, u32>) {
    save_to_json_file("config/ships.json", ships);
}

pub fn save_names(names: &HashMap<u64, String>) {
    save_to_json_file("config/names.json", names);
}

pub fn save_tickers(tickers: &HashMap<u64, String>) {
    save_to_json_file("config/tickers.json", tickers);
}

pub fn save_user_standings(standings: &HashMap<UserId, UserStandings>) {
    save_to_json_file("config/user_standings.json", standings);
}

pub fn load_app_config() -> Result<AppConfig, ConfigError> {
    let settings = Config::builder()
        .add_source(Environment::default().separator("__"))
        .set_override(
            "discord_bot_token",
            std::env::var("DISCORD_BOT_TOKEN").unwrap_or_default(),
        )?
        .set_override(
            "discord_client_id",
            std::env::var("DISCORD_CLIENT_ID").unwrap_or_default(),
        )?
        .set_override(
            "eve_client_id",
            std::env::var("EVE_CLIENT_ID").unwrap_or_default(),
        )?
        .set_override(
            "eve_client_secret",
            std::env::var("EVE_CLIENT_SECRET").unwrap_or_default(),
        )?
        .build()?;
    // info!("App Config: {:#?}", settings);
    settings.try_deserialize()
}

pub fn load_systems() -> Result<HashMap<u32, System>, ConfigError> {
    load_map_from_json_file(Path::new("config/systems.json"))
}

pub fn load_ships() -> Result<HashMap<u32, u32>, ConfigError> {
    load_map_from_json_file(Path::new("config/ships.json"))
}

pub fn load_names() -> Result<HashMap<u64, String>, ConfigError> {
    load_map_from_json_file(Path::new("config/names.json"))
}

pub fn load_tickers() -> Result<HashMap<u64, String>, ConfigError> {
    load_map_from_json_file(Path::new("config/tickers.json"))
}

pub fn save_group_names(group_names: &HashMap<u32, String>) {
    save_to_json_file("config/group_names.json", group_names);
}

pub fn load_group_names() -> Result<HashMap<u32, String>, ConfigError> {
    load_map_from_json_file(Path::new("config/group_names.json"))
}

pub fn load_user_standings() -> Result<HashMap<UserId, UserStandings>, ConfigError> {
    load_map_from_json_file(Path::new("config/user_standings.json"))
}

pub fn load_all_subscriptions(dir: &str) -> HashMap<GuildId, Vec<Subscription>> {
    let mut all_subscriptions = HashMap::new();
    let path = Path::new(dir);
    if !path.is_dir() {
        return all_subscriptions;
    }

    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            if let Some(filename_str) = path.file_name().and_then(|s| s.to_str()) {
                if let Some(guild_id_str) = filename_str.strip_suffix(".json") {
                    if let Ok(guild_id) = guild_id_str.parse::<u64>() {
                        // Use the correct parsing function for array-based JSON files
                        match load_vec_from_json_file::<Subscription>(&path) {
                            Ok(subs) => {
                                info!("Loaded {} subscriptions for guild {}", subs.len(), guild_id);
                                all_subscriptions.insert(GuildId(guild_id), subs);
                            }
                            Err(e) => {
                                warn!(
                                    "Could not parse {} as subscription file: {:#?}",
                                    filename_str, e
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    all_subscriptions
}

pub fn save_subscriptions_for_guild(
    guild_id: GuildId,
    subscriptions: &[Subscription],
) -> Result<(), std::io::Error> {
    let file_path = format!("config/{}.json", guild_id);
    let json_string = serde_json::to_string_pretty(subscriptions)?;
    fs::write(file_path, json_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn channel_ping_limiter_is_keyed_by_channel_and_uses_a_five_minute_window() {
        let ping_times = Mutex::new(HashMap::new());
        let now = Instant::now();
        assert!(try_acquire_channel_ping_at(&ping_times, 77, now).await);
        assert!(!try_acquire_channel_ping_at(&ping_times, 77, now).await);
        assert!(try_acquire_channel_ping_at(&ping_times, 78, now).await);
        assert!(try_acquire_channel_ping_at(&ping_times, 77, now + CHANNEL_PING_COOLDOWN,).await);
        assert!(
            try_acquire_channel_ping_at(&ping_times, 77, now + CHANNEL_PING_COOLDOWN * 2,).await
        );
    }

    #[tokio::test]
    async fn configured_channel_ping_cooldowns_share_a_channel_timestamp_and_refresh_it() {
        let ping_times = Mutex::new(HashMap::new());
        let now = Instant::now();
        let thirty_minutes = Duration::from_secs(30 * 60);

        assert!(
            try_acquire_channel_ping_with_cooldown_at(&ping_times, 77, thirty_minutes, now,).await
        );
        assert!(
            !try_acquire_channel_ping_with_cooldown_at(
                &ping_times,
                77,
                thirty_minutes,
                now + thirty_minutes - Duration::from_secs(1),
            )
            .await
        );
        assert!(
            try_acquire_channel_ping_with_cooldown_at(&ping_times, 78, thirty_minutes, now,).await
        );
        assert!(
            try_acquire_channel_ping_with_cooldown_at(
                &ping_times,
                77,
                thirty_minutes,
                now + thirty_minutes,
            )
            .await
        );
        assert!(
            !try_acquire_channel_ping_with_cooldown_at(
                &ping_times,
                77,
                thirty_minutes,
                now + thirty_minutes + thirty_minutes - Duration::from_secs(1),
            )
            .await
        );
        assert!(
            try_acquire_channel_ping_with_cooldown_at(
                &ping_times,
                77,
                thirty_minutes,
                now + thirty_minutes * 2,
            )
            .await
        );
    }

    #[test]
    fn test_load_subscription_file() {
        let mut file = NamedTempFile::new().expect("create subscription fixture");
        file.write_all(
            br#"[{"id":"fixture-subscription","description":"Sanitized test subscription","filter":{"Condition":{"Simple":{"IsNpc":true}}},"action":{"channel_id":"123456789","ping_type":null}}]"#,
        )
        .expect("write subscription fixture");

        let subscriptions = load_vec_from_json_file::<Subscription>(file.path())
            .expect("parse subscription fixture");
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].id, "fixture-subscription");
        assert_eq!(subscriptions[0].action.channel_id, "123456789");
        assert_eq!(
            subscriptions[0].root_filter,
            FilterNode::Condition(Filter::Simple(SimpleFilter::IsNpc(true)))
        );
    }

    #[test]
    fn role_ping_subscription_round_trips_and_existing_ping_json_remains_compatible() {
        let subscription = Subscription {
            id: "role-ping".to_string(),
            description: "role notification".to_string(),
            root_filter: FilterNode::Condition(Filter::Simple(SimpleFilter::IsNpc(true))),
            action: Action {
                channel_id: "123456789".to_string(),
                ping_type: Some(PingType::Role {
                    role_id: 987_654_321_098_765_432,
                    max_ping_delay_minutes: Some(15),
                    ping_cooldown_minutes: Some(30),
                }),
            },
        };

        let serialized = serde_json::to_string(&subscription).expect("serialize role subscription");
        let serialized_value: serde_json::Value =
            serde_json::from_str(&serialized).expect("inspect serialized role subscription");
        assert_eq!(
            serialized_value["action"]["ping_type"]["Role"]["role_id"],
            serde_json::json!(987_654_321_098_765_432_u64)
        );
        assert_eq!(
            serialized_value["action"]["ping_type"]["Role"]["max_ping_delay_minutes"],
            serde_json::json!(15)
        );
        assert_eq!(
            serialized_value["action"]["ping_type"]["Role"]["ping_cooldown_minutes"],
            serde_json::json!(30)
        );
        let round_tripped: Subscription =
            serde_json::from_str(&serialized).expect("deserialize role subscription");
        assert_eq!(round_tripped, subscription);

        for (json, expected_ping_type) in [
            (
                r#"{"Here":{"max_ping_delay_minutes":5}}"#,
                Some(PingType::Here {
                    max_ping_delay_minutes: Some(5),
                    ping_cooldown_minutes: None,
                }),
            ),
            (
                r#"{"Everyone":{"max_ping_delay_minutes":null}}"#,
                Some(PingType::Everyone {
                    max_ping_delay_minutes: None,
                    ping_cooldown_minutes: None,
                }),
            ),
            (
                r#"{"Role":{"role_id":987654321098765432,"max_ping_delay_minutes":15}}"#,
                Some(PingType::Role {
                    role_id: 987_654_321_098_765_432,
                    max_ping_delay_minutes: Some(15),
                    ping_cooldown_minutes: None,
                }),
            ),
            ("null", None),
        ] {
            let legacy: Subscription = serde_json::from_str(&format!(
                r#"{{"id":"legacy","description":"legacy ping","filter":{{"Condition":{{"Simple":{{"IsNpc":true}}}}}},"action":{{"channel_id":"123456789","ping_type":{json}}}}}"#
            ))
            .expect("deserialize existing subscription JSON");
            assert_eq!(legacy.action.ping_type, expected_ping_type);
            if let Some(ping_type) = legacy.action.ping_type.as_ref() {
                assert_eq!(
                    ping_type.ping_cooldown_in_minutes(),
                    DEFAULT_CHANNEL_PING_COOLDOWN_MINUTES
                );
                assert!(
                    serde_json::to_value(&legacy).expect("serialize legacy subscription")["action"]
                        ["ping_type"]
                        .as_object()
                        .expect("legacy ping type")
                        .values()
                        .next()
                        .expect("legacy ping variant")
                        .get("ping_cooldown_minutes")
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn test_serialize_complex_subscription() {
        let complex_sub = Subscription {
            id: "complex_rule_1".to_string(),
            description: "Pings for valuable capital kills in key regions or near Jita".to_string(),
            action: Action {
                channel_id: "123456789".to_string(),
                ping_type: Some(PingType::Here {
                    max_ping_delay_minutes: None,
                    ping_cooldown_minutes: None,
                }),
            },
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Simple(SimpleFilter::TotalValue {
                    min: Some(1_000_000_000),
                    max: None,
                })),
                FilterNode::Or(vec![
                    FilterNode::And(vec![
                        FilterNode::Condition(Filter::Simple(SimpleFilter::Region(vec![10000042]))), // The Forge
                        FilterNode::Condition(Filter::Targeted(TargetedFilter {
                            condition: TargetableCondition::ShipGroup(vec![30, 883]),
                            target: Default::default(),
                        })), // Capitals, Supercarriers
                    ]),
                    FilterNode::Condition(Filter::Simple(SimpleFilter::LyRangeFrom(vec![
                        SystemRange {
                            system_id: 30000142,
                            range: 10.0,
                        },
                    ]))),
                ]),
                FilterNode::Not(Box::new(FilterNode::Condition(Filter::Simple(
                    SimpleFilter::IsNpc(true),
                )))),
            ]),
        };

        let json_output = serde_json::to_string_pretty(&complex_sub).unwrap();
        println!("Serialized JSON:\n{}", json_output);

        let parsed_value: serde_json::Value = serde_json::from_str(&json_output).unwrap();
        assert_eq!(parsed_value["id"], "complex_rule_1");
        assert!(parsed_value["filter"]["And"].is_array());
        let and_conditions = parsed_value["filter"]["And"].as_array().unwrap();
        assert_eq!(and_conditions.len(), 3);
        assert!(
            and_conditions[0]["Condition"]["Simple"]["TotalValue"]["min"].is_number(),
            "{}",
            and_conditions[0]["Condition"]["Simple"]["TotalValue"]["min"]
        );
        assert!(and_conditions[1]["Or"].is_array());
        assert!(and_conditions[2]["Not"]["Condition"]["Simple"]["IsNpc"].is_boolean());
    }

    #[test]
    fn test_distance() {
        let system1 = System {
            id: 30000142, // Jita
            name: "Jita".to_string(),
            security_status: 0.9,
            region_id: 10000002, // The Forge
            region: "The Forge".to_string(),
            x: 0.0,
            y: 0.0,
            z: 0.0,
        };

        let system2 = System {
            id: 30000143, // Perimeter
            name: "Perimeter".to_string(),
            security_status: 0.9,
            region_id: 10000002, // The Forge
            region: "The Forge".to_string(),
            x: 1.0,
            y: 1.0,
            z: 1.0,
        };

        let distance = ((system2.x - system1.x).powi(2)
            + (system2.y - system1.y).powi(2)
            + (system2.z - system1.z).powi(2))
        .sqrt();

        assert!(
            (distance - 1.732).abs() < 0.001,
            "Distance calculation is incorrect"
        );
    }

    // #[test]
    // fn test_migrate_schema() {
    //     // You would run this logic once, perhaps in a separate binary or a temporary function call.
    //
    //     // 1. Define the old structures exactly as they were.
    //     #[derive(Deserialize)]
    //     struct OldAction {
    //         channel_id: String,
    //         ping_type: Option<OldPingType>,
    //     }
    //
    //     #[derive(Deserialize)]
    //     #[serde(rename_all = "PascalCase")]
    //     enum OldPingType {
    //         Here,
    //         Everyone,
    //     }
    //
    //     #[derive(Deserialize)]
    //     struct OldSubscription {
    //         id: String,
    //         description: String,
    //         root_filter: FilterNode,
    //         action: OldAction,
    //     }
    //
    //     // 2. Read and deserialize the old file.
    //     let old_json = fs::read_to_string("config/your_guild_id.json")?;
    //     let old_subscriptions: Vec<OldSubscription> = serde_json::from_str(&old_json)?;
    //
    //     // 3. Convert to the new format.
    //     let new_subscriptions: Vec<Subscription> = old_subscriptions
    //         .into_iter()
    //         .map(|old_sub| {
    //             let new_ping_type = old_sub.action.ping_type.map(|pt| match pt {
    //                 OldPingType::Here => PingType::Here {
    //                     max_ping_delay_minutes: None,
    //                 },
    //                 OldPingType::Everyone => PingType::Everyone {
    //                     max_ping_delay_minutes: None,
    //                 },
    //             });
    //
    //             Subscription {
    //                 id: old_sub.id,
    //                 description: old_sub.description,
    //                 root_filter: old_sub.root_filter,
    //                 action: Action {
    //                     channel_id: old_sub.action.channel_id,
    //                     ping_type: new_ping_type,
    //                 },
    //             }
    //         })
    //         .collect();
    //
    //     // 4. Save the new file.
    //     save_subscriptions_for_guild(guild_id, &new_subscriptions)?;
    // }
}
