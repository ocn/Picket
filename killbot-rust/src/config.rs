use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use config::{Config, ConfigError, File};
use moka::future::Cache;
use serenity::model::id::GuildId;
use tracing::{info, warn};
use crate::esi::{Celestial, EsiClient};

// --- Static Game Data Structs ---

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct System {
    #[serde(rename = "system_id")]
    pub id: u32,
    #[serde(rename = "system_name")]
    pub name: String,
    pub security_status: f64,
    pub region_id: u32,
    #[serde(rename = "region_name")]
    pub region: String,
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Ship {
    #[serde(rename = "ship_id")]
    pub id: u32,
    #[serde(rename = "ship_name")]
    pub name: String,
    pub group_id: u32,
    #[serde(rename = "group_name")]
    pub group: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Name {
    pub id: u64,
    pub name: String,
    pub category: String,
}

// --- Subscription AST Structs ---

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub enum Filter {
    TotalValue { min: Option<u64>, max: Option<u64> },
    DroppedValue { min: Option<u64>, max: Option<u64> },
    Region(Vec<u32>),
    System(Vec<u32>),
    Security(String),
    Alliance(Vec<u64>),
    Corporation(Vec<u64>),
    Character(Vec<u64>),
    ShipType(Vec<u32>),
    ShipGroup(Vec<u32>),
    LyRangeFrom { systems: Vec<u32>, range: f64 },
    IsNpc(bool),
    IsSolo(bool),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub enum FilterNode {
    Condition(Filter),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub enum PingType {
    Here,
    Everyone,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Action {
    pub channel_id: u64,
    pub ping_type: Option<PingType>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Subscription {
    pub id: String,
    pub description: String,
    #[serde(rename = "filter")]
    pub root_filter: FilterNode,
    pub action: Action,
}

// --- App Configuration & State ---

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub discord_token: String,
    pub application_id: u64,
    pub redis_q_url: String,
}

pub struct AppState {
    pub systems: Arc<RwLock<HashMap<u32, System>>>,
    pub ships: Arc<RwLock<HashMap<u32, Ship>>>,
    pub names: Arc<RwLock<HashMap<u64, Name>>>,
    pub subscriptions: Arc<RwLock<HashMap<GuildId, Vec<Subscription>>>>,
    pub app_config: Arc<AppConfig>,
    pub esi_client: EsiClient,
    pub celestial_cache: Cache<u32, Arc<Celestial>>,
}

impl AppState {
    pub fn new(
        app_config: AppConfig,
        systems: HashMap<u32, System>,
        ships: HashMap<u32, Ship>,
        names: HashMap<u64, Name>,
        subscriptions: HashMap<GuildId, Vec<Subscription>>,
    ) -> Self {
        AppState {
            systems: Arc::new(RwLock::new(systems)),
            ships: Arc::new(RwLock::new(ships)),
            names: Arc::new(RwLock::new(names)),
            subscriptions: Arc::new(RwLock::new(subscriptions)),
            app_config: Arc::new(app_config),
            esi_client: EsiClient::new(),
            celestial_cache: Cache::new(10_000),
        }
    }
}

// --- Configuration Loading & Saving ---

fn load_from_file<T: for<'de> Deserialize<'de>>(file_path: &Path) -> Result<T, ConfigError> {
    Config::builder()
        .add_source(File::from(file_path))
        .build()?
        .try_deserialize()
}

pub fn load_app_config() -> Result<AppConfig, ConfigError> {
    load_from_file(Path::new("config/app_config.json"))
}

pub fn load_systems() -> Result<HashMap<u32, System>, ConfigError> {
    let systems_vec: Vec<System> = load_from_file(Path::new("config/systems.json"))?;
    Ok(systems_vec.into_iter().map(|s| (s.id, s)).collect())
}

pub fn load_ships() -> Result<HashMap<u32, Ship>, ConfigError> {
    let ships_vec: Vec<Ship> = load_from_file(Path::new("config/ships.json"))?;
    Ok(ships_vec.into_iter().map(|s| (s.id, s)).collect())
}

pub fn load_names() -> Result<HashMap<u64, Name>, ConfigError> {
    let names_vec: Vec<Name> = load_from_file(Path::new("config/names.json"))?;
    Ok(names_vec.into_iter().map(|n| (n.id, n)).collect())
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
                        match load_from_file::<Vec<Subscription>>(&path) {
                            Ok(subs) => {
                                info!("Loaded {} subscriptions for guild {}", subs.len(), guild_id);
                                all_subscriptions.insert(GuildId(guild_id), subs);
                            },
                            Err(e) => warn!("Failed to load subscriptions for guild {}: {}", guild_id, e),
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