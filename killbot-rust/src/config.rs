use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use config::{Config, ConfigError, Environment, File};
use moka::future::Cache;
use serenity::model::id::GuildId;
use tokio::sync::Mutex;
use tracing::{info, warn, error};
use crate::esi::{Celestial, EsiClient};

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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum FilterNode {
    Condition(Filter),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum PingType {
    Here,
    Everyone,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
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

// --- App Configuration & State ---

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub discord_bot_token: String,
    pub discord_client_id: u64,
}

pub struct AppState {
    pub systems: Arc<RwLock<HashMap<u32, System>>>,
    pub ships: Arc<RwLock<HashMap<u32, u32>>>,
    pub names: Arc<RwLock<HashMap<u64, String>>>,
    pub subscriptions: Arc<RwLock<HashMap<GuildId, Vec<Subscription>>>>,
    pub app_config: Arc<AppConfig>,
    pub esi_client: EsiClient,
    pub celestial_cache: Cache<u32, Arc<Celestial>>,
    pub systems_file_lock: Mutex<()>,
    pub ships_file_lock: Mutex<()>,
    pub names_file_lock: Mutex<()>,
}

impl AppState {
    pub fn new(
        app_config: AppConfig,
        systems: HashMap<u32, System>,
        ships: HashMap<u32, u32>,
        names: HashMap<u64, String>,
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
            systems_file_lock: Mutex::new(()),
            ships_file_lock: Mutex::new(()),
            names_file_lock: Mutex::new(()),
        }
    }
}

// --- Configuration Loading & Saving ---

// Correctly parses a file containing a JSON array using serde_json
fn load_vec_from_json_file<T: for<'de> Deserialize<'de>>(file_path: &Path) -> Result<Vec<T>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(file_path)?;
    let data = serde_json::from_str(&content)?;
    Ok(data)
}

fn load_map_from_json_file<K, V>(file_path: &Path) -> Result<HashMap<K, V>, ConfigError>
where
    K: std::cmp::Eq + std::hash::Hash + for<'de> Deserialize<'de>,
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

pub fn load_app_config() -> Result<AppConfig, ConfigError> {
    let settings = Config::builder()
        .add_source(Environment::default().separator("__"))
        .set_override("discord_bot_token", std::env::var("DISCORD_BOT_TOKEN").unwrap_or_default())?
        .set_override("discord_client_id", std::env::var("DISCORD_CLIENT_ID").unwrap_or_default())?
        .build()?;
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

pub fn load_all_subscriptions(dir: &str) -> HashMap<GuildId, Vec<Subscription>> {
    let mut all_subscriptions = HashMap::new();
    let path = Path::new(dir);
    if !path.is_dir() { return all_subscriptions; }

    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            if let Some(filename_str) = path.file_name().and_then(|s| s.to_str()) {
                if let Some(guild_id_str) = filename_str.strip_suffix(".json") {
                    if let Ok(guild_id) = guild_id_str.parse::<u64>() {
                        // Use the correct parsing function for array-based JSON files
                        if let Ok(subs) = load_vec_from_json_file::<Subscription>(&path) {
                            info!("Loaded {} subscriptions for guild {}", subs.len(), guild_id);
                            all_subscriptions.insert(GuildId(guild_id), subs);
                        } else {
                            warn!("Could not parse {} as subscription file.", filename_str);
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
    use std::path::Path;

    #[test]
    fn test_load_subscription_file() {
        let path = Path::new("../config/888224317991706685.new.json");
        assert!(path.exists(), "Subscription file does not exist at {:?}", path);

        // Use the correct function that handles array-based JSON
        let result = load_vec_from_json_file::<Subscription>(path);
        assert!(result.is_ok(), "Failed to parse subscription file: {:?}", result.err());

        let subscriptions = result.unwrap();
        assert_eq!(subscriptions.len(), 202, "Incorrect number of subscriptions loaded");
        assert_eq!(subscriptions[0].id, "1");
        assert_eq!(subscriptions[0].action.channel_id, "1090110979083354200");
    }

    #[test]
    fn test_serialize_complex_subscription() {
        let complex_sub = Subscription {
            id: "complex_rule_1".to_string(),
            description: "Pings for valuable capital kills in key regions or near Jita".to_string(),
            action: Action {
                channel_id: "123456789".to_string(),
                ping_type: Some(PingType::Here),
            },
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::TotalValue { min: Some(1_000_000_000), max: None }),
                FilterNode::Or(vec![
                    FilterNode::And(vec![
                        FilterNode::Condition(Filter::Region(vec![10000042])), // The Forge
                        FilterNode::Condition(Filter::ShipGroup(vec![30, 883])), // Capitals, Supercarriers
                    ]),
                    FilterNode::Condition(Filter::LyRangeFrom {
                        systems: vec![30000142], // Jita
                        range: 10.0,
                    }),
                ]),
                FilterNode::Not(Box::new(FilterNode::Condition(Filter::IsNpc(true)))),
            ]),
        };

        let json_output = serde_json::to_string_pretty(&complex_sub).unwrap();
        println!("Serialized JSON:\n{}", json_output);

        let parsed_value: serde_json::Value = serde_json::from_str(&json_output).unwrap();
        assert_eq!(parsed_value["id"], "complex_rule_1");
        assert!(parsed_value["filter"]["And"].is_array());
        let and_conditions = parsed_value["filter"]["And"].as_array().unwrap();
        assert_eq!(and_conditions.len(), 3);
        assert!(and_conditions[0]["Condition"]["TotalValue"]["min"].is_number());
        assert!(and_conditions[1]["Or"].is_array());
        assert!(and_conditions[2]["Not"]["Condition"]["IsNpc"].is_boolean());
    }
}