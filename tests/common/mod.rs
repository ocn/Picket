//! Shared test helpers for integration tests.

use killbot_rust::config::{load_app_config, AppState, Subscription};
use killbot_rust::models::ZkData;
use moka::future::Cache;
use serenity::model::id::GuildId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Discord channel ID for test embeds
pub const TEST_CHANNEL_ID: u64 = 1115807643748012072;

/// Capital ship group IDs
pub const CAPITAL_GROUPS: &[u32] = &[883, 547, 4594, 485, 1538];

/// Supercapital ship group IDs
pub const SUPERCAP_GROUPS: &[u32] = &[30, 659];

/// Metenox drill type ID
pub const METENOX_DRILL: u32 = 81826;

/// Structure group IDs (upwell structures, POSes, etc.)
pub const STRUCTURE_GROUPS: &[u32] = &[
    1408, 2017, 2016, 1657, 1404, 1406, 1719, 1441, 1327, 1329, 1330, 1442, 1331, 1547, 1548, 1546,
    1562, 1328, 1332, 4744, 4736, 1652, 1537, 1653,
];

/// Load a killmail fixture from the resources directory
pub fn load_fixture(name: &str) -> ZkData {
    let path = format!("resources/{}", name);
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read fixture {}: {}", path, e));
    serde_json::from_str(&contents)
        .unwrap_or_else(|e| panic!("Failed to parse fixture {}: {}", path, e))
}

/// Create an AppState with the given subscriptions
pub async fn create_app_state_with_subscriptions(subscriptions: Vec<Subscription>) -> Arc<AppState> {
    let app_config =
        load_app_config().expect("Failed to load config - check .env for DISCORD_BOT_TOKEN");

    let systems = killbot_rust::config::load_systems().unwrap_or_default();
    let ships = killbot_rust::config::load_ships().unwrap_or_default();
    let names = killbot_rust::config::load_names().unwrap_or_default();
    let tickers = killbot_rust::config::load_tickers().unwrap_or_default();

    // Create subscription map with a fake guild ID
    let fake_guild_id = GuildId(123456789);
    let mut subs_map = HashMap::new();
    subs_map.insert(fake_guild_id, subscriptions);

    Arc::new(AppState {
        subscriptions: Arc::new(std::sync::RwLock::new(subs_map)),
        systems: Arc::new(std::sync::RwLock::new(systems)),
        ships: Arc::new(std::sync::RwLock::new(ships)),
        names: Arc::new(std::sync::RwLock::new(names)),
        tickers: Arc::new(std::sync::RwLock::new(tickers)),
        celestial_cache: Cache::new(10_000),
        esi_client: Default::default(),
        systems_file_lock: Mutex::new(()),
        ships_file_lock: Mutex::new(()),
        names_file_lock: Mutex::new(()),
        tickers_file_lock: Mutex::new(()),
        subscriptions_file_lock: Mutex::new(()),
        app_config: Arc::new(app_config),
        last_ping_times: Mutex::new(HashMap::new()),
        user_standings: Arc::new(Default::default()),
        user_standings_file_lock: Default::default(),
        sso_states: Arc::new(Default::default()),
    })
}

/// Initialize tracing for tests
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init()
        .ok();
}
