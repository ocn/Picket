//! Shared test helpers for integration tests.

use killbot_rust::config::{load_app_config, AppConfig, AppState, Subscription, System};
use killbot_rust::models::ZkData;
use moka::future::Cache;
use serenity::model::id::GuildId;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

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

/// Load a text fixture from the resources directory.
pub fn load_text_fixture(name: &str) -> String {
    let path = format!("resources/{name}");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("Failed to read fixture {path}: {error}"))
}

/// Create an AppState with the given subscriptions
pub async fn create_app_state_with_subscriptions(
    subscriptions: Vec<Subscription>,
) -> Arc<AppState> {
    let app_config =
        load_app_config().expect("Failed to load config - check .env for DISCORD_BOT_TOKEN");

    let systems = killbot_rust::config::load_systems().unwrap_or_default();
    let ships = killbot_rust::config::load_ships().unwrap_or_default();
    let names = killbot_rust::config::load_names().unwrap_or_default();
    let tickers = killbot_rust::config::load_tickers().unwrap_or_default();
    let group_names = killbot_rust::config::load_group_names().unwrap_or_default();

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
        group_names: Arc::new(std::sync::RwLock::new(group_names)),
        celestial_cache: Cache::new(10_000),
        esi_client: Default::default(),
        systems_file_lock: Mutex::new(()),
        ships_file_lock: Mutex::new(()),
        names_file_lock: Mutex::new(()),
        tickers_file_lock: Mutex::new(()),
        group_names_file_lock: Mutex::new(()),
        subscriptions_file_lock: Mutex::new(()),
        app_config: Arc::new(app_config),
        last_ping_times: Mutex::new(HashMap::new()),
        user_standings: Arc::new(Default::default()),
        user_standings_file_lock: Default::default(),
        sso_states: Arc::new(Default::default()),
    })
}

/// Create an AppState with pre-seeded ships/names/tickers/group_names caches and no
/// network-touching config load. Unlike `create_app_state_with_subscriptions`, this does
/// NOT call `load_app_config` (which requires `DISCORD_BOT_TOKEN`), so it is safe to use
/// from non-ignored tests that must run without a `.env` file or network access.
#[allow(dead_code)]
pub fn create_app_state_with_seeded_caches(
    subscriptions: Vec<Subscription>,
    systems: HashMap<u32, System>,
    ships: HashMap<u32, u32>,
    names: HashMap<u64, String>,
    tickers: HashMap<u64, String>,
    group_names: HashMap<u32, String>,
) -> Arc<AppState> {
    let app_config = AppConfig {
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
    };

    let fake_guild_id = GuildId(123456789);
    let mut subs_map = HashMap::new();
    subs_map.insert(fake_guild_id, subscriptions);

    Arc::new(AppState {
        subscriptions: Arc::new(std::sync::RwLock::new(subs_map)),
        systems: Arc::new(std::sync::RwLock::new(systems)),
        ships: Arc::new(std::sync::RwLock::new(ships)),
        names: Arc::new(std::sync::RwLock::new(names)),
        tickers: Arc::new(std::sync::RwLock::new(tickers)),
        group_names: Arc::new(std::sync::RwLock::new(group_names)),
        celestial_cache: Cache::new(10_000),
        esi_client: Default::default(),
        systems_file_lock: Mutex::new(()),
        ships_file_lock: Mutex::new(()),
        names_file_lock: Mutex::new(()),
        tickers_file_lock: Mutex::new(()),
        group_names_file_lock: Mutex::new(()),
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

static TEMPORARY_DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Serialises `CREATE DATABASE` / `DROP DATABASE` admin operations across every
/// test in this binary. PostgreSQL takes a lock on the template database for the
/// duration of each CREATE/DROP; running many of them concurrently (the default
/// test-thread count) contends on that lock and, under machine load, the admin
/// connection or the statement itself can intermittently fail. Holding this
/// process-wide async mutex only around the short admin section removes that
/// contention while every test still provisions its own uniquely named database
/// and runs its body in parallel. A plain async mutex (rather than a Postgres
/// advisory lock) is sufficient because the contention is between tokio tasks
/// inside this single test process.
static DATABASE_ADMIN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A throwaway PostgreSQL database, created against `CONTRACT_TEST_DATABASE_URL`
/// and dropped in `destroy()`. Generic across feeds (contract, sov, ...): each
/// test file connects its own store type(s) against `url` and is responsible
/// for running whatever migrator applies its schema.
///
/// `tests/test_contract_intelligence.rs` keeps its own private copy of this
/// helper (with a `store()` convenience bound to `ContractCollectionStore`);
/// this generic version exists for other integration test binaries so they
/// do not need to depend on `contract_intelligence` merely to provision a
/// database.
#[allow(dead_code)]
pub struct TemporaryDatabase {
    admin_url: String,
    database_name: String,
    pub url: String,
}

#[allow(dead_code)]
impl TemporaryDatabase {
    pub async fn new() -> Self {
        let database = Self::unavailable();
        database.create().await;
        database
    }

    pub fn unavailable() -> Self {
        let admin_url = std::env::var("CONTRACT_TEST_DATABASE_URL").expect(
            "CONTRACT_TEST_DATABASE_URL must point to a PostgreSQL instance for integration tests (for example postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts)",
        );
        let sequence = TEMPORARY_DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("killbot_test_{}_{}", std::process::id(), sequence);
        let mut url = Url::parse(&admin_url).expect("valid CONTRACT_TEST_DATABASE_URL");
        url.set_path(&format!("/{database_name}"));
        Self {
            admin_url,
            database_name,
            url: url.to_string(),
        }
    }

    pub async fn create(&self) {
        let _admin_guard = DATABASE_ADMIN_LOCK.lock().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
            .expect("connect to test PostgreSQL");
        sqlx::query(&format!("CREATE DATABASE {}", self.database_name))
            .execute(&admin)
            .await
            .expect("create temporary test database");
        admin.close().await;
    }

    pub async fn destroy(self) {
        let _admin_guard = DATABASE_ADMIN_LOCK.lock().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
            .expect("reconnect to test PostgreSQL");
        sqlx::query(&format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.database_name
        ))
        .execute(&admin)
        .await
        .expect("drop temporary test database");
        admin.close().await;
    }
}
