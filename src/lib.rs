use rand::{distributions::Alphanumeric, Rng};
use serenity::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn, Level};

pub mod commands;
pub mod config;
pub mod contract_intelligence;
pub mod discord_bot;
pub mod esi;
pub mod esi_cache;
pub mod feed;
pub mod location_evidence;
pub mod models;
pub mod pipeline;
pub mod presentation;
pub mod processor;
pub mod sov_feed;
pub mod structure_resolver;

use crate::commands::contract_subscribe::ContractSubscribeCommand;
use crate::commands::contract_unsubscribe::ContractUnsubscribeCommand;
use crate::commands::find_unsubscribed::FindUnsubscribedChannelsCommand;
use crate::commands::health::HealthCommand;
use crate::commands::sov_subscribe::SovSubscribeCommand;
use crate::commands::sov_timers::SovTimersCommand;
use crate::commands::sov_unsubscribe::SovUnsubscribeCommand;
use commands::diag::DiagCommand;
use commands::subscribe::SubscribeCommand;
use commands::sync_clear::SyncClearCommand;
use commands::sync_remove::SyncRemoveCommand;
use commands::sync_standings::SyncStandingsCommand;
use commands::unsubscribe::UnsubscribeCommand;
use commands::{Command, PingCommand};
use config::FeedProvider;
use discord_bot::CommandMap;
use feed::KillmailFeed;
use structure_resolver::{
    AuthenticatedStructureResolver, StructureResolver, StructureResolverConfig,
    StructureResolverRuntimeStatus,
};

pub struct AppStateContainer;

impl TypeMapKey for AppStateContainer {
    type Value = Arc<config::AppState>;
}

pub struct ContractStoreContainer;

impl TypeMapKey for ContractStoreContainer {
    type Value = contract_intelligence::ContractStoreHandle;
}

pub struct SovStoreContainer;

impl TypeMapKey for SovStoreContainer {
    type Value = sov_feed::SovStoreHandle;
}

/// Shared handle to the sov feed's stargate reachability source (ticket
/// 04): populated once at start-up (see [`sov_reachability_source`]) and
/// read by the collector loops and `/sov_timers` alike. Unlike
/// [`SovStoreContainer`] this never changes after start-up -- there is no
/// live reload of `config/stargates.json` -- so it needs no interior
/// mutability of its own.
pub struct SovReachabilityContainer;

impl TypeMapKey for SovReachabilityContainer {
    type Value = Arc<dyn sov_feed::SovReachabilitySource>;
}

/// Fixed sov campaign collection cadence (spec "Sources and cadence":
/// "Sovereignty campaigns are polled every sixty seconds").
const SOV_COLLECTION_INTERVAL: Duration = Duration::from_secs(60);

/// T-minus stage-evaluation cadence: independent of, and cheaper than,
/// `SOV_COLLECTION_INTERVAL` because it makes no ESI request (ticket 03:
/// "A stage-evaluation pass runs at least every thirty seconds").
const SOV_STAGE_INTERVAL: Duration = Duration::from_secs(30);

/// Default reachability origin: Turnur (spec "Reachability": "Single
/// origin: Turnur (configurable by environment)").
const SOV_DEFAULT_HOME_SYSTEM_ID: i64 = 30_002_086;

/// Reads `SOV_HOME_SYSTEM_ID`, falling back to Turnur when unset or not a
/// valid integer. Never panics: a malformed override just logs a warning
/// and keeps the default rather than taking down start-up.
fn sov_home_system_id() -> i64 {
    match std::env::var("SOV_HOME_SYSTEM_ID") {
        Ok(value) => value.trim().parse::<i64>().unwrap_or_else(|_| {
            warn!(
                "SOV_HOME_SYSTEM_ID '{value}' is not a valid integer; using the default (Turnur, {SOV_DEFAULT_HOME_SYSTEM_ID})"
            );
            SOV_DEFAULT_HOME_SYSTEM_ID
        }),
        Err(_) => SOV_DEFAULT_HOME_SYSTEM_ID,
    }
}

/// Loads `config/stargates.json` and computes reachability from the
/// configured home system, or degrades to "graph unavailable" when the
/// file is missing or malformed (spec: "fails loudly" means logged, never
/// a process exit or a delay to Discord start-up -- review decisions
/// carried over from ticket 02's ADR 0002 discussion). Pure CPU/local-disk
/// work over a few thousand systems, so this runs synchronously before the
/// Discord client is built without risking any startup delay.
fn sov_reachability_source(enabled: bool) -> Arc<dyn sov_feed::SovReachabilitySource> {
    if !enabled {
        return Arc::new(sov_feed::StaticSovReachability(None));
    }
    let home_system_id = sov_home_system_id();
    let path = std::path::Path::new(sov_feed::DEFAULT_STARGATE_GRAPH_PATH);
    match sov_feed::load_stargate_graph_file(path) {
        Ok(file) => {
            let graph = sov_feed::StargateGraph::from_file(file);
            info!(
                "Loaded stargate graph: sde_version={}, systems={}, edges={}, home_system_id={home_system_id}",
                graph.sde_version,
                graph.system_count(),
                graph.edge_count()
            );
            let reachability = sov_feed::Reachability::compute(&graph, home_system_id, &[]);
            Arc::new(sov_feed::StaticSovReachability(Some(reachability)))
        }
        Err(error) => {
            error!(
                "Sov reachability disabled: could not load {}: {error}. The sov feed runs without reachability: Reachable filter leaves never match and /sov_timers reports the graph as unavailable.",
                path.display()
            );
            Arc::new(sov_feed::StaticSovReachability(None))
        }
    }
}

fn generate_queue_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

pub async fn run() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Killbot-Rust...");

    // --- Load all configurations ---
    let app_config = match config::load_app_config() {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load application configuration: {}", e);
            return;
        }
    };

    info!("Feed provider: {}", app_config.killmail_feed_provider);
    info!("ESI HTTP timeout: {}s", app_config.esi_http_timeout_secs);
    info!(
        "Killmail process timeout: {}s",
        app_config.killmail_process_timeout_secs
    );
    info!(
        "RedisQ connect timeout: {}s / request timeout: {}s",
        app_config.redisq_connect_timeout_secs, app_config.redisq_request_timeout_secs
    );
    info!(
        "R2Z2 connect timeout: {}s / request timeout: {}s / poll interval: {}s",
        app_config.r2z2_connect_timeout_secs,
        app_config.r2z2_request_timeout_secs,
        app_config.r2z2_poll_interval_secs
    );

    let systems = config::load_systems().unwrap_or_else(|e| {
        warn!(
            "Failed to load systems.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let ships = config::load_ships().unwrap_or_else(|e| {
        warn!(
            "Failed to load ships.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let names = config::load_names().unwrap_or_else(|e| {
        warn!(
            "Failed to load names.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let tickers = config::load_tickers().unwrap_or_else(|e| {
        warn!(
            "Failed to load tickers.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let group_names = config::load_group_names().unwrap_or_else(|e| {
        warn!(
            "Failed to load group_names.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });

    let user_standings = config::load_user_standings().unwrap_or_else(|e| {
        warn!(
            "Failed to load user_standings.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });

    let subscriptions = config::load_all_subscriptions("config/");

    // --- Initialize application state ---
    let app_state = Arc::new(config::AppState::new(
        app_config.clone(),
        systems,
        ships,
        names,
        tickers,
        group_names,
        subscriptions,
        user_standings,
    ));

    // --- Initialize Commands ---
    let mut command_map: HashMap<String, Box<dyn Command>> = HashMap::new();

    let ping_command = Box::new(PingCommand);
    command_map.insert(ping_command.name(), ping_command);

    let subscribe_command = Box::new(SubscribeCommand);
    command_map.insert(subscribe_command.name(), subscribe_command);

    let unsubscribe_command = Box::new(UnsubscribeCommand);
    command_map.insert(unsubscribe_command.name(), unsubscribe_command);

    let diag_command = Box::new(DiagCommand);
    command_map.insert(diag_command.name(), diag_command);

    let health_command = Box::new(HealthCommand);
    command_map.insert(health_command.name(), health_command);

    let sync_standings_command = Box::new(SyncStandingsCommand);
    command_map.insert(sync_standings_command.name(), sync_standings_command);

    let sync_remove_command = Box::new(SyncRemoveCommand);
    command_map.insert(sync_remove_command.name(), sync_remove_command);

    let sync_clear_command = Box::new(SyncClearCommand);
    command_map.insert(sync_clear_command.name(), sync_clear_command);

    let find_unsubscribed_command = Box::new(FindUnsubscribedChannelsCommand);
    command_map.insert(find_unsubscribed_command.name(), find_unsubscribed_command);

    let contract_subscribe_command = Box::new(ContractSubscribeCommand);
    command_map.insert(
        contract_subscribe_command.name(),
        contract_subscribe_command,
    );

    let contract_unsubscribe_command = Box::new(ContractUnsubscribeCommand);
    command_map.insert(
        contract_unsubscribe_command.name(),
        contract_unsubscribe_command,
    );

    let sov_subscribe_command = Box::new(SovSubscribeCommand);
    command_map.insert(sov_subscribe_command.name(), sov_subscribe_command);

    let sov_unsubscribe_command = Box::new(SovUnsubscribeCommand);
    command_map.insert(sov_unsubscribe_command.name(), sov_unsubscribe_command);

    let sov_timers_command = Box::new(SovTimersCommand);
    command_map.insert(sov_timers_command.name(), sov_timers_command);

    let command_map_arc = Arc::new(command_map);

    let contract_runtime = match std::env::var("CONTRACT_DATABASE_URL") {
        Ok(database_url) => Some((
            database_url,
            contract_intelligence::new_contract_store_handle(),
        )),
        Err(_) => {
            info!("Contract collection disabled: CONTRACT_DATABASE_URL is not configured");
            None
        }
    };
    let feed_health_telemetry = Arc::new(feed::FeedHealthTelemetry::new());
    let r2z2_health_enabled = app_config.killmail_feed_provider == FeedProvider::R2z2;

    // Sov campaign feed: same optionality as the contract runtime (spec
    // "Persistence and process model"). Nothing here touches Postgres: the
    // handle starts empty and `spawn_sov_collection_loop` (below, after the
    // Discord client exists) connects and applies migrations from inside
    // its own retry loop, so a briefly-unavailable Postgres at boot can
    // never delay the gateway connect or the killmail pipeline (review
    // finding 1 on
    // `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
    let sov_store_handle = sov_feed::new_sov_store_handle();

    // Stargate reachability graph (ticket 04): loaded synchronously here,
    // before the Discord client exists, because it is pure local-disk and
    // CPU work over a few thousand systems (milliseconds), never network
    // IO -- so, unlike the sov store, there is no need to defer it to a
    // background retry loop. A missing or malformed
    // `config/stargates.json` degrades to "graph unavailable" rather than
    // failing start-up (see `sov_reachability_source`'s doc comment).
    let sov_reachability = sov_reachability_source(contract_runtime.is_some());

    // --- Start Discord Bot ---
    let discord_token = app_config.discord_bot_token.clone();
    let intents = GatewayIntents::non_privileged()
        | GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT
        | GatewayIntents::GUILD_INTEGRATIONS;
    let mut client = Client::builder(&discord_token, intents)
        .event_handler(discord_bot::Handler)
        .await
        .expect("Error creating Discord client");

    {
        let mut data = client.data.write().await;
        data.insert::<AppStateContainer>(app_state.clone());
        data.insert::<CommandMap>(command_map_arc.clone());
        if let Some((_, store_handle)) = &contract_runtime {
            data.insert::<ContractStoreContainer>(store_handle.clone());
        }
        if contract_runtime.is_some() {
            data.insert::<SovStoreContainer>(sov_store_handle.clone());
            data.insert::<SovReachabilityContainer>(sov_reachability.clone());
        }
    }

    let http_client = client.cache_and_http.http.clone();

    if let Some((database_url, _)) = &contract_runtime {
        let timeout = Duration::from_secs(app_config.esi_http_timeout_secs);
        match sov_feed::HttpSovereigntyEsi::new(timeout) {
            Ok(esi) => {
                let esi: Arc<dyn sov_feed::SovereigntyEsi> = Arc::new(esi);
                let delivery: Arc<dyn sov_feed::SovDelivery> =
                    Arc::new(discord_bot::DiscordSovDelivery::new(http_client.clone()));
                let directory: Arc<dyn sov_feed::SovSystemDirectory> = Arc::new(
                    discord_bot::DiscordSovSystemDirectory::new(app_state.clone()),
                );
                let tickers: Arc<dyn sov_feed::SovTickerResolver> = Arc::new(
                    discord_bot::DiscordSovTickerResolver::new(app_state.clone()),
                );
                spawn_sov_collection_loop(
                    database_url.clone(),
                    sov_store_handle,
                    SOV_COLLECTION_INTERVAL,
                    esi.clone(),
                    delivery.clone(),
                    directory.clone(),
                    tickers.clone(),
                    sov_reachability.clone(),
                );
                // T-minus stage evaluation (ticket 03): a separate,
                // independently-reconnecting loop rather than a faster
                // cadence inside `run_sov_collection_loop`, mirroring how
                // `run_terminal_resolution_recovery_loop` and
                // `run_proximity_reconciliation_loop` already run their
                // own cadences alongside the contract feed's main
                // collection loop. It never touches `sov_store_handle`
                // (already owned by the collection loop above) or ESI, so
                // a stall here cannot affect campaign polling or command
                // handling.
                spawn_sov_stage_loop(
                    database_url.clone(),
                    SOV_STAGE_INTERVAL,
                    esi,
                    delivery,
                    directory,
                    tickers,
                    sov_reachability,
                );
            }
            Err(error) => {
                warn!("Sov campaign feed disabled: HTTP client initialization failed: {error}");
            }
        }
    } else {
        info!("Sov campaign feed disabled: CONTRACT_DATABASE_URL is not configured");
    }

    if let Some((database_url, store_handle)) = contract_runtime {
        let interval = std::env::var("CONTRACT_COLLECTION_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300);
        let timeout = Duration::from_secs(app_config.esi_http_timeout_secs);
        let ship_groups = Arc::new(discord_bot::DiscordShipGroupResolver::new(
            app_state.clone(),
        ));
        let delivery = Arc::new(discord_bot::DiscordContractDelivery::new(
            http_client.clone(),
        ));
        let ping_limiter = Arc::new(contract_intelligence::AppStateContractPingLimiter::new(
            app_state.clone(),
        ));
        let health_config = match contract_intelligence::HealthRuntimeConfig::from_environment() {
            Ok(config) => Some(config),
            Err(error) => {
                warn!("health monitoring disabled by invalid configuration: {error}");
                None
            }
        };
        let health_database_url = database_url.clone();
        let (structure_resolver, structure_resolver_runtime): (
            Option<Arc<dyn StructureResolver>>,
            Option<StructureResolverRuntimeStatus>,
        ) = match StructureResolverConfig::from_environment() {
            Ok(config) if config.is_enabled() => {
                let runtime = config.runtime_status();
                match AuthenticatedStructureResolver::new(config, timeout) {
                    Ok(resolver) => (Some(Arc::new(resolver)), Some(runtime)),
                    Err(error) => {
                        warn!("structure resolver disabled after HTTP initialization failure: {error}");
                        (
                            None,
                            Some(StructureResolverRuntimeStatus::initialization_failed()),
                        )
                    }
                }
            }
            Ok(config) => (None, Some(config.runtime_status())),
            Err(error) => {
                warn!("structure resolver disabled by invalid configuration: {error}");
                (
                    None,
                    Some(StructureResolverRuntimeStatus::invalid_configuration()),
                )
            }
        };
        match contract_intelligence::contract_regional_concurrency_from_environment() {
            Ok(max_concurrent_regions) => {
                contract_intelligence::spawn_contract_collection_loop_with_notifications_structure_resolver_and_region_concurrency(
                    database_url.clone(),
                    store_handle,
                    Duration::from_secs(interval),
                    timeout,
                    ship_groups.clone(),
                    delivery.clone(),
                    ping_limiter.clone(),
                    structure_resolver.clone(),
                    structure_resolver_runtime,
                    max_concurrent_regions,
                );
            }
            Err(error) => {
                warn!("contract collection disabled by invalid CONTRACT_REGIONAL_CONCURRENCY: {error}");
            }
        }
        contract_intelligence::spawn_terminal_resolution_recovery_loop(
            database_url.clone(),
            timeout,
            ship_groups.clone(),
            delivery.clone(),
            ping_limiter.clone(),
            structure_resolver.clone(),
        );
        match contract_intelligence::proximity_reconciliation_interval_from_environment() {
            Ok(interval) => {
                contract_intelligence::spawn_proximity_reconciliation_loop(
                    database_url,
                    interval,
                    timeout,
                    ship_groups,
                    delivery,
                    ping_limiter,
                    structure_resolver,
                );
            }
            Err(error) => warn!(
                "proximity reconciliation disabled by invalid CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS: {error}"
            ),
        }
        if let Some(health_config) = health_config {
            contract_intelligence::spawn_health_monitor_loop(
                health_database_url,
                health_config,
                feed_health_telemetry.clone(),
                r2z2_health_enabled,
            );
        }
    }

    tokio::spawn(async move {
        if let Err(why) = client.start().await {
            error!("Discord client error: {:?}", why);
        }
    });

    // --- Initialize killmail feed ---
    let feed: Box<dyn KillmailFeed> = match app_config.killmail_feed_provider {
        FeedProvider::R2z2 => Box::new(feed::r2z2::R2z2Feed::new(&app_state.app_config)),
        FeedProvider::Redisq => {
            let queue_id = generate_queue_id();
            Box::new(feed::redisq::RedisQFeed::new(
                &queue_id,
                Duration::from_secs(app_config.redisq_connect_timeout_secs),
                Duration::from_secs(app_config.redisq_request_timeout_secs),
            ))
        }
    };

    // --- Validate pipeline config ---
    if app_config.killmail_workers < 1 {
        error!(
            "KILLMAIL_WORKERS must be >= 1 (got {})",
            app_config.killmail_workers
        );
        return;
    }
    if app_config.killmail_queue_size < 1 || app_config.killmail_queue_size > 4096 {
        error!(
            "KILLMAIL_QUEUE_SIZE must be 1..=4096 (got {})",
            app_config.killmail_queue_size
        );
        return;
    }

    info!(
        "Pipeline: workers={}, queue_size={}, post_process_sleep_ms={}",
        app_config.killmail_workers,
        app_config.killmail_queue_size,
        app_config.killmail_post_process_sleep_ms
    );

    // --- Start concurrent pipeline ---
    let (result_tx, result_rx) = mpsc::channel(app_config.killmail_queue_size);
    let semaphore = Arc::new(Semaphore::new(app_config.killmail_workers));

    info!("Listening for killmails...");

    // Spawn dispatcher task
    let dispatcher_state = app_state.clone();
    let dispatcher_http = http_client.clone();
    tokio::spawn(async move {
        pipeline::run_dispatcher(result_rx, dispatcher_state, dispatcher_http).await;
    });

    // Run producer on current task (main loop)
    pipeline::run_producer_with_health(
        feed,
        app_state,
        result_tx,
        semaphore,
        feed_health_telemetry,
    )
    .await;
}

/// Spawns the sov campaign feed's independent collection loop. Generic
/// over the sov feed's trait objects (not concrete Discord types) so tests
/// can inject fakes, mirroring
/// `contract_intelligence::spawn_contract_collection_loop_with_notifications`.
/// Never awaited on the path to starting the Discord client: this function
/// itself does not connect to Postgres, it only spawns a task that does
/// (review finding 1 on
/// `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
#[allow(clippy::too_many_arguments)]
pub fn spawn_sov_collection_loop(
    database_url: String,
    store_handle: sov_feed::SovStoreHandle,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_collection_loop(
        database_url,
        store_handle,
        interval,
        esi,
        delivery,
        directory,
        tickers,
        reachability,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_sov_collection_loop(
    database_url: String,
    store_handle: sov_feed::SovStoreHandle,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) {
    let mut consecutive_failures = 0_u32;
    loop {
        let mut delay = interval;
        // Reconnect every iteration, like the contract feed's own loop
        // (`run_contract_collection_loop_with_notifications_and_region_concurrency`):
        // this both re-applies migrations idempotently (harmless; sqlx's
        // migration lock serializes concurrent runs) and lets the loop
        // self-heal after Postgres was briefly unavailable, without any
        // separate reconnect path.
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    let store = Arc::new(store);
                    *store_handle.write().await = Some(store.clone());
                    let collector = sov_feed::SovCollector::new(
                        (*store).clone(),
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        directory.clone(),
                        tickers.clone(),
                        reachability.clone(),
                    );
                    match run_sov_cycle_isolated(async move { collector.collect_cycle().await })
                        .await
                    {
                        Ok(report) => {
                            if let Some(paused_until) = report.paused_until {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                delay = contract_intelligence::collection_retry_delay(
                                    interval,
                                    Some(paused_until),
                                    consecutive_failures,
                                );
                            } else {
                                consecutive_failures = 0;
                            }
                        }
                        Err(error) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            delay = contract_intelligence::collection_retry_delay(
                                interval,
                                None,
                                consecutive_failures,
                            );
                            warn!("sov campaign collection paused after failure: {error}");
                        }
                    }
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    delay = contract_intelligence::collection_retry_delay(
                        interval,
                        None,
                        consecutive_failures,
                    );
                    *store_handle.write().await = None;
                    warn!(
                        "sov campaign feed database unavailable; retrying without an in-memory fallback: {error}"
                    );
                }
            },
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                delay = contract_intelligence::collection_retry_delay(
                    interval,
                    None,
                    consecutive_failures,
                );
                *store_handle.write().await = None;
                warn!("sov campaign feed migrations unavailable; retrying: {error}");
            }
        }
        tokio::time::sleep(delay).await;
    }
}

/// Spawns the sov campaign feed's T-minus stage-evaluation loop (ticket
/// 03). Runs independently of [`spawn_sov_collection_loop`] at a cheaper,
/// more frequent cadence because `SovCollector::evaluate_stage_cycle`
/// makes no ESI request; it only reads persisted campaigns and
/// subscriptions against the clock. Mirrors
/// `contract_intelligence::spawn_terminal_resolution_recovery_loop`:
/// reconnects every tick (self-healing, idempotent migrations) and does
/// not populate [`sov_feed::SovStoreHandle`], since the main collection
/// loop above already owns that handle for command lookups.
#[allow(clippy::too_many_arguments)]
pub fn spawn_sov_stage_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_stage_loop(
        database_url,
        interval,
        esi,
        delivery,
        directory,
        tickers,
        reachability,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_sov_stage_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) {
    let mut cadence = tokio::time::interval_at(tokio::time::Instant::now(), interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    let collector = sov_feed::SovCollector::new(
                        store,
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        directory.clone(),
                        tickers.clone(),
                        reachability.clone(),
                    );
                    if let Err(error) =
                        run_sov_cycle_isolated(
                            async move { collector.evaluate_stage_cycle().await },
                        )
                        .await
                    {
                        warn!("sov stage evaluation cycle failed: {error}");
                    }
                }
                Err(error) => warn!("sov stage evaluation database unavailable: {error}"),
            },
            Err(error) => warn!("sov stage evaluation migrations unavailable: {error}"),
        }
    }
}

/// Runs one sov collector cycle (`collect_cycle` or
/// `evaluate_stage_cycle`) in its own task so a panic inside it -- e.g.
/// malformed persisted data reaching a panicking arithmetic operation --
/// is caught and logged rather than silently killing the long-running
/// loop that called it (review finding 1c on ticket 03: a panicking
/// `evaluate_stage_cycle` used to take down `run_sov_stage_loop`
/// permanently, with no tracing and no health signal, since that loop
/// makes no ESI call for `sov_esi_progress` to ever notice). A panic or
/// task cancellation is folded into `Err` alongside an ordinary cycle
/// failure, so callers keep their existing success/failure branching
/// unchanged; only a genuinely successful cycle returns `Ok`.
async fn run_sov_cycle_isolated<F, T, E>(future: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    match tokio::spawn(future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(join_error) if join_error.is_panic() => {
            error!("sov collector cycle panicked and was isolated: {join_error}");
            Err(format!("panicked: {join_error}"))
        }
        Err(join_error) => Err(format!("cancelled: {join_error}")),
    }
}

#[cfg(test)]
mod sov_cycle_isolation_tests {
    use super::run_sov_cycle_isolated;

    #[tokio::test]
    async fn a_panicking_cycle_future_is_caught_and_reported_as_an_error() {
        // Review finding 1c: proves the wrapper itself survives a panic
        // inside the wrapped future rather than propagating it to the
        // caller (and, by construction via `tokio::spawn`, to whatever
        // task awaits the wrapper) -- a cheap substitute for injecting a
        // panicking fake collector cycle through the full loop.
        let result: Result<(), String> = run_sov_cycle_isolated::<_, (), String>(async {
            panic!("simulated collector panic");
        })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("panicked"));
    }

    #[tokio::test]
    async fn a_successful_cycle_future_returns_its_value() {
        let result: Result<u32, String> =
            run_sov_cycle_isolated::<_, u32, String>(async { Ok(42_u32) }).await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn a_failing_cycle_future_returns_its_error_message() {
        let result: Result<(), String> =
            run_sov_cycle_isolated(async { Err::<(), _>("boom".to_string()) }).await;
        assert_eq!(result, Err("boom".to_string()));
    }
}
