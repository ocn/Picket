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
pub mod feed;
pub mod location_evidence;
pub mod models;
pub mod pipeline;
pub mod processor;

use crate::commands::contract_subscribe::ContractSubscribeCommand;
use crate::commands::contract_unsubscribe::ContractUnsubscribeCommand;
use crate::commands::find_unsubscribed::FindUnsubscribedChannelsCommand;
use crate::commands::health::HealthCommand;
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

pub struct AppStateContainer;

impl TypeMapKey for AppStateContainer {
    type Value = Arc<config::AppState>;
}

pub struct ContractStoreContainer;

impl TypeMapKey for ContractStoreContainer {
    type Value = contract_intelligence::ContractStoreHandle;
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
    }

    let http_client = client.cache_and_http.http.clone();

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
        let health_config = match contract_intelligence::HealthRuntimeConfig::from_environment() {
            Ok(config) => Some(config),
            Err(error) => {
                warn!("health monitoring disabled by invalid configuration: {error}");
                None
            }
        };
        let health_database_url = database_url.clone();
        match contract_intelligence::contract_regional_concurrency_from_environment() {
            Ok(max_concurrent_regions) => {
                contract_intelligence::spawn_contract_collection_loop_with_notifications_and_region_concurrency(
                    database_url,
                    store_handle,
                    Duration::from_secs(interval),
                    timeout,
                    ship_groups,
                    delivery,
                    Arc::new(contract_intelligence::AppStateContractPingLimiter::new(
                        app_state.clone(),
                    )),
                    max_concurrent_regions,
                );
            }
            Err(error) => {
                warn!("contract collection disabled by invalid CONTRACT_REGIONAL_CONCURRENCY: {error}");
            }
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
