use rand::{distributions::Alphanumeric, Rng};
use serenity::http::Http;
use serenity::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn, Level};

pub mod commands;
pub mod config;
pub mod discord_bot;
pub mod esi;
pub mod feed;
pub mod models;
pub mod processor;

use crate::commands::find_unsubscribed::FindUnsubscribedChannelsCommand;
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
use models::ZkDataNoEsi;

pub struct AppStateContainer;

impl TypeMapKey for AppStateContainer {
    type Value = Arc<config::AppState>;
}

fn generate_queue_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

async fn process_single_killmail(
    kill_id: i64,
    zk_data_no_esi: ZkDataNoEsi,
    app_state: &Arc<config::AppState>,
    http_client: &Arc<Http>,
) {
    // Load ESI data containing killmail information
    let zk_data = match app_state
        .esi_client
        .load_killmail(zk_data_no_esi.zkb.esi.clone())
        .await
    {
        Ok(killmail) => models::ZkData {
            kill_id: zk_data_no_esi.kill_id,
            killmail,
            zkb: zk_data_no_esi.zkb,
        },
        Err(e) => {
            error!("Error loading killmail data from ESI: {}", e);
            return;
        }
    };

    let matched = processor::process_killmail(app_state, &zk_data).await;

    if !matched.is_empty() {
        for (guild_id, subscription, filter_result) in matched {
            info!(
                "[Kill: {}] Matched subscription '{}' for channel {}, this filter was a match: {}",
                kill_id, subscription.description, subscription.action.channel_id, filter_result.name
            );
            if let Err(e) = discord_bot::send_killmail_message(
                http_client,
                app_state,
                &subscription,
                &zk_data,
                filter_result,
            )
            .await
            {
                match e {
                    discord_bot::KillmailSendError::CleanupChannel(e) => {
                        warn!(
                            "Cleaning up subscriptions for channel {} due to error: {:#?}",
                            subscription.action.channel_id, e
                        );
                        let _lock = app_state.subscriptions_file_lock.lock().await;
                        let mut subs_map = app_state.subscriptions.write().unwrap();

                        if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
                            guild_subs.retain(|s| {
                                s.action.channel_id != subscription.action.channel_id
                            });
                            if let Err(save_err) = config::save_subscriptions_for_guild(
                                guild_id, guild_subs,
                            ) {
                                error!("Failed to save subscriptions after cleanup for guild {}: {}", guild_id, save_err);
                            }
                        }
                    }
                    discord_bot::KillmailSendError::Other(err) => {
                        error!(
                            "Error sending message for subscription {}: {}",
                            subscription.id, err
                        );
                    }
                }
            }
        }
    }
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
    info!("Killmail process timeout: {}s", app_config.killmail_process_timeout_secs);
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

    let sync_standings_command = Box::new(SyncStandingsCommand);
    command_map.insert(sync_standings_command.name(), sync_standings_command);

    let sync_remove_command = Box::new(SyncRemoveCommand);
    command_map.insert(sync_remove_command.name(), sync_remove_command);

    let sync_clear_command = Box::new(SyncClearCommand);
    command_map.insert(sync_clear_command.name(), sync_clear_command);

    let find_unsubscribed_command = Box::new(FindUnsubscribedChannelsCommand);
    command_map.insert(find_unsubscribed_command.name(), find_unsubscribed_command);

    let command_map_arc = Arc::new(command_map);

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
    }

    let http_client = client.cache_and_http.http.clone();

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

    info!("Listening for killmails...");

    // --- Main killmail processing loop ---
    loop {
        match feed.next().await {
            Ok(Some(zk_data_no_esi)) => {
                let kill_id = zk_data_no_esi.kill_id;
                info!("[Kill: {}] Received", kill_id);

                let timeout_secs = app_state.app_config.killmail_process_timeout_secs;
                match tokio::time::timeout(
                    Duration::from_secs(timeout_secs),
                    process_single_killmail(kill_id, zk_data_no_esi, &app_state, &http_client),
                )
                .await
                {
                    Ok(()) => {}
                    Err(_) => error!(
                        "[Kill: {}] Processing timed out after {}s, skipping",
                        kill_id, timeout_secs
                    ),
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(None) => {
                // Feed already handled its own wait/backoff — no extra sleep
            }
            Err(e) => {
                error!("Feed error: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
