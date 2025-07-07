use std::collections::HashMap;
use std::sync::Arc;
use tokio;
use tracing::{error, info, Level, warn};
use tracing_subscriber;
use serenity::prelude::*;
use rand::{distributions::Alphanumeric, Rng};

mod config;
mod discord_bot;
mod models;
mod processor;
mod redis_q;
mod esi;
mod commands;

use commands::{Command, PingCommand};
use commands::subscribe::SubscribeCommand;
use commands::unsubscribe::UnsubscribeCommand;
use commands::diag::DiagCommand;
use discord_bot::CommandMap;

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

#[tokio::main]
async fn main() {
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
    
    let systems = config::load_systems().unwrap_or_else(|e| {
        warn!("Failed to load systems.json: {}. Starting with an empty map.", e);
        HashMap::new()
    });
    let ships = config::load_ships().unwrap_or_else(|e| {
        warn!("Failed to load ships.json: {}. Starting with an empty map.", e);
        HashMap::new()
    });
    let names = config::load_names().unwrap_or_else(|e| {
        warn!("Failed to load names.json: {}. Starting with an empty map.", e);
        HashMap::new()
    });
    
    let subscriptions = config::load_all_subscriptions("config/");

    // --- Initialize application state ---
    let app_state = Arc::new(config::AppState::new(
        app_config.clone(),
        systems,
        ships,
        names,
        subscriptions,
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

    let command_map_arc = Arc::new(command_map);


    // --- Start Discord Bot ---
    let discord_token = app_config.discord_bot_token.clone();
    let intents = GatewayIntents::non_privileged() | GatewayIntents::GUILDS | GatewayIntents::GUILD_INTEGRATIONS;
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

    // --- Main killmail processing loop ---
    let queue_id = generate_queue_id();
    let listener = redis_q::RedisQListener::new(&queue_id);
    info!("Listening for killmails from RedisQ...");

    loop {
        match listener.listen().await {
            Ok(Some(zk_data)) => {
                info!("Received killmail {}", zk_data.killmail.killmail_id);
                let matched = processor::process_killmail(&app_state, &zk_data);

                if !matched.is_empty() {
                    info!(
                        "Killmail {} matched {} subscriptions",
                        zk_data.killmail.killmail_id,
                        matched.len()
                    );
                    
                    for subscription in matched {
                        if let Err(e) = discord_bot::send_killmail_message(
                            &http_client,
                            &app_state,
                            &subscription,
                            &zk_data,
                        )
                        .await
                        {
                            error!(
                                "Error sending message for subscription {}: {}",
                                subscription.id, e
                            );
                        }
                    }
                }
            }
            Ok(None) => {
                // No new data, continue loop
            }
            Err(e) => {
                error!("Error listening for killmails: {}", e);
                // Wait a bit before retrying to avoid spamming logs on persistent errors
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}