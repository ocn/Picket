use serenity::async_trait;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use serenity::builder::CreateEmbed;
use std::sync::Arc;
use serenity::http::Http;
use serenity::model::prelude::{ChannelId, Interaction};
use tracing::{info, error, warn};
use chrono::{DateTime, Utc, FixedOffset};
use crate::config::{AppState, Subscription, System, save_systems, save_ships, save_names};
use crate::esi::Celestial;
use crate::models::ZkData;
use serenity::model::guild::UnavailableGuild;
use crate::commands::Command;
use std::collections::HashMap;

pub struct CommandMap;
impl TypeMapKey for CommandMap {
    type Value = Arc<HashMap<String, Box<dyn Command>>>;
}

pub struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, data_about_bot: Ready) {
        info!("Discord bot {} is connected!", data_about_bot.user.name);

        let data = ctx.data.read().await;
        let command_map = data.get::<CommandMap>().unwrap();

        if let Err(e) = serenity::model::application::command::Command::set_global_application_commands(&ctx.http, |commands| {
            for cmd in command_map.values() {
                commands.create_application_command(|c| cmd.register(c));
            }
            commands
        }).await {
            error!("Failed to register global commands: {}", e);
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        if let Interaction::ApplicationCommand(command) = interaction {
            let data = ctx.data.read().await;
            let command_map = data.get::<CommandMap>().unwrap();
            let app_state = data.get::<crate::AppStateContainer>().unwrap();

            if let Some(cmd) = command_map.get(&command.data.name) {
                cmd.execute(&ctx, &command, app_state).await;
            }
        }
    }

    async fn guild_delete(&self, _ctx: Context, incomplete: UnavailableGuild) {
        info!("Kicked from guild: {}", incomplete.id);
        let mut subs = _ctx.data.write().await;
        let app_state = subs.get_mut::<crate::AppStateContainer>().unwrap();
        let mut subscriptions = app_state.subscriptions.write().unwrap();
        subscriptions.remove(&incomplete.id);
        if let Err(e) = crate::config::save_subscriptions_for_guild(incomplete.id, &[]) {
            error!("Failed to delete subscription file for guild {}: {}", incomplete.id, e);
        }
    }
}

// --- Dynamic Data Fetching and Caching ---

pub async fn get_system(app_state: &Arc<AppState>, system_id: u32) -> Option<System> {
    {
        let systems = app_state.systems.read().unwrap();
        if let Some(system) = systems.get(&system_id) {
            return Some(system.clone());
        }
    }
    match app_state.esi_client.get_system(system_id).await {
        Ok(system) => {
            let _lock = app_state.systems_file_lock.lock().await;
            let mut systems = app_state.systems.write().unwrap();
            systems.insert(system_id, system.clone());
            save_systems(&systems);
            Some(system)
        },
        Err(e) => { warn!("Failed to fetch system data for {}: {}", system_id, e); None }
    }
}

pub async fn get_ship_group_id(app_state: &Arc<AppState>, ship_id: u32) -> Option<u32> {
    {
        let ships = app_state.ships.read().unwrap();
        if let Some(group_id) = ships.get(&ship_id) {
            return Some(*group_id);
        }
    }
    match app_state.esi_client.get_ship_group_id(ship_id).await {
        Ok(group_id) => {
            let _lock = app_state.ships_file_lock.lock().await;
            let mut ships = app_state.ships.write().unwrap();
            ships.insert(ship_id, group_id);
            save_ships(&ships);
            Some(group_id)
        },
        Err(e) => { warn!("Failed to fetch ship group for {}: {}", ship_id, e); None }
    }
}

async fn get_name(app_state: &Arc<AppState>, id: u64) -> Option<String> {
    {
        let names = app_state.names.read().unwrap();
        if let Some(name) = names.get(&id) {
            return Some(name.clone());
        }
    }
    match app_state.esi_client.get_name(id).await {
        Ok(name) => {
            let _lock = app_state.names_file_lock.lock().await;
            let mut names = app_state.names.write().unwrap();
            names.insert(id, name.clone());
            save_names(&names);
            Some(name)
        },
        Err(e) => { warn!("Failed to fetch name for ID {}: {}", id, e); None }
    }
}

async fn get_closest_celestial(app_state: &Arc<AppState>, zk_data: &ZkData) -> Option<Arc<Celestial>> {
    let killmail = &zk_data.killmail;
    if killmail.victim.position.is_none() {
        return None;
    }
    let position = killmail.victim.position.as_ref().unwrap();
    let cache_key = killmail.solar_system_id;

    if let Some(celestial) = app_state.celestial_cache.get(&cache_key) {
        return Some(celestial);
    }

    let celestial = app_state.esi_client.get_celestial(
        killmail.solar_system_id,
        position.x,
        position.y,
        position.z,
    ).await.ok();

    if let Some(celestial) = celestial {
        let celestial_arc = Arc::new(celestial);
        app_state.celestial_cache.insert(cache_key, celestial_arc.clone()).await;
        Some(celestial_arc)
    } else {
        None
    }
}

// --- Message Sending and Embed Building ---

pub async fn send_killmail_message(
    http: &Arc<Http>,
    app_state: &Arc<AppState>,
    subscription: &Subscription,
    zk_data: &ZkData,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = ChannelId(subscription.action.channel_id.parse()?);
    let embed = build_killmail_embed(app_state, zk_data).await;

    let result = channel.send_message(http, |m| m.set_embed(embed)).await;

    if let Err(e) = result {
        if let serenity::Error::Http(http_err) = &e {
            if let serenity::http::error::Error::UnsuccessfulRequest(resp) = &**http_err {
                if resp.status_code == serenity::http::StatusCode::FORBIDDEN {
                    error!("Forbidden to send message to channel {}. Removing subscriptions.", channel);
                    return Err(Box::new(e));
                }
            }
        }
        error!("Failed to send message to channel {}: {}", channel, e);
        return Err(Box::new(e));
    }

    Ok(())
}

fn abbreviate_number(n: f64) -> String {
    if n < 1_000.0 { return format!("{:.0}", n); }
    if n < 1_000_000.0 { return format!("{:.1}K", n / 1_000.0); }
    if n < 1_000_000_000.0 { return format!("{:.1}M", n / 1_000_000.0); }
    if n < 1_000_000_000_000.0 { return format!("{:.1}B", n / 1_000_000_000.0); }
    format!("{:.1}T", n / 1_000_000_000_000.0)
}

fn get_relative_time(killmail_time: &str) -> String {
    let kill_time = DateTime::parse_from_rfc3339(killmail_time).unwrap_or_else(|_| Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()));
    let now = Utc::now();
    let diff = now.signed_duration_since(kill_time);

    if diff.num_weeks() > 52 { return format!("{} years ago", diff.num_weeks() / 52); }
    if diff.num_weeks() > 1 { return format!("{} weeks ago", diff.num_weeks()); }
    if diff.num_days() > 1 { return format!("{} days ago", diff.num_days()); }
    if diff.num_hours() > 1 { return format!("{} hours ago", diff.num_hours()); }
    if diff.num_minutes() > 1 { return format!("{} minutes ago", diff.num_minutes()); }
    format!("{} seconds ago", diff.num_seconds())
}

fn str_alliance_icon(id: u64) -> String { format!("https://images.evetech.net/alliances/{}/logo?size=64", id) }
fn str_corp_icon(id: u64) -> String { format!("https://images.evetech.net/corporations/{}/logo?size=64", id) }
fn str_ship_render(id: u32) -> String { format!("https://images.evetech.net/types/{}/render?size=128", id) }
fn str_ship_icon(id: u32) -> String { format!("https://images.evetech.net/types/{}/icon?size=64", id) }

async fn build_killmail_embed(app_state: &Arc<AppState>, zk_data: &ZkData) -> CreateEmbed {
    let mut embed = CreateEmbed::default();
    let killmail = &zk_data.killmail;
    
    let system_info = get_system(app_state, killmail.solar_system_id).await;
    let system_name = system_info.as_ref().map_or("Unknown System", |s| &s.name);
    let region_name = system_info.as_ref().map_or("Unknown Region", |s| &s.region);

    let total_value_str = abbreviate_number(zk_data.zkb.total_value);

    let victim_ship_name = get_name(app_state, killmail.victim.ship_type_id as u64).await.unwrap_or_else(|| "Unknown Ship".to_string());
    let victim_corp_id = killmail.victim.corporation_id.unwrap_or(0);
    let victim_char_id = killmail.victim.character_id.unwrap_or(0);
    let victim_name = get_name(app_state, victim_char_id).await.or_else(|| futures::executor::block_on(get_name(app_state, victim_corp_id))).unwrap_or_else(|| "N/A".to_string());
    let victim_details = format!("[{}]({})", victim_name, format!("https://zkillboard.com/character/{}/", victim_char_id));

    let final_blow_attacker = killmail.attackers.iter().find(|a| a.final_blow).or_else(|| killmail.attackers.first());
    let mut attacker_details = "N/A".to_string();
    let mut author_icon_url = str_ship_icon(killmail.victim.ship_type_id);
    if let Some(attacker) = final_blow_attacker {
        let attacker_char_id = attacker.character_id.unwrap_or(0);
        let attacker_corp_id = attacker.corporation_id.unwrap_or(0);
        let attacker_alliance_id = attacker.alliance_id.unwrap_or(0);
        let attacker_name = get_name(app_state, attacker_char_id).await.or_else(|| futures::executor::block_on(get_name(app_state, attacker_corp_id))).unwrap_or_else(|| "N/A".to_string());
        let attacker_ship_name = if let Some(id) = attacker.ship_type_id { get_name(app_state, id as u64).await.unwrap_or_else(|| "Unknown Ship".to_string()) } else { "Unknown Ship".to_string() };
        attacker_details = format!("{} ({})", attacker_name, attacker_ship_name);
        if attacker_alliance_id != 0 {
            author_icon_url = str_alliance_icon(attacker_alliance_id);
        } else if attacker_corp_id != 0 {
            author_icon_url = str_corp_icon(attacker_corp_id);
        }
    }

    let mut location_details = String::new();
    if let Some(celestial) = get_closest_celestial(app_state, zk_data).await {
        let distance_km = celestial.distance / 1000.0;
        location_details = format!("~{:.1}km from {}", distance_km, celestial.item_name);
    }

    embed.author(|a| a
        .name(format!("{} | {} killed in {}", victim_name, victim_ship_name, system_name))
        .url(format!("https://zkillboard.com/kill/{}/", killmail.killmail_id))
        .icon_url(author_icon_url)
    );

    embed.thumbnail(str_ship_render(killmail.victim.ship_type_id));
    embed.color(0xdd2e2e);

    embed.field("Victim", victim_details, true);
    embed.field("Final Blow", attacker_details, true);
    embed.field("System", format!("{} / {}", system_name, region_name), true);

    embed.field("Value", total_value_str, true);
    embed.field("Attackers", killmail.attackers.len().to_string(), true);
    embed.field("Location", location_details, true);

    embed.footer(|f| f.text(format!("Kill ID: {} • {}", killmail.killmail_id, get_relative_time(&killmail.killmail_time))));
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(&killmail.killmail_time) {
        embed.timestamp(timestamp.to_rfc3339());
    }

    embed
}
