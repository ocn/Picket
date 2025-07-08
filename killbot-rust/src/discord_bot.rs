use crate::commands::Command;
use crate::config::{save_names, save_ships, save_systems, AppState, Subscription, System};
use crate::esi::Celestial;
use crate::models::{Attacker, ZkData};
use crate::processor::{Color, FilterResult};
use chrono::{DateTime, FixedOffset, Utc};
use futures::future::join_all;
use serenity::async_trait;
use serenity::builder::CreateEmbed;
use serenity::http::Http;
use serenity::model::gateway::Ready;
use serenity::model::guild::UnavailableGuild;
use serenity::model::prelude::{ChannelId, Interaction};
use serenity::prelude::*;
use serenity::utils::Colour;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct CommandMap;
impl TypeMapKey for CommandMap {
    type Value = Arc<HashMap<String, Box<dyn Command>>>;
}

pub struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn guild_delete(&self, _ctx: Context, incomplete: UnavailableGuild) {
        info!("Kicked from guild: {}", incomplete.id);
        let mut subs = _ctx.data.write().await;
        let app_state = subs.get_mut::<crate::AppStateContainer>().unwrap();
        let mut subscriptions = app_state.subscriptions.write().unwrap();
        subscriptions.remove(&incomplete.id);
        if let Err(e) = crate::config::save_subscriptions_for_guild(incomplete.id, &[]) {
            error!(
                "Failed to delete subscription file for guild {}: {}",
                incomplete.id, e
            );
        }
    }

    async fn ready(&self, ctx: Context, data_about_bot: Ready) {
        info!("Discord bot {} is connected!", data_about_bot.user.name);

        let data = ctx.data.read().await;
        let command_map = data.get::<CommandMap>().unwrap();

        if let Err(e) =
            serenity::model::application::command::Command::set_global_application_commands(
                &ctx.http,
                |commands| {
                    for cmd in command_map.values() {
                        commands.create_application_command(|c| cmd.register(c));
                    }
                    commands
                },
            )
            .await
        {
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
        }
        Err(e) => {
            warn!("Failed to fetch system data for {}: {}", system_id, e);
            None
        }
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
        }
        Err(e) => {
            warn!("Failed to fetch ship group for {}: {}", ship_id, e);
            None
        }
    }
}

pub async fn get_name(app_state: &Arc<AppState>, id: u64) -> Option<String> {
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
        }
        Err(e) => {
            warn!("Failed to fetch name for ID {}: {}", id, e);
            None
        }
    }
}

async fn get_closest_celestial(
    app_state: &Arc<AppState>,
    zk_data: &ZkData,
) -> Option<Arc<Celestial>> {
    let killmail = &zk_data.killmail;
    killmail.victim.position.as_ref()?;
    let position = killmail.victim.position.as_ref().unwrap();
    let cache_key = killmail.solar_system_id;

    if let Some(celestial) = app_state.celestial_cache.get(&cache_key) {
        return Some(celestial);
    }

    let celestial = app_state
        .esi_client
        .get_celestial(killmail.solar_system_id, position.x, position.y, position.z)
        .await
        .ok();

    if let Some(celestial) = celestial {
        let celestial_arc = Arc::new(celestial);
        app_state
            .celestial_cache
            .insert(cache_key, celestial_arc.clone())
            .await;
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
    filter_result: FilterResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = ChannelId(subscription.action.channel_id.parse()?);
    let embed = build_killmail_embed(app_state, zk_data, filter_result).await; // Pass it here

    let result = channel.send_message(http, |m| m.set_embed(embed)).await;

    if let Err(e) = result {
        if let serenity::Error::Http(http_err) = &e {
            if let serenity::http::error::Error::UnsuccessfulRequest(resp) = &**http_err {
                if resp.status_code == serenity::http::StatusCode::FORBIDDEN {
                    error!(
                        "Forbidden to send message to channel {}. Removing subscriptions.",
                        channel
                    );
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
    if n < 1_000.0 {
        return format!("{:.0}", n);
    }
    if n < 1_000_000.0 {
        return format!("{:.1}K", n / 1_000.0);
    }
    if n < 1_000_000_000.0 {
        return format!("{:.1}M", n / 1_000_000.0);
    }
    if n < 1_000_000_000_000.0 {
        return format!("{:.1}B", n / 1_000_000_000.0);
    }
    format!("{:.1}T", n / 1_000_000_000_000.0)
}

fn get_relative_time(killmail_time: &str) -> String {
    let kill_time = DateTime::parse_from_rfc3339(killmail_time)
        .unwrap_or_else(|_| Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()));
    let now = Utc::now();
    let diff = now.signed_duration_since(kill_time);

    if diff.num_weeks() > 52 {
        return format!("{} years ago", diff.num_weeks() / 52);
    }
    if diff.num_weeks() > 1 {
        return format!("{} weeks ago", diff.num_weeks());
    }
    if diff.num_days() > 1 {
        return format!("{} days ago", diff.num_days());
    }
    if diff.num_hours() > 1 {
        return format!("{} hours ago", diff.num_hours());
    }
    if diff.num_minutes() > 1 {
        return format!("{} minutes ago", diff.num_minutes());
    }
    format!("{} seconds ago", diff.num_seconds())
}

fn str_alliance_icon(id: u64) -> String {
    format!("https://images.evetech.net/alliances/{}/logo?size=64", id)
}
fn str_corp_icon(id: u64) -> String {
    format!(
        "https://images.evetech.net/corporations/{}/logo?size=64",
        id
    )
}
fn str_ship_render(id: u32) -> String {
    format!("https://images.evetech.net/types/{}/render?size=128", id)
}
fn str_ship_icon(id: u32) -> String {
    format!("https://images.evetech.net/types/{}/icon?size=64", id)
}
fn str_pilot_zk(id: u64) -> String {
    format!("https://zkillboard.com/character/{}/", id)
}
fn str_corp_zk(id: u64) -> String {
    format!("https://zkillboard.com/corporation/{}/", id)
}
fn str_alliance_zk(id: u64) -> String {
    format!("https://zkillboard.com/alliance/{}/", id)
}
fn str_system_dotlan(id: u32) -> String {
    format!("http://evemaps.dotlan.net/system/{}", id)
}
fn str_region_dotlan(id: u32) -> String {
    format!("http://evemaps.dotlan.net/region/{}", id)
}
fn str_location(id: u64) -> String {
    format!("https://zkillboard.com/location/{}/", id)
}

// Returns: ID, count
fn most_common_ship_type(attackers: &[Attacker]) -> Option<(u64, u64)> {
    attackers
        .iter()
        .fold(HashMap::new(), |mut map, val| {
            if val.ship_type_id.is_none() {
                return map;
            }
            map.entry(val.ship_type_id.unwrap() as u64)
                .and_modify(|freq| *freq += 1)
                .or_insert(1u64);
            map
        })
        .iter()
        .max_by(|a, b| a.1.cmp(b.1))
        .map(|(k, v)| (*k, *v))
}

/// Formats a DateTime object into a YYYYMMDDHH00 string, suitable for battle report URLs.
/// This function effectively rounds the time down to the nearest hour.
fn format_datetime_to_timestamp(date: &DateTime<FixedOffset>) -> String {
    // Convert to UTC to ensure consistency, similar to getUTCFullYear, etc.
    let date_utc = date.with_timezone(&Utc);
    // Format the date and append "00" for the minutes.
    format!("{}00", date_utc.format("%Y%m%d%H"))
}

async fn build_killmail_embed(
    app_state: &Arc<AppState>,
    zk_data: &ZkData,
    filter_result: FilterResult,
) -> CreateEmbed {
    let mut embed = CreateEmbed::default();
    let killmail = &zk_data.killmail;

    let system_info = get_system(app_state, killmail.solar_system_id).await;
    let system_name = system_info.as_ref().map_or("Unknown System", |s| &s.name);
    let system_id = system_info.as_ref().map_or(0, |s| s.id);
    let region_name = system_info.as_ref().map_or("Unknown Region", |s| &s.region);
    let region_id = system_info.as_ref().map_or(0, |s| s.region_id);

    let total_value_str = abbreviate_number(zk_data.zkb.total_value);
    let relative_time = get_relative_time(&killmail.killmail_time);
    let killmail_time = DateTime::parse_from_rfc3339(&killmail.killmail_time)
        .unwrap_or_else(|_| Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()));

    let related_br = format!(
        "https://br.evetools.org/related/${}/{}",
        region_id,
        format_datetime_to_timestamp(&killmail_time)
    );

    // Location details
    let mut location_details = String::new();
    if let Some(celestial) = get_closest_celestial(app_state, zk_data).await {
        let distance_km = celestial.distance / 1000.0;
        let distance_str = if distance_km > 1500.0 {
            format!("{:.1} AU", distance_km / 149_597_870.7)
        } else {
            format!("{:.1} km", distance_km)
        };
        location_details = format!(
            "on: [{}]({}), {} away",
            celestial.item_name,
            str_location(celestial.item_id),
            distance_str,
        );
    }

    // Victim details
    let victim_ship_name = get_name(app_state, killmail.victim.ship_type_id as u64)
        .await
        .unwrap_or_else(|| "Unknown Ship".to_string());
    let victim_corp_id = killmail.victim.corporation_id.unwrap_or(0);
    // let victim_char_id = killmail.victim.character_id.unwrap_or(0);
    // let victim_name = get_name(app_state, victim_char_id)
    //     .await
    //     .or_else(|| futures::executor::block_on(get_name(app_state, victim_corp_id)))
    //     .unwrap_or_else(|| "N/A".to_string());
    let mut victim_details = String::new();
    if let Some(alliance_id) = killmail.victim.alliance_id {
        if let Some(name) = get_name(app_state, alliance_id).await {
            victim_details.push_str(&format!(
                "[{}]({})",
                &name[0..name.len().min(40)],
                str_alliance_zk(alliance_id)
            ));
        }
    }
    if victim_corp_id != 0 {
        if !victim_details.is_empty() {
            victim_details.push_str(" / ");
        }
        if let Some(name) = get_name(app_state, victim_corp_id).await {
            victim_details.push_str(&format!(
                "[{}]({})",
                &name[0..name.len().min(40)],
                str_corp_zk(victim_corp_id)
            ));
        }
    }

    // Title + Author Text
    let most_common_ship = most_common_ship_type(&killmail.attackers);
    let title;
    let mut author_text;
    if filter_result.min_pilots.is_some() {
        author_text = format!(
            "{}+ ships in {} ({})",
            killmail.attackers.len(),
            system_name,
            region_name
        );
        title = if let Some((type_id, count)) = &most_common_ship {
            let ship_name = get_name(app_state, *type_id)
                .await
                .unwrap_or_else(|| "Unknown Ship".to_string());
            format!("`{}` died to {}x `{}`", victim_ship_name, count, ship_name)
        } else {
            "Missing 0".to_string()
        };
    } else if let Some(ref matched_ship) = filter_result.matched_ship {
        author_text = format!(
            "{} in {} ({})",
            matched_ship.ship_name, system_name, region_name
        );
        if let Some((type_id, count)) = most_common_ship {
            match filter_result.color.unwrap_or_default() {
                Color::Green => {
                    title = format!("`{}` destroyed", victim_ship_name);
                }
                Color::Red => {
                    let ship_name = get_name(app_state, type_id)
                        .await
                        .unwrap_or_else(|| "Unknown Ship".to_string());
                    title = format!("`{}` died to {}x `{}`", victim_ship_name, count, ship_name)
                }
            }
        } else {
            title = relative_time.clone();
        }
    } else {
        title = "Placeholder".to_string();
        author_text = "".to_string();
    }
    author_text += &format!("\n{}", relative_time);

    // console.log('attackerparams.data');

    // Find the attacker who made the final blow
    let last_hit_attacker: Option<&Attacker> = killmail.attackers.iter().find(|a| a.final_blow);

    // Determine the primary icon to render for the embed thumbnail and the affiliation icon for the author field.
    // The logic follows a specific priority:
    // 1. Matched ship from the filter
    // 2. Victim's ship
    // 3. Final blow attacker's ship
    // 4. Final blow attacker's weapon
    let (id_of_icon_to_render, affiliation_icon_url_to_render) = {
        if let Some(ref matched_ship) = &filter_result.matched_ship {
            let type_id = matched_ship.type_id;
            let url = if let Some(alliance_id) = matched_ship.alliance_id {
                str_alliance_icon(alliance_id)
            } else if let Some(corp_id) = matched_ship.corp_id {
                str_corp_icon(corp_id)
            } else {
                str_ship_render(type_id)
            };
            (type_id, url)
        } else if killmail.victim.ship_type_id != 0 {
            let url = if let Some(alliance_id) = killmail.victim.alliance_id {
                str_alliance_icon(alliance_id)
            } else if let Some(corporation_id) = killmail.victim.corporation_id {
                str_corp_icon(corporation_id)
            } else {
                str_ship_render(killmail.victim.ship_type_id)
            };
            (killmail.victim.ship_type_id, url)
        } else if let Some(attacker) = last_hit_attacker {
            if let Some(ship_type_id) = attacker.ship_type_id {
                let url = if let Some(alliance_id) = attacker.alliance_id {
                    str_alliance_icon(alliance_id)
                } else if let Some(corporation_id) = attacker.corporation_id {
                    str_corp_icon(corporation_id)
                } else {
                    str_ship_render(ship_type_id)
                };
                (ship_type_id, url)
            } else if let Some(weapon_type_id) = attacker.weapon_type_id {
                let url = if let Some(alliance_id) = attacker.alliance_id {
                    str_alliance_icon(alliance_id)
                } else if let Some(corporation_id) = attacker.corporation_id {
                    str_corp_icon(corporation_id)
                } else {
                    str_ship_render(weapon_type_id)
                };
                (weapon_type_id, url)
            } else {
                (0, String::new()) // Fallback
            }
        } else {
            (0, String::new()) // Fallback
        }
    };
    if id_of_icon_to_render == 0 {
        warn!(
            "Could not determine an icon to render for killmail {}",
            zk_data.kill_id
        );
    }
    info!("Rendering icon: {}", str_ship_render(id_of_icon_to_render));

    // --- Attacker Affiliation List ---

    // Concurrently, fetch names for all unique alliance and corporation IDs involved.
    let mut ids_to_fetch: Vec<u64> = killmail
        .attackers
        .iter()
        .filter_map(|a| a.alliance_id.or(a.corporation_id))
        .collect();
    ids_to_fetch.sort_unstable();
    ids_to_fetch.dedup();

    let name_futures = ids_to_fetch.iter().map(|id| get_name(app_state, *id));
    let name_results = join_all(name_futures).await;

    let id_to_name: HashMap<u64, String> = ids_to_fetch
        .into_iter()
        .zip(name_results)
        .filter_map(|(id, name_opt)| name_opt.map(|name| (id, name)))
        .collect();

    // Count attackers by their affiliation name.
    let mut alliance_counts: HashMap<String, u32> = HashMap::new();
    for attacker in &killmail.attackers {
        if let Some(id) = attacker.alliance_id.or(attacker.corporation_id) {
            let name = id_to_name
                .get(&id)
                .cloned()
                .unwrap_or_else(|| "Unknown".to_string());
            *alliance_counts.entry(name).or_insert(0) += 1;
        }
    }

    // Sort affiliations by the number of attackers.
    let mut sorted_attackers: Vec<(String, u32)> = alliance_counts.into_iter().collect();
    sorted_attackers.sort_by(|a, b| b.1.cmp(&a.1));

    // --- Format Attacker List for Embed ---

    let mut attacker_alliances = "```".to_string();
    let mut displayed_entries: Vec<(String, u32)> = Vec::new();
    let mut others_count = 0;
    const DISPLAY_THRESHOLD: u32 = 15;

    // Separate entries into "displayed" and "others".
    for (i, (name, count)) in sorted_attackers.into_iter().enumerate() {
        if count >= DISPLAY_THRESHOLD || i == 0 {
            displayed_entries.push((name, count));
        } else {
            others_count += count;
        }
    }

    // Calculate padding for alignment.
    let mut max_name_length = 0;
    const OTHERS_STR: &str = "...others";
    for (name, _) in &displayed_entries {
        max_name_length = max_name_length.max(name.len());
    }
    if others_count > 0 {
        max_name_length = max_name_length.max(OTHERS_STR.len());
    }
    max_name_length = max_name_length.min(26);
    const PADDING: usize = 3;

    // Build the formatted string.
    for (key, value) in displayed_entries {
        let spaces = " ".repeat(max_name_length - key.len().min(26) + PADDING);
        let formatted_key = if key.len() > 26 {
            format!("{}-\n{}", &key[..26], &key[26..])
        } else {
            key
        };
        attacker_alliances.push_str(&format!("{}{}{}{}\n", formatted_key, spaces, "x", value));
    }

    if others_count > 0 {
        let spaces = " ".repeat(max_name_length - OTHERS_STR.len() + PADDING);
        attacker_alliances.push_str(&format!(
            "{}{}{}{}\n",
            OTHERS_STR, spaces, "x", others_count
        ));
    }

    attacker_alliances.push('`');

    // console.log('attackerparams.dataDone');

    let affiliation = format!(
        "{}[killed]({}): {}\nin: [{}]({}) ([{}]({}))\n{}",
        attacker_alliances,
        related_br,
        victim_details,
        system_name,
        str_system_dotlan(system_id),
        region_name,
        str_region_dotlan(region_id),
        location_details,
    );

    // Build the embed
    embed.title(title);
    embed.author(|a| {
        a.name(author_text)
            .url(format!(
                "https://zkillboard.com/kill/{}/",
                killmail.killmail_id
            ))
            .icon_url(affiliation_icon_url_to_render)
    });
    embed.thumbnail(str_ship_render(id_of_icon_to_render));
    embed.color(match filter_result.color.unwrap_or_default() {
        Color::Green => Colour::DARK_GREEN,
        Color::Red => Colour::RED,
    });
    embed.field(
        format!("({}) Attackers Involved", killmail.attackers.len()),
        affiliation,
        false,
    );
    embed.footer(|f| {
        f.text(format!(
            "Value: {} • EVETime: {}",
            total_value_str, relative_time,
        ))
    });
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(&killmail.killmail_time) {
        embed.timestamp(timestamp.to_rfc3339());
    }

    embed
}
