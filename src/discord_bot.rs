use crate::commands::sync_standings::SyncStandingsCommand;
use crate::commands::Command;
use crate::config::{
    save_names, save_ships, save_systems, save_user_standings, AppState, Filter, FilterNode,
    PingType, SimpleFilter, StandingSource, Subscription, System,
};
use crate::esi::Celestial;
use crate::models::{Attacker, ZkData};
use crate::processor::{AttackerKey, Color, NamedFilterResult};
use chrono::{DateTime, FixedOffset, Utc};
use serenity::async_trait;
use serenity::builder::CreateEmbed;
use serenity::http::error::Error;
use serenity::http::Http;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::model::guild::UnavailableGuild;
use serenity::model::prelude::{ChannelId, Interaction};
use serenity::prelude::*;
use serenity::utils::Colour;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, trace, warn};

const SHIP_GROUP_PRIORITY: &[u32] = &[
    30,   // Titan
    659,  // Supercarrier
    4594, // Lancer
    485,  // Dreadnought
    1538, // FAX
    547,  // Carrier
    883,  // Capital Industrial Ship
    902,  // Jump Freighter
    513,  // Freighter
];

/// Ship group ID to display name mapping (group_id, singular, plural)
/// Priority order: capitals first, then subcaps by importance
const GROUP_NAMES: &[(u32, &str, &str)] = &[
    // Capitals
    (30, "Titan", "Titans"),
    (659, "Super", "Supers"),
    (4594, "Lancer", "Lancers"),
    (485, "Dread", "Dreads"),
    (1538, "FAX", "FAX"),
    (547, "Carrier", "Carriers"),
    (883, "Cap Indy", "Cap Indys"),
    (902, "JF", "JFs"),
    (513, "Freighter", "Freighters"),
    // Battleships
    (898, "Blops", "Blops"),
    (900, "Marauder", "Marauders"),
    (27, "BS", "BS"),
    // Battlecruisers
    (419, "BC", "BCs"),
    (540, "CS", "CS"),
    (1201, "ABC", "ABCs"),
    // Cruisers
    (963, "T3C", "T3Cs"),
    (894, "HIC", "HICs"),
    (832, "Logi", "Logi"),
    (358, "HAC", "HACs"),
    (906, "C Recon", "C Recons"),
    (833, "F Recon", "F Recons"),
    (1972, "Flag", "Flags"),
    (26, "Cruiser", "Cruisers"),
    // Destroyers
    (541, "Dictor", "Dictors"),
    (1305, "T3D", "T3Ds"),
    (1534, "Cmd Dessie", "Cmd Dessies"),
    (420, "Destroyer", "Destroyers"),
    // Frigates
    (834, "Bomber", "Bombers"),
    (324, "AF", "AFs"),
    (831, "Ceptor", "Ceptors"),
    (830, "CovOps", "CovOps"),
    (1527, "Logi Frig", "Logi Frigs"),
    (893, "EAS", "EAS"),
    (25, "Frigate", "Frigates"),
    // Misc
    (28, "T1 Indy", "T1 Indys"),
    (380, "T2 Indy", "T2 Indys"),
    (1283, "Mining Barge", "Mining Barges"),
    (463, "Mining Frig", "Mining Frigs"),
    (29, "Pod", "Pods"),
];

/// Sentinel value for unknown ship groups (counted in +N)
const GROUP_UNKNOWN: u32 = 0;

/// Get the display name for a ship group, using singular or plural form based on count
fn get_group_name(group_id: u32, count: u32) -> Option<&'static str> {
    GROUP_NAMES
        .iter()
        .find(|(id, _, _)| *id == group_id)
        .map(|(_, singular, plural)| {
            if count == 1 {
                *singular
            } else {
                *plural
            }
        })
}

/// Check if a group ID has a known display name
fn is_known_group(group_id: u32) -> bool {
    GROUP_NAMES.iter().any(|(id, _, _)| *id == group_id)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MatchedEntity {
    pub ship_name: String,
    pub type_id: u32,
    pub group_id: u32,
    pub corp_id: Option<u64>,
    pub alliance_id: Option<u64>,
    pub color: Color,
}

#[derive(Debug)]
pub enum KillmailSendError {
    CleanupChannel(serenity::Error),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for KillmailSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KillmailSendError::CleanupChannel(e) => write!(f, "Channel cleanup required: {}", e),
            KillmailSendError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for KillmailSendError {}

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
        let _lock = app_state.subscriptions_file_lock.lock().await;
        let mut subscriptions = app_state.subscriptions.write().unwrap();
        subscriptions.remove(&incomplete.id);
        if let Err(e) = crate::config::save_subscriptions_for_guild(incomplete.id, &[]) {
            error!(
                "Failed to delete subscription file for guild {}: {}",
                incomplete.id, e
            );
        }
    }

    async fn message(&self, ctx: Context, msg: Message) {
        // Ignore messages from bots and messages that are not in DMs
        if msg.author.bot || msg.guild_id.is_some() {
            return;
        }

        // Check if the message content looks like an SSO callback URL
        if msg.content.starts_with("https://github.headempty.space/") && msg.content.len() < 2000 {
            let url = match url::Url::parse(&msg.content) {
                Ok(url) => url,
                Err(_) => {
                    // Inform the user that the URL is invalid
                    if let Err(why) = msg.channel_id.say(&ctx.http, "Invalid URL format.").await {
                        error!("Error sending message: {:?}", why);
                    }
                    return;
                }
            };

            let query_params: HashMap<String, String> = url.query_pairs().into_owned().collect();
            let code = query_params.get("code");
            let state = query_params.get("state");

            if let (Some(code), Some(state)) = (code, state) {
                let data = ctx.data.read().await;
                let app_state = data.get::<crate::AppStateContainer>().unwrap();

                let sso_state = {
                    let mut sso_states = app_state.sso_states.lock().await;
                    sso_states.remove(state)
                };

                if let Some(sso_state) = sso_state {
                    let client_id = app_state.app_config.eve_client_id.clone();
                    let client_secret = app_state.app_config.eve_client_secret.clone();

                    // 1. Exchange code for token
                    match app_state
                        .esi_client
                        .exchange_code_for_token(code, &client_id, &client_secret)
                        .await
                    {
                        Ok(token) => {
                            let mut full_token = token.clone();
                            let _ = msg
                                .channel_id
                                .say(
                                    &ctx.http,
                                    format!(
                                        "Successfully authenticated as {}. Fetching contacts...",
                                        token.character_name
                                    ),
                                )
                                .await;

                            // 2. Fetch affiliations and contacts
                            let (corp_id, alliance_id) = match app_state
                                .esi_client
                                .get_character_affiliation(token.character_id)
                                .await
                            {
                                Ok(affiliation) => affiliation,
                                Err(e) => {
                                    error!("Failed to get character affiliation: {}", e);
                                    let _ = msg
                                        .channel_id
                                        .say(
                                            &ctx.http,
                                            "Failed to fetch character affiliation. Aborting.",
                                        )
                                        .await;
                                    return;
                                }
                            };

                            let source_entity_id = match sso_state.standing_source {
                                StandingSource::Character => token.character_id,
                                StandingSource::Corporation => corp_id,
                                StandingSource::Alliance => alliance_id.unwrap_or(corp_id), // Fallback to corp if no alliance
                            };

                            let contacts = app_state
                                .esi_client
                                .get_contacts(
                                    source_entity_id,
                                    &token.access_token,
                                    match sso_state.standing_source {
                                        StandingSource::Character => "characters",
                                        StandingSource::Corporation => "corporations",
                                        StandingSource::Alliance => "alliances",
                                    },
                                )
                                .await
                                .unwrap_or_default();

                            // 3. Save token and contacts
                            {
                                let _lock = app_state.user_standings_file_lock.lock().await;
                                let mut standings_map = app_state.user_standings.write().unwrap();
                                let user_standings =
                                    standings_map.entry(sso_state.discord_user_id).or_default();

                                // Add the affiliation to the token before saving
                                full_token.corporation_id = corp_id;
                                full_token.alliance_id = alliance_id;

                                user_standings
                                    .tokens
                                    .retain(|t| t.character_id != full_token.character_id);
                                user_standings.tokens.push(full_token);

                                user_standings
                                    .contact_lists
                                    .contacts
                                    .insert(source_entity_id, contacts);
                                save_user_standings(&standings_map);
                            }

                            // 4. Update subscription
                            let guild_id = sso_state.original_interaction.guild_id.unwrap();
                            let mut subscription_updated = false;
                            {
                                let _lock = app_state.subscriptions_file_lock.lock().await;
                                let mut subs_map = app_state.subscriptions.write().unwrap();
                                if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
                                    let original_channel_id =
                                        sso_state.original_interaction.channel_id.to_string();
                                    if let Some(sub) = guild_subs.iter_mut().find(|s| {
                                        s.id == sso_state.subscription_id
                                            && s.action.channel_id == original_channel_id
                                    }) {
                                        let new_filter = FilterNode::Condition(Filter::Simple(
                                            SimpleFilter::IgnoreHighStanding {
                                                synched_by_user_id: sso_state.discord_user_id.0,
                                                source: sso_state.standing_source,
                                                source_entity_id,
                                            },
                                        ));

                                        if let FilterNode::And(ref mut conditions) = sub.root_filter
                                        {
                                            // Remove any existing high standing filters before adding the new one.
                                            conditions.retain(|c| {
                                                !matches!(
                                                    c,
                                                    FilterNode::Condition(Filter::Simple(
                                                        SimpleFilter::IgnoreHighStanding { .. }
                                                    ))
                                                )
                                            });
                                            conditions.push(new_filter);
                                        } else {
                                            // If it's not an AND node, it might be a single condition.
                                            // We'll wrap the old and new filters in an AND node.
                                            let old_root = sub.root_filter.clone();
                                            // But first, check if the old root is the one we want to replace.
                                            if matches!(
                                                &old_root,
                                                FilterNode::Condition(Filter::Simple(
                                                    SimpleFilter::IgnoreHighStanding { .. }
                                                ))
                                            ) {
                                                sub.root_filter = new_filter;
                                            } else {
                                                sub.root_filter =
                                                    FilterNode::And(vec![old_root, new_filter]);
                                            }
                                        }
                                        if let Err(e) = crate::config::save_subscriptions_for_guild(
                                            guild_id, guild_subs,
                                        ) {
                                            error!(
                                                "Failed to save subscriptions for guild {}: {}",
                                                guild_id, e
                                            );
                                        } else {
                                            subscription_updated = true;
                                        }
                                    }
                                }
                            }

                            if subscription_updated {
                                let _ = msg.channel_id.say(&ctx.http, format!("Subscription '{}' has been successfully synced with {}'s standings.", sso_state.subscription_id, token.character_name)).await;
                            } else {
                                let _ = msg
                                    .channel_id
                                    .say(
                                        &ctx.http,
                                        "No matching subscription found. Please try again.",
                                    )
                                    .await;
                            }
                        }
                        Err(e) => {
                            error!("Failed to exchange token: {}", e);
                            let _ = msg
                                .channel_id
                                .say(
                                    &ctx.http,
                                    "Failed to authenticate with EVE SSO. Please try again.",
                                )
                                .await;
                        }
                    }
                } else if let Err(why) = msg
                    .channel_id
                    .say(
                        &ctx.http,
                        "Invalid or expired state. Please try the command again.",
                    )
                    .await
                {
                    error!("Error sending message: {:?}", why);
                }
            }
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
        match interaction {
            Interaction::ApplicationCommand(command) => {
                let data = ctx.data.read().await;
                let command_map = data.get::<CommandMap>().unwrap();
                let app_state = data.get::<crate::AppStateContainer>().unwrap();

                if let Some(cmd) = command_map.get(&command.data.name) {
                    cmd.execute(&ctx, &command, app_state).await;
                }
            }
            Interaction::MessageComponent(component_interaction) => {
                let custom_id = &component_interaction.data.custom_id;
                let data = ctx.data.read().await;
                let app_state = data.get::<crate::AppStateContainer>().unwrap();

                if custom_id.starts_with("standings_select_") {
                    let state = custom_id.strip_prefix("standings_select_").unwrap();
                    let sso_state = app_state.sso_states.lock().await.remove(state);

                    if let (Some(sso_state), Some(values)) =
                        (sso_state, Some(&component_interaction.data.values))
                    {
                        if let Some(character_id_str) = values.first() {
                            let character_id = character_id_str.parse::<u64>().unwrap();

                            let token_clone = {
                                let standings_map = app_state.user_standings.read().unwrap();
                                if let Some(user_standings) =
                                    standings_map.get(&sso_state.discord_user_id)
                                {
                                    user_standings
                                        .tokens
                                        .iter()
                                        .find(|t| t.character_id == character_id)
                                        .cloned()
                                } else {
                                    None
                                }
                            };

                            if let Some(token) = token_clone {
                                if let Ok((corp_id, alliance_id)) = app_state
                                    .esi_client
                                    .get_character_affiliation(token.character_id)
                                    .await
                                {
                                    let source_entity_id = match sso_state.standing_source {
                                        StandingSource::Character => token.character_id,
                                        StandingSource::Corporation => corp_id,
                                        StandingSource::Alliance => alliance_id.unwrap_or(corp_id),
                                    };

                                    let guild_id = sso_state.original_interaction.guild_id.unwrap();
                                    let mut subscription_updated = false;
                                    {
                                        let _lock = app_state.subscriptions_file_lock.lock().await;
                                        let mut subs_map = app_state.subscriptions.write().unwrap();
                                        if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
                                            let original_channel_id = sso_state
                                                .original_interaction
                                                .channel_id
                                                .to_string();
                                            if let Some(sub) = guild_subs.iter_mut().find(|s| {
                                                s.id == sso_state.subscription_id
                                                    && s.action.channel_id == original_channel_id
                                            }) {
                                                let new_filter =
                                                    FilterNode::Condition(Filter::Simple(
                                                        SimpleFilter::IgnoreHighStanding {
                                                            synched_by_user_id: sso_state
                                                                .discord_user_id
                                                                .0,
                                                            source: sso_state.standing_source,
                                                            source_entity_id,
                                                        },
                                                    ));
                                                if let FilterNode::And(ref mut conditions) =
                                                    sub.root_filter
                                                {
                                                    // Remove any existing high standing filters before adding the new one.
                                                    conditions.retain(|c| {
                                                        !matches!(c, FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding { .. })))
                                                    });
                                                    conditions.push(new_filter);
                                                } else {
                                                    // If it's not an AND node, it might be a single condition.
                                                    // We'll wrap the old and new filters in an AND node.
                                                    let old_root = sub.root_filter.clone();
                                                    // But first, check if the old root is the one we want to replace.
                                                    if matches!(
                                                        &old_root,
                                                        FilterNode::Condition(Filter::Simple(
                                                            SimpleFilter::IgnoreHighStanding { .. }
                                                        ))
                                                    ) {
                                                        sub.root_filter = new_filter;
                                                    } else {
                                                        sub.root_filter = FilterNode::And(vec![
                                                            old_root, new_filter,
                                                        ]);
                                                    }
                                                }

                                                if let Err(e) =
                                                    crate::config::save_subscriptions_for_guild(
                                                        guild_id, guild_subs,
                                                    )
                                                {
                                                    error!("Failed to save subscriptions for guild {}: {}", guild_id, e);
                                                } else {
                                                    subscription_updated = true;
                                                }
                                            }
                                        }
                                    }

                                    if subscription_updated {
                                        if let Err(why) = component_interaction
                                            .create_interaction_response(&ctx.http, |r| {
                                                r.interaction_response_data(|m| {
                                                    m.content(format!(
                                                        "Subscription '{}' updated successfully.",
                                                        sso_state.subscription_id
                                                    ))
                                                    .ephemeral(true)
                                                })
                                            })
                                            .await
                                        {
                                            error!("Cannot respond to interaction: {}", why);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if custom_id.starts_with("standings_reauth_") {
                    let state = custom_id.strip_prefix("standings_reauth_").unwrap();
                    let sso_states = app_state.sso_states.lock().await;
                    if let Some(sso_state) = sso_states.get(state) {
                        let sync_command = SyncStandingsCommand;
                        sync_command
                            .initiate_sso(&ctx, &sso_state.original_interaction, app_state, state)
                            .await;
                        let _ = component_interaction
                            .create_interaction_response(&ctx.http, |r| {
                                r.interaction_response_data(|m| {
                                    m.content("A new authorization link has been sent to your DMs.")
                                        .ephemeral(true)
                                })
                            })
                            .await;
                    }
                }
            }
            _ => {}
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
    let position = match killmail.victim.position.as_ref() {
        None => {
            warn!(
                "Killmail {} has no position data for victim: {:#?}\nLocation ID: {:#?}",
                killmail.killmail_id, killmail.victim.position, zk_data.zkb.location_id
            );
            return None;
        }
        Some(pos) => pos,
    };
    let cache_key = killmail.solar_system_id;

    if let Some(celestial) = app_state.celestial_cache.get(&cache_key) {
        return Some(celestial);
    }

    let celestial = app_state
        .esi_client
        .get_celestial(killmail.solar_system_id, position.x, position.y, position.z)
        .await;

    match celestial {
        Ok(celestial) => {
            let celestial_arc = Arc::new(celestial);
            app_state
                .celestial_cache
                .insert(cache_key, celestial_arc.clone())
                .await;
            Some(celestial_arc)
        }
        Err(e) => {
            warn!(
                "Failed to fetch celestial data for system {} and location {:#?}: {:#?}",
                killmail.solar_system_id, zk_data.zkb.location_id, e
            );
            None
        }
    }
}

async fn select_best_entity_for_display(
    app_state: &Arc<AppState>,
    zk_data: &ZkData,
    matched_attackers: &HashSet<AttackerKey>,
    victim_matched: bool,
) -> Option<MatchedEntity> {
    let mut potential_matches = Vec::new();

    // Find the full Attacker structs corresponding to the matched keys
    let attacker_map: HashMap<AttackerKey, &Attacker> = zk_data
        .killmail
        .attackers
        .iter()
        .map(|a| (AttackerKey::new(a), a))
        .collect();

    for key in matched_attackers {
        if let Some(attacker) = attacker_map.get(key) {
            if let Some(type_id) = attacker.ship_type_id.or(attacker.weapon_type_id) {
                if let Some(group_id) = get_ship_group_id(app_state, type_id).await {
                    potential_matches.push(MatchedEntity {
                        ship_name: get_name(app_state, type_id as u64)
                            .await
                            .unwrap_or_default(),
                        type_id,
                        group_id,
                        corp_id: attacker.corporation_id,
                        alliance_id: attacker.alliance_id,
                        color: Color::Green,
                    });
                }
            }
        }
    }

    // If the victim was also a match, add them to the list of potential entities to display
    if victim_matched {
        if let Some(group_id) =
            get_ship_group_id(app_state, zk_data.killmail.victim.ship_type_id).await
        {
            potential_matches.push(MatchedEntity {
                ship_name: get_name(app_state, zk_data.killmail.victim.ship_type_id as u64)
                    .await
                    .unwrap_or_default(),
                type_id: zk_data.killmail.victim.ship_type_id,
                group_id,
                corp_id: zk_data.killmail.victim.corporation_id,
                alliance_id: zk_data.killmail.victim.alliance_id,
                color: Color::Red,
            });
        }
    }

    // Prioritize the list and return the best one
    potential_matches.into_iter().min_by_key(|entity| {
        SHIP_GROUP_PRIORITY
            .iter()
            .position(|&p| p == entity.group_id)
            .unwrap_or(usize::MAX)
    })
}

// --- Message Sending and Embed Building ---

pub async fn send_killmail_message(
    http: &Arc<Http>,
    app_state: &Arc<AppState>,
    subscription: &Subscription,
    zk_data: &ZkData,
    filter_result: NamedFilterResult,
) -> Result<(), KillmailSendError> {
    let channel = match subscription.action.channel_id.parse::<u64>() {
        Ok(id) => ChannelId(id),
        Err(e) => {
            error!(
                "[Kill: {}] Invalid channel ID '{}': {:#?}",
                zk_data.kill_id, subscription.action.channel_id, e
            );
            return Err(KillmailSendError::Other("Invalid channel ID".into()));
        }
    };
    let embed = build_killmail_embed(app_state, zk_data, &filter_result, subscription).await;

    let content = match &subscription.action.ping_type {
        None => None,
        Some(ping_type) => {
            let kill_time = DateTime::parse_from_rfc3339(&zk_data.killmail.killmail_time)
                .unwrap_or_else(|_| Utc::now().into());
            let kill_age = Utc::now().signed_duration_since(kill_time);

            let max_delay = ping_type.max_ping_delay_in_minutes().unwrap_or(0);
            if max_delay == 0 || kill_age.num_minutes() <= max_delay as i64 {
                let channel_id = subscription.action.channel_id.parse::<u64>().unwrap_or(0);
                let mut ping_times = app_state.last_ping_times.lock().await;

                let now = Instant::now();
                let last_ping = ping_times
                    .entry(channel_id)
                    .or_insert(now - Duration::from_secs(301));

                if now.duration_since(*last_ping) > Duration::from_secs(300) {
                    *last_ping = now;
                    Some(match ping_type {
                        PingType::Here { .. } => "@here",
                        PingType::Everyone { .. } => "@everyone",
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    let result = channel
        .send_message(http, |m| {
            if let Some(content) = content {
                m.content(content)
            } else {
                m
            }
            .set_embed(embed)
        })
        .await;

    if let Err(e) = result {
        if let serenity::Error::Http(http_err) = &e {
            match &**http_err {
                Error::UnsuccessfulRequest(resp) => match resp.status_code {
                    serenity::http::StatusCode::FORBIDDEN => {
                        error!(
                            "[Kill: {}] Forbidden to send message to channel {}. Removing subscriptions.",
                            zk_data.kill_id, channel
                        );
                        return Err(KillmailSendError::CleanupChannel(e));
                    }
                    serenity::http::StatusCode::NOT_FOUND => {
                        error!(
                            "[Kill: {}] Channel {} not found. Removing subscriptions.",
                            zk_data.kill_id, channel
                        );
                        return Err(KillmailSendError::CleanupChannel(e));
                    }
                    _ => {}
                },
                _ => {
                    error!(
                        "[Kill: {}] HTTP error while sending message to channel {}: {:#?}",
                        zk_data.kill_id, channel, e
                    );
                    return Err(KillmailSendError::Other(Box::new(e)));
                }
            }
        }
        error!(
            "[Kill: {}] Failed to send message to channel {}: {:#?}",
            zk_data.kill_id, channel, e
        );
        return Err(KillmailSendError::Other(Box::new(e)));
    }
    info!(
        "[Kill: {}] Sent message to channel {}",
        zk_data.kill_id, channel
    );
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
    let kill_time = match DateTime::parse_from_rfc3339(killmail_time) {
        Ok(t) => t.with_timezone(&Utc),
        Err(e) => {
            error!("Failed to parse killmail time '{}': {}", killmail_time, e);
            return "just now".to_string();
        } // Early return on parse failure
    };
    let now = Utc::now();
    let diff = now.signed_duration_since(kill_time);

    let seconds = diff.num_seconds();
    if seconds < 1 {
        return "just now".to_string();
    }
    if seconds == 1 {
        return "1 second later".to_string();
    }

    let minutes = seconds / 60;
    let hours = minutes / 60;
    let days = hours / 24;
    let weeks = days / 7;
    // Approximation: 4 weeks per month. Note: This is not perfectly accurate.
    let months = weeks / 4;
    let years = months / 12;

    if years > 1 {
        return format!("{} years later", years);
    }
    if years == 1 {
        return "1 year later".to_string();
    }
    if months > 1 {
        return format!("{} months later", months);
    }
    if months == 1 {
        return "1 month later".to_string();
    }
    if weeks > 1 {
        return format!("{} weeks later", weeks);
    }
    if weeks == 1 {
        return "1 week later".to_string();
    }
    if days > 1 {
        return format!("{} days later", days);
    }
    if days == 1 {
        return "1 day later".to_string();
    }
    if hours > 1 {
        return format!("{} hours later", hours);
    }
    if hours == 1 {
        return "1 hour later".to_string();
    }
    if minutes > 1 {
        return format!("{} minutes later", minutes);
    }
    if minutes == 1 {
        return "1 minute later".to_string();
    }

    format!("{} seconds later", seconds)
}

fn str_alliance_icon(id: u64) -> String {
    format!("https://images.evetech.net/alliances/{}/logo?size=64", id)
}

#[allow(unused)]
fn str_corp_icon(id: u64) -> String {
    format!(
        "https://images.evetech.net/corporations/{}/logo?size=64",
        id
    )
}

fn str_ship_icon(id: u32) -> String {
    format!("https://images.evetech.net/types/{}/icon?size=64", id)
}

#[allow(unused)]
fn str_pilot_zk(id: u64) -> String {
    format!("https://zkillboard.com/character/{}/", id)
}
#[allow(unused)]
fn str_corp_zk(id: u64) -> String {
    format!("https://zkillboard.com/corporation/{}/", id)
}
#[allow(unused)]
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

enum DotlanJumpType {
    Super,
    Fax,
    Blops,
}
fn str_jump_dotlan(from: &str, to: &str, with: DotlanJumpType) -> String {
    let with = match with {
        DotlanJumpType::Super => "Nyx",
        DotlanJumpType::Fax => "Lif",
        DotlanJumpType::Blops => "Sin",
    };
    format!(
        "https://evemaps.dotlan.net/jump/{},555/{}:{}",
        with, from, to
    )
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

/// Get the most common attacker ship group for title display
async fn get_most_common_attacker_group(
    app_state: &Arc<AppState>,
    attackers: &[Attacker],
) -> (u64, String) {
    // Count attackers by ship group
    let mut group_counts: HashMap<u32, u64> = HashMap::new();
    for attacker in attackers {
        if let Some(ship_type_id) = attacker.ship_type_id {
            if let Some(group_id) = get_ship_group_id(app_state, ship_type_id).await {
                *group_counts.entry(group_id).or_insert(0) += 1;
            }
        }
    }

    // Find the most common group
    if let Some((group_id, count)) = group_counts.into_iter().max_by_key(|(_, c)| *c) {
        let group_name = get_group_name(group_id, count as u32)
            .unwrap_or("ships")
            .to_string();
        (count, group_name)
    } else {
        (attackers.len() as u64, "ships".to_string())
    }
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
    named_filter_result: &NamedFilterResult,
    subscription: &Subscription,
) -> CreateEmbed {
    let mut embed = CreateEmbed::default();
    let filter_result = &named_filter_result.filter_result;
    let killmail = &zk_data.killmail;

    // --- Basic Data Fetching ---
    let system_info = get_system(app_state, killmail.solar_system_id).await;
    let system_name = system_info.as_ref().map_or("Unknown System", |s| &s.name);
    let system_id = system_info.as_ref().map_or(0, |s| s.id);
    let region_name = system_info.as_ref().map_or("Unknown Region", |s| &s.region);
    let region_id = system_info.as_ref().map_or(0, |s| s.region_id);

    let best_match = select_best_entity_for_display(
        app_state,
        zk_data,
        &filter_result.matched_attackers,
        filter_result.matched_victim,
    )
    .await;

    let total_value_str = abbreviate_number(zk_data.zkb.total_value);
    let relative_time = get_relative_time(&killmail.killmail_time);
    let killmail_time = DateTime::parse_from_rfc3339(&killmail.killmail_time)
        .unwrap_or_else(|_| Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()));

    let killmail_url = format!("https://zkillboard.com/kill/{}/", killmail.killmail_id);
    let related_br = format!(
        "https://br.evetools.org/related/{}/{}",
        system_id,
        format_datetime_to_timestamp(&killmail_time)
    );

    // --- Victim Info ---
    let victim_ship_name = get_name(app_state, killmail.victim.ship_type_id as u64)
        .await
        .unwrap_or_else(|| "Unknown Ship".to_string());

    // Get victim character name and zkillboard link
    // For structures (no character_id), use the structure/ship name instead
    let (victim_char_name, victim_char_link) = if let Some(char_id) = killmail.victim.character_id {
        let name = get_name(app_state, char_id)
            .await
            .unwrap_or_else(|| "Unknown".to_string());
        let link = format!("https://zkillboard.com/character/{}/", char_id);
        (name, Some(link))
    } else {
        // No character = structure kill, use the structure name
        (victim_ship_name.clone(), None)
    };

    // Get victim ticker and zkillboard link (alliance preferred, corp fallback)
    let (victim_ticker, victim_affiliation_link) = if let Some(alliance_id) = killmail.victim.alliance_id {
        let ticker = get_ticker(app_state, alliance_id, true).await;
        let link = format!("https://zkillboard.com/alliance/{}/", alliance_id);
        (ticker, Some(link))
    } else if let Some(corp_id) = killmail.victim.corporation_id {
        let ticker = get_ticker(app_state, corp_id, false).await;
        let link = format!("https://zkillboard.com/corporation/{}/", corp_id);
        (ticker, Some(link))
    } else {
        (None, None)
    };

    // --- Fleet Composition ---
    let fleet_comp = compute_fleet_composition(app_state, &killmail.attackers).await;

    // --- Determine Display Ship Type for Title ---
    // For ship type/group tracking (Green): use matched ship group count
    // For entity tracking (alliance/corp) or victim matches: use most common attacker group
    let (title_ship_count, title_ship_group_name) = if let Some(ref matched) = best_match {
        if matched.color == Color::Green && subscription.root_filter.contains_ship_filter() {
            // Ship tracking: count all attackers with the matched ship group
            let tracked_group = matched.group_id;
            let mut count = 0u64;
            for attacker in &killmail.attackers {
                if let Some(ship_id) = attacker.ship_type_id {
                    if let Some(gid) = get_ship_group_id(app_state, ship_id).await {
                        if gid == tracked_group {
                            count += 1;
                        }
                    }
                }
            }
            let plural_name = get_group_name(tracked_group, count as u32)
                .unwrap_or("ships")
                .to_string();
            (count.max(1), plural_name)
        } else {
            // Entity tracking (alliance/corp) or victim match: use most common attacker group
            get_most_common_attacker_group(app_state, &killmail.attackers).await
        }
    } else {
        // No match: show most common attacker group
        get_most_common_attacker_group(app_state, &killmail.attackers).await
    };

    // --- Title (dynamic based on color) with backticks around ship names ---
    let title = match best_match.as_ref().map(|m| m.color) {
        Some(Color::Green) => {
            // Kill: "{count}x `{group}` killed a `{victim_ship}`"
            format!(
                "{}x `{}` killed a `{}`",
                title_ship_count, title_ship_group_name, victim_ship_name
            )
        }
        Some(Color::Red) => {
            // Loss: "`{victim_ship}` died to {count}x `{group}`"
            format!(
                "`{}` died to {}x `{}`",
                victim_ship_name, title_ship_count, title_ship_group_name
            )
        }
        None => {
            // Default: show what killed them
            format!(
                "`{}` died to {}x `{}`",
                victim_ship_name, title_ship_count, title_ship_group_name
            )
        }
    };

    // --- Author (Battle Report link) ---
    let author_ship_name = if let Some(ref matched) = best_match {
        get_group_name(matched.group_id, title_ship_count as u32)
            .unwrap_or(&matched.ship_name)
            .to_string()
    } else {
        title_ship_group_name.clone()
    };

    let author_text = format!(
        "BR: {} in {} ({})\nKillmail posted {}",
        author_ship_name, system_name, region_name, relative_time
    );

    // Author icon: green = tracked ship, red = most common attacker ship
    let author_icon = if let Some(ref matched) = best_match {
        if matched.color == Color::Green {
            str_ship_icon(matched.type_id)
        } else {
            // Red (loss): show most common attacker ship type
            most_common_ship_type(&killmail.attackers)
                .map(|(type_id, _)| str_ship_icon(type_id as u32))
                .unwrap_or_else(|| str_ship_icon(killmail.victim.ship_type_id))
        }
    } else {
        str_ship_icon(killmail.victim.ship_type_id)
    };

    // --- Location Details ---
    let location_line = format!(
        "**in:** [{}]({}) ([{}]({}))",
        system_name,
        str_system_dotlan(system_id),
        region_name,
        str_region_dotlan(region_id)
    );

    let celestial_line = if let Some(celestial) = get_closest_celestial(app_state, zk_data).await {
        let distance_km = celestial.distance / 1000.0;
        let distance_str = if distance_km > 1_500_000.0 {
            format!("{:.1} AU", distance_km / 149_597_870.7)
        } else {
            format!("{:.1} km", distance_km)
        };
        format!(
            "**on:** [{}]({}), {} away",
            celestial.item_name,
            str_location(celestial.item_id),
            distance_str
        )
    } else {
        String::new()
    };

    let range_line = if let Some(matched_system_range) = &filter_result.light_year_range {
        if matched_system_range.range > 0.0 {
            let matched_base_system_name = get_system(app_state, matched_system_range.system_id)
                .await
                .map_or_else(|| "Unknown System".to_string(), |s| s.name);
            format!(
                "**range:** {:.1} LY from {} ([Supers]({})|[FAX]({})|[Blops]({}))",
                matched_system_range.range,
                matched_base_system_name,
                str_jump_dotlan(&matched_base_system_name, system_name, DotlanJumpType::Super),
                str_jump_dotlan(&matched_base_system_name, system_name, DotlanJumpType::Fax),
                str_jump_dotlan(&matched_base_system_name, system_name, DotlanJumpType::Blops)
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // --- Attackers Field with Fleet Composition ---
    let overall_fleet_comp = fleet_comp.format_overall();
    let alliance_breakdown = fleet_comp.format_alliance_breakdown(app_state).await;

    let attackers_content = format!("{}\n```\n{}```", overall_fleet_comp, alliance_breakdown);

    // --- Victim Field with zkillboard links ---
    let victim_display = {
        // Format character name with link if available
        let char_display = match victim_char_link {
            Some(link) => format!("[{}]({})", victim_char_name, link),
            None => victim_char_name,
        };

        // Format ticker with link if available
        match (victim_ticker, victim_affiliation_link) {
            (Some(ticker), Some(link)) => format!("[[{}]]({}) {}", ticker, link, char_display),
            (Some(ticker), None) => format!("[{}] {}", ticker, char_display),
            (None, _) => char_display,
        }
    };

    // --- Footer (alliance logo of matched entity) ---
    let footer_icon = match best_match.as_ref() {
        Some(matched) => {
            if let Some(alliance_id) = matched.alliance_id {
                str_alliance_icon(alliance_id)
            } else if let Some(corp_id) = matched.corp_id {
                str_corp_icon(corp_id)
            } else {
                str_ship_icon(killmail.victim.ship_type_id)
            }
        }
        None => {
            // Fallback to victim's alliance/corp
            if let Some(alliance_id) = killmail.victim.alliance_id {
                str_alliance_icon(alliance_id)
            } else if let Some(corp_id) = killmail.victim.corporation_id {
                str_corp_icon(corp_id)
            } else {
                str_ship_icon(killmail.victim.ship_type_id)
            }
        }
    };

    // --- Build the Embed ---
    embed.title(title);
    embed.url(killmail_url);
    embed.author(|a| a.name(author_text).url(related_br).icon_url(author_icon));
    embed.thumbnail(str_ship_icon(killmail.victim.ship_type_id)); // Always victim ship
    embed.color(match best_match.as_ref().map(|bm| bm.color).unwrap_or_default() {
        Color::Green => Colour::DARK_GREEN,
        Color::Red => Colour::RED,
    });

    // Location fields
    let mut location_content = location_line;
    if !celestial_line.is_empty() {
        location_content.push_str(&format!("\n{}", celestial_line));
    }
    if !range_line.is_empty() {
        location_content.push_str(&format!("\n{}", range_line));
    }
    embed.description(location_content);

    // Attackers field
    embed.field(
        format!("({}) Attackers Involved", killmail.attackers.len()),
        attackers_content,
        false,
    );

    // Victim field
    embed.field("Victim", victim_display, false);

    // Footer
    embed.footer(|f| {
        f.text(format!(
            "Value: {} \u{2022} EVETime: {}",
            total_value_str,
            killmail_time.format("%d/%m/%Y, %H:%M"),
        ))
        .icon_url(footer_icon)
    });

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(&killmail.killmail_time) {
        embed.timestamp(timestamp.to_rfc3339());
    }

    embed
}

/// Ship category groups for fleet composition display
const SUPER_GROUPS: &[u32] = &[30, 659]; // Titans, Supercarriers
const CAP_GROUPS: &[u32] = &[4594, 485, 1538, 547, 883, 902, 513]; // Lancers, Dreads, FAX, Carriers, Cap Indy, JF, Freighters

/// Fleet composition data for attackers
struct FleetComposition {
    /// Overall counts by group_id, sorted by priority
    overall: Vec<(u32, u32)>,
    /// Per-affiliation counts: affiliation_id -> Vec<(group_id, count)>
    by_affiliation: Vec<(u64, u32, Vec<(u32, u32)>)>, // (affiliation_id, total_count, groups)
}

impl FleetComposition {
    /// Format overall fleet composition by category (supers, caps, subcaps):
    /// Single line if ≤43 chars, otherwise multi-line
    fn format_overall(&self) -> String {
        let mut category_lines = Vec::new();

        // Supers line (Titans, Supercarriers)
        if let Some(line) = Self::format_category_line_plain(&self.overall, |gid| SUPER_GROUPS.contains(&gid)) {
            category_lines.push(line);
        }

        // Caps line (Dreads, FAX, Carriers, etc.)
        if let Some(line) = Self::format_category_line_plain(&self.overall, |gid| CAP_GROUPS.contains(&gid)) {
            category_lines.push(line);
        }

        // Subcaps line (everything else that's known)
        if let Some(line) = Self::format_category_line_plain(&self.overall, |gid| {
            gid != GROUP_UNKNOWN && !SUPER_GROUPS.contains(&gid) && !CAP_GROUPS.contains(&gid)
        }) {
            category_lines.push(line);
        }

        // Try single line first (join all with ", ")
        let single_line = category_lines.join(", ");
        if single_line.len() <= 43 {
            single_line
        } else {
            // Multi-line format
            category_lines.join("\n")
        }
    }

    /// Format a category line for overall (no └ prefix), up to 2 types + overflow
    /// Selects top 2 by count, displays in GROUP_NAMES priority order
    fn format_category_line_plain(groups: &[(u32, u32)], category_filter: impl Fn(u32) -> bool) -> Option<String> {
        let mut filtered: Vec<_> = groups
            .iter()
            .filter(|(gid, _)| category_filter(*gid))
            .cloned()
            .collect();

        if filtered.is_empty() {
            return None;
        }

        let total: u32 = filtered.iter().map(|(_, c)| c).sum();

        // Sort by count DESC to select top 2 most numerous
        filtered.sort_by(|a, b| b.1.cmp(&a.1));

        // Take top 2, then re-sort by GROUP_NAMES priority for display
        let mut top2: Vec<_> = filtered.into_iter().take(2).collect();
        top2.sort_by_key(|(gid, _)| {
            GROUP_NAMES.iter().position(|(id, _, _)| id == gid).unwrap_or(usize::MAX)
        });

        let mut parts = Vec::new();
        let mut shown_count = 0u32;

        for (group_id, count) in top2.iter() {
            if let Some(name) = get_group_name(*group_id, *count) {
                parts.push(format!("{}x {}", count, name));
                shown_count += count;
            }
        }

        let remaining = total.saturating_sub(shown_count);
        if remaining > 0 {
            parts.push(format!("+{}", remaining));
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(", "))
        }
    }

    /// Format a category line (supers, caps, or subcaps) with up to 2 types + overflow
    /// Selects top 2 by count, displays in GROUP_NAMES priority order
    fn format_category_line(groups: &[(u32, u32)], category_filter: impl Fn(u32) -> bool) -> Option<String> {
        let mut filtered: Vec<_> = groups
            .iter()
            .filter(|(gid, _)| category_filter(*gid))
            .cloned()
            .collect();

        if filtered.is_empty() {
            return None;
        }

        let total: u32 = filtered.iter().map(|(_, c)| c).sum();

        // Sort by count DESC to select top 2 most numerous
        filtered.sort_by(|a, b| b.1.cmp(&a.1));

        // Take top 2, then re-sort by GROUP_NAMES priority for display
        let mut top2: Vec<_> = filtered.into_iter().take(2).collect();
        top2.sort_by_key(|(gid, _)| {
            GROUP_NAMES.iter().position(|(id, _, _)| id == gid).unwrap_or(usize::MAX)
        });

        let mut parts = Vec::new();
        let mut shown_count = 0u32;

        for (group_id, count) in top2.iter() {
            if let Some(name) = get_group_name(*group_id, *count) {
                parts.push(format!("{} {}", count, name));
                shown_count += count;
            }
        }

        let remaining = total.saturating_sub(shown_count);
        if remaining > 0 {
            parts.push(format!("+{}", remaining));
        }

        if parts.is_empty() {
            None
        } else {
            Some(format!(" \u{2514} {}", parts.join(", ")))
        }
    }

    /// Format alliance breakdown with ship categories (supers, caps, subcaps)
    /// Only shows affiliations with >10 participants (except the first), max 8 affiliations
    async fn format_alliance_breakdown(&self, app_state: &Arc<AppState>) -> String {
        let mut lines = Vec::new();
        let max_affiliations = 8;
        let min_participants = 10;
        let mut shown_count = 0;
        let mut others_total: u32 = 0;

        for (i, (affiliation_id, total_count, groups)) in self.by_affiliation.iter().enumerate() {
            // Skip affiliations with ≤10 participants (except the first one)
            // Also stop after showing max_affiliations
            if shown_count >= max_affiliations || (i > 0 && *total_count <= min_participants) {
                others_total += total_count;
                continue;
            }

            // Get ticker for this affiliation - try alliance first, then corp
            let ticker = match get_ticker(app_state, *affiliation_id, true).await {
                Some(t) => t,
                None => get_ticker(app_state, *affiliation_id, false)
                    .await
                    .unwrap_or_else(|| "???".to_string()),
            };

            lines.push(format!("[{}] {}", ticker, total_count));

            // Supers line (Titans, Supercarriers)
            if let Some(line) = Self::format_category_line(groups, |gid| SUPER_GROUPS.contains(&gid)) {
                lines.push(line);
            }

            // Caps line (Dreads, FAX, Carriers, etc.)
            if let Some(line) = Self::format_category_line(groups, |gid| CAP_GROUPS.contains(&gid)) {
                lines.push(line);
            }

            // Subcaps line (everything else that's not unknown)
            if let Some(line) = Self::format_category_line(groups, |gid| {
                gid != GROUP_UNKNOWN && !SUPER_GROUPS.contains(&gid) && !CAP_GROUPS.contains(&gid)
            }) {
                lines.push(line);
            }

            shown_count += 1;
        }

        // Add "others" line if there are aggregated affiliations
        if others_total > 0 {
            lines.push(format!("others {}", others_total));
        }

        lines.join("\n")
    }
}

/// Compute fleet composition by aggregating attackers by ship group
async fn compute_fleet_composition(
    app_state: &Arc<AppState>,
    attackers: &[Attacker],
) -> FleetComposition {
    // Count by group overall
    let mut group_counts: HashMap<u32, u32> = HashMap::new();
    // Count by (affiliation, group) - for ship breakdown
    let mut affiliation_groups: HashMap<u64, HashMap<u32, u32>> = HashMap::new();
    // Count total attackers per affiliation (including those without ships)
    let mut affiliation_totals: HashMap<u64, u32> = HashMap::new();
    // Track unknown groups for debugging
    let mut unknown_groups: HashMap<u32, u32> = HashMap::new();

    for attacker in attackers {
        let affiliation_id = attacker.alliance_id.or(attacker.corporation_id).unwrap_or(0);

        // Count ALL attackers for affiliation totals
        *affiliation_totals.entry(affiliation_id).or_insert(0) += 1;

        // Only count ships for group breakdown
        if let Some(ship_id) = attacker.ship_type_id {
            let group_id = get_ship_group_id(app_state, ship_id).await.unwrap_or(0);

            let effective_group_id = if is_known_group(group_id) {
                group_id
            } else {
                *unknown_groups.entry(group_id).or_insert(0) += 1;
                GROUP_UNKNOWN
            };

            *group_counts.entry(effective_group_id).or_insert(0) += 1;
            *affiliation_groups
                .entry(affiliation_id)
                .or_default()
                .entry(effective_group_id)
                .or_insert(0) += 1;
        }
    }

    // Log unknown groups for debugging
    if !unknown_groups.is_empty() {
        trace!("Unknown ship groups encountered: {:?}", unknown_groups);
    }

    // Sort overall by GROUP_NAMES order (priority)
    let mut overall: Vec<(u32, u32)> = group_counts.into_iter().collect();
    overall.sort_by_key(|(group_id, _)| {
        GROUP_NAMES
            .iter()
            .position(|(id, _, _)| id == group_id)
            .unwrap_or(usize::MAX)
    });

    // Sort affiliations by total count (using affiliation_totals which includes ALL attackers)
    let mut by_affiliation: Vec<(u64, u32, Vec<(u32, u32)>)> = affiliation_totals
        .into_iter()
        .map(|(aff_id, total)| {
            // Get ship groups for this affiliation (may be empty if all attackers had no ship)
            let groups = affiliation_groups.remove(&aff_id).unwrap_or_default();
            let mut group_vec: Vec<(u32, u32)> = groups.into_iter().collect();
            group_vec.sort_by_key(|(group_id, _)| {
                GROUP_NAMES
                    .iter()
                    .position(|(id, _, _)| id == group_id)
                    .unwrap_or(usize::MAX)
            });
            (aff_id, total, group_vec)
        })
        .collect();
    by_affiliation.sort_by(|a, b| b.1.cmp(&a.1));

    FleetComposition {
        overall,
        by_affiliation,
    }
}

/// Get ticker for an entity (alliance or corporation)
async fn get_ticker(app_state: &Arc<AppState>, id: u64, is_alliance: bool) -> Option<String> {
    // Check tickers cache first
    {
        let tickers = app_state.tickers.read().unwrap();
        if let Some(ticker) = tickers.get(&id) {
            return Some(ticker.clone());
        }
    }

    // Fetch from ESI
    match app_state.esi_client.get_ticker(id, is_alliance).await {
        Ok(ticker) => {
            let _lock = app_state.tickers_file_lock.lock().await;
            let mut tickers = app_state.tickers.write().unwrap();
            tickers.insert(id, ticker.clone());
            crate::config::save_tickers(&tickers);
            Some(ticker)
        }
        Err(e) => {
            trace!("Failed to fetch ticker for {}: {}", id, e);
            None
        }
    }
}
