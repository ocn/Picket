use crate::commands::sync_standings::SyncStandingsCommand;
use crate::commands::Command;
use crate::config::{
    save_names, save_ships, save_systems, save_user_standings, AppState, Filter, FilterNode,
    PingType, SimpleFilter, StandingSource, Subscription, System,
};
use crate::contract_intelligence::{
    parse_contract_discord_message_id, ContractDelivery, ContractDeliveryError,
    ContractMessageEdit, ContractNotificationMessage, HealthDiscordPublisher, HealthPublishError,
    PreparedContractDelivery, ShipGroupLookup, ShipGroupResolver,
};
use crate::esi::Celestial;
use crate::models::{Attacker, ZkData};
use crate::presentation::{
    compact_location_description, CompactLocation, LocationOn, LocationRange, LocationRegion,
    LocationSystem,
};
use crate::processor::{AttackerKey, Color, NamedFilterResult};
use crate::sov_feed;
use crate::watchlist_feed;
use chrono::{DateTime, FixedOffset, Utc};
use serde_json::Value;
use serenity::async_trait;
use serenity::builder::{CreateEmbed, CreateMessage, ParseValue};
use serenity::http::Http;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::model::guild::UnavailableGuild;
use serenity::model::id::GuildId;
use serenity::model::prelude::{ChannelId, Interaction};
use serenity::prelude::*;
use serenity::utils::Colour;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace, warn};

pub(crate) const SHIP_GROUP_PRIORITY: &[u32] = &[
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
        .map(
            |(_, singular, plural)| {
                if count == 1 {
                    *singular
                } else {
                    *plural
                }
            },
        )
}

/// Check if a group ID has a known display name
fn is_known_group(group_id: u32) -> bool {
    GROUP_NAMES.iter().any(|(id, _, _)| *id == group_id)
}

/// Select top N groups, preferring known groups over unknown ones.
/// Known groups fill slots first (sorted by count DESC, then GROUP_NAMES priority, then group_id).
/// Unknown groups fill remaining slots. Final output is sorted by GROUP_NAMES display priority.
fn select_top_groups(filtered: Vec<(u32, u32)>, limit: usize) -> Vec<(u32, u32)> {
    let (mut known, mut unknown): (Vec<_>, Vec<_>) = filtered
        .into_iter()
        .partition(|(gid, _)| is_known_group(*gid));

    // Sort by count DESC, tie-break by GROUP_NAMES priority (lower = better), then group_id
    let sort_fn = |a: &(u32, u32), b: &(u32, u32)| {
        b.1.cmp(&a.1)
            .then_with(|| {
                let pa = GROUP_NAMES
                    .iter()
                    .position(|(id, _, _)| id == &a.0)
                    .unwrap_or(usize::MAX);
                let pb = GROUP_NAMES
                    .iter()
                    .position(|(id, _, _)| id == &b.0)
                    .unwrap_or(usize::MAX);
                pa.cmp(&pb)
            })
            .then_with(|| a.0.cmp(&b.0))
    };
    known.sort_by(sort_fn);
    unknown.sort_by(sort_fn);

    let mut selected: Vec<(u32, u32)> = known.into_iter().take(limit).collect();
    let remaining_slots = limit.saturating_sub(selected.len());
    selected.extend(unknown.into_iter().take(remaining_slots));

    // Final display sort by GROUP_NAMES priority
    selected.sort_by_key(|(gid, _)| {
        GROUP_NAMES
            .iter()
            .position(|(id, _, _)| id == gid)
            .unwrap_or(usize::MAX)
    });
    selected
}

/// A Fleet Composition Tally key: either a Ship Group (today's behavior) or a
/// Type-Tracked Ship's Ship Type, carrying its parent Ship Group for category
/// classification and display-position sorting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TallyKey {
    Group(u32),
    Type { type_id: u32, parent_group: u32 },
}

impl TallyKey {
    /// The Ship Group a Tally Entry is classified and sorted by.
    fn parent_group(&self) -> u32 {
        match self {
            TallyKey::Group(group_id) => *group_id,
            TallyKey::Type { parent_group, .. } => *parent_group,
        }
    }

    /// The ID used to tie-break same-count Tally Entries: a Ship Type's own type ID,
    /// or a Ship Group's group ID.
    fn sort_id(&self) -> u32 {
        match self {
            TallyKey::Group(group_id) => *group_id,
            TallyKey::Type { type_id, .. } => *type_id,
        }
    }
}

/// Select top N Tally Entries, preferring Type-Tracked Ships over Ship Groups.
/// Type entries claim named slots first (sorted by count DESC, tie-break by type_id),
/// then remaining slots are filled by the existing group selection (`select_top_groups`)
/// over the non-type entries. Group entries gain no new priority: with no type entries,
/// this is byte-identical to `select_top_groups`. Final output sorts by each entry's
/// parent group's GROUP_NAMES display priority.
fn select_top_entries(filtered: Vec<(TallyKey, u32)>, limit: usize) -> Vec<(TallyKey, u32)> {
    let (mut type_entries, group_entries): (Vec<_>, Vec<_>) = filtered
        .into_iter()
        .partition(|(key, _)| matches!(key, TallyKey::Type { .. }));

    type_entries.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.sort_id().cmp(&b.0.sort_id()))
    });

    let mut selected: Vec<(TallyKey, u32)> = type_entries.into_iter().take(limit).collect();
    let remaining_slots = limit.saturating_sub(selected.len());

    if remaining_slots > 0 {
        let group_input: Vec<(u32, u32)> = group_entries
            .into_iter()
            .map(|(key, count)| (key.parent_group(), count))
            .collect();
        selected.extend(
            select_top_groups(group_input, remaining_slots)
                .into_iter()
                .map(|(group_id, count)| (TallyKey::Group(group_id), count)),
        );
    }

    selected.sort_by_key(|(key, _)| {
        GROUP_NAMES
            .iter()
            .position(|(id, _, _)| *id == key.parent_group())
            .unwrap_or(usize::MAX)
    });
    selected
}

/// Get a group name dynamically - checks GROUP_NAMES first, then ESI cache
/// Returns the ESI name (e.g., "Cruiser") if not in our custom GROUP_NAMES
async fn get_dynamic_group_name(app_state: &Arc<AppState>, group_id: u32, count: u32) -> String {
    // First check our custom GROUP_NAMES for abbreviated display names
    if let Some(name) = get_group_name(group_id, count) {
        return name.to_string();
    }

    // Check the ESI cache
    {
        let group_names = app_state.group_names.read().unwrap();
        if let Some(name) = group_names.get(&group_id) {
            // ESI names are singular (e.g., "Cruiser"), we don't pluralize them
            return name.clone();
        }
    }

    // Fetch from ESI and cache
    match app_state.esi_client.get_group_name(group_id).await {
        Ok(name) => {
            let _lock = app_state.group_names_file_lock.lock().await;
            let mut group_names = app_state.group_names.write().unwrap();
            group_names.insert(group_id, name.clone());
            crate::config::save_group_names(&group_names);
            name
        }
        Err(e) => {
            warn!("Failed to fetch group name for {}: {}", group_id, e);
            "ships".to_string()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MatchedEntity {
    pub ship_name: String,
    pub type_id: u32,
    /// The flown hull's type ID, if the attacker actually has one. `None` when the
    /// attacker was only identified via `weapon_type_id` (i.e. `type_id` above is the
    /// weapon, not a hull). Type-Tracked Ship display must gate on this field, never
    /// on `type_id`, so a weapon ID is never mistaken for a hull.
    pub hull_type_id: Option<u32>,
    pub group_id: u32,
    pub corp_id: Option<u64>,
    pub alliance_id: Option<u64>,
    pub color: Color,
}

#[derive(Debug)]
pub struct PreparedDispatch {
    pub guild_id: GuildId,
    pub subscription: Subscription,
    pub zk_data: ZkData,
    pub embed: CreateEmbed,
    pub filter_result: NamedFilterResult,
}

pub struct DiscordContractDelivery {
    http: Arc<Http>,
    edit_client: reqwest::Client,
    edit_api_base: String,
}

const CONTRACT_REPAIR_HTTP_TIMEOUT: Duration = Duration::from_secs(90);

fn contract_repair_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(CONTRACT_REPAIR_HTTP_TIMEOUT)
        .build()
        .expect("construct contract repair HTTP client")
}

fn contract_repair_authorization_header(
    token: &str,
) -> Result<reqwest::header::HeaderValue, ContractDeliveryError> {
    let mut value = token.parse::<reqwest::header::HeaderValue>().map_err(|_| {
        ContractDeliveryError::permanent("Discord bot token is not a valid HTTP header")
    })?;
    value.set_sensitive(true);
    Ok(value)
}

pub struct DiscordHealthPublisher {
    http: Arc<Http>,
}

impl DiscordHealthPublisher {
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }
}

#[async_trait]
impl HealthDiscordPublisher for DiscordHealthPublisher {
    async fn create_view(
        &self,
        channel_id: u64,
        content: &str,
    ) -> Result<String, HealthPublishError> {
        let message = ChannelId(channel_id)
            .send_message(&self.http, |builder| {
                configure_health_view_message(builder, channel_id, content)
            })
            .await
            .map_err(health_publish_error)?;
        Ok(message.id.to_string())
    }

    async fn update_view(
        &self,
        channel_id: u64,
        message_id: &str,
        content: &str,
    ) -> Result<(), HealthPublishError> {
        ChannelId(channel_id)
            .edit_message(&self.http, health_message_id(message_id)?, |builder| {
                builder
                    .content(content)
                    .allowed_mentions(|mentions| mentions.empty_parse())
            })
            .await
            .map_err(health_publish_error)?;
        Ok(())
    }

    async fn create_incident(
        &self,
        channel_id: u64,
        content: &str,
        mention_operator_id: Option<u64>,
    ) -> Result<String, HealthPublishError> {
        let message = ChannelId(channel_id)
            .send_message(&self.http, |builder| {
                configure_health_incident_message(builder, channel_id, content, mention_operator_id)
            })
            .await
            .map_err(health_publish_error)?;
        Ok(message.id.to_string())
    }

    async fn update_incident(
        &self,
        channel_id: u64,
        message_id: &str,
        content: &str,
    ) -> Result<(), HealthPublishError> {
        ChannelId(channel_id)
            .edit_message(&self.http, health_message_id(message_id)?, |builder| {
                builder
                    .content(content)
                    .allowed_mentions(|mentions| mentions.empty_parse())
            })
            .await
            .map_err(health_publish_error)?;
        Ok(())
    }
}

fn configure_health_view_message<'a, 'builder>(
    builder: &'builder mut CreateMessage<'a>,
    channel_id: u64,
    content: &str,
) -> &'builder mut CreateMessage<'a> {
    builder
        .content(content)
        .allowed_mentions(|mentions| mentions.empty_parse());
    builder
        .0
        .insert("nonce", Value::String(format!("hwv-{channel_id}")));
    builder.0.insert("enforce_nonce", Value::Bool(true));
    builder
}

fn configure_health_incident_message<'a, 'builder>(
    builder: &'builder mut CreateMessage<'a>,
    channel_id: u64,
    content: &str,
    mention_operator_id: Option<u64>,
) -> &'builder mut CreateMessage<'a> {
    let content = mention_operator_id
        .map(|operator_id| format!("<@{operator_id}>\n{content}"))
        .unwrap_or_else(|| content.to_string());
    builder.content(content).allowed_mentions(|mentions| {
        let mentions = mentions.empty_parse();
        if let Some(operator_id) = mention_operator_id {
            mentions.users([operator_id])
        } else {
            mentions
        }
    });
    builder
        .0
        .insert("nonce", Value::String(format!("hwi-{channel_id}")));
    builder.0.insert("enforce_nonce", Value::Bool(true));
    builder
}

fn health_message_id(message_id: &str) -> Result<u64, HealthPublishError> {
    message_id.parse::<u64>().map_err(|_| {
        HealthPublishError::Permanent("persisted health Discord message ID is invalid".to_string())
    })
}

fn health_publish_error(error: serenity::Error) -> HealthPublishError {
    let detail = error.to_string();
    if let serenity::Error::Http(http_error) = &error {
        if let serenity::http::error::Error::UnsuccessfulRequest(response) = &**http_error {
            let status = response.status_code.as_u16();
            return health_publish_error_from_response(
                status,
                response.error.code,
                &response.error.message,
            );
        }
    }
    HealthPublishError::Transient(detail)
}

fn health_publish_error_from_response(
    status: u16,
    discord_code: isize,
    message: &str,
) -> HealthPublishError {
    let detail = format!("Discord HTTP {status}, JSON code {discord_code}: {message}");
    if discord_code == 10_008 {
        HealthPublishError::UnknownMessage(detail)
    } else if matches!(status, 400 | 401 | 403 | 404) {
        HealthPublishError::Permanent(detail)
    } else {
        HealthPublishError::Transient(detail)
    }
}

impl DiscordContractDelivery {
    pub fn new(http: Arc<Http>) -> Self {
        Self {
            http,
            edit_client: contract_repair_http_client(),
            edit_api_base: "https://discord.com/api/v10".to_string(),
        }
    }

    #[doc(hidden)]
    pub fn new_with_edit_api_base(http: Arc<Http>, edit_api_base: String) -> Self {
        Self {
            http,
            edit_client: contract_repair_http_client(),
            edit_api_base,
        }
    }
}

#[async_trait]
impl ContractDelivery for DiscordContractDelivery {
    async fn send(
        &self,
        delivery: PreparedContractDelivery,
    ) -> Result<String, ContractDeliveryError> {
        let message = ChannelId(delivery.channel_id)
            .send_message(&self.http, |builder| {
                configure_contract_delivery_message(builder, &delivery)
            })
            .await
            .map_err(contract_delivery_error)?;
        Ok(message.id.to_string())
    }

    async fn edit(&self, edit: ContractMessageEdit) -> Result<(), ContractDeliveryError> {
        let message_id = parse_contract_discord_message_id(&edit.discord_message_id)
            .map_err(ContractDeliveryError::permanent)?;
        let payload = serde_json::json!({
            "embeds": [contract_notification_embed(&edit.message).0],
            "allowed_mentions": { "parse": [] },
        });
        let response = self
            .edit_client
            .patch(format!(
                "{}/channels/{}/messages/{message_id}",
                self.edit_api_base, edit.channel_id
            ))
            .header(
                reqwest::header::AUTHORIZATION,
                contract_repair_authorization_header(&self.http.token)?,
            )
            .json(&payload)
            .send()
            .await
            .map_err(|_| ContractDeliveryError::transient("Discord edit transport failure"))?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<f64>().ok());
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        let code = body.get("code").and_then(Value::as_i64).unwrap_or_default() as isize;
        if status.as_u16() == 429 {
            let seconds = retry_after
                .or_else(|| body.get("retry_after").and_then(Value::as_f64))
                .unwrap_or(1.0);
            let deadline =
                Utc::now() + chrono::Duration::milliseconds((seconds.max(0.0) * 1000.0) as i64);
            return Err(ContractDeliveryError::transient_after(
                "Discord HTTP 429",
                deadline,
            ));
        }
        let detail = format!("Discord HTTP {}, JSON code {code}", status.as_u16());
        if is_temporary_discord_delivery_code(code)
            || (status.is_server_error() && !is_permanent_discord_delivery_code(code))
        {
            return Err(ContractDeliveryError::transient(detail));
        }
        if matches!(status.as_u16(), 400 | 401 | 403 | 404 | 405)
            || is_permanent_discord_delivery_code(code)
        {
            let delivery_scoped = matches!(status.as_u16(), 401 | 403 | 404 | 405)
                || matches!(code, 10003 | 10008 | 50001 | 50008 | 50013 | 50014 | 50025);
            return Err(if delivery_scoped {
                ContractDeliveryError::permanent_delivery(detail)
            } else {
                ContractDeliveryError::permanent(detail)
            });
        }
        Err(ContractDeliveryError::ambiguous(detail))
    }
}

fn configure_contract_delivery_message<'a, 'builder>(
    builder: &'builder mut CreateMessage<'a>,
    delivery: &PreparedContractDelivery,
) -> &'builder mut CreateMessage<'a> {
    if delivery.ping {
        builder.content(delivery.ping_type.content());
    }
    let builder = builder
        .allowed_mentions(|mentions| {
            let mentions = mentions.empty_parse();
            if delivery.ping {
                mentions.parse(ParseValue::Everyone)
            } else {
                mentions
            }
        })
        .set_embed(contract_notification_embed(&delivery.message));
    builder
        .0
        .insert("nonce", Value::String(delivery.nonce.clone()));
    builder
        .0
        .insert("enforce_nonce", Value::Bool(delivery.enforce_nonce));
    builder
}

#[cfg(test)]
pub(crate) fn configured_contract_delivery_payload_for_test(
    delivery: &PreparedContractDelivery,
) -> serde_json::Map<String, Value> {
    let mut builder = CreateMessage::default();
    configure_contract_delivery_message(&mut builder, delivery);
    builder
        .0
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn contract_delivery_error(error: serenity::Error) -> ContractDeliveryError {
    let message = error.to_string();
    if let serenity::Error::Http(http_error) = &error {
        if let serenity::http::error::Error::UnsuccessfulRequest(response) = &**http_error {
            let status = response.status_code.as_u16();
            let code = response.error.code;
            let detail = format!(
                "Discord HTTP {status}, JSON code {code}: {}",
                response.error.message
            );
            if status == 429 || status >= 500 || is_temporary_discord_delivery_code(code) {
                return ContractDeliveryError::transient(detail);
            }
            if status == 401 || is_permanent_discord_delivery_code(code) {
                return ContractDeliveryError::permanent(detail);
            }
            return ContractDeliveryError::ambiguous(detail);
        }
    }
    ContractDeliveryError::ambiguous(message)
}

fn is_temporary_discord_delivery_code(code: isize) -> bool {
    matches!(code, 20016 | 20028 | 20029 | 40004 | 40006 | 40062 | 130000)
}

fn is_permanent_discord_delivery_code(code: isize) -> bool {
    matches!(
        code,
        10003 | 10008 | 50001 | 50008 | 50013 | 50014 | 50025 | 50035
    )
}

pub struct DiscordShipGroupResolver {
    app_state: Arc<AppState>,
}

impl DiscordShipGroupResolver {
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self { app_state }
    }
}

#[async_trait]
impl ShipGroupResolver for DiscordShipGroupResolver {
    async fn group_for_type(&self, type_id: i64) -> ShipGroupLookup {
        let Ok(type_id) = u32::try_from(type_id) else {
            return ShipGroupLookup::Resolved(None);
        };
        if let Some(group_id) = self.app_state.ships.read().unwrap().get(&type_id).copied() {
            return ShipGroupLookup::Resolved(Some(i64::from(group_id)));
        }
        match self.app_state.esi_client.get_ship_group_id(type_id).await {
            Ok(group_id) => {
                let _lock = self.app_state.ships_file_lock.lock().await;
                let mut ships = self.app_state.ships.write().unwrap();
                ships.insert(type_id, group_id);
                save_ships(&ships);
                ShipGroupLookup::Resolved(Some(i64::from(group_id)))
            }
            Err(error) => {
                warn!(
                    type_id,
                    "contract ship group lookup will be retried: {error}"
                );
                ShipGroupLookup::TemporarilyUnavailable
            }
        }
    }
}

pub fn contract_notification_embed(message: &ContractNotificationMessage) -> CreateEmbed {
    let mut embed = CreateEmbed::default();
    embed.title(&message.title);
    if let Some(description) = &message.description {
        embed.description(description);
    }
    if let Some(author) = &message.author {
        embed.author(|builder| builder.name(author));
    }
    if let Some(thumbnail_url) = &message.thumbnail_url {
        embed.thumbnail(thumbnail_url);
    }
    if let Some(footer) = &message.footer {
        embed.footer(|builder| builder.text(footer));
    }
    for field in &message.fields {
        embed.field(&field.name, &field.value, field.inline);
    }
    if let Some(timestamp) = message.timestamp {
        embed.timestamp(timestamp.to_rfc3339());
    }
    embed
}

/// Renders one sov campaign notification (spec "Embed": title, fields,
/// footer naming the stage). Mirrors `contract_notification_embed`.
pub fn sov_campaign_embed(message: &sov_feed::SovNotificationMessage) -> CreateEmbed {
    let mut embed = CreateEmbed::default();
    embed.title(&message.title);
    for field in &message.fields {
        embed.field(&field.name, &field.value, field.inline);
    }
    embed.footer(|builder| builder.text(&message.footer));
    embed
}

pub struct DiscordSovDelivery {
    http: Arc<Http>,
}

impl DiscordSovDelivery {
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }
}

#[async_trait]
impl sov_feed::SovDelivery for DiscordSovDelivery {
    async fn send(
        &self,
        delivery: sov_feed::PreparedSovDelivery,
    ) -> Result<String, sov_feed::SovDeliveryError> {
        let message = ChannelId(delivery.channel_id)
            .send_message(&self.http, |builder| {
                if let Some(role_id) = delivery.role_id {
                    builder.content(format!("<@&{role_id}>"));
                }
                builder.allowed_mentions(|mentions| {
                    let mentions = mentions.empty_parse();
                    match delivery.role_id {
                        Some(role_id) => mentions.roles([role_id]),
                        None => mentions,
                    }
                });
                builder.set_embed(sov_campaign_embed(&delivery.message));
                builder
                    .0
                    .insert("nonce", Value::String(delivery.nonce.clone()));
                builder
                    .0
                    .insert("enforce_nonce", Value::Bool(delivery.enforce_nonce));
                builder
            })
            .await
            .map_err(sov_delivery_error)?;
        Ok(message.id.to_string())
    }
}

/// Classifies a Serenity send failure as transient or permanent, reusing
/// the same Discord HTTP status/JSON-code judgment as the contract feed's
/// `contract_delivery_error` (`is_temporary_discord_delivery_code`,
/// `is_permanent_discord_delivery_code`): 429/5xx and the known-temporary
/// codes retry; 401/403/404/405 and the known-permanent codes (channel or
/// message gone, missing permissions, and similar) never retry (review
/// finding 3).
fn sov_delivery_error(error: serenity::Error) -> sov_feed::SovDeliveryError {
    let message = error.to_string();
    if let serenity::Error::Http(http_error) = &error {
        if let serenity::http::error::Error::UnsuccessfulRequest(response) = &**http_error {
            let status = response.status_code.as_u16();
            let code = response.error.code;
            let detail = format!(
                "Discord HTTP {status}, JSON code {code}: {}",
                response.error.message
            );
            if status == 429 || status >= 500 || is_temporary_discord_delivery_code(code) {
                return sov_feed::SovDeliveryError::transient(detail);
            }
            if matches!(status, 401 | 403 | 404 | 405) || is_permanent_discord_delivery_code(code) {
                return sov_feed::SovDeliveryError::permanent(detail);
            }
            // Ambiguous Discord outcomes retry rather than give up
            // permanently: this feed has no repair/edit path to revisit a
            // permanently-abandoned delivery later, unlike the contract
            // feed's `Ambiguous` tier.
            return sov_feed::SovDeliveryError::transient(detail);
        }
    }
    sov_feed::SovDeliveryError::transient(message)
}

/// Synchronous system/region lookups for the sov feed, backed by the
/// killfeed's `config/systems.json` cache. No ESI fallback: sov campaigns
/// only ever reference known k-space systems already present in that
/// cache.
pub struct DiscordSovSystemDirectory {
    app_state: Arc<AppState>,
}

impl DiscordSovSystemDirectory {
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self { app_state }
    }
}

impl sov_feed::SovSystemDirectory for DiscordSovSystemDirectory {
    fn resolve(&self, solar_system_id: i64) -> Option<sov_feed::SovSystemInfo> {
        let system_id = u32::try_from(solar_system_id).ok()?;
        let systems = self.app_state.systems.read().unwrap();
        systems
            .get(&system_id)
            .map(|system| sov_feed::SovSystemInfo {
                name: system.name.clone(),
                region_name: system.region.clone(),
                region_id: system.region_id as i64,
            })
    }
}

/// Alliance ticker lookup for the sov feed's defender ticker, reusing the
/// killfeed's tickers cache with an ESI fallback.
pub struct DiscordSovTickerResolver {
    app_state: Arc<AppState>,
}

impl DiscordSovTickerResolver {
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self { app_state }
    }
}

#[async_trait]
impl sov_feed::SovTickerResolver for DiscordSovTickerResolver {
    async fn alliance_ticker(&self, alliance_id: i64) -> Option<String> {
        let id = u64::try_from(alliance_id).ok()?;
        get_ticker(&self.app_state, id, true).await
    }

    /// Corporation ticker for the sovereignty map's "current owner" field
    /// (ticket 07), reusing the same cache/ESI path as `alliance_ticker`
    /// with `is_alliance: false`.
    async fn corporation_ticker(&self, corporation_id: i64) -> Option<String> {
        let id = u64::try_from(corporation_id).ok()?;
        get_ticker(&self.app_state, id, false).await
    }

    /// Faction name for the sovereignty map's "current owner" field
    /// (ticket 07, FW/NPC systems). ESI's `/universe/names/` bulk resolver
    /// (`EsiClient::get_name`) covers faction IDs like any other category;
    /// unlike alliance/corporation tickers this is not cached in
    /// `config/tickers.json` -- EVE has only a few dozen factions and the
    /// map polls hourly, so the extra request per unresolved faction per
    /// poll is not worth a new cache file for this ticket.
    async fn faction_name(&self, faction_id: i64) -> Option<String> {
        let id = u64::try_from(faction_id).ok()?;
        self.app_state.esi_client.get_name(id).await.ok()
    }
}

/// Renders one watchlist notification (spec "Embed": alliance/corporation
/// names and tickers, the event, Dotlan and zKillboard links; footer names
/// the event kind). Mirrors [`sov_campaign_embed`].
pub fn watchlist_embed(message: &watchlist_feed::WatchlistNotificationMessage) -> CreateEmbed {
    let mut embed = CreateEmbed::default();
    embed.title(&message.title);
    for field in &message.fields {
        embed.field(&field.name, &field.value, field.inline);
    }
    embed.footer(|builder| builder.text(&message.footer));
    embed
}

pub struct DiscordWatchlistDelivery {
    http: Arc<Http>,
}

impl DiscordWatchlistDelivery {
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }
}

#[async_trait]
impl watchlist_feed::WatchlistDelivery for DiscordWatchlistDelivery {
    async fn send(
        &self,
        delivery: watchlist_feed::PreparedWatchlistDelivery,
    ) -> Result<String, watchlist_feed::WatchlistDeliveryError> {
        let message = ChannelId(delivery.channel_id)
            .send_message(&self.http, |builder| {
                if let Some(role_id) = delivery.role_id {
                    builder.content(format!("<@&{role_id}>"));
                }
                builder.allowed_mentions(|mentions| {
                    let mentions = mentions.empty_parse();
                    match delivery.role_id {
                        Some(role_id) => mentions.roles([role_id]),
                        None => mentions,
                    }
                });
                builder.set_embed(watchlist_embed(&delivery.message));
                builder
                    .0
                    .insert("nonce", Value::String(delivery.nonce.clone()));
                builder
                    .0
                    .insert("enforce_nonce", Value::Bool(delivery.enforce_nonce));
                builder
            })
            .await
            .map_err(watchlist_delivery_error)?;
        Ok(message.id.to_string())
    }
}

/// Classifies a Serenity send failure as transient or permanent, reusing
/// the same Discord HTTP status/JSON-code judgment as the sov feed's
/// `sov_delivery_error`.
fn watchlist_delivery_error(error: serenity::Error) -> watchlist_feed::WatchlistDeliveryError {
    let message = error.to_string();
    if let serenity::Error::Http(http_error) = &error {
        if let serenity::http::error::Error::UnsuccessfulRequest(response) = &**http_error {
            let status = response.status_code.as_u16();
            let code = response.error.code;
            let detail = format!(
                "Discord HTTP {status}, JSON code {code}: {}",
                response.error.message
            );
            if status == 429 || status >= 500 || is_temporary_discord_delivery_code(code) {
                return watchlist_feed::WatchlistDeliveryError::transient(detail);
            }
            if matches!(status, 401 | 403 | 404 | 405) || is_permanent_discord_delivery_code(code) {
                return watchlist_feed::WatchlistDeliveryError::permanent(detail);
            }
            return watchlist_feed::WatchlistDeliveryError::transient(detail);
        }
    }
    watchlist_feed::WatchlistDeliveryError::transient(message)
}

/// Resolves alliance/corporation names and tickers for watchlist embeds,
/// reusing the killfeed's tickers/names caches with an ESI fallback
/// (`GET /corporations/{id}/`, `GET /alliances/{id}/`).
pub struct DiscordWatchlistResolver {
    app_state: Arc<AppState>,
    /// Set when `note_corporation` mutated the in-memory names/tickers maps,
    /// so `flush_noted_corporations` writes each backing file at most once per
    /// member-snapshot pass instead of once per fresh corporation (ticket-09
    /// finding 5).
    names_dirty: std::sync::atomic::AtomicBool,
    tickers_dirty: std::sync::atomic::AtomicBool,
}

impl DiscordWatchlistResolver {
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self {
            app_state,
            names_dirty: std::sync::atomic::AtomicBool::new(false),
            tickers_dirty: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl watchlist_feed::WatchlistEntityResolver for DiscordWatchlistResolver {
    async fn corporation(&self, corporation_id: i64) -> watchlist_feed::WatchlistCorporation {
        let id = match u64::try_from(corporation_id) {
            Ok(id) => id,
            Err(_) => {
                return watchlist_feed::WatchlistCorporation {
                    name: None,
                    ticker: None,
                }
            }
        };
        watchlist_feed::WatchlistCorporation {
            name: get_name(&self.app_state, id).await,
            ticker: get_ticker(&self.app_state, id, false).await,
        }
    }

    async fn alliance(&self, alliance_id: i64) -> watchlist_feed::WatchlistAlliance {
        let id = match u64::try_from(alliance_id) {
            Ok(id) => id,
            Err(_) => {
                return watchlist_feed::WatchlistAlliance {
                    name: None,
                    ticker: None,
                }
            }
        };
        watchlist_feed::WatchlistAlliance {
            name: get_name(&self.app_state, id).await,
            ticker: get_ticker(&self.app_state, id, true).await,
        }
    }

    async fn note_corporation(
        &self,
        corporation_id: i64,
        name: Option<String>,
        ticker: Option<String>,
    ) {
        use std::sync::atomic::Ordering;
        let Ok(id) = u64::try_from(corporation_id) else {
            return;
        };
        // Update only the in-memory maps here; the backing JSON files are
        // written once by `flush_noted_corporations` at the end of the pass
        // (finding 5). The in-memory update is what a same-pass embed render
        // reads, so nothing waits on the flush.
        if let Some(name) = name {
            let mut names = self.app_state.names.write().unwrap();
            if names.get(&id) != Some(&name) {
                names.insert(id, name);
                self.names_dirty.store(true, Ordering::Relaxed);
            }
        }
        if let Some(ticker) = ticker {
            let mut tickers = self.app_state.tickers.write().unwrap();
            if tickers.get(&id) != Some(&ticker) {
                tickers.insert(id, ticker);
                self.tickers_dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    async fn flush_noted_corporations(&self) {
        use std::sync::atomic::Ordering;
        if self.names_dirty.swap(false, Ordering::Relaxed) {
            let _lock = self.app_state.names_file_lock.lock().await;
            let names = self.app_state.names.read().unwrap();
            save_names(&names);
        }
        if self.tickers_dirty.swap(false, Ordering::Relaxed) {
            let _lock = self.app_state.tickers_file_lock.lock().await;
            let tickers = self.app_state.tickers.read().unwrap();
            crate::config::save_tickers(&tickers);
        }
    }
}

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
                        hull_type_id: attacker.ship_type_id,
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
                hull_type_id: Some(zk_data.killmail.victim.ship_type_id),
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

/// Send error type for killmail messages.
/// Used by integration tests to distinguish channel cleanup errors from other failures.
#[derive(Debug)]
pub enum KillmailSendError {
    CleanupChannel(serenity::Error),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for KillmailSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KillmailSendError::CleanupChannel(e) => write!(f, "Channel cleanup required: {e}"),
            KillmailSendError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for KillmailSendError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillmailAllowedMentions {
    None,
    Everyone,
    Role(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedKillmailNotification {
    pub content: Option<String>,
    pub allowed_mentions: KillmailAllowedMentions,
}

impl PreparedKillmailNotification {
    fn requests_channel_ping(self) -> bool {
        self.content.is_some()
    }
}

pub fn prepare_killmail_notification(
    subscription: &Subscription,
    killmail_time: &str,
    evaluated_at: DateTime<Utc>,
    channel_ping_eligible: bool,
) -> PreparedKillmailNotification {
    let killmail_time =
        DateTime::parse_from_rfc3339(killmail_time).unwrap_or_else(|_| evaluated_at.fixed_offset());
    let kill_age = evaluated_at
        .fixed_offset()
        .signed_duration_since(killmail_time);

    let notification = match &subscription.action.ping_type {
        Some(ping_type)
            if channel_ping_eligible
                && (ping_type.max_ping_delay_in_minutes().unwrap_or(0) == 0
                    || kill_age.num_minutes()
                        <= i64::from(ping_type.max_ping_delay_in_minutes().unwrap_or(0))) =>
        {
            match ping_type {
                PingType::Here { .. } => {
                    (Some("@here".to_string()), KillmailAllowedMentions::Everyone)
                }
                PingType::Everyone { .. } => (
                    Some("@everyone".to_string()),
                    KillmailAllowedMentions::Everyone,
                ),
                PingType::Role { role_id, .. } => (
                    Some(format!("<@&{role_id}>")),
                    KillmailAllowedMentions::Role(*role_id),
                ),
            }
        }
        _ => (None, KillmailAllowedMentions::None),
    };

    PreparedKillmailNotification {
        content: notification.0,
        allowed_mentions: notification.1,
    }
}

pub(crate) async fn prepare_killmail_notification_for_delivery(
    app_state: &AppState,
    subscription: &Subscription,
    killmail_time: &str,
    channel_id: u64,
) -> PreparedKillmailNotification {
    let evaluated_at = Utc::now();
    let eligible_without_cooldown =
        prepare_killmail_notification(subscription, killmail_time, evaluated_at, true)
            .requests_channel_ping();
    let channel_ping_eligible = match subscription.action.ping_type.as_ref() {
        Some(ping_type) if eligible_without_cooldown => {
            crate::config::try_acquire_channel_ping_with_cooldown(
                &app_state.last_ping_times,
                channel_id,
                ping_type.channel_ping_cooldown(),
            )
            .await
        }
        _ => false,
    };

    prepare_killmail_notification(
        subscription,
        killmail_time,
        evaluated_at,
        channel_ping_eligible,
    )
}

pub(crate) fn configure_killmail_notification_message(
    builder: &mut CreateMessage<'_>,
    notification: PreparedKillmailNotification,
    embed: CreateEmbed,
) {
    if let Some(content) = notification.content {
        builder.content(content);
    }
    builder
        .allowed_mentions(|mentions| {
            let mentions = mentions.empty_parse();
            match notification.allowed_mentions {
                KillmailAllowedMentions::None => mentions,
                KillmailAllowedMentions::Everyone => mentions.parse(ParseValue::Everyone),
                KillmailAllowedMentions::Role(role_id) => mentions.roles([role_id]),
            }
        })
        .set_embed(embed);
}

/// Standalone send function used by integration embed tests.
/// Production code shares the notification preparation and message configuration helpers.
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
    let notification = prepare_killmail_notification_for_delivery(
        app_state,
        subscription,
        &zk_data.killmail.killmail_time,
        channel.0,
    )
    .await;

    let result = channel
        .send_message(http, |m| {
            configure_killmail_notification_message(m, notification, embed);
            m
        })
        .await;

    if let Err(e) = result {
        if let serenity::Error::Http(http_err) = &e {
            if let serenity::http::error::Error::UnsuccessfulRequest(resp) = &**http_err {
                if matches!(
                    resp.status_code,
                    serenity::http::StatusCode::FORBIDDEN | serenity::http::StatusCode::NOT_FOUND
                ) {
                    return Err(KillmailSendError::CleanupChannel(e));
                }
            }
        }
        return Err(KillmailSendError::Other(Box::new(e)));
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
/// Prioritizes known groups (from GROUP_NAMES), falls back to ESI names for unknown groups
/// Returns (count, group_name, representative_ship_type_id)
async fn get_most_common_attacker_group(
    app_state: &Arc<AppState>,
    attackers: &[Attacker],
) -> (u64, String, Option<u32>) {
    // Count attackers by ship group, tracking known vs all groups separately
    let mut known_group_counts: HashMap<u32, u64> = HashMap::new();
    let mut all_group_counts: HashMap<u32, u64> = HashMap::new();
    let mut group_ship_type: HashMap<u32, u32> = HashMap::new(); // group_id -> first ship_type_id seen

    for attacker in attackers {
        if let Some(ship_type_id) = attacker.ship_type_id {
            if let Some(group_id) = get_ship_group_id(app_state, ship_type_id).await {
                // Track all groups for fallback
                if group_id != GROUP_UNKNOWN {
                    *all_group_counts.entry(group_id).or_insert(0) += 1;
                    group_ship_type.entry(group_id).or_insert(ship_type_id);
                }
                // Track known groups separately (prioritized for title)
                if is_known_group(group_id) {
                    *known_group_counts.entry(group_id).or_insert(0) += 1;
                }
            }
        }
    }

    // Prefer known groups (they have nice abbreviated names like "BS")
    // When counts tie, use GROUP_NAMES priority so title matches fleet comp ordering
    if let Some((group_id, count)) = known_group_counts.into_iter().max_by(|(g1, c1), (g2, c2)| {
        match c1.cmp(c2) {
            std::cmp::Ordering::Equal => {
                // Tie-break by GROUP_NAMES priority (lower index = higher priority)
                let p1 = GROUP_NAMES
                    .iter()
                    .position(|(id, _, _)| id == g1)
                    .unwrap_or(usize::MAX);
                let p2 = GROUP_NAMES
                    .iter()
                    .position(|(id, _, _)| id == g2)
                    .unwrap_or(usize::MAX);
                p2.cmp(&p1) // Reverse: lower index should win
            }
            other => other,
        }
    }) {
        let group_name = get_group_name(group_id, count as u32)
            .unwrap_or("ships")
            .to_string();
        let ship_type_id = group_ship_type.get(&group_id).copied();
        return (count, group_name, ship_type_id);
    }

    // Fall back to any group and use ESI name
    // When counts tie, use GROUP_NAMES priority for consistency
    if let Some((group_id, count)) =
        all_group_counts
            .into_iter()
            .max_by(|(g1, c1), (g2, c2)| match c1.cmp(c2) {
                std::cmp::Ordering::Equal => {
                    let p1 = GROUP_NAMES
                        .iter()
                        .position(|(id, _, _)| id == g1)
                        .unwrap_or(usize::MAX);
                    let p2 = GROUP_NAMES
                        .iter()
                        .position(|(id, _, _)| id == g2)
                        .unwrap_or(usize::MAX);
                    p2.cmp(&p1)
                }
                other => other,
            })
    {
        let group_name = get_dynamic_group_name(app_state, group_id, count as u32).await;
        let ship_type_id = group_ship_type.get(&group_id).copied();
        return (count, group_name, ship_type_id);
    }

    // No groups at all
    (attackers.len() as u64, "ships".to_string(), None)
}

/// Formats a DateTime object into a YYYYMMDDHH00 string, suitable for battle report URLs.
/// This function effectively rounds the time down to the nearest hour.
fn format_datetime_to_timestamp(date: &DateTime<FixedOffset>) -> String {
    // Convert to UTC to ensure consistency, similar to getUTCFullYear, etc.
    let date_utc = date.with_timezone(&Utc);
    // Format the date and append "00" for the minutes.
    format!("{}00", date_utc.format("%Y%m%d%H"))
}

pub async fn build_killmail_embed(
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
    let (victim_ticker, victim_affiliation_link) =
        if let Some(alliance_id) = killmail.victim.alliance_id {
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

    // --- Determine Display Ship Type for Title ---
    // Type-Tracked Ship (Green, matched hull is in the subscription's ShipType set):
    //   use the Ship Type name, count scoped to that exact hull.
    // For other ship type/group tracking (Green): use matched ship group count.
    // For entity tracking (alliance/corp) or victim matches: use most common attacker group.
    // Also capture a representative ship type ID for the author icon
    let ship_type_ids = subscription.root_filter.ship_type_ids();

    // --- Fleet Composition ---
    let fleet_comp =
        compute_fleet_composition(app_state, &killmail.attackers, &ship_type_ids).await;
    let (title_ship_count, title_ship_group_name, title_ship_type_id) = if let Some(ref matched) =
        best_match
    {
        if matched.color == Color::Green
            && matched
                .hull_type_id
                .is_some_and(|hull| ship_type_ids.contains(&hull))
        {
            // Type-Tracked Ship: count only attackers flying this exact hull (never the weapon).
            // Gating on `hull_type_id` (not `type_id`) matters when an attacker has no
            // `ship_type_id` and matched the subscription's ShipType filter only via
            // `weapon_type_id` — `type_id` would be the weapon in that case.
            let tracked_type = matched
                .hull_type_id
                .expect("checked by the `is_some_and` guard above");
            let count = killmail
                .attackers
                .iter()
                .filter(|a| a.ship_type_id == Some(tracked_type))
                .count() as u64;
            let type_name = get_name(app_state, tracked_type as u64)
                .await
                .unwrap_or_default();
            (count.max(1), type_name, Some(tracked_type))
        } else if matched.color == Color::Green && subscription.root_filter.contains_ship_filter() {
            // Ship Group tracking: count all attackers with the matched ship group
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
            let plural_name = get_dynamic_group_name(app_state, tracked_group, count as u32).await;
            // Use the matched ship type for the icon
            (count.max(1), plural_name, Some(matched.type_id))
        } else {
            // Entity tracking (alliance/corp) or victim match: use most common attacker group
            get_most_common_attacker_group(app_state, &killmail.attackers).await
        }
    } else {
        // No match: show most common attacker group
        get_most_common_attacker_group(app_state, &killmail.attackers).await
    };

    // --- Title (dynamic based on color) with backticks around ship names ---
    // Green (kill) = "{count}x {group} killed a {victim_ship}"
    // Red (loss) = "{victim_ship} died to {count}x {group}"
    // No match (global feeds) = treat as kill (green)
    let effective_color = best_match.as_ref().map(|m| m.color).unwrap_or(Color::Green); // Default to green for global feeds

    let title = match effective_color {
        Color::Green => {
            format!(
                "{}x `{}` killed a `{}`",
                title_ship_count, title_ship_group_name, victim_ship_name
            )
        }
        Color::Red => {
            format!(
                "`{}` died to {}x `{}`",
                victim_ship_name, title_ship_count, title_ship_group_name
            )
        }
    };

    // --- Author (Battle Report link) ---
    // Use the same ship name as the title for consistency
    let author_text = format!(
        "BR: {} in {} ({})\nKillmail posted {}",
        title_ship_group_name, system_name, region_name, relative_time
    );

    // Author icon: use the same ship type as title for consistency
    // Green = title ship (tracked or most common), Red = most common attacker ship
    let author_icon = match effective_color {
        Color::Green => {
            // Green: use the title ship type (matches author text and title)
            title_ship_type_id
                .map(str_ship_icon)
                .unwrap_or_else(|| str_ship_icon(killmail.victim.ship_type_id))
        }
        Color::Red => {
            // Red (loss): show most common attacker ship type
            most_common_ship_type(&killmail.attackers)
                .map(|(type_id, _)| str_ship_icon(type_id as u32))
                .unwrap_or_else(|| str_ship_icon(killmail.victim.ship_type_id))
        }
    };

    // --- Location Details ---
    let celestial = get_closest_celestial(app_state, zk_data).await;
    let celestial_distance = celestial.as_ref().map(|celestial| {
        let distance_km = celestial.distance / 1000.0;
        if distance_km > 1_500_000.0 {
            format!("{:.1} AU", distance_km / 149_597_870.7)
        } else {
            format!("{:.1} km", distance_km)
        }
    });

    let range = if let Some(matched_system_range) = &filter_result.light_year_range {
        if matched_system_range.range > 0.0 {
            let matched_base_system_name = get_system(app_state, matched_system_range.system_id)
                .await
                .map_or_else(|| "Unknown System".to_string(), |s| s.name);
            Some((matched_system_range.range, matched_base_system_name))
        } else {
            None
        }
    } else {
        None
    };
    let celestial_suffix = celestial_distance
        .as_deref()
        .map(|distance| format!("{distance} away"));

    // --- Attackers Field with Fleet Composition ---
    let overall_fleet_comp = fleet_comp.format_overall(app_state).await;
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
    embed.color(match effective_color {
        Color::Green => Colour::DARK_GREEN,
        Color::Red => Colour::RED,
    });

    embed.description(compact_location_description(CompactLocation {
        system: Some(LocationSystem {
            name: system_name,
            id: system_id,
        }),
        region: Some(LocationRegion {
            name: region_name,
            id: region_id,
        }),
        on: celestial.as_ref().map(|celestial| LocationOn {
            name: &celestial.item_name,
            id: celestial.item_id,
            suffix: celestial_suffix.as_deref(),
        }),
        range: range
            .as_ref()
            .map(|(light_years, reference_system)| LocationRange {
                light_years: *light_years,
                reference_system,
                destination_system: system_name,
            }),
    }));

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

/// Get the display name for a Tally Entry: the Ship Type name (always singular)
/// for Type-Tracked Ships, the dynamic Ship Group name (singular/plural by count)
/// for everything else.
async fn get_tally_entry_name(app_state: &Arc<AppState>, key: TallyKey, count: u32) -> String {
    match key {
        TallyKey::Group(group_id) => get_dynamic_group_name(app_state, group_id, count).await,
        TallyKey::Type { type_id, .. } => get_name(app_state, type_id as u64)
            .await
            .unwrap_or_else(|| "ships".to_string()),
    }
}

/// Fleet composition data for attackers
struct FleetComposition {
    /// Overall counts by Tally Entry key, sorted by priority
    overall: Vec<(TallyKey, u32)>,
    /// Per-affiliation counts: affiliation_id -> Vec<(key, count)>
    by_affiliation: Vec<(u64, u32, Vec<(TallyKey, u32)>)>, // (affiliation_id, total_count, entries)
}

impl FleetComposition {
    /// Format overall fleet composition by category (supers, caps, subcaps):
    /// Single line if ≤43 chars, otherwise multi-line
    async fn format_overall(&self, app_state: &Arc<AppState>) -> String {
        let mut category_lines = Vec::new();

        // Supers line (Titans, Supercarriers)
        if let Some(line) = Self::format_category_line_plain(
            &self.overall,
            |gid| SUPER_GROUPS.contains(&gid),
            app_state,
        )
        .await
        {
            category_lines.push(line);
        }

        // Caps line (Dreads, FAX, Carriers, etc.)
        if let Some(line) = Self::format_category_line_plain(
            &self.overall,
            |gid| CAP_GROUPS.contains(&gid),
            app_state,
        )
        .await
        {
            category_lines.push(line);
        }

        // Subcaps line (everything else including unknown)
        if let Some(line) = Self::format_category_line_plain(
            &self.overall,
            |gid| !SUPER_GROUPS.contains(&gid) && !CAP_GROUPS.contains(&gid),
            app_state,
        )
        .await
        {
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
    /// Selects top 2 (Type-Tracked Ships first), displays in GROUP_NAMES priority order
    async fn format_category_line_plain(
        entries: &[(TallyKey, u32)],
        category_filter: impl Fn(u32) -> bool,
        app_state: &Arc<AppState>,
    ) -> Option<String> {
        let filtered: Vec<_> = entries
            .iter()
            .filter(|(key, _)| category_filter(key.parent_group()))
            .cloned()
            .collect();

        if filtered.is_empty() {
            return None;
        }

        let total: u32 = filtered.iter().map(|(_, c)| c).sum();

        // Select top 2, Type-Tracked Ships first, then preferring known groups over unknown
        let top2 = select_top_entries(filtered, 2);

        let mut parts = Vec::new();
        let mut shown_count = 0u32;

        for (key, count) in top2.iter() {
            let name = get_tally_entry_name(app_state, *key, *count).await;
            parts.push(format!("{}x {}", count, name));
            shown_count += count;
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
    /// Selects top 2, Type-Tracked Ships first. Displays in GROUP_NAMES priority order.
    async fn format_category_line(
        entries: &[(TallyKey, u32)],
        category_filter: impl Fn(u32) -> bool,
        app_state: &Arc<AppState>,
    ) -> Option<String> {
        let filtered: Vec<_> = entries
            .iter()
            .filter(|(key, _)| category_filter(key.parent_group()))
            .cloned()
            .collect();

        if filtered.is_empty() {
            return None;
        }

        let total: u32 = filtered.iter().map(|(_, c)| c).sum();

        // Select top 2, Type-Tracked Ships first, then preferring known groups over unknown
        let top2 = select_top_entries(filtered, 2);

        let mut parts = Vec::new();
        let mut shown_count = 0u32;

        for (key, count) in top2.iter() {
            let name = get_tally_entry_name(app_state, *key, *count).await;
            parts.push(format!("{} {}", count, name));
            shown_count += count;
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
            if let Some(line) =
                Self::format_category_line(groups, |gid| SUPER_GROUPS.contains(&gid), app_state)
                    .await
            {
                lines.push(line);
            }

            // Caps line (Dreads, FAX, Carriers, etc.)
            if let Some(line) =
                Self::format_category_line(groups, |gid| CAP_GROUPS.contains(&gid), app_state).await
            {
                lines.push(line);
            }

            // Subcaps line (everything else including unknown)
            if let Some(line) = Self::format_category_line(
                groups,
                |gid| !SUPER_GROUPS.contains(&gid) && !CAP_GROUPS.contains(&gid),
                app_state,
            )
            .await
            {
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

/// Compute fleet composition by aggregating attackers by Tally Entry key: a Type-Tracked
/// Ship's flown hull is in `ship_type_ids` tallies under its Ship Type, apart from its
/// Ship Group; everyone else tallies under their Ship Group as before. No double counting.
async fn compute_fleet_composition(
    app_state: &Arc<AppState>,
    attackers: &[Attacker],
    ship_type_ids: &HashSet<u32>,
) -> FleetComposition {
    // Count by Tally Entry key overall
    let mut entry_counts: HashMap<TallyKey, u32> = HashMap::new();
    // Count by (affiliation, key) - for ship breakdown
    let mut affiliation_entries: HashMap<u64, HashMap<TallyKey, u32>> = HashMap::new();
    // Count total attackers per affiliation (including those without ships)
    let mut affiliation_totals: HashMap<u64, u32> = HashMap::new();
    // Track unknown groups for debugging
    let mut unknown_groups: HashMap<u32, u32> = HashMap::new();

    for attacker in attackers {
        let affiliation_id = attacker
            .alliance_id
            .or(attacker.corporation_id)
            .unwrap_or(0);

        // Count ALL attackers for affiliation totals
        *affiliation_totals.entry(affiliation_id).or_insert(0) += 1;

        // Only count ships for group breakdown
        if let Some(ship_id) = attacker.ship_type_id {
            let group_id = get_ship_group_id(app_state, ship_id)
                .await
                .unwrap_or(GROUP_UNKNOWN);

            // Track unknown groups for debugging (but keep the actual group_id for ESI lookup)
            if !is_known_group(group_id) && group_id != GROUP_UNKNOWN {
                *unknown_groups.entry(group_id).or_insert(0) += 1;
            }

            let key = if ship_type_ids.contains(&ship_id) {
                TallyKey::Type {
                    type_id: ship_id,
                    parent_group: group_id,
                }
            } else {
                TallyKey::Group(group_id)
            };

            *entry_counts.entry(key).or_insert(0) += 1;
            *affiliation_entries
                .entry(affiliation_id)
                .or_default()
                .entry(key)
                .or_insert(0) += 1;
        }
    }

    // Log unknown groups for debugging
    if !unknown_groups.is_empty() {
        trace!(
            "Unknown ship groups (will use ESI names): {:?}",
            unknown_groups
        );
    }

    // Sort overall by GROUP_NAMES order (priority); a type entry sorts at its parent's position
    let mut overall: Vec<(TallyKey, u32)> = entry_counts.into_iter().collect();
    overall.sort_by_key(|(key, _)| {
        GROUP_NAMES
            .iter()
            .position(|(id, _, _)| *id == key.parent_group())
            .unwrap_or(usize::MAX)
    });

    // Sort affiliations by total count (using affiliation_totals which includes ALL attackers)
    let mut by_affiliation: Vec<(u64, u32, Vec<(TallyKey, u32)>)> = affiliation_totals
        .into_iter()
        .map(|(aff_id, total)| {
            // Get entries for this affiliation (may be empty if all attackers had no ship)
            let entries = affiliation_entries.remove(&aff_id).unwrap_or_default();
            let mut entry_vec: Vec<(TallyKey, u32)> = entries.into_iter().collect();
            entry_vec.sort_by_key(|(key, _)| {
                GROUP_NAMES
                    .iter()
                    .position(|(id, _, _)| *id == key.parent_group())
                    .unwrap_or(usize::MAX)
            });
            (aff_id, total, entry_vec)
        })
        .collect();
    by_affiliation.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| {
                // Prefer affiliations with known ship groups over NPC-only groups
                let a_known =
                    a.2.iter()
                        .any(|(key, _)| is_known_group(key.parent_group()));
                let b_known =
                    b.2.iter()
                        .any(|(key, _)| is_known_group(key.parent_group()));
                b_known.cmp(&a_known)
            })
            .then_with(|| {
                // Prefer real entities (non-zero ID) over unaffiliated NPCs
                (b.0 != 0).cmp(&(a.0 != 0))
            })
    });

    FleetComposition {
        overall,
        by_affiliation,
    }
}

/// Get ticker for an entity (alliance or corporation)
pub(crate) async fn get_ticker(
    app_state: &Arc<AppState>,
    id: u64,
    is_alliance: bool,
) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn killmail_subscription(ping_type: Option<PingType>) -> Subscription {
        Subscription {
            id: "notification-test".to_string(),
            description: "notification test".to_string(),
            root_filter: FilterNode::Condition(Filter::Simple(SimpleFilter::IsNpc(true))),
            action: crate::config::Action {
                channel_id: "77".to_string(),
                ping_type,
            },
        }
    }

    fn notification_evaluation_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2025-02-03T04:05:00Z")
            .expect("valid notification evaluation time")
            .with_timezone(&Utc)
    }

    fn notification_app_state() -> AppState {
        AppState::new(
            crate::config::AppConfig {
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
            },
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[tokio::test]
    async fn delivery_uses_subscription_cooldown_while_omitted_cooldown_stays_at_five_minutes() {
        let killmail_time = Utc::now().to_rfc3339();
        let configured = killmail_subscription(Some(PingType::Here {
            max_ping_delay_minutes: None,
            ping_cooldown_minutes: Some(30),
        }));
        let configured_app_state = notification_app_state();
        configured_app_state.last_ping_times.lock().await.insert(
            77,
            tokio::time::Instant::now() - Duration::from_secs(10 * 60),
        );

        let configured_notification = prepare_killmail_notification_for_delivery(
            &configured_app_state,
            &configured,
            &killmail_time,
            77,
        )
        .await;
        assert_eq!(configured_notification.content, None);
        assert_eq!(
            configured_notification.allowed_mentions,
            KillmailAllowedMentions::None
        );

        let default = killmail_subscription(Some(PingType::Here {
            max_ping_delay_minutes: None,
            ping_cooldown_minutes: None,
        }));
        let default_app_state = notification_app_state();
        default_app_state.last_ping_times.lock().await.insert(
            77,
            tokio::time::Instant::now() - Duration::from_secs(10 * 60),
        );

        let default_notification = prepare_killmail_notification_for_delivery(
            &default_app_state,
            &default,
            &killmail_time,
            77,
        )
        .await;
        assert_eq!(default_notification.content.as_deref(), Some("@here"));
        assert_eq!(
            default_notification.allowed_mentions,
            KillmailAllowedMentions::Everyone
        );
    }

    #[test]
    fn fresh_here_notification_prepares_content_and_everyone_parsing() {
        let subscription = killmail_subscription(Some(PingType::Here {
            max_ping_delay_minutes: Some(5),
            ping_cooldown_minutes: None,
        }));
        let evaluated_at = notification_evaluation_time();
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T04:00:00Z",
            evaluated_at,
            true,
        );

        assert_eq!(
            notification,
            PreparedKillmailNotification {
                content: Some("@here".to_string()),
                allowed_mentions: KillmailAllowedMentions::Everyone,
            }
        );

        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, CreateEmbed::default());
        assert_eq!(builder.0["content"].as_str(), Some("@here"));
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!(["everyone"])
        );
    }

    #[test]
    fn stale_notification_omits_ping_and_mentions() {
        let subscription = killmail_subscription(Some(PingType::Here {
            max_ping_delay_minutes: Some(5),
            ping_cooldown_minutes: None,
        }));
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T03:59:00Z",
            notification_evaluation_time(),
            true,
        );

        assert_eq!(
            notification,
            PreparedKillmailNotification {
                content: None,
                allowed_mentions: KillmailAllowedMentions::None,
            }
        );

        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, CreateEmbed::default());
        assert!(builder.0.get("content").is_none());
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
    }

    #[test]
    fn fresh_role_notification_allows_exactly_the_selected_role() {
        let subscription = killmail_subscription(Some(PingType::Role {
            role_id: 987_654_321_098_765_432,
            max_ping_delay_minutes: Some(5),
            ping_cooldown_minutes: None,
        }));
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T04:00:00Z",
            notification_evaluation_time(),
            true,
        );

        assert_eq!(
            notification,
            PreparedKillmailNotification {
                content: Some("<@&987654321098765432>".to_string()),
                allowed_mentions: KillmailAllowedMentions::Role(987_654_321_098_765_432),
            }
        );

        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, CreateEmbed::default());
        assert_eq!(
            builder.0["content"].as_str(),
            Some("<@&987654321098765432>")
        );
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
        assert_eq!(
            builder.0["allowed_mentions"]["roles"],
            serde_json::json!(["987654321098765432"])
        );
        assert!(builder.0["allowed_mentions"].get("users").is_none());
    }

    #[test]
    fn stale_role_notification_omits_mention_content_and_allowlist() {
        let subscription = killmail_subscription(Some(PingType::Role {
            role_id: 123,
            max_ping_delay_minutes: Some(5),
            ping_cooldown_minutes: None,
        }));
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T03:59:00Z",
            notification_evaluation_time(),
            true,
        );
        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, CreateEmbed::default());

        assert!(builder.0.get("content").is_none());
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
        assert!(builder.0["allowed_mentions"].get("roles").is_none());
    }

    #[test]
    fn cooldown_eligible_everyone_notification_allows_only_everyone_parsing() {
        let subscription = killmail_subscription(Some(PingType::Everyone {
            max_ping_delay_minutes: None,
            ping_cooldown_minutes: None,
        }));
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T00:00:00Z",
            notification_evaluation_time(),
            true,
        );

        assert_eq!(notification.content.as_deref(), Some("@everyone"));
        assert_eq!(
            notification.allowed_mentions,
            KillmailAllowedMentions::Everyone
        );

        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, CreateEmbed::default());
        assert_eq!(builder.0["content"].as_str(), Some("@everyone"));
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!(["everyone"])
        );
    }

    #[test]
    fn cooldown_suppressed_notification_still_configures_the_embed() {
        let subscription = killmail_subscription(Some(PingType::Here {
            max_ping_delay_minutes: None,
            ping_cooldown_minutes: None,
        }));
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T04:04:00Z",
            notification_evaluation_time(),
            false,
        );
        let mut embed = CreateEmbed::default();
        embed.title("matched @everyone killmail");
        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, embed);

        assert!(builder.0.get("content").is_none());
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
        assert_eq!(
            builder.0["embeds"][0]["title"].as_str(),
            Some("matched @everyone killmail")
        );
    }

    #[test]
    fn cooldown_suppressed_role_notification_omits_mention_content_and_allowlist() {
        let subscription = killmail_subscription(Some(PingType::Role {
            role_id: 123,
            max_ping_delay_minutes: None,
            ping_cooldown_minutes: None,
        }));
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T04:04:00Z",
            notification_evaluation_time(),
            false,
        );
        let mut embed = CreateEmbed::default();
        embed.title("matched role killmail");
        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, embed);

        assert!(builder.0.get("content").is_none());
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
        assert!(builder.0["allowed_mentions"].get("roles").is_none());
        assert_eq!(
            builder.0["embeds"][0]["title"].as_str(),
            Some("matched role killmail")
        );
    }

    #[test]
    fn non_pinging_subscription_explicitly_allows_no_mentions() {
        let subscription = killmail_subscription(None);
        let notification = prepare_killmail_notification(
            &subscription,
            "2025-02-03T04:04:00Z",
            notification_evaluation_time(),
            true,
        );
        let mut builder = CreateMessage::default();
        configure_killmail_notification_message(&mut builder, notification, CreateEmbed::default());

        assert!(builder.0.get("content").is_none());
        assert_eq!(
            builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
    }

    #[test]
    fn contract_repair_authorization_header_is_sensitive_and_redacted() {
        let token = "Bot controlled-contract-repair-token";
        let header = contract_repair_authorization_header(token).expect("valid token header");
        assert!(header.is_sensitive());
        assert!(!format!("{header:?}").contains(token));
        assert!(CONTRACT_REPAIR_HTTP_TIMEOUT < Duration::from_secs(120));
    }

    #[test]
    fn known_groups_preferred_over_unknown() {
        // 2 unknown with high counts + 2 known with low counts
        let input = vec![(9990, 50), (9991, 30), (358, 10), (832, 5)];
        let result = select_top_groups(input, 2);
        // Known groups selected; Logi(832) before HAC(358) per GROUP_NAMES priority
        assert_eq!(result, vec![(832, 5), (358, 10)]);
    }

    #[test]
    fn falls_back_to_unknown_when_no_known() {
        let input = vec![(9990, 50), (9991, 30)];
        let result = select_top_groups(input, 2);
        // Both selected, sorted by group_id (both have usize::MAX priority)
        assert_eq!(result, vec![(9990, 50), (9991, 30)]);
    }

    #[test]
    fn mixed_fill_when_one_known() {
        // 1 known + 2 unknown
        let input = vec![(358, 10), (9990, 50), (9991, 30)];
        let result = select_top_groups(input, 2);
        // HAC first (has GROUP_NAMES entry), then highest-count unknown
        assert_eq!(result, vec![(358, 10), (9990, 50)]);
    }

    #[test]
    fn deterministic_on_count_ties() {
        // 3 known groups all count=5: BS(27), Logi(832), HAC(358)
        let input = vec![(27, 5), (832, 5), (358, 5)];
        let result = select_top_groups(input, 2);
        // Top 2 by GROUP_NAMES priority: BS pos 11, Logi pos 12 (HAC pos 13 excluded)
        assert_eq!(result, vec![(27, 5), (832, 5)]);
    }

    #[test]
    fn empty_input() {
        let result = select_top_groups(vec![], 2);
        assert_eq!(result, Vec::<(u32, u32)>::new());
    }

    #[test]
    fn only_discord_unknown_message_code_recreates_a_health_message() {
        assert!(matches!(
            health_publish_error_from_response(404, 10_008, "Unknown Message"),
            HealthPublishError::UnknownMessage(_)
        ));
        assert!(matches!(
            health_publish_error_from_response(404, 10_003, "Unknown Channel"),
            HealthPublishError::Permanent(_)
        ));
        assert!(matches!(
            health_publish_error_from_response(403, 50_013, "Missing Permissions"),
            HealthPublishError::Permanent(_)
        ));
    }

    #[test]
    fn single_group() {
        let input = vec![(358, 7)];
        let result = select_top_groups(input, 2);
        assert_eq!(result, vec![(358, 7)]);
    }

    #[test]
    fn select_top_entries_with_no_type_entries_matches_select_top_groups() {
        // Group-only input must be byte-identical to select_top_groups: this is the
        // key regression guarantee for subscriptions without ship-type filters.
        let group_input = vec![(9990, 50), (9991, 30), (358, 10), (832, 5)];
        let entries: Vec<(TallyKey, u32)> = group_input
            .iter()
            .map(|(gid, count)| (TallyKey::Group(*gid), *count))
            .collect();
        let expected: Vec<(TallyKey, u32)> = select_top_groups(group_input, 2)
            .into_iter()
            .map(|(gid, count)| (TallyKey::Group(gid), count))
            .collect();
        assert_eq!(select_top_entries(entries, 2), expected);
    }

    #[test]
    fn select_top_entries_claims_a_slot_for_a_small_count_type_before_larger_groups() {
        // A small-count Type-Tracked Ship (Malediction, parent Ceptor 831, count 1)
        // must claim a named slot ahead of larger untracked groups (Dictor 541 count 2,
        // AF 324 count 2), displacing the second-largest group into overflow.
        let input = vec![
            (TallyKey::Group(324), 2), // AF
            (TallyKey::Group(541), 2), // Dictor
            (
                TallyKey::Type {
                    type_id: 11186,
                    parent_group: 831,
                },
                1,
            ), // Malediction (Ceptor)
        ];
        let result = select_top_entries(input, 2);
        // Dictor wins the remaining group slot (541 has display priority over 324 on
        // a count tie); Malediction claims the guaranteed type slot; AF is displaced.
        // Final order by parent group's GROUP_NAMES position: Dictor (541) before
        // Ceptor's position (831).
        assert_eq!(
            result,
            vec![
                (TallyKey::Group(541), 2),
                (
                    TallyKey::Type {
                        type_id: 11186,
                        parent_group: 831
                    },
                    1
                ),
            ]
        );
    }

    #[test]
    fn select_top_entries_orders_multiple_type_entries_by_count_then_type_id() {
        let input = vec![
            (
                TallyKey::Type {
                    type_id: 22456,
                    parent_group: 541,
                },
                1,
            ),
            (
                TallyKey::Type {
                    type_id: 11393,
                    parent_group: 324,
                },
                3,
            ),
            (TallyKey::Group(832), 10),
        ];
        // Both type entries claim slots (limit 2) ahead of the much larger Logi group;
        // final display order follows each entry's parent group's GROUP_NAMES position
        // (Dictor 541 before AF 324), not the count-based selection order.
        let result = select_top_entries(input, 2);
        assert_eq!(
            result,
            vec![
                (
                    TallyKey::Type {
                        type_id: 22456,
                        parent_group: 541
                    },
                    1
                ),
                (
                    TallyKey::Type {
                        type_id: 11393,
                        parent_group: 324
                    },
                    3
                ),
            ]
        );
    }

    #[test]
    fn contract_embed_keeps_the_contract_address_as_its_own_field() {
        let embed = contract_notification_embed(&ContractNotificationMessage {
            title: "Public contract listed".to_string(),
            description: Some("@\u{200b}everyone cannot ping".to_string()),
            fields: vec![crate::contract_intelligence::ContractEmbedField {
                name: "Contract Address".to_string(),
                value: "contract:0//45".to_string(),
                inline: false,
            }],
            presentation_revision:
                crate::contract_intelligence::CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
            author: None,
            thumbnail_url: None,
            footer: None,
            timestamp: None,
        });

        assert_eq!(embed.0["title"].as_str(), Some("Public contract listed"));
        assert_eq!(
            embed.0["description"].as_str(),
            Some("@\u{200b}everyone cannot ping")
        );
        let fields = embed.0["fields"].as_array().expect("embed fields");
        assert_eq!(fields[0]["name"].as_str(), Some("Contract Address"));
        assert_eq!(fields[0]["value"].as_str(), Some("contract:0//45"));
    }

    #[test]
    fn contract_delivery_nonce_and_allowed_mentions_are_explicit() {
        let message = ContractNotificationMessage {
            title: "@everyone cannot notify anyone".to_string(),
            description: None,
            fields: vec![],
            presentation_revision:
                crate::contract_intelligence::CONTRACT_NOTIFICATION_PRESENTATION_REVISION,
            author: None,
            thumbnail_url: None,
            footer: None,
            timestamp: None,
        };
        let delivery = PreparedContractDelivery {
            delivery_id: 1,
            guild_id: 2,
            channel_id: 3,
            subscription_id: "ships".to_string(),
            contract_id: 4,
            event_kind: crate::contract_intelligence::ContractEventKind::Listed,
            ping: true,
            ping_type: crate::contract_intelligence::ContractPingType::Here,
            nonce: "ci-1".to_string(),
            enforce_nonce: true,
            message: message.clone(),
            delivery_claim_token: None,
        };
        let mut ping_builder = CreateMessage::default();
        configure_contract_delivery_message(&mut ping_builder, &delivery);
        assert_eq!(ping_builder.0["content"].as_str(), Some("@here"));
        assert_eq!(ping_builder.0["nonce"].as_str(), Some("ci-1"));
        assert_eq!(ping_builder.0["enforce_nonce"].as_bool(), Some(true));
        assert_eq!(
            ping_builder.0["allowed_mentions"]["parse"],
            serde_json::json!(["everyone"])
        );

        let mut non_pinging_builder = CreateMessage::default();
        configure_contract_delivery_message(
            &mut non_pinging_builder,
            &PreparedContractDelivery {
                ping: false,
                enforce_nonce: false,
                ..delivery.clone()
            },
        );
        assert!(non_pinging_builder.0.get("content").is_none());
        assert_eq!(
            non_pinging_builder.0["enforce_nonce"].as_bool(),
            Some(false)
        );
        assert_eq!(
            non_pinging_builder.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );

        let mut everyone_builder = CreateMessage::default();
        configure_contract_delivery_message(
            &mut everyone_builder,
            &PreparedContractDelivery {
                ping_type: crate::contract_intelligence::ContractPingType::Everyone,
                ..delivery
            },
        );
        assert_eq!(everyone_builder.0["content"].as_str(), Some("@everyone"));
        assert_eq!(
            everyone_builder.0["allowed_mentions"]["parse"],
            serde_json::json!(["everyone"])
        );
    }

    fn discord_http_error(
        status_code: serenity::http::StatusCode,
        code: isize,
        message: &str,
    ) -> serenity::Error {
        serenity::Error::Http(Box::new(serenity::http::error::Error::UnsuccessfulRequest(
            serenity::http::error::ErrorResponse {
                status_code,
                url: url::Url::parse("https://discord.com/api/v10/channels/3/messages")
                    .expect("valid Discord API URL"),
                error: serde_json::from_value(serde_json::json!({
                    "code": code,
                    "message": message,
                }))
                .expect("valid Discord JSON error"),
            },
        )))
    }

    #[test]
    fn contract_delivery_http_errors_keep_temporary_discord_errors_prepared() {
        assert!(matches!(
            contract_delivery_error(discord_http_error(
                serenity::http::StatusCode::BAD_REQUEST,
                40004,
                "Send messages has been temporarily disabled",
            )),
            ContractDeliveryError::Transient(_)
        ));
        assert!(matches!(
            contract_delivery_error(discord_http_error(
                serenity::http::StatusCode::FORBIDDEN,
                50013,
                "Missing Permissions",
            )),
            ContractDeliveryError::Permanent(_)
        ));
        assert!(matches!(
            contract_delivery_error(discord_http_error(
                serenity::http::StatusCode::NOT_FOUND,
                10008,
                "Unknown Message",
            )),
            ContractDeliveryError::Permanent(_)
        ));
        assert!(matches!(
            contract_delivery_error(discord_http_error(
                serenity::http::StatusCode::BAD_REQUEST,
                0,
                "General error",
            )),
            ContractDeliveryError::Ambiguous(_)
        ));
        assert!(matches!(
            contract_delivery_error(discord_http_error(
                serenity::http::StatusCode::TOO_MANY_REQUESTS,
                0,
                "You are being rate limited.",
            )),
            ContractDeliveryError::Transient(_)
        ));
    }

    #[test]
    fn health_messages_use_channel_scoped_nonces_and_only_the_configured_incident_mention() {
        let mut view = CreateMessage::default();
        configure_health_view_message(&mut view, 444, "Health: degraded");
        assert_eq!(view.0["allowed_mentions"]["parse"], serde_json::json!([]));
        assert_eq!(view.0["nonce"], "hwv-444");
        assert_eq!(view.0["enforce_nonce"], true);

        let mut incident = CreateMessage::default();
        configure_health_incident_message(
            &mut incident,
            444,
            "Health incident: critical",
            Some(crate::commands::health::HEALTH_OPERATOR_ID),
        );
        assert_eq!(
            incident.0["content"],
            format!(
                "<@{}>\nHealth incident: critical",
                crate::commands::health::HEALTH_OPERATOR_ID
            )
        );
        assert_eq!(
            incident.0["allowed_mentions"]["parse"],
            serde_json::json!([])
        );
        assert_eq!(
            incident.0["allowed_mentions"]["users"],
            serde_json::json!([crate::commands::health::HEALTH_OPERATOR_ID.to_string()])
        );
        assert_eq!(incident.0["nonce"], "hwi-444");
        assert_eq!(incident.0["enforce_nonce"], true);

        let mut remediated_view = CreateMessage::default();
        configure_health_view_message(&mut remediated_view, 555, "Health: degraded");
        let mut remediated_incident = CreateMessage::default();
        configure_health_incident_message(
            &mut remediated_incident,
            555,
            "Health incident: critical",
            None,
        );
        assert_eq!(remediated_view.0["nonce"], "hwv-555");
        assert_eq!(remediated_incident.0["nonce"], "hwi-555");
        assert_ne!(view.0["nonce"], remediated_view.0["nonce"]);
        assert_ne!(incident.0["nonce"], remediated_incident.0["nonce"]);
        assert!(view.0["nonce"]
            .as_str()
            .is_some_and(|nonce| nonce.len() <= 25));
        assert!(incident.0["nonce"]
            .as_str()
            .is_some_and(|nonce| nonce.len() <= 25));
    }
}
