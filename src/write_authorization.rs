use crate::config::SsoState;
use crate::write_policy::WritePolicy;
use async_trait::async_trait;
use serenity::model::application::command::CommandOptionType;
use serenity::model::application::component::ComponentType;
use serenity::model::application::interaction::application_command::ApplicationCommandInteraction;
use serenity::model::application::interaction::message_component::MessageComponentInteraction;
use serenity::model::channel::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub const WRITE_DENIED: &str = "Not authorized to change Picket in this server.";
pub const CONTINUATION_DENIED: &str =
    "Unable to complete this request. Please try the command again.";

#[async_trait]
pub trait SlashAdapter: Send {
    async fn deny(&mut self);
    async fn work(&mut self, request: &ApplicationCommandInteraction);
}

#[derive(Clone, Copy)]
enum Access {
    Query,
    Write,
    OperatorQuery,
}

fn operation(request: &ApplicationCommandInteraction) -> Option<(&'static str, Access)> {
    if request.kind
        != serenity::model::application::interaction::InteractionType::ApplicationCommand
        || request.data.kind != serenity::model::application::command::CommandType::ChatInput
    {
        return None;
    }
    match request.data.name.as_str() {
        "ping" => Some(("ping", Access::Query)),
        "diag" => Some(("diag", Access::Query)),
        "health" => Some(("health", Access::OperatorQuery)),
        "sov_timers" => Some(("sov_timers", Access::Query)),
        "find_unsubscribed" => Some(("find_unsubscribed", Access::Query)),
        "subscribe" => Some(("subscribe", Access::Write)),
        "unsubscribe" => Some(("unsubscribe", Access::Write)),
        "contract_subscribe" => Some(("contract_subscribe", Access::Write)),
        "contract_unsubscribe" => Some(("contract_unsubscribe", Access::Write)),
        "sov_subscribe" => Some(("sov_subscribe", Access::Write)),
        "sov_unsubscribe" => Some(("sov_unsubscribe", Access::Write)),
        "watch_subscribe" => Some(("watch_subscribe", Access::Write)),
        "watch_unsubscribe" => Some(("watch_unsubscribe", Access::Write)),
        "sync_standings" => Some(("sync_standings", Access::Write)),
        "sync_remove" => Some(("sync_remove", Access::Write)),
        "sync_clear" => Some(("sync_clear", Access::Write)),
        "watch" => {
            let [option] = request.data.options.as_slice() else {
                return None;
            };
            if option.kind != CommandOptionType::SubCommand
                || option.value.is_some()
                || option.resolved.is_some()
                || option.options.len() > 25
                || option.options.iter().any(|child| {
                    matches!(
                        child.kind,
                        CommandOptionType::SubCommand | CommandOptionType::SubCommandGroup
                    )
                })
            {
                return None;
            }
            match option.name.as_str() {
                "add" => Some(("watch_add", Access::Write)),
                "remove" => Some(("watch_remove", Access::Write)),
                "list" => Some(("watch_list", Access::Query)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn guild_writer(policy: &WritePolicy, request: &ApplicationCommandInteraction) -> bool {
    let (Some(guild), Some(member)) = (request.guild_id, request.member.as_ref()) else {
        return false;
    };
    guild.0 != 0
        && request.user.id.0 != 0
        && member.guild_id == guild
        && member.user.id == request.user.id
        && policy.allows(
            guild.0,
            request.user.id.0,
            &member.roles.iter().map(|id| id.0).collect::<Vec<_>>(),
        )
}

pub async fn dispatch_slash<A: SlashAdapter>(
    policy: &WritePolicy,
    request: &ApplicationCommandInteraction,
    adapter: &mut A,
) {
    let classification = operation(request);
    let (action, allowed) = match classification {
        Some((action, Access::Query)) => (action, true),
        Some((action, Access::OperatorQuery)) => (
            action,
            request.user.id.0 == crate::commands::health::HEALTH_OPERATOR_ID,
        ),
        Some((action, Access::Write)) => (action, guild_writer(policy, request)),
        None => ("unknown", false),
    };
    tracing::info!(
        actor = request.user.id.0,
        guild = request.guild_id.map(|id| id.0),
        action,
        reason = if allowed { "allowed" } else { "denied" },
        "write_authorization"
    );
    if allowed {
        adapter.work(request).await;
    } else {
        adapter.deny().await;
    }
}

pub type PendingStates = Arc<Mutex<HashMap<String, Arc<SsoState>>>>;

#[derive(Clone)]
pub struct MemberIdentity {
    pub guild_id: u64,
    pub user_id: u64,
    pub role_ids: Vec<u64>,
}

pub struct SubscriptionBinding {
    pub guild_id: u64,
    pub channel_id: u64,
    pub subscription_id: String,
}

pub enum ContinuationAction {
    Complete { code: String },
    Select { character_id: u64 },
    Reauthorize { state: String },
}

enum PendingAction<'a> {
    Complete(url::Url),
    Select(&'a [String]),
    Reauthorize(&'a [String]),
}

impl PendingAction<'_> {
    fn name(&self) -> &'static str {
        match self {
            Self::Complete(_) => "standings_callback",
            Self::Select(_) => "standings_select",
            Self::Reauthorize(_) => "standings_reauth",
        }
    }

    fn authorized(self, key: &str) -> Option<ContinuationAction> {
        match self {
            Self::Select(values) => {
                let [value] = values else {
                    return None;
                };
                Some(ContinuationAction::Select {
                    character_id: positive_id(value)?,
                })
            }
            Self::Reauthorize(values) => {
                values.is_empty().then(|| ContinuationAction::Reauthorize {
                    state: key.to_string(),
                })
            }
            Self::Complete(url) => {
                let pairs = url.query_pairs().collect::<Vec<_>>();
                if pairs.len() != 2 {
                    return None;
                }
                let code = pairs.iter().find(|(name, _)| name == "code")?.1.as_ref();
                if code.is_empty() || code.len() > 1500 {
                    return None;
                }
                Some(ContinuationAction::Complete {
                    code: code.to_string(),
                })
            }
        }
    }
}

#[async_trait]
pub trait ContinuationAdapter: Send {
    async fn acknowledge(&mut self) -> bool;
    async fn deny(&mut self);
    async fn member(&mut self, guild_id: u64, user_id: u64) -> Result<MemberIdentity, ()>;
    fn target_exists(&mut self, binding: &SubscriptionBinding) -> bool;
    fn owns_character(&mut self, user_id: u64, character_id: u64) -> bool;
    async fn work(&mut self, state: Arc<SsoState>, action: ContinuationAction) -> String;
    async fn finish(&mut self, message: &str);
}

fn valid_state(state: &str) -> bool {
    state.len() == 16 && state.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn positive_id(value: &str) -> Option<u64> {
    if value.len() > 20
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok().filter(|id| *id != 0)
}

fn component_action(request: &MessageComponentInteraction) -> Option<(&str, PendingAction<'_>)> {
    if request.kind != serenity::model::application::interaction::InteractionType::MessageComponent
    {
        return None;
    }
    if let Some(state) = request.data.custom_id.strip_prefix("standings_select_") {
        if !valid_state(state) || request.data.component_type != ComponentType::SelectMenu {
            return None;
        }
        Some((state, PendingAction::Select(&request.data.values)))
    } else {
        let state = request.data.custom_id.strip_prefix("standings_reauth_")?;
        if !valid_state(state) || request.data.component_type != ComponentType::Button {
            return None;
        }
        Some((state, PendingAction::Reauthorize(&request.data.values)))
    }
}

fn callback_action(content: &str) -> Option<(String, PendingAction<'_>)> {
    if content.len() > 2000 {
        return None;
    }
    let url = url::Url::parse(content).ok()?;
    if url.scheme() != "https"
        || url.host_str() != Some("github.headempty.space")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.fragment().is_some()
    {
        return None;
    }
    let states = url
        .query_pairs()
        .filter(|(name, _)| name == "state")
        .map(|(_, state)| state.into_owned())
        .collect::<Vec<_>>();
    let [state] = states.as_slice() else {
        return None;
    };
    if !valid_state(state) {
        return None;
    }
    Some((state.clone(), PendingAction::Complete(url)))
}

async fn reject<A: ContinuationAdapter>(
    adapter: &mut A,
    actor: u64,
    guild: Option<u64>,
    action: &'static str,
    reason: &'static str,
) {
    tracing::info!(actor, guild, action, reason, "write_authorization");
    adapter.deny().await;
}

pub async fn dispatch_component<A: ContinuationAdapter>(
    policy: &WritePolicy,
    states: &PendingStates,
    request: &MessageComponentInteraction,
    adapter: &mut A,
) {
    if !adapter.acknowledge().await {
        return;
    }
    let identity_matches = match (request.guild_id, request.member.as_ref()) {
        (None, None) => true,
        (Some(guild), Some(member)) => {
            member.guild_id == guild && member.user.id == request.user.id
        }
        _ => false,
    };
    if !identity_matches {
        reject(
            adapter,
            request.user.id.0,
            request.guild_id.map(|id| id.0),
            "unknown",
            "identity_mismatch",
        )
        .await;
        return;
    }
    let Some((key, action)) = component_action(request) else {
        reject(
            adapter,
            request.user.id.0,
            request.guild_id.map(|id| id.0),
            "unknown",
            "malformed",
        )
        .await;
        return;
    };
    continue_request(
        policy,
        states,
        ContinuationRequest {
            actor: request.user.id.0,
            request_guild: request.guild_id.map(|id| id.0),
            request_channel: Some(request.channel_id.0),
            key,
            action,
        },
        adapter,
    )
    .await;
}

pub async fn dispatch_dm<A: ContinuationAdapter>(
    policy: &WritePolicy,
    states: &PendingStates,
    request: &Message,
    adapter: &mut A,
) {
    if request.author.bot
        || request.guild_id.is_some()
        || !request
            .content
            .starts_with("https://github.headempty.space/")
    {
        return;
    }
    let Some((key, action)) = callback_action(&request.content) else {
        reject(
            adapter,
            request.author.id.0,
            None,
            "standings_callback",
            "malformed",
        )
        .await;
        return;
    };
    continue_request(
        policy,
        states,
        ContinuationRequest {
            actor: request.author.id.0,
            request_guild: None,
            request_channel: None,
            key: &key,
            action,
        },
        adapter,
    )
    .await;
}

struct ContinuationRequest<'a> {
    actor: u64,
    request_guild: Option<u64>,
    request_channel: Option<u64>,
    key: &'a str,
    action: PendingAction<'a>,
}

async fn continue_request<A: ContinuationAdapter>(
    policy: &WritePolicy,
    states: &PendingStates,
    request: ContinuationRequest<'_>,
    adapter: &mut A,
) {
    let ContinuationRequest {
        actor,
        request_guild,
        request_channel,
        key,
        action,
    } = request;
    let action_name = action.name();
    let snapshot = states.lock().await.get(key).cloned();
    let Some(snapshot) = snapshot else {
        reject(adapter, actor, None, action_name, "invalid_continuation").await;
        return;
    };
    if actor == 0
        || snapshot.discord_user_id.0 != actor
        || snapshot.original_interaction.user.id.0 != actor
    {
        reject(adapter, actor, None, action_name, "invalid_continuation").await;
        return;
    }
    let Some(guild) = snapshot
        .original_interaction
        .guild_id
        .map(|id| id.0)
        .filter(|id| *id != 0)
    else {
        reject(adapter, actor, None, action_name, "invalid_binding").await;
        return;
    };
    let channel = snapshot.original_interaction.channel_id.0;
    let binding = SubscriptionBinding {
        guild_id: guild,
        channel_id: channel,
        subscription_id: snapshot.subscription_id.clone(),
    };
    if channel == 0
        || snapshot.subscription_id.is_empty()
        || request_guild.is_some_and(|id| id != guild || request_channel != Some(channel))
        || !adapter.target_exists(&binding)
    {
        reject(adapter, actor, Some(guild), action_name, "invalid_binding").await;
        return;
    }
    let member = tokio::time::timeout(Duration::from_secs(2), adapter.member(guild, actor)).await;
    let allowed = matches!(member, Ok(Ok(ref member)) if member.guild_id == guild && member.user_id == actor && policy.allows(guild, actor, &member.role_ids));
    if !allowed {
        reject(
            adapter,
            actor,
            Some(guild),
            action_name,
            "membership_or_grant_denied",
        )
        .await;
        return;
    }
    if !adapter.target_exists(&binding) {
        reject(adapter, actor, Some(guild), action_name, "invalid_binding").await;
        return;
    }
    let Some(action) = action.authorized(key) else {
        reject(adapter, actor, Some(guild), action_name, "malformed").await;
        return;
    };
    if let ContinuationAction::Select { character_id } = &action {
        if !adapter.owns_character(actor, *character_id) {
            reject(
                adapter,
                actor,
                Some(guild),
                action_name,
                "invalid_character",
            )
            .await;
            return;
        }
    }
    let claimed = {
        let mut pending = states.lock().await;
        if pending
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, &snapshot))
        {
            if !matches!(action, ContinuationAction::Reauthorize { .. }) {
                pending.remove(key);
            }
            true
        } else {
            false
        }
    };
    if !claimed {
        reject(adapter, actor, Some(guild), action_name, "already_claimed").await;
        return;
    }
    tracing::info!(
        actor,
        guild,
        action = action_name,
        reason = "allowed",
        "write_authorization"
    );
    let result = adapter.work(snapshot, action).await;
    adapter.finish(&result).await;
}

pub fn initialize_with_policy<T>(
    path: &std::path::Path,
    state_dir: &std::path::Path,
    initialize: impl FnOnce(Arc<WritePolicy>) -> T,
) -> T {
    let policy = WritePolicy::load(path, state_dir).unwrap_or_else(|reason| {
        tracing::error!(%reason, "write_policy_load_failed; user writes disabled");
        WritePolicy::default()
    });
    initialize(Arc::new(policy))
}

pub struct SerenitySlashAdapter<'a> {
    pub ctx: &'a serenity::prelude::Context,
    pub app_state: &'a Arc<crate::config::AppState>,
    pub commands: &'a HashMap<String, Box<dyn crate::commands::Command>>,
    pub request: &'a ApplicationCommandInteraction,
}

#[async_trait]
impl SlashAdapter for SerenitySlashAdapter<'_> {
    async fn deny(&mut self) {
        if self
            .request
            .create_interaction_response(&self.ctx.http, |response| {
                response.interaction_response_data(|message| {
                    message.content(WRITE_DENIED).ephemeral(true)
                })
            })
            .await
            .is_err()
        {
            tracing::warn!(reason = "response_failed", "write_authorization_transport");
        }
    }

    async fn work(&mut self, request: &ApplicationCommandInteraction) {
        if let Some(command) = self.commands.get(&request.data.name) {
            command.execute(self.ctx, request, self.app_state).await;
        } else {
            self.deny().await;
        }
    }
}
