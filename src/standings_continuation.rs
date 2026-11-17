use crate::commands::sync_standings::SyncStandingsCommand;
use crate::config::{
    save_user_standings, AppState, Filter, FilterNode, SimpleFilter, SsoState, StandingSource,
    Subscription,
};
use crate::write_authorization::{
    ContinuationAction, ContinuationAdapter, MemberIdentity, SubscriptionBinding,
    CONTINUATION_DENIED, WRITE_DENIED,
};
use async_trait::async_trait;
use serenity::model::application::interaction::message_component::MessageComponentInteraction;
use serenity::model::application::interaction::InteractionResponseType;
use serenity::model::channel::Message;
use serenity::model::id::{GuildId, UserId};
use serenity::prelude::Context;
use std::collections::HashMap;
use std::sync::Arc;

pub enum ContinuationTransport<'a> {
    Component(&'a MessageComponentInteraction),
    DirectMessage(&'a Message),
}

pub struct SerenityContinuationAdapter<'a> {
    pub ctx: &'a Context,
    pub app_state: &'a Arc<AppState>,
    pub transport: ContinuationTransport<'a>,
}

impl SerenityContinuationAdapter<'_> {
    async fn respond(&self, content: &str) {
        let failed = match self.transport {
            ContinuationTransport::Component(request) => request
                .edit_original_interaction_response(&self.ctx.http, |response| {
                    response.content(content)
                })
                .await
                .is_err(),
            ContinuationTransport::DirectMessage(request) => request
                .channel_id
                .say(&self.ctx.http, content)
                .await
                .is_err(),
        };
        if failed {
            tracing::warn!(reason = "response_failed", "standings_continuation");
        }
    }
}

#[async_trait]
impl ContinuationAdapter for SerenityContinuationAdapter<'_> {
    async fn acknowledge(&mut self) -> bool {
        let ContinuationTransport::Component(request) = self.transport else {
            return true;
        };
        let acknowledged = request
            .create_interaction_response(&self.ctx.http, |response| {
                response
                    .kind(InteractionResponseType::DeferredChannelMessageWithSource)
                    .interaction_response_data(|message| message.ephemeral(true))
            })
            .await
            .is_ok();
        if !acknowledged {
            tracing::warn!(reason = "acknowledgement_failed", "standings_continuation");
        }
        acknowledged
    }

    async fn deny(&mut self) {
        let content = match self.transport {
            ContinuationTransport::Component(request) if request.guild_id.is_some() => WRITE_DENIED,
            _ => CONTINUATION_DENIED,
        };
        self.respond(content).await;
    }

    async fn member(&mut self, guild_id: u64, user_id: u64) -> Result<MemberIdentity, ()> {
        self.ctx
            .http
            .get_member(guild_id, user_id)
            .await
            .map(|member| MemberIdentity {
                guild_id: member.guild_id.0,
                user_id: member.user.id.0,
                role_ids: member.roles.iter().map(|role| role.0).collect(),
            })
            .map_err(|_| ())
    }

    fn target_exists(&mut self, binding: &SubscriptionBinding) -> bool {
        self.app_state
            .subscriptions
            .read()
            .unwrap()
            .get(&GuildId(binding.guild_id))
            .is_some_and(|subscriptions| {
                subscriptions.iter().any(|subscription| {
                    subscription.id == binding.subscription_id
                        && subscription.action.channel_id == binding.channel_id.to_string()
                })
            })
    }

    fn owns_character(&mut self, user_id: u64, character_id: u64) -> bool {
        self.app_state
            .user_standings
            .read()
            .unwrap()
            .get(&UserId(user_id))
            .is_some_and(|standings| {
                standings
                    .tokens
                    .iter()
                    .any(|token| token.character_id == character_id)
            })
    }

    async fn work(&mut self, state: Arc<SsoState>, action: ContinuationAction) -> String {
        match action {
            ContinuationAction::Reauthorize { state: key } => {
                if SyncStandingsCommand
                    .initiate_sso(self.ctx, &state.original_interaction, self.app_state, &key)
                    .await
                {
                    "A new authorization link has been sent to your DMs.".to_string()
                } else {
                    "Unable to send an authorization link. Please try the command again."
                        .to_string()
                }
            }
            ContinuationAction::Select { character_id } => {
                select_character(self.app_state, &state, character_id).await
            }
            ContinuationAction::Complete { code } => {
                complete_oauth(self.app_state, &state, &code).await
            }
        }
    }

    async fn finish(&mut self, message: &str) {
        self.respond(message).await;
    }
}

pub fn apply_standings_to_bound_subscription(
    subscriptions: &mut HashMap<GuildId, Vec<Subscription>>,
    state: &SsoState,
    source_entity_id: u64,
) -> bool {
    let Some(guild_id) = state.original_interaction.guild_id else {
        return false;
    };
    let Some(subscription) = subscriptions.get_mut(&guild_id).and_then(|subscriptions| {
        subscriptions.iter_mut().find(|subscription| {
            subscription.id == state.subscription_id
                && subscription.action.channel_id
                    == state.original_interaction.channel_id.to_string()
        })
    }) else {
        return false;
    };
    let new_filter = FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding {
        synched_by_user_id: state.discord_user_id.0,
        source: state.standing_source,
        source_entity_id,
    }));
    match &mut subscription.root_filter {
        FilterNode::And(conditions) => {
            conditions.retain(|condition| {
                !matches!(
                    condition,
                    FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding { .. }))
                )
            });
            conditions.push(new_filter);
        }
        FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding { .. })) => {
            subscription.root_filter = new_filter
        }
        _ => {
            subscription.root_filter =
                FilterNode::And(vec![subscription.root_filter.clone(), new_filter])
        }
    }
    true
}

async fn update_subscription(
    app_state: &AppState,
    state: &SsoState,
    source_entity_id: u64,
) -> bool {
    let _lock = app_state.subscriptions_file_lock.lock().await;
    let mut subscriptions = app_state.subscriptions.write().unwrap();
    if !apply_standings_to_bound_subscription(&mut subscriptions, state, source_entity_id) {
        return false;
    }
    let Some(guild_id) = state.original_interaction.guild_id else {
        return false;
    };
    let saved =
        crate::config::save_subscriptions_for_guild(guild_id, &subscriptions[&guild_id]).is_ok();
    if !saved {
        tracing::error!(
            reason = "subscription_save_failed",
            "standings_continuation"
        );
    }
    saved
}

fn source_entity(
    state: &SsoState,
    character_id: u64,
    corporation_id: u64,
    alliance_id: Option<u64>,
) -> u64 {
    match state.standing_source {
        StandingSource::Character => character_id,
        StandingSource::Corporation => corporation_id,
        StandingSource::Alliance => alliance_id.unwrap_or(corporation_id),
    }
}

async fn select_character(app_state: &AppState, state: &SsoState, character_id: u64) -> String {
    let token = app_state
        .user_standings
        .read()
        .unwrap()
        .get(&state.discord_user_id)
        .and_then(|standings| {
            standings
                .tokens
                .iter()
                .find(|token| token.character_id == character_id)
                .cloned()
        });
    let Some(token) = token else {
        return CONTINUATION_DENIED.to_string();
    };
    let Ok((corporation, alliance)) = app_state
        .esi_client
        .get_character_affiliation(token.character_id)
        .await
    else {
        tracing::warn!(reason = "affiliation_failed", "standings_continuation");
        return "Failed to fetch character affiliation. Please try the command again.".to_string();
    };
    if update_subscription(
        app_state,
        state,
        source_entity(state, token.character_id, corporation, alliance),
    )
    .await
    {
        format!(
            "Subscription '{}' updated successfully.",
            state.subscription_id
        )
    } else {
        "No matching subscription found. Please try again.".to_string()
    }
}

async fn complete_oauth(app_state: &AppState, state: &SsoState, code: &str) -> String {
    let token = app_state
        .esi_client
        .exchange_code_for_token(
            code,
            &app_state.app_config.eve_client_id,
            &app_state.app_config.eve_client_secret,
        )
        .await;
    let Ok(mut token) = token else {
        tracing::warn!(reason = "oauth_exchange_failed", "standings_continuation");
        return "Failed to authenticate with EVE SSO. Please try again.".to_string();
    };
    let Ok((corporation, alliance)) = app_state
        .esi_client
        .get_character_affiliation(token.character_id)
        .await
    else {
        tracing::warn!(reason = "affiliation_failed", "standings_continuation");
        return "Failed to fetch character affiliation. Aborting.".to_string();
    };
    let source_id = source_entity(state, token.character_id, corporation, alliance);
    let source_kind = match state.standing_source {
        StandingSource::Character => "characters",
        StandingSource::Corporation => "corporations",
        StandingSource::Alliance => "alliances",
    };
    let contacts = app_state
        .esi_client
        .get_contacts(source_id, &token.access_token, source_kind)
        .await
        .unwrap_or_default();
    let character_name = token.character_name.clone();
    token.corporation_id = corporation;
    token.alliance_id = alliance;
    {
        let _lock = app_state.user_standings_file_lock.lock().await;
        let mut standings = app_state.user_standings.write().unwrap();
        let user = standings.entry(state.discord_user_id).or_default();
        user.tokens
            .retain(|saved| saved.character_id != token.character_id);
        user.tokens.push(token);
        user.contact_lists.contacts.insert(source_id, contacts);
        save_user_standings(&standings);
    }
    if update_subscription(app_state, state, source_id).await {
        format!(
            "Subscription '{}' has been successfully synced with {}'s standings.",
            state.subscription_id, character_name
        )
    } else {
        "No matching subscription found. Please try again.".to_string()
    }
}
