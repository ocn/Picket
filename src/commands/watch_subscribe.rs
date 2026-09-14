//! `/watch_subscribe`: create or replace a watchlist subscription for this
//! channel, choosing which event kinds to receive and an optional role ping
//! (ticket 08).

use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::watchlist_feed::{available_watchlist_store, parse_event_kinds, WatchlistSubscription};
use crate::WatchlistStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::model::Permissions;
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct WatchSubscribeCommand;

impl WatchSubscribeCommand {
    fn subscription(
        guild_id: u64,
        channel_id: u64,
        command: &ApplicationCommandInteraction,
    ) -> Result<WatchlistSubscription, String> {
        let name = match get_option_value(&command.data.options, "name") {
            Some(CommandDataOptionValue::String(value)) => value.as_str(),
            _ => return Err("missing required option: name".to_string()),
        };
        let raw_kinds = match get_option_value(&command.data.options, "event_kinds") {
            Some(CommandDataOptionValue::String(value)) => value.as_str(),
            None => "",
            _ => return Err("event_kinds must be a string option".to_string()),
        };
        let role_id = match get_option_value(&command.data.options, "role") {
            Some(CommandDataOptionValue::Role(role)) => Some(role.id.0),
            None => None,
            _ => return Err("role must be a role option".to_string()),
        };
        // Ticket 20: an optional direct user ping beside the role. `/watch_subscribe`
        // replaces a subscription wholesale, so `ping_user_id` is re-supplied on
        // every invocation (there is no partial re-subscribe / carry-forward here,
        // unlike `/sov_subscribe`); omitting `user` clears any prior user ping.
        let ping_user_id = match get_option_value(&command.data.options, "user") {
            Some(CommandDataOptionValue::User(user, _member)) => Some(user.id.0),
            None => None,
            _ => return Err("user must be a user option".to_string()),
        };
        let event_kinds = parse_event_kinds(raw_kinds)
            .map_err(|error| format!("invalid event_kinds: {error}"))?;
        let subscription = WatchlistSubscription {
            guild_id,
            channel_id,
            name: name.to_string(),
            event_kinds,
            role_id,
            ping_user_id,
            options: serde_json::json!({}),
        };
        subscription.validate()?;
        Ok(subscription)
    }
}

#[async_trait]
impl Command for WatchSubscribeCommand {
    fn name(&self) -> String {
        "watch_subscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("watch_subscribe")
            .default_member_permissions(Permissions::MANAGE_GUILD)
            .dm_permission(false)
            .description("Subscribe this channel to watchlist corporation join/leave alerts.")
            .create_option(|option| {
                option
                    .name("name")
                    .description("Stable subscription name for this channel.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("event_kinds")
                    .description("Comma list incl. corp_joined/left, member_delta, war_declared/ally_joined/retracted/finished.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("role")
                    .description("Role to ping on alerts from this subscription.")
                    .kind(CommandOptionType::Role)
            })
            .create_option(|option| {
                option
                    .name("user")
                    .description("User to ping directly on alerts from this subscription.")
                    .kind(CommandOptionType::User)
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        _app_state: &Arc<AppState>,
    ) {
        let store_handle = ctx
            .data
            .read()
            .await
            .get::<WatchlistStoreContainer>()
            .cloned();
        let response = match command.guild_id {
            Some(guild_id) => Self::subscription(guild_id.0, command.channel_id.0, command)
                .map_err(|error| format!("Invalid watchlist subscription: {error}")),
            None => {
                Err("Watchlist subscriptions can only be created in a server channel.".to_string())
            }
        };
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let subscription = match response {
                Ok(subscription) => subscription,
                Err(response) => return response,
            };
            let Some(store_handle) = store_handle else {
                return "Watchlist subscriptions are unavailable because the watchlist database is not connected."
                    .to_string();
            };
            let Some(store) = available_watchlist_store(&store_handle).await else {
                return "Watchlist subscriptions are unavailable because the watchlist database is not connected."
                    .to_string();
            };
            match store.upsert_subscription(&subscription).await {
                Ok(()) => format!(
                    "Watchlist subscription '{}' saved for this channel.",
                    subscription.name
                ),
                Err(error) => {
                    error!("cannot persist watchlist subscription: {error}");
                    "Watchlist subscription could not be saved.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to watch subscribe command: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_subscribe_options_have_bounded_descriptions() {
        let mut command = CreateApplicationCommand::default();
        WatchSubscribeCommand.register(&mut command);
        let options = command.0["options"]
            .as_array()
            .expect("watch subscribe options");
        assert!(options.len() <= 25);
        for option in options {
            let description = option["description"]
                .as_str()
                .expect("every option has a description");
            assert!(
                description.len() <= 100,
                "option '{}' description is {} chars",
                option["name"],
                description.len()
            );
        }
    }
}
