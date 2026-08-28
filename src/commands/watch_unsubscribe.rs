//! `/watch_unsubscribe`: remove a watchlist subscription from this channel
//! (ticket 08).

use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::watchlist_feed::available_watchlist_store;
use crate::WatchlistStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct WatchUnsubscribeCommand;

#[async_trait]
impl Command for WatchUnsubscribeCommand {
    fn name(&self) -> String {
        "watch_unsubscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("watch_unsubscribe")
            .description("Remove a watchlist subscription from this channel.")
            .create_option(|option| {
                option
                    .name("name")
                    .description("Stable subscription name to remove.")
                    .kind(CommandOptionType::String)
                    .required(true)
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
        let removal = match (
            command.guild_id,
            get_option_value(&command.data.options, "name"),
        ) {
            (Some(guild_id), Some(CommandDataOptionValue::String(name))) => {
                Ok((guild_id.0, command.channel_id.0, name.to_string()))
            }
            (None, _) => Err("Watchlist subscriptions can only be removed in a server channel."),
            _ => Err("Invalid watchlist subscription name."),
        };
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let (guild_id, channel_id, name) = match removal {
                Ok(removal) => removal,
                Err(response) => return response.to_string(),
            };
            let Some(store_handle) = store_handle else {
                return "Watchlist subscriptions are unavailable because the watchlist database is not connected."
                    .to_string();
            };
            let Some(store) = available_watchlist_store(&store_handle).await else {
                return "Watchlist subscriptions are unavailable because the watchlist database is not connected."
                    .to_string();
            };
            match store.remove_subscription(guild_id, channel_id, &name).await {
                Ok(true) => format!("Watchlist subscription '{name}' removed from this channel."),
                Ok(false) => format!("No watchlist subscription '{name}' exists in this channel."),
                Err(error) => {
                    error!("cannot remove watchlist subscription: {error}");
                    "Watchlist subscription could not be removed.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to watch unsubscribe command: {error}");
        }
    }
}
