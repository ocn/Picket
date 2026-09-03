use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::sov_feed::available_sov_store;
use crate::SovStoreContainer;
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

pub struct SovUnsubscribeCommand;

#[async_trait]
impl Command for SovUnsubscribeCommand {
    fn name(&self) -> String {
        "sov_unsubscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sov_unsubscribe")
            .default_member_permissions(Permissions::MANAGE_GUILD)
            .dm_permission(false)
            .description("Remove a sovereignty campaign subscription from this channel.")
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
        let store_handle = ctx.data.read().await.get::<SovStoreContainer>().cloned();
        let removal = match (
            command.guild_id,
            get_option_value(&command.data.options, "name"),
        ) {
            (Some(guild_id), Some(CommandDataOptionValue::String(name))) => {
                Ok((guild_id.0, command.channel_id.0, name.to_string()))
            }
            (None, _) => Err("Sov subscriptions can only be removed in a server channel."),
            _ => Err("Invalid sov subscription name."),
        };
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let (guild_id, channel_id, name) = match removal {
                Ok(removal) => removal,
                Err(response) => return response.to_string(),
            };
            let Some(store_handle) = store_handle else {
                return "Sov subscriptions are unavailable because the sov database is not connected."
                    .to_string();
            };
            let Some(store) = available_sov_store(&store_handle).await else {
                return "Sov subscriptions are unavailable because the sov database is not connected."
                    .to_string();
            };
            match store.remove_subscription(guild_id, channel_id, &name).await {
                Ok(true) => format!("Sov subscription '{name}' removed from this channel."),
                Ok(false) => format!("No sov subscription '{name}' exists in this channel."),
                Err(error) => {
                    error!("cannot remove sov subscription: {error}");
                    "Sov subscription could not be removed.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to sov unsubscribe command: {error}");
        }
    }
}
