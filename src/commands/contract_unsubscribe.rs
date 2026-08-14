use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::contract_intelligence::available_contract_store;
use crate::ContractStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct ContractUnsubscribeCommand;

#[async_trait]
impl Command for ContractUnsubscribeCommand {
    fn name(&self) -> String {
        "contract_unsubscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("contract_unsubscribe")
            .description("Remove a public contract subscription from this channel.")
            .create_option(|option| {
                option
                    .name("id")
                    .description("Stable subscription ID to remove.")
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
            .get::<ContractStoreContainer>()
            .cloned();
        let removal = match (
            command.guild_id,
            get_option_value(&command.data.options, "id"),
        ) {
            (Some(guild_id), Some(CommandDataOptionValue::String(id))) => {
                Ok((guild_id.0, command.channel_id.0, id.to_string()))
            }
            (None, _) => Err("Contract subscriptions can only be removed in a server channel."),
            _ => Err("Invalid contract subscription ID."),
        };
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let (guild_id, channel_id, id) = match removal {
                Ok(removal) => removal,
                Err(response) => return response.to_string(),
            };
            let Some(store_handle) = store_handle else {
                return "Contract subscriptions are unavailable because the contract database is not connected."
                    .to_string();
            };
            let Some(store) = available_contract_store(&store_handle).await else {
                return "Contract subscriptions are unavailable because the contract database is not connected."
                    .to_string();
            };
            match store
                .remove_contract_subscription(guild_id, channel_id, &id)
                .await
            {
                Ok(true) => format!("Contract subscription '{}' removed from this channel.", id),
                Ok(false) => format!("No contract subscription '{}' exists in this channel.", id),
                Err(error) => {
                    error!("cannot remove contract subscription: {error}");
                    "Contract subscription could not be removed.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to contract unsubscribe command: {error}");
        }
    }
}
