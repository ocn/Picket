use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::contract_intelligence::ContractCollectionStore;
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
        let response = match (
            command.guild_id,
            get_option_value(&command.data.options, "id"),
        ) {
            (Some(guild_id), Some(CommandDataOptionValue::String(id))) => {
                match std::env::var("CONTRACT_DATABASE_URL") {
                    Ok(database_url) => match ContractCollectionStore::connect(&database_url).await {
                        Ok(store) => match store
                            .remove_contract_subscription(guild_id.0, command.channel_id.0, id)
                            .await
                        {
                            Ok(true) => format!("Contract subscription '{}' removed from this channel.", id),
                            Ok(false) => format!("No contract subscription '{}' exists in this channel.", id),
                            Err(error) => {
                                error!("cannot remove contract subscription: {error}");
                                "Contract subscription could not be removed.".to_string()
                            }
                        },
                        Err(error) => {
                            error!("contract database unavailable for removal command: {error}");
                            "Contract subscriptions are unavailable because the contract database is not connected.".to_string()
                        }
                    },
                    Err(_) => "Contract subscriptions are unavailable because CONTRACT_DATABASE_URL is not configured.".to_string(),
                }
            }
            (None, _) => {
                "Contract subscriptions can only be removed in a server channel.".to_string()
            }
            _ => "Invalid contract subscription ID.".to_string(),
        };
        if let Err(error) = command
            .create_interaction_response(&ctx.http, |response_builder| {
                response_builder
                    .interaction_response_data(|message| message.content(response).ephemeral(true))
            })
            .await
        {
            error!("Cannot respond to contract unsubscribe command: {error}");
        }
    }
}
