use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::contract_intelligence::{
    ContractEventAction, ContractEventActions, ContractEventKind, ContractFilter,
    ContractItemDirection, ContractSubscription,
};
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

pub struct ContractSubscribeCommand;

impl ContractSubscribeCommand {
    fn subscription(
        guild_id: u64,
        channel_id: u64,
        command: &ApplicationCommandInteraction,
    ) -> Result<ContractSubscription, String> {
        let option = |name| match get_option_value(&command.data.options, name) {
            Some(CommandDataOptionValue::String(value)) => Ok(value.as_str()),
            _ => Err(format!("missing required option: {name}")),
        };
        let direction = match option("direction")? {
            "offered" => ContractItemDirection::Offered,
            "requested" => ContractItemDirection::Requested,
            _ => return Err("direction must be offered or requested".to_string()),
        };
        let listed = match option("listed_action")? {
            "ignore" => ContractEventAction::Ignore,
            "post" => ContractEventAction::Post,
            "post_and_ping" => ContractEventAction::PostAndPing,
            _ => return Err("listed_action must be ignore, post, or post_and_ping".to_string()),
        };
        let subscription = ContractSubscription {
            guild_id,
            channel_id,
            id: option("id")?.to_string(),
            description: option("description")?.to_string(),
            filter: ContractFilter {
                events: vec![ContractEventKind::Listed],
                item_direction: direction,
                type_ids: parse_ids(get_option_value(&command.data.options, "type_ids"))?,
                ship_group_ids: parse_ids(get_option_value(
                    &command.data.options,
                    "ship_group_ids",
                ))?,
            },
            event_actions: ContractEventActions { listed },
        };
        subscription.validate()?;
        Ok(subscription)
    }
}

fn parse_ids(option: Option<&CommandDataOptionValue>) -> Result<Vec<i64>, String> {
    let Some(CommandDataOptionValue::String(value)) = option else {
        return Ok(Vec::new());
    };
    value
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .trim()
                .parse::<i64>()
                .map_err(|_| format!("invalid numeric ID: {}", value.trim()))
        })
        .collect()
}

#[async_trait]
impl Command for ContractSubscribeCommand {
    fn name(&self) -> String {
        "contract_subscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("contract_subscribe")
            .description("Create or replace a public contract subscription for this channel.")
            .create_option(|option| {
                option
                    .name("id")
                    .description("Stable subscription ID for this channel.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("description")
                    .description("Short description of this subscription.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("direction")
                    .description("Whether the issuer offers or requests the ship.")
                    .kind(CommandOptionType::String)
                    .add_string_choice("Offered Item", "offered")
                    .add_string_choice("Requested Item", "requested")
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("type_ids")
                    .description("Comma-separated ship type IDs.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("ship_group_ids")
                    .description("Comma-separated ship group IDs.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("listed_action")
                    .description("Action for Listed events.")
                    .kind(CommandOptionType::String)
                    .add_string_choice("Ignore", "ignore")
                    .add_string_choice("Post", "post")
                    .add_string_choice("Post and ping", "post_and_ping")
                    .required(true)
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        _app_state: &Arc<AppState>,
    ) {
        let store = ctx
            .data
            .read()
            .await
            .get::<ContractStoreContainer>()
            .cloned();
        let response = match command.guild_id {
            Some(guild_id) => Self::subscription(guild_id.0, command.channel_id.0, command)
                .map_err(|error| format!("Invalid contract subscription: {error}")),
            None => {
                Err("Contract subscriptions can only be created in a server channel.".to_string())
            }
        };
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let subscription = match response {
                Ok(subscription) => subscription,
                Err(response) => return response,
            };
            let Some(store) = store else {
                return "Contract subscriptions are unavailable because the contract database is not connected."
                    .to_string();
            };
            match store.upsert_contract_subscription(&subscription).await {
                Ok(()) => format!(
                    "Contract subscription '{}' saved for this channel.",
                    subscription.id
                ),
                Err(error) => {
                    error!("cannot persist contract subscription: {error}");
                    "Contract subscription could not be saved.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to contract subscribe command: {error}");
        }
    }
}
