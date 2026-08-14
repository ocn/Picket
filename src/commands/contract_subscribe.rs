use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::contract_intelligence::{
    available_contract_store, ContractEventActions, ContractFilter, ContractSubscription,
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
        Self::subscription_from_documents(
            guild_id,
            channel_id,
            option("id")?,
            option("description")?,
            option("filter")?,
            option("event_actions")?,
        )
    }

    fn subscription_from_documents(
        guild_id: u64,
        channel_id: u64,
        id: &str,
        description: &str,
        filter_document: &str,
        event_actions_document: &str,
    ) -> Result<ContractSubscription, String> {
        let subscription = ContractSubscription {
            guild_id,
            channel_id,
            id: id.to_string(),
            description: description.to_string(),
            filter: serde_json::from_str::<ContractFilter>(filter_document)
                .map_err(|error| format!("invalid filter JSON: {error}"))?,
            event_actions: serde_json::from_str::<ContractEventActions>(event_actions_document)
                .map_err(|error| format!("invalid event actions JSON: {error}"))?,
        };
        subscription.validate()?;
        Ok(subscription)
    }
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
                    .name("filter")
                    .description("Contract filter JSON using condition, and, or, and not nodes.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("event_actions")
                    .description("Event action JSON, for example {\"listed\":\"post\"}.")
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
            let Some(store_handle) = store_handle else {
                return "Contract subscriptions are unavailable because the contract database is not connected."
                    .to_string();
            };
            let Some(store) = available_contract_store(&store_handle).await else {
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

#[cfg(test)]
mod tests {
    use super::*;

    const FILTER: &str = r#"{
        "root": {
            "and": [
                {"condition": {"event_kinds": ["listed"]}},
                {"or": [
                    {"condition": {"item_types": {"direction": "offered", "ids": [587]}}},
                    {"not": {"condition": {"title_fragment": "scam"}}}
                ]}
            ]
        }
    }"#;

    #[test]
    fn contract_subscribe_documents_express_the_recursive_grammar() {
        let subscription = ContractSubscribeCommand::subscription_from_documents(
            42,
            77,
            "capitals",
            "capital contracts",
            FILTER,
            r#"{"listed":"post_and_ping"}"#,
        )
        .expect("valid recursive subscription document");
        assert_eq!(subscription.guild_id, 42);
        assert_eq!(subscription.channel_id, 77);
        assert!(matches!(
            subscription.filter.root,
            crate::contract_intelligence::ContractFilterNode::And(_)
        ));
    }

    #[test]
    fn invalid_contract_subscribe_documents_do_not_produce_a_subscription() {
        for (filter, actions) in [
            (r#"{"root":{"and":[]}}"#, r#"{"listed":"post"}"#),
            (
                r#"{"root":{"condition":{"location_ids":[0]}}}"#,
                r#"{"listed":"post"}"#,
            ),
            (FILTER, r#"{"listed":"post","unknown":"post"}"#),
            (FILTER, r#"{"listed":"unknown"}"#),
        ] {
            assert!(ContractSubscribeCommand::subscription_from_documents(
                42,
                77,
                "capitals",
                "capital contracts",
                filter,
                actions,
            )
            .is_err());
        }
    }
}
