use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::{AppState, SystemRange};
use crate::contract_intelligence::{
    available_contract_store, ContractEventActions, ContractFilter, ContractFilterCondition,
    ContractFilterNode, ContractPingType, ContractSubscription,
};
use crate::ContractStoreContainer;
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

pub struct ContractSubscribeCommand;

struct ContractSubscriptionDocuments<'a> {
    id: &'a str,
    description: &'a str,
    filter: &'a str,
    event_actions: &'a str,
    ping_type: Option<&'a str>,
    ly_ranges_json: Option<&'a str>,
}

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
        let ping_type = match get_option_value(&command.data.options, "ping_type") {
            Some(CommandDataOptionValue::String(value)) => Some(value.as_str()),
            None => None,
            _ => return Err("ping_type must be a string option".to_string()),
        };
        let ly_ranges_json = match get_option_value(&command.data.options, "ly_ranges_json") {
            Some(CommandDataOptionValue::String(value)) => Some(value.as_str()),
            None => None,
            _ => return Err("ly_ranges_json must be a string option".to_string()),
        };
        Self::subscription_from_documents_with_ranges(
            guild_id,
            channel_id,
            ContractSubscriptionDocuments {
                id: option("id")?,
                description: option("description")?,
                filter: option("filter")?,
                event_actions: option("event_actions")?,
                ping_type,
                ly_ranges_json,
            },
        )
    }

    #[cfg(test)]
    fn subscription_from_documents(
        guild_id: u64,
        channel_id: u64,
        id: &str,
        description: &str,
        filter_document: &str,
        event_actions_document: &str,
        ping_type: Option<&str>,
    ) -> Result<ContractSubscription, String> {
        Self::subscription_from_documents_with_ranges(
            guild_id,
            channel_id,
            ContractSubscriptionDocuments {
                id,
                description,
                filter: filter_document,
                event_actions: event_actions_document,
                ping_type,
                ly_ranges_json: None,
            },
        )
    }

    fn subscription_from_documents_with_ranges(
        guild_id: u64,
        channel_id: u64,
        documents: ContractSubscriptionDocuments<'_>,
    ) -> Result<ContractSubscription, String> {
        let mut event_actions =
            serde_json::from_str::<ContractEventActions>(documents.event_actions)
                .map_err(|error| format!("invalid event actions JSON: {error}"))?;
        if let Some(ping_type) = documents.ping_type {
            let ping_type = match ping_type {
                "here" => ContractPingType::Here,
                "everyone" => ContractPingType::Everyone,
                value => return Err(format!("invalid ping type: {value}")),
            };
            event_actions.configure_ping_type(ping_type);
        }
        let mut filter = serde_json::from_str::<ContractFilter>(documents.filter)
            .map_err(|error| format!("invalid filter JSON: {error}"))?;
        if let Some(json) = documents.ly_ranges_json {
            let ranges = serde_json::from_str::<Vec<SystemRange>>(json)
                .map_err(|error| format!("Invalid JSON format for ly_ranges_json: {error}"))?;
            validate_system_ranges(&ranges)?;
            filter.root = ContractFilterNode::And(vec![
                filter.root,
                ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(ranges)),
            ]);
        }
        let subscription = ContractSubscription {
            guild_id,
            channel_id,
            id: documents.id.to_string(),
            description: documents.description.to_string(),
            filter,
            event_actions,
        };
        subscription.validate()?;
        Ok(subscription)
    }
}

fn validate_system_ranges(ranges: &[SystemRange]) -> Result<(), String> {
    if ranges.is_empty() {
        return Err("ly_ranges_json must include at least one system range".to_string());
    }
    if ranges.iter().any(|range| range.system_id == 0) {
        return Err("ly range system IDs must be positive integers".to_string());
    }
    if ranges
        .iter()
        .any(|range| !range.range.is_finite() || range.range <= 0.0)
    {
        return Err("ly ranges must be finite, positive light-year values".to_string());
    }
    Ok(())
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
            .default_member_permissions(Permissions::MANAGE_GUILD)
            .dm_permission(false)
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
            .create_option(|option| {
                option
                    .name("ly_ranges_json")
                    .description(
                        "System range JSON, e.g. [{\"system_id\":30002086,\"range\":8.0}].",
                    )
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("ping_type")
                    .description("Use @here or @everyone when an event action is post_and_ping.")
                    .kind(CommandOptionType::String)
                    .add_string_choice("Here", "here")
                    .add_string_choice("Everyone", "everyone")
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
            Some("everyone"),
        )
        .expect("valid recursive subscription document");
        assert_eq!(subscription.guild_id, 42);
        assert_eq!(subscription.channel_id, 77);
        assert!(matches!(
            subscription.filter.root,
            crate::contract_intelligence::ContractFilterNode::And(_)
        ));
        assert_eq!(
            subscription.event_actions.listed,
            crate::contract_intelligence::ContractEventAction::PostAndPingEveryone
        );
    }

    #[test]
    fn contract_subscribe_composes_compact_light_year_ranges_with_the_filter_root() {
        let subscription = ContractSubscribeCommand::subscription_from_documents_with_ranges(
            42,
            77,
            ContractSubscriptionDocuments {
                id: "capitals-near-turnur-or-kurniainen",
                description: "capital contracts near either center",
                filter: FILTER,
                event_actions: r#"{"listed":"post"}"#,
                ping_type: None,
                ly_ranges_json: Some(
                    r#"[{"system_id":30002086,"range":8.0},{"system_id":30003089,"range":8.0}]"#,
                ),
            },
        )
        .expect("valid compact range document");
        assert!(matches!(
            subscription.filter.root,
            ContractFilterNode::And(ref nodes)
                if matches!(nodes[1], ContractFilterNode::Condition(ContractFilterCondition::LyRangeFrom(ref ranges)) if ranges == &vec![
                    SystemRange { system_id: 30_002_086, range: 8.0 },
                    SystemRange { system_id: 30_003_089, range: 8.0 },
                ])
        ));
    }

    #[test]
    fn contract_subscribe_rejects_invalid_light_year_range_documents_before_persistence() {
        for ranges in [
            "not json",
            "[]",
            r#"[{"system_id":0,"range":8.0}]"#,
            r#"[{"system_id":30002086,"range":0.0}]"#,
            r#"[{"system_id":30002086,"range":1e999}]"#,
        ] {
            assert!(
                ContractSubscribeCommand::subscription_from_documents_with_ranges(
                    42,
                    77,
                    ContractSubscriptionDocuments {
                        id: "capitals",
                        description: "capital contracts",
                        filter: FILTER,
                        event_actions: r#"{"listed":"post"}"#,
                        ping_type: None,
                        ly_ranges_json: Some(ranges),
                    },
                )
                .is_err()
            );
        }
    }

    #[test]
    fn sale_only_event_actions_default_listed_to_ignore_and_preserve_explicit_legacy_actions() {
        let sale_only = ContractSubscribeCommand::subscription_from_documents(
            42,
            77,
            "sale-only",
            "confirmed sales only",
            FILTER,
            r#"{"sale_confirmed":"post"}"#,
            None,
        )
        .expect("sale-only actions are valid");
        assert_eq!(
            sale_only.event_actions.listed,
            crate::contract_intelligence::ContractEventAction::Ignore
        );
        assert_eq!(
            sale_only.event_actions.sale_confirmed,
            crate::contract_intelligence::ContractEventAction::Post
        );

        let legacy = ContractSubscribeCommand::subscription_from_documents(
            42,
            77,
            "legacy-listed",
            "legacy listed action",
            FILTER,
            r#"{"listed":"post_and_ping","sale_confirmed":"post"}"#,
            Some("everyone"),
        )
        .expect("explicit legacy listed action remains valid");
        assert_eq!(
            legacy.event_actions.listed,
            crate::contract_intelligence::ContractEventAction::PostAndPingEveryone
        );
        assert_eq!(
            legacy.event_actions.sale_confirmed,
            crate::contract_intelligence::ContractEventAction::Post
        );
    }

    #[test]
    fn contract_subscribe_exposes_a_bounded_ping_type_option() {
        let mut command = CreateApplicationCommand::default();
        ContractSubscribeCommand.register(&mut command);
        let options = command.0["options"]
            .as_array()
            .expect("contract subscribe options");
        assert!(options.len() <= 25);
        let ping_type = options
            .iter()
            .find(|option| option["name"] == "ping_type")
            .expect("configured ping type option");
        assert_eq!(
            ping_type["choices"].as_array().expect("ping choices").len(),
            2
        );
        let ranges = options
            .iter()
            .find(|option| option["name"] == "ly_ranges_json")
            .expect("configured light-year ranges option");
        assert_eq!(ranges["type"], CommandOptionType::String.num());
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
                None,
            )
            .is_err());
        }
    }
}
