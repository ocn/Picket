use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::sov_feed::{
    available_sov_store, SovFilter, SovFilterCondition, SovFilterNode, SovSubscription,
};
use crate::SovStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct SovSubscribeCommand;

struct SovSubscriptionDocuments<'a> {
    name: &'a str,
    filter: &'a str,
    region_id: Option<i64>,
    defender_alliance_id: Option<i64>,
    role_id: Option<u64>,
}

impl SovSubscribeCommand {
    fn subscription(
        guild_id: u64,
        channel_id: u64,
        command: &ApplicationCommandInteraction,
    ) -> Result<SovSubscription, String> {
        let name = match get_option_value(&command.data.options, "name") {
            Some(CommandDataOptionValue::String(value)) => value.as_str(),
            _ => return Err("missing required option: name".to_string()),
        };
        let filter = match get_option_value(&command.data.options, "filter") {
            Some(CommandDataOptionValue::String(value)) => value.as_str(),
            _ => return Err("missing required option: filter".to_string()),
        };
        let region_id = match get_option_value(&command.data.options, "region_id") {
            Some(CommandDataOptionValue::Integer(value)) => Some(*value),
            None => None,
            _ => return Err("region_id must be an integer option".to_string()),
        };
        let defender_alliance_id =
            match get_option_value(&command.data.options, "defender_alliance_id") {
                Some(CommandDataOptionValue::Integer(value)) => Some(*value),
                None => None,
                _ => return Err("defender_alliance_id must be an integer option".to_string()),
            };
        let role_id = match get_option_value(&command.data.options, "role") {
            Some(CommandDataOptionValue::Role(role)) => Some(role.id.0),
            None => None,
            _ => return Err("role must be a role option".to_string()),
        };
        Self::subscription_from_documents(
            guild_id,
            channel_id,
            SovSubscriptionDocuments {
                name,
                filter,
                region_id,
                defender_alliance_id,
                role_id,
            },
        )
    }

    fn subscription_from_documents(
        guild_id: u64,
        channel_id: u64,
        documents: SovSubscriptionDocuments<'_>,
    ) -> Result<SovSubscription, String> {
        let mut filter = serde_json::from_str::<SovFilter>(documents.filter)
            .map_err(|error| format!("invalid filter JSON: {error}"))?;
        let mut extra = Vec::new();
        if let Some(region_id) = documents.region_id {
            extra.push(SovFilterNode::Condition(SovFilterCondition::Region(vec![
                region_id,
            ])));
        }
        if let Some(alliance_id) = documents.defender_alliance_id {
            extra.push(SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![alliance_id],
            }));
        }
        if !extra.is_empty() {
            extra.insert(0, filter.root);
            filter.root = SovFilterNode::And(extra);
        }
        let subscription = SovSubscription {
            guild_id,
            channel_id,
            name: documents.name.to_string(),
            filter,
            options: serde_json::json!({}),
            role_id: documents.role_id,
        };
        subscription.validate()?;
        Ok(subscription)
    }
}

#[async_trait]
impl Command for SovSubscribeCommand {
    fn name(&self) -> String {
        "sov_subscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sov_subscribe")
            .description("Create or replace a sovereignty campaign subscription for this channel.")
            .create_option(|option| {
                option
                    .name("name")
                    .description("Stable subscription name for this channel.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("filter")
                    .description("Sov filter JSON using condition, and, or, and not nodes.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("region_id")
                    .description("Convenience: also require this region ID.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("defender_alliance_id")
                    .description("Convenience: also require this defending alliance ID.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("role")
                    .description("Role to ping on alerts from this subscription.")
                    .kind(CommandOptionType::Role)
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        _app_state: &Arc<AppState>,
    ) {
        let store_handle = ctx.data.read().await.get::<SovStoreContainer>().cloned();
        let response = match command.guild_id {
            Some(guild_id) => Self::subscription(guild_id.0, command.channel_id.0, command)
                .map_err(|error| format!("Invalid sov subscription: {error}")),
            None => Err("Sov subscriptions can only be created in a server channel.".to_string()),
        };
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let subscription = match response {
                Ok(subscription) => subscription,
                Err(response) => return response,
            };
            let Some(store_handle) = store_handle else {
                return "Sov subscriptions are unavailable because the sov database is not connected."
                    .to_string();
            };
            let Some(store) = available_sov_store(&store_handle).await else {
                return "Sov subscriptions are unavailable because the sov database is not connected."
                    .to_string();
            };
            match store.upsert_subscription(&subscription).await {
                Ok(()) => format!(
                    "Sov subscription '{}' saved for this channel.",
                    subscription.name
                ),
                Err(error) => {
                    error!("cannot persist sov subscription: {error}");
                    "Sov subscription could not be saved.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to sov subscribe command: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILTER: &str = r#"{
        "root": {
            "and": [
                {"condition": {"vulnerable_within": {"hours": 12}}},
                {"condition": {"event_type": ["ihub_defense", "tcu_defense"]}}
            ]
        }
    }"#;

    #[test]
    fn sov_subscribe_documents_express_the_recursive_grammar() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "nullsec-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                role_id: None,
            },
        )
        .expect("valid recursive subscription document");
        assert_eq!(subscription.guild_id, 42);
        assert_eq!(subscription.channel_id, 77);
        assert_eq!(subscription.name, "nullsec-front");
        assert!(matches!(subscription.filter.root, SovFilterNode::And(_)));
    }

    #[test]
    fn sov_subscribe_composes_convenience_options_with_the_filter_root() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "region-and-defender",
                filter: FILTER,
                region_id: Some(10_000_060),
                defender_alliance_id: Some(99_000_001),
                role_id: Some(555),
            },
        )
        .expect("valid composed document");
        assert_eq!(subscription.role_id, Some(555));
        match subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert_eq!(nodes.len(), 3);
                assert!(matches!(
                    nodes[1],
                    SovFilterNode::Condition(SovFilterCondition::Region(ref ids)) if ids == &vec![10_000_060]
                ));
                assert!(matches!(
                    nodes[2],
                    SovFilterNode::Condition(SovFilterCondition::Defender { ref alliance_ids }) if alliance_ids == &vec![99_000_001]
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_rejects_invalid_documents() {
        for (name, filter) in [
            ("", FILTER),
            ("capitals", r#"{"root":{"and":[]}}"#),
            ("capitals", "not json"),
            ("capitals", r#"{"root":{"condition":{"event_type":[]}}}"#),
        ] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name,
                    filter,
                    region_id: None,
                    defender_alliance_id: None,
                    role_id: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_exposes_bounded_options() {
        let mut command = CreateApplicationCommand::default();
        SovSubscribeCommand.register(&mut command);
        let options = command.0["options"]
            .as_array()
            .expect("sov subscribe options");
        assert!(options.len() <= 25);
        let role = options
            .iter()
            .find(|option| option["name"] == "role")
            .expect("configured role option");
        assert_eq!(role["type"], CommandOptionType::Role.num());
    }
}
