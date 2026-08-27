use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::sov_feed::{
    available_sov_store, parse_tminus_marks_minutes, SovFilter, SovFilterCondition, SovFilterNode,
    SovSubscription, SOV_TMINUS_MARKS_OPTION_KEY,
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
    /// Raw `tminus_marks` command option: a comma-separated list of
    /// minutes, or an empty string to disable marks. `None` when the
    /// option was omitted, which leaves the stored `options` document
    /// without the key so evaluation applies the default (spec: "an
    /// option; default [120, 30] when omitted; an explicit empty list
    /// disables marks").
    tminus_marks: Option<&'a str>,
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
        let tminus_marks = match get_option_value(&command.data.options, "tminus_marks") {
            Some(CommandDataOptionValue::String(value)) => Some(value.as_str()),
            None => None,
            _ => return Err("tminus_marks must be a string option".to_string()),
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
                tminus_marks,
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
        let mut options = serde_json::json!({});
        if let Some(raw) = documents.tminus_marks {
            let marks = parse_tminus_marks_minutes(raw)
                .map_err(|error| format!("invalid tminus_marks: {error}"))?;
            options[SOV_TMINUS_MARKS_OPTION_KEY] = serde_json::json!(marks);
        }
        let subscription = SovSubscription {
            guild_id,
            channel_id,
            name: documents.name.to_string(),
            filter,
            options,
            role_id: documents.role_id,
        };
        subscription.validate()?;
        Ok(subscription)
    }

    /// Merges a freshly-built subscription document's `options` against
    /// whatever was already persisted for the same subscription, so
    /// re-running `/sov_subscribe` without `tminus_marks` preserves
    /// custom marks instead of silently resetting them to the default
    /// (review finding 2 on ticket 03). `requested` is exactly `{}` when
    /// the command supplied no options at all this time (every option
    /// this ticket adds is opt-in); any other shape reflects something
    /// the user explicitly set in this invocation, which always wins over
    /// whatever was stored before. A brand-new subscription (`existing`
    /// is `None`) falls back to `requested` unchanged, i.e. `{}`, so
    /// evaluation applies the default marks as usual.
    fn merge_options(
        existing: Option<&serde_json::Value>,
        requested: serde_json::Value,
    ) -> serde_json::Value {
        if requested == serde_json::json!({}) {
            existing.cloned().unwrap_or(requested)
        } else {
            requested
        }
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
            .create_option(|option| {
                option
                    .name("tminus_marks")
                    .description(
                        "Comma-separated T-minus marks, minutes (default 120,30; empty disables; capped by VulnerableWithin).",
                    )
                    .kind(CommandOptionType::String)
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
            let mut subscription = match response {
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
            // Preserve whatever `options` (e.g. custom T-minus marks) an
            // existing subscription of the same name already has when
            // this invocation did not touch them (review finding 2).
            let existing = store
                .subscription(subscription.guild_id, subscription.channel_id, &subscription.name)
                .await
                .ok()
                .flatten();
            subscription.options = SovSubscribeCommand::merge_options(
                existing.as_ref().map(|found| &found.options),
                subscription.options,
            );
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
                tminus_marks: None,
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
                tminus_marks: None,
            },
        )
        .expect("valid composed document");
        assert_eq!(subscription.role_id, Some(555));
        assert_eq!(subscription.options, serde_json::json!({}));
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
                    tminus_marks: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_stores_parsed_and_deduped_tminus_marks_in_options() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "custom-marks",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                role_id: None,
                tminus_marks: Some("120, 45, 120"),
            },
        )
        .expect("valid document with custom tminus marks");
        assert_eq!(
            subscription.options,
            serde_json::json!({"tminus_marks_minutes": [120, 45]})
        );
    }

    #[test]
    fn sov_subscribe_empty_tminus_marks_stores_an_explicit_empty_list() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "no-marks",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                role_id: None,
                tminus_marks: Some(""),
            },
        )
        .expect("valid document disabling tminus marks");
        assert_eq!(
            subscription.options,
            serde_json::json!({"tminus_marks_minutes": []})
        );
    }

    #[test]
    fn sov_subscribe_omitted_tminus_marks_leaves_options_without_the_key() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "default-marks",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                role_id: None,
                tminus_marks: None,
            },
        )
        .expect("valid document omitting tminus marks");
        assert_eq!(subscription.options, serde_json::json!({}));
    }

    #[test]
    fn sov_subscribe_rejects_invalid_tminus_marks() {
        for raw in ["0", "-5", "not-a-number", "120,abc"] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name: "bad-marks",
                    filter: FILTER,
                    region_id: None,
                    defender_alliance_id: None,
                    role_id: None,
                    tminus_marks: Some(raw),
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
        // Discord rejects command registration if any option description
        // exceeds 100 characters.
        for option in options {
            let description = option["description"]
                .as_str()
                .expect("every option has a description");
            assert!(
                description.len() <= 100,
                "option '{}' description is {} chars: {description}",
                option["name"],
                description.len()
            );
        }
    }

    #[test]
    fn merge_options_preserves_existing_marks_when_the_command_omits_them() {
        // Review finding 2: re-running `/sov_subscribe` without
        // `tminus_marks` must not silently reset previously-configured
        // marks back to the default.
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), serde_json::json!({}));
        assert_eq!(merged, existing);
    }

    #[test]
    fn merge_options_uses_the_omitted_document_for_a_brand_new_subscription() {
        let merged = SovSubscribeCommand::merge_options(None, serde_json::json!({}));
        assert_eq!(merged, serde_json::json!({}));
    }

    #[test]
    fn merge_options_lets_explicit_new_marks_replace_whatever_was_stored() {
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let requested = serde_json::json!({"tminus_marks_minutes": [120, 30]});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested.clone());
        assert_eq!(merged, requested);
    }

    #[test]
    fn merge_options_lets_an_explicit_empty_list_disable_marks_even_when_marks_existed_before() {
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let requested = serde_json::json!({"tminus_marks_minutes": []});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested.clone());
        assert_eq!(merged, requested);
    }
}
