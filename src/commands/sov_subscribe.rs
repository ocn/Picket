use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::sov_feed::{
    available_sov_store, parse_tminus_marks_minutes, parse_tz_window, SovFilter,
    SovFilterCondition, SovFilterNode, SovSubscription, SOV_REACHABLE_MAX_JUMPS,
    SOV_TMINUS_MARKS_OPTION_KEY, SOV_TZ_SHIFT_ENABLED_OPTION_KEY, SOV_TZ_WINDOW_OPTION_KEY,
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
    max_jumps: Option<i64>,
    /// Convenience: whether the `Reachable` leaf composed from `max_jumps`
    /// allows frigate-sized wormhole connections on its route (ticket 05,
    /// spec "`/sov_subscribe` gains an `allow_frigate_holes` boolean
    /// convenience option"). Meaningless without `max_jumps` -- validated
    /// in `subscription_from_documents`.
    allow_frigate_holes: Option<bool>,
    role_id: Option<u64>,
    /// Raw `tminus_marks` command option: a comma-separated list of
    /// minutes, or an empty string to disable marks. `None` when the
    /// option was omitted, which leaves the stored `options` document
    /// without the key so evaluation applies the default (spec: "an
    /// option; default [120, 30] when omitted; an explicit empty list
    /// disables marks").
    tminus_marks: Option<&'a str>,
    /// Raw `tz_window` command option: `HH:MM-HH:MM` EVE/UTC, may cross
    /// midnight (ticket 07). `None` when omitted, leaving the stored
    /// `options` document without the key so evaluation applies the
    /// default (00:00-04:00).
    tz_window: Option<&'a str>,
    /// `tz_shift_enabled` command option: whether the `tz_window_entered`
    /// stage participates for this subscription at all (ticket 07;
    /// default false when omitted).
    tz_shift_enabled: Option<bool>,
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
        let max_jumps = match get_option_value(&command.data.options, "max_jumps") {
            Some(CommandDataOptionValue::Integer(value)) => Some(*value),
            None => None,
            _ => return Err("max_jumps must be an integer option".to_string()),
        };
        let allow_frigate_holes =
            match get_option_value(&command.data.options, "allow_frigate_holes") {
                Some(CommandDataOptionValue::Boolean(value)) => Some(*value),
                None => None,
                _ => return Err("allow_frigate_holes must be a boolean option".to_string()),
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
        let tz_window = match get_option_value(&command.data.options, "tz_window") {
            Some(CommandDataOptionValue::String(value)) => Some(value.as_str()),
            None => None,
            _ => return Err("tz_window must be a string option".to_string()),
        };
        let tz_shift_enabled = match get_option_value(&command.data.options, "tz_shift_enabled") {
            Some(CommandDataOptionValue::Boolean(value)) => Some(*value),
            None => None,
            _ => return Err("tz_shift_enabled must be a boolean option".to_string()),
        };
        Self::subscription_from_documents(
            guild_id,
            channel_id,
            SovSubscriptionDocuments {
                name,
                filter,
                region_id,
                defender_alliance_id,
                max_jumps,
                allow_frigate_holes,
                role_id,
                tminus_marks,
                tz_window,
                tz_shift_enabled,
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
                watchlist: false,
            }));
        }
        if let Some(max_jumps) = documents.max_jumps {
            extra.push(SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps,
                allow_frigate_holes: documents.allow_frigate_holes.unwrap_or(false),
            }));
        } else if documents.allow_frigate_holes.is_some() {
            return Err("allow_frigate_holes requires max_jumps to also be set".to_string());
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
        if let Some(raw) = documents.tz_window {
            // Validated eagerly (rather than deferred to evaluation time)
            // so `/sov_subscribe` rejects a malformed window immediately;
            // the normalized `HH:MM-HH:MM` text is stored, not the raw
            // input, so extra whitespace never round-trips into `options`.
            let window =
                parse_tz_window(raw).map_err(|error| format!("invalid tz_window: {error}"))?;
            options[SOV_TZ_WINDOW_OPTION_KEY] = serde_json::json!(window.to_option_string());
        }
        if let Some(enabled) = documents.tz_shift_enabled {
            options[SOV_TZ_SHIFT_ENABLED_OPTION_KEY] = serde_json::json!(enabled);
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
    /// re-running `/sov_subscribe` with only some options preserves the
    /// keys this invocation did not touch instead of silently resetting
    /// them to their defaults (review finding 2 on ticket 03; extended in
    /// ticket 07 now that `tz_window`/`tz_shift_enabled` share the same
    /// document as `tminus_marks_minutes`).
    ///
    /// This is a per-key object merge, not all-or-nothing: start from the
    /// existing document (or `{}` when none / not an object) and overlay
    /// every key *present* in `requested`. Because
    /// `subscription_from_documents` only writes a key when its command
    /// option was supplied, a key present in `requested` is exactly a key
    /// the user set this invocation, and it always wins over the stored
    /// value -- including an explicit empty `tminus_marks_minutes: []`
    /// (a present key that disables marks) while any untouched `tz_window`
    /// / `tz_shift_enabled` survive, and vice versa. A brand-new
    /// subscription (`existing` is `None`) with no options supplied stays
    /// `{}`, so evaluation applies every default as usual.
    fn merge_options(
        existing: Option<&serde_json::Value>,
        requested: serde_json::Value,
    ) -> serde_json::Value {
        let mut merged = existing
            .and_then(|value| value.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(object) = requested.as_object() {
            for (key, value) in object {
                merged.insert(key.clone(), value.clone());
            }
        }
        serde_json::Value::Object(merged)
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
                    .name("max_jumps")
                    .description("Convenience: also require reachable from home within this many jumps (1-11).")
                    .kind(CommandOptionType::Integer)
                    .min_int_value(1)
                    .max_int_value(SOV_REACHABLE_MAX_JUMPS)
            })
            .create_option(|option| {
                option
                    .name("allow_frigate_holes")
                    .description("With max_jumps: allow frigate-sized wormholes on the route.")
                    .kind(CommandOptionType::Boolean)
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
            .create_option(|option| {
                option
                    .name("tz_window")
                    .description(
                        "Timezone window HH:MM-HH:MM EVE (default 00:00-04:00; may cross midnight).",
                    )
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("tz_shift_enabled")
                    .description("Alert when a watched hub's vulnerability window enters tz_window.")
                    .kind(CommandOptionType::Boolean)
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
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
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
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: Some(555),
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
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
                    SovFilterNode::Condition(SovFilterCondition::Defender { ref alliance_ids, .. }) if alliance_ids == &vec![99_000_001]
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_composes_max_jumps_as_a_reachable_condition() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "reachable-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: Some(11),
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document with max_jumps");
        match subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert_eq!(nodes.len(), 2);
                assert!(matches!(
                    nodes[1],
                    SovFilterNode::Condition(SovFilterCondition::Reachable {
                        max_jumps: 11,
                        allow_frigate_holes: false,
                    })
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_composes_allow_frigate_holes_onto_the_reachable_condition() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "frigate-holes-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: Some(6),
                allow_frigate_holes: Some(true),
                role_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document with max_jumps and allow_frigate_holes");
        match subscription.filter.root {
            SovFilterNode::And(nodes) => {
                assert!(matches!(
                    nodes[1],
                    SovFilterNode::Condition(SovFilterCondition::Reachable {
                        max_jumps: 6,
                        allow_frigate_holes: true,
                    })
                ));
            }
            other => panic!("expected an And node, got {other:?}"),
        }
    }

    #[test]
    fn sov_subscribe_rejects_allow_frigate_holes_without_max_jumps() {
        assert!(SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "orphan-frigate-flag",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: Some(true),
                role_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .is_err());
    }

    #[test]
    fn sov_subscribe_rejects_max_jumps_outside_one_through_eleven() {
        for max_jumps in [0, SOV_REACHABLE_MAX_JUMPS + 1] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name: "bad-max-jumps",
                    filter: FILTER,
                    region_id: None,
                    defender_alliance_id: None,
                    max_jumps: Some(max_jumps),
                    allow_frigate_holes: None,
                    role_id: None,
                    tminus_marks: None,
                    tz_window: None,
                    tz_shift_enabled: None,
                },
            )
            .is_err());
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
                    max_jumps: None,
                    allow_frigate_holes: None,
                    role_id: None,
                    tminus_marks: None,
                    tz_window: None,
                    tz_shift_enabled: None,
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
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: Some("120, 45, 120"),
                tz_window: None,
                tz_shift_enabled: None,
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
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: Some(""),
                tz_window: None,
                tz_shift_enabled: None,
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
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
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
                    max_jumps: None,
                    allow_frigate_holes: None,
                    role_id: None,
                    tminus_marks: Some(raw),
                    tz_window: None,
                    tz_shift_enabled: None,
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

    #[test]
    fn merge_options_overlays_tz_shift_without_dropping_stored_tminus_marks() {
        // Fix round finding 1: `/sov_subscribe name:X tminus_marks:90`
        // then `/sov_subscribe name:X tz_shift_enabled:true` must keep the
        // custom marks -- a partial re-subscribe overlays one key rather
        // than replacing the whole document.
        let existing = serde_json::json!({"tminus_marks_minutes": [90]});
        let requested = serde_json::json!({"tz_shift_enabled": true});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested);
        assert_eq!(
            merged,
            serde_json::json!({"tminus_marks_minutes": [90], "tz_shift_enabled": true})
        );
    }

    #[test]
    fn merge_options_overlays_tminus_marks_without_dropping_stored_tz_keys() {
        // The reverse order of the finding-1 scenario: tz keys were set
        // first, then a later invocation supplies only `tminus_marks`.
        let existing = serde_json::json!({"tz_window": "06:00-10:00", "tz_shift_enabled": true});
        let requested = serde_json::json!({"tminus_marks_minutes": [90]});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested);
        assert_eq!(
            merged,
            serde_json::json!({
                "tz_window": "06:00-10:00",
                "tz_shift_enabled": true,
                "tminus_marks_minutes": [90],
            })
        );
    }

    #[test]
    fn merge_options_lets_an_explicit_empty_tminus_disable_while_tz_keys_survive() {
        // An explicit empty list is a present key (disables marks) yet the
        // untouched tz keys must still survive the overlay.
        let existing = serde_json::json!({"tz_window": "06:00-10:00", "tz_shift_enabled": true});
        let requested = serde_json::json!({"tminus_marks_minutes": []});
        let merged = SovSubscribeCommand::merge_options(Some(&existing), requested);
        assert_eq!(
            merged,
            serde_json::json!({
                "tz_window": "06:00-10:00",
                "tz_shift_enabled": true,
                "tminus_marks_minutes": [],
            })
        );
    }

    // --- tz_window / tz_shift_enabled (ticket 07) ---

    #[test]
    fn sov_subscribe_stores_a_normalized_tz_window_and_tz_shift_enabled_in_options() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "tz-shift-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: None,
                tz_window: Some(" 06:00-10:00 "),
                tz_shift_enabled: Some(true),
            },
        )
        .expect("valid document with tz_window and tz_shift_enabled");
        assert_eq!(
            subscription.options,
            serde_json::json!({"tz_window": "06:00-10:00", "tz_shift_enabled": true})
        );
    }

    #[test]
    fn sov_subscribe_omitted_tz_options_leave_options_without_those_keys() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "default-tz",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: None,
                tz_window: None,
                tz_shift_enabled: None,
            },
        )
        .expect("valid document omitting tz options");
        assert_eq!(subscription.options, serde_json::json!({}));
    }

    #[test]
    fn sov_subscribe_rejects_a_malformed_tz_window() {
        for raw in ["not-a-window", "24:00-01:00", "06:00-06:00"] {
            assert!(SovSubscribeCommand::subscription_from_documents(
                42,
                77,
                SovSubscriptionDocuments {
                    name: "bad-tz-window",
                    filter: FILTER,
                    region_id: None,
                    defender_alliance_id: None,
                    max_jumps: None,
                    allow_frigate_holes: None,
                    role_id: None,
                    tminus_marks: None,
                    tz_window: Some(raw),
                    tz_shift_enabled: None,
                },
            )
            .is_err());
        }
    }

    #[test]
    fn sov_subscribe_tz_window_option_and_tminus_marks_compose_in_the_same_options_document() {
        let subscription = SovSubscribeCommand::subscription_from_documents(
            42,
            77,
            SovSubscriptionDocuments {
                name: "everything-front",
                filter: FILTER,
                region_id: None,
                defender_alliance_id: None,
                max_jumps: None,
                allow_frigate_holes: None,
                role_id: None,
                tminus_marks: Some("120"),
                tz_window: Some("22:00-02:00"),
                tz_shift_enabled: Some(true),
            },
        )
        .expect("valid document combining tminus_marks and tz options");
        assert_eq!(
            subscription.options,
            serde_json::json!({
                "tminus_marks_minutes": [120],
                "tz_window": "22:00-02:00",
                "tz_shift_enabled": true,
            })
        );
    }
}
