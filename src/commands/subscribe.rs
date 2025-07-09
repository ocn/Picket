use crate::commands::{get_option_value, Command};
use crate::config::{
    save_subscriptions_for_guild, Action, AppState, Filter, FilterNode, PingType, 
    SimpleFilter, Subscription, SystemRange, Target, TargetableCondition, TargetedFilter,
};
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct SubscribeCommand;

fn parse_ids<T: std::str::FromStr>(
    options: &[serenity::model::application::interaction::application_command::CommandDataOption],
    name: &str,
) -> Option<Vec<T>> {
    if let Some(CommandDataOptionValue::String(s)) = get_option_value(options, name) {
        let ids: Vec<T> = s
            .split(',')
            .filter_map(|id| id.trim().parse::<T>().ok())
            .collect();
        if ids.is_empty() {
            None
        } else {
            Some(ids)
        }
    } else {
        None
    }
}

#[async_trait]
impl Command for SubscribeCommand {
    fn name(&self) -> String {
        "subscribe".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("subscribe")
            .description("Create a new subscription for killmail notifications.")
            .create_option(|option| {
                option
                    .name("id")
                    .description("A unique identifier for this subscription.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("description")
                    .description("A description for the subscription.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("min_value")
                    .description("Minimum total value of the killmail in ISK.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("max_value")
                    .description("Maximum total value of the killmail in ISK.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("region_ids")
                    .description("A comma-separated list of region IDs to include.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("system_ids")
                    .description("A comma-separated list of system IDs to include.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("alliance_ids")
                    .description("A comma-separated list of alliance IDs to match.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("corp_ids")
                    .description("A comma-separated list of corporation IDs to match.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("char_ids")
                    .description("A comma-separated list of character IDs to match.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("ship_type_ids")
                    .description("A comma-separated list of ship type IDs to match.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("ship_group_ids")
                    .description("A comma-separated list of ship group IDs to match.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                 option
                     .name("target")
                     .description("Apply filters like ships/alliances to: any, attacker, or victim.")
                     .kind(CommandOptionType::String)
                     .add_string_choice("Any", "any")
                     .add_string_choice("Attacker", "attacker")
                     .add_string_choice("Victim", "victim")
             })            .create_option(|option| {
                option
                    .name("is_npc")
                    .description("Filter for NPC kills (true/false).")
                    .kind(CommandOptionType::Boolean)
            })
            .create_option(|option| {
                option
                    .name("is_solo")
                    .description("Filter for solo kills (true/false).")
                    .kind(CommandOptionType::Boolean)
            })
            .create_option(|option| {
                option
                    .name("min_pilots")
                    .description("Minimum number of pilots involved in the killmail.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("max_pilots")
                    .description("Maximum number of pilots involved in the killmail.")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("name_fragment")
                    .description("A fragment to match against ship names.")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("time_range_start")
                    .description("The start of the time range (0-23).")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("time_range_end")
                    .description("The end of the time range (0-23).")
                    .kind(CommandOptionType::Integer)
            })
            .create_option(|option| {
                option
                    .name("ly_ranges_json")
                    .description("A JSON string for system ranges, e.g., '[{\"system_id\":30000142, \"range\":10.0}]'")
                    .kind(CommandOptionType::String)
            })
            .create_option(|option| {
                option
                    .name("ping_type")
                    .description("Whether to ping @here or @everyone for a    match.")
                    .kind(CommandOptionType::String)
                    .add_string_choice("Here", "here")
                    .add_string_choice("Everyone", "everyone")
            })
            .create_option(|option| {
                option
                    .name("max_ping_delay_minutes")
                    .description("The maximum age of a killmail (in minutes) to be eligible for a    ping.")
                 .kind(CommandOptionType::Integer)
         })
            .create_option(|option| {
                option
                    .name("security")
                    .description("A security status range (e.g., \"-1.0..=0.4\" for low/nullsec).")
                    .kind(CommandOptionType::String)
             })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    ) {
        let options = &command.data.options;
        let mut filters = Vec::new();

        let guild_id = match command.guild_id {
            Some(id) => id,
            None => {
                return;
            }
        };

        let id = match get_option_value(options, "id") {
            Some(CommandDataOptionValue::String(s)) => s.clone(),
            _ => {
                return;
            }
        };

        let description = match get_option_value(options, "description") {
            Some(CommandDataOptionValue::String(s)) => s.clone(),
            _ => {
                return;
            }
        };

        let min_value = get_option_value(options, "min_value").and_then(|v| {
            if let CommandDataOptionValue::Integer(i) = v {
                Some(*i as u64)
            } else {
                None
            }
        });
        let max_value = get_option_value(options, "max_value").and_then(|v| {
            if let CommandDataOptionValue::Integer(i) = v {
                Some(*i as u64)
            } else {
                None
            }
        });
        if min_value.is_some() || max_value.is_some() {
            filters.push(Filter::Simple(SimpleFilter::TotalValue {
                min: min_value,
                max: max_value,
            }));
        }

        let target = match get_option_value(options, "target") {
            Some(CommandDataOptionValue::String(s)) => match s.as_str() {
                "attacker" => Target::Attacker,
                "victim" => Target::Victim,
                _ => Target::Any,
            },
            _ => Target::Any,
        };

        if let Some(ids) = parse_ids::<u32>(options, "region_ids") {
            filters.push(Filter::Simple(SimpleFilter::Region(ids)));
        }
        if let Some(ids) = parse_ids::<u32>(options, "system_ids") {
            filters.push(Filter::Simple(SimpleFilter::System(ids)));
        }
        if let Some(ids) = parse_ids::<u64>(options, "alliance_ids") {
            filters.push(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::Alliance(ids),
                target,
            }));
        }
        if let Some(ids) = parse_ids::<u64>(options, "corp_ids") {
            filters.push(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::Corporation(ids),
                target,
            }));
        }
        if let Some(ids) = parse_ids::<u64>(options, "char_ids") {
            filters.push(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::Character(ids),
                target,
            }));
        }
        if let Some(ids) = parse_ids::<u32>(options, "ship_type_ids") {
            filters.push(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::ShipType(ids),
                target,
            }));
        }
        if let Some(ids) = parse_ids::<u32>(options, "ship_group_ids") {
            filters.push(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::ShipGroup(ids),
                target,
            }));
        }
        if let Some(CommandDataOptionValue::Boolean(b)) = get_option_value(options, "is_npc") {
            filters.push(Filter::Simple(SimpleFilter::IsNpc(*b)));
        }
        if let Some(CommandDataOptionValue::Boolean(b)) = get_option_value(options, "is_solo") {
            filters.push(Filter::Simple(SimpleFilter::IsSolo(*b)));
        }

        if let Some(CommandDataOptionValue::String(json_str)) =
            get_option_value(options, "ly_ranges_json")
        {
            match serde_json::from_str::<Vec<SystemRange>>(json_str) {
                Ok(system_ranges) => {
                    if !system_ranges.is_empty() {
                        filters.push(Filter::Simple(SimpleFilter::LyRangeFrom(system_ranges)));
                    }
                }
                Err(e) => {
                    // Send a reply to the user that their JSON was invalid.
                    if let Err(why) = command
                        .create_interaction_response(&ctx.http, |response| {
                            response.interaction_response_data(|message| {
                                message
                                    .content(format!(
                                        "Invalid JSON format for ly_ranges_json: {}",
                                        e
                                    ))
                                    .ephemeral(true)
                            })
                        })
                        .await
                    {
                        error!("Cannot respond to slash command: {}", why);
                        return;
                    }
                }
            }
        }

        let min_pilots = get_option_value(options, "min_pilots").and_then(|v| {
            if let CommandDataOptionValue::Integer(i) = v {
                Some(*i as u32)
            } else {
                None
            }
        });
        let max_pilots = get_option_value(options, "max_pilots").and_then(|v| {
            if let CommandDataOptionValue::Integer(i) = v {
                Some(*i as u32)
            } else {
                None
            }
        });
        if min_pilots.is_some() || max_pilots.is_some() {
            filters.push(Filter::Simple(SimpleFilter::Pilots {
                min: min_pilots,
                max: max_pilots,
            }));
        }

        if let Some(CommandDataOptionValue::String(s)) = get_option_value(options, "name_fragment")
        {
            filters.push(Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::NameFragment(s.clone()),
            }));
        }

        let time_range_start = get_option_value(options, "time_range_start").and_then(|v| {
            if let CommandDataOptionValue::Integer(i) = v {
                Some(*i as u32)
            } else {
                None
            }
        });
        let time_range_end = get_option_value(options, "time_range_end").and_then(|v| {
            if let CommandDataOptionValue::Integer(i) = v {
                Some(*i as u32)
            } else {
                None
            }
        });
        if let (Some(start), Some(end)) = (time_range_start, time_range_end) {
            filters.push(Filter::Simple(SimpleFilter::TimeRange { start, end }));
        }

        if let Some(CommandDataOptionValue::String(s)) = get_option_value(options, "security") {
            filters.push(Filter::Simple(SimpleFilter::Security(s.clone())));
        }

        let max_ping_delay_minutes =
            get_option_value(options, "max_ping_delay_minutes").and_then(|v| {
                if let CommandDataOptionValue::Integer(i) = v {
                    Some(*i as u32)
                } else {
                    None
                }
            });
        let ping_type = get_option_value(options, "ping_type").and_then(|v| {
            if let CommandDataOptionValue::String(s) = v {
                match s.as_str() {
                    "here" => Some(PingType::Here {
                        max_ping_delay_minutes,
                    }),
                    "everyone" => Some(PingType::Everyone {
                        max_ping_delay_minutes,
                    }),
                    _ => None,
                }
            } else {
                None
            }
        });

        let root_filter = if filters.len() > 1 {
            FilterNode::And(filters.into_iter().map(FilterNode::Condition).collect())
        } else {
            filters
                .pop()
                .map(FilterNode::Condition)
                .unwrap_or(FilterNode::And(vec![])) // Match all if no filters
        };

        let new_sub = Subscription {
            id: id.clone(),
            description,
            root_filter,
            action: Action {
                channel_id: command.channel_id.0.to_string(),
                ping_type,
            },
        };
        let command_channel_id_str = command.channel_id.to_string();

        let _lock = app_state.subscriptions_file_lock.lock().await;
        let response_content = {
            let mut subs_map = app_state.subscriptions.write().unwrap();
            let guild_subs = subs_map.entry(guild_id).or_default();

            guild_subs
                .retain(|sub| sub.id != id || sub.action.channel_id != command_channel_id_str);
            guild_subs.push(new_sub);

            match save_subscriptions_for_guild(guild_id, guild_subs) {
                Ok(_) => format!("Subscription '{}' created/updated successfully!", id),
                Err(e) => {
                    error!("Failed to save subscriptions for guild {}: {}", guild_id, e);
                    format!("Error saving subscription '{}'.", id)
                }
            }
        };
        drop(_lock);

        if let Err(why) = command
            .create_interaction_response(&ctx.http, |response| {
                response.interaction_response_data(|message| {
                    message.content(response_content).ephemeral(true)
                })
            })
            .await
        {
            error!("Cannot respond to slash command: {}", why);
        }
    }
}
