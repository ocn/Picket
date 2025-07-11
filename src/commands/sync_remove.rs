use crate::commands::{get_option_value, Command};
use crate::config::{save_subscriptions_for_guild, AppState, Filter, FilterNode, SimpleFilter};
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct SyncRemoveCommand;

#[async_trait]
impl Command for SyncRemoveCommand {
    fn name(&self) -> String {
        "sync_remove".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sync_remove")
            .description("Remove a standing synchronization from a subscription.")
            .create_option(|option| {
                option
                    .name("subscription_id")
                    .description("The ID of the subscription to remove the sync from.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    ) {
        let guild_id = match command.guild_id {
            Some(id) => id,
            None => {
                // Should not happen in a guild-based command
                return;
            }
        };

        let subscription_id = match get_option_value(&command.data.options, "subscription_id") {
            Some(CommandDataOptionValue::String(s)) => s,
            _ => return,
        };

        let command_channel_id_str = command.channel_id.to_string();
        
        let response_content = {
            let _lock = app_state.subscriptions_file_lock.lock().await;
            let mut subs_map = app_state.subscriptions.write().unwrap();

            if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
                if let Some(sub) = guild_subs.iter_mut().find(|s| {
                    s.id == *subscription_id && s.action.channel_id == command_channel_id_str
                }) {
                    let mut sync_found_and_removed = false;
                    if let FilterNode::And(ref mut conditions) = sub.root_filter {
                        let initial_len = conditions.len();
                        conditions.retain(|c| {
                            !matches!(
                                c,
                                FilterNode::Condition(Filter::Simple(
                                    SimpleFilter::IgnoreHighStanding { .. }
                                ))
                            )
                        });
                        if conditions.len() < initial_len {
                            sync_found_and_removed = true;
                        }
                    }

                    if sync_found_and_removed {
                        match save_subscriptions_for_guild(guild_id, guild_subs) {
                            Ok(_) => {
                                format!(
                                    "Successfully removed standing sync from subscription '{}'.",
                                    subscription_id
                                )
                            }
                            Err(e) => {
                                error!("Failed to save subscriptions after removing sync: {}", e);
                                "An error occurred while saving the updated subscription.".to_string()
                            }
                        }
                    } else {
                        format!(
                            "No standing sync was found on subscription '{}'.",
                            subscription_id
                        )
                    }
                } else {
                    "No matching subscription found.".to_string()
                }
            } else {
                "No subscriptions found for this guild.".to_string()
            }
        };

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
