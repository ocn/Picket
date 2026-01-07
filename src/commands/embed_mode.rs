use crate::commands::Command;
use crate::config::{save_subscriptions_for_guild, AppState, EmbedMode};
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;

pub struct EmbedModeCommand;

#[async_trait]
impl Command for EmbedModeCommand {
    fn name(&self) -> String {
        "embed-mode".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("embed-mode")
            .description("Change the embed display mode for a subscription")
            .create_option(|opt| {
                opt.name("subscription_id")
                    .description("The subscription ID to modify")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|opt| {
                opt.name("mode")
                    .description("The embed mode to use")
                    .kind(CommandOptionType::String)
                    .required(true)
                    .add_string_choice("tracking", "tracking")
                    .add_string_choice("killfeed", "killfeed")
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
                let _ = command
                    .create_interaction_response(&ctx.http, |response| {
                        response.interaction_response_data(|message| {
                            message
                                .content("This command can only be used in a server.")
                                .ephemeral(true)
                        })
                    })
                    .await;
                return;
            }
        };

        // Extract arguments
        let subscription_id = command
            .data
            .options
            .iter()
            .find(|opt| opt.name == "subscription_id")
            .and_then(|opt| opt.resolved.as_ref())
            .and_then(|val| {
                if let CommandDataOptionValue::String(s) = val {
                    Some(s.clone())
                } else {
                    None
                }
            });

        let mode_str = command
            .data
            .options
            .iter()
            .find(|opt| opt.name == "mode")
            .and_then(|opt| opt.resolved.as_ref())
            .and_then(|val| {
                if let CommandDataOptionValue::String(s) = val {
                    Some(s.clone())
                } else {
                    None
                }
            });

        let (subscription_id, mode_str) = match (subscription_id, mode_str) {
            (Some(id), Some(mode)) => (id, mode),
            _ => {
                let _ = command
                    .create_interaction_response(&ctx.http, |response| {
                        response.interaction_response_data(|message| {
                            message
                                .content("Missing required arguments.")
                                .ephemeral(true)
                        })
                    })
                    .await;
                return;
            }
        };

        let embed_mode = match mode_str.as_str() {
            "tracking" => EmbedMode::Tracking,
            "killfeed" => EmbedMode::Killfeed,
            _ => {
                let _ = command
                    .create_interaction_response(&ctx.http, |response| {
                        response.interaction_response_data(|message| {
                            message
                                .content("Invalid mode. Use 'tracking' or 'killfeed'.")
                                .ephemeral(true)
                        })
                    })
                    .await;
                return;
            }
        };

        let channel_id_str = command.channel_id.0.to_string();

        let response_content = {
            let _lock = app_state.subscriptions_file_lock.lock().await;
            let mut subs_map = app_state.subscriptions.write().unwrap();

            if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
                // Find subscription by ID in current channel
                if let Some(sub) = guild_subs.iter_mut().find(|s| {
                    s.id == subscription_id && s.action.channel_id == channel_id_str
                }) {
                    let old_mode = sub.action.embed_mode;
                    sub.action.embed_mode = embed_mode;

                    if let Err(e) = save_subscriptions_for_guild(guild_id, guild_subs) {
                        format!("Failed to save subscription: {}", e)
                    } else {
                        format!(
                            "Updated subscription `{}` embed mode from `{:?}` to `{:?}`.",
                            subscription_id, old_mode, embed_mode
                        )
                    }
                } else {
                    format!(
                        "No subscription with ID `{}` found in this channel.",
                        subscription_id
                    )
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
            tracing::error!("Cannot respond to slash command: {}", why);
        }
    }
}
