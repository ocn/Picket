use crate::commands::Command;
use crate::config::AppState;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::channel::ChannelType;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::error;

pub struct FindUnsubscribedChannelsCommand;

#[async_trait]
impl Command for FindUnsubscribedChannelsCommand {
    fn name(&self) -> String {
        "find_unsubscribed".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("find_unsubscribed")
            .description("Finds all channels in this server that have no active subscriptions.")
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
                if let Err(why) = command
                    .create_interaction_response(&ctx.http, |r| {
                        r.interaction_response_data(|m| {
                            m.content("This command can only be used in a server.")
                                .ephemeral(true)
                        })
                    })
                    .await
                {
                    error!("Cannot respond to slash command: {}", why);
                }
                return;
            }
        };

        // Get all channels in the guild
        let channels = match guild_id.channels(&ctx.http).await {
            Ok(ch) => ch,
            Err(e) => {
                error!("Could not fetch channels for guild {}: {}", guild_id, e);
                if let Err(why) = command
                    .create_interaction_response(&ctx.http, |r| {
                        r.interaction_response_data(|m| {
                            m.content("Error: Could not fetch channel list for this server.")
                                .ephemeral(true)
                        })
                    })
                    .await
                {
                    error!("Cannot respond to slash command: {}", why);
                }
                return;
            }
        };

        // Get all channel IDs that have at least one subscription
        let subscribed_channel_ids: HashSet<u64> = {
            let subs_map = app_state.subscriptions.read().unwrap();
            subs_map
                .get(&guild_id)
                .map(|guild_subs| {
                    guild_subs
                        .iter()
                        .filter_map(|sub| sub.action.channel_id.parse::<u64>().ok())
                        .collect()
                })
                .unwrap_or_default()
        };

        let mut unsubscribed_channels = Vec::new();
        for (channel_id, guild_channel) in &channels {
            // We only care about standard text channels
            if guild_channel.kind != ChannelType::Text {
                continue;
            }

            if !subscribed_channel_ids.contains(&channel_id.0) {
                let category_name = guild_channel
                    .parent_id
                    .as_ref()
                    .and_then(|cat_id| channels.get(cat_id))
                    .map_or("No Category".to_string(), |cat| cat.name.clone());

                unsubscribed_channels.push(format!(
                    "- **{}** (Category: `{}`)",
                    guild_channel.name, category_name
                ));
            }
        }

        let response_content = if unsubscribed_channels.is_empty() {
            "All channels in this server have at least one subscription.".to_string()
        } else {
            format!(
                "**Channels with no subscriptions:**\n{}",
                unsubscribed_channels.join("\n")
            )
        };

        // Discord has a 2000 character limit for messages. We'll send in chunks if needed.
        for chunk in response_content.chars().collect::<Vec<char>>().chunks(2000) {
            let chunk_str: String = chunk.iter().collect();
            if let Err(why) = command
                .create_followup_message(&ctx.http, |m| m.content(&chunk_str).ephemeral(true))
                .await
            {
                error!("Cannot send followup message: {}", why);
            }
        }

        // Send initial "thinking" response to prevent timeout
        if let Err(why) = command
            .create_interaction_response(&ctx.http, |r| {
                r.interaction_response_data(|m| m.content("Scanning...").ephemeral(true))
            })
            .await
        {
            error!("Cannot respond to slash command: {}", why);
        }
    }
}
