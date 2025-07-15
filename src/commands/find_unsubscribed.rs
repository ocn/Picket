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
        // Step 1: Immediately defer the response to avoid the 3-second timeout.
        if let Err(why) = command.defer(&ctx.http).await {
            error!("Cannot defer interaction: {}", why);
            return;
        }

        let guild_id = match command.guild_id {
            Some(id) => id,
            None => {
                // This case should ideally not be reached due to guild-only command nature
                if let Err(why) = command
                    .edit_original_interaction_response(&ctx.http, |r| {
                        r.content("This command can only be used in a server.")
                    })
                    .await
                {
                    error!("Cannot edit original response: {}", why);
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
                    .edit_original_interaction_response(&ctx.http, |r| {
                        r.content("Error: Could not fetch channel list for this server.")
                    })
                    .await
                {
                    error!("Cannot edit original response: {}", why);
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
            if guild_channel.kind != ChannelType::Text {
                continue;
            }

            if !subscribed_channel_ids.contains(&channel_id.0) {
                let category_name = guild_channel
                    .parent_id
                    .as_ref()
                    .and_then(|cat_id| {
                        channels.get(cat_id).map(|c| c.name.clone())
                    })
                    .unwrap_or_else(|| "No Category".to_string());

                unsubscribed_channels.push(format!(
                    "- **{}** (Category: `{}`)",
                    guild_channel.name, category_name
                ));
            }
        }

        let response_content = if unsubscribed_channels.is_empty() {
            "All text channels in this server have at least one subscription.".to_string()
        } else {
            format!(
                "**Channels with no subscriptions:**\n{}",
                unsubscribed_channels.join("\n")
            )
        };

        // Step 2: Send the first (and possibly only) part of the response by editing the original deferred message.
        let chunks: Vec<String> = response_content
            .chars()
            .collect::<Vec<char>>()
            .chunks(2000)
            .map(|c| c.iter().collect())
            .collect();

        if let Some(first_chunk) = chunks.first() {
            if let Err(why) = command
                .edit_original_interaction_response(&ctx.http, |r| r.content(first_chunk))
                .await
            {
                error!("Cannot edit original response: {}", why);
            }
        }

        // Step 3: If there are more parts, send them as followup messages.
        if chunks.len() > 1 {
            for chunk in chunks.iter().skip(1) {
                if let Err(why) = command
                    .create_followup_message(&ctx.http, |m| m.content(chunk))
                    .await
                {
                    error!("Cannot send followup message: {}", why);
                }
            }
        }
    }
}
