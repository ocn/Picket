use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::sync::Arc;
use crate::commands::Command;
use crate::config::AppState;

pub struct DiagCommand;

#[async_trait]
impl Command for DiagCommand {
    fn name(&self) -> String {
        "diag".to_string()
    }

    fn register<'a>(&self, command: &'a mut CreateApplicationCommand) -> &'a mut CreateApplicationCommand {
        command
            .name("diag")
            .description("Show diagnostic information for subscriptions in this channel.")
    }

    async fn execute(&self, ctx: &Context, command: &ApplicationCommandInteraction, app_state: &Arc<AppState>) {
        let guild_id = match command.guild_id {
            Some(id) => id,
            None => {
                // Handle case where command is not used in a guild
                return;
            }
        };

        let response_content = { // Scoped to drop the lock guard
            let subscriptions = app_state.subscriptions.read().unwrap();
            let guild_subs = subscriptions.get(&guild_id);

            if let Some(subs) = guild_subs {
                let channel_subs: Vec<_> = subs.iter()
                    .filter(|s| s.action.channel_id == command.channel_id.0)
                    .collect();

                if !channel_subs.is_empty() {
                    let mut content = "Subscriptions for this channel:\n".to_string();
                    for sub in channel_subs {
                        content.push_str(&format!("- ID: `{}`, Description: `{}`\n", sub.id, sub.description));
                    }
                    content
                } else {
                    "No subscriptions found for this channel.".to_string()
                }
            } else {
                "No subscriptions found for this guild.".to_string()
            }
        };

        if let Err(why) = command
            .create_interaction_response(&ctx.http, |response| {
                response.interaction_response_data(|message| message.content(response_content).ephemeral(true))
            })
            .await
        {
            tracing::error!("Cannot respond to slash command: {}", why);
        }
    }
}