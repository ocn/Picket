use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::{ApplicationCommandInteraction, CommandDataOptionValue};
use serenity::prelude::Context;
use std::sync::Arc;
use crate::commands::{Command, get_option_value};
use crate::config::{AppState, save_subscriptions_for_guild};
use serenity::model::prelude::command::CommandOptionType;
use tracing::error;

pub struct UnsubscribeCommand;

#[async_trait]
impl Command for UnsubscribeCommand {
    fn name(&self) -> String {
        "unsubscribe".to_string()
    }

    fn register<'a>(&self, command: &'a mut CreateApplicationCommand) -> &'a mut CreateApplicationCommand {
        command
            .name("unsubscribe")
            .description("Remove a killmail subscription.")
            .create_option(|option| {
                option
                    .name("id")
                    .description("The unique identifier of the subscription to remove.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
    }

    async fn execute(&self, ctx: &Context, command: &ApplicationCommandInteraction, app_state: &Arc<AppState>) {
        let guild_id = match command.guild_id {
            Some(id) => id,
            None => { return; }
        };

        let id_to_remove = match get_option_value(&command.data.options, "id") {
            Some(CommandDataOptionValue::String(s)) => s,
            _ => { return; }
        };

        let channel_id_str = command.channel_id.to_string();

        let response_content = {
            let mut subs_map = app_state.subscriptions.write().unwrap();
            if let Some(guild_subs) = subs_map.get_mut(&guild_id) {
                let initial_len = guild_subs.len();
                guild_subs.retain(|sub| sub.id != *id_to_remove || sub.action.channel_id != channel_id_str);

                if guild_subs.len() < initial_len {
                    match save_subscriptions_for_guild(guild_id, guild_subs) {
                        Ok(_) => format!("Successfully removed subscription '{}'.", id_to_remove),
                        Err(e) => {
                            error!("Failed to save subscriptions after removal for guild {}: {}", guild_id, e);
                            format!("Error saving changes after removing subscription '{}'.", id_to_remove)
                        }
                    }
                } else {
                    format!("No subscription found with ID '{}'.", id_to_remove)
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
            error!("Cannot respond to slash command: {}", why);
        }
    }
}
