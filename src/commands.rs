use crate::config::AppState;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOption, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub mod diag;
pub mod subscribe;
pub mod sync_standings;
pub mod sync_remove;
pub mod sync_clear;
pub mod unsubscribe;

#[async_trait]
pub trait Command: Send + Sync {
    fn name(&self) -> String;
    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand;
    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    );
}

// --- HELPER FUNCTIONS ---

pub fn get_option_value<'a>(
    options: &'a [CommandDataOption],
    name: &str,
) -> Option<&'a CommandDataOptionValue> {
    options
        .iter()
        .find(|opt| opt.name == name)
        .and_then(|opt| opt.resolved.as_ref())
}

// --- PING COMMAND (for testing) ---

pub struct PingCommand;

#[async_trait]
impl Command for PingCommand {
    fn name(&self) -> String {
        "ping".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command.name("ping").description("A simple ping command")
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        _app_state: &Arc<AppState>,
    ) {
        if let Err(why) = command
            .create_interaction_response(&ctx.http, |response| {
                response.interaction_response_data(|message| message.content("Pong!"))
            })
            .await
        {
            error!("Cannot respond to slash command: {}", why);
        }
    }
}
