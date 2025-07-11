use crate::commands::Command;
use crate::config::{save_user_standings, AppState};
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct SyncClearCommand;

#[async_trait]
impl Command for SyncClearCommand {
    fn name(&self) -> String {
        "sync_clear".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sync_clear")
            .description("Clears all of your saved EVE character data and contact lists.")
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    ) {
        let user_id = command.user.id;
        
        let response_content = {
            let _lock = app_state.user_standings_file_lock.lock().await;
            let mut standings_map = app_state.user_standings.write().unwrap();

            if standings_map.remove(&user_id).is_some() {
                save_user_standings(&standings_map);
                "Successfully cleared all of your saved character tokens and contact lists. Any existing syncs will no longer work.".to_string()
            } else {
                "You had no saved data to clear.".to_string()
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
