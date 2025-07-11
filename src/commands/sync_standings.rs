use crate::commands::Command;
use crate::config::{AppState, SsoState, StandingSource};
use rand::distributions::Alphanumeric;
use rand::Rng;
use serenity::async_trait;
use serenity::builder::{CreateActionRow, CreateButton, CreateSelectMenu};
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;
use serenity::builder::CreateApplicationCommand;

pub struct SyncStandingsCommand;

fn generate_state_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}

#[async_trait]
impl Command for SyncStandingsCommand {
    fn name(&self) -> String {
        "sync_standings".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sync_standings")
            .description("Sync a subscription with your EVE contacts to ignore blues.")
            .create_option(|option| {
                option
                    .name("subscription_id")
                    .description("The ID of the subscription to sync.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("source")
                    .description("The source of the standings to use for filtering.")
                    .kind(CommandOptionType::String)
                    .required(true)
                    .add_string_choice("Character", "character")
                    .add_string_choice("Corporation", "corporation")
                    .add_string_choice("Alliance", "alliance")
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    ) {
        if let Err(why) = command
            .create_interaction_response(&ctx.http, |response| {
                response.interaction_response_data(|message| {
                    message
                        .content("Please check your Direct Messages to complete the process.")
                        .ephemeral(true)
                })
            })
            .await
        {
            error!("Cannot respond to slash command: {}", why);
            return;
        }

        let subscription_id = command.data.options[0].value.as_ref().unwrap().as_str().unwrap().to_string();

        let standing_source = match command.data.options[1].value.as_ref().unwrap().as_str().unwrap() {
            "corporation" => StandingSource::Corporation,
            "alliance" => StandingSource::Alliance,
            _ => StandingSource::Character,
        };

        let user_id = command.user.id;
        let existing_tokens: Vec<_> = {
            let standings_map = app_state.user_standings.read().unwrap();
            standings_map
                .get(&user_id)
                .map(|s| s.tokens.clone())
                .unwrap_or_default()
        };

        let state = generate_state_string();
        let sso_state = SsoState {
            discord_user_id: command.user.id,
            subscription_id,
            standing_source,
            original_interaction: command.clone(),
        };

        // Store the state so we can retrieve it when a component is clicked
        app_state
            .sso_states
            .lock()
            .await
            .insert(state.clone(), sso_state);

        if existing_tokens.is_empty() {
            self.initiate_sso(ctx, command, app_state, &state).await;
        } else {
            let mut select_menu = CreateSelectMenu::default();
            select_menu.custom_id(format!("standings_select_{}", state));
            select_menu.options(|f| {
                for token in existing_tokens {
                    f.create_option(|o| {
                        o.label(token.character_name).value(token.character_id.to_string())
                    });
                }
                f
            });

            let mut reauth_button = CreateButton::default();
            reauth_button.custom_id(format!("standings_reauth_{}", state));
            reauth_button.label("Authorize New Character");

            let mut components = CreateActionRow::default();
            components.add_select_menu(select_menu);
            let mut components2 = CreateActionRow::default();
            components2.add_button(reauth_button);

            if let Err(why) = command
                .user
                .direct_message(&ctx.http, |m| {
                    m.content("You have already authorized characters. Please choose one to sync with, or authorize a new one.")
                     .components(|c| {
                         c.add_action_row(components)
                          .add_action_row(components2)
                     })
                })
                .await
            {
                error!("Error sending DM with character selection: {:?}", why);
            }
        }
    }
}

impl SyncStandingsCommand {
    pub async fn initiate_sso(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
        state: &str,
    ) {
        let client_id = &app_state.app_config.eve_client_id;
        let redirect_uri = "https://github.headempty.space/";
        let scopes = "esi-corporations.read_contacts.v1 esi-alliances.read_contacts.v1";

        let sso_url = format!(
            "https://login.eveonline.com/v2/oauth/authorize?response_type=code&redirect_uri={}&client_id={}&scope={}&state={}",
            urlencoding::encode(redirect_uri),
            client_id,
            urlencoding::encode(scopes),
            &state
        );

        let instructions = format!(
            "Please follow this link to authorize the bot to read your standings. After authorizing, you will be redirected to a page on github.com. **Copy the entire URL from your browser's address bar and paste it back here in this DM.**\n\n{}",
            sso_url
        );

        if let Err(why) = command
            .user
            .direct_message(&ctx.http, |m| m.content(instructions))
            .await
        {
            error!("Error sending DM for SSO link: {:?}", why);
        }
    }
}
