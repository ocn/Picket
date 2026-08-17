use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::Command;
use crate::config::AppState;
use crate::contract_intelligence::{available_contract_store, HealthSnapshot};
use crate::ContractStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub const HEALTH_OPERATOR_ID: u64 = 146_451_271_497_416_704;
const MAX_HEALTH_RESPONSE_CHARS: usize = 1_900;

pub struct HealthCommand;

pub fn render_health_response(user_id: u64, snapshot: &HealthSnapshot) -> Option<String> {
    if user_id != HEALTH_OPERATOR_ID {
        return None;
    }
    let mut response = format!(
        "Health: {}\nObserved: {}",
        snapshot.status.as_str(),
        snapshot.observed_at.to_rfc3339(),
    );
    let mut omitted = 0;
    for check in &snapshot.checks {
        let line = format!(
            "\n- {}: {} — {}",
            check.key,
            check.status.as_str(),
            check.evidence.replace('@', "@\u{200b}"),
        );
        if response.chars().count() + line.chars().count() <= MAX_HEALTH_RESPONSE_CHARS {
            response.push_str(&line);
        } else {
            omitted += 1;
        }
    }
    if omitted > 0 {
        let suffix = format!("\n… {omitted} additional check(s) truncated");
        response = truncate_to_chars(
            &response,
            MAX_HEALTH_RESPONSE_CHARS.saturating_sub(suffix.chars().count()),
        );
        response.push_str(&suffix);
    }
    Some(response)
}

fn truncate_to_chars(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

#[async_trait]
impl Command for HealthCommand {
    fn name(&self) -> String {
        "health".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("health")
            .description("Show the current persisted contract-monitoring health snapshot.")
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        _app_state: &Arc<AppState>,
    ) {
        if command.user.id.0 != HEALTH_OPERATOR_ID {
            if let Err(error) = command
                .create_interaction_response(&ctx.http, |response| {
                    response.interaction_response_data(|message| {
                        message.content("Not authorized.").ephemeral(true)
                    })
                })
                .await
            {
                error!("Cannot respond to unauthorized health command: {error}");
            }
            return;
        }
        let store_handle = ctx
            .data
            .read()
            .await
            .get::<ContractStoreContainer>()
            .cloned();
        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let Some(store_handle) = store_handle else {
                return "Health storage is unavailable.".to_string();
            };
            let Some(store) = available_contract_store(&store_handle).await else {
                return "Health storage is unavailable.".to_string();
            };
            match store.health_snapshot().await {
                Ok(Some(snapshot)) => render_health_response(HEALTH_OPERATOR_ID, &snapshot)
                    .expect("configured operator can render health"),
                Ok(None) => "No health snapshot has been recorded yet.".to_string(),
                Err(error) => {
                    error!("cannot read persisted health snapshot: {error}");
                    "Health storage is unavailable.".to_string()
                }
            }
        })
        .await
        {
            error!("Cannot respond to health command: {error}");
        }
    }
}
