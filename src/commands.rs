use crate::config::AppState;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOption, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub mod contract_command;
pub mod contract_subscribe;
pub mod contract_unsubscribe;
pub mod diag;
pub mod find_unsubscribed;
pub mod health;
pub mod sov_subscribe;
pub mod sov_timers;
pub mod sov_unsubscribe;
pub mod subscribe;
pub mod sync_clear;
pub mod sync_remove;
pub mod sync_standings;
pub mod unsubscribe;
pub mod watch;
pub mod watch_subscribe;
pub mod watch_unsubscribe;

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

#[cfg(test)]
mod permission_gating_tests {
    //! Ticket 16: every subscription-management command must register
    //! `default_member_permissions(MANAGE_GUILD)` and `dm_permission(false)`,
    //! while the read-open commands (`ping`, `diag`, `sov_timers`, `health`)
    //! must carry neither key. Each test builds the command's registration
    //! JSON and inspects the raw builder map, following the existing
    //! `sov_subscribe_exposes_bounded_options` pattern.
    use super::contract_subscribe::ContractSubscribeCommand;
    use super::contract_unsubscribe::ContractUnsubscribeCommand;
    use super::diag::DiagCommand;
    use super::find_unsubscribed::FindUnsubscribedChannelsCommand;
    use super::health::HealthCommand;
    use super::sov_subscribe::SovSubscribeCommand;
    use super::sov_timers::SovTimersCommand;
    use super::sov_unsubscribe::SovUnsubscribeCommand;
    use super::subscribe::SubscribeCommand;
    use super::sync_clear::SyncClearCommand;
    use super::sync_remove::SyncRemoveCommand;
    use super::sync_standings::SyncStandingsCommand;
    use super::unsubscribe::UnsubscribeCommand;
    use super::watch::WatchCommand;
    use super::watch_subscribe::WatchSubscribeCommand;
    use super::watch_unsubscribe::WatchUnsubscribeCommand;
    use super::{Command, PingCommand};
    use serenity::builder::CreateApplicationCommand;
    use serenity::model::Permissions;

    fn built(command: &dyn Command) -> CreateApplicationCommand {
        let mut builder = CreateApplicationCommand::default();
        command.register(&mut builder);
        builder
    }

    fn assert_gated_on_manage_guild(command: &dyn Command) {
        let name = command.name();
        let builder = built(command);
        assert_eq!(
            builder
                .0
                .get("default_member_permissions")
                .and_then(|value| value.as_str()),
            Some(Permissions::MANAGE_GUILD.bits().to_string().as_str()),
            "/{name} must register default_member_permissions == MANAGE_GUILD"
        );
        assert_eq!(
            builder
                .0
                .get("dm_permission")
                .and_then(|value| value.as_bool()),
            Some(false),
            "/{name} must register dm_permission == false"
        );
    }

    fn assert_no_default_permission(command: &dyn Command) {
        let name = command.name();
        let builder = built(command);
        assert!(
            builder.0.get("default_member_permissions").is_none(),
            "/{name} must not set a default member permission"
        );
        assert!(
            builder.0.get("dm_permission").is_none(),
            "/{name} must not set dm_permission"
        );
    }

    #[test]
    fn subscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&SubscribeCommand);
    }

    #[test]
    fn unsubscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&UnsubscribeCommand);
    }

    #[test]
    fn contract_subscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&ContractSubscribeCommand);
    }

    #[test]
    fn contract_unsubscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&ContractUnsubscribeCommand);
    }

    #[test]
    fn sov_subscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&SovSubscribeCommand);
    }

    #[test]
    fn sov_unsubscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&SovUnsubscribeCommand);
    }

    #[test]
    fn watch_group_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&WatchCommand);
    }

    #[test]
    fn watch_subscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&WatchSubscribeCommand);
    }

    #[test]
    fn watch_unsubscribe_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&WatchUnsubscribeCommand);
    }

    #[test]
    fn sync_standings_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&SyncStandingsCommand);
    }

    #[test]
    fn sync_remove_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&SyncRemoveCommand);
    }

    #[test]
    fn sync_clear_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&SyncClearCommand);
    }

    #[test]
    fn find_unsubscribed_is_gated_on_manage_guild() {
        assert_gated_on_manage_guild(&FindUnsubscribedChannelsCommand);
    }

    #[test]
    fn read_open_commands_carry_no_default_permission() {
        assert_no_default_permission(&PingCommand);
        assert_no_default_permission(&DiagCommand);
        assert_no_default_permission(&SovTimersCommand);
        assert_no_default_permission(&HealthCommand);
    }
}
