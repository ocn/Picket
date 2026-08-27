//! `/sov_timers`: an ephemeral, on-demand board of the live (non-ended)
//! sov campaigns that match one channel's subscriptions right now (spec
//! "Sov timer subscription language": "Commands use underscore names:
//! ... `/sov_timers` (ephemeral)"; ticket
//! `.scratch/esi-intel-feeds/issues/04-stargate-reachability-and-sov-timers-board.md`).
//!
//! Unlike the collector's alert stages, this command does no dedup and
//! sends no delivery row: it is a synchronous snapshot, computed the same
//! way `render_stage_message` computes an alert (filter evaluation against
//! the same [`crate::sov_feed::SovReachabilitySource`] and
//! [`crate::sov_feed::SovSystemDirectory`]), rendered into one bounded
//! plain-text reply.
//!
//! [`render_sov_timers`] is pure -- it takes already-resolved
//! [`SovTimerEntry`] rows, not a Discord context or the store -- so it is
//! unit-tested directly, mirroring the pure/impure split in
//! `sov_subscribe.rs`'s `subscription_from_documents`.

use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::Command;
use crate::config::AppState;
use crate::discord_bot::{DiscordSovSystemDirectory, DiscordSovTickerResolver};
use crate::sov_feed::{
    available_sov_store, SovReachabilitySource, SovStoreHandle, SovSystemDirectory,
    SovTickerResolver, StaticSovReachability,
};
use crate::{SovReachabilityContainer, SovStoreContainer};
use chrono::{DateTime, Utc};
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::interaction::application_command::ApplicationCommandInteraction;
use serenity::prelude::Context;
use std::sync::Arc;
use tracing::error;

pub struct SovTimersCommand;

/// One resolved row of the `/sov_timers` board.
pub struct SovTimerEntry {
    pub system_name: String,
    pub region_name: String,
    pub defender_ticker: String,
    pub start_time: DateTime<Utc>,
    /// Already-rendered jumps text: `"N jumps"`, `"unreachable"`, or
    /// `"graph unavailable"`.
    pub jumps_text: String,
}

/// Practical content budget for the rendered reply, leaving headroom
/// under Discord's 2000-character interaction response limit for the
/// header line and a possible "... and N more" trailer.
const SOV_TIMERS_CONTENT_BUDGET: usize = 1900;

/// Renders the `/sov_timers` board from already-resolved entries (sorted
/// by the caller, soonest start time first). Bounded: once adding another
/// row would exceed [`SOV_TIMERS_CONTENT_BUDGET`], the remaining rows
/// collapse into a single "... and N more" trailer rather than growing the
/// reply past Discord's limit (spec acceptance: "bounded output ...
/// truncate with a count of omitted rows"). The first row is always shown
/// regardless of its own length, so a single very long row can never
/// produce an empty board.
pub fn render_sov_timers(entries: &[SovTimerEntry]) -> String {
    if entries.is_empty() {
        return "No live sov campaigns currently match this channel's subscriptions.".to_string();
    }
    let mut lines = vec!["Live sov campaigns matching this channel's subscriptions:".to_string()];
    let mut shown = 0usize;
    for entry in entries {
        let row = format_row(entry);
        let projected: usize =
            lines.iter().map(|line| line.len() + 1).sum::<usize>() + row.len() + 1;
        if shown > 0 && projected > SOV_TIMERS_CONTENT_BUDGET {
            break;
        }
        lines.push(row);
        shown += 1;
    }
    if shown < entries.len() {
        lines.push(format!("... and {} more", entries.len() - shown));
    }
    lines.join("\n")
}

fn format_row(entry: &SovTimerEntry) -> String {
    let unix = entry.start_time.timestamp();
    format!(
        "**{}** ({}) \u{2014} defender {} \u{2014} {} EVE <t:{unix}:R> \u{2014} {}",
        entry.system_name,
        entry.region_name,
        entry.defender_ticker,
        entry.start_time.format("%Y-%m-%d %H:%M:%S"),
        entry.jumps_text,
    )
}

#[async_trait]
impl Command for SovTimersCommand {
    fn name(&self) -> String {
        "sov_timers".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("sov_timers")
            .description("List live sov campaigns matching this channel's subscriptions.")
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    ) {
        let guild_id = command.guild_id;
        let channel_id = command.channel_id.0;
        let data = ctx.data.read().await;
        let store_handle = data.get::<SovStoreContainer>().cloned();
        let reachability = data.get::<SovReachabilityContainer>().cloned();
        drop(data);
        let app_state = app_state.clone();

        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let Some(guild_id) = guild_id else {
                return "/sov_timers can only be used in a server channel.".to_string();
            };
            render_for_channel(
                &app_state,
                store_handle,
                reachability,
                guild_id.0,
                channel_id,
            )
            .await
        })
        .await
        {
            error!("Cannot respond to sov timers command: {error}");
        }
    }
}

async fn render_for_channel(
    app_state: &Arc<AppState>,
    store_handle: Option<SovStoreHandle>,
    reachability: Option<Arc<dyn SovReachabilitySource>>,
    guild_id: u64,
    channel_id: u64,
) -> String {
    let Some(store_handle) = store_handle else {
        return "Sov timers are unavailable because the sov database is not connected.".to_string();
    };
    let Some(store) = available_sov_store(&store_handle).await else {
        return "Sov timers are unavailable because the sov database is not connected.".to_string();
    };
    let reachability: Arc<dyn SovReachabilitySource> =
        reachability.unwrap_or_else(|| Arc::new(StaticSovReachability(None)));

    let subscriptions = match store.subscriptions_for_channel(guild_id, channel_id).await {
        Ok(subscriptions) => subscriptions,
        Err(error) => {
            error!("cannot load sov subscriptions for /sov_timers: {error}");
            return "Sov timers could not be loaded.".to_string();
        }
    };
    if subscriptions.is_empty() {
        return "No sov subscriptions are configured for this channel.".to_string();
    }

    let campaigns = match store.open_campaigns().await {
        Ok(campaigns) => campaigns,
        Err(error) => {
            error!("cannot load open sov campaigns for /sov_timers: {error}");
            return "Sov timers could not be loaded.".to_string();
        }
    };

    let directory = DiscordSovSystemDirectory::new(app_state.clone());
    let tickers = DiscordSovTickerResolver::new(app_state.clone());
    let observed_at = Utc::now();
    let region_of = |system_id: i64| directory.resolve(system_id).map(|info| info.region_id);
    let reachable_jumps = |system_id: i64| reachability.reachable(system_id).map(|info| info.jumps);

    let mut matched: Vec<_> = campaigns
        .into_iter()
        .filter(|campaign| {
            subscriptions.iter().any(|subscription| {
                subscription.filter.root.matches(
                    campaign,
                    observed_at,
                    &region_of,
                    &reachable_jumps,
                )
            })
        })
        .collect();
    matched.sort_by_key(|campaign| campaign.start_time);

    let mut entries = Vec::with_capacity(matched.len());
    for campaign in matched {
        let system_info = directory.resolve(campaign.solar_system_id);
        let (system_name, region_name) = system_info
            .map(|info| (info.name, info.region_name))
            .unwrap_or_else(|| {
                (
                    campaign.solar_system_id.to_string(),
                    "Unknown Region".to_string(),
                )
            });
        let defender_ticker = match campaign.defender_id {
            Some(alliance_id) => tickers
                .alliance_ticker(alliance_id)
                .await
                .unwrap_or_else(|| "Unknown".to_string()),
            None => "Unknown".to_string(),
        };
        let jumps_text = match reachability.reachable(campaign.solar_system_id) {
            Some(info) => format!(
                "{} jump{}",
                info.jumps,
                if info.jumps == 1 { "" } else { "s" }
            ),
            None if reachability.graph_available() => "unreachable".to_string(),
            None => "graph unavailable".to_string(),
        };
        entries.push(SovTimerEntry {
            system_name,
            region_name,
            defender_ticker,
            start_time: campaign.start_time,
            jumps_text,
        });
    }

    render_sov_timers(&entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(system_name: &str, jumps_text: &str) -> SovTimerEntry {
        SovTimerEntry {
            system_name: system_name.to_string(),
            region_name: "Tenal".to_string(),
            defender_ticker: "TEST".to_string(),
            start_time: DateTime::parse_from_rfc3339("2026-08-27T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            jumps_text: jumps_text.to_string(),
        }
    }

    #[test]
    fn render_sov_timers_reports_the_clear_empty_case() {
        assert_eq!(
            render_sov_timers(&[]),
            "No live sov campaigns currently match this channel's subscriptions."
        );
    }

    #[test]
    fn render_sov_timers_renders_every_row_when_within_budget() {
        let entries = vec![entry("Turnur", "1 jump"), entry("Jita", "4 jumps")];
        let rendered = render_sov_timers(&entries);
        assert!(rendered.contains("Turnur"));
        assert!(rendered.contains("1 jump"));
        assert!(rendered.contains("Jita"));
        assert!(rendered.contains("4 jumps"));
        assert!(!rendered.contains("more"));
    }

    #[test]
    fn render_sov_timers_shows_unreachable_and_graph_unavailable_wording_distinctly() {
        let entries = vec![
            entry("Turnur", "unreachable"),
            entry("Jita", "graph unavailable"),
        ];
        let rendered = render_sov_timers(&entries);
        assert!(rendered.contains("unreachable"));
        assert!(rendered.contains("graph unavailable"));
    }

    #[test]
    fn render_sov_timers_truncates_with_an_omitted_count_when_over_budget() {
        // Each row is padded well past the content budget divided by a
        // plausible row count, so a handful of rows already exceeds it.
        let long_name = "X".repeat(400);
        let entries: Vec<SovTimerEntry> = (0..10).map(|_| entry(&long_name, "1 jump")).collect();
        let rendered = render_sov_timers(&entries);
        assert!(rendered.len() < 2000);
        assert!(rendered.contains("more"));
        // At least the first row is always shown even though it alone is
        // large, so the board is never empty just because rows are long.
        assert!(rendered.contains(&long_name));
    }
}
