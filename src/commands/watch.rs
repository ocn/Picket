//! `/watch add|remove|list`: manage a guild's registry of Watched Entities
//! (ticket 08). `add` resolves a ticker through the killfeed ticker cache
//! first, then an ESI name lookup, and confirms the resolved name and id
//! ephemerally; duplicate adds are idempotent. No tags, groups, or
//! per-entity options.

use crate::commands::contract_command::{defer_then_edit, SerenityContractCommandResponder};
use crate::commands::{get_option_value, Command};
use crate::config::AppState;
use crate::watchlist_feed::{available_watchlist_store, WatchedEntity, WatchlistKind};
use crate::WatchlistStoreContainer;
use serenity::async_trait;
use serenity::builder::CreateApplicationCommand;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::interaction::application_command::{
    ApplicationCommandInteraction, CommandDataOption, CommandDataOptionValue,
};
use serenity::prelude::Context;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub struct WatchCommand;

const NOT_CONNECTED: &str =
    "Watchlist is unavailable because the watchlist database is not connected.";

/// One resolved entity ready to store, with the display text for the
/// ephemeral confirmation.
struct ResolvedEntity {
    id: i64,
    name: Option<String>,
    ticker: Option<String>,
}

impl WatchCommand {
    fn kind_option(options: &[CommandDataOption]) -> Result<WatchlistKind, String> {
        match get_option_value(options, "kind") {
            Some(CommandDataOptionValue::String(value)) => WatchlistKind::parse(value),
            _ => Err("missing required option: kind".to_string()),
        }
    }

    fn ticker_option(options: &[CommandDataOption]) -> Result<String, String> {
        match get_option_value(options, "ticker") {
            Some(CommandDataOptionValue::String(value)) => Ok(value.trim().to_string()),
            _ => Err("missing required option: ticker".to_string()),
        }
    }

    /// Resolves a ticker (or full name) to an entity id/name/ticker: the
    /// killfeed ticker cache first (reverse lookup on the cached ticker
    /// string), then `POST /universe/ids/` by name (ESI resolves names, not
    /// tickers). `None` when neither path resolves.
    ///
    /// The ticker cache mixes alliances and corporations keyed only by the
    /// ticker string, and several entities can share a ticker, so a bare
    /// cache hit is not trustworthy on its own (fix round finding 3). Every
    /// candidate id that matches the ticker is confirmed to be of the
    /// requested kind through ESI public info (`GET /alliances/{id}/` or
    /// `GET /corporations/{id}/` via `get_ticker`; a 404 means the id is the
    /// wrong kind) before it is accepted. The lookup is bounded to a handful
    /// of candidates, and falls back to the name path when none confirm.
    async fn resolve(
        app_state: &Arc<AppState>,
        kind: WatchlistKind,
        query: &str,
    ) -> Option<ResolvedEntity> {
        let candidate_ids = {
            let tickers = app_state.tickers.read().unwrap();
            candidate_ids_for_ticker(&tickers, query)
        };
        for id in candidate_ids {
            // Confirm the candidate is of the requested kind: `get_ticker`
            // hits the kind-specific public-info endpoint, which 404s (Err)
            // when the id is the other kind. On confirmation, prefer the ESI
            // ticker (authoritative) over the query string.
            match app_state
                .esi_client
                .get_ticker(id, kind.is_alliance())
                .await
            {
                Ok(ticker) => {
                    let name = crate::discord_bot::get_name(app_state, id).await;
                    return Some(ResolvedEntity {
                        id: id as i64,
                        name,
                        ticker: Some(ticker),
                    });
                }
                Err(_) => continue,
            }
        }
        match app_state
            .esi_client
            .resolve_watchlist_entity(kind.is_alliance(), query)
            .await
        {
            Ok(Some((id, name))) => {
                let ticker =
                    crate::discord_bot::get_ticker(app_state, id, kind.is_alliance()).await;
                Some(ResolvedEntity {
                    id: id as i64,
                    name: Some(name),
                    ticker,
                })
            }
            Ok(None) => None,
            Err(error) => {
                error!("watchlist entity resolution failed for '{query}': {error}");
                None
            }
        }
    }
}

/// Returns every cached entity id whose ticker matches `query`
/// (case-insensitively), sorted ascending for deterministic candidate order,
/// and bounded to a handful so kind-confirmation stays cheap even on a ticker
/// shared by many entities (fix round finding 3). Pure so it is unit-testable
/// without a network.
fn candidate_ids_for_ticker(tickers: &HashMap<u64, String>, query: &str) -> Vec<u64> {
    const MAX_CANDIDATES: usize = 8;
    let mut ids: Vec<u64> = tickers
        .iter()
        .filter(|(_, ticker)| ticker.eq_ignore_ascii_case(query))
        .map(|(id, _)| *id)
        .collect();
    ids.sort_unstable();
    ids.truncate(MAX_CANDIDATES);
    ids
}

fn describe(kind: WatchlistKind, resolved: &ResolvedEntity) -> String {
    let name = resolved.name.clone().unwrap_or_else(|| "?".to_string());
    match &resolved.ticker {
        Some(ticker) => format!("{} {name} [{ticker}] (id {})", kind.as_str(), resolved.id),
        None => format!("{} {name} (id {})", kind.as_str(), resolved.id),
    }
}

#[async_trait]
impl Command for WatchCommand {
    fn name(&self) -> String {
        "watch".to_string()
    }

    fn register<'a>(
        &self,
        command: &'a mut CreateApplicationCommand,
    ) -> &'a mut CreateApplicationCommand {
        command
            .name("watch")
            .description("Manage this server's watchlist of alliances and corporations.")
            .create_option(|option| {
                option
                    .name("add")
                    .description("Add an alliance or corporation to the watchlist.")
                    .kind(CommandOptionType::SubCommand)
                    .create_sub_option(|sub| {
                        sub.name("kind")
                            .description("alliance or corporation.")
                            .kind(CommandOptionType::String)
                            .add_string_choice("alliance", "alliance")
                            .add_string_choice("corporation", "corporation")
                            .required(true)
                    })
                    .create_sub_option(|sub| {
                        sub.name("ticker")
                            .description(
                                "Ticker known to the cache, or the full alliance/corporation name.",
                            )
                            .kind(CommandOptionType::String)
                            .required(true)
                    })
            })
            .create_option(|option| {
                option
                    .name("remove")
                    .description("Remove an alliance or corporation from the watchlist.")
                    .kind(CommandOptionType::SubCommand)
                    .create_sub_option(|sub| {
                        sub.name("kind")
                            .description("alliance or corporation.")
                            .kind(CommandOptionType::String)
                            .add_string_choice("alliance", "alliance")
                            .add_string_choice("corporation", "corporation")
                            .required(true)
                    })
                    .create_sub_option(|sub| {
                        sub.name("ticker")
                            .description(
                                "Ticker known to the cache, or the full alliance/corporation name.",
                            )
                            .kind(CommandOptionType::String)
                            .required(true)
                    })
            })
            .create_option(|option| {
                option
                    .name("list")
                    .description("List the watched alliances and corporations.")
                    .kind(CommandOptionType::SubCommand)
            })
    }

    async fn execute(
        &self,
        ctx: &Context,
        command: &ApplicationCommandInteraction,
        app_state: &Arc<AppState>,
    ) {
        let store_handle = ctx
            .data
            .read()
            .await
            .get::<WatchlistStoreContainer>()
            .cloned();
        let guild_id = command.guild_id.map(|id| id.0);
        let added_by = command.user.id.0;
        let app_state = app_state.clone();
        let subcommand = command.data.options.first().cloned();

        let mut responder = SerenityContractCommandResponder::new(ctx, command);
        if let Err(error) = defer_then_edit(&mut responder, async move {
            let Some(guild_id) = guild_id else {
                return "The watchlist can only be managed in a server channel.".to_string();
            };
            let Some(store_handle) = store_handle else {
                return NOT_CONNECTED.to_string();
            };
            let Some(store) = available_watchlist_store(&store_handle).await else {
                return NOT_CONNECTED.to_string();
            };
            let Some(subcommand) = subcommand else {
                return "Missing /watch subcommand.".to_string();
            };
            match subcommand.name.as_str() {
                "add" => {
                    let kind = match WatchCommand::kind_option(&subcommand.options) {
                        Ok(kind) => kind,
                        Err(error) => return format!("Invalid /watch add: {error}"),
                    };
                    let query = match WatchCommand::ticker_option(&subcommand.options) {
                        Ok(query) => query,
                        Err(error) => return format!("Invalid /watch add: {error}"),
                    };
                    let Some(resolved) = WatchCommand::resolve(&app_state, kind, &query).await
                    else {
                        return format!(
                            "Could not resolve {} '{query}'. Provide a ticker known to the bot's cache, or the full name.",
                            kind.as_str()
                        );
                    };
                    let entity = WatchedEntity {
                        guild_id,
                        kind,
                        entity_id: resolved.id,
                        ticker: resolved.ticker.clone(),
                        name: resolved.name.clone(),
                    };
                    match store
                        .add_entity(&entity, Some(added_by), chrono::Utc::now())
                        .await
                    {
                        Ok(_) => format!("Watching {}.", describe(kind, &resolved)),
                        Err(error) => {
                            error!("cannot add watchlist entity: {error}");
                            "Watchlist entry could not be saved.".to_string()
                        }
                    }
                }
                "remove" => {
                    let kind = match WatchCommand::kind_option(&subcommand.options) {
                        Ok(kind) => kind,
                        Err(error) => return format!("Invalid /watch remove: {error}"),
                    };
                    let query = match WatchCommand::ticker_option(&subcommand.options) {
                        Ok(query) => query,
                        Err(error) => return format!("Invalid /watch remove: {error}"),
                    };
                    let Some(resolved) = WatchCommand::resolve(&app_state, kind, &query).await
                    else {
                        return format!(
                            "Could not resolve {} '{query}' to remove.",
                            kind.as_str()
                        );
                    };
                    match store.remove_entity(guild_id, kind, resolved.id).await {
                        Ok(true) => format!("Stopped watching {}.", describe(kind, &resolved)),
                        Ok(false) => format!(
                            "{} '{query}' was not on the watchlist.",
                            kind.as_str()
                        ),
                        Err(error) => {
                            error!("cannot remove watchlist entity: {error}");
                            "Watchlist entry could not be removed.".to_string()
                        }
                    }
                }
                "list" => match store.list_entities(guild_id).await {
                    Ok(entities) if entities.is_empty() => {
                        "The watchlist is empty.".to_string()
                    }
                    Ok(entities) => {
                        let mut lines = vec!["Watchlist:".to_string()];
                        for entity in entities {
                            let name = entity.name.clone().unwrap_or_else(|| "?".to_string());
                            let ticker = entity
                                .ticker
                                .clone()
                                .map(|ticker| format!(" [{ticker}]"))
                                .unwrap_or_default();
                            lines.push(format!(
                                "- {} {name}{ticker} (id {})",
                                entity.kind.as_str(),
                                entity.entity_id
                            ));
                        }
                        lines.join("\n")
                    }
                    Err(error) => {
                        error!("cannot list watchlist entities: {error}");
                        "Watchlist could not be listed.".to_string()
                    }
                },
                other => format!("Unknown /watch subcommand: {other}"),
            }
        })
        .await
        {
            error!("Cannot respond to watch command: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_ids_are_kind_agnostic_deterministic_and_bounded() {
        let mut tickers = HashMap::new();
        // Two distinct entities (e.g. an alliance and a corporation) share
        // the ticker "DRG", plus an unrelated entry.
        tickers.insert(99_000_002u64, "DRG".to_string());
        tickers.insert(98_000_001u64, "drg".to_string());
        tickers.insert(98_999_999u64, "OTHER".to_string());

        let candidates = candidate_ids_for_ticker(&tickers, "drg");
        // Both matching ids, sorted ascending, deterministic regardless of
        // HashMap iteration order; the non-matching id is excluded.
        assert_eq!(candidates, vec![98_000_001, 99_000_002]);

        // No match yields an empty list (caller falls back to the name path).
        assert!(candidate_ids_for_ticker(&tickers, "nope").is_empty());
    }

    #[test]
    fn candidate_ids_are_capped_to_a_handful() {
        let mut tickers = HashMap::new();
        for id in 0..50u64 {
            tickers.insert(id, "SAME".to_string());
        }
        let candidates = candidate_ids_for_ticker(&tickers, "same");
        // Bounded so kind-confirmation over ESI stays cheap; the lowest ids
        // are kept because the list is sorted before truncation.
        assert_eq!(candidates.len(), 8);
        assert_eq!(candidates, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
