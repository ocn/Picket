use rand::{distributions::Alphanumeric, Rng};
use serenity::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn, Level};

pub mod commands;
pub mod config;
pub mod contract_intelligence;
pub mod discord_bot;
pub mod esi;
pub mod esi_cache;
pub mod feed;
pub mod location_evidence;
pub mod models;
pub mod pipeline;
pub mod presentation;
pub mod processor;
#[cfg(test)]
mod screenshot_payloads;
pub mod sov_feed;
pub mod structure_resolver;
pub mod watchlist_feed;

use crate::commands::contract_subscribe::ContractSubscribeCommand;
use crate::commands::contract_unsubscribe::ContractUnsubscribeCommand;
use crate::commands::find_unsubscribed::FindUnsubscribedChannelsCommand;
use crate::commands::health::HealthCommand;
use crate::commands::sov_subscribe::SovSubscribeCommand;
use crate::commands::sov_timers::SovTimersCommand;
use crate::commands::sov_unsubscribe::SovUnsubscribeCommand;
use crate::commands::watch::WatchCommand;
use crate::commands::watch_subscribe::WatchSubscribeCommand;
use crate::commands::watch_unsubscribe::WatchUnsubscribeCommand;
use commands::diag::DiagCommand;
use commands::subscribe::SubscribeCommand;
use commands::sync_clear::SyncClearCommand;
use commands::sync_remove::SyncRemoveCommand;
use commands::sync_standings::SyncStandingsCommand;
use commands::unsubscribe::UnsubscribeCommand;
use commands::{Command, PingCommand};
use config::FeedProvider;
use discord_bot::CommandMap;
use feed::KillmailFeed;
use structure_resolver::{
    AuthenticatedStructureResolver, StructureResolver, StructureResolverConfig,
    StructureResolverRuntimeStatus,
};

pub struct AppStateContainer;

impl TypeMapKey for AppStateContainer {
    type Value = Arc<config::AppState>;
}

pub struct ContractStoreContainer;

impl TypeMapKey for ContractStoreContainer {
    type Value = contract_intelligence::ContractStoreHandle;
}

pub struct SovStoreContainer;

impl TypeMapKey for SovStoreContainer {
    type Value = sov_feed::SovStoreHandle;
}

pub struct WatchlistStoreContainer;

impl TypeMapKey for WatchlistStoreContainer {
    type Value = watchlist_feed::WatchlistStoreHandle;
}

/// Composition-root adapter letting the sov collector resolve a
/// `Defender { watchlist: true }` leaf against a guild's current watchlist
/// (ticket 08) without `sov_feed` depending on `watchlist_feed`. Wraps a
/// connected [`watchlist_feed::WatchlistStore`] behind
/// [`sov_feed::SovWatchlistSource`].
pub struct WatchlistDefenderSource {
    store: Arc<watchlist_feed::WatchlistStore>,
}

impl WatchlistDefenderSource {
    pub fn new(store: Arc<watchlist_feed::WatchlistStore>) -> Self {
        Self { store }
    }
}

#[serenity::async_trait]
impl sov_feed::SovWatchlistSource for WatchlistDefenderSource {
    async fn watched_alliance_ids(&self, guild_id: u64) -> Result<Vec<i64>, sqlx::Error> {
        self.store.watched_alliance_ids(guild_id).await
    }
}

/// Shared handle to the sov feed's reachability source (ticket 04's
/// stargate-only `StaticSovReachability`, or ticket 05's
/// `DynamicChainSovReachability` when Wanderer chain data is configured):
/// populated once at start-up (see [`sov_reachability_source`]) and read
/// by the collector loops and `/sov_timers` alike. Unlike
/// [`SovStoreContainer`] the *container slot itself* never changes after
/// start-up -- there is no live reload of `config/stargates.json`, and
/// this `Arc` is never replaced -- so the slot needs no interior
/// mutability of its own; `DynamicChainSovReachability`, when present,
/// owns its *own* interior mutability for the routes it serves through
/// this same unchanging `Arc<dyn SovReachabilitySource>`.
pub struct SovReachabilityContainer;

impl TypeMapKey for SovReachabilityContainer {
    type Value = Arc<dyn sov_feed::SovReachabilitySource>;
}

/// Fixed sov campaign collection cadence (spec "Sources and cadence":
/// "Sovereignty campaigns are polled every sixty seconds").
const SOV_COLLECTION_INTERVAL: Duration = Duration::from_secs(60);

/// T-minus stage-evaluation cadence: independent of, and cheaper than,
/// `SOV_COLLECTION_INTERVAL` because it makes no ESI request (ticket 03:
/// "A stage-evaluation pass runs at least every thirty seconds"). Also
/// drives the `tz_window_entered` evaluation pass (ticket 07), which is
/// likewise ESI-free.
const SOV_STAGE_INTERVAL: Duration = Duration::from_secs(30);

/// Sovereignty Hub structures collection cadence: `GET /sovereignty/structures/`'s
/// own `max-age=300` (spec "Sources and cadence": "Sovereignty Hub
/// vulnerability windows every five minutes (their cache age)"; ticket 07).
const SOV_STRUCTURES_COLLECTION_INTERVAL: Duration = Duration::from_secs(300);

/// Sovereignty map collection cadence: `GET /sovereignty/map/`'s own
/// `max-age=3600` (spec "Sources and cadence": "sovereignty ownership map
/// hourly"; ticket 07).
const SOV_MAP_COLLECTION_INTERVAL: Duration = Duration::from_secs(3600);

/// Wanderer chain poll cadence (spec "Sources and cadence": "The Wanderer
/// chain is polled every two minutes (systems and connections)").
const SOV_CHAIN_POLL_INTERVAL: Duration = Duration::from_secs(120);

/// Watchlist alliance corporation-list poll cadence (ticket 08, spec
/// "Hourly collectors"): the `/alliances/{id}/corporations/` route caches
/// for one hour, so an hourly conditional re-poll is almost always a 304.
const WATCHLIST_COLLECTION_INTERVAL: Duration = Duration::from_secs(3600);

/// Default reachability origin: Turnur (spec "Reachability": "Single
/// origin: Turnur (configurable by environment)").
const SOV_DEFAULT_HOME_SYSTEM_ID: i64 = 30_002_086;

/// Facts the sov campaign feed's one-time start-up log line needs (ticket
/// 16): the reachability origin, and -- when `config/stargates.json`
/// loaded -- the loaded graph's `(systems, edges)` size. `graph == None`
/// means the file was missing or malformed and reachability degraded to
/// stargate-unavailable.
#[derive(Clone, Copy, Debug)]
pub struct SovFeedStartupSummary {
    pub home_system_id: i64,
    pub graph: Option<(usize, usize)>,
}

/// Builds the sov campaign feed's start-up log message (ticket 16). Pure so
/// it can be unit-tested without a tracing subscriber; the loop passes the
/// built string straight to `info!`. `reconnected` selects the
/// later-reconnect wording emitted after a database outage rather than the
/// once-per-process start wording.
fn sov_campaign_feed_start_message(summary: SovFeedStartupSummary, reconnected: bool) -> String {
    let lead = if reconnected {
        "sov campaign feed reconnected"
    } else {
        "sov campaign feed started"
    };
    match summary.graph {
        Some((systems, edges)) => format!(
            "{lead} (home system {}, stargate graph {systems} systems / {edges} edges)",
            summary.home_system_id
        ),
        None => format!(
            "{lead} (home system {}, stargate graph unavailable)",
            summary.home_system_id
        ),
    }
}

/// Builds the Wanderer chain reachability start-up log message (ticket 16).
/// The map slug is safe to log; the API key never is and is not accepted
/// here. `reconnected` selects the later-reconnect wording.
fn sov_chain_reachability_start_message(map: &str, poll_secs: u64, reconnected: bool) -> String {
    let lead = if reconnected {
        "sov chain reachability reconnected"
    } else {
        "sov chain reachability enabled"
    };
    format!("{lead} (map {map}, poll every {poll_secs} s)")
}

/// Builds the watchlist feed's start-up log message (ticket 16).
fn watchlist_feed_start_message(reconnected: bool) -> String {
    if reconnected {
        "watchlist feed reconnected".to_string()
    } else {
        "watchlist feed started".to_string()
    }
}

/// Reads `SOV_HOME_SYSTEM_ID`, falling back to Turnur when unset or not a
/// valid integer. An empty or whitespace-only value is treated exactly like
/// an unset variable (silent fallback to the default) so a Compose
/// passthrough of the form `SOV_HOME_SYSTEM_ID: ${SOV_HOME_SYSTEM_ID:-}`
/// -- which resolves to `""` when the operator leaves it unset -- is not a
/// misconfiguration (ticket 16). Never panics: any other malformed override
/// just logs a warning and keeps the default rather than taking down
/// start-up.
fn sov_home_system_id() -> i64 {
    let Ok(value) = std::env::var("SOV_HOME_SYSTEM_ID") else {
        return SOV_DEFAULT_HOME_SYSTEM_ID;
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return SOV_DEFAULT_HOME_SYSTEM_ID;
    }
    trimmed.parse::<i64>().unwrap_or_else(|_| {
        warn!(
            "SOV_HOME_SYSTEM_ID '{value}' is not a valid integer; using the default (Turnur, {SOV_DEFAULT_HOME_SYSTEM_ID})"
        );
        SOV_DEFAULT_HOME_SYSTEM_ID
    })
}

/// Loads `config/stargates.json` and computes reachability from the
/// configured home system, or degrades to "graph unavailable" when the
/// file is missing or malformed (spec: "fails loudly" means logged, never
/// a process exit or a delay to Discord start-up -- review decisions
/// carried over from ticket 02's ADR 0002 discussion). Pure CPU/local-disk
/// work over a few thousand systems, so this runs synchronously before the
/// Discord client is built without risking any startup delay.
///
/// When `wanderer_configured` is also true (ticket 05: all three
/// `WANDERER_*` variables set), a successfully-loaded graph additionally
/// produces a [`sov_feed::DynamicChainSovReachability`], returned
/// separately so the composition root can hand it to the chain poll loop
/// for [`sov_feed::DynamicChainSovReachability::recompute`] calls -- the
/// trait object alone (`Arc<dyn SovReachabilitySource>`) cannot expose
/// that concrete method. Until the first successful chain fetch, it
/// behaves exactly like the stargate-only case (spec: "otherwise
/// reachability stays stargate-only").
fn sov_reachability_source(
    enabled: bool,
    wanderer_configured: bool,
) -> (
    Arc<dyn sov_feed::SovReachabilitySource>,
    Option<Arc<sov_feed::DynamicChainSovReachability>>,
    SovFeedStartupSummary,
) {
    if !enabled {
        // The summary is never consumed on the disabled path (the campaign
        // collection loop is not spawned), so avoid reading the environment
        // here and keep the historical no-op behaviour exactly.
        return (
            Arc::new(sov_feed::StaticSovReachability(None)),
            None,
            SovFeedStartupSummary {
                home_system_id: SOV_DEFAULT_HOME_SYSTEM_ID,
                graph: None,
            },
        );
    }
    let home_system_id = sov_home_system_id();
    let path = std::path::Path::new(sov_feed::DEFAULT_STARGATE_GRAPH_PATH);
    match sov_feed::load_stargate_graph_file(path) {
        Ok(file) => {
            let graph = sov_feed::StargateGraph::from_file(file);
            let (systems, edges) = (graph.system_count(), graph.edge_count());
            info!(
                "Loaded stargate graph: sde_version={}, systems={systems}, edges={edges}, home_system_id={home_system_id}",
                graph.sde_version,
            );
            let summary = SovFeedStartupSummary {
                home_system_id,
                graph: Some((systems, edges)),
            };
            if wanderer_configured {
                let dynamic = Arc::new(sov_feed::DynamicChainSovReachability::new(
                    graph,
                    home_system_id,
                ));
                (dynamic.clone(), Some(dynamic), summary)
            } else {
                let reachability = sov_feed::Reachability::compute(&graph, home_system_id, &[]);
                (
                    Arc::new(sov_feed::StaticSovReachability(Some(reachability))),
                    None,
                    summary,
                )
            }
        }
        Err(error) => {
            error!(
                "Sov reachability disabled: could not load {}: {error}. The sov feed runs without reachability: Reachable filter leaves never match and /sov_timers reports the graph as unavailable.",
                path.display()
            );
            (
                Arc::new(sov_feed::StaticSovReachability(None)),
                None,
                SovFeedStartupSummary {
                    home_system_id,
                    graph: None,
                },
            )
        }
    }
}

fn generate_queue_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

pub async fn run() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Killbot-Rust...");

    // --- Load all configurations ---
    let app_config = match config::load_app_config() {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load application configuration: {}", e);
            return;
        }
    };

    info!("Feed provider: {}", app_config.killmail_feed_provider);
    info!("ESI HTTP timeout: {}s", app_config.esi_http_timeout_secs);
    info!(
        "Killmail process timeout: {}s",
        app_config.killmail_process_timeout_secs
    );
    info!(
        "RedisQ connect timeout: {}s / request timeout: {}s",
        app_config.redisq_connect_timeout_secs, app_config.redisq_request_timeout_secs
    );
    info!(
        "R2Z2 connect timeout: {}s / request timeout: {}s / poll interval: {}s",
        app_config.r2z2_connect_timeout_secs,
        app_config.r2z2_request_timeout_secs,
        app_config.r2z2_poll_interval_secs
    );

    let systems = config::load_systems().unwrap_or_else(|e| {
        warn!(
            "Failed to load systems.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let ships = config::load_ships().unwrap_or_else(|e| {
        warn!(
            "Failed to load ships.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let names = config::load_names().unwrap_or_else(|e| {
        warn!(
            "Failed to load names.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let tickers = config::load_tickers().unwrap_or_else(|e| {
        warn!(
            "Failed to load tickers.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });
    let group_names = config::load_group_names().unwrap_or_else(|e| {
        warn!(
            "Failed to load group_names.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });

    let user_standings = config::load_user_standings().unwrap_or_else(|e| {
        warn!(
            "Failed to load user_standings.json: {}. Starting with an empty map.",
            e
        );
        HashMap::new()
    });

    let subscriptions = config::load_all_subscriptions("config/");

    // --- Initialize application state ---
    let app_state = Arc::new(config::AppState::new(
        app_config.clone(),
        systems,
        ships,
        names,
        tickers,
        group_names,
        subscriptions,
        user_standings,
    ));

    // --- Initialize Commands ---
    let mut command_map: HashMap<String, Box<dyn Command>> = HashMap::new();

    let ping_command = Box::new(PingCommand);
    command_map.insert(ping_command.name(), ping_command);

    let subscribe_command = Box::new(SubscribeCommand);
    command_map.insert(subscribe_command.name(), subscribe_command);

    let unsubscribe_command = Box::new(UnsubscribeCommand);
    command_map.insert(unsubscribe_command.name(), unsubscribe_command);

    let diag_command = Box::new(DiagCommand);
    command_map.insert(diag_command.name(), diag_command);

    let health_command = Box::new(HealthCommand);
    command_map.insert(health_command.name(), health_command);

    let sync_standings_command = Box::new(SyncStandingsCommand);
    command_map.insert(sync_standings_command.name(), sync_standings_command);

    let sync_remove_command = Box::new(SyncRemoveCommand);
    command_map.insert(sync_remove_command.name(), sync_remove_command);

    let sync_clear_command = Box::new(SyncClearCommand);
    command_map.insert(sync_clear_command.name(), sync_clear_command);

    let find_unsubscribed_command = Box::new(FindUnsubscribedChannelsCommand);
    command_map.insert(find_unsubscribed_command.name(), find_unsubscribed_command);

    let contract_subscribe_command = Box::new(ContractSubscribeCommand);
    command_map.insert(
        contract_subscribe_command.name(),
        contract_subscribe_command,
    );

    let contract_unsubscribe_command = Box::new(ContractUnsubscribeCommand);
    command_map.insert(
        contract_unsubscribe_command.name(),
        contract_unsubscribe_command,
    );

    let sov_subscribe_command = Box::new(SovSubscribeCommand);
    command_map.insert(sov_subscribe_command.name(), sov_subscribe_command);

    let sov_unsubscribe_command = Box::new(SovUnsubscribeCommand);
    command_map.insert(sov_unsubscribe_command.name(), sov_unsubscribe_command);

    let sov_timers_command = Box::new(SovTimersCommand);
    command_map.insert(sov_timers_command.name(), sov_timers_command);

    let watch_command = Box::new(WatchCommand);
    command_map.insert(watch_command.name(), watch_command);

    let watch_subscribe_command = Box::new(WatchSubscribeCommand);
    command_map.insert(watch_subscribe_command.name(), watch_subscribe_command);

    let watch_unsubscribe_command = Box::new(WatchUnsubscribeCommand);
    command_map.insert(watch_unsubscribe_command.name(), watch_unsubscribe_command);

    let command_map_arc = Arc::new(command_map);

    let contract_runtime = match std::env::var("CONTRACT_DATABASE_URL") {
        Ok(database_url) => Some((
            database_url,
            contract_intelligence::new_contract_store_handle(),
        )),
        Err(_) => {
            info!("Contract collection disabled: CONTRACT_DATABASE_URL is not configured");
            None
        }
    };
    let feed_health_telemetry = Arc::new(feed::FeedHealthTelemetry::new());
    let r2z2_health_enabled = app_config.killmail_feed_provider == FeedProvider::R2z2;

    // Sov campaign feed: same optionality as the contract runtime (spec
    // "Persistence and process model"). Nothing here touches Postgres: the
    // handle starts empty and `spawn_sov_collection_loop` (below, after the
    // Discord client exists) connects and applies migrations from inside
    // its own retry loop, so a briefly-unavailable Postgres at boot can
    // never delay the gateway connect or the killmail pipeline (review
    // finding 1 on
    // `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
    let sov_store_handle = sov_feed::new_sov_store_handle();

    // Watchlist feed (ticket 08): same optionality and late-bound-handle
    // pattern as the sov feed. The handle starts empty; the watchlist
    // collection loop (below) populates it once it connects, so a
    // briefly-unavailable Postgres at boot never delays the gateway or the
    // killmail pipeline, and `/watch*` commands answer "not connected"
    // rather than panicking until it is populated.
    let watchlist_store_handle = watchlist_feed::new_watchlist_store_handle();

    // Stargate reachability graph (ticket 04): loaded synchronously here,
    // before the Discord client exists, because it is pure local-disk and
    // CPU work over a few thousand systems (milliseconds), never network
    // IO -- so, unlike the sov store, there is no need to defer it to a
    // background retry loop. A missing or malformed
    // `config/stargates.json` degrades to "graph unavailable" rather than
    // failing start-up (see `sov_reachability_source`'s doc comment).
    //
    // Wanderer chain reachability (ticket 05) is nested inside the sov
    // feed's own optionality: it only ever activates when the sov feed
    // itself is enabled (`contract_runtime.is_some()`) *and* all three
    // `WANDERER_*` variables are set; otherwise reachability stays
    // stargate-only, exactly ticket 04's behaviour.
    let wanderer_config = contract_runtime
        .is_some()
        .then(sov_feed::WandererConfig::from_environment)
        .flatten();
    let (sov_reachability, dynamic_chain_reachability, sov_startup_summary) =
        sov_reachability_source(contract_runtime.is_some(), wanderer_config.is_some());

    // --- Start Discord Bot ---
    let discord_token = app_config.discord_bot_token.clone();
    let intents = GatewayIntents::non_privileged()
        | GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT
        | GatewayIntents::GUILD_INTEGRATIONS;
    let mut client = Client::builder(&discord_token, intents)
        .event_handler(discord_bot::Handler)
        .await
        .expect("Error creating Discord client");

    {
        let mut data = client.data.write().await;
        data.insert::<AppStateContainer>(app_state.clone());
        data.insert::<CommandMap>(command_map_arc.clone());
        if let Some((_, store_handle)) = &contract_runtime {
            data.insert::<ContractStoreContainer>(store_handle.clone());
        }
        if contract_runtime.is_some() {
            data.insert::<SovStoreContainer>(sov_store_handle.clone());
            data.insert::<SovReachabilityContainer>(sov_reachability.clone());
            data.insert::<WatchlistStoreContainer>(watchlist_store_handle.clone());
        }
    }

    let http_client = client.cache_and_http.http.clone();

    if let Some((database_url, _)) = &contract_runtime {
        let timeout = Duration::from_secs(app_config.esi_http_timeout_secs);
        match sov_feed::HttpSovereigntyEsi::new(timeout) {
            Ok(esi) => {
                let esi: Arc<dyn sov_feed::SovereigntyEsi> = Arc::new(esi);
                let delivery: Arc<dyn sov_feed::SovDelivery> =
                    Arc::new(discord_bot::DiscordSovDelivery::new(http_client.clone()));
                let directory: Arc<dyn sov_feed::SovSystemDirectory> = Arc::new(
                    discord_bot::DiscordSovSystemDirectory::new(app_state.clone()),
                );
                let tickers: Arc<dyn sov_feed::SovTickerResolver> = Arc::new(
                    discord_bot::DiscordSovTickerResolver::new(app_state.clone()),
                );
                spawn_sov_collection_loop(
                    database_url.clone(),
                    sov_store_handle,
                    SOV_COLLECTION_INTERVAL,
                    esi.clone(),
                    delivery.clone(),
                    directory.clone(),
                    tickers.clone(),
                    sov_reachability.clone(),
                    sov_startup_summary,
                );
                // Sovereignty Hub structures and sovereignty map (ticket
                // 07): two more independently-reconnecting loops at their
                // own ESI-cache-age cadences, sharing the same `esi`
                // client (one more trait method each, same base URL) and
                // never touching `sov_store_handle` -- a stall in either
                // cannot affect campaign polling, T-minus/reachability/
                // tz-window evaluation, or command handling.
                spawn_sov_structures_loop(
                    database_url.clone(),
                    SOV_STRUCTURES_COLLECTION_INTERVAL,
                    esi.clone(),
                    delivery.clone(),
                    directory.clone(),
                    tickers.clone(),
                    sov_reachability.clone(),
                );
                spawn_sov_map_loop(
                    database_url.clone(),
                    SOV_MAP_COLLECTION_INTERVAL,
                    esi.clone(),
                    delivery.clone(),
                    directory.clone(),
                    tickers.clone(),
                    sov_reachability.clone(),
                );
                // T-minus stage evaluation (ticket 03), reachability
                // transitions (ticket 06), and tz-window-entered
                // evaluation (ticket 07): a separate, independently-
                // reconnecting loop rather than a faster cadence inside
                // `run_sov_collection_loop`, mirroring how
                // `run_terminal_resolution_recovery_loop` and
                // `run_proximity_reconciliation_loop` already run their
                // own cadences alongside the contract feed's main
                // collection loop. It never touches `sov_store_handle`
                // (already owned by the collection loop above) or ESI, so
                // a stall here cannot affect campaign polling or command
                // handling.
                spawn_sov_stage_loop(
                    database_url.clone(),
                    SOV_STAGE_INTERVAL,
                    esi,
                    delivery,
                    directory,
                    tickers,
                    sov_reachability,
                );
            }
            Err(error) => {
                warn!("Sov campaign feed disabled: HTTP client initialization failed: {error}");
            }
        }

        // Watchlist feed (ticket 08): an independent hourly collection loop
        // beside the sov runtime, behind the same database gate, with its
        // own late-bound store handle. Never awaited on the path to starting
        // the Discord client.
        match watchlist_feed::HttpWatchlistEsi::new(Duration::from_secs(
            app_config.esi_http_timeout_secs,
        )) {
            Ok(watchlist_esi) => {
                let watchlist_esi: Arc<dyn watchlist_feed::WatchlistEsi> = Arc::new(watchlist_esi);
                let watchlist_delivery: Arc<dyn watchlist_feed::WatchlistDelivery> = Arc::new(
                    discord_bot::DiscordWatchlistDelivery::new(http_client.clone()),
                );
                let watchlist_resolver: Arc<dyn watchlist_feed::WatchlistEntityResolver> = Arc::new(
                    discord_bot::DiscordWatchlistResolver::new(app_state.clone()),
                );
                spawn_watchlist_collection_loop(
                    database_url.clone(),
                    watchlist_store_handle,
                    WATCHLIST_COLLECTION_INTERVAL,
                    watchlist_esi,
                    watchlist_delivery,
                    watchlist_resolver,
                );
            }
            Err(error) => {
                warn!("Watchlist feed disabled: HTTP client initialization failed: {error}");
            }
        }
    } else {
        info!("Sov campaign feed disabled: CONTRACT_DATABASE_URL is not configured");
    }

    // Wanderer chain reachability poll loop (ticket 05): only spawned
    // when both the sov feed's database and all three `WANDERER_*`
    // variables are configured. Independent of every other sov loop --
    // it never touches `sov_store_handle` (owned by the campaign
    // collection loop) -- so a stall here cannot affect campaign polling,
    // T-minus evaluation, or command handling, and vice versa.
    if let (Some((database_url, _)), Some(wanderer_config), Some(dynamic_chain)) = (
        &contract_runtime,
        wanderer_config.as_ref(),
        dynamic_chain_reachability.as_ref(),
    ) {
        let timeout = Duration::from_secs(app_config.esi_http_timeout_secs);
        match sov_feed::WandererClient::new(wanderer_config, timeout) {
            Ok(client) => {
                let client = Arc::new(client);
                // One-shot start-up probe (spec: "the client probes the
                // SSE stream once ... without depending on it"); never
                // awaited by the poll loop below.
                tokio::spawn({
                    let client = client.clone();
                    async move {
                        sov_feed::log_sse_probe_result(&client, Duration::from_secs(5)).await;
                    }
                });
                let chain_source: Arc<dyn sov_feed::WandererChainSource> = client;
                spawn_sov_chain_loop(
                    database_url.clone(),
                    SOV_CHAIN_POLL_INTERVAL,
                    chain_source,
                    dynamic_chain.clone(),
                    wanderer_config.map.clone(),
                );
            }
            Err(error) => {
                warn!("Wanderer chain reachability disabled: HTTP client initialization failed: {error}");
            }
        }
    }

    if let Some((database_url, store_handle)) = contract_runtime {
        let interval = std::env::var("CONTRACT_COLLECTION_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300);
        let timeout = Duration::from_secs(app_config.esi_http_timeout_secs);
        let ship_groups = Arc::new(discord_bot::DiscordShipGroupResolver::new(
            app_state.clone(),
        ));
        let delivery = Arc::new(discord_bot::DiscordContractDelivery::new(
            http_client.clone(),
        ));
        let ping_limiter = Arc::new(contract_intelligence::AppStateContractPingLimiter::new(
            app_state.clone(),
        ));
        let health_config = match contract_intelligence::HealthRuntimeConfig::from_environment() {
            Ok(config) => Some(config),
            Err(error) => {
                warn!("health monitoring disabled by invalid configuration: {error}");
                None
            }
        };
        let health_database_url = database_url.clone();
        let (structure_resolver, structure_resolver_runtime): (
            Option<Arc<dyn StructureResolver>>,
            Option<StructureResolverRuntimeStatus>,
        ) = match StructureResolverConfig::from_environment() {
            Ok(config) if config.is_enabled() => {
                let runtime = config.runtime_status();
                match AuthenticatedStructureResolver::new(config, timeout) {
                    Ok(resolver) => (Some(Arc::new(resolver)), Some(runtime)),
                    Err(error) => {
                        warn!("structure resolver disabled after HTTP initialization failure: {error}");
                        (
                            None,
                            Some(StructureResolverRuntimeStatus::initialization_failed()),
                        )
                    }
                }
            }
            Ok(config) => (None, Some(config.runtime_status())),
            Err(error) => {
                warn!("structure resolver disabled by invalid configuration: {error}");
                (
                    None,
                    Some(StructureResolverRuntimeStatus::invalid_configuration()),
                )
            }
        };
        match contract_intelligence::contract_regional_concurrency_from_environment() {
            Ok(max_concurrent_regions) => {
                contract_intelligence::spawn_contract_collection_loop_with_notifications_structure_resolver_and_region_concurrency(
                    database_url.clone(),
                    store_handle,
                    Duration::from_secs(interval),
                    timeout,
                    ship_groups.clone(),
                    delivery.clone(),
                    ping_limiter.clone(),
                    structure_resolver.clone(),
                    structure_resolver_runtime,
                    max_concurrent_regions,
                );
            }
            Err(error) => {
                warn!("contract collection disabled by invalid CONTRACT_REGIONAL_CONCURRENCY: {error}");
            }
        }
        contract_intelligence::spawn_terminal_resolution_recovery_loop(
            database_url.clone(),
            timeout,
            ship_groups.clone(),
            delivery.clone(),
            ping_limiter.clone(),
            structure_resolver.clone(),
        );
        match contract_intelligence::proximity_reconciliation_interval_from_environment() {
            Ok(interval) => {
                contract_intelligence::spawn_proximity_reconciliation_loop(
                    database_url,
                    interval,
                    timeout,
                    ship_groups,
                    delivery,
                    ping_limiter,
                    structure_resolver,
                );
            }
            Err(error) => warn!(
                "proximity reconciliation disabled by invalid CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS: {error}"
            ),
        }
        if let Some(health_config) = health_config {
            contract_intelligence::spawn_health_monitor_loop(
                health_database_url,
                health_config,
                feed_health_telemetry.clone(),
                r2z2_health_enabled,
            );
        }
    }

    tokio::spawn(async move {
        if let Err(why) = client.start().await {
            error!("Discord client error: {:?}", why);
        }
    });

    // --- Initialize killmail feed ---
    let feed: Box<dyn KillmailFeed> = match app_config.killmail_feed_provider {
        FeedProvider::R2z2 => Box::new(feed::r2z2::R2z2Feed::new(&app_state.app_config)),
        FeedProvider::Redisq => {
            let queue_id = generate_queue_id();
            Box::new(feed::redisq::RedisQFeed::new(
                &queue_id,
                Duration::from_secs(app_config.redisq_connect_timeout_secs),
                Duration::from_secs(app_config.redisq_request_timeout_secs),
            ))
        }
    };

    // --- Validate pipeline config ---
    if app_config.killmail_workers < 1 {
        error!(
            "KILLMAIL_WORKERS must be >= 1 (got {})",
            app_config.killmail_workers
        );
        return;
    }
    if app_config.killmail_queue_size < 1 || app_config.killmail_queue_size > 4096 {
        error!(
            "KILLMAIL_QUEUE_SIZE must be 1..=4096 (got {})",
            app_config.killmail_queue_size
        );
        return;
    }

    info!(
        "Pipeline: workers={}, queue_size={}, post_process_sleep_ms={}",
        app_config.killmail_workers,
        app_config.killmail_queue_size,
        app_config.killmail_post_process_sleep_ms
    );

    // --- Start concurrent pipeline ---
    let (result_tx, result_rx) = mpsc::channel(app_config.killmail_queue_size);
    let semaphore = Arc::new(Semaphore::new(app_config.killmail_workers));

    info!("Listening for killmails...");

    // Spawn dispatcher task
    let dispatcher_state = app_state.clone();
    let dispatcher_http = http_client.clone();
    tokio::spawn(async move {
        pipeline::run_dispatcher(result_rx, dispatcher_state, dispatcher_http).await;
    });

    // Run producer on current task (main loop)
    pipeline::run_producer_with_health(
        feed,
        app_state,
        result_tx,
        semaphore,
        feed_health_telemetry,
    )
    .await;
}

/// Connects a watchlist-store-backed [`sov_feed::SovWatchlistSource`] for
/// resolving `Defender { watchlist: true }` leaves (ticket 08), falling back
/// to [`sov_feed::NoSovWatchlist`] (watchlist leaves never match) when the
/// watchlist store is momentarily unavailable. Migrations are already
/// applied by the loop's `ContractCollectionStore::connect`, so the
/// `watchlist_*` tables exist by the time this runs.
async fn connect_sov_watchlist_source(database_url: &str) -> Arc<dyn sov_feed::SovWatchlistSource> {
    match watchlist_feed::WatchlistStore::connect(database_url).await {
        Ok(store) => Arc::new(WatchlistDefenderSource::new(Arc::new(store))),
        Err(error) => {
            warn!("watchlist source unavailable for sov defender resolution: {error}");
            Arc::new(sov_feed::NoSovWatchlist)
        }
    }
}

/// Spawns the sov campaign feed's independent collection loop. Generic
/// over the sov feed's trait objects (not concrete Discord types) so tests
/// can inject fakes, mirroring
/// `contract_intelligence::spawn_contract_collection_loop_with_notifications`.
/// Never awaited on the path to starting the Discord client: this function
/// itself does not connect to Postgres, it only spawns a task that does
/// (review finding 1 on
/// `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
#[allow(clippy::too_many_arguments)]
pub fn spawn_sov_collection_loop(
    database_url: String,
    store_handle: sov_feed::SovStoreHandle,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
    startup_summary: SovFeedStartupSummary,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_collection_loop(
        database_url,
        store_handle,
        interval,
        esi,
        delivery,
        directory,
        tickers,
        reachability,
        startup_summary,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_sov_collection_loop(
    database_url: String,
    store_handle: sov_feed::SovStoreHandle,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
    startup_summary: SovFeedStartupSummary,
) {
    let mut consecutive_failures = 0_u32;
    // Once-per-process start-up log flags (ticket 16): `started` gates the
    // one-time "started" line to the first successful connect + migration
    // check; `failed_since_start` makes a later success after a database
    // outage log the distinct "reconnected" variant instead.
    let mut started = false;
    let mut failed_since_start = false;
    loop {
        let mut delay = interval;
        // Reconnect every iteration, like the contract feed's own loop
        // (`run_contract_collection_loop_with_notifications_and_region_concurrency`):
        // this both re-applies migrations idempotently (harmless; sqlx's
        // migration lock serializes concurrent runs) and lets the loop
        // self-heal after Postgres was briefly unavailable, without any
        // separate reconnect path.
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    let store = Arc::new(store);
                    *store_handle.write().await = Some(store.clone());
                    if !started {
                        info!(
                            "{}",
                            sov_campaign_feed_start_message(startup_summary, false)
                        );
                        started = true;
                    } else if failed_since_start {
                        info!("{}", sov_campaign_feed_start_message(startup_summary, true));
                        failed_since_start = false;
                    }
                    let watchlist_source = connect_sov_watchlist_source(&database_url).await;
                    let collector = sov_feed::SovCollector::new(
                        (*store).clone(),
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        directory.clone(),
                        tickers.clone(),
                        reachability.clone(),
                    )
                    .with_watchlist_source(watchlist_source);
                    match run_sov_cycle_isolated(async move { collector.collect_cycle().await })
                        .await
                    {
                        Ok(report) => {
                            if let Some(paused_until) = report.paused_until {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                delay = contract_intelligence::collection_retry_delay(
                                    interval,
                                    Some(paused_until),
                                    consecutive_failures,
                                );
                            } else {
                                consecutive_failures = 0;
                            }
                        }
                        Err(error) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            delay = contract_intelligence::collection_retry_delay(
                                interval,
                                None,
                                consecutive_failures,
                            );
                            warn!("sov campaign collection paused after failure: {error}");
                        }
                    }
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    delay = contract_intelligence::collection_retry_delay(
                        interval,
                        None,
                        consecutive_failures,
                    );
                    *store_handle.write().await = None;
                    failed_since_start = true;
                    warn!(
                        "sov campaign feed database unavailable; retrying without an in-memory fallback: {error}"
                    );
                }
            },
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                delay = contract_intelligence::collection_retry_delay(
                    interval,
                    None,
                    consecutive_failures,
                );
                *store_handle.write().await = None;
                failed_since_start = true;
                warn!("sov campaign feed migrations unavailable; retrying: {error}");
            }
        }
        tokio::time::sleep(delay).await;
    }
}

/// Spawns the sov campaign feed's T-minus stage-evaluation loop (ticket
/// 03). Runs independently of [`spawn_sov_collection_loop`] at a cheaper,
/// more frequent cadence because `SovCollector::evaluate_stage_cycle`
/// makes no ESI request; it only reads persisted campaigns and
/// subscriptions against the clock. Mirrors
/// `contract_intelligence::spawn_terminal_resolution_recovery_loop`:
/// reconnects every tick (self-healing, idempotent migrations) and does
/// not populate [`sov_feed::SovStoreHandle`], since the main collection
/// loop above already owns that handle for command lookups.
#[allow(clippy::too_many_arguments)]
pub fn spawn_sov_stage_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_stage_loop(
        database_url,
        interval,
        esi,
        delivery,
        directory,
        tickers,
        reachability,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_sov_stage_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) {
    let mut cadence = tokio::time::interval_at(tokio::time::Instant::now(), interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    let watchlist_source = connect_sov_watchlist_source(&database_url).await;
                    let collector = sov_feed::SovCollector::new(
                        store,
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        directory.clone(),
                        tickers.clone(),
                        reachability.clone(),
                    )
                    .with_watchlist_source(watchlist_source);
                    if let Err(error) =
                        run_sov_cycle_isolated(
                            async move { collector.evaluate_stage_cycle().await },
                        )
                        .await
                    {
                        warn!("sov stage evaluation cycle failed: {error}");
                    }
                }
                Err(error) => warn!("sov stage evaluation database unavailable: {error}"),
            },
            Err(error) => warn!("sov stage evaluation migrations unavailable: {error}"),
        }
    }
}

/// Spawns the Sovereignty Hub structures poll loop (ticket 07). Fixed
/// cadence via `interval_at` + `MissedTickBehavior::Skip`, mirroring
/// `spawn_sov_stage_loop`: reconnects every tick (self-healing, idempotent
/// migrations), and produces no delivery of its own, so a stall here
/// cannot affect campaign polling, stage evaluation, or command handling.
#[allow(clippy::too_many_arguments)]
pub fn spawn_sov_structures_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_structures_loop(
        database_url,
        interval,
        esi,
        delivery,
        directory,
        tickers,
        reachability,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_sov_structures_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) {
    let mut cadence = tokio::time::interval_at(tokio::time::Instant::now(), interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    let collector = sov_feed::SovCollector::new(
                        store,
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        directory.clone(),
                        tickers.clone(),
                        reachability.clone(),
                    );
                    if let Err(error) =
                        run_sov_cycle_isolated(
                            async move { collector.collect_structures_cycle().await },
                        )
                        .await
                    {
                        warn!("sov structures collection cycle failed: {error}");
                    }
                }
                Err(error) => warn!("sov structures feed database unavailable: {error}"),
            },
            Err(error) => warn!("sov structures feed migrations unavailable: {error}"),
        }
    }
}

/// Spawns the sovereignty map poll loop (ticket 07). Mirrors
/// `spawn_sov_structures_loop` at its own hourly cadence.
#[allow(clippy::too_many_arguments)]
pub fn spawn_sov_map_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_map_loop(
        database_url,
        interval,
        esi,
        delivery,
        directory,
        tickers,
        reachability,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_sov_map_loop(
    database_url: String,
    interval: Duration,
    esi: Arc<dyn sov_feed::SovereigntyEsi>,
    delivery: Arc<dyn sov_feed::SovDelivery>,
    directory: Arc<dyn sov_feed::SovSystemDirectory>,
    tickers: Arc<dyn sov_feed::SovTickerResolver>,
    reachability: Arc<dyn sov_feed::SovReachabilitySource>,
) {
    let mut cadence = tokio::time::interval_at(tokio::time::Instant::now(), interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    let collector = sov_feed::SovCollector::new(
                        store,
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        directory.clone(),
                        tickers.clone(),
                        reachability.clone(),
                    );
                    if let Err(error) =
                        run_sov_cycle_isolated(async move { collector.collect_map_cycle().await })
                            .await
                    {
                        warn!("sov map collection cycle failed: {error}");
                    }
                }
                Err(error) => warn!("sov map feed database unavailable: {error}"),
            },
            Err(error) => warn!("sov map feed migrations unavailable: {error}"),
        }
    }
}

/// Spawns the Wanderer chain reachability poll loop (ticket 05). Mirrors
/// `spawn_sov_stage_loop`: fixed cadence via `interval_at` +
/// `MissedTickBehavior::Skip` (no exponential backoff -- Wanderer errors
/// are not coordinated through the shared ESI limiter, so there is no
/// shared pause state to honour, and the spec's cadence is a fixed "every
/// two minutes" regardless of recent errors), reconnects every tick
/// (self-healing, idempotent migrations), and never touches
/// `sov_store_handle` or ESI, so a stall here cannot affect campaign
/// polling, T-minus evaluation, or command handling.
fn spawn_sov_chain_loop(
    database_url: String,
    interval: Duration,
    source: Arc<dyn sov_feed::WandererChainSource>,
    reachability: Arc<sov_feed::DynamicChainSovReachability>,
    map: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_sov_chain_loop(
        database_url,
        interval,
        source,
        reachability,
        map,
    ))
}

async fn run_sov_chain_loop(
    database_url: String,
    interval: Duration,
    source: Arc<dyn sov_feed::WandererChainSource>,
    reachability: Arc<sov_feed::DynamicChainSovReachability>,
    map: String,
) {
    // Once-per-process start-up log flags (ticket 16), mirroring
    // `run_sov_collection_loop`: the first successful connect + migration
    // check logs the "enabled" line; a later success after a database outage
    // logs the distinct "reconnected" variant.
    let mut started = false;
    let mut failed_since_start = false;
    let poll_secs = interval.as_secs();
    // Seed the in-memory chain from the last persisted snapshot before the
    // first fetch (ticket 05 blocker), so a restart during a Wanderer outage
    // serves the last known chain instead of dropping to stargate-only until
    // the map topology next changes. Best effort: if the database is
    // momentarily unavailable here, the first successful fetch's forced
    // recompute (`collect_cycle`) repopulates the chain anyway.
    if let Ok(store) = sov_feed::SovStore::connect(&database_url).await {
        let seed_collector =
            sov_feed::SovChainCollector::new(store, source.clone(), reachability.clone());
        match seed_collector
            .seed_reachability_from_persisted_snapshot()
            .await
        {
            Ok(true) => {
                info!("sov chain reachability seeded from the last persisted snapshot")
            }
            Ok(false) => {}
            Err(error) => warn!("sov chain reachability seed skipped: {error}"),
        }
    }

    let mut cadence = tokio::time::interval_at(tokio::time::Instant::now(), interval);
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(_limiter_store) => match sov_feed::SovStore::connect(&database_url).await {
                Ok(store) => {
                    if !started {
                        info!(
                            "{}",
                            sov_chain_reachability_start_message(&map, poll_secs, false)
                        );
                        started = true;
                    } else if failed_since_start {
                        info!(
                            "{}",
                            sov_chain_reachability_start_message(&map, poll_secs, true)
                        );
                        failed_since_start = false;
                    }
                    let collector = sov_feed::SovChainCollector::new(
                        store,
                        source.clone(),
                        reachability.clone(),
                    );
                    match run_sov_cycle_isolated(async move { collector.collect_cycle().await })
                        .await
                    {
                        Ok(report) => {
                            if report.recomputed {
                                info!("sov chain reachability recomputed: topology changed");
                            }
                        }
                        // The Wanderer error/code/body summary is already
                        // folded into `error`'s `Display` impl (spec: "warn!
                        // with the Wanderer error code/body summary"); the
                        // last good snapshot and the last computed routes
                        // are untouched (see `SovChainCollector::collect_cycle`'s
                        // doc comment).
                        Err(error) => warn!("sov chain collection cycle failed: {error}"),
                    }
                }
                Err(error) => {
                    failed_since_start = true;
                    warn!("sov chain feed database unavailable: {error}");
                }
            },
            Err(error) => {
                failed_since_start = true;
                warn!("sov chain feed migrations unavailable: {error}");
            }
        }
    }
}

/// Spawns the watchlist feed's independent hourly collection loop (ticket
/// 08). Mirrors [`spawn_sov_collection_loop`]: it does not itself connect to
/// Postgres, only spawns a task that reconnects every iteration
/// (self-healing, idempotent migrations) and isolates each cycle's panics
/// via [`run_sov_cycle_isolated`].
pub fn spawn_watchlist_collection_loop(
    database_url: String,
    store_handle: watchlist_feed::WatchlistStoreHandle,
    interval: Duration,
    esi: Arc<dyn watchlist_feed::WatchlistEsi>,
    delivery: Arc<dyn watchlist_feed::WatchlistDelivery>,
    resolver: Arc<dyn watchlist_feed::WatchlistEntityResolver>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_watchlist_collection_loop(
        database_url,
        store_handle,
        interval,
        esi,
        delivery,
        resolver,
    ))
}

async fn run_watchlist_collection_loop(
    database_url: String,
    store_handle: watchlist_feed::WatchlistStoreHandle,
    interval: Duration,
    esi: Arc<dyn watchlist_feed::WatchlistEsi>,
    delivery: Arc<dyn watchlist_feed::WatchlistDelivery>,
    resolver: Arc<dyn watchlist_feed::WatchlistEntityResolver>,
) {
    let mut consecutive_failures = 0_u32;
    // Once-per-process start-up log flags (ticket 16), mirroring
    // `run_sov_collection_loop`.
    let mut started = false;
    let mut failed_since_start = false;
    loop {
        let mut delay = interval;
        match contract_intelligence::ContractCollectionStore::connect(&database_url).await {
            Ok(limiter_store) => match watchlist_feed::WatchlistStore::connect(&database_url).await
            {
                Ok(store) => {
                    let store = Arc::new(store);
                    *store_handle.write().await = Some(store.clone());
                    if !started {
                        info!("{}", watchlist_feed_start_message(false));
                        started = true;
                    } else if failed_since_start {
                        info!("{}", watchlist_feed_start_message(true));
                        failed_since_start = false;
                    }
                    let collector = watchlist_feed::WatchlistCollector::new(
                        (*store).clone(),
                        esi.clone(),
                        Arc::new(limiter_store),
                        delivery.clone(),
                        resolver.clone(),
                    );
                    match run_sov_cycle_isolated(async move { collector.collect_cycle().await })
                        .await
                    {
                        Ok(report) => {
                            if let Some(paused_until) = report.paused_until {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                delay = contract_intelligence::collection_retry_delay(
                                    interval,
                                    Some(paused_until),
                                    consecutive_failures,
                                );
                            } else {
                                consecutive_failures = 0;
                            }
                        }
                        Err(error) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            delay = contract_intelligence::collection_retry_delay(
                                interval,
                                None,
                                consecutive_failures,
                            );
                            warn!("watchlist collection paused after failure: {error}");
                        }
                    }
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    delay = contract_intelligence::collection_retry_delay(
                        interval,
                        None,
                        consecutive_failures,
                    );
                    *store_handle.write().await = None;
                    failed_since_start = true;
                    warn!(
                        "watchlist feed database unavailable; retrying without an in-memory fallback: {error}"
                    );
                }
            },
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                delay = contract_intelligence::collection_retry_delay(
                    interval,
                    None,
                    consecutive_failures,
                );
                *store_handle.write().await = None;
                failed_since_start = true;
                warn!("watchlist feed migrations unavailable; retrying: {error}");
            }
        }
        tokio::time::sleep(delay).await;
    }
}

/// Runs one sov collector cycle (`collect_cycle` or
/// `evaluate_stage_cycle`) in its own task so a panic inside it -- e.g.
/// malformed persisted data reaching a panicking arithmetic operation --
/// is caught and logged rather than silently killing the long-running
/// loop that called it (review finding 1c on ticket 03: a panicking
/// `evaluate_stage_cycle` used to take down `run_sov_stage_loop`
/// permanently, with no tracing and no health signal, since that loop
/// makes no ESI call for `sov_esi_progress` to ever notice). A panic or
/// task cancellation is folded into `Err` alongside an ordinary cycle
/// failure, so callers keep their existing success/failure branching
/// unchanged; only a genuinely successful cycle returns `Ok`.
async fn run_sov_cycle_isolated<F, T, E>(future: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    match tokio::spawn(future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(join_error) if join_error.is_panic() => {
            error!("sov collector cycle panicked and was isolated: {join_error}");
            Err(format!("panicked: {join_error}"))
        }
        Err(join_error) => Err(format!("cancelled: {join_error}")),
    }
}

#[cfg(test)]
mod sov_cycle_isolation_tests {
    use super::run_sov_cycle_isolated;

    #[tokio::test]
    async fn a_panicking_cycle_future_is_caught_and_reported_as_an_error() {
        // Review finding 1c: proves the wrapper itself survives a panic
        // inside the wrapped future rather than propagating it to the
        // caller (and, by construction via `tokio::spawn`, to whatever
        // task awaits the wrapper) -- a cheap substitute for injecting a
        // panicking fake collector cycle through the full loop.
        let result: Result<(), String> = run_sov_cycle_isolated::<_, (), String>(async {
            panic!("simulated collector panic");
        })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("panicked"));
    }

    #[tokio::test]
    async fn a_successful_cycle_future_returns_its_value() {
        let result: Result<u32, String> =
            run_sov_cycle_isolated::<_, u32, String>(async { Ok(42_u32) }).await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn a_failing_cycle_future_returns_its_error_message() {
        let result: Result<(), String> =
            run_sov_cycle_isolated(async { Err::<(), _>("boom".to_string()) }).await;
        assert_eq!(result, Err("boom".to_string()));
    }
}

#[cfg(test)]
mod startup_log_message_tests {
    use super::{
        sov_campaign_feed_start_message, sov_chain_reachability_start_message,
        watchlist_feed_start_message, SovFeedStartupSummary,
    };

    #[test]
    fn sov_campaign_started_reports_home_system_and_graph_size() {
        let summary = SovFeedStartupSummary {
            home_system_id: 30_002_086,
            graph: Some((8_035, 26_942)),
        };
        assert_eq!(
            sov_campaign_feed_start_message(summary, false),
            "sov campaign feed started (home system 30002086, stargate graph 8035 systems / 26942 edges)"
        );
    }

    #[test]
    fn sov_campaign_reconnected_uses_the_reconnected_lead() {
        let summary = SovFeedStartupSummary {
            home_system_id: 30_002_086,
            graph: Some((8_035, 26_942)),
        };
        assert_eq!(
            sov_campaign_feed_start_message(summary, true),
            "sov campaign feed reconnected (home system 30002086, stargate graph 8035 systems / 26942 edges)"
        );
    }

    #[test]
    fn sov_campaign_started_reports_graph_unavailable_when_not_loaded() {
        let summary = SovFeedStartupSummary {
            home_system_id: 30_000_142,
            graph: None,
        };
        assert_eq!(
            sov_campaign_feed_start_message(summary, false),
            "sov campaign feed started (home system 30000142, stargate graph unavailable)"
        );
    }

    #[test]
    fn sov_chain_enabled_names_the_map_slug_and_poll_cadence() {
        assert_eq!(
            sov_chain_reachability_start_message("home-chain", 120, false),
            "sov chain reachability enabled (map home-chain, poll every 120 s)"
        );
    }

    #[test]
    fn sov_chain_reconnected_uses_the_reconnected_lead() {
        assert_eq!(
            sov_chain_reachability_start_message("home-chain", 120, true),
            "sov chain reachability reconnected (map home-chain, poll every 120 s)"
        );
    }

    #[test]
    fn watchlist_feed_started_and_reconnected_variants() {
        assert_eq!(
            watchlist_feed_start_message(false),
            "watchlist feed started"
        );
        assert_eq!(
            watchlist_feed_start_message(true),
            "watchlist feed reconnected"
        );
    }
}
