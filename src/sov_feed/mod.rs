//! Sovereignty campaign feed: watches public sovereignty campaigns and
//! alerts subscribed channels when a new campaign appears with a start
//! time within twelve hours (spec `.scratch/esi-intel-feeds/spec.md`,
//! ticket
//! `.scratch/esi-intel-feeds/issues/02-sov-campaign-appeared-alert-end-to-end.md`).
//!
//! Only the `appeared` Alert Stage is implemented here. The schema (a
//! forward-compatible `options` JSONB column on subscriptions and a
//! free-form `stage` text column on deliveries) and the filter grammar are
//! kept ready for later tickets (T-minus marks, reachability, timezone
//! windows) without guessing their shape.
//!
//! This module depends on [`crate::esi_cache`] for ESI conditional-caching
//! and shared-limiter primitives, not on [`crate::contract_intelligence`]:
//! the composition root (`src/lib.rs`) is the only place that bridges the
//! two feeds, obtaining a `ContractCollectionStore` on every reconnect
//! attempt for its shared `esi_collection_limiter_state` row and to apply
//! migrations (which cover every file under `migrations/`, including this
//! feed's). Neither connect is ever awaited before the Discord client is
//! constructed: [`SovStoreHandle`] starts empty and is populated by the
//! background retry loop, so a briefly-unavailable Postgres at boot cannot
//! delay the gateway connection or the killmail pipeline, and self-heals
//! once Postgres recovers.

pub mod chain;
pub mod collector;
pub mod esi;
pub mod graph;
pub mod model;
pub mod store;
pub mod wanderer;

pub use chain::{
    connection_topology_changed, log_sse_probe_result, ChainSnapshot, DynamicChainSovReachability,
    PathRisk, SovChainCollectionError, SovChainCollectionReport, SovChainCollector, SovChainStatus,
    SOV_CHAIN_STALE_AFTER,
};
pub use collector::{
    NoSovWatchlist, SovClock, SovCollectionError, SovCollectionReport, SovCollector,
    SovMapCollectionReport, SovReachabilityInfo, SovReachabilitySource, SovStageEvaluationReport,
    SovStructuresCollectionReport, SovSystemDirectory, SovSystemInfo, SovTickerResolver,
    SovWatchlistSource, StaticSovReachability, SystemSovClock,
};
pub use esi::{HttpSovereigntyEsi, SovereigntyEsi, SOV_COMPATIBILITY_DATE};
pub use graph::{
    load_stargate_graph_file, Reachability, StargateGraph, StargateGraphFile,
    StargateGraphLoadError, DEFAULT_STARGATE_GRAPH_PATH,
};
pub use model::{
    effective_tminus_marks_minutes, effective_tz_shift_enabled, effective_tz_window,
    parse_tminus_marks_minutes, parse_tz_window, tz_window_overlap_minutes, PreparedSovDelivery,
    SovAlertStage, SovCampaign, SovDelivery, SovDeliveryError, SovEmbedField, SovFilter,
    SovFilterCondition, SovFilterNode, SovMapEntry, SovNotificationMessage, SovStructure,
    SovSubscription, TzWindow, SOV_DEFAULT_TMINUS_MARKS_MINUTES, SOV_DEFAULT_TZ_WINDOW,
    SOV_EVENT_TYPES, SOV_HUB_STRUCTURE_TYPE_IDS, SOV_REACHABLE_MAX_JUMPS,
    SOV_TMINUS_MARKS_MAX_COUNT, SOV_TMINUS_MARKS_OPTION_KEY, SOV_TMINUS_MARK_MAX_MINUTES,
    SOV_TZ_SHIFT_ENABLED_OPTION_KEY, SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES, SOV_TZ_WINDOW_OPTION_KEY,
    SOV_VULNERABLE_WITHIN_MAX_HOURS,
};
pub use store::{
    available_sov_store, new_sov_store_handle, SovPruneCounts, SovReachabilityTransition, SovStore,
    SovStoreHandle, SovTzWindowTransition, SOV_CAMPAIGNS_RESOURCE_KEY, SOV_MAP_RESOURCE_KEY,
    SOV_RETENTION, SOV_STRUCTURES_RESOURCE_KEY,
};
pub use wanderer::{
    WandererChainSource, WandererClient, WandererConfig, WandererConnection,
    WandererConnectionType, WandererError, WandererMassStatus, WandererShipSizeType,
    WandererSseProbeResult, WandererSystem, WandererSystemStatus, WandererTimeStatus,
};
