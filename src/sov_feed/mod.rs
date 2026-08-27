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

pub mod collector;
pub mod esi;
pub mod model;
pub mod store;

pub use collector::{
    SovClock, SovCollectionError, SovCollectionReport, SovCollector, SovStageEvaluationReport,
    SovSystemDirectory, SovSystemInfo, SovTickerResolver, SystemSovClock,
};
pub use esi::{HttpSovereigntyEsi, SovereigntyEsi, SOV_COMPATIBILITY_DATE};
pub use model::{
    effective_tminus_marks_minutes, parse_tminus_marks_minutes, PreparedSovDelivery, SovAlertStage,
    SovCampaign, SovDelivery, SovDeliveryError, SovEmbedField, SovFilter, SovFilterCondition,
    SovFilterNode, SovNotificationMessage, SovSubscription, SOV_DEFAULT_TMINUS_MARKS_MINUTES,
    SOV_EVENT_TYPES, SOV_TMINUS_MARKS_MAX_COUNT, SOV_TMINUS_MARKS_OPTION_KEY,
    SOV_TMINUS_MARK_MAX_MINUTES, SOV_VULNERABLE_WITHIN_MAX_HOURS,
};
pub use store::{
    available_sov_store, new_sov_store_handle, SovStore, SovStoreHandle, SOV_CAMPAIGNS_RESOURCE_KEY,
};
