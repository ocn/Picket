//! Watchlist feed: a per-guild registry of watched alliances and
//! corporations (`/watch`), channel subscriptions (`/watch_subscribe`), and
//! an hourly collector that alerts subscribed channels when a corporation
//! joins or leaves a watched alliance (spec
//! `.scratch/esi-intel-feeds/spec.md`, ticket
//! `.scratch/esi-intel-feeds/issues/08-watchlist-registry-and-corporation-join-leave-alerts.md`).
//!
//! Only the `corp_joined` / `corp_left` events are implemented here. The
//! registry, subscription, delivery and collector skeleton are built so
//! later tickets (09 member deltas / corp alliance change, 10 wars) extend
//! them without a schema migration.
//!
//! Like [`crate::sov_feed`] this module depends on [`crate::esi_cache`] for
//! conditional-caching and shared-limiter primitives; the composition root
//! (`src/lib.rs`) is the only place that bridges it to the shared limiter
//! row and applies migrations. [`WatchlistStoreHandle`] starts empty and is
//! populated by the background retry loop, so a briefly-unavailable Postgres
//! at boot cannot delay the gateway connection or the killmail pipeline.

pub mod collector;
pub mod esi;
pub mod model;
pub mod store;

pub use collector::{
    SystemWatchlistClock, WatchlistClock, WatchlistCollectionError, WatchlistCollectionReport,
    WatchlistCollector,
};
pub use esi::{CorporationInfo, HttpWatchlistEsi, WatchlistEsi, WATCHLIST_COMPATIBILITY_DATE};
pub use model::{
    evaluate_member_delta, parse_event_kinds, MemberDeltaAlert, MemberDeltaDirection,
    MemberDeltaOutcome, MemberReference, PreparedWatchlistDelivery, WatchedEntity,
    WatchlistAlliance, WatchlistCorporation, WatchlistDelivery, WatchlistDeliveryError,
    WatchlistDeliveryErrorKind, WatchlistEmbedField, WatchlistEntityResolver, WatchlistEvent,
    WatchlistEventKind, WatchlistKind, WatchlistNotificationMessage, WatchlistSubscription,
    WATCHLIST_EVENT_KINDS,
};
pub use store::{
    alliance_resource_key, available_watchlist_store, corp_resource_key,
    new_watchlist_store_handle, WatchlistStore, WatchlistStoreHandle,
};
