//! Pure data types for the watchlist feed: the per-guild registry of
//! Watched Entities, the corporation join/leave event grammar, channel
//! subscriptions, and the delivery/notification shapes. Nothing here
//! touches the database or the network, so parsing and event classification
//! are unit-testable without a running PostgreSQL instance (Testing
//! Decisions, `.scratch/esi-intel-feeds/spec.md`).
//!
//! Ticket 08 implements only the `corp_joined` / `corp_left` events; later
//! tickets (09 member deltas / corp alliance change, 10 wars) extend
//! [`WatchlistEventKind`] and the free-form `event_kind` delivery column
//! without a schema migration.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The two kinds of entity a guild can watch (spec "Watchlist feed":
/// "Registry per guild: kind (alliance or corporation)"). The wire form is
/// the lowercase string stored in `watchlist_entities.kind` and matched by
/// that table's `CHECK` constraint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchlistKind {
    Alliance,
    Corporation,
}

impl WatchlistKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Alliance => "alliance",
            Self::Corporation => "corporation",
        }
    }

    /// Parses the `/watch add kind` option. Case-insensitive on the two
    /// accepted values; every other input is rejected rather than guessed.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "alliance" => Ok(Self::Alliance),
            "corporation" | "corp" => Ok(Self::Corporation),
            other => Err(format!("unknown watchlist kind: '{other}'")),
        }
    }

    /// Whether an `/universe/ids` name resolution for this kind reads the
    /// `alliances` (vs `corporations`) result bucket, and whether a ticker
    /// lookup for it hits the `/alliances/{id}/` (vs `/corporations/{id}/`)
    /// route.
    pub fn is_alliance(self) -> bool {
        matches!(self, Self::Alliance)
    }
}

/// One Watched Entity as persisted in `watchlist_entities`. `ticker`/`name`
/// are resolved once at `/watch add` time and stored verbatim so `/watch
/// list` and the collector need no re-resolution for the entity itself
/// (spec "Watchlist feed").
#[derive(Clone, Debug, PartialEq)]
pub struct WatchedEntity {
    pub guild_id: u64,
    pub kind: WatchlistKind,
    pub entity_id: i64,
    pub ticker: Option<String>,
    pub name: Option<String>,
}

/// One classified membership-change event, produced by diffing a fresh
/// alliance corporation listing against the persisted snapshot. Only
/// [`Self::CorpJoined`] / [`Self::CorpLeft`] exist in ticket 08.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchlistEventKind {
    /// A corporation appeared in a watched alliance's corporation list that
    /// was absent (or previously marked `left_at`) last snapshot.
    CorpJoined,
    /// A corporation that was a member last snapshot is absent from a fresh
    /// listing.
    CorpLeft,
}

/// Every event kind a subscription may select today (spec: "Subscriptions
/// per channel choose event kinds"). Later tickets append to this list; a
/// stored `event_kinds` array element this does not recognize is simply
/// dropped at read time (forward-compatible), never a parse failure.
pub const WATCHLIST_EVENT_KINDS: &[WatchlistEventKind] =
    &[WatchlistEventKind::CorpJoined, WatchlistEventKind::CorpLeft];

impl WatchlistEventKind {
    /// The stored `event_kind` string (part of the delivery dedup key and
    /// the `watchlist_subscriptions.event_kinds` array).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CorpJoined => "corp_joined",
            Self::CorpLeft => "corp_left",
        }
    }

    /// Parses one stored/typed event-kind token. `None` for an unrecognized
    /// token so a forward-compatible `event_kinds` array (a kind added by a
    /// later ticket, read by an older binary) drops the unknown element
    /// rather than failing the whole subscription.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "corp_joined" => Some(Self::CorpJoined),
            "corp_left" => Some(Self::CorpLeft),
            _ => None,
        }
    }

    /// The embed footer text naming this event kind (spec "Events ...
    /// footer names the event kind").
    pub fn footer_label(self) -> &'static str {
        match self {
            Self::CorpJoined => "Corporation Joined",
            Self::CorpLeft => "Corporation Left",
        }
    }
}

/// Upper bound on how many distinct event kinds a subscription may carry.
/// Small and fixed; only a defensive parse-time bound so a pathological
/// comma list cannot store an unbounded array.
pub const WATCHLIST_EVENT_KINDS_MAX_COUNT: usize = 16;

/// Parses the `/watch_subscribe event_kinds` command option: a
/// comma-separated list of event-kind tokens. An empty (or all-whitespace)
/// input parses to *every* known kind (spec: "default all"). Every listed
/// token must be a recognized kind; unknown tokens are rejected here (at
/// subscribe time the user is telling us exactly what they want, unlike the
/// forward-compatible read path). Kinds are deduplicated preserving
/// first-occurrence order.
pub fn parse_event_kinds(raw: &str) -> Result<Vec<WatchlistEventKind>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(WATCHLIST_EVENT_KINDS.to_vec());
    }
    let mut seen = std::collections::HashSet::new();
    let mut kinds = Vec::new();
    for part in trimmed.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        let kind = WatchlistEventKind::parse(token)
            .ok_or_else(|| format!("unknown watchlist event kind: '{token}'"))?;
        if seen.insert(kind.as_str()) {
            kinds.push(kind);
            if kinds.len() > WATCHLIST_EVENT_KINDS_MAX_COUNT {
                return Err(format!(
                    "cannot configure more than {WATCHLIST_EVENT_KINDS_MAX_COUNT} event kinds"
                ));
            }
        }
    }
    if kinds.is_empty() {
        return Ok(WATCHLIST_EVENT_KINDS.to_vec());
    }
    Ok(kinds)
}

/// A channel's watchlist subscription. `options` is an opaque,
/// forward-compatible JSON document mirroring `sov_subscriptions.options`;
/// ticket 08 neither reads nor writes any option.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchlistSubscription {
    pub guild_id: u64,
    pub channel_id: u64,
    pub name: String,
    pub event_kinds: Vec<WatchlistEventKind>,
    pub role_id: Option<u64>,
    pub options: serde_json::Value,
}

impl WatchlistSubscription {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("subscription name cannot be empty".to_string());
        }
        if self.event_kinds.is_empty() {
            return Err("subscription must select at least one event kind".to_string());
        }
        Ok(())
    }

    /// Whether this subscription wants deliveries for `kind`.
    pub fn wants(&self, kind: WatchlistEventKind) -> bool {
        self.event_kinds.contains(&kind)
    }
}

/// One corporation's identity, resolved for embed rendering at prepare time
/// (spec: "Resolve corporation names/tickers for the embed"). Cached into
/// the killfeed tickers/names caches by the resolver so a later poll does
/// not re-fetch.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchlistCorporation {
    pub name: Option<String>,
    pub ticker: Option<String>,
}

/// One alliance's identity for embed rendering.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchlistAlliance {
    pub name: Option<String>,
    pub ticker: Option<String>,
}

/// Resolves alliance/corporation identity for embeds, backed by the
/// killfeed caches with an ESI fallback (mirrors
/// [`crate::sov_feed::SovTickerResolver`]). Called only at prepare time and
/// bounded (one resolution per event's corporation and alliance).
#[async_trait]
pub trait WatchlistEntityResolver: Send + Sync {
    async fn corporation(&self, corporation_id: i64) -> WatchlistCorporation;
    async fn alliance(&self, alliance_id: i64) -> WatchlistAlliance;
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WatchlistEmbedField {
    pub name: String,
    pub value: String,
    pub inline: bool,
}

/// A fully-rendered notification, assembled once at prepare time and stored
/// verbatim (mirrors [`crate::sov_feed::SovNotificationMessage`]) so a
/// restart renders identical content.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WatchlistNotificationMessage {
    pub title: String,
    pub fields: Vec<WatchlistEmbedField>,
    pub footer: String,
}

/// A delivery claimed and ready to send, or already sent (mirrors
/// [`crate::sov_feed::PreparedSovDelivery`]).
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedWatchlistDelivery {
    pub delivery_id: i64,
    pub guild_id: u64,
    pub channel_id: u64,
    pub subscription_name: String,
    pub event_kind: String,
    pub entity_id: i64,
    pub evidence_key: String,
    pub role_id: Option<u64>,
    pub message: WatchlistNotificationMessage,
    pub nonce: String,
    pub enforce_nonce: bool,
    /// Post-increment attempt count from the claim, used to scale the
    /// transient-failure retry backoff (mirrors the sov feed).
    pub attempt_count: i32,
    pub(crate) delivery_claim_token: Option<String>,
}

/// Whether a delivery failure should be retried or given up on
/// permanently. Mirrors [`crate::sov_feed::SovDeliveryErrorKind`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchlistDeliveryErrorKind {
    Transient,
    Permanent,
}

#[derive(Clone, Debug)]
pub struct WatchlistDeliveryError {
    pub kind: WatchlistDeliveryErrorKind,
    pub message: String,
}

impl WatchlistDeliveryError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            kind: WatchlistDeliveryErrorKind::Transient,
            message: message.into(),
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            kind: WatchlistDeliveryErrorKind::Permanent,
            message: message.into(),
        }
    }

    pub fn is_permanent(&self) -> bool {
        self.kind == WatchlistDeliveryErrorKind::Permanent
    }
}

impl std::fmt::Display for WatchlistDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for WatchlistDeliveryError {}

#[async_trait]
pub trait WatchlistDelivery: Send + Sync {
    async fn send(
        &self,
        delivery: PreparedWatchlistDelivery,
    ) -> Result<String, WatchlistDeliveryError>;
}

/// One resolved membership-change event within a watched alliance, produced
/// by [`crate::watchlist_feed::collector`]'s diff. `evidence_date` is the
/// date the transition was observed on (the corporation's fresh
/// `first_seen`/`left_at` date), part of the delivery `evidence_key` so a
/// corporation that leaves and later rejoins deduplicates independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchlistEvent {
    pub alliance_id: i64,
    pub corporation_id: i64,
    pub kind: WatchlistEventKind,
    pub evidence_date: chrono::NaiveDate,
}

impl WatchlistEvent {
    /// The delivery `evidence_key`: corporation id plus the transition date
    /// (spec: "evidence_key = corporation id + first/last seen date").
    ///
    /// The date granularity is deliberate and accepted per spec (fix round
    /// finding 2): a corporation that leaves, rejoins, and leaves again
    /// within the same UTC day collapses to a single `corp_left` delivery,
    /// because the two `corp_left` events share `corporation_id:date` and the
    /// delivery unique constraint dedups them. Sub-day churn is not tracked.
    pub fn evidence_key(&self) -> String {
        format!("{}:{}", self.corporation_id, self.evidence_date)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_kinds_parse_default_all_on_empty_input() {
        assert_eq!(
            parse_event_kinds("").unwrap(),
            WATCHLIST_EVENT_KINDS.to_vec()
        );
        assert_eq!(
            parse_event_kinds("   ").unwrap(),
            WATCHLIST_EVENT_KINDS.to_vec()
        );
    }

    #[test]
    fn event_kinds_parse_a_comma_list_deduped_in_order() {
        assert_eq!(
            parse_event_kinds("corp_left, corp_joined, corp_left").unwrap(),
            vec![WatchlistEventKind::CorpLeft, WatchlistEventKind::CorpJoined]
        );
    }

    #[test]
    fn event_kinds_reject_an_unknown_token() {
        assert!(parse_event_kinds("corp_joined, war_declared").is_err());
    }

    #[test]
    fn event_kind_round_trips_through_as_str_and_parse() {
        for kind in WATCHLIST_EVENT_KINDS {
            assert_eq!(WatchlistEventKind::parse(kind.as_str()), Some(*kind));
        }
        assert_eq!(WatchlistEventKind::parse("nope"), None);
    }

    #[test]
    fn watchlist_kind_parses_case_insensitively() {
        assert_eq!(
            WatchlistKind::parse("Alliance").unwrap(),
            WatchlistKind::Alliance
        );
        assert_eq!(
            WatchlistKind::parse("CORP").unwrap(),
            WatchlistKind::Corporation
        );
        assert!(WatchlistKind::parse("faction").is_err());
    }

    #[test]
    fn evidence_key_combines_corporation_and_date() {
        let event = WatchlistEvent {
            alliance_id: 99_004_901,
            corporation_id: 98_077_439,
            kind: WatchlistEventKind::CorpJoined,
            evidence_date: chrono::NaiveDate::from_ymd_opt(2026, 8, 28).unwrap(),
        };
        assert_eq!(event.evidence_key(), "98077439:2026-08-28");
    }

    #[test]
    fn subscription_validates_a_non_empty_name_and_kind_set() {
        let mut subscription = WatchlistSubscription {
            guild_id: 1,
            channel_id: 2,
            name: "watch".to_string(),
            event_kinds: vec![WatchlistEventKind::CorpJoined],
            role_id: None,
            options: serde_json::json!({}),
        };
        assert!(subscription.validate().is_ok());
        subscription.name = "  ".to_string();
        assert!(subscription.validate().is_err());
        subscription.name = "watch".to_string();
        subscription.event_kinds.clear();
        assert!(subscription.validate().is_err());
    }
}
