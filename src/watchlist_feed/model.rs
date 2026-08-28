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
    /// A watched entity's member count moved more than ten percent against
    /// the snapshot nearest seven days ago (ticket 09). Fires once per band
    /// crossing, re-arming only after the count returns inside the band.
    MemberDelta,
    /// A watched corporation's current alliance differs from its previous
    /// snapshot's alliance, including joining or leaving an alliance
    /// entirely (ticket 09).
    CorpChangedAlliance,
}

/// Every event kind a subscription may select today (spec: "Subscriptions
/// per channel choose event kinds"). Later tickets append to this list; a
/// stored `event_kinds` array element this does not recognize is simply
/// dropped at read time (forward-compatible), never a parse failure.
pub const WATCHLIST_EVENT_KINDS: &[WatchlistEventKind] = &[
    WatchlistEventKind::CorpJoined,
    WatchlistEventKind::CorpLeft,
    WatchlistEventKind::MemberDelta,
    WatchlistEventKind::CorpChangedAlliance,
];

impl WatchlistEventKind {
    /// The stored `event_kind` string (part of the delivery dedup key and
    /// the `watchlist_subscriptions.event_kinds` array).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CorpJoined => "corp_joined",
            Self::CorpLeft => "corp_left",
            Self::MemberDelta => "member_delta",
            Self::CorpChangedAlliance => "corp_changed_alliance",
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
            "member_delta" => Some(Self::MemberDelta),
            "corp_changed_alliance" => Some(Self::CorpChangedAlliance),
            _ => None,
        }
    }

    /// The embed footer text naming this event kind (spec "Events ...
    /// footer names the event kind").
    pub fn footer_label(self) -> &'static str {
        match self {
            Self::CorpJoined => "Corporation Joined",
            Self::CorpLeft => "Corporation Left",
            Self::MemberDelta => "Member Count Change",
            Self::CorpChangedAlliance => "Corporation Changed Alliance",
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

    /// Feeds a corporation's identity (as returned by
    /// `GET /corporations/{id}/` during the member-snapshot pass) into the
    /// shared killfeed caches so a later [`Self::corporation`] resolution at
    /// prepare time is a cache hit and the embed needs no extra ESI lookup
    /// (ticket 09). Default no-op for resolvers without a writable cache
    /// (the test fake).
    async fn note_corporation(
        &self,
        _corporation_id: i64,
        _name: Option<String>,
        _ticker: Option<String>,
    ) {
    }

    /// Persists whatever the member-snapshot pass accumulated through
    /// [`Self::note_corporation`], called once at the end of the pass (ticket
    /// 09). This lets an implementation update only its in-memory caches per
    /// corporation and flush the backing store a single time, instead of
    /// rewriting it once per fresh corporation. Default no-op.
    async fn flush_noted_corporations(&self) {}
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

// --- Member-count delta band crossing (ticket 09) ---
//
// A pure, database-free function so the band-crossing rule is unit-testable
// (Testing Decisions, `.scratch/esi-intel-feeds/spec.md`: "member-delta band
// crossing must be a pure function with unit tests").

/// The ten-percent band half-width, as an integer percentage so the
/// comparison avoids floating point.
pub const MEMBER_DELTA_THRESHOLD_PERCENT: i128 = 10;

/// The reference window: the delta is measured against the snapshot nearest
/// this far in the past.
pub const MEMBER_DELTA_REFERENCE_WINDOW: chrono::Duration = chrono::Duration::days(7);

/// Minimum age of the reference snapshot before a delta may fire. The
/// reference is the snapshot *nearest* seven days ago, but if the oldest
/// history we have is younger than this, we have too little history to judge
/// a "seven day" move and stay silent. Chosen as 6.5 days: close enough to
/// the seven-day window to accept the normal hourly snapshot that lands just
/// before or after the exact mark, but far enough to reject a freshly-added
/// entity whose only history is a few days old.
pub const MEMBER_DELTA_MIN_HISTORY: chrono::Duration =
    chrono::Duration::milliseconds(6 * 24 * 3600 * 1000 + 12 * 3600 * 1000);

/// The direction of a member-count crossing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberDeltaDirection {
    Up,
    Down,
}

impl MemberDeltaDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "up" => Some(Self::Up),
            "down" => Some(Self::Down),
            _ => None,
        }
    }
}

/// The reference snapshot a delta is measured against: the count and when it
/// was observed (the snapshot nearest [`MEMBER_DELTA_REFERENCE_WINDOW`] ago).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemberReference {
    pub count: i64,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

/// A fired member-count crossing, carrying the evidence the embed renders.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemberDeltaAlert {
    pub direction: MemberDeltaDirection,
    pub reference_count: i64,
    pub current_count: i64,
    pub reference_observed_at: chrono::DateTime<chrono::Utc>,
}

impl MemberDeltaAlert {
    /// The percentage magnitude of the move, rounded down, for the embed.
    /// `reference_count` is guaranteed positive by [`evaluate_member_delta`].
    pub fn percentage(&self) -> i64 {
        let delta = (self.current_count as i128 - self.reference_count as i128).abs();
        ((delta * 100) / self.reference_count as i128) as i64
    }
}

/// The outcome of evaluating one entity's member count against its reference
/// and armed state.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MemberDeltaOutcome {
    /// No reference snapshot old enough (or reference count zero): stay
    /// silent, leave band state unchanged.
    InsufficientHistory,
    /// Inside the +/-10 % band: re-arm (no event).
    InsideBand,
    /// Outside the band but the entity already fired this crossing: no event.
    AlreadyFired,
    /// A fresh crossing: fire once.
    Fires(MemberDeltaAlert),
}

/// The band-crossing rule (spec user stories 45-46). Pure and total: no
/// database, no clock reads beyond the `now` passed in, no panics, no
/// division by a zero reference.
///
/// - `armed` is `true` when the entity is inside the band (or has never
///   fired) and may fire on the next crossing; `false` when it fired the
///   current crossing and must return inside the band to re-arm.
/// - A reference count of zero never fires (no division, and a percentage of
///   any positive count is undefined against zero).
/// - A reference younger than [`MEMBER_DELTA_MIN_HISTORY`] is insufficient
///   history and stays silent.
pub fn evaluate_member_delta(
    now: chrono::DateTime<chrono::Utc>,
    current_count: i64,
    reference: Option<MemberReference>,
    armed: bool,
) -> MemberDeltaOutcome {
    let Some(reference) = reference else {
        return MemberDeltaOutcome::InsufficientHistory;
    };
    if reference.count <= 0 {
        return MemberDeltaOutcome::InsufficientHistory;
    }
    if now.signed_duration_since(reference.observed_at) < MEMBER_DELTA_MIN_HISTORY {
        return MemberDeltaOutcome::InsufficientHistory;
    }
    let delta = (current_count as i128 - reference.count as i128).abs();
    let outside = delta * 100 >= reference.count as i128 * MEMBER_DELTA_THRESHOLD_PERCENT;
    if !outside {
        return MemberDeltaOutcome::InsideBand;
    }
    if !armed {
        return MemberDeltaOutcome::AlreadyFired;
    }
    let direction = if current_count >= reference.count {
        MemberDeltaDirection::Up
    } else {
        MemberDeltaDirection::Down
    };
    MemberDeltaOutcome::Fires(MemberDeltaAlert {
        direction,
        reference_count: reference.count,
        current_count,
        reference_observed_at: reference.observed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
    fn event_kinds_parse_the_two_new_kinds() {
        assert_eq!(
            parse_event_kinds("member_delta, corp_changed_alliance").unwrap(),
            vec![
                WatchlistEventKind::MemberDelta,
                WatchlistEventKind::CorpChangedAlliance
            ]
        );
        assert_eq!(
            WatchlistEventKind::parse("member_delta"),
            Some(WatchlistEventKind::MemberDelta)
        );
        assert_eq!(
            WatchlistEventKind::parse("corp_changed_alliance"),
            Some(WatchlistEventKind::CorpChangedAlliance)
        );
    }

    #[test]
    fn default_all_event_kinds_includes_the_new_kinds() {
        let all = parse_event_kinds("").unwrap();
        assert!(all.contains(&WatchlistEventKind::MemberDelta));
        assert!(all.contains(&WatchlistEventKind::CorpChangedAlliance));
    }

    // --- member_delta pure function ---

    fn at(days_ago: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap()
            - chrono::Duration::days(days_ago)
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap()
    }

    #[test]
    fn member_delta_fires_up_on_a_ten_percent_rise() {
        let outcome = evaluate_member_delta(
            now(),
            110,
            Some(MemberReference {
                count: 100,
                observed_at: at(7),
            }),
            true,
        );
        match outcome {
            MemberDeltaOutcome::Fires(alert) => {
                assert_eq!(alert.direction, MemberDeltaDirection::Up);
                assert_eq!(alert.reference_count, 100);
                assert_eq!(alert.current_count, 110);
                assert_eq!(alert.percentage(), 10);
            }
            other => panic!("expected a fire, got {other:?}"),
        }
    }

    #[test]
    fn member_delta_fires_down_on_a_ten_percent_drop() {
        let outcome = evaluate_member_delta(
            now(),
            90,
            Some(MemberReference {
                count: 100,
                observed_at: at(7),
            }),
            true,
        );
        match outcome {
            MemberDeltaOutcome::Fires(alert) => {
                assert_eq!(alert.direction, MemberDeltaDirection::Down);
                assert_eq!(alert.percentage(), 10);
            }
            other => panic!("expected a fire, got {other:?}"),
        }
    }

    #[test]
    fn member_delta_inside_band_does_not_fire() {
        assert_eq!(
            evaluate_member_delta(
                now(),
                109,
                Some(MemberReference {
                    count: 100,
                    observed_at: at(7),
                }),
                true,
            ),
            MemberDeltaOutcome::InsideBand
        );
    }

    #[test]
    fn member_delta_fires_once_per_crossing() {
        // Already outside the band and disarmed: no second fire.
        assert_eq!(
            evaluate_member_delta(
                now(),
                120,
                Some(MemberReference {
                    count: 100,
                    observed_at: at(7),
                }),
                false,
            ),
            MemberDeltaOutcome::AlreadyFired
        );
    }

    #[test]
    fn member_delta_rearms_after_returning_inside_the_band() {
        // Disarmed, but the count is now back inside the band: re-arm.
        assert_eq!(
            evaluate_member_delta(
                now(),
                105,
                Some(MemberReference {
                    count: 100,
                    observed_at: at(7),
                }),
                false,
            ),
            MemberDeltaOutcome::InsideBand
        );
    }

    #[test]
    fn member_delta_never_fires_against_a_zero_reference() {
        assert_eq!(
            evaluate_member_delta(
                now(),
                50,
                Some(MemberReference {
                    count: 0,
                    observed_at: at(7),
                }),
                true,
            ),
            MemberDeltaOutcome::InsufficientHistory
        );
    }

    #[test]
    fn member_delta_needs_enough_history() {
        // A big move but the reference is only three days old: too little
        // history, stay silent.
        assert_eq!(
            evaluate_member_delta(
                now(),
                200,
                Some(MemberReference {
                    count: 100,
                    observed_at: at(3),
                }),
                true,
            ),
            MemberDeltaOutcome::InsufficientHistory
        );
        // No reference at all is likewise insufficient.
        assert_eq!(
            evaluate_member_delta(now(), 200, None, true),
            MemberDeltaOutcome::InsufficientHistory
        );
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
