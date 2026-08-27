//! Pure data types for the sovereignty campaign feed: campaign facts, the
//! recursive subscription filter grammar, and delivery/notification shapes.
//! Nothing here touches the database or the network, so filter evaluation
//! is unit-testable without a running PostgreSQL instance (Testing
//! Decisions, `.scratch/esi-intel-feeds/spec.md`).

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};

/// A public sovereignty entosis event, as observed by one collection cycle.
/// Mirrors `GET /sovereignty/campaigns/`
/// (`.scratch/esi-intel-feeds/research/01-esi-endpoint-catalog.md`).
/// `defender_id`/`defender_score`/`attackers_score` are only present on
/// Defense Events per the ESI schema; Freeport Events omit them.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SovCampaign {
    pub campaign_id: i64,
    pub event_type: String,
    pub structure_id: i64,
    pub solar_system_id: i64,
    pub constellation_id: i64,
    #[serde(default)]
    pub defender_id: Option<i64>,
    #[serde(default)]
    pub defender_score: Option<f64>,
    #[serde(default)]
    pub attackers_score: Option<f64>,
    pub start_time: DateTime<Utc>,
}

/// Known `event_type` values from the ESI enum (Defense Events and Freeport
/// Events). The wire type stays a plain `String` so an unrecognized future
/// value never fails deserialization; this list is only used to validate
/// the `EventType` filter leaf at subscribe time.
pub const SOV_EVENT_TYPES: &[&str] = &[
    "tcu_defense",
    "ihub_defense",
    "station_defense",
    "station_freeport",
];

/// Alert Stage: one of the discrete, once-only reasons a Sov Campaign is
/// announced (spec "Alert stages and dedup"). `Appeared` and `TMinus` are
/// implemented; later tickets add reachability and timezone-window stages
/// here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SovAlertStage {
    Appeared,
    /// A configured T-minus mark, in minutes before the campaign's
    /// `start_time` (spec "Alert stages and dedup": `tminus:<minutes>`).
    TMinus(i64),
}

impl SovAlertStage {
    /// The dedup-authority stage key stored in `sov_alert_deliveries.stage`
    /// (part of its unique constraint together with subscription and
    /// subject). Owned because `TMinus` formats its minutes into the key.
    pub fn as_str(self) -> String {
        match self {
            Self::Appeared => "appeared".to_string(),
            Self::TMinus(minutes) => format!("tminus:{minutes}"),
        }
    }

    /// The embed footer text naming this stage (spec "Embed": "Footer
    /// names the stage", example `T-120m`).
    pub fn footer_label(self) -> String {
        match self {
            Self::Appeared => "Appeared".to_string(),
            Self::TMinus(minutes) => format!("T-{minutes}m"),
        }
    }
}

/// Per-subscription option key storing configured T-minus marks, in
/// minutes before a campaign's `start_time` (spec "Sov timer subscription
/// language": "T-minus marks in minutes (default 120 and 30)"). Absent
/// entirely from a subscription's `options` document means "use the
/// default"; present as an empty array means marks are disabled for that
/// subscription.
pub const SOV_TMINUS_MARKS_OPTION_KEY: &str = "tminus_marks_minutes";

/// Default T-minus marks applied when a subscription's `options` has no
/// `tminus_marks_minutes` key at all -- including every subscription
/// persisted before this ticket landed, since `options` defaults to `{}`
/// (spec: "default 120 and 30").
pub const SOV_DEFAULT_TMINUS_MARKS_MINUTES: &[i64] = &[120, 30];

/// Upper bound on a single T-minus mark, in minutes (30 days). Rejected at
/// parse time so `/sov_subscribe` cannot store a value large enough to
/// overflow `chrono::TimeDelta` construction; `evaluate_stage_cycle` also
/// guards against an out-of-range value already persisted by some other
/// route (review finding 1 on ticket 03: a huge `i64` panicked
/// `ChronoDuration::minutes`).
pub const SOV_TMINUS_MARK_MAX_MINUTES: i64 = 43_200;

/// Upper bound on the number of distinct T-minus marks a subscription may
/// configure (review finding 1/4 on ticket 03).
pub const SOV_TMINUS_MARKS_MAX_COUNT: usize = 10;

/// Parses the `/sov_subscribe` `tminus_marks` command option: a
/// comma-separated list of T-minus marks in minutes. Each mark must parse
/// as a positive integer no greater than [`SOV_TMINUS_MARK_MAX_MINUTES`];
/// marks are deduplicated while preserving first-occurrence order, and at
/// most [`SOV_TMINUS_MARKS_MAX_COUNT`] distinct marks are accepted. An
/// empty (or all-whitespace) input parses to an empty list -- the explicit
/// "disable marks" input, distinct from the option being omitted
/// entirely.
pub fn parse_tminus_marks_minutes(raw: &str) -> Result<Vec<i64>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let mut seen = std::collections::HashSet::new();
    let mut marks = Vec::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        let minutes: i64 = part
            .parse()
            .map_err(|_| format!("tminus mark is not an integer: '{part}'"))?;
        if minutes <= 0 {
            return Err(format!(
                "tminus marks must be positive integers, got {minutes}"
            ));
        }
        if minutes > SOV_TMINUS_MARK_MAX_MINUTES {
            return Err(format!(
                "tminus marks cannot exceed {SOV_TMINUS_MARK_MAX_MINUTES} minutes (30 days), got {minutes}"
            ));
        }
        if seen.insert(minutes) {
            marks.push(minutes);
            if marks.len() > SOV_TMINUS_MARKS_MAX_COUNT {
                return Err(format!(
                    "tminus marks cannot configure more than {SOV_TMINUS_MARKS_MAX_COUNT} distinct values"
                ));
            }
        }
    }
    Ok(marks)
}

/// The effective T-minus marks for a subscription's stored `options`
/// document: the `tminus_marks_minutes` array when present (including an
/// explicit empty array, which disables marks), or
/// [`SOV_DEFAULT_TMINUS_MARKS_MINUTES`] when the key is absent. This
/// covers subscriptions persisted before this ticket as well as any
/// subscription created without the `tminus_marks` command option (spec:
/// "Existing subscriptions with no stored marks behave as default").
pub fn effective_tminus_marks_minutes(options: &serde_json::Value) -> Vec<i64> {
    match options.get(SOV_TMINUS_MARKS_OPTION_KEY) {
        Some(serde_json::Value::Array(values)) => {
            values.iter().filter_map(|value| value.as_i64()).collect()
        }
        _ => SOV_DEFAULT_TMINUS_MARKS_MINUTES.to_vec(),
    }
}

pub(crate) const SOV_MAX_FILTER_DEPTH: usize = 16;
pub(crate) const SOV_MAX_FILTER_NODES: usize = 128;

/// The recursive And/Or/Not filter tree over [`SovFilterCondition`] leaves
/// (spec "Sov timer subscription language").
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SovFilterNode {
    Condition(SovFilterCondition),
    And(Vec<SovFilterNode>),
    Or(Vec<SovFilterNode>),
    Not(Box<SovFilterNode>),
}

impl SovFilterNode {
    pub fn validate(&self) -> Result<(), String> {
        let mut node_count = 0usize;
        self.validate_at(0, &mut node_count)
    }

    fn validate_at(&self, depth: usize, node_count: &mut usize) -> Result<(), String> {
        if depth > SOV_MAX_FILTER_DEPTH {
            return Err(format!(
                "sov filter nesting cannot exceed {SOV_MAX_FILTER_DEPTH} levels"
            ));
        }
        *node_count += 1;
        if *node_count > SOV_MAX_FILTER_NODES {
            return Err(format!(
                "sov filters cannot contain more than {SOV_MAX_FILTER_NODES} nodes"
            ));
        }
        match self {
            Self::Condition(condition) => condition.validate(),
            Self::And(nodes) | Self::Or(nodes) => {
                if nodes.is_empty() {
                    return Err("AND and OR sov filter nodes must not be empty".to_string());
                }
                for node in nodes {
                    node.validate_at(depth + 1, node_count)?;
                }
                Ok(())
            }
            Self::Not(node) => node.validate_at(depth + 1, node_count),
        }
    }

    /// Evaluates the filter tree against one campaign's facts as of
    /// `observed_at`. `region_of` resolves a solar system ID to a region ID
    /// for the `Region` leaf; an unresolvable system never matches
    /// `Region`. `reachable_jumps` resolves a solar system ID to its jump
    /// distance from the configured home system for the `Reachable` leaf
    /// (ticket 04 added stargates, ticket 05 the Wanderer chain); the
    /// second argument is that leaf's own `allow_frigate_holes`, so a
    /// caller backed by [`crate::sov_feed::SovReachabilitySource`] can pick
    /// the matching one of its two cached BFS results. `None` covers both
    /// "unreachable" and "no stargate graph is loaded" -- either way
    /// `Reachable` never matches. Passing resolvers instead of precomputed
    /// maps keeps this pure and unit-testable while letting production
    /// supply live lookups.
    pub fn matches(
        &self,
        campaign: &SovCampaign,
        observed_at: DateTime<Utc>,
        region_of: &dyn Fn(i64) -> Option<i64>,
        reachable_jumps: &dyn Fn(i64, bool) -> Option<i64>,
    ) -> bool {
        match self {
            Self::Condition(condition) => {
                condition.matches(campaign, observed_at, region_of, reachable_jumps)
            }
            Self::And(nodes) => nodes
                .iter()
                .all(|node| node.matches(campaign, observed_at, region_of, reachable_jumps)),
            Self::Or(nodes) => nodes
                .iter()
                .any(|node| node.matches(campaign, observed_at, region_of, reachable_jumps)),
            Self::Not(node) => !node.matches(campaign, observed_at, region_of, reachable_jumps),
        }
    }

    /// Whether this filter tree contains any `Reachable` leaf configured
    /// with `allow_frigate_holes = true`. `matches` above already threads
    /// each leaf's own `allow_frigate_holes` through independently and
    /// needs no such aggregation; this is only for choosing which of the
    /// two cached routes (spec: "with and without frigate holes") to
    /// *display* for a subscription as a whole -- the embed's
    /// "Reachability"/"Path Risk" fields and `/sov_timers`' jump count,
    /// both of which show one route per subscription/channel, not one per
    /// leaf. A subscription combining a frigate-permitting and a
    /// frigate-excluding `Reachable` leaf (via `Or`/`Not`) is a corner
    /// case the spec does not resolve explicitly; showing the more
    /// permissive route is the conservative choice for display, since the
    /// frigate-inclusive route is always at least as short.
    pub fn allows_frigate_holes_anywhere(&self) -> bool {
        match self {
            Self::Condition(SovFilterCondition::Reachable {
                allow_frigate_holes,
                ..
            }) => *allow_frigate_holes,
            Self::Condition(_) => false,
            Self::And(nodes) | Self::Or(nodes) => {
                nodes.iter().any(Self::allows_frigate_holes_anywhere)
            }
            Self::Not(node) => node.allows_frigate_holes_anywhere(),
        }
    }
}

/// Upper bound on `VulnerableWithin { hours }`, in hours (30 days).
/// Rejected at parse time so `/sov_subscribe` cannot store a value large
/// enough to overflow `chrono::TimeDelta` construction; `matches` also
/// guards against an out-of-range value already persisted by some other
/// route (review finding on ticket 03, the `VulnerableWithin` twin of the
/// T-minus marks blocker: an ordinary huge `i64` panicked
/// `ChronoDuration::hours`).
pub const SOV_VULNERABLE_WITHIN_MAX_HOURS: i64 = 24 * 30;

/// Upper bound on `Reachable { max_jumps }` (spec "Reachability": "Reachable
/// means at most eleven jumps"; ticket 04: "capped at eleven and rejected
/// above that at parse time"). The lower bound is 1: zero or negative
/// jumps never means anything a subscriber would configure.
pub const SOV_REACHABLE_MAX_JUMPS: i64 = 11;

/// Leaves of the sov filter grammar. The watchlist-backed `Defender`
/// variant (spec "sov feed's defender filter... reference the watchlist")
/// is deliberately out of scope here.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SovFilterCondition {
    VulnerableWithin {
        hours: i64,
    },
    Defender {
        alliance_ids: Vec<i64>,
    },
    Region(Vec<i64>),
    System(Vec<i64>),
    EventType(Vec<String>),
    /// Matches when the campaign's system is reachable from the configured
    /// home system in at most `max_jumps` stargate-and-chain jumps (spec
    /// "Sov timer subscription language"; ticket 04 added stargates,
    /// ticket 05 merges the Wanderer chain into the same unified graph).
    /// `allow_frigate_holes` selects which of the two cached BFS results
    /// (spec "Reachability": "Two BFS results are cached per chain
    /// snapshot") a lookup uses: `false` (the default, `serde(default)` so
    /// every filter persisted before this ticket keeps parsing and keeps
    /// its prior behaviour unchanged) excludes frigate-sized wormhole
    /// connections from the route; `true` allows them.
    Reachable {
        max_jumps: i64,
        #[serde(default)]
        allow_frigate_holes: bool,
    },
}

impl SovFilterCondition {
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::VulnerableWithin { hours } => {
                if *hours <= 0 {
                    Err("vulnerable_within hours must be positive".to_string())
                } else if *hours > SOV_VULNERABLE_WITHIN_MAX_HOURS {
                    Err(format!(
                        "vulnerable_within hours cannot exceed {SOV_VULNERABLE_WITHIN_MAX_HOURS} hours (30 days), got {hours}"
                    ))
                } else {
                    Ok(())
                }
            }
            Self::Defender { alliance_ids } => validate_ids(alliance_ids, "defender alliance ID"),
            Self::Region(ids) => validate_ids(ids, "region ID"),
            Self::System(ids) => validate_ids(ids, "system ID"),
            Self::EventType(kinds) => {
                if kinds.is_empty() {
                    return Err("event_type filter cannot be empty".to_string());
                }
                if let Some(unknown) = kinds
                    .iter()
                    .find(|kind| !SOV_EVENT_TYPES.contains(&kind.as_str()))
                {
                    return Err(format!("unknown sov event type: {unknown}"));
                }
                Ok(())
            }
            Self::Reachable { max_jumps, .. } => {
                if *max_jumps < 1 {
                    Err("reachable max_jumps must be at least 1".to_string())
                } else if *max_jumps > SOV_REACHABLE_MAX_JUMPS {
                    Err(format!(
                        "reachable max_jumps cannot exceed {SOV_REACHABLE_MAX_JUMPS}, got {max_jumps}"
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn matches(
        &self,
        campaign: &SovCampaign,
        observed_at: DateTime<Utc>,
        region_of: &dyn Fn(i64) -> Option<i64>,
        reachable_jumps: &dyn Fn(i64, bool) -> Option<i64>,
    ) -> bool {
        match self {
            Self::VulnerableWithin { hours } => {
                // Non-panicking construction (mirrors the T-minus marks
                // fix): `/sov_subscribe` already rejects `hours` above
                // `SOV_VULNERABLE_WITHIN_MAX_HOURS`, but a value stored
                // some other way (direct filter jsonb write, a future
                // migration) must never reach the panicking
                // `ChronoDuration::hours`, nor overflow the
                // `DateTime<Utc>` addition. Treat either overflow as
                // "does not match", never as "always matches".
                let Some(window) = ChronoDuration::try_hours(*hours) else {
                    return false;
                };
                let Some(deadline) = observed_at.checked_add_signed(window) else {
                    return false;
                };
                campaign.start_time <= deadline
            }
            Self::Defender { alliance_ids } => campaign
                .defender_id
                .is_some_and(|id| alliance_ids.contains(&id)),
            Self::Region(ids) => region_of(campaign.solar_system_id)
                .is_some_and(|region_id| ids.contains(&region_id)),
            Self::System(ids) => ids.contains(&campaign.solar_system_id),
            Self::EventType(kinds) => kinds.iter().any(|kind| kind == &campaign.event_type),
            Self::Reachable {
                max_jumps,
                allow_frigate_holes,
            } => {
                // Defensive re-check (mirrors the T-minus/VulnerableWithin
                // ceiling guards): `/sov_subscribe` already rejects
                // `max_jumps` outside 1..=SOV_REACHABLE_MAX_JUMPS, but a
                // value stored some other way must never match "always" or
                // "never" by accident -- treat it as never-due, consistent
                // with the "graph unavailable" case just below.
                if *max_jumps < 1 || *max_jumps > SOV_REACHABLE_MAX_JUMPS {
                    return false;
                }
                reachable_jumps(campaign.solar_system_id, *allow_frigate_holes)
                    .is_some_and(|jumps| jumps >= 0 && jumps <= *max_jumps)
            }
        }
    }
}

fn validate_ids(ids: &[i64], label: &str) -> Result<(), String> {
    if ids.is_empty() {
        return Err(format!("{label} list cannot be empty"));
    }
    if ids.iter().any(|id| *id <= 0) {
        return Err(format!("{label} values must be positive"));
    }
    Ok(())
}

/// The recursive filter, rooted, as persisted in `sov_subscriptions.filter`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SovFilter {
    pub root: SovFilterNode,
}

impl SovFilter {
    pub fn validate(&self) -> Result<(), String> {
        self.root.validate()
    }
}

/// A channel's sovereignty campaign subscription. `options` is an opaque,
/// forward-compatible JSON document: later tickets (T-minus marks,
/// timezone window) add fields there without a schema migration; this
/// ticket neither reads nor writes any option.
#[derive(Clone, Debug, PartialEq)]
pub struct SovSubscription {
    pub guild_id: u64,
    pub channel_id: u64,
    pub name: String,
    pub filter: SovFilter,
    pub options: serde_json::Value,
    pub role_id: Option<u64>,
}

impl SovSubscription {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("subscription name cannot be empty".to_string());
        }
        self.filter.validate()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SovEmbedField {
    pub name: String,
    pub value: String,
    pub inline: bool,
}

/// A fully progressively-enriched notification, assembled once at prepare
/// time and stored verbatim so a restart renders identical content
/// (mirrors `ContractNotificationMessage` in `contract_intelligence.rs`).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SovNotificationMessage {
    pub title: String,
    pub fields: Vec<SovEmbedField>,
    pub footer: String,
}

/// A delivery claimed and ready to send, or already sent (mirrors
/// `PreparedContractDelivery`).
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedSovDelivery {
    pub delivery_id: i64,
    pub guild_id: u64,
    pub channel_id: u64,
    pub subscription_name: String,
    pub subject_kind: String,
    pub subject_id: i64,
    pub stage: String,
    pub role_id: Option<u64>,
    pub message: SovNotificationMessage,
    pub nonce: String,
    pub enforce_nonce: bool,
    /// Post-increment attempt count from the claim that produced this
    /// delivery, used to scale the transient-failure retry backoff (review
    /// finding 3).
    pub attempt_count: i32,
    pub(crate) delivery_claim_token: Option<String>,
}

/// Whether a delivery failure should be retried or given up on
/// permanently. Mirrors `ContractDeliveryError`'s Transient/Permanent
/// split, simplified to the two tiers this feed's dedup and lease model
/// needs (no repair/edit path exists for sov deliveries).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SovDeliveryErrorKind {
    /// Retry after the lease-expiry backoff (network blips, rate limits,
    /// Discord 5xx).
    Transient,
    /// Never retry: the delivery is durably marked `failed` and left there
    /// for operator attention (channel/message deleted, missing
    /// permissions, and the other permanent Discord codes the contract
    /// delivery path already classifies).
    Permanent,
}

/// A delivery failure. Transient failures leave the delivery `prepared`
/// with a backed-off lease so the next cycle retries the claim
/// (restart-safe by construction, like the contract feed's delivery path).
/// Permanent failures are durably recorded as `failed` and never re-claimed.
#[derive(Clone, Debug)]
pub struct SovDeliveryError {
    pub kind: SovDeliveryErrorKind,
    pub message: String,
}

impl SovDeliveryError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            kind: SovDeliveryErrorKind::Transient,
            message: message.into(),
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            kind: SovDeliveryErrorKind::Permanent,
            message: message.into(),
        }
    }

    pub fn is_permanent(&self) -> bool {
        self.kind == SovDeliveryErrorKind::Permanent
    }
}

impl std::fmt::Display for SovDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for SovDeliveryError {}

#[async_trait]
pub trait SovDelivery: Send + Sync {
    async fn send(&self, delivery: PreparedSovDelivery) -> Result<String, SovDeliveryError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::HashMap;

    fn make_campaign(
        campaign_id: i64,
        event_type: &str,
        solar_system_id: i64,
        defender_id: Option<i64>,
        start_time: DateTime<Utc>,
    ) -> SovCampaign {
        SovCampaign {
            campaign_id,
            event_type: event_type.to_string(),
            structure_id: 1,
            solar_system_id,
            constellation_id: 1,
            defender_id,
            defender_score: Some(0.6),
            attackers_score: Some(0.4),
            start_time,
        }
    }

    fn region_of(map: &HashMap<i64, i64>) -> impl Fn(i64) -> Option<i64> + '_ {
        move |system_id| map.get(&system_id).copied()
    }

    #[test]
    fn vulnerable_within_matches_only_inside_the_configured_window() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(
            1,
            "ihub_defense",
            30_000_001,
            Some(99_000_001),
            observed_at + ChronoDuration::hours(5),
        );
        let node = SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 6 });
        assert!(node.matches(&campaign, observed_at, &|_| None, &|_, _| None));

        let node = SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 4 });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn vulnerable_within_never_panics_on_an_out_of_range_hours_value_and_never_matches() {
        // Twin of the T-minus marks blocker: an ordinary huge i64 used to
        // panic `ChronoDuration::hours`. Bypassing validate() (as a
        // directly-written filter document could), matches() must return
        // false rather than panic or "always match".
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(
            1,
            "ihub_defense",
            30_000_001,
            Some(99_000_001),
            observed_at + ChronoDuration::hours(5),
        );
        let node = SovFilterNode::Condition(SovFilterCondition::VulnerableWithin {
            hours: 9_999_999_999_999_999,
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));

        let node =
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: i64::MAX });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn vulnerable_within_validate_rejects_hours_above_the_ceiling() {
        assert!(
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin {
                hours: SOV_VULNERABLE_WITHIN_MAX_HOURS + 1,
            })
            .validate()
            .is_err()
        );
        assert!(
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin {
                hours: 9_999_999_999_999_999,
            })
            .validate()
            .is_err()
        );
    }

    #[test]
    fn vulnerable_within_validate_accepts_exactly_the_ceiling_value() {
        assert!(
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin {
                hours: SOV_VULNERABLE_WITHIN_MAX_HOURS,
            })
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn defender_matches_only_configured_alliance_ids() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_000_001],
        });
        assert!(node.matches(&campaign, observed_at, &|_| None, &|_, _| None));

        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_999_999],
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));

        let no_defender_campaign =
            make_campaign(1, "station_freeport", 30_000_001, None, observed_at);
        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_000_001],
        });
        assert!(!node.matches(&no_defender_campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn region_matches_through_the_supplied_resolver() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let mut map = HashMap::new();
        map.insert(30_000_001, 10_000_060);
        let node = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060]));
        assert!(node.matches(&campaign, observed_at, &region_of(&map), &|_, _| None));

        let node = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_061]));
        assert!(!node.matches(&campaign, observed_at, &region_of(&map), &|_, _| None));

        // An unresolvable system never matches Region.
        let node = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060]));
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn system_and_event_type_match_directly_on_campaign_facts() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        assert!(
            SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_001])).matches(
                &campaign,
                observed_at,
                &|_| None,
                &|_, _| None
            )
        );
        assert!(
            !SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_002])).matches(
                &campaign,
                observed_at,
                &|_| None,
                &|_, _| None
            )
        );
        assert!(SovFilterNode::Condition(SovFilterCondition::EventType(vec![
            "ihub_defense".to_string()
        ]))
        .matches(&campaign, observed_at, &|_| None, &|_, _| None));
        assert!(
            !SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                "tcu_defense".to_string()
            ]))
            .matches(&campaign, observed_at, &|_| None, &|_, _| None)
        );
    }

    #[test]
    fn and_or_not_compose_leaves_recursively() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let and_node = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_001])),
            SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                "ihub_defense".to_string()
            ])),
        ]);
        assert!(and_node.matches(&campaign, observed_at, &|_| None, &|_, _| None));

        let or_node = SovFilterNode::Or(vec![
            SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_099])),
            SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                "ihub_defense".to_string()
            ])),
        ]);
        assert!(or_node.matches(&campaign, observed_at, &|_| None, &|_, _| None));

        let not_node = SovFilterNode::Not(Box::new(SovFilterNode::Condition(
            SovFilterCondition::EventType(vec!["tcu_defense".to_string()]),
        )));
        assert!(not_node.matches(&campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn reachable_matches_when_jumps_are_at_or_under_max_and_not_when_over() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let reachable_jumps = |system_id: i64, _allow_frigate_holes: bool| -> Option<i64> {
            if system_id == 30_000_001 {
                Some(4)
            } else {
                None
            }
        };

        let node = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 4,
            allow_frigate_holes: false,
        });
        assert!(node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));

        let node = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 3,
            allow_frigate_holes: false,
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));

        // A system with no reachability fact at all (unreachable, or no
        // graph loaded) never matches.
        let node = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 11,
            allow_frigate_holes: false,
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn reachable_leaf_without_a_stored_allow_frigate_holes_key_defaults_to_false() {
        // Nit from the ticket 05 review: a filter document persisted
        // before this ticket (or hand-written without the new field) must
        // keep parsing and default to the pre-ticket-05 behaviour --
        // frigate holes excluded -- via `#[serde(default)]`.
        let leaf: SovFilterCondition = serde_json::from_str(r#"{"reachable":{"max_jumps":8}}"#)
            .expect("pre-ticket leaf parses");
        assert_eq!(
            leaf,
            SovFilterCondition::Reachable {
                max_jumps: 8,
                allow_frigate_holes: false,
            }
        );

        let filter: SovFilter =
            serde_json::from_str(r#"{"root":{"condition":{"reachable":{"max_jumps":8}}}}"#)
                .expect("pre-ticket full filter document parses");
        filter
            .validate()
            .expect("pre-ticket document still validates");
        assert!(matches!(
            filter.root,
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 8,
                allow_frigate_holes: false,
            })
        ));
    }

    #[test]
    fn reachable_threads_its_own_allow_frigate_holes_into_the_resolver() {
        // Ticket 05: `allow_frigate_holes` selects which of the two cached
        // BFS results a `SovReachabilitySource` uses; `matches` must pass
        // each leaf's own flag through rather than a fixed value.
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let reachable_jumps = |_: i64, allow_frigate_holes: bool| -> Option<i64> {
            if allow_frigate_holes {
                Some(3)
            } else {
                None
            }
        };

        let excludes_frigate_holes = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 11,
            allow_frigate_holes: false,
        });
        assert!(!excludes_frigate_holes.matches(
            &campaign,
            observed_at,
            &|_| None,
            &reachable_jumps
        ));

        let allows_frigate_holes = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 11,
            allow_frigate_holes: true,
        });
        assert!(allows_frigate_holes.matches(&campaign, observed_at, &|_| None, &reachable_jumps));
    }

    #[test]
    fn allows_frigate_holes_anywhere_finds_a_leaf_nested_under_and_or_not() {
        let plain = SovFilterNode::Condition(SovFilterCondition::EventType(vec![
            "ihub_defense".to_string()
        ]));
        assert!(!plain.allows_frigate_holes_anywhere());

        let excludes = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 5,
            allow_frigate_holes: false,
        });
        assert!(!excludes.allows_frigate_holes_anywhere());

        let nested = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 12 }),
            SovFilterNode::Not(Box::new(SovFilterNode::Or(vec![SovFilterNode::Condition(
                SovFilterCondition::Reachable {
                    max_jumps: 5,
                    allow_frigate_holes: true,
                },
            )]))),
        ]);
        assert!(nested.allows_frigate_holes_anywhere());
    }

    #[test]
    fn reachable_never_matches_when_the_graph_is_unavailable() {
        // Spec: a missing/malformed stargate graph degrades the feed to
        // "Reachable never matches", not a startup failure.
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let node = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 11,
            allow_frigate_holes: false,
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &|_, _| None));
    }

    #[test]
    fn reachable_composes_with_and_alongside_defender() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let reachable_jumps = |_: i64, _: bool| Some(2);
        let node = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
            SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![99_000_001],
            }),
        ]);
        assert!(node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));

        // Reachable alone fails once the max_jumps budget is too small,
        // even though Defender still matches.
        let node = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 1,
                allow_frigate_holes: false,
            }),
            SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![99_000_001],
            }),
        ]);
        assert!(!node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));
    }

    #[test]
    fn reachable_validate_accepts_one_through_eleven_and_rejects_zero_and_above_eleven() {
        assert!(SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 1,
            allow_frigate_holes: false
        })
        .validate()
        .is_ok());
        assert!(SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: SOV_REACHABLE_MAX_JUMPS,
            allow_frigate_holes: false,
        })
        .validate()
        .is_ok());
        assert!(SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 0,
            allow_frigate_holes: false
        })
        .validate()
        .is_err());
        assert!(SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: SOV_REACHABLE_MAX_JUMPS + 1,
            allow_frigate_holes: false,
        })
        .validate()
        .is_err());
        assert!(SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: -1,
            allow_frigate_holes: false
        })
        .validate()
        .is_err());
    }

    #[test]
    fn reachable_matches_defensively_returns_false_for_an_out_of_range_max_jumps_bypassing_validate(
    ) {
        // Twin of the T-minus/VulnerableWithin ceiling guards: a value
        // stored some other way (direct filter jsonb write) must never
        // reach "always matches" or "always doesn't apply the budget".
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let reachable_jumps = |_: i64, _: bool| Some(2);
        let node = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: SOV_REACHABLE_MAX_JUMPS + 1,
            allow_frigate_holes: false,
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));

        let node = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 0,
            allow_frigate_holes: false,
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));
    }

    #[test]
    fn validate_rejects_empty_and_or_and_unknown_event_types() {
        assert!(SovFilterNode::And(vec![]).validate().is_err());
        assert!(SovFilterNode::Or(vec![]).validate().is_err());
        assert!(
            SovFilterNode::Condition(SovFilterCondition::EventType(vec![]))
                .validate()
                .is_err()
        );
        assert!(SovFilterNode::Condition(SovFilterCondition::EventType(vec![
            "not_a_real_event".to_string()
        ]))
        .validate()
        .is_err());
        assert!(
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 0 })
                .validate()
                .is_err()
        );
        assert!(SovFilterNode::Condition(SovFilterCondition::Region(vec![]))
            .validate()
            .is_err());
    }

    #[test]
    fn validate_accepts_a_well_formed_recursive_tree() {
        let node = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 6 }),
            SovFilterNode::Or(vec![
                SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060])),
                SovFilterNode::Not(Box::new(SovFilterNode::Condition(
                    SovFilterCondition::EventType(vec!["station_freeport".to_string()]),
                ))),
            ]),
        ]);
        assert!(node.validate().is_ok());
    }

    #[test]
    fn parse_tminus_marks_minutes_parses_and_dedupes_preserving_order() {
        assert_eq!(
            parse_tminus_marks_minutes("120, 30, 120").unwrap(),
            vec![120, 30]
        );
        assert_eq!(parse_tminus_marks_minutes("45").unwrap(), vec![45]);
    }

    #[test]
    fn parse_tminus_marks_minutes_empty_or_whitespace_input_yields_an_empty_list() {
        assert_eq!(parse_tminus_marks_minutes("").unwrap(), Vec::<i64>::new());
        assert_eq!(
            parse_tminus_marks_minutes("   ").unwrap(),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn parse_tminus_marks_minutes_rejects_non_positive_and_non_integer_values() {
        assert!(parse_tminus_marks_minutes("0").is_err());
        assert!(parse_tminus_marks_minutes("-5").is_err());
        assert!(parse_tminus_marks_minutes("not-a-number").is_err());
        assert!(parse_tminus_marks_minutes("120,abc").is_err());
    }

    #[test]
    fn parse_tminus_marks_minutes_rejects_marks_above_the_ceiling() {
        // Review finding 1: an ordinary huge i64 (e.g. a typo) must never
        // reach `ChronoDuration::minutes`, which panics out of range.
        assert!(parse_tminus_marks_minutes("9999999999999999").is_err());
        assert!(
            parse_tminus_marks_minutes(&(SOV_TMINUS_MARK_MAX_MINUTES + 1).to_string()).is_err()
        );
    }

    #[test]
    fn parse_tminus_marks_minutes_accepts_exactly_the_ceiling_value() {
        assert_eq!(
            parse_tminus_marks_minutes(&SOV_TMINUS_MARK_MAX_MINUTES.to_string()).unwrap(),
            vec![SOV_TMINUS_MARK_MAX_MINUTES]
        );
    }

    #[test]
    fn parse_tminus_marks_minutes_rejects_more_than_the_max_count() {
        let too_many = (1..=(SOV_TMINUS_MARKS_MAX_COUNT as i64 + 1))
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_tminus_marks_minutes(&too_many).is_err());
    }

    #[test]
    fn parse_tminus_marks_minutes_accepts_exactly_the_max_count() {
        let exactly_max = (1..=(SOV_TMINUS_MARKS_MAX_COUNT as i64))
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            parse_tminus_marks_minutes(&exactly_max).unwrap().len(),
            SOV_TMINUS_MARKS_MAX_COUNT
        );
    }

    #[test]
    fn effective_tminus_marks_minutes_defaults_when_the_key_is_absent() {
        assert_eq!(
            effective_tminus_marks_minutes(&serde_json::json!({})),
            SOV_DEFAULT_TMINUS_MARKS_MINUTES.to_vec()
        );
        assert_eq!(
            effective_tminus_marks_minutes(&serde_json::json!({"unrelated": true})),
            SOV_DEFAULT_TMINUS_MARKS_MINUTES.to_vec()
        );
    }

    #[test]
    fn effective_tminus_marks_minutes_returns_stored_values_including_an_explicit_empty_list() {
        assert_eq!(
            effective_tminus_marks_minutes(&serde_json::json!({"tminus_marks_minutes": [60, 15]})),
            vec![60, 15]
        );
        assert_eq!(
            effective_tminus_marks_minutes(&serde_json::json!({"tminus_marks_minutes": []})),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn subscription_validate_rejects_an_empty_name() {
        let subscription = SovSubscription {
            guild_id: 1,
            channel_id: 1,
            name: "  ".to_string(),
            filter: SovFilter {
                root: SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_001])),
            },
            options: serde_json::json!({}),
            role_id: None,
        };
        assert!(subscription.validate().is_err());
    }

    #[test]
    fn readme_sov_subscribe_filter_examples_round_trip() {
        // Pins the two `/sov_subscribe filter:` examples documented in
        // README.md's "Sov timer subscription filter grammar" section: if
        // either literal ever stops parsing or validating, this test
        // catches it before the README goes stale.
        let reachable_roaming_example = r#"{"root":{"and":[{"condition":{"vulnerable_within":{"hours":12}}},{"condition":{"reachable":{"max_jumps":8,"allow_frigate_holes":true}}}]}}"#;
        let parsed: SovFilter = serde_json::from_str(reachable_roaming_example)
            .expect("README reachable-roaming example parses");
        parsed
            .validate()
            .expect("README reachable-roaming example validates");
        assert!(matches!(parsed.root, SovFilterNode::And(ref nodes) if nodes.len() == 2));
        assert!(parsed.root.allows_frigate_holes_anywhere());

        let jita_not_freeport_example = r#"{"root":{"and":[{"condition":{"system":[30000142]}},{"not":{"condition":{"event_type":["station_freeport"]}}}]}}"#;
        let parsed: SovFilter = serde_json::from_str(jita_not_freeport_example)
            .expect("README Jita-not-freeport example parses");
        parsed
            .validate()
            .expect("README Jita-not-freeport example validates");
        assert!(matches!(parsed.root, SovFilterNode::And(ref nodes) if nodes.len() == 2));
    }
}
