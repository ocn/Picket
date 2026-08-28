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
    /// An observed unreachable-to-reachable transition for one
    /// subscription's `Reachable` leaf, keyed by the persisted transition
    /// sequence number (spec "Alert stages and dedup": "the reachable
    /// stage includes the transition sequence number", "it re-arms after
    /// an observed unreachable interval"; ticket 06). The sequence comes
    /// from `SovStore::record_reachability_observation`, never computed
    /// here.
    Reachable(i64),
    /// An observed not-in-window-to-in-window transition for one
    /// subscription's timezone window against one Sovereignty Hub's
    /// vulnerability window, keyed by the persisted transition sequence
    /// number (spec "Alert stages and dedup": "tz_window_entered ... the
    /// window-shift alert only on the transition into the window"; ticket
    /// 07). Mirrors [`Self::Reachable`] exactly; the sequence comes from
    /// `SovStore::record_tz_window_observation`, never computed here.
    TzWindowEntered(i64),
}

impl SovAlertStage {
    /// The dedup-authority stage key stored in `sov_alert_deliveries.stage`
    /// (part of its unique constraint together with subscription and
    /// subject). Owned because `TMinus`/`Reachable` format a number into
    /// the key.
    pub fn as_str(self) -> String {
        match self {
            Self::Appeared => "appeared".to_string(),
            Self::TMinus(minutes) => format!("tminus:{minutes}"),
            Self::Reachable(sequence) => format!("reachable:{sequence}"),
            Self::TzWindowEntered(sequence) => format!("tz_window_entered:{sequence}"),
        }
    }

    /// The embed footer text naming this stage (spec "Embed": "Footer
    /// names the stage", example `T-120m`; ticket 06: "the embed footer
    /// reads 'now reachable'" -- the transition sequence is dedup
    /// plumbing, not shown to the user). `TzWindowEntered`'s footer is
    /// only the fixed prefix (ticket 07 spec: `footer reads "vuln window
    /// entered <window>"`); the caller (`SovCollector::render_tz_window_message`)
    /// appends the subscription's own window text, which this stage value
    /// alone does not carry.
    pub fn footer_label(self) -> String {
        match self {
            Self::Appeared => "Appeared".to_string(),
            Self::TMinus(minutes) => format!("T-{minutes}m"),
            Self::Reachable(_) => "now reachable".to_string(),
            Self::TzWindowEntered(_) => "vuln window entered".to_string(),
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
            Self::Not(node) => {
                // Fix round finding 3: a `Reachable` leaf anywhere under a
                // `Not` (any depth, including double negation) is
                // rejected. `Not(Reachable)` would make
                // `reachable_leaf_state` report `true` exactly when the
                // campaign is physically *unreachable* -- no jumps, no
                // route, no Path Risk to render -- so a genuine
                // false-to-true flip of that inverted value would still
                // try to fire a "now reachable" embed with nothing to
                // show. `has_reachable_leaf` already recurses through the
                // whole subtree (including further nested And/Or/Not), so
                // this catches every depth in one check.
                if node.has_reachable_leaf() {
                    return Err("reachable cannot be negated".to_string());
                }
                node.validate_at(depth + 1, node_count)
            }
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

    /// Whether this filter tree contains a `Reachable` leaf anywhere
    /// (ticket 06: "persist state per (subscription, campaign) ... for
    /// subscriptions whose filter contains a Reachable leaf"). Callers
    /// must check this before persisting or reading reachability
    /// transition state for a subscription -- a subscription this returns
    /// `false` for must never get a `sov_reachability_state` row at all.
    pub fn has_reachable_leaf(&self) -> bool {
        match self {
            Self::Condition(SovFilterCondition::Reachable { .. }) => true,
            Self::Condition(_) => false,
            Self::And(nodes) | Self::Or(nodes) => nodes.iter().any(Self::has_reachable_leaf),
            Self::Not(node) => node.has_reachable_leaf(),
        }
    }

    /// The boolean value of this filter tree's `Reachable` leaf (or
    /// leaves) alone, ignoring every other leaf type entirely (ticket 06:
    /// "state tracking itself follows the Reachable leaf only" -- as
    /// opposed to [`Self::matches`], which requires the *whole* filter to
    /// hold, used to gate whether a transition actually fires an alert).
    /// Returns `None` when the tree contains no `Reachable` leaf at all;
    /// callers must check [`Self::has_reachable_leaf`] first and never
    /// persist state for a subscription this returns `None` for.
    ///
    /// A non-`Reachable` leaf contributes nothing (`None`) to the
    /// aggregation: `And`/`Or` fold their children treating a `None`
    /// child as the identity element (`true` for `And`, `false` for `Or`)
    /// so a single `Reachable` leaf nested alongside e.g. a `Region` leaf
    /// still resolves to that leaf's own value, and a node whose children
    /// are *all* `None` is itself `None`. A subscription combining two
    /// different `Reachable` leaves (different `max_jumps`) via `Or`/`And`
    /// is a corner case the spec does not resolve explicitly -- this
    /// mirrors `allows_frigate_holes_anywhere`'s same disclaimer.
    pub fn reachable_leaf_state(
        &self,
        campaign: &SovCampaign,
        reachable_jumps: &dyn Fn(i64, bool) -> Option<i64>,
    ) -> Option<bool> {
        match self {
            Self::Condition(condition) => condition.reachable_leaf_state(campaign, reachable_jumps),
            Self::And(nodes) => nodes.iter().fold(None, |acc, node| {
                combine_and(acc, node.reachable_leaf_state(campaign, reachable_jumps))
            }),
            Self::Or(nodes) => nodes.iter().fold(None, |acc, node| {
                combine_or(acc, node.reachable_leaf_state(campaign, reachable_jumps))
            }),
            Self::Not(node) => node
                .reachable_leaf_state(campaign, reachable_jumps)
                .map(|value| !value),
        }
    }

    /// Evaluates this filter tree against one Sovereignty Hub's facts for
    /// the `tz_window_entered` Alert Stage (ticket 07, spec: "`Reachable`/
    /// `VulnerableWithin` leaves are ignored for this stage; `Defender`
    /// (hub owner alliance), `Region`, `System` apply; `EventType` is not
    /// applicable (treat as matching)"). "Ignored"/"not applicable" leaves
    /// evaluate as always-matching here -- the same tree used for the
    /// campaign-shaped `matches` above, reinterpreted leaf by leaf for a
    /// structure that has no event type, start time, or reachability
    /// concept of its own. A `Not` over one of these neutral leaves does
    /// invert the neutral `true` to `false`, same as it would for any
    /// other leaf; this is a known, documented corner case (mirroring the
    /// disclaimers on [`Self::allows_frigate_holes_anywhere`] and
    /// [`Self::reachable_leaf_state`]), not one the spec's test list
    /// exercises.
    pub fn matches_structure(
        &self,
        defender_alliance_id: Option<i64>,
        solar_system_id: i64,
        region_of: &dyn Fn(i64) -> Option<i64>,
    ) -> bool {
        match self {
            Self::Condition(condition) => {
                condition.matches_structure(defender_alliance_id, solar_system_id, region_of)
            }
            Self::And(nodes) => nodes.iter().all(|node| {
                node.matches_structure(defender_alliance_id, solar_system_id, region_of)
            }),
            Self::Or(nodes) => nodes.iter().any(|node| {
                node.matches_structure(defender_alliance_id, solar_system_id, region_of)
            }),
            Self::Not(node) => {
                !node.matches_structure(defender_alliance_id, solar_system_id, region_of)
            }
        }
    }
}

/// `And` aggregation for [`SovFilterNode::reachable_leaf_state`]: `None`
/// (no `Reachable` leaf on this side) is the identity element.
fn combine_and(acc: Option<bool>, child: Option<bool>) -> Option<bool> {
    match (acc, child) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) => Some(a && b),
    }
}

/// `Or` aggregation for [`SovFilterNode::reachable_leaf_state`]: `None`
/// (no `Reachable` leaf on this side) is the identity element.
fn combine_or(acc: Option<bool>, child: Option<bool>) -> Option<bool> {
    match (acc, child) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) => Some(a || b),
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

    /// The `Reachable`-leaf-only counterpart of [`Self::matches`], used by
    /// [`SovFilterNode::reachable_leaf_state`]: `Some(bool)` for a
    /// `Reachable` leaf (with the same defensive out-of-range guard as
    /// `matches`), `None` for every other leaf type (it contributes no
    /// reachability fact).
    fn reachable_leaf_state(
        &self,
        campaign: &SovCampaign,
        reachable_jumps: &dyn Fn(i64, bool) -> Option<i64>,
    ) -> Option<bool> {
        match self {
            Self::Reachable {
                max_jumps,
                allow_frigate_holes,
            } => {
                if *max_jumps < 1 || *max_jumps > SOV_REACHABLE_MAX_JUMPS {
                    Some(false)
                } else {
                    Some(
                        reachable_jumps(campaign.solar_system_id, *allow_frigate_holes)
                            .is_some_and(|jumps| jumps >= 0 && jumps <= *max_jumps),
                    )
                }
            }
            Self::VulnerableWithin { .. }
            | Self::Defender { .. }
            | Self::Region(_)
            | Self::System(_)
            | Self::EventType(_) => None,
        }
    }

    /// The structure-shaped counterpart of [`Self::matches`], used by
    /// [`SovFilterNode::matches_structure`] (ticket 07): `Reachable` and
    /// `VulnerableWithin` are ignored (always match); `EventType` is not
    /// applicable to a structure (always matches); `Defender` compares
    /// against the hub's own owner alliance; `Region`/`System` compare
    /// against the hub's system, exactly as for a campaign.
    fn matches_structure(
        &self,
        defender_alliance_id: Option<i64>,
        solar_system_id: i64,
        region_of: &dyn Fn(i64) -> Option<i64>,
    ) -> bool {
        match self {
            Self::VulnerableWithin { .. } | Self::Reachable { .. } | Self::EventType(_) => true,
            Self::Defender { alliance_ids } => {
                defender_alliance_id.is_some_and(|id| alliance_ids.contains(&id))
            }
            Self::Region(ids) => {
                region_of(solar_system_id).is_some_and(|region_id| ids.contains(&region_id))
            }
            Self::System(ids) => ids.contains(&solar_system_id),
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

// --- Sovereignty Hubs and the sovereignty map (ticket 07) ---

/// `structure_type_id` values this feed treats as Sovereignty Hubs (spec:
/// "sov hubs only"). Evidence: `GET /sovereignty/structures/` against
/// `esi.evetech.net` on 2026-08-27 returned 2708 structures, all with
/// `structure_type_id: 32458`; `GET /universe/types/32458/` resolves that
/// ID to name `"Sovereignty Hub"`, group `1012`. The research catalog
/// (`.scratch/esi-intel-feeds/research/01-esi-endpoint-catalog.md`) notes
/// the route "lists sovereignty structures only (Sovereignty Hubs and
/// legacy TCUs)"; no legacy TCU type ID appeared in the live tally (post-
/// Equinox TCUs cannot be entosised and, evidently, no longer appear in
/// this listing at all), so this list holds exactly the one observed ID.
/// A structure of any other `structure_type_id` (this list ever grows, or
/// a genuine legacy TCU reappears) is excluded from `sov_structures`
/// entirely by [`crate::sov_feed::collector::SovCollector::collect_structures_cycle`].
pub const SOV_HUB_STRUCTURE_TYPE_IDS: &[i64] = &[32458];

/// One Sovereignty Hub, as observed by one collection cycle. Mirrors
/// `GET /sovereignty/structures/`
/// (`.scratch/esi-intel-feeds/research/01-esi-endpoint-catalog.md`):
/// `alliance_id`, `vulnerability_occupancy_level`, `vulnerable_start_time`,
/// and `vulnerable_end_time` are all optional on the wire (a freshly
/// deployed or contested hub may not have an owner or a vulnerability
/// window yet); a hub with no vulnerability window is never in-window for
/// the `tz_window_entered` stage (spec: "A hub whose vulnerability fields
/// are null/absent is not-in-window").
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SovStructure {
    pub structure_id: i64,
    pub structure_type_id: i64,
    #[serde(default)]
    pub alliance_id: Option<i64>,
    pub solar_system_id: i64,
    #[serde(default)]
    pub vulnerability_occupancy_level: Option<f64>,
    #[serde(default)]
    pub vulnerable_start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub vulnerable_end_time: Option<DateTime<Utc>>,
}

/// One system's current sovereignty owner, as observed by one collection
/// cycle. Mirrors `GET /sovereignty/map/`: a system may carry any subset
/// of `alliance_id`/`corporation_id`/`faction_id` simultaneously (player
/// sov systems carry alliance and corporation together; FW/NPC systems
/// carry only a faction). The wire field is `system_id`; renamed here to
/// `solar_system_id` for consistency with every other sov feed type.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SovMapEntry {
    #[serde(rename = "system_id")]
    pub solar_system_id: i64,
    #[serde(default)]
    pub alliance_id: Option<i64>,
    #[serde(default)]
    pub corporation_id: Option<i64>,
    #[serde(default)]
    pub faction_id: Option<i64>,
}

/// A per-subscription timezone window, parsed from the `tz_window` option
/// (spec "Sov timer subscription language": "timezone window (default
/// 00:00-04:00 EVE), may cross midnight"). Stored as minutes-since-midnight
/// so overlap arithmetic never re-parses `HH:MM` text.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TzWindow {
    /// 0..=1439.
    pub start_minutes: u16,
    /// 0..=1439. When less than `start_minutes`, the window crosses
    /// midnight (spec: "may cross midnight").
    pub end_minutes: u16,
}

impl TzWindow {
    /// The window's duration in minutes: `end - start` when it does not
    /// cross midnight, `(1440 - start) + end` when it does. Always in
    /// `1..=1440` for a window built by [`parse_tz_window`] (which
    /// rejects `start == end`); a `TzWindow` value bypassing that
    /// constructor with equal start/end degrades to the full 1440-minute
    /// day rather than zero, by the same formula -- never negative, never
    /// panicking.
    fn duration_minutes(&self) -> u16 {
        if self.end_minutes > self.start_minutes {
            self.end_minutes - self.start_minutes
        } else {
            (1440 - self.start_minutes) + self.end_minutes
        }
    }

    /// This window's concrete UTC interval for the occurrence that begins
    /// on `day`, or `None` on an internal overflow (never expected for any
    /// in-range `NaiveDate`/`TzWindow`, but every constructor here is
    /// `checked_*`/`Option`-returning end to end -- no panicking path
    /// exists for a persisted or malformed value to reach).
    fn occurrence_on(&self, day: chrono::NaiveDate) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let start_naive = day.and_hms_opt(
            u32::from(self.start_minutes / 60),
            u32::from(self.start_minutes % 60),
            0,
        )?;
        let start = DateTime::<Utc>::from_naive_utc_and_offset(start_naive, Utc);
        let end = start
            .checked_add_signed(ChronoDuration::minutes(i64::from(self.duration_minutes())))?;
        Some((start, end))
    }

    /// Renders `HH:MM–HH:MM` (en dash) for embeds and footers (spec
    /// "Embed": footer `"vuln window entered 00:00–04:00"`).
    pub fn display(&self) -> String {
        format!(
            "{}\u{2013}{}",
            format_hhmm(self.start_minutes),
            format_hhmm(self.end_minutes)
        )
    }

    /// Renders `HH:MM-HH:MM` (plain ASCII hyphen), the normalized form
    /// stored in a subscription's `options` document -- round-trips
    /// through [`parse_tz_window`] unchanged, unlike [`Self::display`]'s
    /// en dash. Used by `/sov_subscribe` so re-typed whitespace or
    /// formatting in the raw command input never ends up persisted
    /// verbatim.
    pub fn to_option_string(&self) -> String {
        format!(
            "{}-{}",
            format_hhmm(self.start_minutes),
            format_hhmm(self.end_minutes)
        )
    }
}

fn format_hhmm(total_minutes: u16) -> String {
    format!("{:02}:{:02}", total_minutes / 60, total_minutes % 60)
}

/// Parses one `HH:MM` clock value into minutes-since-midnight. Bounded,
/// non-panicking `u16` parsing throughout (review decision carried over
/// from ticket 03/05's T-minus-marks and `max_jumps` guards): no input
/// ever reaches a panicking integer conversion.
fn parse_hhmm(raw: &str) -> Result<u16, String> {
    let raw = raw.trim();
    let (hour_str, minute_str) = raw
        .split_once(':')
        .ok_or_else(|| format!("expected HH:MM, got '{raw}'"))?;
    let hour: u16 = hour_str
        .parse()
        .map_err(|_| format!("invalid hour '{hour_str}'"))?;
    let minute: u16 = minute_str
        .parse()
        .map_err(|_| format!("invalid minute '{minute_str}'"))?;
    if hour > 23 {
        return Err(format!("hour must be 0-23, got {hour}"));
    }
    if minute > 59 {
        return Err(format!("minute must be 0-59, got {minute}"));
    }
    Ok(hour * 60 + minute)
}

/// Parses the `/sov_subscribe` `tz_window` command option:
/// `HH:MM-HH:MM`, EVE/UTC time, optionally crossing midnight (spec: "may
/// cross midnight"). `start == end` is rejected as ambiguous (neither "the
/// whole day" nor "an instant" is spelled out by the spec) rather than
/// silently picking one.
pub fn parse_tz_window(raw: &str) -> Result<TzWindow, String> {
    let trimmed = raw.trim();
    let (start_str, end_str) = trimmed
        .split_once('-')
        .ok_or_else(|| format!("tz_window must be HH:MM-HH:MM, got '{trimmed}'"))?;
    let start_minutes = parse_hhmm(start_str)?;
    let end_minutes = parse_hhmm(end_str)?;
    if start_minutes == end_minutes {
        return Err("tz_window start and end must differ".to_string());
    }
    Ok(TzWindow {
        start_minutes,
        end_minutes,
    })
}

/// Per-subscription option key storing the raw `tz_window` string (spec
/// "Sov timer subscription language").
pub const SOV_TZ_WINDOW_OPTION_KEY: &str = "tz_window";

/// Per-subscription option key storing the `tz_shift_enabled` boolean.
pub const SOV_TZ_SHIFT_ENABLED_OPTION_KEY: &str = "tz_shift_enabled";

/// Default timezone window applied when a subscription's `options` has no
/// `tz_window` key at all (spec: "default 00:00-04:00 EVE").
pub const SOV_DEFAULT_TZ_WINDOW: &str = "00:00-04:00";

/// Minimum overlap, in minutes, between a hub's vulnerability window and a
/// subscription's timezone window for the `tz_window_entered` stage to be
/// eligible to fire (spec: "at least sixty minutes").
pub const SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES: i64 = 60;

/// The effective [`TzWindow`] for a subscription's stored `options`
/// document: the parsed `tz_window` string when present, or
/// [`SOV_DEFAULT_TZ_WINDOW`] when the key is absent -- covering every
/// subscription persisted before this ticket as well as any subscription
/// created without the `tz_window` command option. `None` when a stored
/// value fails to parse (defensive: `/sov_subscribe` already validates via
/// [`parse_tz_window`], but a value stored some other way, e.g. a direct
/// `options` write, must degrade to "never in window" rather than panic
/// or silently fall back to the default).
pub fn effective_tz_window(options: &serde_json::Value) -> Option<TzWindow> {
    match options
        .get(SOV_TZ_WINDOW_OPTION_KEY)
        .and_then(|v| v.as_str())
    {
        Some(raw) => parse_tz_window(raw).ok(),
        None => parse_tz_window(SOV_DEFAULT_TZ_WINDOW).ok(),
    }
}

/// The effective `tz_shift_enabled` flag for a subscription's stored
/// `options` document: `false` when absent or not a boolean (spec:
/// "default false"; only subscriptions that explicitly opt in ever
/// participate in the `tz_window_entered` stage).
pub fn effective_tz_shift_enabled(options: &serde_json::Value) -> bool {
    options
        .get(SOV_TZ_SHIFT_ENABLED_OPTION_KEY)
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

/// Safety bound on how many candidate days [`tz_window_overlap_minutes`]
/// scans. Sovereignty vulnerability windows are always a few hours long in
/// practice (ESI-observed: a handful of hours), so a genuine hub never
/// approaches this; malformed or adversarial persisted timestamps far
/// apart degrade to "no overlap" rather than an unbounded loop.
const SOV_TZ_WINDOW_OVERLAP_MAX_SPAN_DAYS: i64 = 14;

/// The total overlap, in minutes, between a hub's concrete vulnerability
/// interval (`vulnerable_start_time..vulnerable_end_time`, UTC) and every
/// daily occurrence of a subscription's [`TzWindow`] on the days that
/// interval spans (spec: "the occurrences of `HH:MM-HH:MM` UTC on the days
/// the vulnerability interval spans"). Non-panicking throughout: every
/// date/time construction is `checked_*`/`Option`-returning, and an
/// out-of-order or implausibly wide interval (guarded by
/// [`SOV_TZ_WINDOW_OVERLAP_MAX_SPAN_DAYS`]) returns `0` rather than
/// looping unboundedly or overflowing.
pub fn tz_window_overlap_minutes(
    vulnerable_start: DateTime<Utc>,
    vulnerable_end: DateTime<Utc>,
    window: TzWindow,
) -> i64 {
    if vulnerable_end <= vulnerable_start {
        return 0;
    }
    let span_days = (vulnerable_end.date_naive() - vulnerable_start.date_naive()).num_days();
    if !(0..=SOV_TZ_WINDOW_OVERLAP_MAX_SPAN_DAYS).contains(&span_days) {
        return 0;
    }
    // Scanning starts one day before the interval's start date so a
    // window occurrence that began the day before (and crosses midnight
    // into the interval) is still considered.
    let Some(mut day) = vulnerable_start
        .date_naive()
        .checked_sub_days(chrono::Days::new(1))
    else {
        return 0;
    };
    let last_day = vulnerable_end.date_naive();
    let mut total = ChronoDuration::zero();
    loop {
        if day > last_day {
            break;
        }
        if let Some((window_start, window_end)) = window.occurrence_on(day) {
            let overlap_start = window_start.max(vulnerable_start);
            let overlap_end = window_end.min(vulnerable_end);
            if overlap_end > overlap_start {
                // Summing per-day intersections (rather than taking the
                // per-day max) is safe because an ESI sovereignty
                // vulnerability window is a single contiguous few-hour
                // interval that touches at most one daily window
                // occurrence in practice; combined with the 14-day span
                // cap above, it can never accumulate 60 minutes out of
                // many tiny cross-day slivers on a real hub. The synthetic
                // multi-day test pins this chosen rule.
                total += overlap_end - overlap_start;
            }
        }
        let Some(next_day) = day.checked_add_days(chrono::Days::new(1)) else {
            break;
        };
        day = next_day;
    }
    total.num_minutes()
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

    // --- SovAlertStage::Reachable / has_reachable_leaf / reachable_leaf_state (ticket 06) ---

    #[test]
    fn reachable_stage_key_and_footer_are_formatted_from_the_sequence() {
        assert_eq!(SovAlertStage::Reachable(1).as_str(), "reachable:1");
        assert_eq!(SovAlertStage::Reachable(42).as_str(), "reachable:42");
        // Distinct sequences produce distinct keys, so
        // `sov_alert_deliveries`'s unique constraint re-arms per
        // transition rather than deduping every reachable alert for a
        // campaign together.
        assert_ne!(
            SovAlertStage::Reachable(1).as_str(),
            SovAlertStage::Reachable(2).as_str()
        );
        assert_eq!(SovAlertStage::Reachable(1).footer_label(), "now reachable");
        assert_eq!(
            SovAlertStage::Reachable(7).footer_label(),
            "now reachable",
            "the footer never leaks the transition sequence to the user"
        );
    }

    #[test]
    fn has_reachable_leaf_finds_a_leaf_nested_under_and_or_not_and_is_false_without_one() {
        let none = SovFilterNode::Condition(SovFilterCondition::EventType(vec![
            "ihub_defense".to_string()
        ]));
        assert!(!none.has_reachable_leaf());

        let direct = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 5,
            allow_frigate_holes: false,
        });
        assert!(direct.has_reachable_leaf());

        let nested = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 12 }),
            SovFilterNode::Not(Box::new(SovFilterNode::Or(vec![SovFilterNode::Condition(
                SovFilterCondition::Reachable {
                    max_jumps: 5,
                    allow_frigate_holes: true,
                },
            )]))),
        ]);
        assert!(nested.has_reachable_leaf());
    }

    #[test]
    fn validate_rejects_a_reachable_leaf_directly_under_not() {
        let node = SovFilterNode::Not(Box::new(SovFilterNode::Condition(
            SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            },
        )));
        let error = node
            .validate()
            .expect_err("Not(Reachable) must be rejected");
        assert_eq!(error, "reachable cannot be negated");
    }

    #[test]
    fn validate_rejects_a_reachable_leaf_nested_under_not_at_any_depth() {
        // Nested inside And/Or under the Not.
        let nested = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 12 }),
            SovFilterNode::Not(Box::new(SovFilterNode::Or(vec![SovFilterNode::Condition(
                SovFilterCondition::Reachable {
                    max_jumps: 5,
                    allow_frigate_holes: true,
                },
            )]))),
        ]);
        assert!(nested.validate().is_err());

        // Double negation: still rejected -- "any Reachable leaf under a
        // Not, any depth" is the rule, regardless of whether an even
        // number of negations would cancel out semantically.
        let double_negated = SovFilterNode::Not(Box::new(SovFilterNode::Not(Box::new(
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
        ))));
        assert!(double_negated.validate().is_err());

        // A Not much further from the Reachable leaf than the leaf's own
        // direct parent still catches it.
        let deeply_nested = SovFilterNode::Not(Box::new(SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060])),
            SovFilterNode::Or(vec![
                SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                    "ihub_defense".to_string()
                ])),
                SovFilterNode::Condition(SovFilterCondition::Reachable {
                    max_jumps: 3,
                    allow_frigate_holes: false,
                }),
            ]),
        ])));
        assert!(deeply_nested.validate().is_err());
    }

    #[test]
    fn validate_still_accepts_not_over_a_non_reachable_leaf() {
        let node = SovFilterNode::Not(Box::new(SovFilterNode::Condition(
            SovFilterCondition::EventType(vec!["station_freeport".to_string()]),
        )));
        assert!(node.validate().is_ok());

        // A Reachable leaf elsewhere in the same tree, but not under this
        // Not, is still fine.
        let mixed = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
            SovFilterNode::Not(Box::new(SovFilterNode::Condition(
                SovFilterCondition::EventType(vec!["station_freeport".to_string()]),
            ))),
        ]);
        assert!(mixed.validate().is_ok());
    }

    #[test]
    fn reachable_leaf_state_is_none_for_a_subscription_with_no_reachable_leaf() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_000_001],
        });
        assert_eq!(node.reachable_leaf_state(&campaign, &|_, _| Some(2)), None);
    }

    #[test]
    fn reachable_leaf_state_reports_the_leafs_own_boolean_independent_of_other_leaves() {
        // A campaign whose Reachable leaf matches but whose Region leaf
        // does not: `reachable_leaf_state` still reports `true`, because
        // it follows the Reachable leaf alone -- gating the actual
        // `reachable` alert on the *full* filter is a separate concern
        // handled by the collector, not this function.
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let node = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            }),
            SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_099])),
        ]);
        let reachable_jumps = |_: i64, _: bool| Some(2);
        assert_eq!(
            node.reachable_leaf_state(&campaign, &reachable_jumps),
            Some(true)
        );
        // The full filter, by contrast, fails: Region never resolves
        // through the empty resolver used here.
        assert!(!node.matches(&campaign, observed_at, &|_| None, &reachable_jumps));
    }

    #[test]
    fn reachable_leaf_state_reflects_unreachable_and_out_of_range_max_jumps() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);

        let unreachable = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: 5,
            allow_frigate_holes: false,
        });
        assert_eq!(
            unreachable.reachable_leaf_state(&campaign, &|_, _| None),
            Some(false)
        );

        // Defensive guard, mirroring `matches`: an out-of-range max_jumps
        // bypassing validate() never reports "always reachable".
        let out_of_range = SovFilterNode::Condition(SovFilterCondition::Reachable {
            max_jumps: SOV_REACHABLE_MAX_JUMPS + 1,
            allow_frigate_holes: false,
        });
        assert_eq!(
            out_of_range.reachable_leaf_state(&campaign, &|_, _| Some(1)),
            Some(false)
        );
    }

    #[test]
    fn reachable_leaf_state_not_inverts_the_underlying_leaf() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let node = SovFilterNode::Not(Box::new(SovFilterNode::Condition(
            SovFilterCondition::Reachable {
                max_jumps: 5,
                allow_frigate_holes: false,
            },
        )));
        assert_eq!(
            node.reachable_leaf_state(&campaign, &|_, _| Some(2)),
            Some(false)
        );
        assert_eq!(
            node.reachable_leaf_state(&campaign, &|_, _| None),
            Some(true)
        );
    }

    // --- tz_window (ticket 07) ---

    fn eve(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn parse_tz_window_accepts_a_well_formed_window() {
        let window = parse_tz_window("00:00-04:00").expect("valid window");
        assert_eq!(window.start_minutes, 0);
        assert_eq!(window.end_minutes, 240);
        assert_eq!(window.display(), "00:00\u{2013}04:00");
    }

    #[test]
    fn parse_tz_window_accepts_a_window_crossing_midnight() {
        let window = parse_tz_window("22:00-02:00").expect("valid crossing window");
        assert_eq!(window.start_minutes, 22 * 60);
        assert_eq!(window.end_minutes, 2 * 60);
    }

    #[test]
    fn parse_tz_window_rejects_malformed_or_out_of_range_input() {
        for raw in [
            "not-a-window",
            "24:00-01:00",
            "01:00-24:00",
            "01:60-02:00",
            "01:00-02:60",
            "01:00",
            "",
            "01:00-01:00",
        ] {
            assert!(
                parse_tz_window(raw).is_err(),
                "expected '{raw}' to be rejected"
            );
        }
    }

    #[test]
    fn tz_window_overlap_minutes_is_zero_when_the_window_does_not_touch_the_interval() {
        let window = parse_tz_window("00:00-04:00").unwrap();
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 27, 6, 0), eve(2026, 8, 27, 8, 0), window);
        assert_eq!(overlap, 0);
    }

    #[test]
    fn tz_window_overlap_minutes_covers_the_whole_window_when_the_interval_contains_it() {
        let window = parse_tz_window("00:00-04:00").unwrap();
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 26, 22, 0), eve(2026, 8, 27, 6, 0), window);
        assert_eq!(overlap, 240);
    }

    #[test]
    fn tz_window_overlap_minutes_computes_a_partial_overlap_at_or_above_sixty_minutes() {
        let window = parse_tz_window("00:00-04:00").unwrap();
        // Interval 03:15-05:15 overlaps the window for 45 minutes (03:15-04:00).
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 27, 3, 15), eve(2026, 8, 27, 5, 15), window);
        assert_eq!(overlap, 45);
        assert!(overlap < SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES);

        // Interval 03:00-05:00 overlaps for exactly 60 minutes.
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 27, 3, 0), eve(2026, 8, 27, 5, 0), window);
        assert_eq!(overlap, 60);
        assert!(overlap >= SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES);
    }

    #[test]
    fn tz_window_overlap_minutes_handles_a_window_crossing_midnight() {
        let window = parse_tz_window("22:00-02:00").unwrap();
        // Interval spans 23:00 through 01:00: fully inside the crossing window.
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 27, 23, 0), eve(2026, 8, 28, 1, 0), window);
        assert_eq!(overlap, 120);
    }

    #[test]
    fn tz_window_overlap_minutes_sums_overlap_across_an_interval_spanning_two_days() {
        let window = parse_tz_window("00:00-04:00").unwrap();
        // Interval 2026-08-26 23:00 through 2026-08-27 02:00 crosses one
        // midnight and overlaps the window on both days: 1h on the 26th
        // (23:00-24:00 is outside 00:00-04:00, so actually 0) -- restated:
        // the interval overlaps only the 27th's occurrence (00:00-02:00 =
        // 120 minutes), since 23:00-24:00 on the 26th is not inside
        // 00:00-04:00 at all.
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 26, 23, 0), eve(2026, 8, 27, 2, 0), window);
        assert_eq!(overlap, 120);

        // An interval that truly spans two full occurrences: 2026-08-26
        // 02:00 through 2026-08-27 02:00 overlaps 00:00-04:00 on the 26th
        // (02:00-04:00 = 120m) and on the 27th (00:00-02:00 = 120m).
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 26, 2, 0), eve(2026, 8, 27, 2, 0), window);
        assert_eq!(overlap, 240);
    }

    #[test]
    fn tz_window_overlap_minutes_sums_small_daily_slivers_across_a_multi_day_interval() {
        // Pins the chosen rule (sum per-day intersections, NOT per-day max)
        // for a synthetic interval whose daily overlaps are individually
        // below the 60-minute threshold but sum to exactly it. Window
        // 00:00-04:00; interval 2026-08-26 03:30 -> 2026-08-27 00:30:
        //   - the 26th's occurrence overlaps 03:30-04:00 = 30 minutes
        //   - the 27th's occurrence overlaps 00:00-00:30 = 30 minutes
        // Summing yields 60 (>= threshold); a per-day max would yield 30.
        let window = parse_tz_window("00:00-04:00").unwrap();
        let overlap =
            tz_window_overlap_minutes(eve(2026, 8, 26, 3, 30), eve(2026, 8, 27, 0, 30), window);
        assert_eq!(overlap, 60);
        assert!(overlap >= SOV_TZ_WINDOW_MIN_OVERLAP_MINUTES);
    }

    #[test]
    fn tz_window_overlap_minutes_never_panics_on_an_out_of_order_or_implausibly_wide_interval() {
        let window = parse_tz_window("00:00-04:00").unwrap();
        // end before start
        assert_eq!(
            tz_window_overlap_minutes(eve(2026, 8, 27, 4, 0), eve(2026, 8, 27, 0, 0), window),
            0
        );
        // implausibly wide (beyond the safety bound)
        assert_eq!(
            tz_window_overlap_minutes(eve(2020, 1, 1, 0, 0), eve(2030, 1, 1, 0, 0), window),
            0
        );
    }

    #[test]
    fn effective_tz_window_defaults_when_absent_and_parses_when_present() {
        assert_eq!(
            effective_tz_window(&serde_json::json!({})),
            Some(parse_tz_window(SOV_DEFAULT_TZ_WINDOW).unwrap())
        );
        assert_eq!(
            effective_tz_window(&serde_json::json!({"tz_window": "06:00-10:00"})),
            Some(parse_tz_window("06:00-10:00").unwrap())
        );
        assert_eq!(
            effective_tz_window(&serde_json::json!({"tz_window": "not-a-window"})),
            None
        );
    }

    #[test]
    fn effective_tz_shift_enabled_defaults_to_false() {
        assert!(!effective_tz_shift_enabled(&serde_json::json!({})));
        assert!(!effective_tz_shift_enabled(
            &serde_json::json!({"tz_shift_enabled": "not-a-bool"})
        ));
        assert!(effective_tz_shift_enabled(
            &serde_json::json!({"tz_shift_enabled": true})
        ));
    }

    #[test]
    fn tz_window_entered_stage_key_and_footer_are_formatted_from_the_sequence() {
        assert_eq!(
            SovAlertStage::TzWindowEntered(1).as_str(),
            "tz_window_entered:1"
        );
        assert_ne!(
            SovAlertStage::TzWindowEntered(1).as_str(),
            SovAlertStage::TzWindowEntered(2).as_str()
        );
        assert_eq!(
            SovAlertStage::TzWindowEntered(1).footer_label(),
            "vuln window entered"
        );
    }

    #[test]
    fn matches_structure_ignores_reachable_vulnerable_within_and_event_type() {
        let node = SovFilterNode::And(vec![
            SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 6 }),
            SovFilterNode::Condition(SovFilterCondition::Reachable {
                max_jumps: 1,
                allow_frigate_holes: false,
            }),
            SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                "station_freeport".to_string()
            ])),
        ]);
        assert!(node.matches_structure(Some(99_000_001), 30_000_001, &|_| None));
    }

    #[test]
    fn matches_structure_applies_defender_region_and_system() {
        let mut regions = std::collections::HashMap::new();
        regions.insert(30_000_001_i64, 10_000_060_i64);
        let region_of = region_of(&regions);

        let defender = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_000_001],
        });
        assert!(defender.matches_structure(Some(99_000_001), 30_000_001, &|_| None));
        assert!(!defender.matches_structure(Some(99_999_999), 30_000_001, &|_| None));
        assert!(!defender.matches_structure(None, 30_000_001, &|_| None));

        let region = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060]));
        assert!(region.matches_structure(None, 30_000_001, &region_of));
        assert!(!region.matches_structure(None, 30_000_002, &region_of));

        let system = SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_001]));
        assert!(system.matches_structure(None, 30_000_001, &|_| None));
        assert!(!system.matches_structure(None, 30_000_002, &|_| None));
    }

    #[test]
    fn matches_structure_composes_with_and_or_not() {
        let node = SovFilterNode::Not(Box::new(SovFilterNode::Condition(
            SovFilterCondition::System(vec![30_000_099]),
        )));
        assert!(node.matches_structure(None, 30_000_001, &|_| None));

        let node = SovFilterNode::Or(vec![
            SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_099])),
            SovFilterNode::Condition(SovFilterCondition::Defender {
                alliance_ids: vec![99_000_001],
            }),
        ]);
        assert!(node.matches_structure(Some(99_000_001), 30_000_001, &|_| None));
    }
}
