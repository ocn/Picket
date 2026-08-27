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
/// announced (spec "Alert stages and dedup"). Only `Appeared` is
/// implemented by this ticket; later tickets add T-minus and reachability
/// stages here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SovAlertStage {
    Appeared,
}

impl SovAlertStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Appeared => "appeared",
        }
    }

    pub fn footer_label(self) -> &'static str {
        match self {
            Self::Appeared => "Appeared",
        }
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
    /// `Region`. Passing a resolver instead of a precomputed map keeps this
    /// pure and unit-testable while letting production supply a live
    /// systems-cache lookup.
    pub fn matches(
        &self,
        campaign: &SovCampaign,
        observed_at: DateTime<Utc>,
        region_of: &dyn Fn(i64) -> Option<i64>,
    ) -> bool {
        match self {
            Self::Condition(condition) => condition.matches(campaign, observed_at, region_of),
            Self::And(nodes) => nodes
                .iter()
                .all(|node| node.matches(campaign, observed_at, region_of)),
            Self::Or(nodes) => nodes
                .iter()
                .any(|node| node.matches(campaign, observed_at, region_of)),
            Self::Not(node) => !node.matches(campaign, observed_at, region_of),
        }
    }
}

/// Leaves of the sov filter grammar. Only the leaves this ticket needs are
/// implemented; `Reachable` (ticket 04) and the watchlist-backed
/// `Defender` variant (spec "sov feed's defender filter... reference the
/// watchlist") are deliberately out of scope here.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SovFilterCondition {
    VulnerableWithin { hours: i64 },
    Defender { alliance_ids: Vec<i64> },
    Region(Vec<i64>),
    System(Vec<i64>),
    EventType(Vec<String>),
}

impl SovFilterCondition {
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::VulnerableWithin { hours } => {
                if *hours <= 0 {
                    Err("vulnerable_within hours must be positive".to_string())
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
        }
    }

    fn matches(
        &self,
        campaign: &SovCampaign,
        observed_at: DateTime<Utc>,
        region_of: &dyn Fn(i64) -> Option<i64>,
    ) -> bool {
        match self {
            Self::VulnerableWithin { hours } => {
                campaign.start_time <= observed_at + ChronoDuration::hours(*hours)
            }
            Self::Defender { alliance_ids } => campaign
                .defender_id
                .is_some_and(|id| alliance_ids.contains(&id)),
            Self::Region(ids) => region_of(campaign.solar_system_id)
                .is_some_and(|region_id| ids.contains(&region_id)),
            Self::System(ids) => ids.contains(&campaign.solar_system_id),
            Self::EventType(kinds) => kinds.iter().any(|kind| kind == &campaign.event_type),
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
        assert!(node.matches(&campaign, observed_at, &|_| None));

        let node = SovFilterNode::Condition(SovFilterCondition::VulnerableWithin { hours: 4 });
        assert!(!node.matches(&campaign, observed_at, &|_| None));
    }

    #[test]
    fn defender_matches_only_configured_alliance_ids() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_000_001],
        });
        assert!(node.matches(&campaign, observed_at, &|_| None));

        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_999_999],
        });
        assert!(!node.matches(&campaign, observed_at, &|_| None));

        let no_defender_campaign =
            make_campaign(1, "station_freeport", 30_000_001, None, observed_at);
        let node = SovFilterNode::Condition(SovFilterCondition::Defender {
            alliance_ids: vec![99_000_001],
        });
        assert!(!node.matches(&no_defender_campaign, observed_at, &|_| None));
    }

    #[test]
    fn region_matches_through_the_supplied_resolver() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        let mut map = HashMap::new();
        map.insert(30_000_001, 10_000_060);
        let node = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060]));
        assert!(node.matches(&campaign, observed_at, &region_of(&map)));

        let node = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_061]));
        assert!(!node.matches(&campaign, observed_at, &region_of(&map)));

        // An unresolvable system never matches Region.
        let node = SovFilterNode::Condition(SovFilterCondition::Region(vec![10_000_060]));
        assert!(!node.matches(&campaign, observed_at, &|_| None));
    }

    #[test]
    fn system_and_event_type_match_directly_on_campaign_facts() {
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();
        let campaign = make_campaign(1, "ihub_defense", 30_000_001, Some(99_000_001), observed_at);
        assert!(
            SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_001])).matches(
                &campaign,
                observed_at,
                &|_| None
            )
        );
        assert!(
            !SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_002])).matches(
                &campaign,
                observed_at,
                &|_| None
            )
        );
        assert!(SovFilterNode::Condition(SovFilterCondition::EventType(vec![
            "ihub_defense".to_string()
        ]))
        .matches(&campaign, observed_at, &|_| None));
        assert!(
            !SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                "tcu_defense".to_string()
            ]))
            .matches(&campaign, observed_at, &|_| None)
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
        assert!(and_node.matches(&campaign, observed_at, &|_| None));

        let or_node = SovFilterNode::Or(vec![
            SovFilterNode::Condition(SovFilterCondition::System(vec![30_000_099])),
            SovFilterNode::Condition(SovFilterCondition::EventType(vec![
                "ihub_defense".to_string()
            ])),
        ]);
        assert!(or_node.matches(&campaign, observed_at, &|_| None));

        let not_node = SovFilterNode::Not(Box::new(SovFilterNode::Condition(
            SovFilterCondition::EventType(vec!["tcu_defense".to_string()]),
        )));
        assert!(not_node.matches(&campaign, observed_at, &|_| None));
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
}
