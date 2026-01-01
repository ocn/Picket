use crate::config::{
    AppState, Filter, FilterNode, SimpleFilter, Subscription, System, SystemRange,
    TargetableCondition,
};
use crate::discord_bot::{get_name, get_ship_group_id, get_system};
use crate::models::{Attacker, ZkData};
use chrono::Timelike;
use futures::future::{BoxFuture, FutureExt};
use serenity::model::prelude::GuildId;
use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NamedFilterResult {
    pub name: String,
    pub filter_result: FilterResult,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FilterResult {
    pub matched_attackers: HashSet<AttackerKey>,
    pub matched_victim: bool,
    pub min_pilots: Option<u32>,
    pub light_year_range: Option<SystemRange>,
}

impl FilterResult {
    pub fn match_all(attackers: Vec<&Attacker>) -> Self {
        let matched_attackers = attackers
            .iter()
            .map(|attacker| AttackerKey::new(attacker))
            .collect();
        FilterResult {
            matched_attackers,
            matched_victim: true,
            ..Default::default()
        }
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum Color {
    Green,
    #[default]
    Red,
}

// A composite key (string) for an entity, used to uniquely identify them across different killmails.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttackerKey(String);

impl AttackerKey {
    /// Creates a stable, unique composite key for an attacker on a killmail.
    /// This handles cases where an attacker might not have a character ID (e.g., structures).
    pub fn new(attacker: &Attacker) -> Self {
        // Using a Vec to build the key handles missing parts gracefully.
        let mut key_parts = Vec::new();

        // We add available IDs in a specific order to ensure consistency.
        if let Some(id) = attacker.ship_type_id {
            key_parts.push(format!("s{}", id)); // 's' for ship
        }
        if let Some(id) = attacker.weapon_type_id {
            key_parts.push(format!("w{}", id)); // 'w' for a weapon
        }
        if let Some(id) = attacker.character_id {
            key_parts.push(format!("c{}", id)); // 'c' for character
        }
        if let Some(id) = attacker.corporation_id {
            key_parts.push(format!("o{}", id)); // 'o' for corp
        }
        if let Some(id) = attacker.alliance_id {
            key_parts.push(format!("a{}", id)); // 'a' for alliance
        }
        if let Some(id) = attacker.faction_id {
            key_parts.push(format!("f{}", id)); // 'f' for faction
        }

        AttackerKey(key_parts.join(":"))
    }
}

impl std::fmt::Display for AttackerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

fn parse_security_range(s: &str) -> Result<RangeInclusive<f64>, ()> {
    let parts: Vec<&str> = s.split("..=").collect();
    if parts.len() != 2 {
        return Err(());
    }
    let start = f64::from_str(parts[0]).map_err(|_| ())?;
    let end = f64::from_str(parts[1]).map_err(|_| ())?;
    Ok(start..=end)
}

fn distance(system1: &System, system2: &System) -> f64 {
    const LY_PER_M: f64 = 1.0 / 9_460_730_472_580_800.0;
    let dx = system1.x - system2.x;
    let dy = system1.y - system2.y;
    let dz = system1.z - system2.z;
    (dx * dx + dy * dy + dz * dz).sqrt() * LY_PER_M
}

pub async fn process_killmail(
    app_state: &Arc<AppState>,
    zk_data: &ZkData,
) -> Vec<(GuildId, Subscription, NamedFilterResult)> {
    // Clone the subscriptions to release the lock before the async loop.
    let all_subscriptions = {
        let subs_guard = app_state.subscriptions.read().unwrap();
        subs_guard.clone()
    };

    let mut matched_subscriptions = Vec::new();

    for (guild_id, subscriptions) in all_subscriptions.iter() {
        for subscription in subscriptions {
            let (match_tree, veto_tree) = partition_filters(&subscription.root_filter);

            // Step 1: Evaluate the primary match conditions.
            let Some(match_tree) = match_tree else {
                continue;
            };
            let Some(primary_matches) = evaluate_filter_node(&match_tree, zk_data, app_state).await
            else {
                tracing::trace!(
                    "[Kill: {}] No matches for subscription '{}' in channel '{}'",
                    zk_data.killmail.killmail_id,
                    subscription.id,
                    subscription.action.channel_id
                );
                continue;
            };

            tracing::trace!(
                "[Kill: {}] Matches for subscription '{}' in channel '{}': {:#?}",
                zk_data.killmail.killmail_id,
                subscription.action.channel_id,
                subscription.id,
                primary_matches.filter_result,
            );

            // Step 2: Evaluate the veto conditions.
            let veto_attackers = if let Some(veto_tree) = veto_tree {
                evaluate_filter_node(&veto_tree, zk_data, app_state)
                    .await
                    .map_or(HashSet::new(), |r| r.filter_result.matched_attackers)
            } else {
                HashSet::new()
            };

            // Step 3: The difference of the matches and vetoes will show non-vetoed matched attackers.
            let final_attackers: HashSet<_> = primary_matches
                .filter_result
                .matched_attackers
                .difference(&veto_attackers)
                .collect();

            let final_victim_match = primary_matches.filter_result.matched_victim;

            if !final_attackers.is_empty() || final_victim_match {
                // Create a new, clean FilterResult with only the surviving entities.
                let final_filter_result = FilterResult {
                    // Use the collected HashSet of references to create a new owned HashSet
                    matched_attackers: final_attackers.into_iter().cloned().collect(),
                    matched_victim: final_victim_match,
                    // Carry over the other metadata from the primary match for display purposes.
                    min_pilots: primary_matches.filter_result.min_pilots,
                    light_year_range: primary_matches.filter_result.light_year_range,
                };

                // Wrap it in a NamedFilterResult
                let final_named_result = NamedFilterResult {
                    name: primary_matches.name,
                    filter_result: final_filter_result,
                };

                // Push the CORRECT, final result.
                matched_subscriptions.push((*guild_id, subscription.clone(), final_named_result));
            }
        }
    }

    matched_subscriptions
}

/// Recursively partitions a filter tree into two separate trees:
/// one for "match" conditions and one for "veto" (IgnoreHighStanding) conditions.
fn partition_filters(node: &FilterNode) -> (Option<FilterNode>, Option<FilterNode>) {
    match node {
        FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding { .. })) => {
            // This is a veto condition.
            (None, Some(node.clone()))
        }
        FilterNode::Condition(_) => {
            // This is a match condition.
            (Some(node.clone()), None)
        }
        FilterNode::And(nodes) => {
            let (mut match_nodes, mut veto_nodes) = (Vec::new(), Vec::new());
            for n in nodes {
                let (m, v) = partition_filters(n);
                if let Some(match_node) = m {
                    match_nodes.push(match_node);
                }
                if let Some(veto_node) = v {
                    veto_nodes.push(veto_node);
                }
            }
            let match_tree = if match_nodes.is_empty() {
                None
            } else if match_nodes.len() == 1 {
                match_nodes.pop()
            } else {
                Some(FilterNode::And(match_nodes))
            };
            let veto_tree = if veto_nodes.is_empty() {
                None
            } else if veto_nodes.len() == 1 {
                veto_nodes.pop()
            } else {
                Some(FilterNode::And(veto_nodes))
            };
            (match_tree, veto_tree)
        }
        FilterNode::Or(nodes) => {
            let (mut match_nodes, mut veto_nodes) = (Vec::new(), Vec::new());
            for n in nodes {
                let (m, v) = partition_filters(n);
                if let Some(match_node) = m {
                    match_nodes.push(match_node);
                }
                if let Some(veto_node) = v {
                    veto_nodes.push(veto_node);
                }
            }
            let match_tree = if match_nodes.is_empty() {
                None
            } else if match_nodes.len() == 1 {
                match_nodes.pop()
            } else {
                Some(FilterNode::Or(match_nodes))
            };
            let veto_tree = if veto_nodes.is_empty() {
                None
            } else if veto_nodes.len() == 1 {
                veto_nodes.pop()
            } else {
                Some(FilterNode::Or(veto_nodes))
            };
            (match_tree, veto_tree)
        }
        FilterNode::Not(inner_node) => {
            // A NOT node is treated as a match condition, as it's a logical inversion, not a veto.
            let (match_node, veto_node) = partition_filters(inner_node);
            if veto_node.is_some() {
                // This is a complex case: NOT(veto). We'll treat it as a global match for now.
                (Some(FilterNode::Not(inner_node.clone())), None)
            } else {
                (
                    Some(FilterNode::Not(Box::from(
                        match_node.unwrap_or(FilterNode::And(vec![])),
                    ))),
                    None,
                )
            }
        }
    }
}

fn evaluate_filter_node<'a>(
    node: &'a FilterNode,
    zk_data: &'a ZkData,
    app_state: &'a Arc<AppState>,
) -> BoxFuture<'a, Option<NamedFilterResult>> {
    async move {
        let kill_id = zk_data.killmail.killmail_id;
        match node {
            FilterNode::Condition(filter) => evaluate_filter(filter, zk_data, app_state).await,
            FilterNode::And(nodes) => {
                let mut results = Vec::new();
                for n in nodes {
                    if let Some(result) = evaluate_filter_node(n, zk_data, app_state).await {
                        tracing::trace!(
                            "[Kill: {}] Filter condition passed for node: {}",
                            kill_id,
                            n.name()
                        );
                        results.push(result);
                    } else {
                        tracing::trace!(
                            "[Kill: {}] Filter condition failed for node: {}",
                            kill_id,
                            n.name()
                        );
                        return None; // One failure means the whole And block fails
                    }
                }
                tracing::trace!("evaluate_filter_node results: {:#?}", results);
                // Merge results
                let final_filter_result = FilterResult {
                    min_pilots: results.iter().find_map(|r| r.filter_result.min_pilots),
                    light_year_range: results
                        .iter()
                        .find(|r| r.filter_result.light_year_range.is_some())
                        .and_then(|r| r.filter_result.light_year_range.clone()),
                    // For matched_attackers: start with first non-empty set, then intersect
                    // with other non-empty sets. Empty sets mean "don't care" (global filters).
                    matched_attackers: {
                        let init = results
                            .iter()
                            .find(|r| !r.filter_result.matched_attackers.is_empty())
                            .cloned()
                            .unwrap_or_default()
                            .filter_result
                            .matched_attackers;
                        results.iter().fold(init, |acc, b| {
                            if b.filter_result.matched_attackers.is_empty() {
                                acc // Skip empty sets (global filters that don't track attackers)
                            } else {
                                acc.intersection(&b.filter_result.matched_attackers)
                                    .cloned()
                                    .collect()
                            }
                        })
                    },
                    // All filters must agree on victim match
                    matched_victim: results.iter().all(|b| b.filter_result.matched_victim),
                };

                tracing::trace!("final: {:#?}", final_filter_result);
                if final_filter_result.matched_victim
                    || !final_filter_result.matched_attackers.is_empty()
                {
                    Some(NamedFilterResult {
                        name: results
                            .iter()
                            .map(|r| r.name.as_str())
                            .collect::<Vec<_>>()
                            .join(" + "),
                        filter_result: final_filter_result,
                    })
                } else {
                    None
                }
            }
            FilterNode::Or(nodes) => {
                let mut final_res: Option<FilterResult> = None;
                for n in nodes {
                    if let Some(result) = evaluate_filter_node(n, zk_data, app_state).await {
                        if let Some(current_res) = &mut final_res {
                            current_res
                                .matched_attackers
                                .extend(result.filter_result.matched_attackers);
                            current_res.matched_victim |= result.filter_result.matched_victim;
                        } else {
                            final_res = Some(result.filter_result);
                        }
                    }
                }
                final_res.map(|fr| NamedFilterResult {
                    name: node.name(),
                    filter_result: fr,
                })
            }
            FilterNode::Not(node) => {
                if evaluate_filter_node(node, zk_data, app_state)
                    .await
                    .is_some()
                {
                    None
                } else {
                    let all_attackers = zk_data
                        .killmail
                        .attackers
                        .iter()
                        .map(AttackerKey::new)
                        .collect();
                    Some(NamedFilterResult {
                        name: format!("Not({})", node.name()),
                        filter_result: FilterResult {
                            matched_attackers: all_attackers,
                            matched_victim: true,
                            ..Default::default()
                        },
                    })
                }
            }
        }
    }
    .boxed()
}

async fn evaluate_filter(
    filter: &Filter,
    zk_data: &ZkData,
    app_state: &Arc<AppState>,
) -> Option<NamedFilterResult> {
    let killmail = &zk_data.killmail;

    let filter_result = match filter {
        Filter::Simple(sf) => {
            let mut res = match sf {
                SimpleFilter::TotalValue { min, max } => {
                    let total_value = zk_data.zkb.total_value;
                    if min.is_none_or(|m| total_value >= m as f64)
                        && max.is_none_or(|m| total_value <= m as f64)
                    {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::DroppedValue { min, max } => {
                    let dropped_value = zk_data.zkb.dropped_value;
                    if min.is_none_or(|m| dropped_value >= m as f64)
                        && max.is_none_or(|m| dropped_value <= m as f64)
                    {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::Region(region_ids) => {
                    if get_system(app_state, killmail.solar_system_id)
                        .await
                        .is_some_and(|s| region_ids.contains(&s.region_id))
                    {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::System(system_ids) => {
                    if system_ids.contains(&killmail.solar_system_id) {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::Security(range_str) => {
                    if let (Some(system), Ok(range)) = (
                        get_system(app_state, killmail.solar_system_id).await,
                        parse_security_range(range_str),
                    ) {
                        let rounded_sec = (system.security_status * 10.0).round() / 10.0;
                        if range.contains(&rounded_sec) {
                            Some(Default::default())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                SimpleFilter::LyRangeFrom(system_ranges) => {
                    if let Some(killmail_system) =
                        get_system(app_state, killmail.solar_system_id).await
                    {
                        let mut matched_ranges: Vec<SystemRange> = vec![];
                        for system_range in system_ranges {
                            if let Some(target_system) =
                                get_system(app_state, system_range.system_id).await
                            {
                                let distance = distance(&killmail_system, &target_system);
                                if distance <= system_range.range {
                                    matched_ranges.push(SystemRange {
                                        system_id: target_system.id,
                                        range: distance,
                                    })
                                }
                                tracing::trace!(
                                    "[Kill: {}] Distance between {} and {}: {} ly",
                                    killmail.killmail_id,
                                    killmail_system.name,
                                    target_system.name,
                                    distance,
                                );
                            } else {
                                warn!(
                                    "Could not find target system {} for LY range check",
                                    system_range.system_id
                                );
                            }
                        }
                        if matched_ranges.is_empty() {
                            None
                        } else {
                            // Sort descending, pop from the back for the shortest range match
                            matched_ranges.sort_by(|a, b| b.range.total_cmp(&a.range));

                            Some(FilterResult {
                                light_year_range: Some(
                                    matched_ranges.pop().expect("non-empty matched vec"),
                                ),
                                ..Default::default()
                            })
                        }
                    } else {
                        warn!(
                            "Could not find killmail system {} for LY range check",
                            killmail.solar_system_id
                        );
                        None
                    }
                }
                SimpleFilter::IsNpc(is_npc) => {
                    if zk_data.zkb.npc == *is_npc {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::IsSolo(is_solo) => {
                    if zk_data.zkb.solo == *is_solo {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::Pilots { min, max } => {
                    let num_pilots = (killmail.attackers.len() + 1) as u32;
                    if min.is_none_or(|m| num_pilots >= m) && max.is_none_or(|m| num_pilots <= m) {
                        Some(FilterResult {
                            min_pilots: Some(min.unwrap_or(0)),
                            ..Default::default()
                        })
                    } else {
                        None
                    }
                }
                SimpleFilter::TimeRange { start, end } => {
                    let res = if let Ok(killmail_time) =
                        chrono::DateTime::parse_from_rfc3339(&killmail.killmail_time)
                    {
                        let killmail_hour = killmail_time.hour();
                        if start <= end {
                            // Simple range within the same day
                            killmail_hour >= *start && killmail_hour <= *end
                        } else {
                            // Range extends across midnight (e.g., 22:00 to 04:00)
                            killmail_hour >= *start || killmail_hour <= *end
                        }
                    } else {
                        warn!("Failed to parse killmail_time: {}", killmail.killmail_time);
                        false
                    };
                    if res {
                        Some(Default::default())
                    } else {
                        None
                    }
                }
                SimpleFilter::IgnoreHighStanding {
                    synched_by_user_id,
                    source,
                    source_entity_id,
                } => {
                    let mut result: FilterResult = Default::default();
                    let standings_map = app_state.user_standings.read().unwrap();
                    if let Some(user_standings) =
                        standings_map.get(&serenity::model::id::UserId(*synched_by_user_id))
                    {
                        let mut implicit_blues: Vec<u64> = vec![*source_entity_id];
                        let context_token = user_standings.tokens.iter().find(|t| match source {
                            crate::config::StandingSource::Character => {
                                t.character_id == *source_entity_id
                            }
                            crate::config::StandingSource::Corporation => {
                                t.corporation_id == *source_entity_id
                            }
                            crate::config::StandingSource::Alliance => {
                                t.alliance_id == Some(*source_entity_id)
                            }
                        });

                        if let Some(token) = context_token {
                            if *source == crate::config::StandingSource::Character {
                                implicit_blues.push(token.corporation_id);
                                if let Some(id) = token.alliance_id {
                                    implicit_blues.push(id);
                                }
                            }
                            if *source == crate::config::StandingSource::Corporation {
                                if let Some(id) = token.alliance_id {
                                    implicit_blues.push(id);
                                }
                            }
                        }

                        let contacts = user_standings.contact_lists.contacts.get(source_entity_id);
                        for attacker in &killmail.attackers {
                            let attacker_ids = [
                                attacker.character_id,
                                attacker.corporation_id,
                                attacker.alliance_id,
                            ];
                            let is_blue = attacker_ids.iter().flatten().any(|id| {
                                implicit_blues.contains(id)
                                    || contacts.is_some_and(|cl| {
                                        cl.iter().any(|c| c.contact_id == *id && c.standing >= 5.0)
                                    })
                            });

                            if is_blue {
                                // Generate the composite key for the blue attacker and insert it.
                                let key = AttackerKey::new(attacker);
                                result.matched_attackers.insert(key);
                            }
                        }
                    }
                    // This filter's purpose is to return the set of vetoed attackers.
                    // It always returns a result, which might be an empty set if no blues were found.
                    return Some(NamedFilterResult {
                        name: filter.name(),
                        filter_result: result,
                    });
                }
            };
            if let Some(res) = &mut res {
                // TODO: Handle this better? For all veto filters?
                if !matches!(sf, SimpleFilter::IgnoreHighStanding { .. }) {
                    res.matched_victim = true;
                }
            }
            res
        }
        Filter::Targeted(tf) => {
            let mut result = FilterResult::default();

            let condition_checker = async |attacker: &Attacker| -> bool {
                match &tf.condition {
                    TargetableCondition::Alliance(ids) => {
                        attacker.alliance_id.is_some_and(|id| ids.contains(&id))
                    }
                    TargetableCondition::Corporation(ids) => {
                        attacker.corporation_id.is_some_and(|id| ids.contains(&id))
                    }
                    TargetableCondition::Character(ids) => {
                        attacker.character_id.is_some_and(|id| ids.contains(&id))
                    }
                    TargetableCondition::ShipType(ids) => {
                        attacker.ship_type_id.is_some_and(|id| ids.contains(&id))
                            || attacker.weapon_type_id.is_some_and(|id| ids.contains(&id))
                    }
                    TargetableCondition::ShipGroup(ids) => {
                        let ship_match = if let Some(id) = attacker.ship_type_id {
                            get_ship_group_id(app_state, id)
                                .await
                                .is_some_and(|gid| ids.contains(&gid))
                        } else {
                            false
                        };
                        let weapon_match = if let Some(id) = attacker.weapon_type_id {
                            get_ship_group_id(app_state, id)
                                .await
                                .is_some_and(|gid| ids.contains(&gid))
                        } else {
                            false
                        };
                        ship_match || weapon_match
                    }
                    TargetableCondition::NameFragment(s) => {
                        if let Some(id) = attacker.ship_type_id {
                            get_name(app_state, id as u64)
                                .await
                                .is_some_and(|n| n.to_lowercase().contains(&s.to_lowercase()))
                        } else if let Some(id) = attacker.weapon_type_id {
                            get_name(app_state, id as u64)
                                .await
                                .is_some_and(|n| n.to_lowercase().contains(&s.to_lowercase()))
                        } else {
                            false
                        }
                    }
                }
            };

            if tf.target.is_victim() {
                // This is a bit of a trick: we create a temporary Attacker struct from the Victim to reuse the checker.
                let victim_as_attacker = Attacker {
                    character_id: killmail.victim.character_id,
                    corporation_id: killmail.victim.corporation_id,
                    alliance_id: killmail.victim.alliance_id,
                    ship_type_id: Some(killmail.victim.ship_type_id),
                    weapon_type_id: None, // Victims don't have a "weapon" in this context
                    faction_id: killmail.victim.faction_id,
                    final_blow: false,
                    damage_done: 0,
                    security_status: 0.0,
                };
                if condition_checker(&victim_as_attacker).await {
                    result.matched_victim = true;
                }
            };

            if tf.target.is_attacker() {
                for attacker in &killmail.attackers {
                    if condition_checker(attacker).await {
                        let _ = result.matched_attackers.insert(AttackerKey::new(attacker));
                    }
                }
            };

            if result.matched_victim || !result.matched_attackers.is_empty() {
                Some(result)
            } else {
                None
            }
        }
    };

    filter_result.map(|filter_result| NamedFilterResult {
        name: filter.name(),
        filter_result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, Filter, FilterNode, System, SystemRange};
    use crate::config::{EveAuthToken, StandingSource, UserStandings};
    use crate::config::{Target, TargetedFilter};
    use crate::models::{Attacker, KillmailData, Position, Victim, ZkData, Zkb};
    use moka::future::Cache;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use tokio::sync::Mutex;

    // Helper to create a mock AppState
    fn mock_app_state() -> Arc<AppState> {
        let mut systems = HashMap::new();
        systems.insert(
            30000142, // Jita
            System {
                id: 30000142,
                name: "Jita".to_string(),
                region_id: 10000002, // The Forge
                region: "The Forge".to_string(),
                security_status: 0.9,
                x: -993254832640.0,
                y: 216484356096.0,
                z: -973193297920.0,
            },
        );
        systems.insert(
            31002222, // Amarr
            System {
                id: 31002222,
                name: "Amarr".to_string(),
                region_id: 10000043, // Domain
                region: "Domain".to_string(),
                security_status: 1.0,
                x: 1.0,
                y: 1.0,
                z: 1.0,
            },
        );

        let mut ships = HashMap::new();
        ships.insert(671, 27); // Catalyst, GroupID 27 (Destroyer)
        ships.insert(587, 25); // Rifter, GroupID 25 (Frigate)
        ships.insert(17738, 419); // Golem, GroupID 419 (Marauder)

        let mut names = HashMap::new();
        names.insert(671, "Catalyst".to_string());
        names.insert(587, "Rifter".to_string());
        names.insert(17738, "Golem".to_string());

        Arc::new(AppState {
            subscriptions: Default::default(),
            systems: Arc::new(RwLock::new(systems)),
            ships: Arc::new(RwLock::new(ships)),
            names: Arc::new(RwLock::new(names)),
            celestial_cache: Cache::new(10_000),
            esi_client: Default::default(),
            systems_file_lock: Mutex::new(()),
            ships_file_lock: Mutex::new(()),
            names_file_lock: Mutex::new(()),
            subscriptions_file_lock: Mutex::new(()),
            app_config: Arc::new(AppConfig {
                discord_bot_token: "".to_string(),
                discord_client_id: 0,
                eve_client_id: "".to_string(),
                eve_client_secret: "".to_string(),
            }),
            last_ping_times: Mutex::new(HashMap::new()),
            user_standings: Arc::new(Default::default()),
            user_standings_file_lock: Default::default(),
            sso_states: Arc::new(Default::default()),
        })
    }

    // Helper to create a default ZkData for testing
    fn default_zk_data() -> ZkData {
        ZkData {
            kill_id: 1,
            killmail: KillmailData {
                killmail_id: 1,
                killmail_time: "2025-07-08T12:00:00Z".to_string(),
                solar_system_id: 30000142, // Jita
                victim: Victim {
                    damage_taken: 1000,
                    ship_type_id: 587, // Rifter
                    character_id: Some(1),
                    corporation_id: Some(101),
                    alliance_id: Some(1001),
                    position: Some(Position {
                        x: 0.0,
                        y: 0.0,
                        z: 0.0,
                    }),
                    faction_id: None,
                    items: vec![],
                },
                attackers: vec![Attacker {
                    final_blow: true,
                    damage_done: 1000,
                    ship_type_id: Some(671), // Catalyst
                    character_id: Some(2),
                    corporation_id: Some(102),
                    alliance_id: Some(1002),
                    weapon_type_id: Some(3),
                    security_status: 0.5,
                    faction_id: None,
                }],
            },
            zkb: Zkb {
                total_value: 10_000_000.0,
                dropped_value: 1_000_000.0,
                npc: false,
                solo: false,
                location_id: None,
                hash: "".to_string(),
                fitted_value: 0.0,
                destroyed_value: 0.0,
                points: 0,
                awox: false,
                labels: vec![],
                esi: "".to_string(),
            },
        }
    }

    fn user_killmail_data() -> ZkData {
        let json_data = r#"
         {
             "killID": 128389930,
             "zkb": {
               "locationID": 40161548,
               "hash": "d00ad190e832f0ca2965c9946b15527c415a70e7",
               "fittedValue": 5148356869.79,
               "droppedValue": 515470667.87,
               "destroyedValue": 4722688524.39,
               "totalValue": 5238159192.26,
               "points": 1,
               "npc": false,
               "solo": false,
               "awox": false,
               "href": ""
             },
             "killmail": {
               "attackers": [],
               "killmail_id": 128389930,
               "killmail_time": "2025-07-06T23:32:26Z",
               "solar_system_id": 30002539,
               "victim": {
                 "alliance_id": 99009845,
                 "character_id": 2114058087,
                 "corporation_id": 98498670,
                 "damage_taken": 856144,
                 "items": [],
                 "position": {
                   "x": -30420382830.688633,
                   "y": 2662073916.025609,
                   "z": 309569446754.9493
                 },
                 "ship_type_id": 19720
               }
             }
         }"#;
        serde_json::from_str(json_data).expect("Failed to parse ZkData from JSON")
    }

    #[test_log::test(tokio::test)]
    async fn test_ship_group_filter_uses_group_id_of_subscription_type_id_list() {
        let zk_data = user_killmail_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Simple(SimpleFilter::TotalValue {
                min: Some(5000000),
                max: None,
            })),
            FilterNode::Condition(Filter::Simple(SimpleFilter::Region(vec![10000030]))),
            // A list of type IDs of which we want to match based on their group ID
            FilterNode::Condition(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::ShipGroup(vec![
                    883, 659, 547, 4594, 485, 1538, 513, 902, 30,
                ]),
                target: Default::default(),
            })),
            FilterNode::Condition(Filter::Simple(SimpleFilter::Security(
                "0.0001..=0.4999".to_string(),
            ))),
        ]);
        // Let's check each condition:
        // 1. TotalValue: 5.2b > 5m. PASS.
        // 2. Region: Siseide (30002539) is in Heimatar (10000030). PASS.
        // 3. Security: Siseide is 0.3, which is in the range 0.0001..=0.4999. PASS.
        // 4. ShipGroup: The victim's ship Revelation (type ID 19722) is the same group as a
        //               Naglfar (type ID 19720).
        //    The Naglfar's GROUP ID is 485 (Dreadnought).
        //    The filter list does NOT contain 485. It contains the TYPE ID 19720.
        //    Therefore, this condition must PASS.

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;

        // Because one condition in the AND block fails, the entire node should fail.
        assert!(result.is_some(), "Filter should pass because the ShipGroup list contains a TypeID (19720), which has the required GroupID (485) to match the incoming killmail.");
    }

    // #[test_log::test(tokio::test)]
    // async fn test_user_scenario_shipgroup_filter_passes_with_correct_group_id() {
    //     let zk_data = user_killmail_data();
    //     let app_state = mock_app_state();
    //
    //     // This filter is corrected to use the proper Ship Group ID for a Dreadnought (485).
    //     let filter_node = FilterNode::And(vec![
    //         FilterNode::Condition(Filter::TotalValue { min: Some(5000000), max: None }),
    //         FilterNode::Condition(Filter::Region(vec![10000030])),
    //         FilterNode::Condition(Filter::ShipGroup(vec![
    //             28352, 23919, 23757, 77283, 485, 37604, 20183, 28850, 11567, // Corrected: 19720 -> 485
    //         ])),
    //         FilterNode::Condition(Filter::Security("0.0001..=0.4999".to_string())),
    //     ]);
    //
    //     // Now, all four conditions should pass.
    //     let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
    //
    //     assert!(result.is_some(), "Filter should pass now that the correct ShipGroupID (485) is used");
    // }

    async fn test_filter(filter: Filter, zk_data: &ZkData, should_pass: bool) {
        let app_state = mock_app_state();
        let result = evaluate_filter(&filter, zk_data, &app_state).await;
        assert_eq!(
            result.is_some(),
            should_pass,
            "Filter test failed for: {:?}",
            filter
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_total_value_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::Simple(SimpleFilter::TotalValue {
                min: Some(5_000_000),
                max: None,
            }),
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::TotalValue {
                min: Some(15_000_000),
                max: None,
            }),
            &zk_data,
            false,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::TotalValue {
                min: None,
                max: Some(15_000_000),
            }),
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::TotalValue {
                min: None,
                max: Some(5_000_000),
            }),
            &zk_data,
            false,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_region_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::Simple(SimpleFilter::Region(vec![10000002])),
            &zk_data,
            true,
        )
        .await; // The Forge
        test_filter(
            Filter::Simple(SimpleFilter::Region(vec![10000043])),
            &zk_data,
            false,
        )
        .await; // Domain
    }

    #[test_log::test(tokio::test)]
    async fn test_system_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::Simple(SimpleFilter::System(vec![30000142])),
            &zk_data,
            true,
        )
        .await; // Jita
        test_filter(
            Filter::Simple(SimpleFilter::System(vec![31002222])),
            &zk_data,
            false,
        )
        .await; // Amarr
    }

    #[test_log::test(tokio::test)]
    async fn test_security_filter() {
        let zk_data = default_zk_data(); // Jita is 0.9
        test_filter(
            Filter::Simple(SimpleFilter::Security("0.8..=1.0".to_string())),
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::Security("0.1..=0.5".to_string())),
            &zk_data,
            false,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_alliance_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::Alliance(vec![1001]),
            }),
            &zk_data,
            true,
        )
        .await; // Victim's alliance
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::Alliance(vec![1002]),
            }),
            &zk_data,
            true,
        )
        .await; // Attacker's alliance
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::Alliance(vec![9999]),
            }),
            &zk_data,
            false,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_corporation_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::Corporation(vec![101]),
            }),
            &zk_data,
            true,
        )
        .await; // Victim's corp
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::Corporation(vec![102]),
            }),
            &zk_data,
            true,
        )
        .await; // Attacker's corp
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::Corporation(vec![9999]),
            }),
            &zk_data,
            false,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_ship_type_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipType(vec![587]),
            }),
            &zk_data,
            true,
        )
        .await; // Victim's ship (Rifter)
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipType(vec![671]),
            }),
            &zk_data,
            true,
        )
        .await; // Attacker's ship (Catalyst)
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipType(vec![17738]),
            }),
            &zk_data,
            false,
        )
        .await; // Golem
    }

    #[test_log::test(tokio::test)]
    async fn test_ship_group_filter() {
        let zk_data = default_zk_data();
        // Victim is a Rifter (Frigate, group 25)
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![25]),
            }),
            &zk_data,
            true,
        )
        .await;
        // Attacker is a Catalyst (Destroyer, group 27)
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![27]),
            }),
            &zk_data,
            true,
        )
        .await;
        // Neither is a Marauder (group 419)
        test_filter(
            Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![419]),
            }),
            &zk_data,
            false,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_ly_filter() {
        let mut zk_data = {
            ZkData {
                kill_id: 1,
                killmail: KillmailData {
                    killmail_id: 1,
                    killmail_time: "2025-07-08T12:00:00Z".to_string(),
                    solar_system_id: 30002086, // Turnur
                    victim: Victim {
                        damage_taken: 1000,
                        ship_type_id: 587, // Rifter
                        character_id: Some(1),
                        corporation_id: Some(101),
                        alliance_id: Some(1001),
                        position: Some(Position {
                            x: 0.0,
                            y: 0.0,
                            z: 0.0,
                        }),
                        faction_id: None,
                        items: vec![],
                    },
                    attackers: vec![Attacker {
                        final_blow: true,
                        damage_done: 1000,
                        ship_type_id: Some(671), // Catalyst
                        character_id: Some(2),
                        corporation_id: Some(102),
                        alliance_id: Some(1002),
                        weapon_type_id: Some(3),
                        security_status: 0.5,
                        faction_id: None,
                    }],
                },
                zkb: Zkb {
                    total_value: 10_000_000.0,
                    dropped_value: 1_000_000.0,
                    npc: false,
                    solo: false,
                    location_id: None,
                    hash: "".to_string(),
                    fitted_value: 0.0,
                    destroyed_value: 0.0,
                    points: 0,
                    awox: false,
                    labels: vec![],
                    esi: "".to_string(),
                },
            }
        };
        zk_data.zkb.npc = true;
        test_filter(
            Filter::Simple(SimpleFilter::LyRangeFrom(vec![
                SystemRange {
                    system_id: 30002086,
                    range: 8.0,
                },
                SystemRange {
                    system_id: 30003067,
                    range: 8.0,
                },
            ])),
            &zk_data,
            true,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_is_npc_filter() {
        let mut zk_data = default_zk_data();
        zk_data.zkb.npc = true;
        test_filter(Filter::Simple(SimpleFilter::IsNpc(true)), &zk_data, true).await;
        test_filter(Filter::Simple(SimpleFilter::IsNpc(false)), &zk_data, false).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_pilots_filter() {
        let zk_data = default_zk_data(); // 2 pilots total
        test_filter(
            Filter::Simple(SimpleFilter::Pilots {
                min: Some(2),
                max: None,
            }),
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::Pilots {
                min: Some(3),
                max: None,
            }),
            &zk_data,
            false,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::Pilots {
                min: None,
                max: Some(2),
            }),
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::Pilots {
                min: None,
                max: Some(1),
            }),
            &zk_data,
            false,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_time_range_filter() {
        let zk_data = default_zk_data(); // Time is 12:00:00
        test_filter(
            Filter::Simple(SimpleFilter::TimeRange { start: 11, end: 13 }),
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Simple(SimpleFilter::TimeRange { start: 14, end: 16 }),
            &zk_data,
            false,
        )
        .await;
        // Test overnight range
        test_filter(
            Filter::Simple(SimpleFilter::TimeRange { start: 22, end: 4 }),
            &zk_data,
            false,
        )
        .await;
        let mut zk_data_night = default_zk_data();
        zk_data_night.killmail.killmail_time = "2025-07-08T23:00:00Z".to_string();
        test_filter(
            Filter::Simple(SimpleFilter::TimeRange { start: 22, end: 4 }),
            &zk_data_night,
            true,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_combined_and_filter_success() {
        // User's case: ShipGroup in a certain Region.
        // Test data: Rifter (Frigate, group 25) in Jita (The Forge, region 10000002)
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Simple(SimpleFilter::Region(vec![10000002]))), // The Forge
            FilterNode::Condition(Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![25]), // group 25 for frigate
            })), // Frigate
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_some(),
            "Combined AND filter should pass when all conditions are met"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_combined_and_filter_fail_region() {
        // Test data: Rifter (Frigate, group 25) in Jita
        // Filter: Wrong region, correct ship group
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Simple(SimpleFilter::Region(vec![10000043]))), // Domain (Wrong)
            FilterNode::Condition(Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![25]),
            })), // Frigate (Correct)
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_none(),
            "Combined AND filter should fail when region is wrong"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_combined_and_filter_fail_shipgroup() {
        // Test data: Rifter (Frigate, group 25) in Jita
        // Filter: Correct region, wrong ship group
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Simple(SimpleFilter::Region(vec![10000002]))), // The Forge (Correct)
            FilterNode::Condition(Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![419]),
            })), // Marauder (Wrong)
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_none(),
            "Combined AND filter should fail when ship group is wrong"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_combined_or_filter_success() {
        // Test data: Rifter (Frigate, group 25) in Jita
        // Filter: Wrong region OR correct ship group
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::Or(vec![
            FilterNode::Condition(Filter::Simple(SimpleFilter::Region(vec![10000043]))), // Domain (Wrong)
            FilterNode::Condition(Filter::Targeted(TargetedFilter {
                target: Target::Any,
                condition: TargetableCondition::ShipGroup(vec![25]), // group 25 for frigate
            })), // Frigate (Correct)
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_some(),
            "Combined OR filter should pass when one condition is met"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_not_filter() {
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        // This filter should pass on its own
        let inner_filter =
            FilterNode::Condition(Filter::Simple(SimpleFilter::System(vec![30000142])));
        // So the NOT filter should fail
        let not_filter = FilterNode::Not(Box::new(inner_filter));

        let result = evaluate_filter_node(&not_filter, &zk_data, &app_state).await;
        assert!(
            result.is_none(),
            "NOT filter should fail when inner condition passes"
        );

        // This filter should fail on its own
        let inner_filter_fail =
            FilterNode::Condition(Filter::Simple(SimpleFilter::System(vec![999])));
        // So the NOT filter should pass
        let not_filter_pass = FilterNode::Not(Box::new(inner_filter_fail));

        let result_pass = evaluate_filter_node(&not_filter_pass, &zk_data, &app_state).await;
        assert!(
            result_pass.is_some(),
            "NOT filter should pass when inner condition fails"
        );
    }

    fn complex_and_filter_subscription() -> FilterNode {
        serde_json::from_str(
            r#"
             {
               "And": [
                 {
                   "Condition": {
                     "Simple": {
                       "TotalValue": {
                         "min": 5000000,
                         "max": null
                       }
                     }
                   }
                 },
                 {
                   "Condition": {
                     "Simple": {
                       "Region": [
                         10000009
                       ]
                     }
                   }
                 },
                 {
                   "Condition": {
                     "Targeted": {
                       "condition": {
                         "ShipGroup": [
                           30
                         ]
                       },
                       "target": "Any"
                     }
                   }
                 },
                 {
                   "Condition": {
                     "Simple": {
                       "Security": "-1.0000..=0.0000"
                     }
                   }
                 }
               ]
             }
             "#,
        )
        .unwrap()
    }

    fn nyx_killmail() -> ZkData {
        ZkData {
            kill_id: 128577810,
            killmail: KillmailData {
                killmail_id: 128577810,
                killmail_time: "2025-07-16T03:39:17Z".to_string(),
                solar_system_id: 30000706, // A-C5TC
                victim: Victim {
                    damage_taken: 50000,
                    ship_type_id: 670, // Capsule
                    character_id: Some(90000001),
                    corporation_id: Some(1000001),
                    alliance_id: Some(2000001),
                    position: None,
                    faction_id: None,
                    items: vec![],
                },
                attackers: vec![
                    Attacker {
                        final_blow: true,
                        damage_done: 50000,
                        ship_type_id: Some(23913), // Nyx
                        character_id: Some(987654321),
                        corporation_id: Some(98657999),
                        alliance_id: Some(1354830081),
                        weapon_type_id: Some(23913),
                        security_status: -10.0,
                        faction_id: None,
                    },
                    Attacker {
                        final_blow: false,
                        damage_done: 100,
                        ship_type_id: Some(621), // Condor
                        character_id: Some(123456789),
                        corporation_id: Some(98657999),
                        alliance_id: Some(1354830081),
                        weapon_type_id: Some(501),
                        security_status: 0.5,
                        faction_id: None,
                    },
                ],
            },
            zkb: Zkb {
                total_value: 6_000_000.0,
                dropped_value: 0.0,
                npc: false,
                solo: false,
                location_id: None,
                hash: "".to_string(),
                fitted_value: 0.0,
                destroyed_value: 0.0,
                points: 0,
                awox: false,
                labels: vec![],
                esi: "".to_string(),
            },
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_complex_and_filter_with_targeted_ship_group() {
        // This test simulates the exact scenario that was failing.
        let app_state = mock_app_state();
        // Add specific data needed for this test case to the mock state
        {
            let mut systems = app_state.systems.write().unwrap();
            systems.insert(
                30000706,
                System {
                    id: 30000706,
                    name: "A-C5TC".to_string(),
                    region_id: 10000009,
                    region: "Vale of the Silent".to_string(),
                    security_status: -0.5,
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            );

            let mut ships = app_state.ships.write().unwrap();
            ships.insert(23913, 30); // Nyx -> Supercarrier (Group ID 30)
            ships.insert(621, 25); // Condor -> Frigate (Group ID 25)

            let mut names = app_state.names.write().unwrap();
            names.insert(23913, "Nyx".to_string());
            names.insert(621, "Condor".to_string());
        }

        let filter_node = complex_and_filter_subscription();
        let zk_data = nyx_killmail();

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;

        // The filter should pass because the Nyx matches all conditions.
        assert!(result.is_some(), "The filter node should have passed.");

        let filter_result = result.unwrap().filter_result;

        // The victim should not have matched.
        assert!(
            !filter_result.matched_victim,
            "Victim should not have matched the ShipGroup filter."
        );

        // The matched_attackers set should contain exactly one key: the Nyx pilot's.
        assert_eq!(
            filter_result.matched_attackers.len(),
            1,
            "There should be exactly one matched attacker: {:#?}",
            filter_result
        );

        let nyx_attacker = zk_data
            .killmail
            .attackers
            .iter()
            .find(|a| a.ship_type_id == Some(23913))
            .unwrap();
        let expected_key = AttackerKey::new(nyx_attacker);
        assert!(
            filter_result.matched_attackers.contains(&expected_key),
            "The matched attacker must be the Nyx."
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_veto_logic_with_mixed_attackers() {
        // SCENARIO:
        // - A non-blue Dreadnought (Attacker 1) is on a killmail.
        // - A blue Frigate (Attacker 2) is also on the killmail.
        // - The victim is also blue.
        // - The subscription looks for Capitals AND ignores blues.
        // EXPECTATION:
        // - The filter should PASS, because the non-blue Dreadnought is a valid match
        //   that is not vetoed.

        let app_state = mock_app_state();
        let user_id = 12345;
        let blue_alliance_id = 99000001;
        let blue_corp_id = 98000001;
        let synced_char_id = 11111;

        // 1. Setup Mock Data
        {
            let mut ships = app_state.ships.write().unwrap();
            ships.insert(19720, 485); // Naglfar -> Dreadnought (Group ID 485)
            ships.insert(587, 25); // Rifter -> Frigate (Group ID 25)

            let mut names = app_state.names.write().unwrap();
            names.insert(19720, "Naglfar".to_string());
            names.insert(587, "Rifter".to_string());

            // Setup the user's standings and token for the veto check
            let mut standings_map = app_state.user_standings.write().unwrap();
            let mut user_standings = UserStandings::default();
            user_standings.tokens.push(EveAuthToken {
                character_id: synced_char_id,
                character_name: "Blue Pilot".to_string(),
                corporation_id: blue_corp_id,
                alliance_id: Some(blue_alliance_id),
                access_token: "".to_string(),
                refresh_token: "".to_string(),
                expires_at: 0,
            });
            standings_map.insert(serenity::model::id::UserId(user_id), user_standings);
        }

        // 2. Construct the Killmail
        let zk_data = ZkData {
            kill_id: 99999,
            killmail: KillmailData {
                killmail_id: 99999,
                killmail_time: "2025-07-16T12:00:00Z".to_string(),
                solar_system_id: 30000142,
                victim: Victim {
                    damage_taken: 100,
                    ship_type_id: 587, // Blue Rifter
                    character_id: Some(3003),
                    corporation_id: Some(blue_corp_id),
                    alliance_id: Some(blue_alliance_id),
                    position: None,
                    faction_id: None,
                    items: vec![],
                },
                attackers: vec![
                    Attacker {
                        // The Non-Blue Capital we want to match
                        final_blow: true,
                        damage_done: 10000,
                        ship_type_id: Some(19720),
                        character_id: Some(1001),
                        corporation_id: Some(101),
                        alliance_id: Some(11),
                        weapon_type_id: None,
                        security_status: -1.0,
                        faction_id: None,
                    },
                    Attacker {
                        // The Blue Frigate that should be vetoed
                        final_blow: false,
                        damage_done: 100,
                        ship_type_id: Some(587),
                        character_id: Some(2002),
                        corporation_id: Some(blue_corp_id),
                        alliance_id: Some(blue_alliance_id),
                        weapon_type_id: None,
                        security_status: 0.5,
                        faction_id: None,
                    },
                ],
            },
            zkb: Zkb {
                total_value: 1_000_000_000.0,
                ..Default::default()
            },
        };

        // 3. Construct the Subscription
        let subscription = Subscription {
            id: "capital_and_veto_test".to_string(),
            description: "".to_string(),
            action: Default::default(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::ShipGroup(vec![485]), // Match Dreadnoughts
                    target: Default::default(),
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding {
                    synched_by_user_id: user_id,
                    source: StandingSource::Character,
                    source_entity_id: synced_char_id,
                })),
            ]),
        };

        // Manually add the subscription to the app state
        app_state
            .subscriptions
            .write()
            .unwrap()
            .insert(GuildId(1), vec![subscription]);

        // 4. Run the processor
        let results = process_killmail(&app_state, &zk_data).await;

        // 5. Assert the outcome
        assert_eq!(
            results.len(),
            1,
            "Expected exactly one subscription to match."
        );

        let (_guild_id, _sub, named_result) = results.first().unwrap();

        // Check the match result
        let primary_matches = &named_result.filter_result;
        assert!(
            !primary_matches.matched_victim,
            "The victim (a frigate) should not have matched the capital filter."
        );
        assert_eq!(
            primary_matches.matched_attackers.len(),
            1,
            "Only the dreadnought should have matched the primary filter."
        );
        let attacker_key = Vec::from_iter(primary_matches.matched_attackers.iter())
            .first()
            .cloned()
            .unwrap();
        assert!(attacker_key.0.contains("s19720"))
    }

    #[test_log::test(tokio::test)]
    async fn test_attacker_only_filter_ignores_victim() {
        // SCENARIO:
        // - A non-blue Dreadnought is the VICTIM.
        // - A blue Dreadnought is the ATTACKER.
        // - The subscription looks for Capital ATTACKERS and ignores blues.
        // EXPECTATION:
        // - The filter should FAIL. The only matching capital is the victim, but the filter
        //   is for attackers only. The only attacker is a capital, but it's blue and should be vetoed.

        let app_state = mock_app_state();
        let user_id = 12345;
        let blue_alliance_id = 99009927; // From your killmail data
        let blue_corp_id = 98478883;
        let synced_char_id = 11111;

        // 1. Setup Mock Data
        {
            let mut ships = app_state.ships.write().unwrap();
            ships.insert(19726, 485); // Revelation -> Dreadnought (Group ID 485)
            ships.insert(52907, 485); // "Capsule - Genolution 'Auroral' 197-variant" is not a dread, let's use a real one for the blue attacker
                                      // For the purpose of this test, let's say the blue attacker is also in a Naglfar
            ships.insert(19720, 485); // Naglfar -> Dreadnought

            // Setup the user's standings for the veto check
            let mut standings_map = app_state.user_standings.write().unwrap();
            let mut user_standings = UserStandings::default();
            user_standings.tokens.push(EveAuthToken {
                character_id: synced_char_id,
                character_name: "Blue Pilot".to_string(),
                corporation_id: blue_corp_id,
                alliance_id: Some(blue_alliance_id),
                access_token: "".to_string(),
                refresh_token: "".to_string(),
                expires_at: 0,
            });
            standings_map.insert(serenity::model::id::UserId(user_id), user_standings);
        }

        // 2. Construct the Killmail from your data
        let zk_data = ZkData {
             kill_id: 128637593,
             killmail: serde_json::from_str(r#"{"attackers":[{"alliance_id":99009927,"character_id":2121351054,"corporation_id":98478883,"damage_done":249421,"final_blow":false,"security_status":-9.6,"ship_type_id":52907,"weapon_type_id":52907},{"alliance_id":99009927,"character_id":2121351026,"corporation_id":98478883,"damage_done":207718,"final_blow":false,"security_status":-9.6,"ship_type_id":52907,"weapon_type_id":52907},{"alliance_id":99009927,"character_id":2122219865,"corporation_id":98478883,"damage_done":147451,"final_blow":true,"security_status":-1.8,"ship_type_id":73790,"weapon_type_id":37298},{"alliance_id":99009927,"character_id":95679911,"corporation_id":98478883,"damage_done":9668,"final_blow":false,"security_status":-10.0,"ship_type_id":22428,"weapon_type_id":22428},{"alliance_id":99009927,"character_id":2118965168,"corporation_id":98478883,"damage_done":6678,"final_blow":false,"security_status":-9.9,"ship_type_id":22436,"weapon_type_id":24509},{"alliance_id":99009927,"character_id":1800487696,"corporation_id":590940989,"damage_done":4703,"final_blow":false,"security_status":-9.9,"ship_type_id":22428,"weapon_type_id":22428},{"alliance_id":99009927,"character_id":2121752432,"corporation_id":98478883,"damage_done":3524,"faction_id":500010,"final_blow":false,"security_status":-7.3,"ship_type_id":22440,"weapon_type_id":2953},{"alliance_id":99009927,"character_id":1210978935,"corporation_id":98478883,"damage_done":3059,"final_blow":false,"security_status":-9.9,"ship_type_id":22428,"weapon_type_id":2446},{"alliance_id":99009927,"character_id":2119958707,"corporation_id":98478883,"damage_done":2690,"final_blow":false,"security_status":-10.0,"ship_type_id":22428,"weapon_type_id":15887},{"alliance_id":99009927,"character_id":2119722210,"corporation_id":98478883,"damage_done":2688,"final_blow":false,"security_status":-10.0,"ship_type_id":22440,"weapon_type_id":22440},{"alliance_id":99009927,"character_id":1113404550,"corporation_id":590940989,"damage_done":2403,"final_blow":false,"security_status":-9.7,"ship_type_id":22428,"weapon_type_id":22428},{"alliance_id":99009927,"character_id":1658503065,"corporation_id":590940989,"damage_done":151,"final_blow":false,"security_status":-10.0,"ship_type_id":22428,"weapon_type_id":4147},{"character_id":2121332622,"corporation_id":98615046,"damage_done":0,"final_blow":false,"security_status":-2.8,"ship_type_id":670,"weapon_type_id":3244},{"alliance_id":99013187,"character_id":96143629,"corporation_id":98713865,"damage_done":0,"final_blow":false,"security_status":-10.0,"ship_type_id":33151,"weapon_type_id":3146}],"killmail_id":128637593,"killmail_time":"2025-07-19T04:56:47Z","solar_system_id":30002719,"victim":{"alliance_id":99014140,"character_id":2113230779,"corporation_id":98802264,"damage_taken":640154,"items":[{"flag":14,"item_type_id":2048,"quantity_dropped":1,"singleton":0},{"flag":94,"item_type_id":31820,"quantity_destroyed":1,"singleton":0},{"flag":5,"item_type_id":2811,"quantity_dropped":1500,"singleton":0},{"flag":24,"item_type_id":41489,"quantity_destroyed":1,"singleton":0},{"flag":28,"item_type_id":4292,"quantity_destroyed":1,"singleton":0},{"flag":5,"item_type_id":24521,"quantity_dropped":1500,"singleton":0},{"flag":133,"item_type_id":16275,"quantity_dropped":375,"singleton":0},{"flag":13,"item_type_id":49738,"quantity_dropped":1,"singleton":0},{"flag":155,"item_type_id":41489,"quantity_destroyed":78,"singleton":0},{"flag":30,"item_type_id":27345,"quantity_dropped":7,"singleton":0},{"flag":11,"item_type_id":1541,"quantity_dropped":1,"singleton":0},{"flag":25,"item_type_id":41492,"quantity_destroyed":1,"singleton":0},{"flag":27,"item_type_id":37292,"quantity_dropped":1,"singleton":0},{"flag":5,"item_type_id":41489,"quantity_dropped":1,"singleton":0},{"flag":133,"item_type_id":17888,"quantity_dropped":193009,"singleton":0},{"flag":23,"item_type_id":2281,"quantity_destroyed":1,"singleton":0},{"flag":19,"item_type_id":47702,"quantity_destroyed":1,"singleton":0},{"flag":29,"item_type_id":37292,"quantity_destroyed":1,"singleton":0},{"flag":5,"item_type_id":24519,"quantity_destroyed":1500,"singleton":0},{"flag":29,"item_type_id":27345,"quantity_destroyed":7,"singleton":0},{"flag":15,"item_type_id":49738,"quantity_dropped":1,"singleton":0},{"flag":5,"item_type_id":27359,"quantity_destroyed":1500,"singleton":0},{"flag":31,"item_type_id":14168,"quantity_destroyed":1,"singleton":0},{"flag":22,"item_type_id":47736,"quantity_destroyed":1,"singleton":0},{"flag":27,"item_type_id":27345,"quantity_dropped":7,"singleton":0},{"flag":93,"item_type_id":31820,"quantity_destroyed":1,"singleton":0},{"flag":30,"item_type_id":37292,"quantity_dropped":1,"singleton":0},{"flag":5,"item_type_id":27345,"quantity_destroyed":1308,"singleton":0},{"flag":92,"item_type_id":31720,"quantity_destroyed":1,"singleton":0},{"flag":12,"item_type_id":49738,"quantity_destroyed":1,"singleton":0},{"flag":5,"item_type_id":27351,"quantity_dropped":1500,"singleton":0},{"flag":5,"item_type_id":24523,"quantity_dropped":1500,"singleton":0},{"flag":25,"item_type_id":41489,"quantity_dropped":2,"singleton":0},{"flag":21,"item_type_id":24443,"quantity_dropped":1,"singleton":0},{"flag":20,"item_type_id":41507,"quantity_destroyed":1,"singleton":0},{"flag":24,"item_type_id":41492,"quantity_dropped":1,"singleton":0}],"position":{"x":1886548484863.2898,"y":-243470362632.75974,"z":-2231950897505.0303},"ship_type_id":19726}}"#).unwrap(),
             zkb: Zkb { total_value: 5978380820.02, ..Default::default() },
         };

        // 3. Construct the Subscription
        let subscription = Subscription {
            id: "attacker_only_capital_test".to_string(),
            description: "".to_string(),
            action: Default::default(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::ShipGroup(vec![485]), // Match Dreadnoughts
                    target: crate::config::Target::Attacker, // IMPORTANT: Target is Attacker only
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::IgnoreHighStanding {
                    synched_by_user_id: user_id,
                    source: StandingSource::Alliance,
                    source_entity_id: blue_alliance_id,
                })),
            ]),
        };

        app_state
            .subscriptions
            .write()
            .unwrap()
            .insert(GuildId(1), vec![subscription]);

        // 4. Run the processor
        let results = process_killmail(&app_state, &zk_data).await;

        // 5. Assert the outcome
        assert!(results.is_empty(), "Expected zero matches, found {}. The only matching capital attacker was blue and should have been vetoed, and the non-blue capital victim
should have been ignored by the attacker-only filter: {:#?}", results.len(), results);
    }

    /// Helper to load a killmail fixture from the resources directory
    fn load_fixture(name: &str) -> ZkData {
        let path = format!("resources/{}", name);
        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read fixture {}: {}", path, e));
        serde_json::from_str(&contents)
            .unwrap_or_else(|e| panic!("Failed to parse fixture {}: {}", path, e))
    }

    /// Test that a "capital attacker" filter does NOT match a capital LOSS
    /// where the victim is a capital but all attackers are subcaps.
    ///
    /// This test verifies the fix for the is_victim() bug where Target::Attacker
    /// was incorrectly matching victims.
    #[test_log::test(tokio::test)]
    async fn test_capital_attacker_filter_ignores_capital_victim() {
        // Load the Naglfar loss killmail - victim is a Dreadnought, attackers are all subcaps
        let zk_data = load_fixture("132249213_naglfar_loss.json");

        // Verify fixture data is as expected
        assert_eq!(zk_data.killmail.victim.ship_type_id, 19722, "Victim should be a Naglfar");

        let app_state = mock_app_state();

        // Add ship group mappings for this killmail
        {
            let mut ships = app_state.ships.write().unwrap();
            // Naglfar (victim) - Dreadnought group 485
            ships.insert(19722, 485);
            // Attacker ships - all subcaps (not group 485)
            ships.insert(624, 28);    // Badger - Industrial
            ships.insert(22428, 906); // Maulus Navy Issue - Combat Recon
            ships.insert(22430, 906); // Exequror Navy Issue - Combat Recon
            ships.insert(22440, 906); // Osprey Navy Issue - Combat Recon
            ships.insert(44996, 1527); // Kikimora - Destroyer
            ships.insert(73796, 1527); // Draugur - Destroyer
        }

        // Create a subscription that looks for capital ATTACKERS only
        let subscription = Subscription {
            id: "capital_attacker_test".to_string(),
            description: "Track capital attacker activity".to_string(),
            action: Default::default(),
            root_filter: FilterNode::Condition(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::ShipGroup(vec![485]), // Dreadnought group
                target: Target::Attacker, // ONLY match attackers, not victims
            })),
        };

        app_state
            .subscriptions
            .write()
            .unwrap()
            .insert(GuildId(1), vec![subscription]);

        // Run the processor
        let results = process_killmail(&app_state, &zk_data).await;

        // The filter should NOT match because:
        // - The Naglfar (capital) is the VICTIM
        // - All ATTACKERS are subcaps
        // - The filter targets Attacker only
        assert!(
            results.is_empty(),
            "Capital attacker filter should NOT match a capital loss. \
             The victim is a Naglfar (capital) but all attackers are subcaps. \
             Found {} matches: {:#?}",
            results.len(),
            results
        );
    }

    /// Test that a "capital attacker" filter DOES match when a capital is attacking.
    /// This is the positive case - a Thanatos (carrier) killing a Drill should match.
    #[test_log::test(tokio::test)]
    async fn test_capital_attacker_filter_matches_capital_attacker() {
        // Load the Drill kill - attackers include a Thanatos (carrier) and Nyx (supercarrier)
        let zk_data = load_fixture("132253134_drill_kill_by_thanatos.json");

        // Verify fixture data is as expected
        assert_eq!(zk_data.killmail.victim.ship_type_id, 81826, "Victim should be a Drill");

        let app_state = mock_app_state();

        // Add ship group mappings for this killmail
        {
            let mut ships = app_state.ships.write().unwrap();
            // Victim - Drill (Upwell structure, not a capital)
            ships.insert(81826, 1404); // Upwell structure group
            // Attacker ships
            ships.insert(23911, 547);  // Thanatos - Carrier (capital!)
            ships.insert(23913, 659);  // Nyx - Supercarrier (capital!)
            ships.insert(22428, 906);  // Maulus Navy Issue - Combat Recon (subcap)
        }

        // Create a subscription that looks for capital ATTACKERS
        // Include common capital groups: Dread (485), Carrier (547), Super (659), FAX (1538)
        let subscription = Subscription {
            id: "capital_attacker_test".to_string(),
            description: "Track capital attacker activity".to_string(),
            action: Default::default(),
            root_filter: FilterNode::Condition(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::ShipGroup(vec![485, 547, 659, 1538]),
                target: Target::Attacker, // ONLY match attackers
            })),
        };

        app_state
            .subscriptions
            .write()
            .unwrap()
            .insert(GuildId(1), vec![subscription]);

        // Run the processor
        let results = process_killmail(&app_state, &zk_data).await;

        // The filter SHOULD match because:
        // - The attackers include a Thanatos (carrier, group 547) and Nyx (super, group 659)
        // - The filter targets capital Attackers
        assert!(
            !results.is_empty(),
            "Capital attacker filter SHOULD match when capitals are attacking. \
             Attackers include Thanatos (carrier) and Nyx (supercarrier)."
        );

        // Verify the matched attackers are the capitals
        let matched = &results[0].2.filter_result;
        assert!(
            !matched.matched_attackers.is_empty(),
            "Should have matched the capital attackers"
        );
        assert!(
            !matched.matched_victim,
            "Should NOT have matched the victim (Drill is not a capital)"
        );
    }
}
