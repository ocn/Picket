use crate::config::{AppState, Filter, FilterNode, Subscription, System};
use crate::discord_bot::{get_ship_group_id, get_system};
use crate::models::ZkData;
use chrono::Timelike;
use futures::future::{BoxFuture, FutureExt};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug, Clone, Default)]
pub struct FilterResult {
    pub(crate) matched_ship: Option<MatchedShip>,
    pub(crate) color: Option<Color>,
    pub(crate) min_pilots: Option<u32>,
}

#[derive(Debug, Copy, Clone, Default)]
pub(crate) enum Color {
    Green,
    #[default]
    Red,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MatchedShip {
    pub(crate) ship_name: String,
    pub(crate) type_id: u32,
    pub(crate) corp_id: Option<u64>,
    pub(crate) alliance_id: Option<u64>,
}

pub async fn process_killmail(
    app_state: &Arc<AppState>,
    zk_data: &ZkData,
) -> Vec<(Subscription, FilterResult)> {
    let mut matched_subscriptions = Vec::new();
    let subscriptions_map = app_state.subscriptions.read().unwrap();
    let kill_id = zk_data.killmail.killmail_id;

    for (guild_id, subscriptions_vec) in subscriptions_map.iter() {
        info!(
            "[Kill: {}] Evaluating kill against '{}' subscriptions for guild ID {}",
            kill_id,
            subscriptions_vec.len(),
            guild_id.0
        );
        for subscription in subscriptions_vec.iter() {
            if let Some(result) =
                evaluate_filter_node(&subscription.root_filter, zk_data, app_state).await
            {
                matched_subscriptions.push((subscription.clone(), result));
            }
        }
    }
    matched_subscriptions
}

fn evaluate_filter_node<'a>(
    node: &'a FilterNode,
    zk_data: &'a ZkData,
    app_state: &'a Arc<AppState>,
) -> BoxFuture<'a, Option<FilterResult>> {
    async move {
        match node {
            FilterNode::Condition(filter) => evaluate_filter(filter, zk_data, app_state).await,
            FilterNode::And(nodes) => {
                let mut results = Vec::new();
                for n in nodes {
                    if let Some(result) = evaluate_filter_node(n, zk_data, app_state).await {
                        results.push(result);
                    } else {
                        return None; // One failure means the whole And block fails
                    }
                }
                // Merge results
                let final_result = FilterResult {
                    color: results
                        .iter()
                        .find(|r| r.matched_ship.is_some())
                        .map_or(results[0].color, |r| r.color),
                    matched_ship: results
                        .iter()
                        .find(|r| r.matched_ship.is_some())
                        .and_then(|r| r.matched_ship.clone()),
                    min_pilots: results.iter().find_map(|r| r.min_pilots),
                };
                Some(final_result)
            }
            FilterNode::Or(nodes) => {
                for n in nodes {
                    if let Some(result) = evaluate_filter_node(n, zk_data, app_state).await {
                        return Some(result); // Return the first match
                    }
                }
                None
            }
            FilterNode::Not(node) => {
                if evaluate_filter_node(node, zk_data, app_state)
                    .await
                    .is_some()
                {
                    None
                } else {
                    Some(FilterResult::default()) // Default success
                }
            }
        }
    }
    .boxed()
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

async fn evaluate_filter(
    filter: &Filter,
    zk_data: &ZkData,
    app_state: &Arc<AppState>,
) -> Option<FilterResult> {
    let killmail = &zk_data.killmail;

    match filter {
        Filter::TotalValue { min, max } => {
            let total_value = zk_data.zkb.total_value;
            if min.is_none_or(|m| total_value >= m as f64)
                && max.is_none_or(|m| total_value <= m as f64)
            {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::DroppedValue { min, max } => {
            let dropped_value = zk_data.zkb.dropped_value;
            if min.is_none_or(|m| dropped_value >= m as f64)
                && max.is_none_or(|m| dropped_value <= m as f64)
            {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::Region(region_ids) => {
            if get_system(app_state, killmail.solar_system_id)
                .await
                .is_some_and(|s| region_ids.contains(&s.region_id))
            {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::System(system_ids) => {
            if system_ids.contains(&killmail.solar_system_id) {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::Security(range_str) => {
            if let (Some(system), Ok(range)) = (
                get_system(app_state, killmail.solar_system_id).await,
                parse_security_range(range_str),
            ) {
                if range.contains(&system.security_status) {
                    return Some(Default::default());
                }
            }
            None
        }
        Filter::Alliance(alliance_ids) => {
            let victim_match = killmail
                .victim
                .alliance_id
                .is_some_and(|id| alliance_ids.contains(&id));
            let attacker_match = killmail
                .attackers
                .iter()
                .any(|a| a.alliance_id.is_some_and(|id| alliance_ids.contains(&id)));
            if victim_match || attacker_match {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::Corporation(corporation_ids) => {
            let victim_match = killmail
                .victim
                .corporation_id
                .is_some_and(|id| corporation_ids.contains(&id));
            let attacker_match = killmail.attackers.iter().any(|a| {
                a.corporation_id
                    .is_some_and(|id| corporation_ids.contains(&id))
            });
            if victim_match || attacker_match {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::Character(character_ids) => {
            let victim_match = killmail
                .victim
                .character_id
                .is_some_and(|id| character_ids.contains(&id));
            let attacker_match = killmail.attackers.iter().any(|a| {
                a.character_id
                    .is_some_and(|id| character_ids.contains(&id))
            });
            if victim_match || attacker_match {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::ShipType(ship_type_ids) => {
            if ship_type_ids.contains(&killmail.victim.ship_type_id) {
                let ship_name =
                    crate::discord_bot::get_name(app_state, killmail.victim.ship_type_id as u64)
                        .await
                        .unwrap_or_default();
                return Some(FilterResult {
                    color: Some(Color::Red), // Red for victim match
                    matched_ship: Some(MatchedShip {
                        ship_name,
                        type_id: killmail.victim.ship_type_id,
                        corp_id: killmail.victim.corporation_id,
                        alliance_id: killmail.victim.alliance_id,
                    }),
                    min_pilots: None,
                });
            }
            for attacker in &killmail.attackers {
                if let Some(ship_id) = attacker.ship_type_id {
                    if ship_type_ids.contains(&ship_id) {
                        let ship_name = crate::discord_bot::get_name(app_state, ship_id as u64)
                            .await
                            .unwrap_or_default();
                        return Some(FilterResult {
                            color: Some(Color::Green), // Green for attacker match
                            matched_ship: Some(MatchedShip {
                                ship_name,
                                type_id: ship_id,
                                corp_id: attacker.corporation_id,
                                alliance_id: attacker.alliance_id,
                            }),
                            min_pilots: None,
                        });
                    }
                }
            }
            None
        }
        Filter::ShipGroup(ship_group_ids) => {
            if let Some(group_id) = get_ship_group_id(app_state, killmail.victim.ship_type_id).await
            {
                if ship_group_ids.contains(&group_id) {
                    let ship_name = crate::discord_bot::get_name(
                        app_state,
                        killmail.victim.ship_type_id as u64,
                    )
                    .await
                    .unwrap_or_default();
                    return Some(FilterResult {
                        color: Some(Color::Red),
                        matched_ship: Some(MatchedShip {
                            ship_name,
                            type_id: killmail.victim.ship_type_id,
                            corp_id: killmail.victim.corporation_id,
                            alliance_id: killmail.victim.alliance_id,
                        }),
                        min_pilots: None,
                    });
                }
            }
            for attacker in &killmail.attackers {
                if let Some(ship_id) = attacker.ship_type_id {
                    if let Some(group_id) = get_ship_group_id(app_state, ship_id).await {
                        if ship_group_ids.contains(&group_id) {
                            let ship_name = crate::discord_bot::get_name(app_state, ship_id as u64)
                                .await
                                .unwrap_or_default();
                            return Some(FilterResult {
                                color: Some(Color::Green),
                                matched_ship: Some(MatchedShip {
                                    ship_name,
                                    type_id: ship_id,
                                    corp_id: attacker.corporation_id,
                                    alliance_id: attacker.alliance_id,
                                }),
                                min_pilots: None,
                            });
                        }
                    }
                }
            }
            None
        }
        Filter::LyRangeFrom { systems, range } => {
            if let Some(killmail_system) = get_system(app_state, killmail.solar_system_id).await {
                for target_system_id in systems {
                    if let Some(target_system) = get_system(app_state, *target_system_id).await {
                        if distance(&killmail_system, &target_system) <= *range {
                            return Some(Default::default());
                        }
                    } else {
                        warn!(
                            "Could not find target system {} for LY range check",
                            target_system_id
                        );
                    }
                }
            } else {
                warn!(
                    "Could not find killmail system {} for LY range check",
                    killmail.solar_system_id
                );
            }
            None
        }
        Filter::IsNpc(is_npc) => {
            if zk_data.zkb.npc == *is_npc {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::IsSolo(is_solo) => {
            if zk_data.zkb.solo == *is_solo {
                Some(Default::default())
            } else {
                None
            }
        }
        Filter::Pilots { min, max } => {
            let num_pilots = (killmail.attackers.len() + 1) as u32;
            if min.is_none_or(|m| num_pilots >= m) && max.is_none_or(|m| num_pilots <= m) {
                Some(FilterResult {
                    color: None,
                    matched_ship: None,
                    min_pilots: Some(min.unwrap_or(0)),
                })
            } else {
                None
            }
        }
        Filter::NameFragment(fragment) => {
            let lower_fragment = fragment.to_lowercase();

            // Check victim ship name
            if let Some(name) =
                crate::discord_bot::get_name(app_state, killmail.victim.ship_type_id as u64).await
            {
                if name.to_lowercase().contains(&lower_fragment) {
                    return Some(FilterResult {
                        color: Some(Color::Red),
                        matched_ship: None,
                        min_pilots: None,
                    });
                }
            }

            // Check attacker ship names
            for attacker in &killmail.attackers {
                if let Some(ship_id) = attacker.ship_type_id {
                    if let Some(name) =
                        crate::discord_bot::get_name(app_state, ship_id as u64).await
                    {
                        if name.to_lowercase().contains(&lower_fragment) {
                            return Some(FilterResult {
                                color: Some(Color::Green),
                                matched_ship: None,
                                min_pilots: None,
                            });
                        }
                    }
                }
            }
            None
        }
        Filter::TimeRange { start, end } => {
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
    }
}
