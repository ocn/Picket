use crate::config::{AppState, Filter, FilterNode, Subscription, System};
use crate::models::ZkData;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

pub fn process_killmail(app_state: &Arc<AppState>, zk_data: &ZkData) -> Vec<Subscription> {
    let mut matched_subscriptions = Vec::new();
    let subscriptions_map = app_state.subscriptions.read().unwrap();

    for subscriptions_vec in subscriptions_map.values() {
        for subscription in subscriptions_vec.iter() {
            if evaluate_filter_node(&subscription.root_filter, zk_data, app_state) {
                matched_subscriptions.push(subscription.clone());
            }
        }
    }
    matched_subscriptions
}

fn evaluate_filter_node(node: &FilterNode, zk_data: &ZkData, app_state: &Arc<AppState>) -> bool {
    match node {
        FilterNode::Condition(filter) => evaluate_filter(filter, zk_data, app_state),
        FilterNode::And(nodes) => nodes.iter().all(|n| evaluate_filter_node(n, zk_data, app_state)),
        FilterNode::Or(nodes) => nodes.iter().any(|n| evaluate_filter_node(n, zk_data, app_state)),
        FilterNode::Not(node) => !evaluate_filter_node(node, zk_data, app_state),
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

fn get_system(app_state: &Arc<AppState>, system_id: u32) -> Option<System> {
    let systems = app_state.systems.read().unwrap();
    systems.get(&system_id).cloned()
    // This is where we would add dynamic fetching if needed, but for now, we rely on the pre-loaded data.
}

fn distance(system1: &System, system2: &System) -> f64 {
    const LY_PER_M: f64 = 1.0 / 9_460_730_472_580_800.0;
    let dx = system1.x - system2.x;
    let dy = system1.y - system2.y;
    let dz = system1.z - system2.z;
    (dx * dx + dy * dy + dz * dz).sqrt() * LY_PER_M
}

fn evaluate_filter(filter: &Filter, zk_data: &ZkData, app_state: &Arc<AppState>) -> bool {
    let killmail = &zk_data.killmail;
    let killmail_system = get_system(app_state, killmail.solar_system_id);

    match filter {
        Filter::TotalValue { min, max } => {
            let total_value = zk_data.zkb.total_value;
            min.map_or(true, |m| total_value >= m as f64) && max.map_or(true, |m| total_value <= m as f64)
        }
        Filter::DroppedValue { min, max } => {
            let dropped_value = zk_data.zkb.dropped_value;
            min.map_or(true, |m| dropped_value >= m as f64) && max.map_or(true, |m| dropped_value <= m as f64)
        }
        Filter::Region(region_ids) => {
            killmail_system.as_ref().map_or(false, |s| region_ids.contains(&s.region_id))
        }
        Filter::System(system_ids) => system_ids.contains(&killmail.solar_system_id),
        Filter::Security(range_str) => {
            if let (Some(system), Ok(range)) = (killmail_system.as_ref(), parse_security_range(range_str)) {
                range.contains(&system.security_status)
            } else {
                false
            }
        }
        Filter::Alliance(alliance_ids) => {
            killmail.victim.alliance_id.map_or(false, |id| alliance_ids.contains(&id))
                || killmail
                    .attackers
                    .iter()
                    .any(|a| a.alliance_id.map_or(false, |id| alliance_ids.contains(&id)))
        }
        Filter::Corporation(corporation_ids) => {
            killmail.victim.corporation_id.map_or(false, |id| corporation_ids.contains(&id))
                || killmail
                    .attackers
                    .iter()
                    .any(|a| a.corporation_id.map_or(false, |id| corporation_ids.contains(&id)))
        }
        Filter::Character(character_ids) => {
            killmail.victim.character_id.map_or(false, |id| character_ids.contains(&id))
                || killmail
                    .attackers
                    .iter()
                    .any(|a| a.character_id.map_or(false, |id| character_ids.contains(&id)))
        }
        Filter::ShipType(ship_type_ids) => {
            ship_type_ids.contains(&killmail.victim.ship_type_id)
                || killmail
                    .attackers
                    .iter()
                    .any(|a| a.ship_type_id.map_or(false, |id| ship_type_ids.contains(&id)))
        }
        Filter::ShipGroup(ship_group_ids) => {
            let ships = app_state.ships.read().unwrap();
            let victim_match = ships
                .get(&killmail.victim.ship_type_id)
                .map_or(false, |s| ship_group_ids.contains(&s.group_id));
            victim_match
                || killmail.attackers.iter().any(|a| {
                    a.ship_type_id
                        .and_then(|id| ships.get(&id))
                        .map_or(false, |s| ship_group_ids.contains(&s.group_id))
                })
        }
        Filter::LyRangeFrom { systems, range } => {
            if let Some(killmail_system) = killmail_system {
                systems.iter().any(|target_system_id| {
                    if let Some(target_system) = get_system(app_state, *target_system_id) {
                        distance(&killmail_system, &target_system) <= *range
                    } else {
                        warn!("Could not find target system {} for LY range check", target_system_id);
                        false
                    }
                })
            } else {
                warn!("Could not find killmail system {} for LY range check", killmail.solar_system_id);
                false
            }
        }
        Filter::IsNpc(is_npc) => zk_data.zkb.npc == *is_npc,
        Filter::IsSolo(is_solo) => zk_data.zkb.solo == *is_solo,
    }
}