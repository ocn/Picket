use crate::config::{AppState, Filter, FilterNode, Subscription, System};
use crate::models::ZkData;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};
use crate::discord_bot::{get_system, get_ship_group_id};
use futures::future::{BoxFuture, FutureExt};

pub async fn process_killmail(app_state: &Arc<AppState>, zk_data: &ZkData) -> Vec<Subscription> {
    let mut matched_subscriptions = Vec::new();
    let subscriptions_map = app_state.subscriptions.read().unwrap();
    let kill_id = zk_data.killmail.killmail_id;

    for subscriptions_vec in subscriptions_map.values() {
        for subscription in subscriptions_vec.iter() {
            info!(
                "[Kill: {}] Evaluating subscription '{}' for channel {}",
                kill_id, subscription.id, subscription.action.channel_id
            );
            if evaluate_filter_node(&subscription.root_filter, zk_data, app_state).await {
                matched_subscriptions.push(subscription.clone());
            }
        }
    }
    matched_subscriptions
}

fn evaluate_filter_node<'a>(
    node: &'a FilterNode,
    zk_data: &'a ZkData,
    app_state: &'a Arc<AppState>,
) -> BoxFuture<'a, bool> {
    async move {
        match node {
            FilterNode::Condition(filter) => evaluate_filter(filter, zk_data, app_state).await,
            FilterNode::And(nodes) => {
                for n in nodes {
                    if !evaluate_filter_node(n, zk_data, app_state).await {
                        return false;
                    }
                }
                true
            }
            FilterNode::Or(nodes) => {
                for n in nodes {
                    if evaluate_filter_node(n, zk_data, app_state).await {
                        return true;
                    }
                }
                false
            }
            FilterNode::Not(node) => !evaluate_filter_node(node, zk_data, app_state).await,
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

async fn evaluate_filter(filter: &Filter, zk_data: &ZkData, app_state: &Arc<AppState>) -> bool {
    let killmail = &zk_data.killmail;
    
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
            get_system(app_state, killmail.solar_system_id).await.map_or(false, |s| region_ids.contains(&s.region_id))
        }
        Filter::System(system_ids) => system_ids.contains(&killmail.solar_system_id),
        Filter::Security(range_str) => {
            if let (Some(system), Ok(range)) = (get_system(app_state, killmail.solar_system_id).await, parse_security_range(range_str)) {
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
            if let Some(group_id) = get_ship_group_id(app_state, killmail.victim.ship_type_id).await {
                if ship_group_ids.contains(&group_id) {
                    return true;
                }
            }
            for attacker in &killmail.attackers {
                if let Some(ship_id) = attacker.ship_type_id {
                    if let Some(group_id) = get_ship_group_id(app_state, ship_id).await {
                        if ship_group_ids.contains(&group_id) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        Filter::LyRangeFrom { systems, range } => {
            if let Some(killmail_system) = get_system(app_state, killmail.solar_system_id).await {
                for target_system_id in systems {
                    if let Some(target_system) = get_system(app_state, *target_system_id).await {
                        if distance(&killmail_system, &target_system) <= *range {
                            return true;
                        }
                    } else {
                        warn!("Could not find target system {} for LY range check", target_system_id);
                    }
                }
            } else {
                warn!("Could not find killmail system {} for LY range check", killmail.solar_system_id);
            }
            false
        }
        Filter::IsNpc(is_npc) => zk_data.zkb.npc == *is_npc,
        Filter::IsSolo(is_solo) => zk_data.zkb.solo == *is_solo,
    }
}
