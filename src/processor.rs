use crate::config::{AppState, Filter, FilterNode, Subscription, System};
use crate::discord_bot::{get_ship_group_id, get_system};
use crate::models::ZkData;
use chrono::Timelike;
use futures::future::{BoxFuture, FutureExt};
use serenity::model::prelude::GuildId;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

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
) -> Vec<(GuildId, Subscription, FilterResult)> {
    // Clone the subscriptions to release the lock before the async loop.
    // We flatten the map of guilds into a single list of all subscriptions.
    let all_subscriptions = {
        let subs_guard = app_state.subscriptions.read().unwrap();
        subs_guard.clone()
    };
    // The `subs_guard` is dropped here, releasing the lock.

    let mut matched_subscriptions = Vec::new();
    let _kill_id = zk_data.killmail.killmail_id;

    // Iterate over the cloned subscriptions. No lock is held here.
    for (guild_id, subscriptions) in all_subscriptions.iter() {
        for subscription in subscriptions {
            if let Some(result) =
                evaluate_filter_node(&subscription.root_filter, zk_data, app_state).await
            {
                matched_subscriptions.push((*guild_id, subscription.clone(), result));
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
        let kill_id = zk_data.killmail.killmail_id;
        match node {
            FilterNode::Condition(filter) => evaluate_filter(filter, zk_data, app_state).await,
            FilterNode::And(nodes) => {
                let mut results = Vec::new();
                for n in nodes {
                    if let Some(result) = evaluate_filter_node(n, zk_data, app_state).await {
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
            if attacker_match {
                Some(FilterResult {
                    matched_ship: None,
                    color: Some(Color::Green),
                    min_pilots: None,
                })
            } else if victim_match {
                Some(FilterResult {
                    matched_ship: None,
                    color: Some(Color::Red),
                    min_pilots: None,
                })
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
            if attacker_match {
                Some(FilterResult {
                    matched_ship: None,
                    color: Some(Color::Green),
                    min_pilots: None,
                })
            } else if victim_match {
                Some(FilterResult {
                    matched_ship: None,
                    color: Some(Color::Red),
                    min_pilots: None,
                })
            } else {
                None
            }
        }
        Filter::Character(character_ids) => {
            let victim_match = killmail
                .victim
                .character_id
                .is_some_and(|id| character_ids.contains(&id));
            let attacker_match = killmail
                .attackers
                .iter()
                .any(|a| a.character_id.is_some_and(|id| character_ids.contains(&id)));
            if attacker_match {
                Some(FilterResult {
                    matched_ship: None,
                    color: Some(Color::Green),
                    min_pilots: None,
                })
            } else if victim_match {
                Some(FilterResult {
                    matched_ship: None,
                    color: Some(Color::Red),
                    min_pilots: None,
                })
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
        Filter::ShipGroup(ship_group_ids_as_type_ids) => {
            // It is implicit that these "ship group IDs" are actually ship type IDs, and thus
            // must be converted to the proper ship group IDs before use.
            let mut ship_group_ids = vec![];
            for type_id in ship_group_ids_as_type_ids {
                if let Some(group_id) = get_ship_group_id(app_state, *type_id).await {
                    ship_group_ids.push(group_id);
                } else {
                    warn!("Failed to get ship group ID for type ID {}", type_id);
                }
            }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, Filter, FilterNode, System};
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
            }),
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

    #[tokio::test]
    async fn test_ship_group_filter_uses_group_id_of_subscription_type_id_list() {
        let zk_data = user_killmail_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::TotalValue {
                min: Some(5000000),
                max: None,
            }),
            FilterNode::Condition(Filter::Region(vec![10000030])),
            // A list of type IDs of which we want to match based on their group ID
            FilterNode::Condition(Filter::ShipGroup(vec![
                28352, 23919, 23757, 77283, 19722, 37604, 20183, 28850, 11567,
            ])),
            FilterNode::Condition(Filter::Security("0.0001..=0.4999".to_string())),
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

    // #[tokio::test]
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

    #[tokio::test]
    async fn test_total_value_filter() {
        let zk_data = default_zk_data();
        test_filter(
            Filter::TotalValue {
                min: Some(5_000_000),
                max: None,
            },
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::TotalValue {
                min: Some(15_000_000),
                max: None,
            },
            &zk_data,
            false,
        )
        .await;
        test_filter(
            Filter::TotalValue {
                min: None,
                max: Some(15_000_000),
            },
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::TotalValue {
                min: None,
                max: Some(5_000_000),
            },
            &zk_data,
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn test_region_filter() {
        let zk_data = default_zk_data();
        test_filter(Filter::Region(vec![10000002]), &zk_data, true).await; // The Forge
        test_filter(Filter::Region(vec![10000043]), &zk_data, false).await; // Domain
    }

    #[tokio::test]
    async fn test_system_filter() {
        let zk_data = default_zk_data();
        test_filter(Filter::System(vec![30000142]), &zk_data, true).await; // Jita
        test_filter(Filter::System(vec![31002222]), &zk_data, false).await; // Amarr
    }

    #[tokio::test]
    async fn test_security_filter() {
        let zk_data = default_zk_data(); // Jita is 0.9
        test_filter(Filter::Security("0.8..=1.0".to_string()), &zk_data, true).await;
        test_filter(Filter::Security("0.1..=0.5".to_string()), &zk_data, false).await;
    }

    #[tokio::test]
    async fn test_alliance_filter() {
        let zk_data = default_zk_data();
        test_filter(Filter::Alliance(vec![1001]), &zk_data, true).await; // Victim's alliance
        test_filter(Filter::Alliance(vec![1002]), &zk_data, true).await; // Attacker's alliance
        test_filter(Filter::Alliance(vec![9999]), &zk_data, false).await;
    }

    #[tokio::test]
    async fn test_corporation_filter() {
        let zk_data = default_zk_data();
        test_filter(Filter::Corporation(vec![101]), &zk_data, true).await; // Victim's corp
        test_filter(Filter::Corporation(vec![102]), &zk_data, true).await; // Attacker's corp
        test_filter(Filter::Corporation(vec![9999]), &zk_data, false).await;
    }

    #[tokio::test]
    async fn test_ship_type_filter() {
        let zk_data = default_zk_data();
        test_filter(Filter::ShipType(vec![587]), &zk_data, true).await; // Victim's ship (Rifter)
        test_filter(Filter::ShipType(vec![671]), &zk_data, true).await; // Attacker's ship (Catalyst)
        test_filter(Filter::ShipType(vec![17738]), &zk_data, false).await; // Golem
    }

    #[tokio::test]
    async fn test_ship_group_filter() {
        let zk_data = default_zk_data();
        // Victim is a Rifter (Frigate, group 25)
        test_filter(Filter::ShipGroup(vec![25]), &zk_data, true).await;
        // Attacker is a Catalyst (Destroyer, group 27)
        test_filter(Filter::ShipGroup(vec![27]), &zk_data, true).await;
        // Neither is a Marauder (group 419)
        test_filter(Filter::ShipGroup(vec![419]), &zk_data, false).await;
    }

    #[tokio::test]
    async fn test_is_npc_filter() {
        let mut zk_data = default_zk_data();
        zk_data.zkb.npc = true;
        test_filter(Filter::IsNpc(true), &zk_data, true).await;
        test_filter(Filter::IsNpc(false), &zk_data, false).await;
    }

    #[tokio::test]
    async fn test_pilots_filter() {
        let zk_data = default_zk_data(); // 2 pilots total
        test_filter(
            Filter::Pilots {
                min: Some(2),
                max: None,
            },
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Pilots {
                min: Some(3),
                max: None,
            },
            &zk_data,
            false,
        )
        .await;
        test_filter(
            Filter::Pilots {
                min: None,
                max: Some(2),
            },
            &zk_data,
            true,
        )
        .await;
        test_filter(
            Filter::Pilots {
                min: None,
                max: Some(1),
            },
            &zk_data,
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn test_time_range_filter() {
        let zk_data = default_zk_data(); // Time is 12:00:00
        test_filter(Filter::TimeRange { start: 11, end: 13 }, &zk_data, true).await;
        test_filter(Filter::TimeRange { start: 14, end: 16 }, &zk_data, false).await;
        // Test overnight range
        test_filter(Filter::TimeRange { start: 22, end: 4 }, &zk_data, false).await;
        let mut zk_data_night = default_zk_data();
        zk_data_night.killmail.killmail_time = "2025-07-08T23:00:00Z".to_string();
        test_filter(
            Filter::TimeRange { start: 22, end: 4 },
            &zk_data_night,
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn test_combined_and_filter_success() {
        // User's case: ShipGroup in a certain Region.
        // Test data: Rifter (Frigate, group 25) in Jita (The Forge, region 10000002)
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Region(vec![10000002])), // The Forge
            FilterNode::Condition(Filter::ShipGroup(vec![25])),    // Frigate
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_some(),
            "Combined AND filter should pass when all conditions are met"
        );
    }

    #[tokio::test]
    async fn test_combined_and_filter_fail_region() {
        // Test data: Rifter (Frigate, group 25) in Jita
        // Filter: Wrong region, correct ship group
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Region(vec![10000043])), // Domain (Wrong)
            FilterNode::Condition(Filter::ShipGroup(vec![25])),    // Frigate (Correct)
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_none(),
            "Combined AND filter should fail when region is wrong"
        );
    }

    #[tokio::test]
    async fn test_combined_and_filter_fail_shipgroup() {
        // Test data: Rifter (Frigate, group 25) in Jita
        // Filter: Correct region, wrong ship group
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::And(vec![
            FilterNode::Condition(Filter::Region(vec![10000002])), // The Forge (Correct)
            FilterNode::Condition(Filter::ShipGroup(vec![419])),   // Marauder (Wrong)
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_none(),
            "Combined AND filter should fail when ship group is wrong"
        );
    }

    #[tokio::test]
    async fn test_combined_or_filter_success() {
        // Test data: Rifter (Frigate, group 25) in Jita
        // Filter: Wrong region OR correct ship group
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        let filter_node = FilterNode::Or(vec![
            FilterNode::Condition(Filter::Region(vec![10000043])), // Domain (Wrong)
            FilterNode::Condition(Filter::ShipGroup(vec![25])),    // Frigate (Correct)
        ]);

        let result = evaluate_filter_node(&filter_node, &zk_data, &app_state).await;
        assert!(
            result.is_some(),
            "Combined OR filter should pass when one condition is met"
        );
    }

    #[tokio::test]
    async fn test_not_filter() {
        let zk_data = default_zk_data();
        let app_state = mock_app_state();

        // This filter should pass on its own
        let inner_filter = FilterNode::Condition(Filter::System(vec![30000142]));
        // So the NOT filter should fail
        let not_filter = FilterNode::Not(Box::new(inner_filter));

        let result = evaluate_filter_node(&not_filter, &zk_data, &app_state).await;
        assert!(
            result.is_none(),
            "NOT filter should fail when inner condition passes"
        );

        // This filter should fail on its own
        let inner_filter_fail = FilterNode::Condition(Filter::System(vec![999]));
        // So the NOT filter should pass
        let not_filter_pass = FilterNode::Not(Box::new(inner_filter_fail));

        let result_pass = evaluate_filter_node(&not_filter_pass, &zk_data, &app_state).await;
        assert!(
            result_pass.is_some(),
            "NOT filter should pass when inner condition fails"
        );
    }
}
