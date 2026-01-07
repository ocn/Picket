//! Integration tests for Killfeed mode embeds.
//!
//! Simulates a #killfeed style channel where an alliance tracks their members' kills and losses:
//! - Green = alliance member killed someone (attacker match)
//! - Red = alliance member died (victim match)
//!
//! Run with:
//! ```
//! cargo test --test test_killfeed_embeds -- --ignored --nocapture
//! ```
//!
//! Requires:
//! - .env file with DISCORD_BOT_TOKEN

mod common;

use common::*;
use killbot_rust::config::{
    Action, Filter, FilterNode, SimpleFilter, Subscription, Target, TargetableCondition,
    TargetedFilter,
};
use killbot_rust::discord_bot::send_killmail_message;
use killbot_rust::processor::process_killmail;
use serenity::http::Http;
use std::sync::Arc;

/// Test fixtures for killfeed mode
const KILLFEED_FIXTURES: &[(&str, &str)] = &[
    ("132461133_ceptor_npc_test.json", "Ceptor + NPC attacker - tests unknown ship groups"),
    ("132235921_supers_involved.json", "Naglfar FI - high value kill"),
    ("131501165_drill_kill.json", "Metenox drill - 1B+ ISK"),
    ("132249213_naglfar_loss.json", "Naglfar loss - dread kill"),
    ("132302304_caps_attacking.json", "Revelation - cap fight"),
    ("131126432_keepstar_kill.json", "Keepstar - massive battle"),
    ("130734446_unknown_group_test.json", "Revelation killed by Infested Carrier - tests ESI group name lookup"),
    ("132462203_global_feed_test.json", "Astero killed by Astrahus - tests global feed (no entity match)"),
    ("132467594_dictor_af_tie.json", "Squall killed by 2x Dictors + 2x AFs - tests tie-breaking by GROUP_NAMES priority"),
];

/// Deepwater Hooligans alliance ID
const BIGAB_ALLIANCE_ID: u64 = 99009927;

/// Create killfeed-focused test subscriptions
/// Mimics a typical #killfeed channel: alliance kills/losses with value filter
fn create_killfeed_subscriptions() -> Vec<Subscription> {
    vec![
        // BIGAB killfeed: alliance member kills OR losses, value >= 500M ISK
        Subscription {
            id: "bigab-killfeed".to_string(),
            description: "BIGAB kills & losses >500M".to_string(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Simple(SimpleFilter::TotalValue {
                    min: Some(500_000_000),
                    max: None,
                })),
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::Alliance(vec![BIGAB_ALLIANCE_ID]),
                    target: Target::Any, // Match as attacker OR victim
                })),
            ]),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
        // BIGAB killfeed: all kills (no value filter) - for testing low-value kills
        Subscription {
            id: "bigab-all".to_string(),
            description: "BIGAB all kills & losses".to_string(),
            root_filter: FilterNode::Condition(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::Alliance(vec![BIGAB_ALLIANCE_ID]),
                target: Target::Any,
            })),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
        // Dread tracking: for testing unknown attacker group names (ESI lookup)
        Subscription {
            id: "dread-deaths".to_string(),
            description: "Dread deaths - tests ESI group name lookup".to_string(),
            root_filter: FilterNode::Condition(Filter::Targeted(TargetedFilter {
                condition: TargetableCondition::ShipGroup(vec![485]), // Dreadnought
                target: Target::Victim,
            })),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
        // Global feed: value-only filter (no entity match) - tests default green color
        Subscription {
            id: "global-100m".to_string(),
            description: "Global kills >100M ISK".to_string(),
            root_filter: FilterNode::Condition(Filter::Simple(SimpleFilter::TotalValue {
                min: Some(100_000_000),
                max: None,
            })),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
    ]
}

#[tokio::test]
#[ignore]
async fn send_killfeed_embeds() {
    init_tracing();
    dotenvy::dotenv().ok();

    println!("=== Integration Test: Killfeed Embeds ===");
    println!("Channel ID: {}", TEST_CHANNEL_ID);
    println!();

    let subscriptions = create_killfeed_subscriptions();
    println!("Subscriptions:");
    for sub in &subscriptions {
        println!("  - {}: {}", sub.id, sub.description);
    }
    println!();

    let app_state = create_app_state_with_subscriptions(subscriptions).await;
    let http = Arc::new(Http::new(&app_state.app_config.discord_bot_token));

    let mut total_embeds = 0;

    for (fixture, description) in KILLFEED_FIXTURES {
        println!("{}", "=".repeat(70));
        println!("Fixture: {} - {}", fixture, description);
        println!("{}", "=".repeat(70));

        let zk_data = load_fixture(fixture);
        println!(
            "Kill ID: {}, Attackers: {}, Value: {:.0}M ISK",
            zk_data.kill_id,
            zk_data.killmail.attackers.len(),
            zk_data.zkb.total_value / 1_000_000.0
        );

        let matched = process_killmail(&app_state, &zk_data).await;

        if matched.is_empty() {
            println!("  NO MATCHES - skipping\n");
            continue;
        }

        println!("  Matched {} subscription(s):", matched.len());
        for (_guild_id, subscription, named_result) in &matched {
            let fr = &named_result.filter_result;
            // Green = attacker match (kill), Red = victim match (loss)
            let is_kill = !fr.matched_attackers.is_empty();
            let kill_or_loss = if is_kill {
                "KILL (green)"
            } else {
                "LOSS (red)"
            };
            println!(
                "    - {} [{}] (attackers: {}, victim: {})",
                subscription.id,
                kill_or_loss,
                fr.matched_attackers.len(),
                fr.matched_victim
            );
        }

        // Only send one embed per fixture to avoid spam
        if let Some((_guild_id, subscription, named_result)) = matched.into_iter().next() {
            let is_kill = !named_result.filter_result.matched_attackers.is_empty();
            let kill_or_loss = if is_kill { "KILL" } else { "LOSS" };
            println!("  Sending '{}' as {}...", subscription.id, kill_or_loss);

            match send_killmail_message(&http, &app_state, &subscription, &zk_data, named_result)
                .await
            {
                Ok(_) => {
                    println!("    OK");
                    total_embeds += 1;
                }
                Err(e) => eprintln!("    FAILED: {:?}", e),
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        }
        println!();
    }

    println!("{}", "=".repeat(70));
    println!("=== Test complete: {} killfeed embeds sent ===", total_embeds);
    println!("{}", "=".repeat(70));
}
