//! Integration tests for Tracking mode embeds.
//!
//! Run with:
//! ```
//! cargo test --test test_tracking_embeds -- --ignored --nocapture
//! ```
//!
//! Requires:
//! - .env file with DISCORD_BOT_TOKEN

mod common;

use common::*;
use killbot_rust::config::{
    Action, Filter, FilterNode, SimpleFilter, Subscription, SystemRange, Target,
    TargetableCondition, TargetedFilter,
};
use killbot_rust::discord_bot::send_killmail_message;
use killbot_rust::processor::process_killmail;
use serenity::http::Http;
use std::sync::Arc;

/// Test fixtures for tracking mode
const TRACKING_FIXTURES: &[(&str, &str)] = &[
    ("132235921_supers_involved.json", "Naglfar FI killed by fleet with Ragnarok"),
    ("131501165_drill_kill.json", "Metenox drill killed in lowsec"),
    ("132302304_caps_attacking.json", "Revelation killed by 4 Dreads + BS fleet"),
    ("131126432_keepstar_kill.json", "Keepstar kill with 3753 attackers"),
];

/// Create tracking-focused test subscriptions
fn create_tracking_subscriptions() -> Vec<Subscription> {
    vec![
        // Capital ships as attackers - GREEN highlight
        Subscription {
            id: "caps-tracking".to_string(),
            description: "capitals within 8ly of Turnur".to_string(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::ShipGroup(CAPITAL_GROUPS.to_vec()),
                    target: Target::Attacker,
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::LyRangeFrom(vec![
                    SystemRange {
                        system_id: 30002086,
                        range: 8.0,
                    },
                ]))),
            ]),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
        // Supercapital ships as attackers - GREEN highlight
        Subscription {
            id: "supercaps-tracking".to_string(),
            description: "supercaps within 8ly of Turnur".to_string(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::ShipGroup(SUPERCAP_GROUPS.to_vec()),
                    target: Target::Attacker,
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::LyRangeFrom(vec![
                    SystemRange {
                        system_id: 30002086,
                        range: 8.0,
                    },
                ]))),
            ]),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
        // Lowsec drills (Metenox) - RED highlight (drill is victim)
        Subscription {
            id: "lowsec-drills".to_string(),
            description: "lowsec drill kills".to_string(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::ShipType(vec![METENOX_DRILL]),
                    target: Target::Victim,
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::Security(
                    "0.0001..=0.4999".to_string(),
                ))),
            ]),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
        // Nullsec structures - RED highlight (structure is victim)
        Subscription {
            id: "nullsec-structures".to_string(),
            description: "nullsec structure kills".to_string(),
            root_filter: FilterNode::And(vec![
                FilterNode::Condition(Filter::Targeted(TargetedFilter {
                    condition: TargetableCondition::ShipGroup(STRUCTURE_GROUPS.to_vec()),
                    target: Target::Victim,
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::TotalValue {
                    min: Some(5_000_000),
                    max: None,
                })),
                FilterNode::Condition(Filter::Simple(SimpleFilter::Security(
                    "-1.0..=0.0".to_string(),
                ))),
            ]),
            action: Action {
                channel_id: TEST_CHANNEL_ID.to_string(),
                ping_type: None,
            },
        },
    ]
}

#[tokio::test]
#[ignore]
async fn send_tracking_embeds() {
    init_tracing();
    dotenvy::dotenv().ok();

    println!("=== Integration Test: Tracking Embeds ===");
    println!("Channel ID: {}", TEST_CHANNEL_ID);
    println!();

    let subscriptions = create_tracking_subscriptions();
    println!("Subscriptions:");
    for sub in &subscriptions {
        println!("  - {}: {}", sub.id, sub.description);
    }
    println!();

    let app_state = create_app_state_with_subscriptions(subscriptions).await;
    let http = Arc::new(Http::new(&app_state.app_config.discord_bot_token));

    let mut total_embeds = 0;

    for (fixture, description) in TRACKING_FIXTURES {
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
        for (_guild_id, subscription, filter_result) in &matched {
            println!(
                "    - {} (attackers: {}, victim: {})",
                subscription.id,
                filter_result.filter_result.matched_attackers.len(),
                filter_result.filter_result.matched_victim
            );
        }

        for (_guild_id, subscription, filter_result) in matched {
            println!("  Sending '{}'...", subscription.id);

            match send_killmail_message(&http, &app_state, &subscription, &zk_data, filter_result)
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
    println!("=== Test complete: {} tracking embeds sent ===", total_embeds);
    println!("{}", "=".repeat(70));
}
