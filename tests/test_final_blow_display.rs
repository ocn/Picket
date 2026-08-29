mod common;

use common::*;
use killbot_rust::config::{
    Action, Filter, FilterNode, Subscription, System, Target, TargetableCondition, TargetedFilter,
};
use killbot_rust::discord_bot::build_killmail_embed;
use killbot_rust::esi::Celestial;
use killbot_rust::models::ZkData;
use killbot_rust::processor::process_killmail;
use serenity::builder::CreateEmbed;
use std::collections::HashMap;
use std::sync::Arc;

const FIXTURE: &str = "132467594_dictor_af_tie.json";
const SOLAR_SYSTEM_ID: u32 = 30004371;

fn subscription() -> Subscription {
    Subscription {
        id: "final-blow-display".to_string(),
        description: "final-blow-display".to_string(),
        root_filter: FilterNode::Condition(Filter::Targeted(TargetedFilter {
            condition: TargetableCondition::ShipType(vec![11393]),
            target: Target::Attacker,
        })),
        action: Action {
            channel_id: TEST_CHANNEL_ID.to_string(),
            ping_type: None,
        },
    }
}

async fn build_embed(zk_data: ZkData) -> CreateEmbed {
    let subscription = subscription();
    let app_state = create_app_state_with_seeded_caches(
        vec![subscription],
        HashMap::from([(
            SOLAR_SYSTEM_ID,
            System {
                id: SOLAR_SYSTEM_ID,
                name: "BWI1-9".to_string(),
                security_status: -0.2374393045902252,
                region_id: 10000055,
                region: "Branch".to_string(),
                x: 0.0,
                y: 0.0,
                z: 0.0,
            },
        )]),
        HashMap::from([
            (11393, 324),
            (22456, 541),
            (209, 385),
            (37483, 1534),
            (11186, 831),
            (22452, 541),
            (3033, 53),
            (21638, 100),
            (448, 52),
        ]),
        HashMap::from([
            (81008, "Squall".to_string()),
            (696861884, "HigherPower".to_string()),
            (2116470606, "Final Pilot".to_string()),
            (11393, "Retribution".to_string()),
            (22456, "Sabre".to_string()),
            (22452, "Heretic".to_string()),
            (11186, "Malediction".to_string()),
        ]),
        HashMap::from([
            (99009927, "BIGAB".to_string()),
            (99003581, "FRT".to_string()),
            (98798876, "FLEET".to_string()),
        ]),
        HashMap::from([(385, "Heavy Missile".to_string())]),
    );
    app_state
        .celestial_cache
        .insert(
            SOLAR_SYSTEM_ID,
            Arc::new(Celestial {
                item_id: 1,
                type_id: 1,
                item_name: "Test Celestial".to_string(),
                distance: 1_000_000.0,
            }),
        )
        .await;

    let matches = process_killmail(&app_state, &zk_data).await;
    assert_eq!(matches.len(), 1);
    let (_, matched_subscription, named_filter_result) = &matches[0];
    build_killmail_embed(
        &app_state,
        &zk_data,
        named_filter_result,
        matched_subscription,
    )
    .await
}

fn fields(embed: &CreateEmbed) -> &[serde_json::Value] {
    embed.0["fields"].as_array().expect("embed fields")
}

#[tokio::test]
async fn credited_character_renders_beside_victim_with_linked_participants() {
    let embed = build_embed(load_fixture(FIXTURE)).await;
    let fields = fields(&embed);
    let victim_index = fields
        .iter()
        .position(|field| field["name"] == "Victim")
        .expect("Victim field");

    assert_eq!(
        fields[victim_index]["value"],
        "[[FRT]](https://zkillboard.com/alliance/99003581/) [HigherPower](https://zkillboard.com/character/696861884/)"
    );
    assert_eq!(fields[victim_index]["inline"], true);
    assert_eq!(fields[victim_index + 1]["name"], "Final Blow");
    assert_eq!(
        fields[victim_index + 1]["value"],
        "[[BIGAB]](https://zkillboard.com/alliance/99009927/) [Final Pilot](https://zkillboard.com/character/2116470606/)"
    );
    assert_eq!(fields[victim_index + 1]["inline"], true);
}

#[tokio::test]
async fn credited_attacker_without_character_renders_affiliation_and_ship_type() {
    let mut zk_data = load_fixture(FIXTURE);
    zk_data.killmail.attackers[0].character_id = None;
    zk_data.killmail.attackers[0].alliance_id = None;

    let embed = build_embed(zk_data).await;
    let final_blow_field = fields(&embed)
        .iter()
        .find(|field| field["name"] == "Final Blow")
        .expect("Final Blow field");

    assert_eq!(
        final_blow_field["value"],
        "[[FLEET]](https://zkillboard.com/corporation/98798876/) Retribution"
    );
    assert_eq!(final_blow_field["inline"], true);
}

#[tokio::test]
async fn marked_attacker_cardinality_omits_zero_and_uses_first_of_multiple() {
    let mut no_marked_attacker = load_fixture(FIXTURE);
    for attacker in &mut no_marked_attacker.killmail.attackers {
        attacker.final_blow = false;
    }

    let embed = build_embed(no_marked_attacker).await;
    let embed_fields = fields(&embed);
    assert!(embed_fields
        .iter()
        .all(|field| field["name"] != "Final Blow"));
    let victim = embed_fields
        .iter()
        .find(|field| field["name"] == "Victim")
        .expect("Victim field");
    assert_eq!(victim["inline"], false);

    let mut multiple_marked_attackers = load_fixture(FIXTURE);
    multiple_marked_attackers.killmail.attackers[1].final_blow = true;

    let embed = build_embed(multiple_marked_attackers).await;
    let final_blow_field = fields(&embed)
        .iter()
        .find(|field| field["name"] == "Final Blow")
        .expect("Final Blow field");
    assert_eq!(
        final_blow_field["value"],
        "[[BIGAB]](https://zkillboard.com/alliance/99009927/) [Final Pilot](https://zkillboard.com/character/2116470606/)"
    );
}
