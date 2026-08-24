//! Integration tests for Ship Type name display on Type-Tracked Ships (ticket 01).
//!
//! Fixture `132467594_dictor_af_tie.json` has two Retributions (ship_type_id 11393,
//! group 324 "AF") and two different Dictor hulls, Sabre (22456) and Heretic (22452),
//! both group 541 "Dictor". This gives a matched hull that repeats (Retribution x2,
//! useful for scoped-count assertions) and a matched group with mixed hulls (Dictor,
//! useful for the group-only regression check).
//!
//! Runs under plain `cargo test`: no network, no `.env`, no Discord. All ESI-backed
//! lookups the embed builder would otherwise perform are pre-seeded from the real
//! `config/*.json` caches so nothing falls through to the network.

mod common;

use common::*;
use killbot_rust::config::{
    Action, Filter, FilterNode, Subscription, System, Target, TargetableCondition, TargetedFilter,
};
use killbot_rust::discord_bot::build_killmail_embed;
use killbot_rust::esi::Celestial;
use killbot_rust::processor::process_killmail;
use std::collections::HashMap;
use std::sync::Arc;

const FIXTURE: &str = "132467594_dictor_af_tie.json";
const SOLAR_SYSTEM_ID: u32 = 30004371;

fn seeded_systems() -> HashMap<u32, System> {
    HashMap::from([(
        SOLAR_SYSTEM_ID,
        System {
            id: SOLAR_SYSTEM_ID,
            name: "BWI1-9".to_string(),
            security_status: -0.2374393045902252,
            region_id: 10000055,
            region: "Branch".to_string(),
            // Coordinates are irrelevant here: the celestial lookup is bypassed by
            // pre-seeding `celestial_cache` directly (see `build_title`).
            x: 0.0,
            y: 0.0,
            z: 0.0,
        },
    )])
}

/// Ship (hull) and weapon type IDs to group IDs, covering every attacker's
/// `ship_type_id` and `weapon_type_id` in the fixture so `ShipGroup` filter
/// evaluation never falls through to ESI.
fn seeded_ships() -> HashMap<u32, u32> {
    HashMap::from([
        (11393, 324),  // Retribution -> AF
        (22456, 541),  // Sabre -> Dictor
        (209, 385),    // Scourge Heavy Missile (fixture attacker's ship_type_id)
        (37483, 1534), // Magus -> Cmd Dessie
        (11186, 831),  // Malediction -> Ceptor
        (22452, 541),  // Heretic -> Dictor
        (3033, 53),    // Small Focused Beam Laser II (weapon)
        (21638, 100),  // weapon
        (448, 52),     // Warp Scrambler II (weapon)
    ])
}

fn seeded_names() -> HashMap<u64, String> {
    HashMap::from([
        (81008, "Squall".to_string()),
        (696861884, "HigherPower".to_string()),
        (11393, "Retribution".to_string()),
        (22456, "Sabre".to_string()),
        (22452, "Heretic".to_string()),
        (11186, "Malediction".to_string()),
    ])
}

fn seeded_tickers() -> HashMap<u64, String> {
    HashMap::from([
        (99009927, "BIGAB".to_string()),
        (99003581, "FRT".to_string()),
    ])
}

fn seeded_group_names() -> HashMap<u32, String> {
    // Group 385 ("Heavy Missile") isn't in the hardcoded GROUP_NAMES table.
    HashMap::from([(385, "Heavy Missile".to_string())])
}

fn subscription(id: &str, root_filter: FilterNode) -> Subscription {
    Subscription {
        id: id.to_string(),
        description: id.to_string(),
        root_filter,
        action: Action {
            channel_id: TEST_CHANNEL_ID.to_string(),
            ping_type: None,
        },
    }
}

fn ship_type_condition(ids: Vec<u32>) -> FilterNode {
    FilterNode::Condition(Filter::Targeted(TargetedFilter {
        condition: TargetableCondition::ShipType(ids),
        target: Target::Attacker,
    }))
}

fn ship_group_condition(ids: Vec<u32>) -> FilterNode {
    FilterNode::Condition(Filter::Targeted(TargetedFilter {
        condition: TargetableCondition::ShipGroup(ids),
        target: Target::Attacker,
    }))
}

/// Builds the embed for `subscription` against the fixture killmail with fully
/// pre-seeded caches (no network).
async fn build_embed(subscription: Subscription) -> serenity::builder::CreateEmbed {
    let zk_data = load_fixture(FIXTURE);
    let app_state = create_app_state_with_seeded_caches(
        vec![subscription],
        seeded_systems(),
        seeded_ships(),
        seeded_names(),
        seeded_tickers(),
        seeded_group_names(),
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
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one matched subscription, got {matches:?}"
    );
    let (_, matched_subscription, named_filter_result) = &matches[0];

    build_killmail_embed(
        &app_state,
        &zk_data,
        named_filter_result,
        matched_subscription,
    )
    .await
}

/// Builds the embed for `subscription` against the fixture killmail and returns the title.
async fn build_title(subscription: Subscription) -> String {
    let embed = build_embed(subscription).await;
    embed.0["title"]
        .as_str()
        .expect("embed title is a string")
        .to_string()
}

/// Returns the "... Attackers Involved" field's value text.
fn attackers_field_text(embed: &serenity::builder::CreateEmbed) -> String {
    let fields = embed.0["fields"].as_array().expect("embed fields");
    let attackers_field = fields
        .iter()
        .find(|f| {
            f["name"]
                .as_str()
                .is_some_and(|name| name.ends_with("Attackers Involved"))
        })
        .expect("attackers field present");
    attackers_field["value"]
        .as_str()
        .expect("attackers field value is a string")
        .to_string()
}

/// A ShipType filter listing the Retribution hull (11393) titles by Ship Type name
/// with a count scoped to that exact hull (2 Retributions), not the wider AF group
/// (which would also include no other hulls here, but must not use the group name).
#[tokio::test]
async fn type_tracked_ship_titles_by_type_name_with_scoped_count() {
    init_tracing();
    let sub = subscription("retribution-type-track", ship_type_condition(vec![11393]));
    let title = build_title(sub).await;
    assert_eq!(title, "2x `Retribution` killed a `Squall`");
}

/// A subscription whose filter tree has both a ShipType and a ShipGroup condition
/// covering the same ships (Retribution matches ShipType(11393) and ShipGroup(324))
/// must title by Ship Type ("Retribution"), not Ship Group ("AF"/"AFs").
#[tokio::test]
async fn type_wins_over_group_when_both_conditions_cover_same_ship() {
    init_tracing();
    let sub = subscription(
        "type-and-group",
        FilterNode::Or(vec![
            ship_type_condition(vec![11393]),
            ship_group_condition(vec![324]),
        ]),
    );
    let title = build_title(sub).await;
    assert_eq!(title, "2x `Retribution` killed a `Squall`");
}

/// A ShipType filter listing a weapon's type ID (Small Focused Beam Laser II, 3033)
/// matches an attacker only via `weapon_type_id`, never `ship_type_id`. That attacker
/// must not be treated as a Type-Tracked Ship: the title falls back to today's
/// Ship Group logic (AF group, both Retributions counted) instead of showing the
/// weapon's name.
#[tokio::test]
async fn weapon_only_match_is_not_type_tracked() {
    init_tracing();
    let sub = subscription("weapon-only-match", ship_type_condition(vec![3033]));
    let title = build_title(sub).await;
    assert_eq!(title, "2x `AFs` killed a `Squall`");
}

/// Regression: a subscription with only a ShipGroup condition (no ShipType filter
/// anywhere in the tree) renders its title exactly as before this feature existed.
#[tokio::test]
async fn group_only_subscription_title_is_unchanged() {
    init_tracing();
    let sub = subscription("dictor-group-only", ship_group_condition(vec![541]));
    let title = build_title(sub).await;
    assert_eq!(title, "2x `Dictors` killed a `Squall`");
}

/// Fleet Composition Tally (ticket 02): a ShipType filter tracking the Malediction
/// (11186, parent group 831 "Ceptor", count 1 on this kill) must claim a named Tally
/// Entry ahead of the count-based group selection. Without type tracking, the two
/// named subcaps slots would go to the two largest groups by count (Dictor 541 and
/// AF 324, both count 2). With Malediction's guaranteed slot, only one group slot
/// remains: Dictor (higher GROUP_NAMES priority on the count tie) keeps its slot, and
/// AF - despite being just as large as Dictor - is displaced into the "+N" overflow.
/// Counts sum to all 7 attackers with no double counting, and the per-affiliation
/// breakdown line applies the identical rule.
#[tokio::test]
async fn mixed_tally_type_and_group_entries_side_by_side_with_slot_priority() {
    init_tracing();
    let sub = subscription("malediction-type-track", ship_type_condition(vec![11186]));
    let embed = build_embed(sub).await;
    let attackers = attackers_field_text(&embed);
    assert_eq!(
        attackers,
        "2x Dictors, 1x Malediction, +4\n```\n[BIGAB] 5\n \u{2514} 2 Dictors, 1 Malediction, +2\nothers 2```"
    );
}

/// Bug regression: an attacker with no flown hull (`ship_type_id: None`) that matched
/// the subscription's ShipType filter only via `weapon_type_id` must never be treated
/// as a Type-Tracked Ship for display. `MatchedEntity::type_id` falls back to the
/// weapon type ID when there's no hull, so the title gate must key off
/// `MatchedEntity::hull_type_id`, never `type_id`, or a weapon's name leaks into the
/// title. Mutates the Heretic attacker (originally `ship_type_id` 22452) in memory to
/// strip its hull and give it a synthetic weapon type ID absent everywhere else in the
/// fixture, so it is the only attacker the subscription matches (a deterministic
/// `best_match`, avoiding ties on `SHIP_GROUP_PRIORITY`).
#[tokio::test]
async fn attacker_with_no_hull_matched_only_via_weapon_is_not_type_tracked() {
    init_tracing();
    const FAKE_WEAPON_TYPE_ID: u32 = 999_999;

    let mut zk_data = load_fixture(FIXTURE);
    let heretic = zk_data
        .killmail
        .attackers
        .iter_mut()
        .find(|a| a.ship_type_id == Some(22452))
        .expect("fixture has the Heretic attacker (ship_type_id 22452)");
    heretic.ship_type_id = None;
    heretic.weapon_type_id = Some(FAKE_WEAPON_TYPE_ID);

    let mut ships = seeded_ships();
    ships.insert(FAKE_WEAPON_TYPE_ID, 420); // Destroyer group; unused elsewhere in the fixture
    let mut names = seeded_names();
    names.insert(FAKE_WEAPON_TYPE_ID as u64, "Fake Weapon Item".to_string());

    let sub = subscription(
        "hull-less-weapon-match",
        ship_type_condition(vec![FAKE_WEAPON_TYPE_ID]),
    );
    let app_state = create_app_state_with_seeded_caches(
        vec![sub],
        seeded_systems(),
        ships,
        names,
        seeded_tickers(),
        seeded_group_names(),
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
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one matched subscription, got {matches:?}"
    );
    let (_, matched_subscription, named_filter_result) = &matches[0];
    assert_eq!(
        named_filter_result.filter_result.matched_attackers.len(),
        1,
        "expected exactly one matched attacker (the hull-less one), for a deterministic best_match"
    );

    let embed = build_killmail_embed(
        &app_state,
        &zk_data,
        named_filter_result,
        matched_subscription,
    )
    .await;
    let title = embed.0["title"].as_str().expect("embed title is a string");

    // Falls back to the unchanged Ship Group branch: no real hull on this kill maps to
    // group 420 "Destroyer", so the group's raw count is 0 (plural name, existing
    // behavior predating this feature), displayed count floored to 1x. Never the
    // weapon's name.
    assert_eq!(title, "1x `Destroyers` killed a `Squall`");
    assert!(!title.contains("Fake Weapon Item"));
}

/// Regression (ticket 02): a group-only subscription's Fleet Composition Tally -
/// both the overall category line and the per-affiliation breakdown - is byte-identical
/// to the pre-existing group-only tally. Type entries gain no priority here because the
/// subscription's ship-type set is empty.
#[tokio::test]
async fn group_only_subscription_attackers_field_is_unchanged() {
    init_tracing();
    let sub = subscription("dictor-group-only", ship_group_condition(vec![541]));
    let embed = build_embed(sub).await;
    let attackers = attackers_field_text(&embed);
    assert_eq!(
        attackers,
        "2x Dictors, 2x AFs, +3\n```\n[BIGAB] 5\n \u{2514} 2 Dictors, 1 Cmd Dessie, +2\nothers 2```"
    );
}
