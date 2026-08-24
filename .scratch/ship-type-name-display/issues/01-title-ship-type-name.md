# 01 — Title shows the Ship Type name for Type-Tracked Ships

**What to build:** When a subscription containing a ship-type filter fires, the embed title names the matched hull by its Ship Type ("1x `Sidewinder` killed a `X`") instead of its Ship Group ("1x `CovOps`"), and the count is scoped to that exact type — only Sidewinders counted, never the whole group. Subscriptions without ship-type filters keep today's title byte-for-byte. This slice also introduces the shared rule that decides whether an attacker is a Type-Tracked Ship: a walk over the matched subscription's filter tree collecting the union of its ship-type ID lists, checked against the attacker's flown hull (never the weapon). Ship Type names resolve through the existing generic ID-to-name cache with ESI fallback, rendered singular regardless of count. See `.scratch/ship-type-name-display/spec.md` and the "Killfeed language" glossary section for vocabulary.

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] A fixture kill matched by a subscription whose ship-type filter lists the attacker's hull renders a title with the Ship Type name and a count of only attackers of that exact type.
- [x] A ship covered by both a ship-type and a ship-group condition in the same subscription titles by Ship Type (type wins).
- [x] An attacker matched only via weapon type ID is not treated as a Type-Tracked Ship for display.
- [x] A group-only subscription's title is unchanged (regression assertion at the public embed-builder seam).
- [x] New non-ignored test(s) at the public embed-builder seam with pre-seeded caches — no network; minimal coverage only.
- [x] The filter engine, match results, and veto logic are untouched.

## Comments

Implemented the shared Type-Tracked Ship rule as `FilterNode::ship_type_ids(&self) -> HashSet<u32>` (`src/config.rs`), a tree walk modeled on `contains_ship_filter()` that collects the union of all `ShipType` ID lists in a filter tree (backed by a private `collect_ship_type_ids` helper). Ticket 02 can call this same method to build its tally-priority rule.

`build_killmail_embed` (`src/discord_bot.rs`) now computes `subscription.root_filter.ship_type_ids()` once and, when the title's matched entity is Green and its `type_id` (always the flown hull — `MatchedEntity::type_id` only falls back to the weapon when `ship_type_id` is absent) is in that set, titles by the Ship Type name via the existing `get_name` cache/ESI path, with the count scoped to attackers whose `ship_type_id` (never `weapon_type_id`) equals that exact type. Otherwise the original Ship Group branch (`contains_ship_filter()` + `get_dynamic_group_name`) runs unchanged, so group-only and non-ship subscriptions are byte-for-byte identical to before.

Added `tests/test_ship_type_display.rs` (`mod common;`), plus a new `create_app_state_with_seeded_caches` helper in `tests/common/mod.rs` that builds an `AppState` directly (no `load_app_config`, no `.env`, no network) with pre-seeded `systems`/`ships`/`names`/`tickers`/`group_names` caches and a pre-populated `celestial_cache` entry, using the `132467594_dictor_af_tie.json` fixture (2x Retribution/AF, Sabre+Heretic/Dictor). Four tests, all passing: `type_tracked_ship_titles_by_type_name_with_scoped_count`, `type_wins_over_group_when_both_conditions_cover_same_ship`, `weapon_only_match_is_not_type_tracked`, `group_only_subscription_title_is_unchanged`. Verified TDD red state by temporarily stashing the `src/config.rs`/`src/discord_bot.rs` changes: the two type-tracking assertions failed for the expected reason (title still showed the group name) before the implementation, and passed after.

`src/processor.rs` was not touched. `cargo check`, `cargo clippy --all-targets -- -W clippy::all` (no new warnings from this change), and the full non-ignored suite pass except `tests/test_contract_intelligence.rs`, which fails in this environment only because `CONTRACT_TEST_DATABASE_URL` has no reachable Postgres instance — pre-existing and unrelated to this ticket (confirmed via `git diff` that file's only change predates this session).

### Review fix: gate was keying off `MatchedEntity::type_id`, which can be a weapon ID

Code review (after ticket 02 landed on top) caught that the paragraph above was wrong in one respect: `MatchedEntity::type_id` is `attacker.ship_type_id.or(attacker.weapon_type_id)`, so when an attacker has `ship_type_id: None` (legal per `models.rs`) and matched the subscription's `ShipType` filter only via `weapon_type_id`, `matched.type_id` *is* the weapon ID, not a hull. The old gate `ship_type_ids.contains(&matched.type_id)` would then pass, the hull-scoped count (`a.ship_type_id == Some(tracked_type)`) would correctly be 0, and `count.max(1)` coerced that to a title reading `1x <WeaponName> killed a ...` — spec-forbidden ("the display rule keys only off the flown ship's type ID").

Fixed at the root: added `MatchedEntity::hull_type_id: Option<u32>`, populated as `attacker.ship_type_id` directly (i.e. `None` when the attacker has no flown hull) in `select_best_entity_for_display`, and `Some(victim.ship_type_id)` for the victim branch (victims always have a hull). The Type-Tracked title gate in `build_killmail_embed` now reads `matched.hull_type_id.is_some_and(|hull| ship_type_ids.contains(&hull))`, and `tracked_type` is taken from `hull_type_id`. `type_id` itself is untouched and still used for the icon/name fallback elsewhere. Confirmed `compute_fleet_composition` (ticket 02's tally path) was never affected by this bug: it only enters its tally branch `if let Some(ship_id) = attacker.ship_type_id`, so it already keys off the hull exclusively.

Added one regression test, `attacker_with_no_hull_matched_only_via_weapon_is_not_type_tracked` (`tests/test_ship_type_display.rs`): mutates the fixture's Heretic attacker in memory (`ship_type_id: None`, `weapon_type_id: Some(999_999)`, a synthetic ID absent elsewhere in the fixture so it's the only matched attacker, keeping `best_match` deterministic) and asserts the title falls back to the Ship Group branch (`"1x \`Destroyers\` killed a \`Squall\`"`) rather than leaking the weapon's name. Verified this test fails for the right reason against the old gate (reproduced the exact bug: title read `` "1x `Fake Weapon Item` killed a `Squall`" ``) before re-applying the fix.

Also promoted the inline closure in `select_top_entries` (ticket 02) to `TallyKey::sort_id()` (returns a Ship Type's `type_id` or a Ship Group's `group_id`, symmetric with the existing `TallyKey::parent_group()`), and ran `cargo fmt` on the touched files only (`src/discord_bot.rs`, `src/config.rs`, `tests/common/mod.rs`, `tests/test_ship_type_display.rs`).

Re-verified after the fix: `cargo check` clean; `cargo test --test test_ship_type_display` 7/7 passing; `cargo test --lib` 96 passed/0 failed/1 ignored; `cargo clippy --all-targets -- -W clippy::all` 208 warnings total, none newly introduced (checked line-by-line against every changed range) — same count as before this review round. Full `cargo test --no-fail-fast` passes except the same pre-existing, unrelated `test_contract_intelligence.rs` Postgres-dependent failures.
