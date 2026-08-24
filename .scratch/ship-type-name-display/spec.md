# Spec: Ship Type name display for Type-Tracked Ships

Status: ready-for-agent

## Problem Statement

Server operators run killfeed channels whose subscriptions track specific Ship Types (concrete hulls identified by EVE type IDs, e.g. Alliance Tournament hulls like the Sidewinder), not just Ship Groups. When such a subscription fires, the embed still names the ship by its Ship Group ("1x CovOps"), which hides the very information the feed exists to surface — *which* rare hull was seen. Worse, in the Fleet Composition Tally the tracked hull can lose the named-slot contest to larger blobs on the kill and disappear entirely into the "+N" overflow, so an alert about an AT frigate can render without naming the AT frigate at all.

## Solution

When a kill matches a subscription, any attacker whose Ship Type appears in that subscription's ship-type filter list is a **Type-Tracked Ship** and is displayed by its Ship Type name instead of its Ship Group name — "1x Sidewinder" rather than "1x CovOps". Type-Tracked Ships are guaranteed a named Tally Entry ahead of the normal count-based slot selection, so the tracked hull is always visible. All other ships, and all subscriptions without ship-type filters, render exactly as today. The styling and formatting of the embed does not change: same title shape, same category lines, same two-named-entries-plus-overflow structure, same breakdown prefixes.

## User Stories

1. As a killfeed channel operator tracking specific hulls, I want the embed to name the matched Ship Type ("1x Sidewinder"), so that I can see at a glance which tracked hull appeared on a kill.
2. As a killfeed channel operator, I want the embed title's count to be scoped to the named Ship Type (only Sidewinders counted when "Sidewinder" is shown), so that the number and the name never disagree.
3. As a killfeed channel operator, I want Type-Tracked Ships guaranteed a named Tally Entry ahead of count-based selection, so that the hull my subscription exists to track is never buried in the "+N" overflow behind larger blob groups.
4. As a killfeed channel operator with both ship-group and ship-type conditions in one subscription, I want the Ship Type name to win for ships covered by both, so that the more specific match is what I see.
5. As a killfeed reader, I want a mixed kill to resolve names per ship (e.g. "2x Sidewinder, 1x CovOps" when a Buzzard matched only by group rides with two tracked Sidewinders), so that each ship is labelled by how specifically it is tracked.
6. As a killfeed reader, I want Type-Tracked Ships counted apart from their Ship Group with no double counting, so that tallies still sum to the real number of attackers.
7. As a killfeed reader, I want the per-affiliation breakdown lines to use the same type-name rule as the overall Fleet Composition Tally, so that the title, the overall line, and the breakdowns never disagree about the same ship.
8. As an operator of an existing group-only killfeed (the common configuration on the main server), I want my embeds unchanged byte-for-byte, so that this feature cannot regress feeds that never mention Ship Types.
9. As an operator of a subscription with no ship conditions at all (region/value-only feeds), I want my embeds unchanged, so that type display is scoped strictly to subscriptions that opt in via ship-type filters.
10. As an operator running several subscriptions into one channel, I want each embed rendered by its own subscription's rules, so that a type-tracking subscription and a group-tracking subscription posting the same kill each show what they track.
11. As a killfeed reader, I want a Type-Tracked Ship's Tally Entry to sort at its parent Ship Group's display position, so that lines keep their familiar capitals-before-frigates ordering.
12. As a killfeed reader, I want type names rendered singular regardless of count ("2x Sidewinder"), consistent with how ESI-sourced group names already render, so that naming stays predictable without hand-curated plurals.
13. As an operator whose ship-type filter matches an attacker's weapon (fighters, structure weapons), I want display to follow the hull the attacker flies and never the weapon, so that a carrier is not relabelled as its fighter type.
14. As a killfeed reader, I want the victim's ship to keep showing its Ship Type name as it already does, so that the victim line is unaffected by this change.
15. As an operator, I want type names resolved through the existing name cache and ESI fallback, so that type display neither adds new failure modes nor extra latency beyond the lookups the embed already performs.
16. As a maintainer, I want the filter engine and its veto partitioning untouched, so that type display cannot change which kills match or alert.

## Implementation Decisions

- **Render-time rule, no filter-engine changes.** Whether an attacker is a Type-Tracked Ship is decided at embed-build time by checking the attacker's ship type ID against the union of all ship-type filter lists in the matched subscription's filter tree. The processor's match result and the recursive filter evaluation (including veto partitioning) are not modified. This was chosen over plumbing match provenance out of the filter engine because the observable difference is confined to corner cases and the render-side rule leaves the battle-tested engine untouched.
- **The subscription is already available where embeds are assembled** (one embed is built per matched subscription), so the rule needs a tree-walk helper that collects ship-type IDs from a subscription's filter tree — analogous to the existing "contains a ship filter" walk — and a set-membership check per attacker.
- **Hull, not weapon.** Ship-type filters match on either the flown ship or the weapon type; the display rule keys only off the flown ship's type ID.
- **Tally construction.** The Fleet Composition Tally's counting keys become mixed: Type-Tracked Ships tally under their Ship Type; all other ships tally under their Ship Group as today. A group's count excludes ships already tallied under a type (no double counting).
- **Slot priority.** Within each category line (supers/caps/subcaps), Tally Entries for Type-Tracked Ships claim the named slots first, ordered by count among themselves; remaining slots are filled by the existing count-then-known-group selection. Group entries gain no new priority: a subscription with only ship-group filters produces byte-identical output.
- **Display order.** Selected entries still display in the hardcoded group display-priority order; a Ship Type entry sorts at its parent Ship Group's position.
- **Title.** When the matched entity for the title is a Type-Tracked Ship, the title shows the Ship Type name and counts only attackers of that exact type; otherwise title logic is unchanged.
- **Naming.** Ship Type names resolve through the existing generic ID-to-name cache with ESI fallback (the same path the victim ship name already uses). Type names are always singular; the existing singular/plural handling for hardcoded group names is unchanged.
- **Category classification** (supers/caps/subcaps) for a Ship Type entry follows its parent Ship Group, so a type entry lands on the same line its group would have.
- **No schema or config changes.** No new subscription fields, no persisted-state changes, no migration. Existing subscriptions that already list ship-type IDs gain the behavior automatically.

## Testing Decisions

- Tests assert **external behavior only**: the rendered embed title and field text for a given killmail, subscription, and pre-seeded metadata caches. No assertions on internal tally structures beyond the existing unit seam.
- **Primary seam — the public embed builder.** New, non-ignored integration tests call the public embed-build function with fixture killmails, subscriptions containing ship-type filters, and app state whose ship/name/group-name caches are pre-seeded so no network is touched. Assertions inspect the returned embed's title and attacker-field text. Prior art: the existing killfeed integration tests construct exactly these inputs one level higher (at the live send function); the fixture loader and app-state helper in the shared test module are reused.
- **Secondary seam — the existing in-module unit tests** around named-slot selection are extended for the new priority rule (type entries claim slots first; group-only input unchanged).
- **Minimal coverage, per the operator's request.** Only what is required: one test for type-name display with title/count scoping, one for mixed type/group tallies with slot priority and no double counting, one regression guard that a group-only subscription renders unchanged, plus the minimal unit-test additions at the existing seam. Optionally one fixture added to the ignored live-Discord test for manual visual verification — not part of the required automated suite.

## Out of Scope

- Any change to which kills match or alert (filter semantics, veto logic, match provenance).
- Victim ship display (already shows the Ship Type name).
- Slot priority or display changes for ship-group-matched ships, or any change to group-only feeds.
- New subscription configuration (e.g. a per-subscription display toggle).
- Hand-curated plural or abbreviated forms for Ship Type names.
- Changes to embed styling, layout, line structure, or the two-named-entries-plus-overflow format.
- Per-channel deduplication or merging of embeds across subscriptions.

## Further Notes

- Accepted consequence: splitting Type-Tracked Ships out of their groups puts more distinct entries in contention for the two named slots per category line, so the "+N" overflow appears somewhat more often on type-tracked feeds. This follows necessarily from the requirement and the unchanged two-slot format.
- Domain vocabulary for this feature (Ship Type, Ship Group, Type-Tracked Ship, Fleet Composition Tally, Tally Entry) was added to the repo glossary under "Killfeed language" during the design session; the spec uses those terms canonically.
- The glossary file is currently titled for the contract-intelligence context only; retitling it to cover the whole bot was noted as a possible follow-up and is not part of this feature.
