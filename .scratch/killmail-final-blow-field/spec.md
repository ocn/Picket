# Show Final Blow beside Victim in killmail embeds

Status: ready-for-agent

## Problem Statement

Killmail embeds identify the Victim and summarize the attacking fleet, but they do not identify the attacker credited with the Final Blow. Readers must open the linked killmail to answer a basic question that should be visible at a glance. Adding that information must not make the compact embed harder to scan or introduce a separate participant format.

## Solution

Add a **Final Blow** field immediately beside **Victim** whenever a killmail contains an attacker marked as credited with the Final Blow. Render both fields inline and use the same participant formatting and zKillboard links: a linked alliance ticker, falling back to a linked corporation ticker, followed by the linked character name. When the credited attacker has no character, show its Ship Type without an identity link while retaining an available linked affiliation.

If no attacker is marked as credited with the Final Blow, omit the new field and preserve the existing full-width Victim layout. If malformed input marks more than one attacker, display the first in killmail order. Keep the change confined to embed presentation and verify it through the existing credential-free embed-building seam.

## User Stories

1. As a killfeed reader, I want to see who delivered the Final Blow, so that I can understand the kill without opening another page.
2. As a killfeed reader, I want Final Blow shown beside Victim, so that the two sides of the kill are visually comparable.
3. As a killfeed reader, I want the concise label **Final Blow**, so that the embed uses familiar zKillboard terminology without an unnecessarily long heading.
4. As a killfeed reader, I want the Victim and credited attacker formatted consistently, so that I can scan participant identity without learning two display conventions.
5. As a killfeed reader, I want the credited attacker's character name linked to that character's zKillboard page, so that I can inspect the character directly.
6. As a killfeed reader, I want the displayed affiliation ticker linked to its zKillboard page, so that I can inspect the relevant alliance or corporation directly.
7. As a killfeed reader, I want alliance affiliation preferred over corporation affiliation when both exist, so that the credited attacker follows the established Victim presentation.
8. As a killfeed reader, I want corporation affiliation used when no alliance exists, so that unaffiliated corporations still provide useful context.
9. As a killfeed reader, I want unavailable affiliation metadata omitted cleanly, so that a lookup failure does not make the killmail embed unusable.
10. As a killfeed reader, I want an NPC or structure credited with the Final Blow represented by its Ship Type when it has no character, so that non-character attackers are still identifiable.
11. As a killfeed reader, I want a non-character Ship Type left unlinked, so that the embed does not imply a character identity that does not exist.
12. As a mobile Discord reader, I want Victim and Final Blow configured as adjacent inline fields, so that Discord can use its responsive compact layout.
13. As a killfeed reader, I want Victim to remain full-width when no attacker is marked, so that malformed or incomplete input does not leave an isolated narrow field.
14. As an operator, I want a killmail with multiple attackers marked as credited with the Final Blow to render deterministically using the first one, so that malformed upstream data does not block delivery.
15. As an operator, I want missing or duplicated credited-attacker markers handled without errors, alerts, or new validation infrastructure, so that the RedisQ processing loop remains resilient.
16. As an operator, I want the presentation in killfeed and tracking embeds to remain consistent, so that the same killmail does not change participant formatting between delivery paths.
17. As an operator, I want existing attacker summaries, titles, footers, thumbnails, and subscription matching to remain unchanged, so that this focused presentation change cannot alter kill selection or other embed content.
18. As a maintainer, I want the Victim and credited attacker to share one small participant formatter, so that their text and link conventions cannot drift independently.
19. As a maintainer, I want the feature covered without Discord credentials or network access, so that regressions are caught by the normal test suite.
20. As a maintainer, I want tests to assert rendered embed behavior rather than private implementation details, so that the presentation code remains easy to simplify.

## Implementation Decisions

- The existing killmail embed builder remains the presentation boundary. No new module, service, generalized participant model, or delivery path is introduced.
- The field label is **Final Blow**, matching the project glossary. Avoid “Final blow attacker” and “killer.”
- Select the first attacker in killmail order whose credited-attacker marker is true.
- Resolve the credited attacker's identity through the same existing name, Ship Type, and ticker cache/ESI paths used by killmail presentation. No new metadata client or cache is added.
- A small shared participant formatter renders both the Victim and credited attacker. It accepts already-resolved identity and affiliation details; it does not become a broad domain abstraction.
- Character participants render as a linked character name. Non-character participants render as their Ship Type without a character link.
- Affiliation renders before identity. Prefer a linked alliance ticker; when no alliance exists, use a linked corporation ticker. Preserve the existing Victim behavior when ticker metadata is unavailable by omitting the affiliation rather than failing the embed.
- When the credited attacker can be displayed, emit Victim and Final Blow as adjacent inline fields after the existing attacker summary.
- When no attacker is marked as credited with the Final Blow, emit no Final Blow field and keep Victim non-inline as it is today.
- When multiple attackers are marked, use the first and add no warning, rejection, or cardinality-normalization machinery.
- Discord controls the final responsive layout. The implementation requests inline display but does not add spacer fields or client-specific layout workarounds.
- The shared killmail builder makes the behavior apply consistently to killfeed and tracking embeds.
- No schema, serialized model, configuration, subscription, filter, processor, persistence, or migration changes are required.

## Testing Decisions

- Tests assert external presentation behavior: serialized embed field names, values, ordering, links, presence, and inline flags. They do not call or assert the private shared formatter directly.
- Use one existing high-level seam: build an embed through the public killmail embed builder with a fixture killmail, a subscription, and pre-seeded metadata caches, then inspect the returned Serenity embed. This is the same credential-free serialized-embed pattern already used by the Ship Type display tests.
- Keep tests deterministic and offline. Reuse the current fixture and app-state helpers, seed the required character names and tickers, and mutate cloned killmail data for edge cases. Do not add Discord credentials, live HTTP calls, mocks, or a new fixture.
- Add three focused tests:
  1. A normal player credited with the Final Blow renders the exact linked Victim and Final Blow values in adjacent fields, with both fields inline.
  2. A credited attacker without a character renders the linked affiliation followed by the unlinked Ship Type.
  3. Cardinality behavior covers both zero marked attackers (no Final Blow field and non-inline Victim) and multiple marked attackers (the first attacker is rendered).
- Existing killfeed and tracking visual-send tests remain optional manual confirmation and are not required for automated correctness.
- Run the focused embed tests and the normal Rust test suite. Formatting, compilation, and Clippy remain standard completion checks.

## Out of Scope

- Showing the credited attacker's ship, weapon, damage, security status, or combat statistics for character attackers.
- Showing the top-damage attacker or changing the existing Fleet Composition Tally.
- Changing which subscriptions match, attacker/victim filter semantics, veto logic, or notification behavior.
- Adding a display toggle, configuration key, persistence field, migration, or slash command.
- Validating or repairing upstream killmail cardinality beyond deterministic first-match and omission behavior.
- Adding spacer fields or attempting to guarantee one physical row on every Discord client width.
- Redesigning other embed fields, titles, descriptions, footers, thumbnails, or links.
- Introducing a general participant/affiliation subsystem or refactoring unrelated embed code.
- Requiring live Discord integration tests for acceptance.

## Further Notes

- zKillboard uses **Final Blow** as the display label for the credited participant: https://zkillboard.com/information/faq/
- Discord defines an embed field's `inline` property as whether the field should display inline; final wrapping remains client-responsive: https://docs.discord.com/developers/resources/message
- The **Final Blow** term was added to the repository glossary during design. No ADR is needed because the presentation choice is localized and easy to reverse.
- Local issue tracker identity: `.scratch/killmail-final-blow-field/spec.md`.
