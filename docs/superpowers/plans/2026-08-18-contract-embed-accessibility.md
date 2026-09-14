# Contract Embed Accessibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make contract alerts easy to scan on narrow Discord clients without implying that the bot knows an exact sale time when it only has an observation window.

**Architecture:** Preserve the ESI response kind as typed lifecycle provenance, render timestamps as short stacked Discord timestamps, and use the existing durable message-repair queue for an operator-controlled historical rewrite. Keep discovery, filtering, and ping behavior unchanged.

**Tech Stack:** Rust, SQLx/PostgreSQL, Serenity 0.11, Discord embeds, existing contract collector and delivery-repair infrastructure.

## Global Constraints

- Do not combine `<t:...:f>` and `<t:...:R>` on one line.
- Do not label `evidence_response_at` as the sale or purchase time.
- Treat HTTP 403 `accepted by another player` as confirmed acceptance. Treat HTTP 204 `expired or recently accepted` as a closed contract with unknown outcome.
- Keep absolute times localized with Discord style `f`; place relative style `R` on its own line.
- Preserve the observed interval: the event occurred after `last_public_observed_at` and no later than `absence_observed_at`.
- Do not add pings when editing historical messages.
- Historical rewrites must use the existing durable edit claim, lease, retry, and operator-capability boundaries.
- Prototype and inspect the copy before enabling any bulk rewrite.
- Do not alter subscription filters, structure resolution, proximity promotion, or Q13 wording.

## Recommended Copy Prototype

Use this as the implementation target; retain two rejected alternatives as fixtures for comparison, not production branches.

### Stacked and evidence-safe

```text
Ragnarok sale confirmed • 77.5B ISK

Listed
<t:ISSUED:f>
<t:ISSUED:R>

Observed sale window
**After** <t:LAST_PUBLIC:f>
**Before** <t:FIRST_ABSENT:f>

Confirmed by ESI
Accepted by another player
<t:EVIDENCE_RESPONSE:f>
<t:EVIDENCE_RESPONSE:R>
```

For a 204 response, use `Ragnarok closed • outcome unknown`, rename the interval to `Observed closure window`, and render:

```text
ESI result
Expired or recently accepted
<t:EVIDENCE_RESPONSE:f>
<t:EVIDENCE_RESPONSE:R>
```

For a listing, render only:

```text
Listed
<t:ISSUED:f>
<t:ISSUED:R>
```

History must use short, factual lines:

```text
Issuer record
4 confirmed sales
2 confirmed purchases
Latest confirmation <t:MOST_RECENT:d>
```

Do not use `last sale` for the lifecycle-probe timestamp. It is a confirmation timestamp, not the exact transfer time.

---

### Task 1: Preserve typed ESI outcome provenance

**Files:**

- Create: `migrations/20260818000005_add_contract_acceptance_response_kind.sql`
- Modify: `src/contract_intelligence.rs`
- Modify: `tests/test_contract_intelligence.rs`

- [ ] Add `ContractAcceptanceKind::{AcceptedByPlayer, NoContent}` with snake-case serialization and a strict database parser.
- [ ] Extend `ContractAcceptanceEvidence` and `TerminalContractResolution` with `response_kind: Option<ContractAcceptanceKind>` so previously serialized events remain readable.
- [ ] Write a failing real-PostgreSQL test extending `acceptance_evidence_preserves_no_content_and_accepted_player_response_kinds`: restart the store, load both terminal events, and assert the typed kind survives outside `contract_lifecycle_evidence.detail`.
- [ ] Run:

  ```bash
  CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts cargo test --test test_contract_intelligence acceptance_evidence_preserves_no_content_and_accepted_player_response_kinds -- --exact --nocapture
  ```

- [ ] Confirm RED because `contract_resolution_cases` has no response-kind column and pending-terminal queries discard the provenance.
- [ ] Add nullable `acceptance_response_kind TEXT` with a check constraint allowing only `403_accepted_by_player` and `204_no_content`.
- [ ] Backfill the column from the latest `contract_lifecycle_evidence.detail->>'response'`, including the operator-reconciliation 403 suffix if present.
- [ ] Update `confirm_acceptance`, all three pending-terminal queries, `terminal_resolution_event`, and `acceptance_event` to persist and carry the typed kind.
- [ ] Re-run the focused test and confirm GREEN.
- [ ] Update the clean/current migration continuity test to assert migration count 23 and the new column/check constraint.

### Task 2: Stop classifying ambiguous 204 responses as confirmed sales

**Files:**

- Modify: `src/contract_intelligence.rs`
- Modify: `tests/test_contract_intelligence.rs`
- Modify: `migrations/20260818000005_add_contract_acceptance_response_kind.sql`

- [ ] Write a failing collector test with two pre-expiry item-exchange contracts: one probe returns `AcceptedByPlayer`, the other `NoContent`.
- [ ] Assert the 403 case emits `SaleConfirmed` or `PurchaseConfirmed`, while the 204 case emits `ClosedOutcomeUnknown` and never increments confirmed party history.
- [ ] Confirm RED because both probe variants currently call `confirm_acceptance` and become `acceptance_confirmed`.
- [ ] Route `AcceptedByPlayer` through the confirmed-acceptance state only.
- [ ] Route `NoContent` through a terminal unknown-outcome transition that preserves its response kind and observation window without claiming a sale.
- [ ] In the migration, reclassify existing `acceptance_confirmed` rows whose backfilled kind is `204_no_content` to `closed_outcome_unknown`; retain their timestamps and keep `notification_pending` unchanged to avoid duplicate alerts.
- [ ] Re-run the focused collector test and party-history assertions.

### Task 3: Render accessible stacked timestamps and honest evidence copy

**Files:**

- Modify: `src/contract_intelligence.rs`

- [ ] Replace `discord_absolute_relative_timestamp` with:

  ```rust
  fn discord_absolute_timestamp(value: DateTime<Utc>) -> String {
      format!("<t:{}:f>", value.timestamp())
  }

  fn discord_relative_timestamp(value: DateTime<Utc>) -> String {
      format!("<t:{}:R>", value.timestamp())
  }
  ```

- [ ] Write failing renderer tests named:
  - `contract_embed_stacks_absolute_and_relative_timestamps`
  - `contract_embed_renders_acceptance_as_an_observed_window`
  - `contract_embed_renders_no_content_as_unknown_closure`
  - `contract_embed_lines_never_combine_absolute_and_relative_tokens`
- [ ] Assert no field line contains both `:f>` and `:R>`, no field contains `Acceptance confirmed:`, and no field calls `evidence_response_at` the sale time.
- [ ] Replace the single `Timeline` field with the exact fields in the approved prototype. Set each field `inline: false` for predictable narrow-client order.
- [ ] Remove the duplicate `Evidence` field for acceptance events. Its facts now live in `Observed sale window`/`Observed closure window` and `Confirmed by ESI`/`ESI result`.
- [ ] Keep the title plain and compact: `… sale confirmed • …`, `… purchase confirmed • …`, or `… closed • outcome unknown`.
- [ ] Update `format_party_history` to output one fact per line and use `Latest confirmation`, not `last sale` or `last purchase`.
- [ ] Retain `Alert` as a separate field so proximity state stays visible and independently repairable.
- [ ] Run:

  ```bash
  cargo test --lib contract_embed_ -- --nocapture
  ```

### Task 4: Protect stateful fields while allowing format repairs

**Files:**

- Modify: `src/contract_intelligence.rs`
- Modify: `tests/test_contract_intelligence.rs`

- [ ] Add `format_revision: u16` to `ContractNotificationMessage` with `#[serde(default)]`; define `CONTRACT_EMBED_FORMAT_REVISION` for the new layout.
- [ ] Write a failing repair test proving an old sent message upgrades its title, description, timestamp fields, history, footer, and revision while preserving its current `Alert` field.
- [ ] Replace `repaired_contract_message`'s blanket preservation of `stored.fields` with a merge that preserves only stateful fields (`Alert`) and takes all presentation fields from the regenerated message.
- [ ] Assert a repair never changes `ping`, `ping_type`, delivery identity, nonce, or Discord message ID.
- [ ] Assert the normal repair claim/lease prevents two dispatchers from editing the same message concurrently.
- [ ] Run the focused repair tests against temporary PostgreSQL.

### Task 5: Add an operator-controlled channel rerender command

**Files:**

- Modify: `src/contract_intelligence.rs`
- Modify: `src/main.rs`
- Modify: `tests/test_contract_intelligence.rs`
- Modify: `docs/contract-intelligence.md`

- [ ] Extend the capability-protected `contract-delivery` CLI with `rerender-channel --channel-id <id> --limit <n> [--dry-run]`.
- [ ] Write a failing actual-binary/real-PostgreSQL test proving the command rejects absent or invalid stdin capability tokens and never prints the token.
- [ ] Add `ContractCollectionStore::contract_deliveries_needing_format_repair(channel_id, revision, limit)` returning sent rows with a Discord message ID and an older serialized format revision.
- [ ] Regenerate each message from its stored `event`, current party history, and selected primary item; queue it through `prepare_delivery_repair` rather than calling Discord directly.
- [ ] Make `--dry-run` report candidate IDs and counts without changing `desired_message`.
- [ ] Audit the operator ID, channel, revision, requested limit, and candidate count; never log secrets or raw bearer tokens.
- [ ] Assert retired identities (`discord_message_id IS NULL`), permanent failures, and deleted messages are not selected.
- [ ] Assert historical edits carry no mentions and cannot create replacement posts.

### Task 6: Prototype and approve before historical rewrite

**Files:**

- Modify: `docs/contract-intelligence.md`

- [ ] Render three local JSON fixtures from the same production-shaped Ragnarok event: recommended stacked, compact single-field, and evidence-first. Do not send them to Discord.
- [ ] Record the selected stacked layout and rejection reasons: single-line absolute/relative copy wraps badly; evidence-first obscures the contract action.
- [ ] Run the rerender command with `--dry-run` for `#supers-sold` and record the candidate count.
- [ ] Queue exactly three non-pinging canary edits and inspect desktop, narrow desktop, and mobile Discord rendering.
- [ ] Require explicit operator approval before queuing the remainder.
- [ ] Queue remaining repairs in bounded batches that respect the existing Discord retry deadline; stop on any permanent repair failure.
- [ ] Verify no new messages or pings were created and each edited message retained its original Discord ID.

### Task 7: Final verification

**Files:**

- Modify only tests or docs needed to keep established assertions current.

- [ ] Run focused renderer and lifecycle tests:

  ```bash
  cargo test --lib contract_embed_ -- --nocapture
  CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts cargo test --test test_contract_intelligence acceptance_evidence_ -- --test-threads=1 --nocapture
  CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts cargo test --test test_contract_intelligence contract_repair_ -- --test-threads=1 --nocapture
  ```

- [ ] Run the serial real-PostgreSQL contract suite once with durable output capture.
- [ ] Run:

  ```bash
  cargo fmt --all -- --check
  cargo check --all-targets
  cargo clippy --all-targets -- -W clippy::all
  cargo test --lib -- --skip esi::tests::test_esi_timeout_is_configured --test-threads=1
  docker compose config --quiet
  git diff --check
  ```

- [ ] Confirm Discord rendering still suppresses allowed mentions for edits.
- [ ] Confirm new listings, 403 acceptances, 204 closures, proximity corrections, and historical repairs all use the same format revision.
- [ ] Confirm no exact sale time is stated unless a future authoritative source supplies one.
