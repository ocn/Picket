# Contract Alert Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore forward contract alerting and deliver every post-subscription contract event suppressed by the long-sweep recovery bug without duplicating existing Discord messages.

**Architecture:** Keep PostgreSQL as the source of truth. Correct collection-wide recovery detection and ESI terminal-response classification, then add a one-shot RFC3339 backfill option that reconstructs Listed and terminal events from retained facts, manifests, observation contexts, and resolution evidence. Historical events pass through the existing recursive filters, durable outbox, nonce, retry, ping limiter, and Discord adapter; the unique subscription/contract/event key makes reruns idempotent.

**Tech Stack:** Rust 2021, Tokio, SQLx/PostgreSQL 16, Reqwest, Serenity 0.11, Docker Compose.

## Global Constraints

- Preserve all unrelated working-tree and index changes; do not stage, commit, reset, or overwrite them.
- Use the existing HTTP-adapter and PostgreSQL-backed contract-cycle seams.
- Never classify generic `403` as acceptance; only the exact ESI acceptance body is positive evidence.
- Treat exact `Contract not public` as unavailable outcome evidence, equivalent to the existing non-acceptance path.
- Backfill only events at or after the explicit cutoff and only to subscriptions created before each event.
- Mark backfilled messages visibly and retain normal allowed-mention, ping-limiter, nonce, retry, and deduplication behavior.
- Preview production candidates before sending and reconcile every sent outbox row to Discord afterward.

---

### Task 1: Correct Forward Collection and Resolution

**Files:**
- Modify: `src/contract_intelligence.rs`
- Test: `tests/test_contract_intelligence.rs`

**Interfaces:**
- Consumes: `ContractCollector::collect_cycle`, `PublicContractEsi::public_contract_items_probe`.
- Produces: collection-wide `collection_recovery_required` behavior and `ContractItemProbe::NotPublic(CacheMetadata)`.

- [ ] **Step 1: Verify the long-sweep and accepted-player regression tests fail against the prior implementation**

Run the focused tests with the implementation changes temporarily excluded in an isolated baseline checkout. Expected: a long continuous sweep suppresses a new Listed event, and the exact accepted-player response remains a retryable 403.

- [ ] **Step 2: Retain the staged collection-wide recovery fix**

```rust
let recovery_baseline = self
    .store
    .collection_recovery_required(self.recovery_gap)
    .await?;
for region_id in regions {
    self.collect_region(region_id, recovery_baseline).await?;
}
```

- [ ] **Step 3: Write a failing exact-body test for ambiguous 403**

```rust
#[tokio::test]
async fn http_esi_treats_contract_not_public_as_non_acceptance_evidence() {
    let probe = probe_403(r#"{"error":"Contract not public"}"#).await;
    assert!(matches!(probe, ContractItemProbe::NotPublic(_)));
}
```

- [ ] **Step 4: Implement exact 403 classification**

```rust
match body.error.as_str() {
    CONTRACT_ACCEPTED_BY_PLAYER_ERROR => Ok(ContractItemProbe::AcceptedByPlayer(metadata)),
    CONTRACT_NOT_PUBLIC_ERROR => Ok(ContractItemProbe::NotPublic(metadata)),
    _ => Err(EsiError::from_metadata("ESI returned 403 Forbidden", Some(StatusCode::FORBIDDEN), metadata)),
}
```

- [ ] **Step 5: Route NotPublic through the existing non-acceptance terminal branch**

Use the same expiry-versus-unknown decision as `NotFound`; never produce Sale Confirmed or Purchase Confirmed from this response.

- [ ] **Step 6: Run focused tests**

Run: `CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts cargo test --test test_contract_intelligence accepted_by_player -- --nocapture`

Run the focused `not_public` and `normal_long_sweep` tests. Expected: all pass.

### Task 2: Add an Idempotent Historical Alert Backfill

**Files:**
- Modify: `src/contract_intelligence.rs`
- Test: `tests/test_contract_intelligence.rs`

**Interfaces:**
- Consumes: retained `presence_intervals`, `public_contract_facts`, `contract_item_manifests`, `contract_resolution_cases`, active subscriptions, `notify_subscription`, and the durable outbox.
- Produces: `ContractCollector::backfill_notifications_since(DateTime<Utc>) -> Result<ContractBackfillReport, ContractCollectionError>`.

- [ ] **Step 1: Write a failing listing-backfill test**

Create a silent recovery observation after a subscription exists, invoke the public backfill method twice, and assert that one historical Listed event is delivered once while a pre-subscription baseline contract is never delivered.

- [ ] **Step 2: Add timestamped storage readers**

```rust
struct HistoricalContractEvent {
    event: ContractEvent,
    observed_at: DateTime<Utc>,
}

struct TimedContractSubscription {
    subscription: ContractSubscription,
    created_at: DateTime<Utc>,
}
```

Select the earliest retained presence per region/contract at or after the cutoff for Listed events. Select terminal resolution rows by acceptance/absence evidence time. Load only active subscriptions and retain their creation timestamps.

- [ ] **Step 3: Mark historical events durably**

```rust
#[serde(default)]
pub historical_backfill: bool,
```

Prefix the normal footer with `Historical backfill` so delayed listings cannot be mistaken for newly observed contracts. Persist the flag in deferred matches and outbox event JSON.

- [ ] **Step 4: Implement per-subscription historical evaluation**

For each historical event, evaluate only subscriptions whose `created_at <= observed_at`. Reuse observed-context restoration, recursive filters, ship-group resolution, message enrichment, deferred matching, outbox preparation, ordering, pings, nonce enforcement, and delivery retry.

- [ ] **Step 5: Release incorrect nonfinancial suppression**

Clear `suppresses_nonfinancial_notification` only for awaiting cases whose earliest retained presence is at or after the explicit cutoff. This lets later 404/NotPublic outcomes pass through normal filtering without reopening baseline-era contracts.

- [ ] **Step 6: Write and pass terminal-backfill tests**

Cover an already-terminal suppressed event, an awaiting suppressed event that resolves later, rerun idempotency, and exclusion of pre-subscription events.

### Task 3: Wire a One-Shot Operator Trigger

**Files:**
- Modify: `src/lib.rs`
- Modify: `docker-compose.yml`
- Modify: `docs/contract-intelligence.md`
- Test: `tests/test_contract_intelligence.rs` or unit tests beside parsing code

**Interfaces:**
- Consumes: optional `CONTRACT_ALERT_BACKFILL_SINCE` in RFC3339 format.
- Produces: one successful backfill attempt per bot process before its first normal collection cycle; retries after transient database/ESI/Discord failure; idempotent on process restart.

- [ ] **Step 1: Write failing parsing and loop tests**

Assert empty means disabled, valid UTC/offset RFC3339 normalizes to UTC, and invalid non-empty input fails startup visibly rather than silently skipping the requested backfill.

- [ ] **Step 2: Pass the parsed cutoff into the collection loop**

```rust
let mut backfill_complete = backfill_since.is_none();
if !backfill_complete {
    collector.backfill_notifications_since(backfill_since.unwrap()).await?;
    backfill_complete = true;
}
```

- [ ] **Step 3: Add Compose passthrough and operator documentation**

```yaml
CONTRACT_ALERT_BACKFILL_SINCE: ${CONTRACT_ALERT_BACKFILL_SINCE:-}
```

Document preview, one-shot deployment, idempotent restart behavior, visible historical labeling, reconciliation queries, and recreating the container without the variable after completion.

### Task 4: Verify, Deploy, Backfill, and Reconcile

**Files:**
- No additional source files unless verification exposes a defect.

**Interfaces:**
- Consumes: test suite, Docker image, production PostgreSQL, ESI, Discord API.
- Produces: a fixed running collector and a reconciled historical outbox.

- [ ] **Step 1: Preview production candidates read-only**

Compute per-subscription/per-event candidate counts from retained observations and terminal evidence. Compare against existing outbox identities and inspect a sample of titles, values, locations, and timestamps.

- [ ] **Step 2: Run full verification**

Run `cargo fmt --check`, `cargo check`, `cargo clippy -- -W clippy::all`, `cargo test`, and the complete PostgreSQL-backed contract integration test target. Expected: zero failures.

- [ ] **Step 3: Build and deploy with the explicit cutoff**

Run Compose with `CONTRACT_ALERT_BACKFILL_SINCE=2026-08-14T22:08:03Z`, rebuild the bot image, recreate only the bot service, and preserve the PostgreSQL volume.

- [ ] **Step 4: Monitor preparation and delivery**

Wait for the logged backfill report and first fixed collection cycle. Require zero prepared backlog after retries and zero unresolved permanent delivery failures.

- [ ] **Step 5: Reconcile Discord**

For every new sent outbox row, call Discord Get Channel Message and verify channel, message ID, bot author, embed title, historical marker, contract ID, and event kind. Compare per-channel/per-event totals with the preview.

- [ ] **Step 6: Remove the one-shot trigger**

Recreate the bot without `CONTRACT_ALERT_BACKFILL_SINCE`; verify the subsequent cycle creates no duplicate delivery identities and forward listings are no longer treated as recovery baselines.

## Self-Review

- Spec coverage: forward listings, acceptance, ambiguous 403, historical Listed/terminal alerts, suppression recovery, deduplication, labeling, deployment, and Discord reconciliation are covered.
- Placeholder scan: no deferred implementation decisions remain.
- Type consistency: the backfill method, report, timestamped event/subscription records, and loop input agree across tasks.
