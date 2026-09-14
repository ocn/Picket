# PostgreSQL Health Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist an observational health snapshot and heartbeat that operators can read, while retaining only the Discord view identity and rendering seam required by the Ticket 05 watchdog.

**Architecture:** Add one additive PostgreSQL migration for the singleton heartbeat, bounded current health checks, transition records, and singleton Discord view identity. A `HealthCycle` evaluates persisted contract evidence using an injected clock and writes the heartbeat and snapshot; every health error is logged and isolated from collection and the ordinary bot runtime. Ticket 05 owns Discord publishing and watchdog activation.

**Tech Stack:** Rust 2021, Tokio, SQLx/PostgreSQL, Serenity 0.11, Chrono.

## Global Constraints

- PostgreSQL remains the sole durable coordination mechanism; do not add a database or workflow engine.
- Health monitoring is observational: initialization, evaluation, storage, and display failures cannot gate startup, collection, matching, delivery, or repair.
- Only Operator `146451271497416704` can read `/health`; the command is ephemeral and does not mutate state.
- Retain the Discord message identity and a non-pinging rendering seam, but do not create, edit, or publish Discord health messages in this ticket.
- Keep all existing JSON-backed runtime state unchanged and do not implement Ticket 03+ work, the watchdog, or end-to-end subscription health.
- Do not stage, commit, switch branches, or touch refs.

---

### Task 1: Define and prove the health-cycle boundary

**Files:**
- Modify: `tests/test_contract_intelligence.rs`
- Modify: `src/contract_intelligence.rs`

**Interfaces:**
- Consumes: `ContractCollectionStore`, a temporary PostgreSQL database, and `HealthClock`.
- Produces: `HealthCycle::run_once() -> Result<HealthSnapshot, HealthError>` and observable persisted snapshot state.

- [ ] **Step 1: Write the failing high-level health-cycle test**

```rust
let result = HealthCycle::new(store.clone(), clock.clone())
    .run_once()
    .await;
assert_eq!(result.expect("cycle result").overall, HealthStatus::Healthy);
```

- [ ] **Step 2: Run the health-cycle test to verify it fails**

Run: `cargo test --test test_contract_intelligence health_cycle -- --nocapture`

Expected: compilation failure because the public health-cycle interface does not exist.

- [ ] **Step 3: Add only the public health data types and empty cycle implementation needed to compile the test**

```rust
pub trait HealthClock: Send + Sync { fn now(&self) -> DateTime<Utc>; }
pub struct HealthCycle;
```

- [ ] **Step 4: Run the health-cycle test to verify its intended assertion failure**

Run: `cargo test --test test_contract_intelligence health_cycle -- --nocapture`

Expected: FAIL because no snapshot is persisted or the result is not healthy.

### Task 2: Add the health migration and store methods

**Files:**
- Create: `migrations/20260817000001_add_health_snapshots.sql`
- Modify: `src/contract_intelligence.rs`
- Modify: `tests/test_contract_intelligence.rs`

**Interfaces:**
- Consumes: `ContractCollectionStore::connect` migration runner.
- Produces: `record_heartbeat`, `save_health_snapshot`, `health_snapshot`, and `health_view_identity` store methods.

- [ ] **Step 1: Write failing migration tests for a clean database and the current Ticket 01 production level**

```rust
assert!(table_exists(&pool, "health_snapshots").await);
assert_eq!(preserved_count(&pool, "contract_outbound_deliveries").await, 1);
```

- [ ] **Step 2: Run the migration tests to verify the health tables are absent**

Run: `cargo test --test test_contract_intelligence health_snapshot_migration -- --nocapture`

Expected: FAIL because the migration count and health tables do not exist.

- [ ] **Step 3: Add the additive migration and minimal SQLx persistence methods**

```sql
CREATE TABLE health_snapshots (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton));
CREATE TABLE health_check_results (check_key TEXT PRIMARY KEY, status TEXT NOT NULL);
CREATE TABLE health_transitions (check_key TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, UNIQUE (check_key, observed_at));
CREATE TABLE health_discord_views (view_key TEXT PRIMARY KEY, message_id TEXT);
```

- [ ] **Step 4: Run the migration tests to verify clean/current upgrades and contract data preservation**

Run: `cargo test --test test_contract_intelligence health_snapshot_migration -- --nocapture`

Expected: PASS.

### Task 3: Evaluate thresholds, recovery, restart, and storage isolation

**Files:**
- Modify: `src/contract_intelligence.rs`
- Modify: `tests/test_contract_intelligence.rs`

**Interfaces:**
- Consumes: persisted `regional_collection_metadata`, `esi_cache_metadata`, and `contract_outbound_deliveries` evidence.
- Produces: `HealthSnapshot { overall, checks, observed_at }` using `HealthThresholds`.

- [ ] **Step 1: Extend the failing high-level test with degraded, critical, recovery, restart, and store-failure cases**

```rust
assert_eq!(cycle.run_once().await?.overall, HealthStatus::Degraded);
clock.advance(ChronoDuration::minutes(16));
assert_eq!(restarted_cycle.run_once().await?.overall, HealthStatus::Critical);
assert!(run_observed_collection_after_health_failure().await.is_ok());
```

- [ ] **Step 2: Run the test to verify each new case is red**

Run: `cargo test --test test_contract_intelligence health_cycle -- --nocapture`

Expected: FAIL because threshold evaluation, transitions, restart state, or health-error isolation is missing.

- [ ] **Step 3: Implement only heartbeat, PostgreSQL, contract progress, ESI progress, prepared-delivery, and permanent-delivery-failure checks**

```rust
let overall = checks.iter().map(|check| check.status).max().unwrap_or(HealthStatus::Healthy);
store.save_health_snapshot(&snapshot).await?;
```

- [ ] **Step 4: Run the high-level test to verify all health-cycle behavior is green**

Run: `cargo test --test test_contract_intelligence health_cycle -- --nocapture`

Expected: PASS, including recovery after a recreated store and a failed health cycle that does not stop collection.

### Task 4: Preserve the watchdog view seam and expose operator-only `/health`

**Files:**
- Create: `src/commands/health.rs`
- Modify: `src/commands.rs`
- Modify: `src/lib.rs`
- Modify: `src/contract_intelligence.rs`
- Modify: `tests/test_contract_intelligence.rs`

**Interfaces:**
- Consumes: persisted `HealthSnapshot`, `HealthDiscordViewIdentity`, and `ContractStoreContainer`.
- Produces: an operator-only `HealthCommand`, an independent best-effort health-cycle task, and a reusable non-pinging view renderer for Ticket 05.

- [ ] **Step 1: Add failing tests for persisted-snapshot rendering and operator authorization**

```rust
assert!(health_command_response(NON_OPERATOR, snapshot).is_none());
```

- [ ] **Step 2: Run those tests to verify red behavior**

Run: `cargo test --test test_contract_intelligence health -- --nocapture`

Expected: FAIL because the command does not exist.

- [ ] **Step 3: Implement the command rendering and an independently spawned best-effort persistence loop**

```rust
if command.user.id.0 != HEALTH_OPERATOR_ID { return; }
```

- [ ] **Step 4: Run the focused health tests**

Run: `cargo test --test test_contract_intelligence health -- --nocapture`

Expected: PASS; unauthorized callers have no diagnostics or state change. No Discord publisher is activated by Ticket 02.

### Task 5: Verify the Ticket 02 scope

**Files:**
- Modify: `docs/env.sample`
- Modify: `docker-compose.yaml`

**Interfaces:**
- Consumes: the health configuration environment variables.
- Produces: documented runtime defaults without secrets.

- [ ] **Step 1: Add secret-free interval, health channel, and accepted threshold configuration defaults**

```text
HEALTH_EVALUATION_INTERVAL_SECS=60
HEALTH_CHANNEL_ID=1538030920328810536
HEALTH_HEARTBEAT_DEGRADED_SECS=300
HEALTH_HEARTBEAT_CRITICAL_SECS=600
```

- [ ] **Step 2: Format and run focused integration tests**

Run: `cargo fmt --check && cargo test --test test_contract_intelligence health_snapshot_migration health_cycle -- --nocapture`

Expected: exit 0.

- [ ] **Step 3: Run the required repository verification**

Run: `cargo test && cargo check && cargo clippy -- -W clippy::all`

Expected: exit 0 without adding a watchdog, structure resolver, repair system, or subscription-health implementation.
