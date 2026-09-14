# Independent Discord Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a restart-safe watchdog outside the bot process that turns persisted health evidence into one Discord Health View and one consolidated incident.

**Architecture:** The existing bot remains the sole writer of Heartbeat and Health Snapshot records. A new watchdog reads those records and persists only its Discord publication identities and retry state; it uses an injected clock and publisher seam, then a thin Serenity adapter and standalone binary make it independently restartable.

**Tech Stack:** Rust 2021, Tokio, SQLx/PostgreSQL, Serenity 0.11, Chrono, Docker Compose.

## Global Constraints

- Health is observational and cannot gate bot startup, collection, matching, delivery, or repair.
- PostgreSQL is the only durable coordination store; use an additive migration.
- Only Operator `146451271497416704` may receive an incident mention; the Health View never pings.
- Preserve existing user-facing location wording and do not implement Ticket 06 or later work.
- Use the contract-integration temporary PostgreSQL harness, a controlled clock, and a fake Discord publisher.

---

### Task 1: Define the watchdog seam and durable state

**Files:**
- Modify: `tests/test_contract_intelligence.rs`
- Modify: `src/contract_intelligence.rs`
- Create: `migrations/20260817000004_add_health_watchdog_state.sql`

**Interfaces:**
- Produces: `HealthWatchdog::run_once() -> HealthWatchdogResult`, `HealthDiscordPublisher`, and persisted view/incident state.

- [ ] Write a real-PostgreSQL high-level test that creates exactly one view and one consolidated incident from a stale heartbeat.
- [ ] Run the test and verify it fails because no watchdog seam exists.
- [ ] Add the minimal migration, state store, fakeable publisher trait, and one-shot watchdog implementation.
- [ ] Run the focused test and verify it passes.

### Task 2: Prove transitions, restart, and bounded retries

**Files:**
- Modify: `tests/test_contract_intelligence.rs`
- Modify: `src/contract_intelligence.rs`

**Interfaces:**
- Consumes: persisted Heartbeat/Snapshot and publisher outcomes `RetryAfter`, `Permanent`, or transient failure.
- Produces: one updated/resolved incident, persisted publication deadlines, and no automatic permanent-error retry.

- [ ] Add focused red cases for degraded/critical/recovered heartbeat, process restart, first/third PostgreSQL failure, retry-after, and permanent error.
- [ ] Run the focused test to establish each failure.
- [ ] Implement the minimal transition, retry, and failure-state behavior.
- [ ] Run the focused test green.

### Task 3: Add the independent production adapter

**Files:**
- Modify: `src/discord_bot.rs`
- Create: `src/bin/health_watchdog.rs`
- Modify: `Dockerfile`
- Modify: `docker-compose.yaml`
- Modify: `docs/env.sample`

**Interfaces:**
- Produces: `DiscordHealthPublisher` and the `health-watchdog` executable, separately configured from the bot process.

- [ ] Add adapter/binary configuration tests or compile assertions and run them red.
- [ ] Implement no-ping view rendering, operator-only incident allowed mention, and Compose service.
- [ ] Run focused watchdog integration tests, format, check, and Clippy.

### Task 4: Verify the operational boundary

**Files:**
- Modify: `tests/test_contract_intelligence.rs`

- [ ] Assert a collector cycle still completes after watchdog/health publication failure.
- [ ] Run the serial contract integration suite with the local PostgreSQL harness.
- [ ] Run `cargo fmt --check`, `cargo check`, and `cargo clippy -- -W clippy::all`; inspect diff and status without staging or committing.
