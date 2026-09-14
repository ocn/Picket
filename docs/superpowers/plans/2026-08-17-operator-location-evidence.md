# Operator Location Evidence Implementation Plan

> **For agentic workers:** Execute sequentially with one RED → GREEN slice at a time.

**Goal:** Resolve public-contract proximity using auditable, expiring operator location evidence without expanding monitoring beyond public contracts.

**Architecture:** Keep location evidence independent from subscriptions in PostgreSQL. Resolve the highest-precedence current evidence by location identifier before the existing ContractCollector light-year filter runs. Expose mutations only through a process-local operator CLI, never the Discord command map.

**Tech Stack:** Rust, SQLx/PostgreSQL, existing ContractCollector integration harness.

## Global Constraints

- Additive migration only; current public-contract discovery remains unauthenticated.
- Precedence: public NPC, then current access-qualified, then unexpired operator evidence.
- Do not change user-facing location wording or add a public Discord command.
- Do not implement resolver authorization, unverified alerts, repair, or promotion work.
- Do not stage or commit changes.

### Task 1: Persist and inspect operator evidence

**Files:**

- Create: `src/location_evidence.rs`
- Create: `migrations/20260817000002_add_location_evidence.sql`
- Modify: `src/lib.rs`, `src/main.rs`, `tests/test_contract_intelligence.rs`

- [x] Write a failing restricted-CLI add/inspect test with positive identifiers, future expiry, provenance, and an audit row.
- [x] Run the isolated test and observe the missing interface failure.
- [x] Add the smallest store-backed model and CLI boundary that passes it transactionally.
- [x] Re-run the isolated test.

### Task 2: Expiry, supersession, and precedence

**Files:**

- Modify: `src/location_evidence.rs`, `tests/test_contract_intelligence.rs`

- [x] Write a failing test proving expire and supersede retain audit history and that lower/expired evidence cannot displace a higher current fact.
- [x] Run the isolated test and observe failure.
- [x] Implement only the validated mutation and selected-evidence query behavior.
- [x] Re-run the isolated test.

### Task 3: Contract-cycle proximity resolution

**Files:**

- Modify: `src/contract_intelligence.rs`, `src/location_evidence.rs`, `tests/test_contract_intelligence.rs`

- [x] Write a failing ContractCollector-cycle test where an unknown player structure is resolved from operator evidence to one in-range and one out-of-range subscription result.
- [x] Run the isolated test and observe deferred resolution.
- [x] Load selected location evidence into the existing context after public resolution is unavailable and pass the test.
- [x] Re-run the test after reconstructing the store to prove persistence.

### Task 4: Migration compatibility and verification

**Files:**

- Modify: `tests/test_contract_intelligence.rs`

- [x] Extend the existing clean/current migration test for the additive tables and retained prior state.
- [x] Run it against temporary PostgreSQL.
- [x] Run focused evidence tests, serial contract integration, fmt, check, and Clippy.
