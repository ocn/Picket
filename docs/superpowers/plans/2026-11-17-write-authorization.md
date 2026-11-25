# Picket Write Authorization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce the approved per-guild write policy at every user request boundary while automated Feeds continue.

**Architecture:** A pure immutable policy owns decisions. Thin request adapters classify commands and preflight continuations before invoking existing business work. Discord supplies current identity; the operator supplies restart-loaded grants separately from writable state.

**Tech Stack:** Rust, Serenity 0.11, Tokio, Serde, Docker Compose.

## Global Constraints

- Authority: `.scratch/write-authorization-policy/spec.md`, issue `01-implement-write-authorization`; read the complete approved spec before implementation.
- Full-stop only: feature-branch commits allowed; controller owns push/PR. Never merge, auto-merge, queue, deploy, run live Discord/EVE account tests, or expose secrets.
- Never force-add or publish `.scratch/` content. Retain first failures in local proof/report files. Durable intent and proof summaries belong in tracked docs and PR.
- No Administrator bypass. Explicit user OR role grant within original guild and verified membership only. Policy failure denies writes without stopping reads/Feeds.
- Policy must not import Discord HTTP, ESI, or persistence. Dependency direction held by review; enforcement setup is a separate non-blocking issue. No ontology change.
- Private denials, bounded structured logs without request secrets, two-second continuation membership timeout, atomic one-use completion. Preserve subscription tuple semantics and separate existing saves.
- Equivalent implementation decisions go in spec Decided after Go and PR. Do not change approved behavior.

### Task 1: Strict immutable operator policy

**Files:** Create `src/write_policy.rs`; export from `src/lib.rs`; add offline tests in module or `tests/test_write_policy.rs`.
**Interfaces:** Produce `WritePolicy`, a default deny-all value; `WritePolicy::from_json(&str) -> Result<Self, PolicyLoadError>`; `WritePolicy::load(path: &Path, state_dir: &Path) -> Result<Self, PolicyLoadError>`; `WritePolicy::allows(guild_id: u64, user_id: u64, role_ids: &[u64]) -> bool`. Identity/membership establishment remains adapter responsibility. Errors expose only bounded reason categories, never contents.

- [x] Write tests for same-guild user/role OR, cross-guild denial, no grant/admin denial, everyone-role rejection, invalid JSON/version/unknown fields, canonical positive decimal u64 strings only (including guild keys), absent lists/guilds, snapshot immutability.
```rust
assert!(!WritePolicy::default().allows(1, 2, &[3]));
let policy = WritePolicy::from_json(r#"{"version":1,"guilds":{"1":{"allowed_user_ids":["2"]}}}"#).unwrap();
assert!(policy.allows(1, 2, &[]));
assert!(!policy.allows(4, 2, &[]));
```
- [x] Run focused tests red and retain the first output in `.scratch/write-authorization-policy/proof/task-1-red.log`.
- [x] Implement strict parsing, reject duplicate object fields/keys to avoid ambiguous configuration, validate lexical/canonical paths, reject symlinks in every existing path component and resolution inside writable bot state. Missing/unreadable/invalid files return categorized errors; caller later substitutes deny-all. Cover relative/absolute paths, symlink directory/file, path normalization, missing state dir and state alias as applicable.
- [x] Run focused tests green and formatting/compilation as needed; retain commands/exits. Update spec with equivalent decisions. Commit only task files with issue reference.

### Task 2: Guard production commands and standings continuations

**Files:** Modify `src/config.rs`, `src/lib.rs`, `src/discord_bot.rs`, `src/commands.rs`, write registrations in `src/commands/*.rs`; create focused `src/write_authorization.rs` (and continuation adapter module if useful), offline request tests.
**Interfaces:** Consume Task 1 policy. Produce one shared request seam used by real Serenity handlers and fake offline adapters, including membership/response/work. Keep pure policy separate from transport. Initialize one policy snapshot at startup without making Feed/read setup conditional on success.

- [x] Read all active command dispatch and continuation paths, preserving existing work behind guarded closures/adapters. Write failing raw-Serenity request seam tests, not only helper booleans.
```rust
// Test shape: fake adapter records ack/member/work/deny events;
// an unauthorized raw command must record deny and zero work calls;
// an authorized command reaches the intended dummy handler once.
```
- [x] Retain first red test output. Classify all 17 registered commands and every watch subcommand explicitly per approved spec. Validate watch structural discriminator before business parsing; missing/duplicate/malformed/unknown operations never invoke work. Guard writes with matching guild/member/user identity; queries keep health operator guard and find_unsubscribed defaults. Remove Manage Server defaults for writes/watch, retain dm_permission(false).
- [x] Load `policy/write-policy.json` or `PICKET_WRITE_POLICY_PATH` once. Emit categorized startup error and continue with deny-all. Add startup seam proof that missing policy still invokes read/Feed initialization.
- [x] Route component/DM callbacks through shared injectable acknowledgement, membership and work seam. Ownership before consume, original guild/channel/subscription tuple preflight before any credential/affiliation/contact work, current membership via Discord HTTP within two seconds. Components defer ephemerally before lookup, edit private result afterwards. DMs use private generic rejection. Validate selected character belongs to initiator saved authorizations. Atomically claim completing states only after preflight; reauthorization checks without consuming state. Prevent replay/concurrent duplicate exchange. Malformed callback/selection fails safely.
- [x] Test revoked grants/roles, departed member, lookup error/timeout with controlled clock; owner mismatch, channel/guild/key mismatch, deleted target, malformed character, replay and concurrent one-use completion. Test no recreation after deletion and documented then-current same-key update. Capture decision logs and assert sentinel code/token/state/URL/policy strings absent. Denied spies must show no business option parsing, OAuth, instructions, persistence, or state consume.
- [x] Update registration JSON tests for availability and DM restrictions, preserving query restrictions. Run focused and full offline suite; retain first failures. Update spec decisions; commit task files with issue reference.

### Task 3: Read-only policy deployment and operator proof

**Files:** Modify `docker-compose.yaml`, `.gitignore`, `.dockerignore`, relevant operator documentation/env sample/changelog; create `policy/write-policy.example.json`; tracked proof summary under `docs/`.
**Interfaces:** Consume policy load contract and runtime guards; expose separate read-only `/app/policy` mount beside writable `/app/config`.

- [x] Provide exact deny-all example `{ "version": 1, "guilds": {} }`; ignore actual policy files from Git and Docker contexts while retaining example. Never fabricate production IDs.
- [x] Mount `./policy:/app/policy:ro`, preserve containment settings, ensure no second writable policy alias in supplied Compose. Document mkdir/copy, user/role string-ID schema, restart-only changes, migration/old Discord overrides, watch list exposure, fail-closed writes/continuing Feeds, path constraints, safe override mount and threat limits.
```sh
mkdir -p policy
cp policy/write-policy.example.json policy/write-policy.json
```
- [x] Render Compose using dummy environment without reading `.env`. Verify policy readonly/state writable and existing non-root/read-only-root/drop-capabilities/no-new-privileges declarations. If Docker available, isolated dummy mount check only, no production stack; report unavailable evidence precisely.
- [x] Run `cargo fmt --check`, `cargo check`, `cargo test`, `cargo clippy -- -W clippy::all`. Preserve first failures and separate external-service gaps from policy failures. Verify no secrets or scratch content staged. Summarize proof and decisions durably; commit with issue reference.
