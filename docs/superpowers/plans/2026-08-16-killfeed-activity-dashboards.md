# Discord Killfeed Activity Dashboards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one alert-safe, durable, Discord-native killfeed activity dashboard for the configured `sah-8ly` category in `#sandbox`.

**Architecture:** A deep Rust activity module accepts one non-blocking observation only after ordinary source sends finish. It owns strict configuration, PostgreSQL facts, session aggregation, a pure Python/Pillow renderer, and a serialized Discord lifecycle; source alerts share none of its queues, locks, storage, or subprocesses.

**Tech Stack:** Rust 1.88, Tokio, SQLx/PostgreSQL 16, Serenity 0.11.7 plus a proven current-pin route solution, Python >=3.10, Pillow 12.3.0, Discord HTTP/Gateway.

## Global Constraints

- Implement [the production specification](../../../.scratch/killfeed-activity-dashboards/spec.md) exactly; unresolved behavior returns to Wayfinder.
- Start production work in a new worktree from main. Never merge or implement on `prototype/killfeed-dashboard-ui-20260815`; use Variant C at `2560fd3c38f97f1ec053c7d540b72f9ffb0dc5a1` read-only.
- Execute Tasks 1–7 strictly in order. Do not begin a task until the prior task is implemented, verified, reviewed, corrected, and committed.
- For each task, spawn one fresh `gpt-5.6-terra` xhigh implementer using `mattpocock-skills:tdd`; after implementation, spawn one `gpt-5.6-sol` high reviewer using `mattpocock-skills:code-review`. The reviewer checks both spec and repository standards. Apply findings, rerun verification, and obtain a clean final review before proceeding.
- Dashboard work occurs only after ordinary source Discord sends, ping eligibility, and the five-minute channel cooldown. No activity await, lock, database call, Python process, or Discord request enters that path.
- `ACTIVITY_DASHBOARDS_ENABLED` defaults to `false`; activity startup failure never prevents existing killfeed or Contract Intelligence startup.
- Contract Subscriptions, contract channels, correlation, Historical Activity Rank, buttons, and external UI are excluded.
- Every PostgreSQL integration test uses a unique isolated database and `--test-threads=1`.
- The one ignored live Discord test requires explicit approval and cleans up only the messages it creates in `#sandbox`.

---

### Task 1: Alert-safe observation tracer

**Files:**
- Create: `src/activity/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/pipeline.rs`
- Test: `src/activity/mod.rs`
- Test: `src/pipeline.rs`

**Interfaces:**
- Consumes: existing `ProcessedResult::Matched`, `PreparedDispatch`, source ping cooldown, and `Arc<Http>` send path.
- Produces: `ActivityDashboardHandle::try_observe(ActivityObservation) -> HandoffOutcome`, `ActivityDiagnostics`, and one observation per guild/killmail after all source sends.

- [ ] **Step 1: Write failing handle tests**

```rust
#[test]
fn full_activity_queue_returns_immediately_without_waiting() {
    let (handle, mut rx) = ActivityDashboardHandle::test_channel(1);
    assert_eq!(handle.try_observe(observation(100, &[10])), HandoffOutcome::Accepted);
    assert_eq!(handle.try_observe(observation(101, &[10])), HandoffOutcome::Full);
    assert_eq!(rx.try_recv().unwrap().killmail_id, 100);
    assert_eq!(handle.diagnostics().handoff_drops, 1);
}

#[test]
fn disabled_handle_is_a_non_blocking_noop() {
    let handle = ActivityDashboardHandle::disabled();
    assert_eq!(handle.try_observe(observation(100, &[10])), HandoffOutcome::Disabled);
}
```

- [ ] **Step 2: Run the focused tests and confirm red**

Run: `cargo test activity::tests:: --lib`

Expected: compilation fails because `ActivityDashboardHandle`, `ActivityObservation`, and `HandoffOutcome` do not exist.

- [ ] **Step 3: Implement the bounded external seam**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffOutcome { Accepted, Disabled, Full, Closed }

#[derive(Clone, Debug)]
pub struct ActivityObservation {
    pub dispatch_sequence: u64,
    pub guild_id: u64,
    pub killmail_id: i64,
    pub killmail_time: DateTime<Utc>,
    pub observed_total_value_isk: f64,
    pub zkill_url: String,
    pub feed_provider: String,
    pub sources: Vec<ActivitySourceMatch>,
}

impl ActivityDashboardHandle {
    pub fn try_observe(&self, value: ActivityObservation) -> HandoffOutcome {
        match &self.sender {
            None => HandoffOutcome::Disabled,
            Some(sender) => match sender.try_send(ActivityInput::Observation(value)) {
                Ok(()) => HandoffOutcome::Accepted,
                Err(TrySendError::Full(_)) => { self.drops.fetch_add(1, Ordering::Relaxed); HandoffOutcome::Full }
                Err(TrySendError::Closed(_)) => HandoffOutcome::Closed,
            },
        }
    }
}
```

- [ ] **Step 4: Write failing pipeline ordering/dedup tests**

```rust
#[tokio::test]
async fn activity_handoff_occurs_once_after_all_source_sends() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sender = RecordingSourceSender::new(events.clone());
    let observer = RecordingActivityObserver::new(events.clone());
    dispatch_matched_for_test(two_subscriptions_same_source_and_one_other(), &sender, &observer).await;
    assert_eq!(*events.lock().await, vec!["send:10", "send:10", "send:11", "observe"]);
    assert_eq!(observer.observations()[0].sources.iter().map(|s| s.channel_id).collect::<Vec<_>>(), vec![10, 11]);
}

#[tokio::test]
async fn full_activity_queue_does_not_change_source_delivery() {
    let sender = RecordingSourceSender::default();
    let observer = AlwaysFullActivityObserver;
    dispatch_matched_for_test(one_here_dispatch(), &sender, &observer).await;
    assert_eq!(sender.contents(), vec![Some("@here".to_string())]);
}
```

- [ ] **Step 5: Refactor dispatch to return outcomes and hand off once**

`send_prepared_dispatch` returns a `SourceDeliveryOutcome` without changing ping calculation. Group completed dispatch outcomes by guild, canonicalize matching subscriptions, deduplicate source IDs, then call `try_observe` exactly once after the final source send.

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceDeliveryOutcome { Sent { mention: Option<String> }, Failed { detail: String } }

for dispatch in dispatches {
    let outcome = send_prepared_dispatch(dispatch.clone(), app_state, http_client).await;
    observations.entry(dispatch.guild_id).or_default().push((dispatch, outcome));
}
for observation in observations.into_values().map(ActivityObservation::from_dispatches) {
    let outcome = activity.try_observe(observation);
    tracing::debug!(?outcome, "activity observation handoff");
}
```

- [ ] **Step 6: Verify the tracer and existing alert tests**

Run: `cargo test activity::tests:: pipeline::tests:: --lib`

Expected: pass; tests prove send/ping ordering and bounded handoff behavior.

- [ ] **Step 7: Commit after Terra implementation and clean Sol review**

```bash
git add src/activity/mod.rs src/lib.rs src/pipeline.rs
git commit -m "feat: add alert-safe activity handoff"
```

---

### Task 2: Versioned category configuration tracer

**Files:**
- Create: `src/activity/config.rs`
- Create: `src/activity/domain.rs`
- Create: `config/activity_dashboards/888224317991706685.json`
- Modify: `src/activity/mod.rs`
- Test: `src/activity/config.rs`
- Test: `src/activity/domain.rs`

**Interfaces:**
- Consumes: `ActivityObservation`, ordinary subscription snapshots, Discord guild-channel inventory, UTC clock.
- Produces: `ValidatedActivityConfig`, last-known-good/pending revision state, category-resolved observations, exact session/bin functions.

- [ ] **Step 1: Write strict parsing and invariant tests**

```rust
#[test]
fn rejects_unknown_fields_and_duplicate_source_owners() {
    assert!(parse_config(r#"{"schema_version":1,"guild_id":"1","dashboard_channel_id":"2","revision":1,"categories":[],"typo":true}"#).is_err());
    let error = validate_config(config_with_source_in_two_categories(10), &inventory()).unwrap_err();
    assert_eq!(error.code, "duplicate_source_owner");
}

#[test]
fn invalid_reload_keeps_last_known_good() {
    let mut state = ConfigState::default();
    state.accept(valid_revision(1), at("2026-08-16T10:00:00Z")).unwrap();
    assert!(state.accept(invalid_revision(2), at("2026-08-16T10:01:00Z")).is_err());
    assert_eq!(state.last_known_good().unwrap().revision, 1);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test activity::config::tests:: activity::domain::tests:: --lib`

Expected: compilation fails because the configuration and domain modules do not exist.

- [ ] **Step 3: Implement schema, whole-config validation, and activation**

Use `#[serde(deny_unknown_fields)]` on every config record. Semantic revisions activate at the next 11:00 UTC; names/order apply immediately; the latest valid pending revision supersedes older pending revisions.

```rust
pub fn session_start(event_at: DateTime<Utc>) -> DateTime<Utc> {
    let boundary = event_at.date_naive().and_hms_opt(11, 0, 0).unwrap().and_utc();
    if event_at < boundary { boundary - chrono::Duration::days(1) } else { boundary }
}

pub fn bin_start(event_at: DateTime<Utc>) -> DateTime<Utc> {
    let start = session_start(event_at);
    let index = (event_at - start).num_minutes() / 15;
    start + chrono::Duration::minutes(index * 15)
}
```

- [ ] **Step 4: Add table tests for all boundary states**

```rust
#[test]
fn event_time_table_has_96_half_open_bins() {
    for (value, expected_session, expected_bin) in [
        ("2026-08-16T10:59:59Z", "2026-08-15T11:00:00Z", "2026-08-16T10:45:00Z"),
        ("2026-08-16T11:00:00Z", "2026-08-16T11:00:00Z", "2026-08-16T11:00:00Z"),
        ("2026-08-17T10:59:59Z", "2026-08-16T11:00:00Z", "2026-08-17T10:45:00Z"),
    ] { assert_eq!((session_start(at(value)), bin_start(at(value))), (at(expected_session), at(expected_bin))); }
}
```

- [ ] **Step 5: Install the exact disabled-by-default pilot file**

Copy the JSON from the specification, but set the category state to `disabled` until the live pilot task. Validate IDs against a fake inventory in unit tests and against Discord only in the ignored integration test.

- [ ] **Step 6: Verify and commit after review**

Run: `cargo test activity::config::tests:: activity::domain::tests:: --lib`

```bash
git add src/activity config/activity_dashboards/888224317991706685.json
git commit -m "feat: add versioned activity categories"
```

---

### Task 3: Durable activity ledger tracer

**Files:**
- Create: `activity_migrations/20260816000000_create_activity_dashboard.sql`
- Create: `src/activity/store.rs`
- Create: `tests/test_activity_dashboard_store.rs`
- Create: `src/activity/runtime.rs`
- Modify: `src/activity/mod.rs`

**Interfaces:**
- Consumes: validated category-resolved observations.
- Produces: `ActivityStore::record_observation`, idempotent facts/source/category events, sessions/bins, gaps, heartbeat, dirty revisions, retention.

- [ ] **Step 1: Write failing PostgreSQL integration tests**

```rust
#[tokio::test]
async fn repeated_subscription_and_feed_observations_count_once() {
    let store = test_store().await;
    store.record_observation(overlapping_observation()).await.unwrap();
    store.record_observation(overlapping_observation()).await.unwrap();
    let bin = store.bin_summary(888224317991706685, "sah-8ly", at("2026-08-16T11:00:00Z")).await.unwrap();
    assert_eq!((bin.unique_killmails, bin.source_events), (1, 2));
}

#[tokio::test]
async fn restart_gap_turns_overlaid_zero_into_incomplete() {
    let store = test_store().await;
    store.heartbeat(at("2026-08-16T12:00:00Z")).await.unwrap();
    store.recover(at("2026-08-16T12:05:00Z")).await.unwrap();
    assert_eq!(store.coverage(at("2026-08-16T12:00:00Z")).await.unwrap(), Coverage::Gap);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `ACTIVITY_TEST_DATABASE_URL=postgres://killbot_activity:killbot_activity@127.0.0.1:55434/killbot_activity cargo test --test test_activity_dashboard_store -- --test-threads=1`

Expected: compilation fails because `ActivityStore` and the activity schema do not exist.

- [ ] **Step 3: Create the exact schema and constraints**

Create every table/key named in the specification. Store snowflakes as `BIGINT CHECK (> 0)`, event times as `TIMESTAMPTZ`, observed value as `NUMERIC(30,2)`, snapshots as `JSONB`, and lifecycle values with `CHECK` constraints. Use a unique category-event key and a unique source-event key.

```sql
PRIMARY KEY (guild_id, category_id, killmail_id),
FOREIGN KEY (killmail_id) REFERENCES activity_killmails(killmail_id)
```

- [ ] **Step 4: Implement one observation transaction**

```rust
pub async fn record_observation(&self, observation: ResolvedActivityObservation) -> Result<RecordOutcome, ActivityStoreError> {
    let mut tx = self.pool.begin().await?;
    self.insert_killmail(&mut tx, &observation).await?;
    self.insert_subscription_snapshots(&mut tx, &observation).await?;
    let inserted = self.insert_source_and_category_events(&mut tx, &observation).await?;
    self.rebuild_affected_bins_and_dirty_sessions(&mut tx, &observation).await?;
    tx.commit().await?;
    Ok(RecordOutcome { inserted })
}
```

- [ ] **Step 5: Add late-event, regime, retention, and rollback tests**

Tests assert that late events dirty the old session, filter changes mark `definition_changed`, raw rows older than 400 days are removed, summaries/config/messages survive retention, and migration re-run is safe.

- [ ] **Step 6: Verify and commit after review**

Run the focused database command from Step 2; expected all activity-store tests pass.

```bash
git add activity_migrations src/activity tests/test_activity_dashboard_store.rs
git commit -m "feat: persist activity observations"
```

---

### Task 4: Variant-C renderer tracer

**Files:**
- Create: `scripts/render_activity_dashboard.py`
- Create: `scripts/tests/test_render_activity_dashboard.py`
- Create: `resources/activity_dashboard/render-v1.json`
- Create: `requirements/activity-dashboard.txt`
- Create: `src/activity/render.rs`
- Modify: `src/activity/domain.rs`
- Modify: `Dockerfile`

**Interfaces:**
- Consumes: `ActivityRenderV1` JSON from durable session/bin/source state.
- Produces: deterministic 1000 px PNG bytes under 2 MiB, or a typed render failure within 10 seconds.

- [ ] **Step 1: Write failing Python contract tests**

```python
class RenderTests(unittest.TestCase):
    def test_fixed_fixture_is_deterministic_png(self):
        payload = json.loads(Path("resources/activity_dashboard/render-v1.json").read_text())
        first = render(payload)
        second = render(payload)
        self.assertEqual(first, second)
        image = Image.open(io.BytesIO(first))
        self.assertEqual(image.width, 1000)
        self.assertLessEqual(image.height, 1600)
        self.assertLessEqual(len(first), 2 * 1024 * 1024)

    def test_gap_partial_and_zero_have_distinct_non_color_pixels(self):
        image = Image.open(io.BytesIO(render(fixture()))).convert("RGB")
        self.assertNotEqual(marker_pattern(image, "gap"), marker_pattern(image, "partial"))
        self.assertNotEqual(marker_pattern(image, "partial"), marker_pattern(image, "zero"))
```

- [ ] **Step 2: Run and confirm red**

Run: `python3 -m unittest discover -s scripts/tests -p 'test_render_activity_dashboard.py'`

Expected: import/file failure because the renderer does not exist.

- [ ] **Step 3: Implement pure Pillow Variant C**

Pin `Pillow==12.3.0`; load bundled DejaVu Sans; read one JSON object from stdin; write PNG only to stdout. Draw 96 unique-count bins, explicit gap/partial patterns, summary, source IDs, and routing text. Do not import Matplotlib, access network, or write files.

- [ ] **Step 4: Write failing Rust subprocess tests**

```rust
#[tokio::test]
async fn renderer_rejects_timeout_and_oversized_output() {
    assert_eq!(fake_renderer(RenderBehavior::Sleep).render(model()).await.unwrap_err().kind(), RenderErrorKind::Timeout);
    assert_eq!(fake_renderer(RenderBehavior::Bytes(vec![0; 2 * 1024 * 1024 + 1])).render(model()).await.unwrap_err().kind(), RenderErrorKind::TooLarge);
}
```

- [ ] **Step 5: Implement Rust validation and runtime dependencies**

Pipe JSON/stdout/stderr, kill at 10 seconds, cap output, validate PNG signature/IHDR/dimensions, and hash bytes. Install Python >=3.10, the pinned requirements, and DejaVu fonts in the runtime image.

- [ ] **Step 6: Verify and commit after visual/review gate**

Run: Python tests, `cargo test activity::render::tests:: --lib`, and render the fixture to `/private/tmp/killfeed-activity-variant-c.png` for manual image inspection.

```bash
git add scripts resources/activity_dashboard requirements/activity-dashboard.txt src/activity Dockerfile
git commit -m "feat: render activity dashboard png"
```

---

### Task 5: Durable Discord-message tracer

**Files:**
- Create: `src/activity/discord.rs`
- Create: `tests/test_activity_dashboard_discord.rs`
- Modify: `src/activity/runtime.rs`
- Modify: `src/discord_bot.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: one dirty rendered session, persisted message state, shared `Arc<Http>`.
- Produces: exactly one notification-suppressed bot message with replaceable PNG, persisted identity, current pin route, and deletion recovery.

- [ ] **Step 1: Write fake-Discord lifecycle tests**

```rust
#[tokio::test]
async fn create_then_edit_replaces_one_attachment_without_mentions() {
    let discord = FakeActivityDiscord::default();
    let lifecycle = lifecycle(test_store().await, discord.clone());
    lifecycle.reconcile(session_revision(1)).await.unwrap();
    lifecycle.reconcile(session_revision(2)).await.unwrap();
    assert_eq!(discord.creates().len(), 1);
    assert_eq!(discord.edits().len(), 1);
    assert!(discord.creates()[0].suppress_notifications);
    assert!(discord.creates()[0].allowed_mentions.is_empty());
    assert_eq!(discord.edits()[0].attachments.len(), 1);
}

#[tokio::test]
async fn concurrent_unknown_message_recovery_creates_one_replacement() {
    let lifecycle = lifecycle_with_unknown_message();
    tokio::join!(lifecycle.reconcile(revision(2)), lifecycle.reconcile(revision(3)));
    assert_eq!(lifecycle.discord().replacement_creates(), 1);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `ACTIVITY_TEST_DATABASE_URL=postgres://killbot_activity:killbot_activity@127.0.0.1:55434/killbot_activity cargo test --test test_activity_dashboard_discord -- --test-threads=1`

Expected: compilation fails because the Discord lifecycle/port does not exist.

- [ ] **Step 3: Implement create/edit/recovery through one internal port**

Production uses shared Serenity HTTP for message operations. Persist returned IDs before pin. Empty allowed mentions and suppression are mandatory on create; edit omits flags and replaces the sole attachment. Compare-and-swap replacement identity on `10008`.

- [ ] **Step 4: Implement and prove current pin routes**

Add current paginated get-pin/pin/unpin behavior with centralized rate-limit handling. Serenity 0.11's deprecated route is not accepted as the completed production adapter. If this requires a Serenity upgrade or maintained patch, migrate all compile errors and run the broad suite before proceeding.

- [ ] **Step 5: Add one ignored `#sandbox` integration test**

```rust
#[tokio::test]
#[ignore = "creates, edits, pins, unpins, and deletes one #sandbox activity test message"]
async fn sandbox_message_lifecycle_uses_current_routes() {
    run_sandbox_lifecycle(1538030920328810536).await.unwrap();
}
```

The test uses explicit empty mentions, suppressed notifications, a deterministic test PNG, and cleanup scoped to its returned message ID.

- [ ] **Step 6: Verify and commit after approved live proof and review**

Run fake tests first. Run the ignored test only after explicit approval. Then run `cargo test --lib -- --skip esi::tests::test_esi_timeout_is_configured`.

```bash
git add Cargo.toml Cargo.lock src/activity src/discord_bot.rs src/lib.rs tests/test_activity_dashboard_discord.rs
git commit -m "feat: maintain activity dashboard message"
```

---

### Task 6: Cadence, rollover, and recovery tracer

**Files:**
- Modify: `src/activity/runtime.rs`
- Modify: `src/activity/store.rs`
- Modify: `src/activity/discord.rs`
- Modify: `src/discord_bot.rs`
- Test: `src/activity/runtime.rs`
- Test: `tests/test_activity_dashboard_store.rs`
- Test: `tests/test_activity_dashboard_discord.rs`

**Interfaces:**
- Consumes: durable dirty revisions, UTC clock, renderer, Discord events/errors.
- Produces: one-minute coalescing, hash skip, 11:00 rollover, late correction, gaps, restart convergence, bounded retry.

- [ ] **Step 1: Write paused-time scheduler tests**

```rust
#[tokio::test(start_paused = true)]
async fn many_revisions_in_a_minute_publish_only_latest() {
    let runtime = test_runtime();
    for revision in 1..=20 { runtime.mark_dirty(revision).await; }
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(runtime.discord().published_revisions(), vec![20]);
}

#[tokio::test(start_paused = true)]
async fn downtime_finalizes_old_before_creating_new() {
    let runtime = test_runtime_at("2026-08-17T10:59:59Z");
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(runtime.discord().operations(), vec!["finalize-old", "create-new", "unpin-old", "pin-new"]);
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test activity::runtime::tests:: --lib`

Expected: new cadence/rollover assertions fail.

- [ ] **Step 3: Implement serialized scheduling and backoff**

Tick at UTC minute boundaries, maintain one queued refresh/category, coalesce desired revisions, hash-skip, and share one low-concurrency dashboard semaphore. `30046`, 429, 5xx, permission errors, invalid PNG, and DB failures preserve newest desired state with bounded exponential backoff; they never create source-path work.

- [ ] **Step 4: Wire Gateway reconciliation events**

Forward relevant message delete, channel update/delete, pins update, and guild availability events through `try_discord_event`. Startup REST reconciliation remains authoritative.

- [ ] **Step 5: Add restart/late/gap/permission recovery tests**

Tests cover commit-before-publish crash, deleted live message, renderer timeout, 30-second database outage, lost/restored dashboard permission, late prior-session event, stale heartbeat, and convergence to one current message within two successful ticks.

- [ ] **Step 6: Verify and commit after review**

Run all activity unit/store/Discord tests serially where PostgreSQL is used.

```bash
git add src/activity src/discord_bot.rs tests/test_activity_dashboard_store.rs tests/test_activity_dashboard_discord.rs
git commit -m "feat: reconcile activity dashboard sessions"
```

---

### Task 7: Operational pilot package

**Files:**
- Modify: `src/commands/diag.rs`
- Modify: `src/config.rs`
- Modify: `src/lib.rs`
- Modify: `docker-compose.yaml`
- Modify: `docs/env.sample`
- Create: `docs/killfeed-activity-dashboards.md`
- Create: `docs/runbooks/killfeed-activity-dashboard-pilot.md`
- Modify: `config/activity_dashboards/888224317991706685.json`
- Test: `src/commands/diag.rs`
- Test: `tests/test_activity_dashboard_store.rs`

**Interfaces:**
- Consumes: `ActivityDiagnostics`, feature env, completed recovery behavior, reviewed pilot configuration.
- Produces: disabled-by-default deployment, read-only diagnostics, retention job evidence, seven-session pilot/rollback runbook.

- [ ] **Step 1: Write failing feature-state and diagnostic tests**

```rust
#[test]
fn activity_is_disabled_without_explicit_true() {
    assert!(!ActivityRuntimeConfig::from_map(HashMap::new()).enabled);
}

#[tokio::test]
async fn diag_reports_activity_without_secrets() {
    let text = render_activity_diag(sample_diagnostics());
    assert!(text.contains("sah-8ly"));
    assert!(text.contains("queue drops: 0"));
    assert!(!text.contains("postgres://"));
    assert!(!text.contains("token"));
}
```

- [ ] **Step 2: Run and confirm red**

Run: `cargo test commands::diag::tests:: config::tests:: --lib`

Expected: diagnostic/config assertions fail because activity env/diagnostics are not wired.

- [ ] **Step 3: Add exact env and runtime packaging**

`docs/env.sample` and Compose expose `ACTIVITY_DASHBOARDS_ENABLED=false`, `ACTIVITY_DATABASE_URL`, `ACTIVITY_CONFIG_DIR`, and `ACTIVITY_RENDERER_PATH`. Reuse the PostgreSQL container only by explicit URL choice; code remains independent from `CONTRACT_DATABASE_URL`.

- [ ] **Step 4: Add read-only diagnostics and retention evidence**

Extend `/diag` through `ActivityDashboardContainer` without changing command state. Run retention daily in bounded batches and expose its watermark/counters.

- [ ] **Step 5: Write the operator and pilot runbooks**

Document configuration revisions, exact permissions including `PIN_MESSAGES`, Python/PostgreSQL checks, dashboard/source routing, 11:00 rollover, controlled recovery drills, feature-flag rollback, manual unpin, and every ticket-09 evidence field. The runbook requires seven sessions and one natural eligible `@here`; it prohibits synthetic live mentions.

- [ ] **Step 6: Enable only the reviewed pilot category for the pilot release**

Change `sah-8ly` from `disabled` to `enabled` only in the release configuration used for the explicitly approved pilot. No other category is present.

- [ ] **Step 7: Run final verification**

```bash
cargo check
cargo clippy -- -W clippy::all
ACTIVITY_TEST_DATABASE_URL=postgres://killbot_activity:killbot_activity@127.0.0.1:55434/killbot_activity CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:55433/killbot_contracts cargo test -- --skip esi::tests::test_esi_timeout_is_configured --test-threads=1
python3 -m unittest discover -s scripts/tests -p 'test_render_activity_dashboard.py'
git diff --check
```

Expected: all non-credentialed tests pass, visual Discord suites remain ignored, Clippy exits zero, and diff check is clean.

- [ ] **Step 8: Commit after final Sol review**

```bash
git add src/commands/diag.rs src/config.rs src/lib.rs docker-compose.yaml docs config/activity_dashboards/888224317991706685.json
git commit -m "docs: prepare activity dashboard pilot"
```

After this commit, begin the operational pilot only with explicit deployment approval. Passing the seven-session runbook is a later promotion decision, not an implementation-complete claim.
