# 01 — Correct public-contract ESI health evidence

**What to build:** Make the Runtime Health Snapshot recognize current public-contract ESI progress from the evidence the collector actually persists, while preserving stale, paused, and absent-progress behavior and keeping health strictly detect-and-report only.

**Blocked by:** None — can start immediately.

**Status:** resolved

- [ ] A production-shaped health cycle with current public-contract cache or collection evidence reports current ESI progress rather than the deployed false-critical result.
- [ ] Missing or genuinely stale public-contract progress still degrades and becomes critical under the existing thresholds.
- [ ] An active persisted ESI pause remains healthy for the duration of its boundary and does not fabricate progress.
- [ ] Unrelated cache namespaces cannot satisfy public-contract ESI progress.
- [ ] The correction reuses the existing Runtime Health Snapshot and watchdog model without adding a telemetry namespace, schema, migration, page-progress ledger, or monitoring service.
- [ ] Watchdog detection persists and publishes evidence but cannot stop or restart services, change images, restore data, reverse migrations, or remediate runtime state.
- [ ] Existing heartbeat, regional progress, backlog, delivery, resolver, R2Z2, and feed-validation health behavior remains unchanged.
- [ ] Focused health tests, formatting, compilation, and diff checks pass.
- [ ] A Sol high Standards/Spec review finds no blocking issue before the ticket is resolved.

## Answer

- Commit: `c262d8f` — public-contract cache evidence drives ESI-progress health.
- RED: the new health-cycle regression reported `Critical` instead of expected `Healthy` (0/1).
- GREEN: focused health cycle tests 6/6; adjacent R2Z2 health telemetry 1/1.
- Checks: `cargo fmt --check`, `git diff --check`, and `cargo check` passed.
- Sol high Standards/Spec review: PASS, 0 findings.
