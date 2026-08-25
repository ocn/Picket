# 01 — Maximize serial Resolution Throughput safely

**What to build:** Make Terminal Resolution Recovery use the highest safe capacity exposed by the current ESI legacy error window while remaining one serial owner. Recovery must preserve shared error headroom, spread Error-Charged Probes, reuse capacity after non-error responses, retain oldest-first evidence and delivery semantics, and fall back conservatively when authoritative headers are unavailable.

**Blocked by:** None — can start immediately

**Status:** claimed

- [x] A production-shaped recovery seam proves an acceptance-confirming `403` establishes the correct terminal outcome while counting as an Error-Charged Probe for admission.
- [x] Recovery admits no new probe when authoritative current-window remaining error allowance is 40 or lower.
- [x] Recovery resumes from the oldest remaining due case after the authoritative reset.
- [x] Every probe start is separated by at least 250 milliseconds.
- [x] The next probe after an Error-Charged Probe starts at least one second later.
- [x] A non-error-charged `2xx` or `3xx` response makes the next serial probe eligible after the universal 250-millisecond minimum.
- [x] A response without legacy error headers does not erase a valid persisted observation from the current window.
- [x] With no valid current-window error evidence, recovery admits at most 32 probes evenly across one minute.
- [x] Each pass selects at most 256 oldest-due cases and starts no new probe after its 60-second deadline.
- [x] A probe already in progress at the deadline completes its response, state transition, and any durable delivery.
- [x] Unprocessed selected cases remain due without claims, leases, or other mutation.
- [x] Shared limiter consumption by Regional Observation or another ESI path reduces recovery capacity authoritatively.
- [x] Existing persisted pause, bucket pacing, `429`, `Retry-After`, database-failure, and durable-delivery boundaries remain fail-closed.
- [x] Terminal evidence, observation context, notification identity, ping eligibility, nonce behavior, delivery-before-next-probe ordering, and restart safety remain unchanged.
- [x] Regional Observation still performs no due terminal probes; Terminal Resolution Recovery remains the sole consumer.
- [x] Public terminal probes remain unauthenticated and Structure Authorization behavior remains unchanged.
- [x] One ordinary structured summary reports attempted, resolved, Error-Charged Probe count, ending remaining allowance, stop reason, and elapsed time without new persisted telemetry.
- [x] The implementation adds no concurrency, task fan-out, worker pool, channel, semaphore, token bucket, adaptive framework, schema, migration, lease, service, process, or configuration variable.
- [x] Focused recovery, limiter, lifecycle, delivery, health, and restart regressions pass.
- [x] Domain glossary and recovery ADR changes are included without staging unrelated existing edits.

## Remediation evidence

- The follow-up preserves fixed-window legacy error evidence, performs terminal admission atomically in the existing limiter transaction, measures recovery spacing and deadline with Tokio `Instant`, and emits one final recovery summary through a doc-hidden test seam.
- Production-shaped recovery tests cover controlled accepted-player `403`, conditional `304` validation, and an in-flight deadline probe whose prepared delivery is observed before the pass completes.
- Focused exact recovery, limiter, lifecycle, delivery, health, and restart checks passed; `cargo fmt --check`, `cargo check`, and `git diff --check` passed. No full integration suite was run.
- The cadence check `terminal_recovery_runtime_reconnects_and_skips_missed_fixed_minute_ticks` first exited 101 after 12.75 seconds, then passed unchanged under authorized diagnostic capture in 10.83 seconds; no product change was made for the non-reproducing timing flake.
- Tests used only named disposable PostgreSQL 16 containers on loopback high ports `55488` and later `55489`; both were removed after their exact checks.
