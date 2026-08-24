# 03 — Establish single-owner Terminal Resolution Recovery

**What to build:** Process due Awaiting Resolution cases through one bounded in-process recovery cadence independent of Regional Observation completion, preserving evidence-safe outcomes, ESI admission, and restart-safe Discord delivery.

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] Regional completion and end-of-sweep processing no longer start due terminal item probes; the recovery cadence is the only terminal-resolution consumer.
- [x] One recovery tick can run without public regional discovery and admits no more than the fixed conservative batch of approximately 32 oldest-due cases.
- [x] The cadence runs approximately once per minute and skips missed ticks rather than replaying them as an immediate burst.
- [x] A second tick continues with remaining due work without reprocessing cases resolved by the first tick.
- [x] The existing persisted limiter is checked before and during recovery; an active pause, error-limit boundary, bucket boundary, or `Retry-After` stops the current tick safely.
- [x] Available, accepted-player, `204`, `304`, `404`, expiry, inconclusive, timeout, malformed, `5xx`, and `429` outcomes retain their existing evidence and retry semantics.
- [x] Backlog pressure cannot force-classify, discard, expire, or suppress an Awaiting Resolution case without the existing required public evidence.
- [x] Sale Confirmed, Purchase Confirmed, Expired, and Closed — Outcome Unknown retain their notification identity, action, ping eligibility, observation context, restart safety, and at-most-one durable delivery.
- [x] Public lifecycle probes remain unauthenticated; Structure Authorization remains restricted to player-structure operations.
- [x] The implementation reuses existing durable cases, retry deadlines, public ESI client, terminal event construction, limiter, and delivery dispatcher.
- [x] No schema, migration, process role, container, service, external queue, worker pool, lease, row-skipping coordination, tuning variable, or automatic remediation is introduced.
- [x] Focused recovery, limiter, lifecycle, delivery, and cadence tests pass together with formatting, compilation, and diff checks.
- [x] A Sol high Standards/Spec review finds no blocking issue before the ticket is resolved.

## Answer

- Implemented in `570d8cc` and remediated in `cf617d0`.
- RED/GREEN: replaced the private cadence proof and migrated all 30 stale lifecycle/delivery/evidence tests to the public discovery-then-recovery seam; focused tests passed individually.
- Captured serial real-PostgreSQL suite: 273/273 passed in 325.96s. `cargo fmt --check`, `cargo check`, and diff checks passed.
- Sol re-review: Spec PASS and Standards PASS; report naming and duplicated loop construction remain nonblocking judgments.
