# 03 — Establish single-owner Terminal Resolution Recovery

**What to build:** Process due Awaiting Resolution cases through one bounded in-process recovery cadence independent of Regional Observation completion, preserving evidence-safe outcomes, ESI admission, and restart-safe Discord delivery.

**Blocked by:** None — can start immediately.

**Status:** claimed

- [ ] Regional completion and end-of-sweep processing no longer start due terminal item probes; the recovery cadence is the only terminal-resolution consumer.
- [ ] One recovery tick can run without public regional discovery and admits no more than the fixed conservative batch of approximately 32 oldest-due cases.
- [ ] The cadence runs approximately once per minute and skips missed ticks rather than replaying them as an immediate burst.
- [ ] A second tick continues with remaining due work without reprocessing cases resolved by the first tick.
- [ ] The existing persisted limiter is checked before and during recovery; an active pause, error-limit boundary, bucket boundary, or `Retry-After` stops the current tick safely.
- [ ] Available, accepted-player, `204`, `304`, `404`, expiry, inconclusive, timeout, malformed, `5xx`, and `429` outcomes retain their existing evidence and retry semantics.
- [ ] Backlog pressure cannot force-classify, discard, expire, or suppress an Awaiting Resolution case without the existing required public evidence.
- [ ] Sale Confirmed, Purchase Confirmed, Expired, and Closed — Outcome Unknown retain their notification identity, action, ping eligibility, observation context, restart safety, and at-most-one durable delivery.
- [ ] Public lifecycle probes remain unauthenticated; Structure Authorization remains restricted to player-structure operations.
- [ ] The implementation reuses existing durable cases, retry deadlines, public ESI client, terminal event construction, limiter, and delivery dispatcher.
- [ ] No schema, migration, process role, container, service, external queue, worker pool, lease, row-skipping coordination, tuning variable, or automatic remediation is introduced.
- [ ] Focused recovery, limiter, lifecycle, delivery, and cadence tests pass together with formatting, compilation, and diff checks.
- [ ] A Sol high Standards/Spec review finds no blocking issue before the ticket is resolved.
