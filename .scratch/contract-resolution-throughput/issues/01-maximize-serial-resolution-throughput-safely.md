# 01 — Maximize serial Resolution Throughput safely

**What to build:** Make Terminal Resolution Recovery use the highest safe capacity exposed by the current ESI legacy error window while remaining one serial owner. Recovery must preserve shared error headroom, spread Error-Charged Probes, reuse capacity after non-error responses, retain oldest-first evidence and delivery semantics, and fall back conservatively when authoritative headers are unavailable.

**Blocked by:** None — can start immediately

**Status:** claimed

- [ ] A production-shaped recovery seam proves an acceptance-confirming `403` establishes the correct terminal outcome while counting as an Error-Charged Probe for admission.
- [ ] Recovery admits no new probe when authoritative current-window remaining error allowance is 40 or lower.
- [ ] Recovery resumes from the oldest remaining due case after the authoritative reset.
- [ ] Every probe start is separated by at least 250 milliseconds.
- [ ] The next probe after an Error-Charged Probe starts at least one second later.
- [ ] A non-error-charged `2xx` or `3xx` response makes the next serial probe eligible after the universal 250-millisecond minimum.
- [ ] A response without legacy error headers does not erase a valid persisted observation from the current window.
- [ ] With no valid current-window error evidence, recovery admits at most 32 probes evenly across one minute.
- [ ] Each pass selects at most 256 oldest-due cases and starts no new probe after its 60-second deadline.
- [ ] A probe already in progress at the deadline completes its response, state transition, and any durable delivery.
- [ ] Unprocessed selected cases remain due without claims, leases, or other mutation.
- [ ] Shared limiter consumption by Regional Observation or another ESI path reduces recovery capacity authoritatively.
- [ ] Existing persisted pause, bucket pacing, `429`, `Retry-After`, database-failure, and durable-delivery boundaries remain fail-closed.
- [ ] Terminal evidence, observation context, notification identity, ping eligibility, nonce behavior, delivery-before-next-probe ordering, and restart safety remain unchanged.
- [ ] Regional Observation still performs no due terminal probes; Terminal Resolution Recovery remains the sole consumer.
- [ ] Public terminal probes remain unauthenticated and Structure Authorization behavior remains unchanged.
- [ ] One ordinary structured summary reports attempted, resolved, Error-Charged Probe count, ending remaining allowance, stop reason, and elapsed time without new persisted telemetry.
- [ ] The implementation adds no concurrency, task fan-out, worker pool, channel, semaphore, token bucket, adaptive framework, schema, migration, lease, service, process, or configuration variable.
- [ ] Focused recovery, limiter, lifecycle, delivery, health, and restart regressions pass.
- [ ] Domain glossary and recovery ADR changes are included without staging unrelated existing edits.
