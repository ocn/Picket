# Maximize Safe Terminal Resolution Throughput

Status: ready-for-agent
Date: 2026-08-25

## Problem Statement

Terminal Resolution Recovery is healthy and drains Awaiting Resolution cases independently of Regional Observation, but its reviewed fixed admission of 32 cases per minute is now the dominant latency. Live batches process 32 cases serially in roughly 7–9 seconds and then leave most of the minute idle. With roughly 29,000 due cases, the current ideal clearance time is about 15 hours even though PostgreSQL, Discord delivery, and active request latency have unused capacity.

Naively increasing the batch or adding concurrency would be unsafe. The public contract-items route currently exposes the legacy ESI error allowance rather than a route bucket. ESI permits at most 100 non-`2xx`/`3xx` responses per minute before cross-route throttling. Approximately 80% of recent terminal resolutions use an acceptance-confirming `403`: this is valid Acceptance Confirmed evidence to the application but remains an Error-Charged Probe to ESI. Sustained 30–100 probes per second would exhaust the shared allowance within seconds and could disrupt unrelated ESI work.

The operator wants the highest safe Resolution Throughput with the smallest design delta. Recovery must remain one serial in-process owner, use the existing persisted limiter and durable delivery path, preserve error headroom for other ESI work, and introduce no new service, schema, worker pool, concurrency system, lease, queue, configuration surface, or automated remediation. The already-deployed recovery remains healthy; this change is a reviewed fix-forward before the pending Production Verification, not authority to touch Historical Enrichment Repair.

## Solution

Replace the fixed 32-case burst with error-budget-aware serial pacing inside the existing Terminal Resolution Recovery owner. Each one-minute pass selects up to 256 oldest-due cases and processes them serially until the minute deadline or an authoritative limiter boundary. Every probe start is separated by at least 250 milliseconds. An Error-Charged Probe imposes at least one second before the next start; a non-error-charged `2xx`/`3xx` response makes the next probe eligible after only the 250-millisecond minimum.

The latest valid current-window `X-ESI-Error-Limit-Remain` and reset evidence is authoritative. Recovery admits no new probe when remaining allowance is 40 or lower, leaving at least 40 shared error responses for Regional Observation and other ESI work. It resumes after reset. A response without error-limit headers does not erase valid evidence from the current window. When no valid current-window evidence exists, recovery falls back to 32 probes evenly spread over one minute.

The owner remains serial and preserves oldest-first admission, evidence-safe outcome classification, delivery-before-next-probe behavior, restart safety, idempotency, and all existing limiter and `Retry-After` handling. A probe already in progress at the minute deadline may finish; no new probe starts afterward. Unprocessed selected rows remain due without claims or leases.

Correct the stale manual Production Verification preflight so its non-sending assertions use the current canonical terminal presentation. Prove that preflight through a non-ignored test before invoking the ignored transport seam. After implementation, full verification, and Sol-high Standards and Spec review, deploy the fix-forward once with a fresh backup and no automatic rollback. Observe ten complete one-minute error windows. If the gate passes, send exactly one controlled non-pinging Production Verification message to testing channel `1115807643748012072`. Historical Enrichment Repair remains untouched.

## User Stories

1. As an operator, I want Terminal Resolution Recovery to use otherwise-idle safe capacity, so that retained debt clears sooner without threatening ESI availability.
2. As an operator, I want Resolution Throughput measured as durably committed terminal outcomes, so that request starts and Discord alerts cannot misrepresent useful progress.
3. As an operator, I want recovery constrained by the current upstream API behavior, so that an aspirational throughput number cannot override authoritative ESI limits.
4. As an ESI operator, I want an acceptance-confirming `403` treated as an Error-Charged Probe, so that semantically successful resolution still consumes the correct shared allowance.
5. As an ESI operator, I want at least 40 legacy error responses retained for unrelated traffic, so that contract recovery cannot monopolize the shared error window.
6. As an ESI operator, I want recovery to stop admitting work at or below the remaining-error floor, so that it cannot knowingly drive the allowance to zero.
7. As an ESI operator, I want recovery to resume after the authoritative reset, so that safe backpressure does not permanently stall the queue.
8. As an ESI operator, I want valid current-window error metadata retained when one response omits headers, so that transient metadata absence cannot erase known safety evidence.
9. As an ESI operator, I want a conservative fallback when no current-window headers exist, so that lifecycle recovery continues without guessing at available capacity.
10. As an ESI operator, I want fallback probes spread across the minute, so that missing metadata cannot create a burst.
11. As an ESI operator, I want at least 250 milliseconds between every probe start, so that successful-response streaks remain bounded and spread.
12. As an ESI operator, I want Error-Charged Probes spaced by at least one second, so that recovery cannot consume more than 60 of its reserved error slots per minute.
13. As an operator, I want non-error-charged responses to release capacity promptly, so that safe `200`, `204`, and `304` outcomes are not needlessly limited to one per second.
14. As an operator, I want the latest persisted limiter state shared with Regional Observation, so that all public ESI work respects one source of admission truth.
15. As an operator, I want an active `Retry-After` or bucket boundary to stop the current pass, so that error-aware pacing does not weaken existing limiter behavior.
16. As an operator, I want expected yielding at the 40 floor distinguished from a regression, so that correct backpressure does not trigger unnecessary remediation.
17. As an operator, I want admission below the floor treated as a regression, so that a broken pacing calculation halts rollout work visibly.
18. As an operator, I want failure to resume after reset treated as a regression, so that safe backpressure cannot silently become permanent backlog.
19. As an operator, I want oldest-due cases admitted first, so that acceleration preserves deterministic fairness.
20. As an operator, I want up to 256 oldest-due cases available to one pass, so that successful responses are not capped by a 60-row selection.
21. As an operator, I want each pass bounded to one minute, so that the scheduler refreshes current due order and limiter evidence regularly.
22. As an operator, I want an already-running probe allowed to finish at the deadline, so that cancellation cannot leave ambiguous HTTP or database state.
23. As an operator, I want no new probe admitted after the deadline, so that one pass cannot become unbounded.
24. As an operator, I want unprocessed selected cases left due, so that no claim or lease recovery protocol is required.
25. As a maintainer, I want one serial Terminal Resolution Recovery owner, so that no concurrent request race can overshoot a newly observed limiter boundary.
26. As a maintainer, I want no worker pool, task fan-out, channel, semaphore, or adaptive-control framework, so that throughput work remains proportionate to this project.
27. As a maintainer, I want no schema, table, row lease, `SKIP LOCKED` coordination, or migration, so that the existing durable lifecycle model remains authoritative.
28. As a maintainer, I want no new runtime tuning variables, so that production throughput changes require reviewed code rather than ad hoc configuration.
29. As a maintainer, I want recovery-specific admission logic, so that this narrow upstream policy does not become an unnecessary general rate-limiting framework.
30. As a maintainer, I want the existing persisted limiter row reused, so that no second representation of ESI capacity can drift.
31. As a feed subscriber, I want accelerated recovery to preserve public-evidence requirements, so that queue pressure cannot fabricate acceptance, expiry, or unknown closure.
32. As a feed subscriber, I want delivery preparation completed before the next probe, so that existing ordering and failure semantics remain unchanged.
33. As a feed subscriber, I want notification identity, action, ping eligibility, and nonce behavior unchanged, so that acceleration cannot duplicate or broaden alerts.
34. As a feed subscriber, I want observation-time issuer and location evidence preserved, so that pacing changes cannot alter presentation facts.
35. As an operator, I want database or durable delivery failure to stop the current pass, so that throughput cannot outrun persistence.
36. As an operator, I want one concise recovery summary per pass, so that attempted, error-charged, resolved, remaining allowance, stop reason, and elapsed time are visible without a telemetry subsystem.
37. As an operator, I want existing health queries to remain the source for backlog and ETA, so that this change adds no dashboard or persisted metrics model.
38. As an operator, I want actual Resolution Throughput and projected clearance recorded after deployment, so that safe capacity improvement is demonstrated rather than assumed.
39. As an operator, I want the new scheduler to exceed 32 committed outcomes per minute when upstream capacity permits, so that the fix-forward produces a measurable benefit.
40. As an operator, I want unrelated ESI traffic allowed to reduce recovery capacity, so that global service health takes priority over backlog speed.
41. As an operator, I want future `X-Ratelimit-*` bucket headers to remain authoritative without automatically enabling concurrency, so that upstream migration does not silently expand local machinery.
42. As an operator, I want any future concurrent design to require fresh evidence and review, so that today’s serial safety decision is not bypassed.
43. As a maintainer, I want the current presentation preflight expressed as a non-sending automated test, so that stale visual assertions fail before Discord transport.
44. As an operator, I want a failed preflight to produce zero Discord calls, so that correcting test expectations cannot create duplicate verification messages.
45. As an operator, I want the ignored manual transport seam to reuse the tested canonical preflight fixture, so that automated and live presentation expectations cannot drift independently.
46. As an operator, I want the scheduler and visual-preflight correction reviewed together, so that one fix-forward deployment can complete Production Verification.
47. As an operator, I want a fresh validated backup and migration-ledger check before deployment, so that fix-forward evidence begins from a known durable state.
48. As an operator, I want no canary, staging environment, traffic split, or automatic rollback, so that the existing single-server operating model remains simple.
49. As an operator, I want a regression to preserve deployed state and halt subsequent work, so that evidence remains available for explicit direction.
50. As an operator, I want ten complete one-minute error windows observed after deployment, so that floor, reset, resume, and throughput behavior are demonstrated across repeated boundaries.
51. As an operator, I want a complete Regional Observation sweep during the gate, so that faster recovery cannot starve public discovery.
52. As an operator, I want Structure Resolver readiness, current ESI progress, healthy services, and zero delivery debt preserved, so that throughput cannot hide a broken dependency.
53. As an operator, I want no unexpected `420`, `429`, authentication, Discord, panic, database, duplicate-post, or unresolved collection failure, so that the fix-forward has a bounded acceptance predicate.
54. As an operator, I want exactly one non-pinging Production Verification message after the gate passes, so that deployed presentation is proven without operational-feed impact.
55. As an operator, I want Historical Enrichment Repair untouched throughout this work, so that recovery acceleration and old-message edits cannot compete.

## Implementation Decisions

- This specification supersedes only the fixed 32-cases-per-minute admission decision in the earlier recovery-rollout specification. Single ownership, durable lifecycle evidence, public ESI boundaries, delivery semantics, health behavior, and rollout protections remain authoritative.
- Terminal Resolution Recovery remains one in-process serial owner. No concurrent probe tasks or second consumer are introduced.
- One recovery pass has a 60-second work deadline and selects up to 256 due Awaiting Resolution cases ordered by due time, region, and Contract ID.
- The owner checks the work deadline before each admission. A probe already started may complete its HTTP result, transactional state update, and any durable delivery before the pass ends.
- Unprocessed rows from the selected window receive no claim or mutation and remain due for the next oldest-first selection.
- Every probe start is separated from the prior probe start by at least 250 milliseconds.
- A response outside `2xx`/`3xx` is an Error-Charged Probe for pacing even when the application interprets it as conclusive terminal evidence. The next probe start is at least one second later.
- A `2xx`/`3xx` response permits the next serial probe after the universal 250-millisecond minimum.
- Recovery uses the latest valid persisted legacy error-limit remaining/reset observation whose reset deadline is still current. A response with missing headers does not erase that observation.
- Recovery admits no probe when the authoritative remaining-error value is 40 or lower. It waits for the current reset rather than consuming the reserve.
- Regional Observation and all other ESI work retain authority to consume the shared budget. If they reach the floor first, recovery yields even when it processed fewer cases than expected.
- When no valid current-window legacy error-limit observation exists, recovery admits at most 32 probes during the pass, spaced approximately 1.875 seconds apart.
- Existing bucket-limit, error-limit, persisted pause, pacing, `429`, and `Retry-After` behavior remains authoritative. An observed boundary stops admission for the current pass.
- If CCP later supplies new bucket headers for this route, the existing bucket pacing path applies. It does not enable concurrency or automatically raise reviewed constants.
- Database, cache, response, and durable-delivery failures retain current fail-closed behavior and stop or defer work according to existing contracts.
- Terminal outcome classification, public-evidence standards, retained observation context, event construction, notification ordering, ping action, idempotency nonce, and restart recovery do not change.
- Delivery remains coupled to each terminal event before the next probe. No separate delivery worker or outbox-drain phase is introduced.
- The constants are reviewed code, not environment variables or database configuration.
- Add one recovery-specific admission decision with only the state required to return admit, wait-until, fallback, or stop. Do not build a general scheduler, token bucket, adaptive controller, or reusable limiter framework.
- Emit one structured summary after each pass containing attempted, resolved, error-charged, ending remaining allowance, stop reason, and elapsed time. Backlog and ETA continue to come from existing persisted health evidence.
- Correct the stale manual Production Verification title expectations to the current canonical terminal presentation and share the same fixture/assertion path between a non-sending automated preflight and the ignored Discord transport seam.
- Package the scheduler and test-only visual-preflight correction into one reviewed fix-forward candidate. No production code is changed merely to satisfy the manual presentation test.
- Deploy the exact reviewed candidate once after a fresh backup, migration checksum validation, quiet Compose validation, and immediate read-only preflight. There is no automatic rollback or secondary deployment attempt.
- The post-deployment gate observes ten complete one-minute error windows. Yielding at the 40 floor is correct; admission below it, cross-route throttle, failure to resume, regional starvation, or another health regression halts progression.
- **Operator-authorized final evidence interpretation (2026-08-25):** a `deferred_backlog`-only critical transition caused by discovery arrivals is non-blocking only when every other health, service, delivery, limiter, global-auth, and forbidden-error gate remains clean under the already-approved per-structure resolver exception; the same check returns healthy at the immediately following watchdog evaluation within 75 seconds (with its exact duration recorded); the exact ten-minute interval ends with fewer due backlog items than it started with and a defensible positive net-drain ETA; and no `420`, `429`, or recovery admission at or below the 40 reserve occurs. This exception does not weaken any other health regression, regional starvation, delivery debt, global-auth, limiter, or error gate.
- **Operator-authorized ESI cache-label interpretation (2026-08-25):** an `esi_progress` **degraded** label caused solely by stale conditional-body-cache write age is non-blocking only when exactly 114 Regional Observation owners are current, the latest completed regional sweep is no older than seven minutes (five-minute cadence plus two-minute tolerance), and the narrow interval logs contain no regional-collection failure, limiter-starvation, `420`, `429`, panic, or database error. Process identity/restart/OOM, delivery debt, global resolver backoff, all other health checks, and the 40-reserve rule remain clean. An `esi_progress` critical state, an incomplete/stale regional sweep, or any listed error remains blocking. Conditional `304 Not Modified` sweeps may complete successfully without writing a new response body and therefore need not advance the body-cache `updated_at` timestamp; this does not prove ESI collection is stale. A follow-up technical-debt item must separate successful sweep freshness from body-cache-write age in health evidence; it is not part of this rollout.
- Production Verification remains exactly one controlled non-pinging message in channel `1115807643748012072`, sent only after the scheduler gate passes. Historical Enrichment Repair remains separately gated.

## Testing Decisions

- Tests assert externally visible recovery behavior through the existing production-shaped Terminal Resolution Recovery seam: controlled public ESI responses, a real temporary PostgreSQL database, persisted limiter and lifecycle rows, Contract Subscription evaluation, Recording Delivery, and controlled time. This is the principal seam.
- Prefer the public recovery operation and spawned production-loop seam over private helper assertions. A small pure admission-decision test is supplementary only when controlled time cannot express an edge precisely.
- Add a mixed-response test that supplies acceptance-confirming `403`, `200`, `204`, and `304` responses. Assert serial request order, at least one-second spacing after Error-Charged Probes, at least 250-millisecond spacing after other responses, and correct durable outcomes.
- Prove explicitly that a semantically successful accepted-player `403` is counted as error-charged for admission without being persisted as a failed resolution.
- Seed a current-window remaining count immediately above 40, consume the final permitted slot, and prove no later request begins at or below 40.
- Advance through the reset and prove recovery resumes from the oldest remaining due case without replaying resolved cases.
- Seed shared limiter consumption from another ESI path and prove recovery yields to that authoritative lower remaining count.
- Provide one response without legacy headers while valid current-window evidence exists and prove the evidence remains authoritative.
- Start without valid current-window headers and prove exactly the fallback bound is admitted, spread across the minute rather than burst.
- Seed more than 256 due cases and prove selection is oldest-first, at most 256 are loaded for the pass, the deadline bounds new starts, and untouched rows remain due.
- Hold one probe across the deadline and prove it completes durably while no subsequent probe starts in that pass.
- Preserve the existing test that each reportable terminal delivery completes before a later probe. Re-run lifecycle, delivery action, ping, restart, nonce, observation-context, proximity, and failure-persistence regressions.
- Prove an active persisted pause, bucket boundary, `Retry-After`, unexpected `429`, database failure, and durable delivery failure stop or defer work without admission beyond their boundary.
- Prove Regional Observation does not execute due terminal probes and the independent recovery owner remains the only consumer.
- Prove public resolution requests remain unauthenticated and scoped Structure Authorization behavior remains unchanged.
- Add a non-ignored, non-sending canonical Production Verification preflight. It must validate title, description, Issuer, human-readable location, revision, thumbnail, no raw identifier labels, no empty placeholders, and empty allowed mentions before any Discord client or send call is constructed.
- Make the ignored live Discord seam reuse the same canonical fixture and preflight, then assert the resulting message identity and no replacement behavior. A preflight failure must make zero Discord requests.
- Do not use production as a throughput load-test environment. All pacing and boundary cases are proven with controlled time and controlled ESI before deployment.
- Run focused recovery, limiter, lifecycle, delivery, health, resolver, and presentation tests; then the complete serial real-PostgreSQL contract suite, safe library suite, formatting, typechecking, Clippy, fixed-point diff checks, migration checks, and quiet Compose validation.
- Require Sol-high Standards and Spec review at the final fixed point. Any finding requiring concurrency, reservation schema, delivery decoupling, another service, or tuning machinery blocks this specification rather than expanding it.
- Production evidence records each minute’s attempted, resolved, error-charged, remaining allowance, stop reason, service health, regional progress, prepared/permanent delivery counts, and actual Resolution Throughput. For the authorized backlog-only exception, it also records the critical-to-healthy duration, all other gate values, the exact due-backlog start/end, and the net-drain ETA. The gate requires ten complete windows, not backlog clearance.

## Out of Scope

- Sustained 30–100 probes per second on the current public contract-items route.
- Any attempt to circumvent, saturate, probe experimentally for, or treat ESI limits as a performance target.
- Concurrent or parallel Terminal Resolution Recovery probes.
- A worker pool, task fan-out, channel, semaphore, token bucket, adaptive controller, generic scheduler, second consumer, or new process.
- A new schema, migration, reservation counter, error-window ledger, row claim, lease, `SKIP LOCKED`, queue, or workflow engine.
- Runtime environment variables, operator-adjustable throughput settings, or automatic tuning.
- Decoupling terminal evidence persistence from notification preparation or durable delivery.
- Changing Regional Observation, Retained Contract Manifest collection, proximity reconciliation, Structure Authorization, Discord delivery limits, or Historical Enrichment Repair.
- Force-classifying, discarding, expiring, or suppressing Awaiting Resolution cases to improve health.
- Changing Acceptance Confirmed, Sale Confirmed, Purchase Confirmed, Expired, or Closed — Outcome Unknown evidence standards.
- A production load test, canary environment, traffic split, automatic rollback, automatic restart, database restoration, or automated remediation.
- Waiting for the Awaiting Resolution backlog to reach zero before completing Production Verification.
- Automatically enabling concurrency if CCP later migrates the route to a new bucket limit.

## Further Notes

- Live evidence on 2026-08-25 showed the current implementation resolving exactly 32 cases per minute, with a median active batch span of about 7.5 seconds and a serial active capacity above four probes per second. Cadence and the shared upstream allowance, not PostgreSQL or Discord, are the limiting factors.
- Recent production evidence showed roughly 80% of terminal transitions established by an accepted-player `403`. At that mix, the legacy 100-error allowance makes 30–100 probes per second unsafe regardless of local concurrency.
- The expected throughput under this design is workload-dependent. At the observed mix it should exceed 32 committed outcomes per minute and commonly reach roughly 60–75, but the safety invariants and authoritative upstream evidence take precedence over a fixed output target.
- A future concurrent design requires explicit CCP route capacity, a fresh decision on in-flight boundary overshoot, and likely a different admission model. It is not an incremental extension of this specification.
- Domain definitions for Resolution Throughput and Error-Charged Probe are recorded in the project glossary. The serial/error-aware trade-off and deferred concurrency are recorded in the in-process recovery ADR.
- CCP documents the legacy 100-error/minute boundary, route-specific buckets, response headers, `Retry-After`, and the guidance to spread requests and avoid operating at the limit: https://developers.eveonline.com/docs/services/esi/rate-limiting/ and https://developers.eveonline.com/docs/services/esi/best-practices/
- Discord delivery remains independently limited by per-route and global response headers, but the Awaiting Resolution queue is not a prepared-message queue: https://docs.discord.com/developers/topics/rate-limits
- This specification uses the agreed implementation workflow: a Terra xhigh implementation pass, followed by Sol high Standards and Spec review, with findings remediated before production mutation.
