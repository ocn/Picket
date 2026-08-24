# Recover Contract Monitoring and Complete the Enrichment Rollout

Status: ready-for-agent
Date: 2026-08-24

## Problem Statement

The reviewed Progressive Enrichment release cannot complete its production rollout because the deployed Runtime Health Snapshot is critical for three different reasons. ESI progress is falsely critical because the health query reads a resource-key namespace that public contract collection does not write. Regional progress is genuinely too slow because the collector requests or revalidates item manifests for every currently public contract even after a complete Retained Contract Manifest has been stored; The Forge alone recently required about 85 minutes, and a complete sweep required about 127 minutes. Terminal resolution is also genuinely behind because only eight due Awaiting Resolution cases per region are admitted after that region completes, leaving more than 33,000 due cases and a current theoretical drain time measured in hundreds of days.

Production evidence does not indicate a crashed collector, PostgreSQL lock, active ESI pause, process restart, OOM termination, or unresolved permanent Discord delivery failure. Collection remains active but is workload-bound, its health evidence is partly incorrect, and its Terminal Resolution Recovery capacity is coupled to the slowest Regional Observation work.

The operator wants the smallest reliable correction. This project does not justify a new service, queueing platform, worker pool, schema, canary environment, traffic split, automatic rollback, or automated remediation system. The existing PostgreSQL state, public ESI client, persisted limiter, durable delivery path, watchdog, and single production Compose stack must be reused.

## Solution

Correct ESI progress health to observe the public-contract cache and collection evidence the runtime actually persists. Reuse every complete Retained Contract Manifest by Contract ID without re-requesting its immutable item membership; only an absent, incomplete, or previously failed manifest remains eligible for collection during the next regional sweep.

Move due Awaiting Resolution processing out of Regional Observation post-processing and give it one single-consumer, in-process Terminal Resolution Recovery loop. The loop runs once per minute, admits a fixed bounded batch of approximately 32 oldest-due cases, uses the existing public ESI client and persisted limiter, and skips rather than bursts after a missed cadence. There is one terminal-resolution owner, so no leases, row-skipping worker coordination, or second workflow subsystem is introduced. Missing-manifest retry remains part of the now-fast regional sweep; it does not receive a separate worker.

Deploy the reviewed result once to the sole production environment through the guarded Recovery Deployment exception. The existing watchdog automatically detects and reports regressions but never stops services, changes images, restores data, reverses migrations, or performs remediation. A regression halts subsequent rollout actions and waits for explicit operator direction.

The Rollout Health Gate requires one complete post-deployment regional sweep within the existing 30-minute critical boundary, a smaller deferred backlog whose measured throughput projects clearance within 24 hours, current corrected ESI progress, ready runtime services, and no new delivery, authentication-boundary, Discord, panic, database, or sustained rate-limit failure. A 15-minute sweep remains a performance target rather than a rollout blocker.

After the gate passes, send one controlled non-pinging Production Verification message to Discord testing channel `1115807643748012072`. This is a check in the sole production environment, not a canary deployment. Historical Enrichment Repair remains untouched until that verification succeeds, a fresh read-only channel-scoped dry run reports the current exact candidate count, and the operator explicitly approves that count.

## User Stories

1. As an operator, I want ESI progress health based on evidence the contract collector actually persists, so that a healthy collector cannot remain falsely critical because of a resource-key mismatch.
2. As an operator, I want false health evidence corrected before interpreting rollout readiness, so that the Rollout Health Gate reflects production behavior rather than an unreachable condition.
3. As an operator, I want regional freshness distinguished from process liveness, so that an active but overloaded collector is not misdiagnosed as crashed.
4. As an operator, I want the existing 15-minute regional target retained as an aspiration, so that optimization remains visible without turning an arbitrary target into an absolute deployment barrier.
5. As an operator, I want the existing 30-minute critical boundary used for Recovery Deployment acceptance, so that the gate has a documented operational basis.
6. As an operator, I want one complete post-deployment sweep measured directly, so that rollout acceptance does not rely on historical production timings.
7. As an operator, I want deferred backlog movement measured from live state, so that a temporarily green snapshot cannot conceal a non-draining queue.
8. As an operator, I want measured recovery throughput to project current-backlog clearance within 24 hours, so that the accepted drain objective is tied to observed performance.
9. As an operator, I want automatic issue detection without automatic action, so that regressions become visible without the system changing production state on my behalf.
10. As an operator, I want rollout work to halt when a regression is detected, so that Production Verification or historical edits cannot continue through a known problem.
11. As an operator, I want services left running after automatic detection, so that evidence remains available and no unapproved containment action causes additional disruption.
12. As an operator, I want rollback to require explicit direction, so that images, containers, PostgreSQL data, and migrations are never changed automatically.
13. As an operator, I want the watchdog to remain the only automatic detection mechanism, so that this small project does not acquire an orchestration or anomaly-detection subsystem.
14. As an operator, I want complete Retained Contract Manifests reused, so that immutable contract contents are not repeatedly downloaded.
15. As an ESI operator, I want item requests made only for absent, incomplete, or previously failed manifests, so that the application respects upstream cache guidance and minimizes shared-resource load.
16. As a maintainer, I want Retained Contract Manifest reuse keyed by Contract ID, so that presentation, lifecycle, and matching continue using the same complete Offered Item and Requested Item evidence.
17. As a feed subscriber, I want manifest reuse to preserve listing and terminal event correctness, so that performance work cannot suppress or misclassify alerts.
18. As a feed subscriber, I want a newly observed contract with no retained manifest to collect its items normally, so that optimization does not omit new contracts.
19. As an operator, I want a failed or incomplete manifest retried on a later regional sweep, so that transient ESI problems remain recoverable without a new worker.
20. As an operator, I want a contract that disappears before manifest recovery handled under the existing lifecycle and failure rules, so that ordinary contract churn does not become permanent backlog.
21. As an operator, I want successful later collection to resolve the associated manifest failure, so that health evidence does not accumulate obsolete failures.
22. As an operator, I want transport errors, malformed responses, timeouts, and `5xx` results retained as retryable failures, so that real upstream problems remain visible.
23. As an ESI operator, I want `429`, `Retry-After`, error-limit, and bucket-limit boundaries to remain authoritative, so that backlog recovery cannot overrun ESI.
24. As an ESI operator, I want a missed recovery cadence skipped rather than replayed as a burst, so that process delays cannot create a request storm.
25. As an operator, I want Awaiting Resolution cases processed independently of Regional Observation completion, so that The Forge cannot determine global terminal-recovery latency.
26. As an operator, I want one terminal-resolution consumer, so that the regional collector and recovery loop cannot race the same case.
27. As a maintainer, I want Terminal Resolution Recovery to reuse the existing durable cases, retry deadlines, public ESI client, limiter, event preparation, and delivery state, so that no parallel workflow model is introduced.
28. As an operator, I want each recovery tick bounded to a conservative fixed batch, so that recovery capacity is predictable and leaves ESI headroom.
29. As an operator, I want oldest-due cases selected first, so that retained debt has a deterministic drain order.
30. As a feed subscriber, I want existing public-evidence requirements preserved during accelerated recovery, so that backlog pressure cannot fabricate Sale Confirmed, Purchase Confirmed, Expired, or Closed — Outcome Unknown outcomes.
31. As an auditor, I want a case left Awaiting Resolution when evidence remains inconclusive, so that health improvement cannot be achieved by discarding uncertainty.
32. As a feed subscriber, I want terminal notifications and repairs to remain restart-safe and idempotent, so that an independent recovery cadence cannot duplicate messages or pings.
33. As a maintainer, I want no new database schema or migration for this correction, so that the existing durable model remains the only source of truth.
34. As a maintainer, I want no new service, container, process role, worker pool, lease, or queue technology, so that operational complexity remains proportionate to the project.
35. As a maintainer, I want no new runtime tuning variables initially, so that the first correction has one reviewed conservative behavior rather than an unbounded configuration surface.
36. As a maintainer, I want no speculative page-progress telemetry, so that new health state is added only if optimized production sweeps still cannot satisfy existing evidence.
37. As an operator, I want one Recovery Deployment to the sole production environment, so that this project does not require staging or canary infrastructure.
38. As an operator, I want a validated backup and migration ledger before deployment, so that the current durable state remains recoverable through explicit operator action.
39. As an operator, I want Public Contract ESI operations to remain unauthenticated, so that the recovery work cannot leak the Structure Authorization bearer token.
40. As an operator, I want player-structure operations to retain their required scoped bearer token, so that approved location enrichment continues working after recovery changes.
41. As an operator, I want bot, PostgreSQL, watchdog, and Structure Resolver readiness checked after deployment, so that performance improvement cannot hide a broken dependency.
42. As an operator, I want zero new prepared or unresolved permanent deliveries at the gate, so that Discord delivery debt cannot be ignored to finish rollout.
43. As an operator, I want one controlled non-pinging Production Verification message in testing channel `1115807643748012072`, so that the deployed Issuer and human-readable location presentation is inspected without using an operational feed.
44. As a feed subscriber, I want Production Verification to contain no raw identifiers, empty placeholders, unexpected mentions, or replacement posts, so that the approved presentation contract is proven live.
45. As an operator, I want Production Verification to remain distinct from a canary environment, so that validation does not add deployment topology.
46. As an operator, I want Historical Enrichment Repair frozen until recovery and Production Verification succeed, so that old-message edits do not compete with an unhealthy live collector.
47. As an operator, I want a fresh read-only historical dry run, so that current database state defines the candidate set.
48. As an operator, I want to approve the exact current historical count before queueing, so that an earlier approval cannot silently authorize a different or larger batch.
49. As an operator, I want historical edits to retain all existing bounded, non-pinging, in-place repair protections, so that recovery rollout does not weaken the approved migration contract.
50. As a maintainer, I want each of the three recovery slices independently implemented and reviewed, so that health, manifest, and scheduling regressions are found before production deployment.

## Implementation Decisions

- Deliver the correction as three vertical slices: corrected health evidence, Retained Contract Manifest reuse, and single-owner Terminal Resolution Recovery. Each slice is independently tested and reviewed against repository standards and this specification before the next slice begins.
- Correct ESI progress health to use the actual public-contract cache or collection evidence maintained by the running collector. Do not create a second telemetry namespace solely to satisfy health.
- Preserve the Runtime Health Snapshot/watchdog architecture. The watchdog detects, persists, and publishes regressions but has no authority to stop services, change images, restore backups, reverse migrations, or remediate state.
- Keep regional-progress health based on the existing completed-region evidence for the initial correction. Do not add page-progress schema or active-batch telemetry unless post-deployment measurement proves that optimized sweeps still cannot represent useful progress.
- Treat a complete Retained Contract Manifest as authoritative for its Contract ID. Regional collection obtains the retained manifest before considering an item request and combines it with the current regional summary for observation processing.
- Request public contract items only when no complete retained manifest exists or prior manifest collection remains incomplete or failed. Preserve conditional request, cache metadata, limiter, and failure behavior for those requests.
- Keep missing-manifest retry coupled to the regional sweep. No independent manifest worker is added; the expected fast sweep supplies retry cadence.
- Preserve existing disappearance and failure semantics. Expected disappearance follows the lifecycle state machine; retryable transport/upstream failures remain durable; a later successful collection or disappearance resolution closes obsolete failure state.
- Remove due Awaiting Resolution probing from Regional Observation post-processing and end-of-sweep catch-up so that one in-process loop is the sole terminal-resolution consumer.
- Run Terminal Resolution Recovery on a fixed one-minute cadence with missed ticks skipped. Admit a fixed conservative batch of approximately 32 oldest-due cases per tick.
- Reuse the existing durable resolution cases, `next_probe_at` deadlines, public ESI item probe, persisted admission limiter, terminal event construction, notification preparation, and durable delivery dispatcher.
- Check the persisted ESI limiter before and during recovery work. Stop the current recovery tick at an active limiter or `Retry-After` boundary; do not catch up missed ticks with a burst.
- Preserve evidence-safe outcome rules. Recovery capacity never force-classifies, discards, or mutates an Awaiting Resolution case merely to reduce health backlog.
- Preserve notification identity, ping eligibility, observation-time context, delivery idempotency, failure persistence, and restart recovery across the ownership change.
- Add no database table, migration, external queue, service, container, worker pool, lease protocol, `SKIP LOCKED` coordination, tuning environment variable, staged environment, or canary deployment.
- Deploy the final reviewed revision once to the existing production Compose stack through the guarded Recovery Deployment exception. Validate quiet Compose configuration, the pre-deployment backup, and the applied migration ledger first.
- Automatic detection only halts subsequent rollout actions. It does not automatically stop or restart services, roll back images, restore data, reverse migrations, or mutate subscriptions and deliveries.
- Do not change the already-approved five-region Contract Subscription if production inspection confirms its region union and lifecycle actions. Correct only detected drift.
- Require the Rollout Health Gate to observe one complete regional sweep within 30 minutes, a smaller due backlog, measured throughput projecting current-backlog clearance within 24 hours, current ESI progress, ready runtime services, zero prepared or unresolved permanent deliveries, and no new authentication-boundary, Discord, panic, database, or sustained rate-limit failure.
- Treat a 15-minute sweep as a performance target, not a hard gate. Use measured production behavior to decide whether later tightening is justified.
- Send one controlled, non-pinging Production Verification message to testing channel `1115807643748012072`. Inspect Issuer presentation, strongest supported human-readable location, presentation revision, raw-identifier absence, allowed mentions, and Discord transport behavior.
- Keep Historical Enrichment Repair untouched until Production Verification succeeds. Then run a fresh read-only, channel-scoped dry run and require explicit approval of its exact candidate count before queueing.
- Retain the public/authenticated ESI boundary: regional discovery, manifests, lifecycle probes, universe names, affiliations, and health-related public requests remain unauthenticated; only player-structure resolution receives Structure Authorization.
- Follow the accepted in-process recovery boundary recorded in the contract-monitoring recovery ADR and the domain language in the project glossary.

## Testing Decisions

- Tests assert observable behavior through production-shaped boundaries rather than private helper calls, SQL text, task scheduling internals, or implementation-specific data structures.
- The principal seam is the existing Contract Collector/recovery boundary using controlled public ESI behavior, a real temporary PostgreSQL database, retained observations and manifests, Contract Subscription evaluation, Recording Delivery, and persisted state inspection. Extend this seam rather than introducing a parallel harness.
- Use the existing high-level health evaluation cycle as a supporting seam for corrected ESI progress and backlog evidence. No new health test framework is introduced.
- Add a retained-manifest regression that completes an initial observation, advances beyond the public item cache expiry, repeats the same public Contract ID, and proves that the next Regional Observation completes with zero item-endpoint requests while retaining identical Offered Item and Requested Item semantics.
- Add new, incomplete, and previously failed manifest cases proving that each still performs the required public item request, persists or clears pending state correctly, and never renders from a partial manifest.
- Add disappearance cases proving a pending manifest removed from a later complete regional page set does not remain as permanent manifest debt or create an unsupported lifecycle claim.
- Add a large-region seam with many retained contracts and a small number of new contracts. Assert item calls scale with missing manifests rather than total regional summaries.
- Add a corrected-health regression that persists the actual public-contract cache/collection evidence and proves ESI progress becomes current. Include a negative case showing unrelated or absent progress remains stale.
- Add a Terminal Resolution Recovery tick seam that seeds more than one batch of due cases without running public regional discovery, executes one tick, and proves only the fixed bound is admitted in oldest-due order.
- Add a second tick case proving remaining due work drains without reprocessing terminal cases resolved by the first tick.
- Add a single-owner regression proving Regional Observation completion no longer starts terminal item probes and the recovery tick is the only path that does.
- Add recovery cases for Available, accepted-player evidence, `204`, `304`, `404`, expiry, inconclusive evidence, timeout, malformed response, `5xx`, `429`, persisted `Retry-After`, and an already-active limiter.
- Add a cadence regression using controlled time that proves missed ticks are skipped rather than replayed as an immediate burst.
- Add failure persistence and later-success cases proving retryable resolution failures remain due according to their deadline and are resolved when conclusive evidence arrives.
- Add delivery regressions proving Sale Confirmed, Purchase Confirmed, Expired, and Closed — Outcome Unknown retain their existing evidence requirements, identity, ping action, restart safety, and at-most-one prepared/sent terminal notification.
- Add retained observation-location and progressive-enrichment regressions proving the new ownership boundary does not discard Issuer, structure, solar-system, region, or evidence provenance.
- Add public/authenticated wire assertions proving recovery item probes contain no bearer token and authenticated structure calls retain the scoped token.
- Add watchdog/health assertions proving detection persists and reports an unhealthy state without stopping collection, mutating delivery state, or invoking any rollback/remediation operation.
- Reuse existing migration tests to prove the correction needs no schema change and runs against both clean/current test databases and the applied production ledger.
- Run the focused tests for each vertical slice, then the complete serial real-PostgreSQL contract suite, safe library suite, formatting, typechecking, Clippy, and quiet Docker Compose configuration validation at the final fixed point.
- Production validation is read-only until the single Deployment. After deployment, record sweep duration, backlog start/end, measured drain rate, ESI progress, limiter state, resolver readiness, delivery state, service state, and new errors before evaluating the Rollout Health Gate.
- Production Verification sends exactly one controlled non-pinging message to channel `1115807643748012072` and verifies the rendered payload and Discord result. It is not a separate environment or traffic cohort.
- Historical candidate discovery remains read-only until the operator explicitly approves the exact fresh count. Queueing and babysitting continue to use the already-tested Historical Enrichment Repair seams.

## Out of Scope

- A staging, canary, shadow, blue/green, partial-fleet, or traffic-splitting deployment model.
- Automatic rollback, automatic container stop or restart, automatic database restoration, migration reversal, subscription mutation, or automated remediation.
- A new watchdog, rollout orchestrator, anomaly detector, metrics platform, Prometheus stack, or page-level progress ledger.
- A new process, container, service, external queue, workflow engine, worker pool, lease protocol, or horizontal-scaling design.
- New database schema or migrations for manifest reuse or Terminal Resolution Recovery.
- An independent manifest-recovery worker.
- Runtime tuning variables for the initial recovery cadence or batch size.
- Revalidating a complete Retained Contract Manifest merely because HTTP cache metadata has expired.
- Force-classifying, expiring, discarding, or suppressing Awaiting Resolution cases to make health appear better.
- Relaxing Acceptance Confirmed, Sale Confirmed, Purchase Confirmed, Expired, or Closed — Outcome Unknown evidence standards.
- Changing Progressive Enrichment, location precedence, Issuer presentation, Acceptor omission, ping eligibility, or the approved five-region subscription semantics.
- Adding contract-history authorization, extra Structure Authorization scopes, credentials, commands, or UI.
- Starting Historical Enrichment Repair before recovery and Production Verification pass.
- Treating the 15-minute performance target as a permanent service-level agreement.

## Further Notes

- Production evidence at diagnosis was approximately 33,000 due resolution cases, including more than 30,000 in The Forge. Counts are live operational evidence and must be re-queried; they are not implementation constants.
- The deployed eight-cases-per-region-per-sweep admission combined with a roughly 127-minute sweep implied a drain time of roughly 335 days. A 32-per-minute recovery bound has an ideal drain time near 17 hours and is intended to leave substantial ESI headroom; actual production throughput remains authoritative.
- CCP advises respecting cache validators, avoiding unnecessary requests, spreading periodic work, slowing before limits, and honoring `Retry-After`: https://developers.eveonline.com/docs/services/esi/best-practices/ and https://developers.eveonline.com/docs/services/esi/rate-limiting/
- Google SRE guidance recommends service indicators tied to behavior users care about and warns against overly aggressive arbitrary objectives. That supports the measured 30-minute Recovery Deployment gate and treating 15 minutes as a target subject to later production evidence: https://sre.google/sre-book/service-level-objectives/
- Docker Compose supports a single-server production model and service recreation while preserving mounted volumes. This specification nevertheless authorizes no automatic rollback or image change: https://docs.docker.com/compose/how-tos/production/
- The validated pre-deployment backup and current migration ledger remain prerequisites. Backup paths, candidate counts, sweep timings, and service states are live operational details and belong in the rollout handoff rather than implementation constants.
- Implementation follows the previously approved agent workflow: a Terra xhigh implementation pass and a Sol high Standards/Spec review for each of the three vertical slices, followed by final fixed-point verification.
