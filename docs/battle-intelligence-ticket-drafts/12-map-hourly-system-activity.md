# 12 — Map hourly system activity with source freshness

**What to build:** Show recent system jumps and player-ship, pod and NPC losses as separately selectable geographic activity layers.

**Blocked by:** 06 — Configure and inspect geographic coverage

**Status:** draft — awaiting breakdown approval

- [ ] Collect hourly ESI activity into durable interval-keyed observations for the configured map/history scope, respecting source caching and Last-Modified semantics.
- [ ] Expose jumps and player-ship, pod and NPC destruction as distinct layers in a private geographic activity view with interval, source freshness and coverage status.
- [ ] Do not count repeated cached responses as new hours, treat excluded space as observed zeros, or label aggregate counts as live population, identified fleets, movement direction or verified gate-only traffic.
- [ ] Keep activity collection failure isolated from ordinary killfeed and show stale/missing intervals in the view.
- [ ] Verify cached/unchanged responses, nonzero-only responses, excluded systems, retries and source gaps through the approved application seam.
- [ ] This slice supplies its own minimal geographic activity view and does not require the 3D Encounter renderer.

**Spec coverage:** User Stories 43, 53. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

