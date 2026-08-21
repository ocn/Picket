# 07 — Migrate approved history and babysit production

**What to build:** Rediscover the live historical candidate set, verify representative in-place canaries, execute only the explicitly approved bounded remainder, and observe production until the enrichment rollout and repair worker are demonstrably stable.

**Blocked by:** 06 — Deploy and cut over the five-region feed.

**Status:** ready-for-agent

- [ ] Production health is re-queried immediately before historical work; new unresolved failures, Discord errors, rate-limit failures, or a stuck collector stop mutation and are reported.
- [ ] A fresh channel-scoped dry run records eligible and excluded deliveries without mutation.
- [ ] The fresh candidate set is reconciled with the operator's explicit bounded approval; a changed or broader scope stops for renewed approval rather than being inferred.
- [ ] Representative listing, Acceptance Confirmed, and ambiguous or expired canaries are queued through the normal repair worker before the remainder.
- [ ] Canary messages retain their original Discord message identifiers, contain no mentions, create no replacements, and display Issuer plus the strongest retained human-readable location in the approved compact style.
- [ ] Historical canaries perform no authenticated structure collection and never expose raw region or location identifiers.
- [ ] Only the approved remaining delivery set is queued in a bounded batch.
- [ ] The repair worker is observed through completion or a persisted Retry-After boundary; permanent failure stops later dispatch without losing queued state.
- [ ] Final checks report delivery status counts, repair status counts, unresolved permanent failures, duplicate-post indicators, collector progress, deferred-backlog movement, resolver state, and new bot/watchdog log errors.
- [ ] The final handoff records exact candidate, canary, queued, completed, excluded, and failed counts with original Discord links where useful.
