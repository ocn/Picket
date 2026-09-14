# 02 — Discover a live candidate from an opted-in source

**What to build:** Enable one existing subscription as a Discovery Source and show its first observed candidate in a minimal private report list and detail view.

**Blocked by:** 01 — Expose complete killmail observations without changing alerts

**Status:** draft — awaiting breakdown approval

- [ ] Add backwards-compatible opt-in and optional explicit battle priority to existing subscription configuration; no separate capital filter list or inferred priority from ping settings.
- [ ] Persist observations by killmail ID and source-match snapshots in battle-owned storage; overlapping matches or replayed arrivals count once and retain all provenance.
- [ ] Present a provisional candidate with system, event time, observed loss and discovery reasons through a query interface and minimal privately served UI; no shared deployment or Discord UI links.
- [ ] Persist event, receipt and available publication times separately; configuration edits apply to future matches and preserve past rationale.
- [ ] Provide a stable local candidate/report identifier and reload the saved candidate after restart without relying on upstream report URLs.
- [ ] Establish the approved application-level test harness using controlled time, real source filters, temporary PostgreSQL and captured external effects; include a small UI smoke check.

**Spec coverage:** User Stories 1, 5, 6, 7, 11, 18, 49, 54, 56. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

