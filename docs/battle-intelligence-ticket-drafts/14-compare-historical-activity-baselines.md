# 14 — Backfill activity and compare staging-area baselines

**What to build:** Backfill available hourly activity and show weekday/hour comparisons around candidate staging locations.

**Blocked by:** 12 — Map hourly system activity with source freshness

**Status:** draft — awaiting breakdown approval

- [ ] Import an initial configurable ninety days of hourly jumps and destruction archives with durable progress and explicit interval coverage.
- [ ] Show observed history versus equivalent weekday/hour baselines in the geographic activity view, keeping each activity category distinct.
- [ ] Compute comparisons from available nonduplicated intervals without lookahead or filling unknown periods with zero; disclose sample coverage.
- [ ] Allow inspection around configured staging locations with distance context, not Ship Cache inventory or actual clone-install records.
- [ ] Keep recurring activity distinct from confirmed recurring fleet identity and retain source/interval provenance.
- [ ] Verify sparse archives, cached/repeated hours, resumed imports, changing scope and equivalent-period comparisons through the primary seam plus a focused private UI check.

**Spec coverage:** User Stories 44, 45, 46, 47. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

