# 15 — Apply retention while preserving readable summaries

**What to build:** Apply configurable evidence retention and keep older compact reports and activity readable with explicit limits on detailed replay.

**Blocked by:** 11 — Compare Encounters and recap a Roaming Session; 13 — Backfill historical combat into revisable Battle Reports; 14 — Backfill activity and compare staging-area baselines

**Status:** draft — awaiting breakdown approval

- [ ] Provide configurable starting retention of ninety days of detailed killmail evidence and one year of compact reports/hourly activity, separately from initial import depth.
- [ ] Produce a storage estimate and a preview of affected records before enabling deletion; dry-run behavior is verifiable without deleting production data.
- [ ] Preserve readable retained summaries and collection references when detailed evidence expires, with visible unavailable replay/correction capability and retained valuation/coverage qualifications.
- [ ] Coordinate retention with active recovery/import work and persisted notification/revision state so pruning cannot corrupt recovery progress or generate duplicate sends.
- [ ] Apply expiry across report queries and UI without dangling report/collection references or silently implying retained detailed evidence.
- [ ] Verify boundaries, repeated pruning, active imports, expired member details and recoverable notification state with controlled time and real temporary storage.

**Spec coverage:** User Stories 50, 51. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

