# 01 — Expose complete killmail observations without changing alerts

**What to build:** Prefactor the existing processing boundary so a battle observer can receive complete matched and unmatched observations while ordinary killfeed behavior remains intact.

**Blocked by:** None — can start immediately

**Status:** draft — awaiting breakdown approval

- [ ] Keep existing filter evaluation, standing vetoes, embed contents, ordering and mention behavior unchanged when observation is disabled.
- [ ] Expose complete enriched killmails, evaluated matches and available event/publication/receipt metadata before unmatched payloads are discarded; do not scrape Discord or reevaluate source filters differently.
- [ ] Make the observation handoff bounded and nonblocking for ordinary sends; expose observer failure or rejected handoff explicitly rather than claiming successful collection.
- [ ] Verify through the existing processing seam with matched, unmatched, vetoed and slow/failing observer cases; use no live Discord sends.
- [ ] Keep this limited to the additive observation boundary; durable report ingestion and recovery belong to subsequent slices.

**Spec coverage:** User Stories 52. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

