# 08 — Show loss valuation with provenance and missing-data coverage

**What to build:** Display observed report losses with available ISK valuation, its provenance and clear unknown values.

**Blocked by:** 03 — Evolve separate Battle Reports with contextual losses

**Status:** draft — awaiting breakdown approval

- [ ] Persist supplied valuations with source/time provenance and associate them with the correct observation/report revision.
- [ ] Show loss totals, ship composition and valuation coverage in report queries and the private detail; unknown prices must not become zero.
- [ ] For revised evidence or enrichment, recompute the affected report without duplicate losses and retain a visible revision.
- [ ] Define the historical enrichment contract so raw archive imports cannot silently use current prices as event-time values; expose unavailable historical valuation.
- [ ] Keep observed participation distinct from complete rosters and ISK efficiency distinct from tactical victory.
- [ ] Verify mixed known/unknown values, revised valuation, repeated records and missing enrichment through application-level report outputs.

**Spec coverage:** User Stories 22, 48. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

