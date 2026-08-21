# 03 — Preserve enrichment through terminal events

**What to build:** Every terminal Public Contract event starts from its retained Contract Observation, synchronously attempts only useful missing or expired enrichment, and renders without losing stronger Issuer or Observed Location evidence that existed while the contract was public.

**Blocked by:** 01 — Synchronously enrich public-location listings; 02 — Enrich player-structure listings.

**Status:** ready-for-agent

- [ ] Collector-level red tests cover Sale Confirmed, Purchase Confirmed, Expired, and Closed — Outcome Unknown through the same pre-delivery enrichment boundary.
- [ ] Terminal events retain observation-time Issuer, Observed Affiliation, structure, solar-system, region, and evidence provenance across disappearance and restart.
- [ ] Missing or expired location evidence may be retried synchronously before initial terminal delivery, within the same optional 30-second budget.
- [ ] A failed terminal revalidation cannot clear or replace stronger Retained Enrichment Evidence with a regional or unresolved result.
- [ ] Every terminal embed renders a human-readable region or more specific supported location and never a raw location identifier.
- [ ] Existing lifecycle classification, Offered Item and Requested Item direction, contract totals, and observation windows remain unchanged.
- [ ] Acceptance Confirmed does not claim an exact transfer time and does not identify an Acceptor.
- [ ] Unavailable optional identity or location facts are omitted rather than displayed as empty, unknown, or unresolved fields.
- [ ] Once Discord accepts the initial terminal post, later enrichment does not schedule an enrichment-driven edit.
- [ ] Discord delivery retry and repair behavior for actual transport failures remains unchanged.
- [ ] Focused terminal/restart tests and regular typechecking pass.
