# 03 — Preserve enrichment through terminal events

**What to build:** Every terminal Public Contract event starts from its retained Contract Observation, synchronously attempts only useful missing or expired enrichment, and renders without losing stronger Issuer or Observed Location evidence that existed while the contract was public.

**Blocked by:** 01 — Synchronously enrich public-location listings; 02 — Enrich player-structure listings.

**Status:** resolved

- [x] Collector-level red tests cover Sale Confirmed, Purchase Confirmed, Expired, and Closed — Outcome Unknown through the same pre-delivery enrichment boundary.
- [x] Terminal events retain observation-time Issuer, Observed Affiliation, structure, solar-system, region, and evidence provenance across disappearance and restart.
- [x] Missing or expired location evidence may be retried synchronously before initial terminal delivery, within the same optional 30-second budget.
- [x] A failed terminal revalidation cannot clear or replace stronger Retained Enrichment Evidence with a regional or unresolved result.
- [x] Every terminal embed renders a human-readable region or more specific supported location and never a raw location identifier.
- [x] Existing lifecycle classification, Offered Item and Requested Item direction, contract totals, and observation windows remain unchanged.
- [x] Acceptance Confirmed does not claim an exact transfer time and does not identify an Acceptor.
- [x] Unavailable optional identity or location facts are omitted rather than displayed as empty, unknown, or unresolved fields.
- [x] Once Discord accepts the initial terminal post, later enrichment does not schedule an enrichment-driven edit.
- [x] Discord delivery retry and repair behavior for actual transport failures remains unchanged.
- [x] Focused terminal/restart tests and regular typechecking pass.

## Answer

All four terminal lifecycle kinds now reuse retained observation-time identity and location evidence, synchronously retry useful missing or expired location precision within one shared deadline, and fall back without downgrade. Post-disappearance refresh cannot backfill dynamic identity into the original observation, and already-sent terminal messages cannot receive enrichment-driven edits; prepared transport replay and explicit Historical Enrichment Repair remain intact. Nineteen focused terminal tests, transport replay, historical rerender dispatch, formatting, diff validation, and `cargo check` passed. Standards and Spec reviews passed after remediation.
