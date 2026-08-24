# 02 — Reuse Retained Contract Manifests

**What to build:** Make every complete Retained Contract Manifest authoritative for its Contract ID so repeated Regional Observations avoid unnecessary public item requests without changing listing, lifecycle, matching, or presentation behavior.

**Blocked by:** None — can start immediately.

**Status:** claimed

- [ ] A repeated public Contract ID with a complete retained manifest completes its next Regional Observation with zero item-endpoint requests even after HTTP cache metadata expires.
- [ ] Reuse preserves the complete Offered Item and Requested Item membership, manifest identity, presence history, matching result, and any resulting delivery presentation.
- [ ] A newly observed Contract ID with no retained manifest still requests and persists its public items before becoming a complete observed contract.
- [ ] An incomplete or previously failed manifest remains eligible for public item collection and resolves its pending and failure state after success.
- [ ] A pending manifest that disappears from a later complete regional observation is removed or resolved under the existing lifecycle rules without an unsupported terminal claim.
- [ ] A large-region regression proves item-request volume scales with missing manifests rather than total public contract summaries.
- [ ] Public item requests remain unauthenticated and retain existing cache validators, pagination consistency, persisted limiter, `Retry-After`, and failure classification behavior.
- [ ] Missing-manifest retry remains part of regional collection; no independent manifest worker, schema, migration, service, queue, or configuration variable is added.
- [ ] Existing listing, recovery baseline, disappearance, terminal lifecycle, Progressive Enrichment, and durable delivery regressions pass.
- [ ] Focused collector tests, formatting, compilation, and diff checks pass.
- [ ] A Sol high Standards/Spec review finds no blocking issue before the ticket is resolved.
