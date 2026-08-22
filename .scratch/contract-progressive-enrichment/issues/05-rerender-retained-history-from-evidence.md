# 05 — Rerender retained history from Retained Enrichment Evidence

**What to build:** Historical Enrichment Repair migrates eligible retained Discord messages to the enriched presentation revision using only retained evidence and permitted public region-name lookup, preserving the original message identity without mentions or replacement posts.

**Blocked by:** 04 — Guarantee feed-wide presentation parity.

**Status:** claimed

- [ ] A red operator-level test proves dry-run and queue selection target only sent, retained, outdated deliveries with valid Discord message identities and retained data.
- [ ] Historical reconstruction uses the retained Contract Event, Contract Observation, Retained Enrichment Evidence, structure cache, system catalog, and unauthenticated region-name lookup.
- [ ] Historical reconstruction makes no authenticated structure request.
- [ ] Missing required human-readable region names reject the bounded queue atomically rather than exposing a raw identifier or partially queueing a batch.
- [ ] Historical edits preserve Contract Event identity, delivery nonce, channel, ping eligibility, and Discord message identifier.
- [ ] Edit payloads permit no mentions and use PATCH only; a failed edit never creates a replacement POST.
- [ ] Deleted, retired, prepared, malformed, active-repair, and permanently failed identities are excluded and reported consistently.
- [ ] Candidate state is selected freshly for dry run and queue; an explicit delivery set cannot silently expand.
- [ ] Persisted Retry-After stops the batch at its boundary, and a permanent failure leaves later repairs durable but undispatched.
- [ ] Clean and current production migration ledgers remain valid, including backward-compatible retained payloads and applied migration checksums.
- [ ] Focused operator, migration, and Discord-wire tests plus regular typechecking pass.
