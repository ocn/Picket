# 01 — Synchronously enrich public-location listings

**What to build:** Every matched Public Contract listing waits at one bounded Progressive Enrichment barrier while independent public Issuer and Observed Location facts are resolved concurrently, retained, and rendered through the approved compact contract embed before Discord delivery is prepared.

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] A production-shaped Contract Collector test is written red first and proves a matched public-station listing is not prepared before its synchronous enrichment barrier completes.
- [x] Independent Issuer affiliation/name, item-name, and first-step location requests can make progress concurrently while dependent station-to-system-to-region lookups remain correctly ordered.
- [x] Optional enrichment is bounded to 30 seconds and prepares the strongest supported payload available at the deadline without empty or `Unknown` placeholders.
- [x] A public station listing renders the resolved station, solar system, and human-readable region through the shared compact `in/on` presentation.
- [x] Resolved Issuer character, corporation, and alliance evidence is retained with identifiers and provenance while presentation uses names and the contextual `issuer` role.
- [x] Public contract, station, system, constellation, region, affiliation, name, and item requests never carry Structure Authorization.
- [x] ESI cache, ETag, persisted limiter, and Retry-After boundaries remain authoritative while enrichment is concurrent.
- [x] Human-readable region is a required presentation invariant; if every name source is unavailable, initial delivery remains deferred instead of rendering a raw identifier.
- [x] No raw region, station, system, character, corporation, or alliance identifier is used as a user-facing label.
- [x] Focused collector tests and regular typechecking pass.

## Answer

Public-location listings now use one synchronous enrichment deadline across snapshot and notification preparation, resolve independent public identity and location evidence concurrently, defer when no human-readable region is available, retry incomplete required evidence on a later cycle, retain explicit Public ESI Issuer provenance, and omit unavailable optional facts. The focused collector seam passed 6 tests with zero failures, and `cargo check` completed successfully. Standards review found no documented violations; Spec review passed.
