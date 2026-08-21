# 01 — Synchronously enrich public-location listings

**What to build:** Every matched Public Contract listing waits at one bounded Progressive Enrichment barrier while independent public Issuer and Observed Location facts are resolved concurrently, retained, and rendered through the approved compact contract embed before Discord delivery is prepared.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A production-shaped Contract Collector test is written red first and proves a matched public-station listing is not prepared before its synchronous enrichment barrier completes.
- [ ] Independent Issuer affiliation/name, item-name, and first-step location requests can make progress concurrently while dependent station-to-system-to-region lookups remain correctly ordered.
- [ ] Optional enrichment is bounded to 30 seconds and prepares the strongest supported payload available at the deadline without empty or `Unknown` placeholders.
- [ ] A public station listing renders the resolved station, solar system, and human-readable region through the shared compact `in/on` presentation.
- [ ] Resolved Issuer character, corporation, and alliance evidence is retained with identifiers and provenance while presentation uses names and the contextual `issuer` role.
- [ ] Public contract, station, system, constellation, region, affiliation, name, and item requests never carry Structure Authorization.
- [ ] ESI cache, ETag, persisted limiter, and Retry-After boundaries remain authoritative while enrichment is concurrent.
- [ ] Human-readable region is a required presentation invariant; if every name source is unavailable, initial delivery remains deferred instead of rendering a raw identifier.
- [ ] No raw region, station, system, character, corporation, or alliance identifier is used as a user-facing label.
- [ ] Focused collector tests and regular typechecking pass.
