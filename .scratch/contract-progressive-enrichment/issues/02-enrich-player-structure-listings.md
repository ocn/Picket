# 02 — Enrich player-structure listings

**What to build:** A matched Public Contract listing at a player structure uses the existing Structure Authorization to retain and render the strongest permitted structure, solar-system, and regional evidence, while denied or unavailable access degrades truthfully without blocking the public feed.

**Blocked by:** 01 — Synchronously enrich public-location listings.

**Status:** resolved

- [x] A collector-level test is written red first for a current authenticated structure and proves the final listing contains the structure name, solar system, and human-readable region.
- [x] Only the player-structure operation carries the scoped bearer token; every related public request is demonstrably unauthenticated.
- [x] Current structure evidence outranks observation-time, Last-Verified Location, and regional fallback evidence.
- [x] Observation-time evidence outranks expired Last-Verified Location evidence, and a lower-quality response cannot erase stronger retained evidence.
- [x] Last-Verified Location renders the supported solar system and region, `Player-owned structure`, and a compact verification date without presenting a stale structure name as current.
- [x] Denied, revoked, timed-out, or unavailable structure access reduces location precision without changing match eligibility, ping eligibility, lifecycle state, or ordinary delivery.
- [x] Region-only fallback remains human-readable and omits a useless structure line when no more specific retained fact exists.
- [x] Structure response cache, ETag revalidation, credential revision fencing, persisted limiter state, and retry boundaries remain intact.
- [x] Structure facts, verification time, evidence class, and source are retained indefinitely through the existing PostgreSQL evidence authority.
- [x] Focused resolver/collector tests and regular typechecking pass.

## Answer

Player-structure listings now retain the observed region with current authenticated structure evidence, render current structure/system/region precision, and degrade through observation-time evidence before a dated generic Last-Verified Location. Failed revalidation cannot erase stronger retained observation context. Structure Authorization remains confined to the structure operation; focused collector, authorization, cache, ETag, credential-fencing, limiter, and retry tests passed, as did `cargo check`. Standards and Spec reviews passed after the precedence remediation.
