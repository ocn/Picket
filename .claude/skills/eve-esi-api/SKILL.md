---
name: eve-esi-api
description: Use when changing or debugging EVE ESI calls, public-contract lifecycle or recovery, conditional caching, shared rate limits, SSO/JWKS, player-structure resolution, ESI health, or typed EVE identifiers
---

# EVE ESI API

## Quick start

1. Identify the exact operation, authentication scope, and EVE identifiers involved.
2. Inspect CCP's current operation instead of trusting remembered response codes:

```sh
.claude/skills/eve-esi-api/scripts/inspect-openapi.sh \
  '/contracts/public/items/{contract_id}'
```

3. Classify the result independently across five axes:
   - transport;
   - cache validation;
   - shared rate-limit effect;
   - domain evidence;
   - durable state transition.
4. Read [REFERENCE.md](REFERENCE.md) before changing contracts, structures, auth, caching, limiter state, or health checks.

## Non-negotiable rules

- OpenAPI describes the latest schema; observed headers/body describe the actual request. Compare `x-compatibility-date` with runtime `X-Compatibility-Date`; an unversioned request receives the oldest compatible behavior.
- Useful domain evidence can still be Error-Charged. Persist limiter headers before interpretation; preserve incomplete current-window evidence and its fixed anchor.
- Recheck the shared limiter immediately before admission. Hold no database lock across waits or HTTP; use serial bounded admission unless explicit capacity and in-flight reservations justify concurrency.
- Keep content freshness, validation, collection progress, and domain observation as separate clocks. A `304` validates cached content without rewriting it.
- Infer neither sale from disappearance, global auth failure from one inaccessible structure, nor unique stuck objects from retry-row counts.
- Never log access tokens, refresh tokens, JWTs, authorization headers, or credential-bearing response bodies.

## Domain checkpoints

- Only complete regional observations establish presence or **No Longer Public**; disappearance creates **Awaiting Resolution**, not a sale.
- Retain complete manifests. `is_included=true` is Offered; `false` is Requested. Resolve outcomes only from tested evidence and expiration rules.
- Persist lifecycle/outbox state durably and idempotently before dependent work.
- Structure lookup is character-authorized and ACL-dependent; one inaccessible structure is not global auth failure. Preserve historical verified location evidence.
- For heterogeneous JWKS, select the token `kid`, then require the expected key type and algorithm.

## Verification checklist

- Use loopback HTTP plus temporary PostgreSQL at the real collector/resolver seam.
- Assert order, validators, auth, limiter persistence, restart behavior, and durable lifecycle/delivery state.
- Cover missing headers, rollover, `304`, `204`, accepted-player evidence, structure denial, malformed responses, `420`/`429`, and post-response database failure.
- Measure durable transitions and net due-backlog drain separately from HTTP starts and retry rows; shared counters cannot attribute consumption without request history.

## Project map

- `src/contract_intelligence.rs`: public contracts, cache, limiter, recovery, delivery, and health.
- `src/structure_resolver.rs`: SSO/JWT/JWKS validation and authorized structure lookup.
- `src/esi.rs`: killmail and general universe ESI calls.
- `CONTEXT.md`: canonical contract terminology.
- `docs/contract-intelligence.md`: operational behavior.
- `docs/adr/0004-recover-contract-monitoring-in-process.md`: recovery ownership and safety constraints.

## References

See [REFERENCE.md](REFERENCE.md) for the domain-object map, response interpretation matrix, limiter/cache model, health guidance, and official sources.
