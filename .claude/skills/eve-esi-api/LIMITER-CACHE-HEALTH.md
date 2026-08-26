# Limiter, cache, and health

## Shared limiter

ESI operations may expose a route bucket, the legacy shared error allowance, `Retry-After`, or incomplete headers. Inspect the current operation and live response before selecting the limiter model.

Record domain evidence and Error-Charged status independently. Persist authoritative limiter evidence before interpreting the domain result.

For the legacy error allowance:

- non-`2xx`/`3xx` responses consume shared allowance even when their body establishes useful domain evidence;
- `Remain` and `Reset` describe one fixed observation window;
- complete current-window headers may lower persisted remaining capacity;
- missing or incomplete headers preserve a still-valid pair and its original anchor;
- a complete newer pair starts the next window after expiry;
- recovery paces first, then transactionally rechecks shared pause, bucket, and current-window evidence and commits before HTTP;
- the application owns the reserve collectively, so attribution to one caller requires per-request history.

When authoritative headers are unavailable, apply the bounded fallback encoded in current source and ADR. Keep numeric allowances out of this reference.

## Conditional caching and health

Track four clocks:

| Clock | Meaning |
|---|---|
| Content updated | Body or representation changed |
| Validated | `200` or `304` proved the representation current |
| Observation completed | A regional owner completed a consistent sweep |
| Domain observed | A contract or location fact received evidence |

`304 Not Modified` advances validation/progress while retaining the cached body. Collection liveness should use regional completion or validation time; body-cache `updated_at` alone can report stale ESI during successful conditional sweeps.

## Verification

Cover route-bucket presence/absence, complete and missing legacy headers, fixed-window rollover, post-pacing shared-floor races, `Retry-After`, `420`/`429`, `304` validator reuse, restarts from persisted limiter state, and post-response persistence failure. Production windows must separate shared-counter evidence from caller attribution.

Project sources: `src/contract_intelligence.rs` and `docs/adr/0004-recover-contract-monitoring-in-process.md`.

Official sources: [ESI best practices](https://developers.eveonline.com/docs/services/esi/best-practices/) and [ESI rate limiting](https://developers.eveonline.com/docs/services/esi/rate-limiting/).
