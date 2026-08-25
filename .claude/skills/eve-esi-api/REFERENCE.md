# EVE ESI domain reference

## Contents

- [Reasoning model](#reasoning-model)
- [Public-contract objects](#public-contract-objects)
- [Rate-limit model](#rate-limit-model)
- [Conditional caching and health](#conditional-caching-and-health)
- [Structures, locations, and SSO](#structures-locations-and-sso)
- [IDs are typed domain values](#ids-are-typed-domain-values)
- [zKillboard and killmail boundary](#zkillboard-and-killmail-boundary)
- [Persistence and metrics](#persistence-and-metrics)
- [Test seams](#test-seams)
- [Official sources](#official-sources)

## Reasoning model

For every ESI response, answer these independently:

1. **Wire:** Did an HTTP request start, and was the response complete and parseable?
2. **Cache:** Is this new content, a validator-confirmed representation, or no reusable representation?
3. **Limiter:** Which headers are authoritative, which shared allowance changed, and when does its window reset?
4. **Domain:** What fact does the response establish about this exact EVE object?
5. **State:** Which durable transition is permitted, and which facts must remain unchanged?

Do not collapse these questions into `success` versus `failure`.

## Public-contract objects

### Regional contract summary

`GET /contracts/public/{region_id}` returns public contract summaries. Relevant item-exchange fields map into this project's language as follows:

| ESI field | Project meaning |
|---|---|
| `contract_id` | Stable Contract ID and EVE contract-address component |
| `issuer_id` | Issuer character; never Acceptor |
| `issuer_corporation_id` | Issuer's corporation at issuance |
| `start_location_id` | Observed start location identifier |
| `date_issued`, `date_expired` | Issuance and known expiration boundaries |
| `price` | Officially the item-exchange/auction price; this project interprets it as Requested ISK when paired with offered-item evidence |
| `reward` | Officially courier remuneration; this project also preserves a tested item-exchange compatibility rule that interprets a positive reward as Offered ISK when only requested items exist |
| `type` | Filter to the supported contract kind before interpreting money/items |

A summary is a public-presence observation. Its later absence is only meaningful when the regional observation is complete and internally consistent.

### Contract item

`GET /contracts/public/items/{contract_id}` returns manifest members:

| ESI field | Project meaning |
|---|---|
| `record_id` | Contract-system record identity |
| `type_id` | Inventory Type ID, not a Group ID |
| `quantity` | Stack quantity |
| `is_included=true` | Offered Item supplied by the Issuer |
| `is_included=false` | Requested Item required from the accepting party |
| `item_id` | Optional concrete item identity; absent for requested items |

A complete Retained Contract Manifest is immutable project evidence. Reuse it across disappearance and restart; do not refetch it merely because the contract became terminal.

### Lifecycle evidence matrix

| Observation | Permitted conclusion |
|---|---|
| Missing from incomplete/inconclusive regional observation | None |
| Missing from a complete later regional observation | No Longer Public; Awaiting Resolution |
| `304 Not Modified` with a complete retained representation | Representation remains valid; record validation progress |
| Documented `204` contract-items response | Expired or recently accepted; combine with time and retained evidence, never guess |
| Exact project-recognized accepted-player response | Acceptance Confirmed regardless of the expiration boundary; still Error-Charged if non-2xx/3xx |
| No-content at or after known expiry | Expired, not Acceptance Confirmed |
| Other forbidden/not-found/malformed/transient response | Inconclusive unless an explicit tested rule says otherwise |

The Acceptor is not exposed by the global public-contract feed. Omit it rather than inferring it.

## Rate-limit model

ESI may expose a route bucket, the legacy shared error allowance, `Retry-After`, or incomplete headers. Inspect the current operation and live headers; do not assume all routes use the same mechanism.

For the legacy error allowance:

- non-`2xx`/`3xx` responses consume a shared allowance even when the body establishes useful domain evidence;
- `Remain` and `Reset` describe one fixed observation window;
- complete current-window headers may lower the persisted remaining value;
- missing/incomplete headers preserve a still-valid pair;
- a later observation must not move the original window anchor forward;
- after expiry, a complete newer pair starts the next window;
- recovery must recheck the shared row after pacing and immediately before the HTTP start;
- a reserve belongs to the application, not exclusively to one caller, so production telemetry cannot attribute its consumption without per-request history.

If authoritative headers are unavailable, use the project's bounded fallback rather than inferring capacity. Read the current constants and ADR instead of copying numeric values from prose.

## Conditional caching and health

Model at least these timestamps separately:

- **content updated:** response body or representation changed;
- **validated:** `200` or `304` proved the representation current;
- **observation completed:** a regional owner completed a consistent sweep;
- **domain observed:** a contract/location fact was evidenced at a specific time.

`304 Not Modified` should advance validation/progress without rewriting content. A health check based only on body-cache `updated_at` can report stale ESI while conditional regional sweeps are succeeding. Prefer regional completion or validation time for collection liveness.

## Structures, locations, and SSO

Public contracts can contain NPC station IDs or player-structure IDs. Resolving a player structure requires `esi-universe.read_structures.v1` and access through the authorized character's ACL.

Keep these boundaries distinct:

- token refresh/discovery/JWKS/JWT/client/character failure is global authorization failure;
- a structure lookup forbidden/not-found result is per-location access evidence;
- a successful lookup is Access-Qualified Evidence for name, solar system, owner, and optional position;
- previous verified evidence remains historical evidence after access changes.

JWT/JWKS validation must:

1. trust only configured discovery/JWKS origins;
2. require a token `kid`;
3. select the matching supported key and algorithm;
4. ignore unrelated EC/other keys in a heterogeneous JWKS;
5. validate issuer, audience, expiration, subject/character, and required scope;
6. keep refresh/access tokens memory-only unless an explicit provisioning path durably rotates them.

## IDs are typed domain values

Do not interchange:

- `contract_id` and contract item `record_id`;
- inventory `type_id` and `group_id`;
- `region_id`, `solar_system_id`, station/location ID, and player `structure_id`;
- character, corporation, alliance, and owner IDs;
- content observation time and identity-resolution time.

Use names that carry the type (`region_id`, not `id`) and validate positive/range constraints at the boundary.

## zKillboard and killmail boundary

zKillboard RedisQ is a third-party transport, not ESI. Keep its envelope and metadata separate from the official killmail representation:

- the RedisQ package controls feed delivery and timeout behavior;
- zKillboard metadata such as valuation and location hints is not an ESI domain object or authorization fact;
- the embedded killmail reference identifies the official killmail fetch, whose response has its own schema and failure semantics;
- never deserialize third-party metadata into an ESI model merely because both describe the same killmail.

See `src/feed/redisq.rs`, `src/models.rs`, and `src/esi.rs` for the project boundary.

## Persistence and metrics

- A retry/failure row is an attempt record, not a unique failed contract.
- Awaiting Resolution case count is the backlog object count.
- Resolution Throughput counts durably committed terminal transitions.
- Net drain is starting due cases minus ending due cases over an exact interval; arrivals make it differ from transition throughput.
- Prepared/permanent delivery debt measures notification health separately from lifecycle state.
- Persist response/limiter evidence before scheduling retries; database/cache failures after an admitted request must fail closed while retaining request accounting.

## Test seams

Prefer a controlled loopback HTTP server plus temporary PostgreSQL and the public collector/resolver entry point. Assert:

- no `Authorization` header on public contract routes;
- one Bearer header only on the structure route;
- `If-None-Match`/`304` reuse and validation time;
- exact wire status/body classification;
- shared limiter race after pacing and before start;
- restart behavior from persisted limiter/case/outbox state;
- no later probe before required durable delivery;
- no secret in error/debug output.

## Official sources

- [Current ESI OpenAPI](https://esi.evetech.net/meta/openapi.json)
- [ESI best practices](https://developers.eveonline.com/docs/services/esi/best-practices/)
- [ESI rate limiting](https://developers.eveonline.com/docs/services/esi/rate-limiting/)
- [EVE SSO and JWT validation](https://developers.eveonline.com/docs/services/sso/)
- [Structure lookup API Explorer](https://developers.eveonline.com/api-explorer#/operations/GetUniverseStructuresStructureId)

Project behavior may intentionally be stricter than the public specification. When official documentation, live behavior, and project fixtures differ, record all three and require an explicit tested compatibility rule.
