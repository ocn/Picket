# Public contracts

## Regional contract summary

`GET /contracts/public/{region_id}` returns public contract summaries. This project maps supported item-exchange fields as follows:

| ESI field | Project meaning |
|---|---|
| `contract_id` | Stable Contract ID and contract-address component |
| `issuer_id` | Issuer character; Acceptor remains unknown |
| `issuer_corporation_id` | Issuer's corporation at issuance |
| `start_location_id` | Observed start location identifier |
| `date_issued`, `date_expired` | Issuance and known expiration boundaries |
| `price` | Officially the item-exchange/auction price; Requested ISK when paired with offered-item evidence in this project |
| `reward` | Officially courier remuneration; a tested project compatibility rule treats a positive reward as Offered ISK when only requested items exist |
| `type` | Contract kind; filter to supported kinds before interpreting money or items |

A complete regional observation establishes public presence. Only a complete, internally consistent later observation can establish **No Longer Public**.

## Contract item

`GET /contracts/public/items/{contract_id}` returns manifest members:

| ESI field | Project meaning |
|---|---|
| `record_id` | Contract-system record identity |
| `type_id` | Inventory Type ID |
| `quantity` | Stack quantity |
| `is_included=true` | Offered Item supplied by the Issuer |
| `is_included=false` | Requested Item required from the accepting party |
| `item_id` | Optional concrete identity; requested items omit it |

A complete Retained Contract Manifest is immutable evidence. Reuse it across disappearance and restart.

## Lifecycle evidence

| Observation | Durable conclusion |
|---|---|
| Missing from incomplete/inconclusive regional observation | Preserve the prior public state |
| Missing from a complete later regional observation | No Longer Public; create Awaiting Resolution |
| `304 Not Modified` with a retained complete representation | Representation remains valid; advance validation progress |
| Documented `204` before known expiry | Closed Outcome Unknown |
| Documented `204` at or after known expiry | Expired |
| Exact project-recognized accepted-player response | Acceptance Confirmed regardless of expiration; retain Error-Charged status |
| Other forbidden/not-found/malformed/transient response | Preserve Awaiting Resolution and apply the tested retry policy |

Represent Acceptor as unknown because the global public feed omits it. Prepare lifecycle/outbox state durably and idempotently before dependent work.

## Persistence and metrics

- A retry/failure row counts an attempt; Awaiting Resolution cases count unique backlog objects.
- Resolution Throughput counts durably committed terminal transitions.
- Net drain is starting due cases minus ending due cases over an exact interval; arrivals make it differ from transition throughput.
- Prepared/permanent delivery debt measures notification health separately from lifecycle state.
- Account for every admitted request before response processing. End the pass when subsequent database/cache persistence fails.
- Require explicit accepted-player evidence for a sale, and count unique stuck objects from Awaiting Resolution cases rather than retry rows.

## Verification

Cover complete/incomplete regional observations, retained manifests, conditional `304`, documented `204`, exact accepted-player evidence, expiry, malformed/transient responses, restart, outbox idempotency, and delivery-before-dependent-probe ordering.

Project sources: `CONTEXT.md`, `src/contract_intelligence.rs`, `docs/contract-intelligence.md`, and `docs/adr/0004-recover-contract-monitoring-in-process.md`.

Official source: [Current ESI OpenAPI](https://esi.evetech.net/meta/openapi.json).
