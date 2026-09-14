# Global Public Contract Intelligence

## Scope and evidence

This feed observes unauthenticated public EVE item-exchange contracts in every public region. It does not use character, corporation, or alliance authorization. The issuer is public; an acceptor or counterparty is not, so neither is displayed or stored.

The collector first records a complete regional baseline silently. A complete later observation can create a Listed event. When a contract is no longer public, it enters Awaiting Resolution and is not reported as a sale. A pre-expiry `204` from the public item endpoint produces Closed — Outcome Unknown; it does not confirm acceptance. `404` and other unavailable evidence do not confirm acceptance.

| Event action | Public evidence |
| --- | --- |
| `listed` | Newly observed after a regional baseline |
| `sale_confirmed` | Issuer offered a matched ship, requested positive ISK, no items were requested, and acceptance was confirmed |
| `purchase_confirmed` | Issuer requested a matched ship, offered positive ISK, no items were offered, and acceptance was confirmed |
| `expired` | Contract ceased to be public at its known expiry boundary without earlier acceptance evidence |
| `closed_outcome_unknown` | Contract ceased to be public and no supported terminal outcome could be established |

`awaiting_resolution` is internal only and has no alert action. No alert is provisional or retracted; a previously committed contract-delivery message can be repaired durably in place. Public ESI cache headers bound latency; the application does not claim a real-time post-disappearance contract-status lookup.

## Configuration and startup

Docker Compose starts PostgreSQL 16. Its safe default uses the default local-only credentials. If you set a non-default `CONTRACT_DATABASE_PASSWORD` before first startup, also set an explicit `CONTRACT_DATABASE_URL` using the Compose host `postgres:5432` and a percent-encoded password. Compose interpolates values before the container starts; it cannot safely derive a PostgreSQL URI from an arbitrary raw password. Changing either value after the database volume exists does not change the database role password.

```sh
cp docs/env.sample .env
# Set DISCORD_BOT_TOKEN and DISCORD_CLIENT_ID.
# If changing CONTRACT_DATABASE_PASSWORD, set a matching percent-encoded CONTRACT_DATABASE_URL.
docker compose up --build -d
docker compose ps
```

For a non-Compose process, set `CONTRACT_DATABASE_URL` and optionally `CONTRACT_COLLECTION_INTERVAL_SECS` (default: 300 seconds); use `127.0.0.1:5433` for the local exposed Compose port. `CONTRACT_REGIONAL_CONCURRENCY` defaults to `2` and accepts only `1` through `4`; set it to `1` to roll back collection scheduling to sequential regional attempts without changing schema or retained observations. `CONTRACT_PROXIMITY_RECONCILIATION_INTERVAL_SECS` defaults to 30 seconds and accepts only `1` through `30`; it re-evaluates retained Proximity-Unverified alerts and dispatches their durable edits/promotions without starting public ESI collection. An absent value uses the default; malformed, out-of-range, or non-Unicode values disable only the affected loop at startup and leave the killmail feed running. PostgreSQL URI userinfo reserves characters including `:`, `/`, `?`, `#`, `[`, `]`, and `@`; percent-encode them (and a literal `%`) in the URL, while `CONTRACT_DATABASE_PASSWORD` remains the raw password. Each successful connection runs the SQLx migrations. If `CONTRACT_DATABASE_URL` is absent, contract collection and its commands are unavailable, while the killmail feed continues unchanged.

## Durable delivery repair

`CONTRACT_DELIVERY_OPERATOR_ID` must be `discord:146451271497416704`; `CONTRACT_DELIVERY_OPERATOR_TOKEN_SHA256` must be the 64-character hexadecimal SHA-256 digest of a separate 64-character raw token generated from 32 random bytes. Compose passes both settings only to `discordbot`. The token is supplied on standard input (capped at 128 bytes) and is never accepted as an argument, printed, or written to the audit table.

Inspect or requeue an unresolved permanent delivery with the real binary inside the bot container. The `--actor` option is rejected: the configured identity is attached only after the stdin capability succeeds.

```sh
read -r -s CONTRACT_DELIVERY_REPAIR_TOKEN
printf %s "$CONTRACT_DELIVERY_REPAIR_TOKEN" | docker compose exec -T discordbot ./killbot-rust contract-delivery inspect --id 123
printf %s "$CONTRACT_DELIVERY_REPAIR_TOKEN" | docker compose exec -T discordbot ./killbot-rust contract-delivery requeue --id 123
unset CONTRACT_DELIVERY_REPAIR_TOKEN
```

`requeue` records only the configured operator ID and re-enables the durable work after the underlying Discord condition is corrected.

## Optional Structure Resolver

Public contract discovery, manifests, lifecycle probes, issuer facts, and delivery always remain unauthenticated. Provision the feature-gated resolver with `ENV_FILE=.env scripts/structure-resolver-provision.sh`; it requests only `esi-universe.read_structures.v1`, keeps the client secret and complete callback hidden, validates callback state before code exchange, and validates the signed JWT’s issuer, both audiences, expiry, character subject, and exact singleton scope. The procedure follows CCP’s [SSO requirements](https://developers.eveonline.com/docs/services/sso/) for registered redirects, state, refresh-token handling, discovery, and JWKS validation.

The wizard reuses `EVE_CLIENT_ID`/`EVE_CLIENT_SECRET` only when the operator attests in the Developer Portal that the existing application has the exact registered redirect and required scope; the successful state-checked exchange is the compatibility proof. Otherwise it preserves standings SSO and writes the resolver-only `STRUCTURE_RESOLVER_CLIENT_ID`/`STRUCTURE_RESOLVER_CLIENT_SECRET` override. It finally persists `STRUCTURE_RESOLVER_ENABLED`, `STRUCTURE_RESOLVER_CHARACTER_ID`, `STRUCTURE_RESOLVER_REFRESH_TOKEN`, `STRUCTURE_RESOLVER_CREDENTIAL_REVISION`, and `STRUCTURE_RESOLVER_REDIRECT_URI` with mode `0600`; `.env` is excluded from the Docker build context. No repository workflow consumes these credentials, so the wizard does not set CI secrets.

Before enabling enrichment, the wizard probes supplied known structure IDs and reports `full`, `partial`, `denied`, or `indeterminate`. Full means only that each supplied ID resolved; partial and denied access remain Access-Qualified evidence, not universal access. CCP documents that the endpoint needs `esi-universe.read_structures.v1` on a character with docking access and provides no unified discovery of all accessible structures. [Official structure-resolution guidance](https://docs.esi.evetech.net/docs/scenarios/resolve_structure_ids.html) describes those limits. The resolver writes expiring Access-Qualified Location Evidence; public NPC evidence still outranks it, followed by unexpired operator evidence. Successful structure facts and ETags are retained independently of expiry; a restarted resolver revalidates expired entries with `If-None-Match` and reuses a `304 Not Modified` representation only with a renewed expiry. Structure response rate, retry, and ESI error-limit boundaries use the collector's shared persisted admission state.

Every newly listed contract that definitively matches a posting action requests solar-system context for presentation, even when its filter (for example, a region filter) does not require location to decide eligibility. Player-owned structure resolution still uses only the scoped authenticated resolver, and a denial or unavailable location degrades the embed without changing the region match or ping decision. A successful response durably retains the access-qualified structure name for restart-safe `304 Not Modified` revalidation and loads the public solar-system name before the listing embed is retained.

Presentation revision 5 makes the regional public-contract observation the minimum durable location for every final embed, including historical rerenders. It resolves the human-readable region name from the same loaded system catalog used by the main killfeed, falling back to the unauthenticated public region endpoint during live collection, and retains any more-specific structure or solar-system snapshot through terminal acceptance. Historical rendering first reuses retained event, snapshot, cache, and system-catalog evidence, then requests only missing names from the unauthenticated public region endpoint before opening the queue transaction. It rejects the bounded queue atomically rather than exposing a raw region ID when any name remains unavailable. Historical rerender never makes a structure-resolution request; authenticated structure requests continue to use only the scoped resolver client.

Set `STRUCTURE_RESOLVER_ENABLED=false` to roll back only authenticated enrichment. Missing, revoked, invalid, or denied authorization reduces location precision but never gates public collection, lifecycle analysis, or ordinary delivery. Denied structures are retried at most daily and parked after seven elapsed days. Rerun the wizard to replace or rotate authorization: it writes the returned refresh token once, advances `STRUCTURE_RESOLVER_CREDENTIAL_REVISION`, and requires a bot restart. Do not place refresh tokens in logs, command output, diagnostics, database rows, or source-controlled files.

Compose also starts `health-watchdog`, a separate REST-only process: it has no Discord gateway session and cannot block the bot or collection loops. It intentionally has no PostgreSQL health-gated `depends_on`, so it can report a cold-start database outage. It reads the persisted heartbeat and health snapshot, maintains one non-pinging Health View in `HEALTH_CHANNEL_ID`, and opens one consolidated incident when the current degradation set becomes non-empty. Only the configured operator is mentioned when that incident is created; view updates, incident edits, and recovery never mention anyone. `WATCHDOG_EVALUATION_INTERVAL_SECS` defaults to 60 and must be at least 10 seconds. `WATCHDOG_CONFIG_REVISION` defaults to `1`; bump it after correcting a Discord token or permission issue to retry a permanent publication failure. Changing `HEALTH_CHANNEL_ID` creates the persistent messages in the new channel without editing the old channel. PostgreSQL failures are marked degraded on the first consecutive failure and critical on the third; the interval bounds reconnect attempts. During an outage a warm watchdog cache can keep editing its known message IDs. A cold watchdog can create nonce-stable messages, but cannot persist their IDs until PostgreSQL returns; it then adopts those cached IDs rather than creating replacements. `Http::new` keeps Serenity's HTTP rate limiter enabled: it consumes a valid Discord `429 Retry-After` response before the publisher returns; a returned request failure (including an unparseable rate-limit response) uses the watchdog's transient retry deadline. Discord permanent errors are stored as visible health evidence and are not retried until configuration or persisted state changes.

Keep the PostgreSQL volume when rolling back the bot image. To roll back only the scheduling change, set `CONTRACT_REGIONAL_CONCURRENCY=1` and restart `discordbot`; no data rollback is needed. A prior bot release ignores the contract tables; the JSON-backed killmail subscriptions, caches, standings, and feed checkpoints are separate and unchanged. Re-enable the current image and database URL to resume collection; migrations are re-run safely against an already-current database. Do not remove the PostgreSQL volume as a rollback step.

## Subscriptions

Create or replace a subscription in the target guild channel with `/contract_subscribe`; remove it with `/contract_unsubscribe`. The command accepts a recursive filter JSON document and an event-action JSON document.

```text
/contract_subscribe id:super-sales description:public super sales filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed"]}},{"condition":{"item_types":{"direction":"offered","ids":[12345]}}}]}} event_actions:{"listed":"post","sale_confirmed":"post_and_ping"} ping_type:here
```

For a proximity feed, pass the same compact `ly_ranges_json` shape as `/subscribe`. The supplied ranges are combined with the required `filter` root using `AND`; multiple centers are a union. This exact example matches contracts within 8.0 light-years of your staging systems:

```text
/contract_subscribe id:supercaps-near-staging description:Supercapital contracts near staging filter:{"root":{"or":[{"condition":{"ship_groups":{"direction":"offered","ids":[30,659]}}},{"condition":{"ship_groups":{"direction":"requested","ids":[30,659]}}}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post"} ly_ranges_json:[{"system_id":YOUR_SYSTEM_ID,"range":8.0},{"system_id":YOUR_SECOND_SYSTEM_ID,"range":8.0}]
```

`ly_ranges_json` must be a non-empty JSON array of positive system IDs and finite positive ranges. Contract proximity uses the ESI solar-system Cartesian position in metres and includes a distance exactly on the boundary. If an observation-time event position or a center position is unavailable after all non-location predicates match, the system emits one durable, non-pinging Proximity-Unverified Alert. Filter nodes are `condition`, `and`, `or`, and `not`. Conditions support event kinds, offered/requested item presence, item type, ship group, ISK bounds, region, solar system, light-year ranges, security range, location ID, issuer character/corporation, observed issuer alliance, issuance type, and title fragment. IDs must be positive. The maximum filter depth is 16 and the maximum node count is 128.

Each action is `ignore`, `post`, or `post_and_ping`. Omitted actions, including `listed`, default to `ignore`, so `{"sale_confirmed":"post"}` is valid. `ping_type:here` uses `@here`; `ping_type:everyone` changes all pinging actions in that subscription to `@everyone`. A contract ping shares the existing per-channel five-minute limiter with killmail pings. Delivery still posts if a requested ping is rate-limited.

## Recovery, storage, and operation

The collector records public facts, manifests, presence intervals, lifecycle evidence, subscriptions, prepared deliveries, and collection failures in PostgreSQL. Prepared deliveries are replayed after restart and carry an idempotency nonce. Discord nonce enforcement is applied only during the implemented five-minute recent-message window. After an ambiguous failure, a retry after that window disables nonce enforcement; the resulting rare duplicate is intentionally accepted over silently losing a contract alert. A prolonged collection gap creates a silent recovery baseline to prevent notification floods; fresh pre-expiry acceptance evidence can still be confirmed.

Regional Observation first reuses each complete Retained Contract Manifest for the current Contract ID. It requests public items only when that manifest is absent, incomplete, or has a retained collection failure; missing-manifest retry remains part of Regional Observation. Terminal Resolution Recovery is the sole consumer of due Awaiting Resolution cases. Its in-process loop runs once per minute, skips missed ticks, and selects at most 256 oldest-due cases. It starts probes serially at least 250 ms apart after 2xx/3xx responses and at least one second apart after error-charged responses, stopping at a shared persisted ESI limiter, `Retry-After`, or current error-limit reserve of 40. If valid current-window error-limit evidence is absent, it falls back to at most 32 probes at roughly 1.875-second spacing. It reuses the durable case, public ESI client, notification preparation, and delivery path; it adds no schema, worker, service, queue, or tuning variable.

ESI-progress health reads the public-contract cache evidence the collector writes (`contracts/public/%`), while a persisted active ESI pause remains healthy for its deadline. The Runtime Health Snapshot and watchdog only detect, persist, and report degradation; neither stops or restarts a service, changes images, rolls back data or migrations, changes a subscription, or performs remediation.

Each regional attempt has its own committed observation batch. A cycle drains prepared Discord deliveries before starting ESI discovery, then runs at most the configured number of regions. A completed region can prepare and deliver its conclusive events while another region is still in flight; the final cycle report remains in public region-discovery order. When ESI records a persisted limiter or `Retry-After` boundary, no further regional request starts; attempts already in flight retain their own complete or inconclusive batch from actual evidence. A near-exhausted shared rate bucket is persisted and paces every subsequent ESI request in-cycle; it waits between admissions rather than repeatedly restarting an early regional sweep.

An unresolved proximity match creates one durable, non-pinging Proximity-Unverified Alert. Conclusive current location evidence is selected by location identifier under the shared location lock: public evidence, then access-qualified evidence, then operator evidence. Reconciliation edits the original visible alert durably. An in-range result also prepares at most one distinct, restart-safe promotion identity, using the original lifecycle action's retained ping eligibility; an out-of-range result creates no promotion. Repeated evidence, evaluation, dispatcher retry, and restart remain idempotent, and resolver degradation cannot replace retained conclusive evidence with an unconclusive result.

Monitor the bot and watchdog logs for `contract collection cycle finished`, rate-limit pauses, database unavailability, and unresolved delivery failures. Useful local checks are:

```sh
docker compose ps
docker compose config --quiet
docker compose logs --tail=200 discordbot
docker compose logs --tail=200 health-watchdog
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT status, count(*) FROM contract_outbound_deliveries GROUP BY status"
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT failure_kind, count(*) FROM contract_collection_failures WHERE resolved_at IS NULL AND classification IS NULL GROUP BY failure_kind"
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT view_key, channel_id, message_id, failure_kind, next_attempt_at FROM health_discord_views"
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT incident_key, channel_id, message_id, active, failure_kind, next_attempt_at FROM health_discord_incidents"
```

There is no runtime retention job. Contract facts, manifests, presence intervals, deliveries, and unresolved failures are retained. Complete `regional_observations` older than 90 days have a tested library cleanup path but are not automatically pruned. Size the database, back it up, and introduce an explicit retention policy before operating long term.

## Validation

The standard contract suite uses local PostgreSQL and loopback HTTP fixtures, including captured public summary/item payloads, cache and pagination representations, `204`, `304`, `404`, rate limits, and transient responses. It needs no ESI or Discord credentials.

```sh
docker compose up -d postgres
CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts cargo test --test test_contract_intelligence -- --test-threads=1
cargo check
cargo clippy -- -W clippy::all
CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:5433/killbot_contracts cargo test -- --skip esi::tests::test_esi_timeout_is_configured --test-threads=1
```

The final command skips only the independently proven baseline hang in `esi::tests::test_esi_timeout_is_configured`. Existing credentialed killmail and tracking embed checks remain ignored. A contract-specific ignored visual check sends exactly one non-pinging embed to a dedicated channel:

```sh
CONTRACT_TEST_DISCORD_CHANNEL_ID=<dedicated-channel-id> cargo test manual_contract_embed_visual_delivery -- --ignored --nocapture
```

Review the title, compact fields, no ping, and standalone `contract:0//<id>` Contract Address field, then delete the manual message.

## Deliberate exclusions

This release has no authenticated-contract feed, counterparty tracking, provisional alert, Discord retraction, JSON-to-PostgreSQL migration, or horizontal-scaling machinery. It performs durable edits only to repair its own existing contract-delivery messages; an edit never creates a replacement post.

## External references

- [EVE SSO and JWT validation](https://developers.eveonline.com/docs/services/sso/)
- [ESI best practices](https://developers.eveonline.com/docs/services/esi/best-practices/)
- [ESI map data and solar-system endpoints](https://developers.eveonline.com/docs/guides/map-data/)
- [ESI rate limiting](https://developers.eveonline.com/docs/services/esi/rate-limiting/)
- [Discord interaction responses](https://docs.discord.com/developers/interactions/receiving-and-responding)
- [Discord message creation and nonce enforcement](https://docs.discord.com/developers/resources/message)
- [Discord rate limits](https://docs.discord.com/developers/topics/rate-limits)
- [Serenity HTTP rate limiter](https://docs.rs/serenity/0.11.7/serenity/http/struct.Ratelimiter.html)
- [Docker Compose variable interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)
- [PostgreSQL connection strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)

## Slash command reference

Contract subscriptions are PostgreSQL-backed and are separate from killmail subscriptions. Run these commands in the channel that should receive the contract feed.

| Command | Required arguments | Optional arguments | Result |
| --- | --- | --- | --- |
| `/contract_subscribe` | `id`, `description`, `filter`, `event_actions` | `ly_ranges_json`, `ping_type` | Creates or replaces this channel's public-contract subscription with the same `id`. |
| `/contract_unsubscribe` | `id` | None | Removes this channel's contract subscription with that `id`. |

Both `/contract_subscribe` and `/contract_unsubscribe` require the Manage Server (`MANAGE_GUILD`) permission and cannot be used in DMs; server admins can loosen this per role under Server Settings → Integrations → the bot → Command permissions. The [sov and watchlist runbook](docs/sov-and-watchlist-feeds.md#permissions) collects the full gated-command list and the same loosening steps.

`filter` is JSON with one `root` node. Nodes are `condition`, `and`, `or`, and `not`; the examples below use the deployed syntax. `event_actions` is JSON keyed by `listed`, `sale_confirmed`, `purchase_confirmed`, `expired`, and `closed_outcome_unknown`. Each value is `ignore`, `post`, or `post_and_ping`; omitted actions default to `ignore`.

`ly_ranges_json` is a non-empty array of system ranges, such as `[{"system_id":YOUR_SYSTEM_ID,"range":8.0}]`. It is combined with `filter` using `AND`; multiple systems are alternatives. `ping_type:here` sends `@here` for every `post_and_ping` action in the subscription, while `ping_type:everyone` sends `@everyone`.

### Category model

The contract feed follows the same category model as the kill feeds: global feeds watch the configured class everywhere, and a light-year-bounded feed watches the configured class within range of your staging. It is an optional PostgreSQL-backed feed and does not migrate or replace the JSON-backed killmail configuration.

### What contract feeds monitor

These feeds do not show every contract listing. They monitor public `item_exchange` contracts whose offered or requested ship items match the configured ship groups, lifecycle action, and, where configured, location and light-year range. The source is the public [EVE Swagger Interface (ESI)](https://developers.eveonline.com/docs/services/esi/overview/).

They do not monitor private personal, corporation, or alliance contracts, or contract types other than public item exchanges. The collector scans the 114 region IDs returned by ESI and does not exclude wormhole regions. Public ESI does not provide a counterparty for these records, so the feed does not infer or display one.

The lifecycle actions are:

| Action | Meaning |
| --- | --- |
| `listed` | A matching public contract was observed. |
| `sale_confirmed` | Acceptance of an offered-item contract was confirmed with public ESI evidence. |
| `purchase_confirmed` | Acceptance of a requested-item contract was confirmed with public ESI evidence. |
| `expired` | The contract expired without confirmed acceptance evidence. |
| `closed_outcome_unknown` | The contract is no longer public, but the collector could not confirm why it closed. |

`sale_confirmed` and `purchase_confirmed` are not sent just because a listing disappears. The collector requires acceptance evidence. A `closed_outcome_unknown` post only says that the listing stopped being public.

### Example configurations

The following are the three active feeds. The JSON is the current deployed configuration, formatted for direct slash-command use.

#### Global supercaps

Posts all lifecycle actions for public titan and supercarrier contracts in every ESI region the collector scans. It does not ping.

```text
/contract_subscribe id:supercap-global description:Global public titan and supercarrier contracts filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"ids":[30,659],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[30,659],"direction":"requested"}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post","expired":"post","closed_outcome_unknown":"post"}
```

#### Global capitals

Posts all lifecycle actions for public contracts containing the configured capital ship groups in every ESI region the collector scans. It does not ping.

```text
/contract_subscribe id:capital-global description:Global public capital contracts filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"ids":[4594,485,1538,547,883,902,513],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[4594,485,1538,547,883,902,513],"direction":"requested"}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post","expired":"post","closed_outcome_unknown":"post"}
```

#### Supercaps within 8 LY of staging

Posts the same lifecycle actions for public titan and supercarrier contracts within 8 LY of one of two staging systems. Only confirmed sale and purchase actions ping `@here`.

```text
/contract_subscribe id:supercap-staging-8ly description:Public supercapital contracts within 8 ly of staging filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"ids":[30,659],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[30,659],"direction":"requested"}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post_and_ping","purchase_confirmed":"post_and_ping","expired":"post","closed_outcome_unknown":"post"} ly_ranges_json:[{"system_id":YOUR_SYSTEM_ID,"range":8.0},{"system_id":YOUR_SECOND_SYSTEM_ID,"range":8.0}] ping_type:here
```

Use `/contract_unsubscribe id:<id>` in that feed's channel to stop one of these feeds. See [the operator runbook](docs/contract-intelligence.md) for the complete filter grammar and operations details.

