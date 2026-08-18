# Global Public Contract Intelligence

## Scope and evidence

This feed observes unauthenticated public EVE item-exchange contracts in every public region. It does not use character, corporation, or alliance authorization. The issuer is public; an acceptor or counterparty is not, so neither is displayed or stored.

The collector first records a complete regional baseline silently. A complete later observation can create a Listed event. When a contract is no longer public, it enters Awaiting Resolution and is not reported as a sale. A pre-expiry `204` from the public item endpoint confirms acceptance. `404` and other unavailable evidence do not confirm acceptance.

| Event action | Public evidence |
| --- | --- |
| `listed` | Newly observed after a regional baseline |
| `sale_confirmed` | Issuer offered a matched ship, requested positive ISK, no items were requested, and acceptance was confirmed |
| `purchase_confirmed` | Issuer requested a matched ship, offered positive ISK, no items were offered, and acceptance was confirmed |
| `expired` | Contract ceased to be public at its known expiry boundary without earlier acceptance evidence |
| `closed_outcome_unknown` | Contract ceased to be public and no supported terminal outcome could be established |

`awaiting_resolution` is internal only and has no alert action. No alert is provisional, edited, or retracted. Public ESI cache headers bound latency; the application does not claim a real-time post-disappearance contract-status lookup.

## Configuration and startup

Docker Compose starts PostgreSQL 16. Its safe default uses the default local-only credentials. If you set a non-default `CONTRACT_DATABASE_PASSWORD` before first startup, also set an explicit `CONTRACT_DATABASE_URL` using the Compose host `postgres:5432` and a percent-encoded password. Compose interpolates values before the container starts; it cannot safely derive a PostgreSQL URI from an arbitrary raw password. Changing either value after the database volume exists does not change the database role password.

```sh
cp docs/env.sample .env
# Set DISCORD_BOT_TOKEN and DISCORD_CLIENT_ID.
# If changing CONTRACT_DATABASE_PASSWORD, set a matching percent-encoded CONTRACT_DATABASE_URL.
docker compose up --build -d
docker compose ps
```

For a non-Compose process, set `CONTRACT_DATABASE_URL` and optionally `CONTRACT_COLLECTION_INTERVAL_SECS` (default: 300 seconds); use `127.0.0.1:5433` for the local exposed Compose port. `CONTRACT_REGIONAL_CONCURRENCY` defaults to `2` and accepts only `1` through `4`; set it to `1` to roll back collection scheduling to sequential regional attempts without changing schema or retained observations. An absent value uses the default; malformed, out-of-range, or non-Unicode values disable only contract collection at startup and leave the killmail feed running. PostgreSQL URI userinfo reserves characters including `:`, `/`, `?`, `#`, `[`, `]`, and `@`; percent-encode them (and a literal `%`) in the URL, while `CONTRACT_DATABASE_PASSWORD` remains the raw password. Each successful connection runs the SQLx migrations. If `CONTRACT_DATABASE_URL` is absent, contract collection and its commands are unavailable, while the killmail feed continues unchanged.

Compose also starts `health-watchdog`, a separate REST-only process: it has no Discord gateway session and cannot block the bot or collection loops. It intentionally has no PostgreSQL health-gated `depends_on`, so it can report a cold-start database outage. It reads the persisted heartbeat and health snapshot, maintains one non-pinging Health View in `HEALTH_CHANNEL_ID`, and opens one consolidated incident when the current degradation set becomes non-empty. Only the configured operator is mentioned when that incident is created; view updates, incident edits, and recovery never mention anyone. `WATCHDOG_EVALUATION_INTERVAL_SECS` defaults to 60 and must be at least 10 seconds. `WATCHDOG_CONFIG_REVISION` defaults to `1`; bump it after correcting a Discord token or permission issue to retry a permanent publication failure. Changing `HEALTH_CHANNEL_ID` creates the persistent messages in the new channel without editing the old channel. PostgreSQL failures are marked degraded on the first consecutive failure and critical on the third; the interval bounds reconnect attempts. During an outage a warm watchdog cache can keep editing its known message IDs. A cold watchdog can create nonce-stable messages, but cannot persist their IDs until PostgreSQL returns; it then adopts those cached IDs rather than creating replacements. `Http::new` keeps Serenity's HTTP rate limiter enabled: it consumes a valid Discord `429 Retry-After` response before the publisher returns; a returned request failure (including an unparseable rate-limit response) uses the watchdog's transient retry deadline. Discord permanent errors are stored as visible health evidence and are not retried until configuration or persisted state changes.

Keep the PostgreSQL volume when rolling back the bot image. To roll back only the scheduling change, set `CONTRACT_REGIONAL_CONCURRENCY=1` and restart `discordbot`; no data rollback is needed. A prior bot release ignores the contract tables; the JSON-backed killmail subscriptions, caches, standings, and feed checkpoints are separate and unchanged. Re-enable the current image and database URL to resume collection; migrations are re-run safely against an already-current database. Do not remove the PostgreSQL volume as a rollback step.

## Subscriptions

Create or replace a subscription in the target guild channel with `/contract_subscribe`; remove it with `/contract_unsubscribe`. The command accepts a recursive filter JSON document and an event-action JSON document.

```text
/contract_subscribe id:super-sales description:public super sales filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed"]}},{"condition":{"item_types":{"direction":"offered","ids":[12345]}}}]}} event_actions:{"listed":"post","sale_confirmed":"post_and_ping"} ping_type:here
```

For a proximity feed, pass the same compact `ly_ranges_json` shape as `/subscribe`. The supplied ranges are combined with the required `filter` root using `AND`; multiple centers are a union. This exact example matches contracts within 8.0 light-years of Turnur or Kurniainen:

```text
/contract_subscribe id:supercaps-near-turnur description:Supercapital contracts near Turnur or Kurniainen filter:{"root":{"or":[{"condition":{"ship_groups":{"direction":"offered","ids":[30,659]}}},{"condition":{"ship_groups":{"direction":"requested","ids":[30,659]}}}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post"} ly_ranges_json:[{"system_id":30002086,"range":8.0},{"system_id":30003089,"range":8.0}]
```

`ly_ranges_json` must be a non-empty JSON array of positive system IDs and finite positive ranges. Contract proximity uses the ESI solar-system Cartesian position in metres, includes a distance exactly on the boundary, and defers an alert if an observation-time event position or a center position is unavailable. Filter nodes are `condition`, `and`, `or`, and `not`. Conditions support event kinds, offered/requested item presence, item type, ship group, ISK bounds, region, solar system, light-year ranges, security range, location ID, issuer character/corporation, observed issuer alliance, issuance type, and title fragment. IDs must be positive. The maximum filter depth is 16 and the maximum node count is 128.

Each action is `ignore`, `post`, or `post_and_ping`. Omitted actions, including `listed`, default to `ignore`, so `{"sale_confirmed":"post"}` is valid. `ping_type:here` uses `@here`; `ping_type:everyone` changes all pinging actions in that subscription to `@everyone`. A contract ping shares the existing per-channel five-minute limiter with killmail pings. Delivery still posts if a requested ping is rate-limited.

## Recovery, storage, and operation

The collector records public facts, manifests, presence intervals, lifecycle evidence, subscriptions, prepared deliveries, and collection failures in PostgreSQL. Prepared deliveries are replayed after restart and carry an idempotency nonce. Discord nonce enforcement is applied only during the implemented five-minute recent-message window. After an ambiguous failure, a retry after that window disables nonce enforcement; the resulting rare duplicate is intentionally accepted over silently losing a contract alert. A prolonged collection gap creates a silent recovery baseline to prevent notification floods; fresh pre-expiry acceptance evidence can still be confirmed.

Each regional attempt has its own committed observation batch. A cycle drains prepared Discord deliveries before starting ESI discovery, then runs at most the configured number of regions. A completed region can prepare and deliver its conclusive events while another region is still in flight; the final cycle report remains in public region-discovery order. When ESI records a persisted limiter or `Retry-After` boundary, no further regional request starts; attempts already in flight retain their own complete or inconclusive batch from actual evidence. A near-exhausted shared rate bucket is persisted and paces every subsequent ESI request in-cycle; it waits between admissions rather than repeatedly restarting an early regional sweep.

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

This release has no authenticated-contract feed, counterparty tracking, provisional alert, Discord edit/retraction, JSON-to-PostgreSQL migration, or horizontal-scaling machinery.

## External references

- [ESI best practices](https://developers.eveonline.com/docs/services/esi/best-practices/)
- [ESI map data and solar-system endpoints](https://developers.eveonline.com/docs/guides/map-data/)
- [ESI rate limiting](https://developers.eveonline.com/docs/services/esi/rate-limiting/)
- [Discord interaction responses](https://docs.discord.com/developers/interactions/receiving-and-responding)
- [Discord message creation and nonce enforcement](https://docs.discord.com/developers/resources/message)
- [Discord rate limits](https://docs.discord.com/developers/topics/rate-limits)
- [Serenity HTTP rate limiter](https://docs.rs/serenity/0.11.7/serenity/http/struct.Ratelimiter.html)
- [Docker Compose variable interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)
- [PostgreSQL connection strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
