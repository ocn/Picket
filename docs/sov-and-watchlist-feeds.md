# Sovereignty Timer and Watchlist Feeds

This is the operator counterpart of `docs/contract-intelligence.md` for the two intel feeds added on top of the contract database: the sovereignty timer feed and the watchlist feed. It covers enabling both on the existing Compose deployment, confirming they are running, and running them day to day.

## Scope

The sovereignty timer feed watches public sovereignty campaigns and Sovereignty Hub vulnerability windows. It posts when a campaign appears, when a configured T-minus mark is reached, when a campaign's system becomes reachable from the home system, and (per subscription) when a hub's vulnerability window shifts into the subscription's timezone window. It computes reachability over stargates and, when configured, the current Wanderer wormhole chain. See the README section [Sovereignty Timer Feed](../README.md#sovereignty-timer-feed) for the filter grammar, alert stages, and the reachability/chain model; this document does not repeat them.

The watchlist feed keeps a per-server list of alliances and corporations and posts when a corporation joins or leaves a watched alliance, when a watched entity's member count swings sharply, when a watched corporation changes alliance, and when a war is declared by or against a watched entity (and as that war gains allies, is retracted, or ends). See the README section [Watchlist feed](../README.md#watchlist-feed) for the event kinds, the hourly collectors, and the war-scan model.

Both feeds share the contract feed's PostgreSQL database and only start when `CONTRACT_DATABASE_URL` is configured. Neither touches the JSON-backed killmail configuration.

## Configuration and startup

Both feeds require the same database gate as the contract feed: `CONTRACT_DATABASE_URL`. If it is absent, the sov and watchlist feeds do not start and the killmail feed continues unchanged. Under Compose the variable defaults to `postgres://killbot_contracts:killbot_contracts@postgres:5432/killbot_contracts`; the guidance for changing `CONTRACT_DATABASE_PASSWORD` and supplying a matching percent-encoded `CONTRACT_DATABASE_URL` is in `docs/contract-intelligence.md`.

Each successful database connection runs the SQLx migrations. On a database that already holds the contract schema, the first start with this build applies the feed migrations automatically. The seven feed migrations, applied in order, are:

```text
migrations/20260826000000_add_sov_campaign_feed.sql
migrations/20260827000000_add_sov_chain_snapshots.sql
migrations/20260827000001_add_sov_reachability_state.sql
migrations/20260827000002_add_sov_structures_and_map.sql
migrations/20260827000003_add_watchlist_feed.sql
migrations/20260828000000_add_watchlist_member_snapshots.sql
migrations/20260828000001_add_watchlist_wars.sql
```

Take a full backup before that first start, because migrations run on connect and are not reversed automatically:

```sh
docker compose exec -T postgres pg_dump -U killbot_contracts -d killbot_contracts > killbot_contracts-before-feeds.sql
```

### Sovereignty reachability

`SOV_HOME_SYSTEM_ID` sets the reachability origin. It defaults to Turnur (`30002086`) and an empty or whitespace value is treated as unset (the default). A non-integer value logs a warning and falls back to the default rather than failing start-up.

Reachability is computed over `config/stargates.json`, a static stargate adjacency map committed to the repository. Regenerate it only after a rare SDE release that changes the map, with `cargo run --bin build_stargate_graph`; see the README section [Refreshing the stargate graph](../README.md#refreshing-the-stargate-graph). If the file is missing or fails to parse, the feed still starts: `Reachable` filter leaves never match and `/sov_timers` reports the graph as unavailable.

### Wanderer chain reachability

Three variables enable chain reachability. All three must be set; any one missing keeps reachability stargate-only, exactly as if the feature did not exist.

| Variable | Meaning |
| --- | --- |
| `WANDERER_BASE_URL` | Wanderer map host, e.g. `https://wanderer.deepwaterhooligans.com`. |
| `WANDERER_MAP` | Map UUID or slug. |
| `WANDERER_MAP_API_KEY` | Per-map API key. The key is write-capable on Wanderer's side; the bot only ever issues GET requests and its client type has no write methods. |

When all three are set, the bot polls the map every 120 seconds (two GETs per cycle: `GET /api/maps/{map}/systems` and `GET /api/maps/{map}/connections`) and merges the current chain into the reachability graph. The API key is never logged.

### Compose vs non-Compose

Under Compose, all four variables are forwarded to the `discordbot` service with the `${VAR:-}` pattern, so leaving one unset in `.env` passes an empty value that the bot treats as absent. The `health-watchdog` service does not need them. Uncomment and set them in `.env`; the entries already exist in `docs/env.sample`:

```sh
SOV_HOME_SYSTEM_ID=30002086
WANDERER_BASE_URL=https://wanderer.deepwaterhooligans.com
WANDERER_MAP=<map-uuid-or-slug>
WANDERER_MAP_API_KEY=<per-map-key>
```

For a process outside Compose, export the same variables in the environment, and use `127.0.0.1:5433` instead of `postgres:5432` in `CONTRACT_DATABASE_URL`.

## Verifying start-up

Each runtime logs one line once per process start, after its first successful database connection and migration check. A later success after a database outage logs the `reconnected` wording instead. Grep the bot logs:

```sh
docker compose logs discordbot | grep -E "sov campaign feed started|sov chain reachability|watchlist feed started|Loaded stargate graph"
```

The lines to expect (interpolated values shown in angle brackets):

```text
Loaded stargate graph: sde_version=<version>, systems=<n>, edges=<m>, home_system_id=<id>
sov campaign feed started (home system <id>, stargate graph <n> systems / <m> edges)
watchlist feed started
```

When `config/stargates.json` could not be loaded, the campaign line reads `sov campaign feed started (home system <id>, stargate graph unavailable)`.

When all three `WANDERER_*` variables are set, the chain runtime logs:

```text
sov chain reachability enabled (map <slug>, poll every 120 s)
```

When they are not all set, it logs instead:

```text
Wanderer chain reachability disabled: WANDERER_BASE_URL, WANDERER_MAP, and WANDERER_MAP_API_KEY must all be set. Reachability stays stargate-only.
```

If `CONTRACT_DATABASE_URL` is absent, the sov runtime logs `Sov campaign feed disabled: CONTRACT_DATABASE_URL is not configured` and neither feed starts.

### Health keys

`/health` answers only the configured operator id (`146451271497416704`); every other member gets `Not authorized.` It prints one line per check as `- <key>: <status> — <evidence>`. The five feed checks are:

| Key | Neutral state (Healthy) | Degraded | Critical |
| --- | --- | --- | --- |
| `sov_esi_progress` | `no sov campaign ESI progress reported yet (feed not started or not enabled)` | sov campaign ESI progress older than `HEALTH_ESI_PROGRESS_DEGRADED_SECS` | older than `HEALTH_ESI_PROGRESS_CRITICAL_SECS` |
| `watchlist_esi_progress` | `no watchlist ESI progress reported yet (feed not started or no alliance watched)` | watchlist ESI progress older than `HEALTH_WATCHLIST_PROGRESS_DEGRADED_SECS` (default 7200 s) | older than `HEALTH_WATCHLIST_PROGRESS_CRITICAL_SECS` (default 10800 s) |
| `sov_chain_progress` | `no wanderer chain snapshot fetched yet (feature not configured or not started)` | snapshot older than the fixed 10-minute chain staleness | no Critical tier |
| `sov_permanent_delivery_failure` | `no unresolved permanent Discord delivery failures` | none | one or more unresolved permanent delivery failures |
| `watchlist_permanent_delivery_failure` | `no unresolved permanent Discord delivery failures` | none | one or more unresolved permanent delivery failures |

Notes:

- The three progress checks are neutral (Healthy) until the feed first reports, so a fresh or disabled feed never degrades the snapshot.
- `HEALTH_ESI_PROGRESS_DEGRADED_SECS` and `HEALTH_ESI_PROGRESS_CRITICAL_SECS` default to 900 and 1800 seconds (15 and 30 minutes). They are shared with the contract feed's ESI-progress check.
- `HEALTH_WATCHLIST_PROGRESS_DEGRADED_SECS` and `HEALTH_WATCHLIST_PROGRESS_CRITICAL_SECS` default to 7200 and 10800 seconds (2 and 3 hours) because the watchlist collector runs on an hourly (`WATCHLIST_COLLECTION_INTERVAL` = 3600 s) cadence and a fresh-cache cycle skips the request without touching the `watchlist/%` progress rows, so the thresholds are sized at 2x and 3x the cadence rather than reusing the contract feed's 5-minute budget.
- `sov_chain_progress` has no configurable threshold and no Critical tier: a Wanderer outage degrades the board `/sov_timers` shows, it is not escalated like a stalled pipeline.
- The two permanent-delivery-failure checks are binary: any unresolved permanent Discord delivery failure is Critical, otherwise Healthy.

## Permissions

Serenity registers the global command set with a full replace on `ready`. After the deployment hardening in the prior ticket, every subscription-management command registers `default_member_permissions(MANAGE_GUILD)` and `dm_permission(false)`.

| Command | Permission |
| --- | --- |
| `/sov_subscribe`, `/sov_unsubscribe` | Manage Server (`MANAGE_GUILD`), no DMs |
| `/watch` (whole group, including `list`), `/watch_subscribe`, `/watch_unsubscribe` | Manage Server (`MANAGE_GUILD`), no DMs |
| `/sov_timers` | open to every member |

Discord has no per-subcommand default permission, so `/watch list` requires `MANAGE_GUILD` because the `/watch` group does. `/health` is gated in code to the operator id only. Server admins can loosen any of these per role in Discord under Server Settings → Integrations → the bot → Command permissions.

## Operation

All queries below run against the Compose database. Watch the bot logs for the start-up lines above, rate-limit pauses, and per-prune summary lines.

Delivery status counts for each feed:

```sh
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT status, count(*) FROM sov_alert_deliveries GROUP BY status"
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT status, count(*) FROM watchlist_deliveries GROUP BY status"
```

Unresolved permanent delivery failures (these are what drive the two permanent-failure health keys):

```sh
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT count(*) FROM sov_alert_deliveries WHERE status = 'failed' AND failure_kind = 'permanent'"
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT count(*) FROM watchlist_deliveries WHERE status = 'failed' AND failure_kind = 'permanent'"
```

Shared ESI limiter headroom (one singleton row):

```sh
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT rate_limit_group, rate_limit_remaining, rate_limit_used, error_limit_remain, pause_until, next_request_at, pacing_active FROM esi_collection_limiter_state WHERE limiter_scope = TRUE"
```

The war-scan state row (one singleton row) and whether the war baseline has finished:

```sh
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT high_water_mark, baseline_established, baseline_head_max, baseline_cursor, baseline_floor, updated_at FROM watchlist_war_scan_state"
```

Watched entities per guild:

```sh
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT guild_id, kind, count(*) FROM watchlist_entities GROUP BY guild_id, kind ORDER BY guild_id, kind"
```

Member-count snapshot coverage (how much history exists for `member_delta`):

```sh
docker compose exec postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT entity_kind, entity_id, count(*), min(observed_at), max(observed_at) FROM watchlist_member_snapshots GROUP BY entity_kind, entity_id ORDER BY entity_kind, entity_id"
```

### Cadence and cost

The feeds add these collection cadences on top of the killmail and contract loops:

| Loop | Cadence | ESI cost |
| --- | --- | --- |
| Sov campaign collection | every 60 s | `GET /sovereignty/campaigns/`, conditional |
| Sov T-minus / tz-window stage pass | every 30 s | none (no ESI request) |
| Sov Hub structures | every 300 s | `GET /sovereignty/structures/`, matches the route's 5-minute cache |
| Sov ownership map | every 3600 s | `GET /sovereignty/map/`, matches the route's hourly cache |
| Wanderer chain poll | every 120 s | two GETs to the Wanderer map host (not ESI) |
| Watchlist collectors | every 3600 s | alliance corp lists, corporation info, and wars, all conditional |

All ESI requests go through the shared limiter and are conditional, so most re-polls at the route cache age return `304`. The corporation-info pass is bounded to at most 200 fetches per cycle and the open-war re-fetch to at most 200 per cycle, least-recently-fetched first. The wars routes report `x-ratelimit-group: killmail` — the same per-route bucket as killmail detail fetches — so war scanning shares that bucket's remaining/reset with the killmail feed, recorded in the singleton limiter row above.

## Retention

| Data | Retention | Where |
| --- | --- | --- |
| Watchlist member snapshots | 30 days, pruned each cycle | `watchlist_member_snapshots` |
| Finished wars below the head mark | 30 days | `watchlist_wars` |
| Sov campaigns unlisted by ESI, and their `sent` deliveries | 90 days, once per sov collection cycle; orphaned `sov_reachability_state` rows are explicitly deleted in the same transaction; `sov_tz_window_state` is not affected by campaign pruning (it cascades only when its `sov_structures` row is removed) | `sov_campaigns`, `sov_alert_deliveries` |
| Wanderer chain snapshots | older than 24 hours pruned, the latest snapshot always kept | `sov_chain_snapshots` |

`prepared` deliveries and deliveries with an unresolved permanent failure are never pruned, and a delivery whose campaign is still listed is never pruned. The contract tables have no runtime retention job (see `docs/contract-intelligence.md`).

## First-week expectations

- Alliance-membership and corporation-info baselines are established in one cycle and are silent: adding an alliance does not announce its existing corporations, and the first corporation-info snapshot per entity fires nothing.
- `member_delta` measures against the snapshot nearest seven days ago, so it needs roughly 6.5 days of history before it can fire. A freshly added entity stays silent until it has about a week of snapshots.
- The war baseline inspects every id on the newest wars page when it began but fetches at most 200 details per cycle, so it spans about ten hourly cycles before the first live `war_declared` can post.
- An entity added after a war was already stored gets no retroactive `war_declared`; that war becomes a silent baseline for the entity, but its ally joins, retraction, and finish are tracked from then on.
- Removing and re-adding an entity resets its baselines: the next successful cycle re-establishes a silent membership and corporation-info baseline for it.

## Main-server walkthrough

This assumes the contract database already exists and the bot is running the current build. Every command is copy-paste ready; `role:` values are Discord role selectors, shown here as placeholders.

1. Back up the database, then enable the feeds. In `.env`, set `SOV_HOME_SYSTEM_ID`, and the three `WANDERER_*` variables if the Wanderer inputs are available (leave them unset to run stargate-only). Restart the bot:

   ```sh
   docker compose exec -T postgres pg_dump -U killbot_contracts -d killbot_contracts > killbot_contracts-before-feeds.sql
   docker compose up --build -d discordbot
   ```

2. Verify start-up:

   ```sh
   docker compose config --quiet
   docker compose logs discordbot | grep -E "sov campaign feed started|sov chain reachability|watchlist feed started|Loaded stargate graph"
   ```

3. Seed the watchlist with the five seed alliances, by full name. The `ticker` option resolves through the bot's ticker cache first, then an ESI name lookup (`POST /universe/ids/`); ESI resolves names, not tickers, so a bare ticker only works when it is already cached — supply the full name otherwise:

   ```text
   /watch add kind:alliance ticker:Snuffed Out
   /watch add kind:alliance ticker:Shadow Cartel
   /watch add kind:alliance ticker:Minmatar Fleet Alliance
   /watch add kind:alliance ticker:Brotherhood of Spacers
   /watch add kind:alliance ticker:Legion of xXDEATHXx
   ```

   The watchlist is server-wide (stored per guild, not per channel), so `/watch add` and `/watch list` can be run in any channel of the server; only `/watch_subscribe` and `/sov_subscribe` are channel-scoped, subscribing the channel they are run in. The reply confirms each resolved name and id ephemerally. Confirm the list:

   ```text
   /watch list
   ```

4. Create the three production channels' subscriptions.

   In `#sov-timers` — defense campaigns starting within 12 hours in a system reachable within 11 jumps, with the default T-minus marks, pinging the roamers role:

   ```text
   /sov_subscribe name:reachable-timers filter:{"root":{"and":[{"condition":{"vulnerable_within":{"hours":12}}},{"condition":{"reachable":{"max_jumps":11,"allow_frigate_holes":false}}}]}} tminus_marks:120,30 role:@Roamers
   ```

   In `#hostile-wars` — the four war event kinds, pinging a role:

   ```text
   /watch_subscribe name:hostile-wars event_kinds:war_declared,war_ally_joined,war_retracted,war_finished role:@HostileWars
   ```

   In `#hostile-intel` — the four membership event kinds, no role:

   ```text
   /watch_subscribe name:hostile-intel event_kinds:corp_joined,corp_left,member_delta,corp_changed_alliance
   ```

   and, in the same channel, a sov subscription defended by any alliance on this server's watchlist, with timezone-shift alerts enabled and no role:

   ```text
   /sov_subscribe name:hostile-tz filter:{"root":{"condition":{"defender":{"watchlist":true}}}} tz_shift_enabled:true
   ```

5. Confirm the live board in each channel:

   ```text
   /sov_timers
   ```

## Wanderer maintainer request

Send the map maintainer a short request for the three inputs and a dedicated key:

> Could you set up read access for the killbot to the alliance Wanderer map? I need the map slug or UUID, and a dedicated per-map API key for the bot so it can be rotated independently of anyone else's. Please confirm that `https://wanderer.deepwaterhooligans.com/api/maps/{map}/systems` and `.../connections` are the right endpoints. The bot polls both, GET-only, every two minutes; it never writes to the map.

## Deliberate exclusions

- No per-entity tags or groups on the watchlist.
- No Dotlan scraping; the feed only uses public ESI and, when configured, the Wanderer map API.
- Wars older than the initial window that the head-scan established are never inspected: ids below the baseline floor were never on the starting page and are out of scope by design.
- No retroactive `war_declared`: a war stored before its party was watched becomes a silent baseline for that entity.

## Pilot and go-live

The first enablement of these feeds on the main-server deployment runs as a
seven-day, non-pinging pilot in `#sandbox`, gated on explicit evidence, before
the three production channels are created. The interactive checklist
`scripts/esi-intel-feeds-pilot.sh` (generated with the `wizard` skill) prompts
each human step (backup, deploy, `/watch add` ×5, the sandbox `/watch_subscribe`
and two `/sov_subscribe`, the day-2 and day-5 restarts, the Dotlan click-check,
and the Wanderer variables when they arrive), runs the machine checks it can
(migration count, start-up log grep, `psql` counts, the duplicate-delivery
query, a limiter-row sample), and appends every result to the tracked artifact
`docs/rollouts/esi-intel-feeds-pilot.md`. The script is read-only against
PostgreSQL — `SELECT` only, plus the one read-only `pg_dump` backup — never
sends Discord messages, never calls ESI, never edits `.env` or
`docker-compose.yaml`, and never runs `docker compose up/down/restart` itself:
it tells you to run those and then verifies.

### Running the wizard

From the repository root:

```sh
./scripts/esi-intel-feeds-pilot.sh          # menu: pick a step, see each step's recorded status
./scripts/esi-intel-feeds-pilot.sh 5        # run one step non-interactively (0-11)
./scripts/esi-intel-feeds-pilot.sh sample   # append one limiter sample row (used by the hourly cron)
```

State persists in `docs/rollouts/.esi-intel-feeds-pilot.state`, so a re-run
resumes where you left off and every gate step is re-runnable. The steps are
not a straight line — the pilot spans seven days with deliberate restarts on
days 2 and 5 — so the menu is the normal entry point. If neither the Compose
`postgres` service is up nor `CONTRACT_TEST_DATABASE_URL` is set, database
checks degrade with a clear message and a non-zero exit rather than proceeding.

### The gates (from ticket 11)

- **G1 (start-up):** both runtimes' start-up lines present; `sov_esi_progress`
  and `watchlist_esi_progress` report within two cycles; stargate graph loaded
  (system/edge counts logged); sandbox subscriptions listed.
- **G2 (day 1):** watchlist baselines silent (zero `watchlist_deliveries`);
  every `appeared` alert in `#sandbox` corresponds to a campaign in
  `/sov_timers` and in `GET /sovereignty/campaigns/`.
- **G3 (restarts, days 2 and 5):** after each restart, zero duplicate
  deliveries — the query over both delivery tables grouped by their dedup keys
  with `count(*) > 1`, expected empty — and prepared rows drained.
- **G4 (day 7):** at least one real watchlist event delivered exactly once;
  `member_delta` history present for every seeded alliance (snapshots ≥ 6.5 days
  old) even if no delta fired; hourly limiter samples show no pause attributable
  to the new collectors; `/sov_timers` parity re-checked; Dotlan numeric
  corporation URLs click-checked from a real embed; the deferred demos from
  tickets 08–10 recorded.
- **G5 (chain):** after the `WANDERER_*` variables are set,
  `sov chain reachability enabled` logged, `sov_chain_progress` reports, and one
  chain-driven `reachable` transition observed and delivered once. May complete
  after day 7 if the key arrives late; it does not block G1–G4 sign-off.

A failed gate pauses go-live until the cause is fixed and the gate re-run; it
does not abort the pilot. The wizard records each step as `PASS`, `FAIL`, or
`PAUSED`, and a `FAIL` prints that go-live is paused until the gate passes.

### Restart days and limiter sampling

Restart the bot deliberately on day 2 and day 5 (`docker compose restart
discordbot`) and run step 7 (G3) after each; the wizard writes a separate dated
entry per restart. For the limiter, step 8 takes one sample immediately and
prints the exact hourly `crontab` line to add for the pilot's duration:

```
0 * * * * cd <repo> && ./scripts/esi-intel-feeds-pilot.sh sample >/dev/null 2>&1
```

Remove that line with `crontab -e` at the end of the pilot (the wizard reminds
you again at go-live). Each sample appends a `- [limiter]` line to the artifact;
step 9 (G4) summarises them (minimum remaining, any pause).

### Go-live

Step 11 prints the three production invocations from the walkthrough above with
the roamers-role placeholder, has you confirm one non-pinging cycle per channel
before adding `role`, and records the three channels' final subscription rows as
JSON and the handoff note into the artifact. Write the member announcement only
after a successful go-live, and re-triage tickets 12 and 13 against the artifact's
limiter samples.

The evidence artifact lives at `docs/rollouts/esi-intel-feeds-pilot.md`.
