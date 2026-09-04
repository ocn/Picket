# ESI intel feeds — pilot and go-live rollout record

Evidence record for the seven-day, non-pinging pilot of the sovereignty timer
and watchlist feeds, and their go-live. Shaped like
`.scratch/contract-monitoring-recovery-rollout/issues/05-deploy-once-and-complete-production-verification.md`'s
`## Comments` record. Driven by `scripts/esi-intel-feeds-pilot.sh`
(the `wizard` skill), which appends dated entries to the **Evidence log** at
the end of this file rather than editing the placeholders below.

**Append-only choice:** the headings in `## Record (placeholders)` describe
what each gate expects and stay as written; the wizard never edits them. Every
run appends a dated `### <ISO8601> — Step N · <name>` block under
`## Evidence log`, so the file is safe to append to from a cron job or a re-run
and nothing is overwritten. The limiter cron sample appends `- [limiter] …`
lines to the same log.

Ticket: `.scratch/esi-intel-feeds/issues/11-deploy-and-verify-the-pilot.md`.
Runbook: `docs/sov-and-watchlist-feeds.md` (see the "Pilot and go-live" section).

## Record (placeholders)

Filled by the wizard via dated entries in the Evidence log below.

- **Pinned commit** — `git rev-parse HEAD` at pilot start (step 0). _pending_
- **Compose config** — `docker compose config --quiet` clean (step 0). _pending_
- **Backup** — path, size, SHA-256, `pg_restore --list` entry count of the
  pre-deploy `pg_dump` (step 1). _pending_
- **Migrations** — `_sqlx_migrations` count, expected 33 (step 2). _pending_
- **Deployment** — start-up log lines present (step 2). _pending_
- **Seed** — five watchlist alliances for the pilot guild (step 3). _pending_
- **Sandbox subscriptions** — one `/watch_subscribe` (eight kinds, no role) and
  two `/sov_subscribe`, plus the day-0 `/sov_timers` capture (step 4). _pending_
- **Gate G1 (start-up)** — five feed health keys + stargate graph line (step 5). _pending_
- **Gate G2 (day 1)** — zero `watchlist_deliveries`; sov appeared/campaign
  parity (step 6). _pending_
- **Gate G3 (restarts, days 2 & 5)** — zero duplicate deliveries; prepared rows
  drained (step 7, one entry per restart). _pending_
- **Limiter sampling** — hourly `esi_collection_limiter_state` samples (step 8;
  `- [limiter]` lines). _pending_
- **Gate G4 (day 7)** — ≥1 real event delivered once; member-snapshot age;
  limiter summary; Dotlan click-check; 08–10 demos (step 9). _pending_
- **Gate G5 (chain)** — `sov chain reachability enabled`, `sov_chain_progress`,
  one reachable transition (step 10). _pending_
- **Go-live** — three production channels' final subscription rows and channel
  ids; handoff (step 11). _pending_

## Gate summary

A failed gate pauses go-live until the cause is fixed and the gate re-run; it
does not abort the pilot. Each Evidence-log entry ends with a
`**Result: PASS|FAIL|PAUSED|DONE**` line; the wizard also records the same
status in `docs/rollouts/.esi-intel-feeds-pilot.state`.

## Evidence log

_The wizard appends dated entries below this line._

### 2026-09-04T15:48Z — Step 0 · Preflight
- Pinned commit: `a91f222` (tickets 16, 17, 14 and the 11 tooling committed on top of `cab6a35`).
- `docker compose config --quiet`: exit 0. `docker compose ps`: `discordbot` Up 5 days, `killbot-contract-postgres` Up 5 days (healthy), `killbot-health-watchdog` Up 5 days; running image `ocn-killbot-rust:latest` created 2026-08-30T06:21Z (predates tickets 14/16/17).
- `.env` presence (values never read): `DISCORD_BOT_TOKEN` present, `DISCORD_CLIENT_ID` present, `CONTRACT_DATABASE_URL` present, `SOV_HOME_SYSTEM_ID` absent (Turnur default), `WANDERER_BASE_URL`/`WANDERER_MAP`/`WANDERER_MAP_API_KEY` absent (stargate-only start; map slug `turnur` known, key pending), `HEALTH_CHANNEL_ID` absent (Compose default 1538030920328810536 = `#sandbox`).
- Database: `_sqlx_migrations` = 33 already (the running image includes the wars feed); `watchlist_entities`, `watchlist_subscriptions`, `sov_subscriptions` all empty; no rows in either delivery table; `sov_campaigns` = 68; war scan baseline established (mark 762684). Health keys at 15:48:20Z: all five feed keys `healthy` (`sov_esi_progress` 31 s old, `watchlist_esi_progress` 627 s old, chain "not configured").
- Guild `888224317991706685`; sandbox channel `1538030920328810536` (https://discord.com/channels/888224317991706685/1538030920328810536). Day 0 = 2026-09-04; restarts scheduled 2026-09-06 and 2026-09-09; day-7 gate 2026-09-11.
- Operator note: seeding and sandbox subscriptions are written by the orchestrator directly as the rows the commands produce (the operator delegated; bots cannot invoke slash commands); the command path is exercised afterwards with `/watch list` and `/sov_timers`.
- **Result:** PASS

### 2026-09-04T15:49Z — Step 1 · Backup
- `docker compose exec -T postgres pg_dump -U killbot_contracts -d killbot_contracts -Fc` → `backups/esi-intel-feeds-predeploy-20260904T154842Z.dump`, 143 MB, SHA-256 `63d0a336866beae0efe7bc5068b9c5389bf567fad46090e359c41bc4176a266a`, `pg_restore --list` 267 entries.
- **Result:** PASS

### 2026-09-04T17:42Z — Step 2 · Deploy
- `docker compose build discordbot` (image `ocn-killbot-rust:latest` created 2026-09-04T16:00:12Z from `a91f222`), then `docker compose up -d discordbot`: container recreated 17:42:32Z → started 17:42:36Z (swap-only downtime, ~4 s). `killbot-health-watchdog` left on the previous image (same schema; no watchdog code changed in 14/16/17).
- Migrations: `_sqlx_migrations` = 33 (unchanged; sqlx notices "already exists, skipping").
- Start-up lines at 17:42:37Z: `Wanderer chain reachability disabled: WANDERER_BASE_URL, WANDERER_MAP, and WANDERER_MAP_API_KEY must all be set. Reachability stays stargate-only.`; `Loaded stargate graph: sde_version=eve_3480926_20260826_134500, systems=5268, edges=6989, home_system_id=30002086`; `watchlist feed started`; `sov campaign feed started (home system 30002086, stargate graph 5268 systems / 6989 edges)`. No ERROR/panic lines.
- **Result:** PASS

### 2026-09-04T17:45Z — Step 3 · Seed the watchlist
- Rows written as `/watch add` would (operator delegated; `added_by` NULL): `watchlist_entities` for guild 888224317991706685, kind `alliance`: 99004901 `B B C` Snuffed Out; 495729389 `SHDWC` Shadow Cartel; 99011978 `FL33T` Minmatar Fleet Alliance; 99007887 `B0SS` Brotherhood of Spacers; 1411711376 `X.I.X` Legion of xXDEATHXx. Verification query returned exactly those five.
- **Result:** PASS (command-path check `/watch list` requested from the operator)

### 2026-09-04T17:45Z — Step 4 · Sandbox subscriptions
- Channel 1538030920328810536 (`#sandbox`), no role: `watchlist_subscriptions` `sandbox-watchlist` with all eight event kinds, options `{}`; `sov_subscriptions` `reachable-timers` (`vulnerable_within` 12 h AND `reachable` max_jumps 11 / frigate holes false; options `explicit_filter` + `tminus_marks_minutes` [120,30]) and `hostile-tz` (`defender{watchlist:true}`; options `explicit_filter` + `tz_shift_enabled` true, default `00:00-04:00` window). Rows verified.
- **Result:** PASS (day-0 `/sov_timers` capture requested from the operator)
- [limiter] sampled_at=2026-09-04 17:44:02.351983+00 group=sovereignty remaining=580 used=1 error_remain=100 error_reset=60 pause_until=2026-08-21 18:56:00.758394+00 pacing=f

### 2026-09-04T17:46Z — Step 5 · Gate G1 (start-up)
- Both runtimes' start-up lines present (see Step 2); stargate graph loaded (5268 systems / 6989 edges, home 30002086); sandbox subscriptions listed (Step 4).
- Health at the first post-deploy cycle (17:42:37Z) and the following one:
  - `esi_progress | healthy | ESI progress is 7s old`
  - `heartbeat | healthy | bot heartbeat is 60s old`
  - `sov_chain_progress | healthy | no wanderer chain snapshot fetched yet (feature not configured or not started)`
  - `sov_esi_progress | healthy | sov campaign ESI progress is 0s old`
  - `sov_permanent_delivery_failure | healthy | no unresolved permanent Discord delivery failures`
  - `watchlist_esi_progress | healthy | watchlist ESI progress is 344s old`
  - `watchlist_permanent_delivery_failure | healthy | no unresolved permanent Discord delivery failures`
- Pre-existing contract-feed conditions, not attributable to the new feeds: `esi_progress` read 1673 s old at the start-up snapshot (measured before the first post-swap contract cycle; the contract cache was fresh again at 17:43:59Z) and `structure_resolver` degraded ("access denied", resolver credentials — outside this pilot).
- Limiter at 17:43:43Z: group `sovereignty` 580/600 remaining, error allowance 99, no active pause (stale 2026-08-21 `pause_until`), pacing off. First `[limiter]` sample appended by the wizard at 17:44:02Z.
- **Result:** PASS
- **G1 addendum (17:50Z):** the overall snapshot read `critical` at 17:43:38Z because of the contract feed's `deferred_backlog` (27 due manifest/resolution items, oldest ~10.5 days), which oscillated healthy↔critical at 17:13, 17:15 and 17:43 — pre-existing, outside this pilot. The transition log also showed `watchlist_esi_progress` critical 17:08:13Z ("1819s old") → healthy 17:38:25Z on the previous image: the watchlist collector's hourly cadence exceeds the inherited 15/30-minute progress thresholds, so the check flips every hour without a fault. Filed as ticket 18 (fix in progress; a quick redeploy follows). G1 remains PASS on its own criteria.

### 2026-09-04T18:17Z — Redeploy for ticket 18 (watchlist health thresholds)
- Ticket 18 (`11ef81b`): `watchlist_esi_progress` thresholds now 7200/10800 s via `HEALTH_WATCHLIST_PROGRESS_*`. Image rebuilt; `discordbot` and `killbot-health-watchdog` recreated 18:17:11–17Z (~6 s). Both containers carry the two variables; start-up lines at 18:17:19–20Z (`watchlist feed started`, `sov campaign feed started (home system 30002086, stargate graph 5268 systems / 6989 edges)`, chain disabled/stargate-only); killfeed resumed from R2Z2 checkpoint 99370971; watchdog started against channel 1538030920328810536.
- First post-swap health cycle 18:17:20Z: all five feed keys healthy (`sov_esi_progress` 0 s, `watchlist_esi_progress` 0 s); overall snapshot `degraded` solely from the pre-existing `structure_resolver` condition (`deferred_backlog` back to healthy).
- Regression run of the contract file with the build in flight (846 s, load 31–47): 301/303; the two failures are pacing/backoff mock-window tests outside ticket 14's set; one passed serially, the other passed twice once the build finished — logged on ticket 14.
- Watchlist state after the first collection cycle with the seed in place: 1411711376 Legion of xXDEATHXx baseline 2026-09-04 18:17:19.885989+00 corps 34; 495729389 Shadow Cartel baseline 2026-09-04 18:17:19.885989+00 corps 28; 99004901 Snuffed Out baseline 2026-09-04 18:17:19.885989+00 corps 17; 99007887 Brotherhood of Spacers baseline 2026-09-04 18:17:19.885989+00 corps 36; 99011978 Minmatar Fleet Alliance baseline 2026-09-04 18:17:19.885989+00 corps 9; member snapshots: alliance=5, corporation=124; deliveries: watchlist 0, sov 2.
- **Result:** PASS
