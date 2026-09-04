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
