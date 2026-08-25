# 04 — Deploy safely and complete Production Verification

**What to build:** Deploy the exact reviewed fix-forward once to the existing production stack, demonstrate safe increased Resolution Throughput across repeated ESI error windows without starving other work, and complete exactly one controlled non-pinging Production Verification message. Preserve deployed evidence and halt on regression; do not touch Historical Enrichment Repair.

**Blocked by:** 03 — Verify the integrated fix-forward candidate

**Status:** claimed

**Claimed:** 2026-08-25 — Ticket04 rollout agent

- [ ] Immediately before mutation, re-query services, processes, restarts, OOM state, PostgreSQL health, locks, migrations, subscriptions, limiter evidence, resolver readiness, regional progress, backlog, delivery debt, and new errors.
- [ ] Create and validate a fresh production backup, restore listing, checksum, migration ledger, and quiet Compose configuration.
- [ ] Deploy the exact reviewed candidate through one approved Compose operation without canary, staging, traffic split, automatic rollback, automatic restart, or deployment retry.
- [ ] PostgreSQL remains on its existing durable volume and is not recreated or restored.
- [ ] Bot, watchdog, PostgreSQL, and Structure Resolver become and remain ready with zero unexpected restarts or OOM termination.
- [ ] Observe ten complete one-minute legacy error windows after deployment.
- [ ] Recovery never admits a probe when authoritative remaining error allowance is 40 or lower.
- [ ] Recovery yields correctly when unrelated ESI work consumes the shared budget and resumes after reset.
- [ ] No unexpected `420`, `429`, authentication, Discord, panic, database, duplicate-post, sustained limiter, or unresolved collection failure appears.
- [ ] At least one complete Regional Observation sweep finishes during the gate, with corrected ESI progress current and no evidence of discovery starvation.
- [ ] Resolution Throughput exceeds the prior 32-per-minute bound when upstream capacity permits, and actual backlog movement and projected clearance are recorded without waiting for backlog zero.
- [ ] Prepared deliveries and unresolved permanent delivery failures remain zero before Production Verification.
- [ ] If a regression appears, preserve the deployed state and evidence, send no verification message, perform no rollback or remediation, and wait for explicit operator direction.
- [ ] After every gate passes, send exactly one controlled non-pinging Production Verification message to channel `1115807643748012072`.
- [ ] Verify the message has canonical title, Issuer, strongest supported human-readable location, current presentation revision, no raw identifier labels, no empty placeholders, empty allowed mentions, and no replacement post.
- [ ] Record candidate revision, backup, migrations, service state, ten-window limiter evidence, regional sweep, Resolution Throughput, backlog start/end, delivery state, and Production Verification result.
- [ ] Historical Enrichment Repair remains untouched throughout this ticket and still requires separate fresh exact-count approval.
- [ ] Leave the ticket claimed until final Sol-high Standards and Spec review of the production evidence passes; then resolve it without modifying unrelated tracker work.

## Halt evidence

### 2026-08-25 — pre-mutation Rollout Health Gate failed; deployment not started

- **Source boundary:** `HEAD` was `a2f6cd822d45c344db12520e5d0f53367faa6e03`; the tested source was `1148b37fd1980ebf4f008852b3350333e628ba6b`, with only Ticket03 tracker evidence committed between them. A tracked-files-only `a2f6cd8` archive was materialized at `/tmp/hazardous-killbot-ticket04-a2f6cd8`, mode `0700`; its tree matched `git archive` before the archive-local Compose build-context override was added. No dirty or untracked working-tree content entered the prospective Docker build context.
- **Read-only healthy evidence:** current stack project `hazardous-killbot` had one `discordbot` process (PID 9268), one watchdog process (PID 9267), and healthy PostgreSQL (PID 683); all had restart count `0`, `OOMKilled=false`, and no ungranted PostgreSQL lock or wait. PostgreSQL remained on `hazardous-killbot_contract-postgres`. All 26 `_sqlx_migrations` rows were successful and every stored SHA-384 checksum matched its candidate migration file. Prepared and unresolved-permanent deliveries were zero. The designated subscription remained active for exactly Metropolis (`10000042`), Heimatar (`10000030`), The Forge (`10000002`), Etherium Reach (`10000027`), and Perrigen Falls (`10000066`); Listed, Expired, and Closed — Outcome Unknown were `post`, while only Sale Confirmed and Purchase Confirmed were `post_and_ping`.
- **Blocking evidence:** `structure_resolver_runtime` was `degraded` with `last_error = structure resolver access denied` at `2026-08-25 17:40:15Z`; the latest `health_snapshots` row was consequently `degraded`. The immediate limiter read had current-window legacy `error_limit_remain = 0` and reset `34`, while the persisted pause/pacing fields were inactive. There were 3,369 unresolved `resolution_probe` collection failures and five unresolved `embed_context_enrichment` failures. These violate this ticket's absolute pre-deployment gates.
- **Additional read-only state:** the latest health evidence reported 114 regions with current contract progress, current ESI/R2Z2 progress, and 19,454 due backlog items; the 30-minute regional query found 114 completed regions and no incomplete batch. Recent recovery summaries reported 17–29 events per tick, which is not a substitute for this ticket's required post-deployment ten-window evidence.
- **Result:** halted before backup, Compose validation/deployment, preflight test, or Discord transport. No application binary was invoked; no production database, service, image, configuration, subscription, credential, Historical Enrichment Repair, or Discord mutation occurred. No rollback, restart, retry, or remediation was attempted. Leave this ticket claimed pending explicit operator direction.
