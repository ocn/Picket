# Global Public Contract Intelligence Deployment Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy commit `5cbfc2b` as a single-instance, PostgreSQL-backed global public-contract feed without migrating or disrupting the existing JSON-backed killmail feed.

**Architecture:** Provision PostgreSQL first, then replace the bot container once with an image built from a clean checkout of `5cbfc2b`. The application applies 12 forward SQLx migrations on its first successful database connection while Discord and the killmail pipeline start independently. Keep subscriptions empty until every discovered region has a silent baseline; then use a non-pinging supercapital canary before enabling confirmed-event pings.

**Tech Stack:** Rust 1.88, SQLx migrations, PostgreSQL 16, Docker Compose, Serenity/Discord, public EVE ESI.

## Global Constraints

- Deploy exactly one bot instance; this phase does not support horizontal scaling.
- Build from a clean detached checkout of `5cbfc2b`; the current working tree contains unrelated user-owned changes and must not be used as the Docker build context.
- Run production Compose commands from the existing deployment directory so `./config` continues to bind-mount the live JSON state. Never pass `--build` there; use the image built from the clean checkout.
- Run validation against a uniquely named, tmpfs-backed PostgreSQL container. It may share the trusted production Docker daemon, but it must not share a production container name, port, network, or volume.
- On the production host, export the existing Compose project name before running commands from the detached release checkout.
- Do not migrate `config/*.json`; existing killmail subscriptions, caches, standings, and feed checkpoints remain JSON-backed.
- Do not create contract subscriptions until the silent baseline gate passes.
- Do not enable `@here` or `@everyone` during the canary stage.
- In the final feed policy, keep both global feeds non-pinging. Only the proximity supercapital feed in channel `1117504110611157034` may use pinging actions.
- Keep `CONTRACT_COLLECTION_INTERVAL_SECS=300` for the initial rollout; do not poll ahead of ESI cache expiry.
- Use an explicit `CONTRACT_DATABASE_URL`. If its password contains URI-reserved characters, percent-encode only the URL copy.
- Migrations are forward-only. Roll back the application image while retaining the PostgreSQL volume; do not attempt schema down-migrations during incident response.
- Do not run the ignored Discord visual test without explicit approval for one message in a dedicated test channel.

---

## Deployment impact and migration summary

- Expected existing-feed interruption: one normal `discordbot` container restart. PostgreSQL provisioning and baseline collection do not require stopping the killmail feed.
- Required data migration: none.
- Required schema migration: all 12 files in `migrations/`, applied automatically by `ContractCollectionStore::connect` through SQLx.
- Greenfield production database: all tables are new, so migration lock risk is low.
- Existing pilot database: stop every contract-capable bot instance before upgrade. Several `ALTER TABLE` statements take PostgreSQL table locks, and migration `20260813000007` backfills delivery nonces.
- Rollback compatibility: the pre-contract image ignores the additive PostgreSQL schema.

Migration order:

1. `20260813000000`–`00001`: collection facts, manifests, presence, evidence, ESI cache, and limiter state.
2. `20260813000002`–`00003`: subscriptions, delivery outbox, durable history, and deferred matches.
3. `20260813000004`–`00005`: acceptance resolution and nonfinancial terminal states.
4. `20260813000006`: observation-time embed context.
5. `20260813000007`–`00008`: restart-safe delivery and persisted ping type.
6. `20260814000000`–`00001`: outage recovery and scoped failure lifecycles.
7. `20260814000002`: durable per-contract manifest deferral so one unavailable item endpoint cannot invalidate a complete regional summary.

## Files and operational surfaces

- Release source: commit `5cbfc2b`
- Runtime definition: `docker-compose.yaml`
- Environment template: `docs/env.sample`
- Operator reference: `docs/contract-intelligence.md`
- Schema: `migrations/*.sql`
- Runtime startup and failure isolation: `src/lib.rs`, `src/contract_intelligence.rs`
- Pre-deployment test seam: `tests/test_contract_intelligence.rs`
- Persistent existing state: `config/` bind mount
- New persistent state: Compose volume `contract-postgres`

---

### Task 1: Freeze an immutable release and record rollback state

**Files:**
- Read: `docker-compose.yaml`
- Read: `Dockerfile`
- Do not modify: current dirty working tree

**Interfaces:**
- Consumes: accepted release commit `5cbfc2b`
- Produces: clean release checkout, release image tag, rollback image tag, recorded current container/image IDs

- [ ] **Step 1: Record the current runtime before changing it**

Run on the production host from the current deployment directory:

```sh
umask 077
mkdir -p backups
chmod 700 backups
docker compose ps > backups/pre-contract-compose-2026-08-14.txt
docker inspect --type container --format '{"container_id":{{json .Id}},"created":{{json .Created}},"started_at":{{json .State.StartedAt}},"status":{{json .State.Status}},"restart_count":{{.RestartCount}},"image_id":{{json .Image}},"image_name":{{json .Config.Image}},"compose_project":{{json (index .Config.Labels "com.docker.compose.project")}}}' discordbot > backups/pre-contract-discordbot-2026-08-14.json
if rg -qi 'discord|eve|token|secret|password' backups/pre-contract-discordbot-2026-08-14.json; then
  printf '%s\n' 'redacted inspection evidence contains a forbidden term' >&2
  exit 1
fi
docker inspect --format '{{.Image}}' discordbot
export COMPOSE_PROJECT_NAME="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' discordbot)"
test -n "$COMPOSE_PROJECT_NAME"
```

Expected: the bot is running; both evidence files are created under a `0700` directory with the process umask set to `077`; the inspection JSON contains only the allowlisted operational fields and no secret-related terms; and the last command returns one immutable image ID.

- [ ] **Step 2: Preserve the running image under a rollback tag**

```sh
docker image tag "$(docker inspect --format '{{.Image}}' discordbot)" ocn-killbot-rust:rollback-pre-contract-2026-08-14
docker image inspect ocn-killbot-rust:rollback-pre-contract-2026-08-14 --format '{{.Id}}'
```

Expected: the rollback tag resolves to the image ID recorded in Step 1.

- [ ] **Step 3: Create a clean detached checkout**

Run from the repository root:

```sh
git worktree add --detach ../hazardous-killbot-release-5cbfc2b 5cbfc2b
git -C ../hazardous-killbot-release-5cbfc2b status --short
git -C ../hazardous-killbot-release-5cbfc2b rev-parse HEAD
```

Expected: status is empty and `HEAD` is the full hash for `5cbfc2b`.

- [ ] **Step 4: Install a non-secret build environment into the clean checkout**

```sh
cp ../hazardous-killbot-release-5cbfc2b/docs/env.sample ../hazardous-killbot-release-5cbfc2b/.env
chmod 600 ../hazardous-killbot-release-5cbfc2b/.env
```

This file is sufficient for image construction and isolated tests. Do not install production secrets until Task 3.

- [ ] **Step 5: Build and tag the release image**

```sh
cd ../hazardous-killbot-release-5cbfc2b
docker compose config --quiet
docker compose build --pull discordbot
docker image tag ocn-killbot-rust:latest ocn-killbot-rust:contract-intel-5cbfc2b
docker image inspect ocn-killbot-rust:contract-intel-5cbfc2b --format '{{.Id}}'
```

Expected: Compose validates without printing interpolated secrets and the immutable release tag exists.

- [ ] **Step 6: Stop on an identity mismatch**

Do not proceed if the release checkout is dirty, the checkout is not `5cbfc2b`, or the release and rollback tags resolve to the same newly built image unexpectedly.

---

### Task 2: Validate the release before touching production state

**Files:**
- Test: `tests/test_contract_intelligence.rs`
- Read: `docs/contract-intelligence.md`

**Interfaces:**
- Consumes: clean release checkout and a uniquely named, tmpfs-backed PostgreSQL test database
- Produces: test evidence tied to the exact release image source

- [ ] **Step 1: Record production identities, then start isolated PostgreSQL**

Do not use Compose for this validation database: its fixed container name and volume would collide with production. Record the production identities, then start a uniquely named PostgreSQL container backed only by tmpfs:

```sh
docker inspect --format '{{.Id}} {{.Image}}' discordbot
docker inspect --format '{{.Id}} {{.Image}}' killbot-contract-postgres
docker run -d \
  --name killbot-contract-validation-postgres \
  --tmpfs /var/lib/postgresql/data:rw,noexec,nosuid,size=1g \
  --publish 127.0.0.1:55433:5432 \
  --env POSTGRES_USER=killbot_contracts \
  --env POSTGRES_PASSWORD=killbot_contracts \
  --env POSTGRES_DB=killbot_contracts \
  --health-cmd 'pg_isready -U killbot_contracts -d killbot_contracts' \
  --health-interval 2s \
  --health-timeout 2s \
  --health-retries 30 \
  postgres:16-alpine
until test "$(docker inspect --format '{{.State.Health.Status}}' killbot-contract-validation-postgres)" = healthy; do sleep 2; done
```

Expected: the validation database is `healthy`; production container IDs and images are unchanged.

- [ ] **Step 2: Run the contract release suite serially**

```sh
CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:55433/killbot_contracts cargo test --test test_contract_intelligence -- --test-threads=1
```

Expected: 78 non-ignored contract tests pass. The manual Discord visual test remains ignored.

- [ ] **Step 3: Run the approved broad suite**

```sh
CONTRACT_TEST_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@127.0.0.1:55433/killbot_contracts cargo test -- --skip esi::tests::test_esi_timeout_is_configured --test-threads=1
cargo check
cargo clippy -- -W clippy::all
```

Expected: all non-credentialed tests pass; only the independently proven timeout test is filtered and the three external visual suites remain ignored. Clippy may report the accepted pre-existing warnings but must exit zero.

- [ ] **Step 4: Record the release gate**

Save the commit hash, image ID, command outputs, and operator name in the deployment record. Do not proceed on a new failure.

- [ ] **Step 5: Remove only the ephemeral validation database**

```sh
docker stop killbot-contract-validation-postgres
docker rm killbot-contract-validation-postgres
docker inspect --format '{{.Id}} {{.Image}}' discordbot
docker inspect --format '{{.Id}} {{.Image}}' killbot-contract-postgres
```

Expected: the validation container is removed with its tmpfs data; production container IDs and images still match Step 1. No validation volume was created.

---

### Task 3: Prepare PostgreSQL credentials and backups

**Files:**
- Modify outside Git: `.env`
- Backup: `config/`
- Backup when a database already exists: PostgreSQL custom-format dump

**Interfaces:**
- Consumes: current production configuration and optional existing contract database
- Produces: validated database credentials, recoverable JSON backup, recoverable PostgreSQL backup

- [ ] **Step 1: Return to the existing production deployment directory**

The clean checkout is only the build source. Run every production Compose command from the original `hazardous-killbot` deployment directory so `./config` remains the live bind mount:

```sh
cd ../hazardous-killbot
umask 077
export COMPOSE_PROJECT_NAME="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' discordbot)"
test -n "$COMPOSE_PROJECT_NAME"
mkdir -p backups
chmod 700 backups
git diff --exit-code 5cbfc2b -- docker-compose.yaml
docker compose config --quiet
```

Expected: the production directory retains its existing `.env`, `config/`, and Compose project identity; `docker-compose.yaml` matches the release; validation prints no secrets. Do not run `docker compose build` in this dirty directory.

- [ ] **Step 2: Determine whether this is greenfield or an existing pilot database**

```sh
docker volume ls --filter name=contract-postgres
docker ps --all --filter name=killbot-contract-postgres
```

Expected for first production deployment: no populated contract database volume. If a populated volume exists, treat this as an upgrade and do not change its password through Compose environment variables.

- [ ] **Step 3: Configure a greenfield password safely**

Run this step only when Step 2 proved no populated contract database exists. For an existing pilot database, retain its current password and matching `CONTRACT_DATABASE_URL`.

Generate a URL-safe password without printing it:

```sh
umask 077
CONTRACT_DB_PASSWORD_NEW="$(openssl rand -hex 32)"
```

Confirm the production `.env` does not already define the new keys, then append the generated value without printing it:

```sh
test -z "$(grep -E '^(CONTRACT_DATABASE_PASSWORD|CONTRACT_DATABASE_URL|CONTRACT_COLLECTION_INTERVAL_SECS)=' .env)"
printf 'CONTRACT_DATABASE_PASSWORD=%s\n' "$CONTRACT_DB_PASSWORD_NEW" >> .env
printf 'CONTRACT_DATABASE_URL=postgres://killbot_contracts:%s@postgres:5432/killbot_contracts\n' "$CONTRACT_DB_PASSWORD_NEW" >> .env
printf 'CONTRACT_COLLECTION_INTERVAL_SECS=300\n' >> .env
```

Because hexadecimal contains no URI-reserved characters, the URL form needs no additional encoding. Clear the temporary shell variable after editing:

```sh
unset CONTRACT_DB_PASSWORD_NEW
chmod 600 .env
docker compose config --quiet
```

Expected: Compose validates. Never run `docker compose config` without `--quiet` in captured logs because it can render secrets.

- [ ] **Step 4: Preserve existing JSON state**

```sh
tar -czf backups/config-pre-contract-2026-08-14.tar.gz config
chmod 600 backups/config-pre-contract-2026-08-14.tar.gz
tar -tzf backups/config-pre-contract-2026-08-14.tar.gz >/dev/null
```

Expected: the archive verifies. This is a backup only; no JSON data is imported into PostgreSQL.

- [ ] **Step 5: Back up an existing contract database before migration**

Skip only when Task 3 Step 2 proved the database is greenfield. If the existing pilot database is stopped, start only PostgreSQL and wait for `healthy` before dumping it.

```sh
docker compose exec -T postgres pg_dump -U killbot_contracts -d killbot_contracts -Fc > backups/contracts-pre-e5f60ab-2026-08-14.dump
chmod 600 backups/contracts-pre-e5f60ab-2026-08-14.dump
docker compose exec -T postgres pg_restore --list < backups/contracts-pre-e5f60ab-2026-08-14.dump >/dev/null
```

Expected: `pg_restore --list` succeeds. `pg_dump` produces a transactionally consistent logical backup while the server is running.

---

### Task 4: Provision PostgreSQL and apply the migrations

**Files:**
- Runtime: `docker-compose.yaml`
- Schema: `migrations/*.sql`

**Interfaces:**
- Consumes: validated `.env`, release image, and greenfield or backed-up database
- Produces: healthy PostgreSQL 16 database at the current schema version

Run every command in this task from the existing production deployment directory. The globally tagged `ocn-killbot-rust:latest` must be the image built from the clean checkout in Task 1.

- [ ] **Step 0: Make the existing pilot connection explicit**

The existing empty pilot database was initialized with the Compose defaults. If the three contract keys are absent, append the same existing credential and interval without printing the file:

```sh
umask 077
test "$(awk -F= '$1=="CONTRACT_DATABASE_PASSWORD"{n++} END{print n+0}' .env)" -eq 0
test "$(awk -F= '$1=="CONTRACT_DATABASE_URL"{n++} END{print n+0}' .env)" -eq 0
test "$(awk -F= '$1=="CONTRACT_COLLECTION_INTERVAL_SECS"{n++} END{print n+0}' .env)" -eq 0
printf '%s\n' 'CONTRACT_DATABASE_PASSWORD=killbot_contracts' >> .env
printf '%s\n' 'CONTRACT_DATABASE_URL=postgres://killbot_contracts:killbot_contracts@postgres:5432/killbot_contracts' >> .env
printf '%s\n' 'CONTRACT_COLLECTION_INTERVAL_SECS=300' >> .env
chmod 600 .env
test "$(awk -F= '$1=="CONTRACT_DATABASE_PASSWORD"{n++} END{print n+0}' .env)" -eq 1
test "$(awk -F= '$1=="CONTRACT_DATABASE_URL"{n++} END{print n+0}' .env)" -eq 1
test "$(awk -F= '$1=="CONTRACT_COLLECTION_INTERVAL_SECS"{n++} END{print n+0}' .env)" -eq 1
docker compose config --quiet
```

Expected: each key occurs exactly once and Compose validates. Do not print `.env` or rendered Compose output.

- [ ] **Step 1: Start PostgreSQL without replacing the bot**

```sh
docker compose up -d postgres
docker inspect --format '{{.State.Health.Status}}' killbot-contract-postgres
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c 'SELECT current_database(), current_user, version();'
```

Expected: health is `healthy`; database and user are both `killbot_contracts`; PostgreSQL reports major version 16.

- [ ] **Step 2: Reconfirm there is only one bot instance**

```sh
docker ps --filter name=discordbot --format '{{.ID}} {{.Image}} {{.Status}}'
```

Expected: exactly one existing bot container. Do not use a blue/green overlap or `--scale` for this release.

- [ ] **Step 3: Start the release bot once**

```sh
docker image tag ocn-killbot-rust:contract-intel-5cbfc2b ocn-killbot-rust:latest
test "$(docker image inspect ocn-killbot-rust:latest --format '{{.Id}}')" = "$(docker image inspect ocn-killbot-rust:contract-intel-5cbfc2b --format '{{.Id}}')"
docker compose up -d --no-deps --force-recreate discordbot
docker compose ps
docker compose logs --since=5m discordbot
```

Expected: Discord starts, `Listening for killmails...` appears, and the contract task connects without terminating the bot. SQLx applies migrations inside the background contract task.

- [ ] **Step 4: Verify the SQLx migration ledger**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT count(*) AS migration_count, bool_and(success) AS all_successful FROM _sqlx_migrations;"
```

Expected: 12 ordered rows, `migration_count = 12`, and `all_successful = true`.

- [ ] **Step 5: Verify critical schema constraints**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT conrelid::regclass AS table_name, conname, contype FROM pg_constraint WHERE conrelid IN ('contract_resolution_cases'::regclass, 'contract_outbound_deliveries'::regclass) ORDER BY 1,2;"
```

Expected: lifecycle, delivery-status, ping-type checks; delivery identity uniqueness; primary keys; and the subscription-history foreign key are present.

- [ ] **Step 6: Verify killmail continuity before evaluating contracts**

Observe logs for at least one normal feed polling interval:

```sh
docker compose logs --since=10m discordbot
```

Expected: no startup loop, no dispatcher shutdown, and normal R2Z2/killmail producer activity. If PostgreSQL is unavailable, contract commands may be unavailable temporarily, but the killmail producer must remain running.

---

### Task 5: Establish the silent global baseline

**Files:**
- Read: `docs/contract-intelligence.md`
- Database tables: `esi_cache_metadata`, `regional_collection_metadata`, `contract_collection_failures`, `contract_subscriptions`, `contract_outbound_deliveries`

**Interfaces:**
- Consumes: migrated database and running collector with zero contract subscriptions
- Produces: complete silent baseline for the 64 public known-space regions; wormhole, Jove, Pochven, and other special-space regions are outside this deployment gate

- [ ] **Step 1: Prove notifications cannot be emitted during warm-up**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT count(*) AS active_subscriptions FROM contract_subscriptions WHERE deleted_at IS NULL;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT count(*) AS deliveries FROM contract_outbound_deliveries;"
```

Expected: both counts are zero.

- [ ] **Step 2: Watch collection progress without reducing the interval**

```sh
docker compose logs --follow discordbot
```

Expected: completed cycles log `contract collection cycle finished`. ESI rate-limit or cache pauses may delay progress and must be honored.

- [ ] **Step 3: Compare discovered regions with completed baselines**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "WITH expected AS (SELECT value::bigint AS region_id FROM esi_cache_metadata CROSS JOIN LATERAL jsonb_array_elements_text(response) WHERE resource_key = 'regions'), public_expected AS (SELECT region_id FROM expected WHERE region_id BETWEEN 10000001 AND 10000069 AND region_id NOT IN (10000004,10000017,10000019)) SELECT (SELECT count(*) FROM public_expected) AS expected_regions, count(*) AS tracked_regions, count(*) FILTER (WHERE baseline_at IS NOT NULL) AS baselined_regions, min(last_complete_at) AS oldest_completion, max(last_complete_at) AS newest_completion FROM regional_collection_metadata WHERE region_id IN (SELECT region_id FROM public_expected);"
```

Expected gate: `expected_regions = tracked_regions = baselined_regions = 64`, with non-null completion timestamps. The collector may retain harmless metadata for other ESI universe regions, but those regions do not delay this rollout. Do not create subscriptions before equality holds.

- [ ] **Step 4: Require a clean operational state**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT failure_kind, count(*) FROM contract_collection_failures WHERE resolved_at IS NULL AND classification IS NULL GROUP BY failure_kind ORDER BY failure_kind;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT pause_until, error_limit_remain, rate_limit_remaining, updated_at FROM esi_collection_limiter_state;"
```

Expected: no unresolved collection failures scoped to the 64 public regions. Failures in excluded wormhole/special regions are recorded for operations but do not block this rollout. A limiter row is acceptable only when `pause_until` is null or in the past and remaining capacity is no longer exhausted.

- [ ] **Step 5: Take the first post-baseline database backup**

```sh
umask 077
docker compose exec -T postgres pg_dump -U killbot_contracts -d killbot_contracts -Fc > backups/contracts-post-baseline-5cbfc2b-2026-08-14.dump
chmod 600 backups/contracts-post-baseline-5cbfc2b-2026-08-14.dump
docker compose exec -T postgres pg_restore --list < backups/contracts-post-baseline-5cbfc2b-2026-08-14.dump >/dev/null
```

Expected: the dump validates.

---

### Task 6: Run a non-pinging supercapital canary

**Files:**
- Runtime command: `/contract_subscribe`
- Database tables: `contract_subscriptions`, `contract_outbound_deliveries`

**Interfaces:**
- Consumes: completed global baseline and a dedicated Discord canary channel
- Produces: one active supercapital subscription with posts but no mentions

Approved rollout channels:

- Global supercapital canary: `supercaps-sold (global)` — `1117513781505966201`
- Later global capital feed: `caps-sold (global)` — `1184511907160408074`
- Later proximity feed: `supercaps-sold (within 8 ly of Turnur, 8 ly of Kurnanien)` — `1117504110611157034`

- [ ] **Step 1: Verify channel permissions**

In channel `1117513781505966201`, verify the bot can View Channel, Send Messages, and Embed Links, and operators can Use Application Commands. Confirm the guild's existing Discord Integration command overrides restrict `/contract_subscribe` and `/contract_unsubscribe` to the same operator roles used for current feed management; this release intentionally adds no new source-level permission model. Mention Everyone is not required until pinging actions are enabled.

- [ ] **Step 2: Create the canary subscription in the target channel**

Use this exact filter document:

```json
{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"direction":"offered","ids":[30,659]}}},{"condition":{"ship_groups":{"direction":"requested","ids":[30,659]}}}]}]}}
```

Use this exact action document:

```json
{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post","expired":"post","closed_outcome_unknown":"post"}
```

Invoke:

```text
/contract_subscribe id:supercap-global description:Global public titan and supercarrier contracts filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"direction":"offered","ids":[30,659]}}},{"condition":{"ship_groups":{"direction":"requested","ids":[30,659]}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post","expired":"post","closed_outcome_unknown":"post"} ping_type:here
```

Every action is `post`, so this stage cannot mention `@here` or `@everyone`.

- [ ] **Step 3: Verify persisted configuration**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT guild_id, channel_id, subscription_id, description, filter, event_actions, deleted_at FROM contract_subscriptions WHERE subscription_id = 'supercap-global';"
```

Expected: one active row in the intended guild/channel with all actions set to `post`.

- [ ] **Step 4: Observe at least two complete post-subscription cycles**

```sh
docker compose logs --follow discordbot
```

Expected: cycles complete, killmail processing continues, and no contract message contains a mention. A natural matching event may not occur during this short gate.

- [ ] **Step 5: Validate one representative message if no natural event occurs**

Only with explicit approval, run the ignored non-pinging visual test against the dedicated canary channel:

```sh
printf 'Dedicated Discord channel ID: ' >/dev/tty
read -r DEDICATED_CONTRACT_CANARY_CHANNEL_ID
test -n "$DEDICATED_CONTRACT_CANARY_CHANNEL_ID"
CONTRACT_TEST_DISCORD_CHANNEL_ID="$DEDICATED_CONTRACT_CANARY_CHANNEL_ID" cargo test manual_contract_embed_visual_delivery -- --ignored --nocapture
```

Run this from the clean `5cbfc2b` checkout after securely exporting the production Discord credentials into that shell. Do not copy or print those credentials.

Expected: exactly one non-pinging production-format message. Verify the title, offered/requested bundle, ISK, Jita/The Forge location, issuer/history, thumbnail, and isolated fenced `contract:0//214664977`-style address, then delete the test message.

- [ ] **Step 6: Inspect the delivery outbox**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT status, failure_kind, count(*) FROM contract_outbound_deliveries GROUP BY status, failure_kind ORDER BY status, failure_kind;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT id, channel_id, contract_id, event_kind, failure_kind, last_error, last_attempt_at FROM contract_outbound_deliveries WHERE failure_kind IS NOT NULL AND failure_resolved_at IS NULL ORDER BY last_attempt_at, id;"
```

Expected: no unexplained `failed` rows and the unresolved-failure query returns no old transient/ambiguous failures.

---

### Task 7: Enable production actions and pings

**Files:**
- Runtime command: `/contract_subscribe`
- No file or schema changes

**Interfaces:**
- Consumes: successful canary and verified Discord permissions
- Produces: production action policy with pings only for confirmed financial events

- [ ] **Step 1: Grant the bot Mention Everyone only in the production feed channel**

This Discord permission controls both `@here` and `@everyone`. Keep it scoped by channel overwrite rather than granting it broadly across the guild.

- [ ] **Step 2: Replace the canary subscription using the same ID**

Use this production action document:

```json
{"listed":"post","sale_confirmed":"post_and_ping","purchase_confirmed":"post_and_ping","expired":"post","closed_outcome_unknown":"post"}
```

Invoke the complete replacement command:

```text
/contract_subscribe id:supercap-global description:Global public titan and supercarrier contracts filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"direction":"offered","ids":[30,659]}}},{"condition":{"ship_groups":{"direction":"requested","ids":[30,659]}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post_and_ping","purchase_confirmed":"post_and_ping","expired":"post","closed_outcome_unknown":"post"} ping_type:here
```

Reusing `id:supercap-global` replaces the active subscription rather than creating overlapping duplicate feeds.

- [ ] **Step 3: Verify the production policy in PostgreSQL**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT subscription_id, event_actions, deleted_at FROM contract_subscriptions WHERE subscription_id = 'supercap-global' ORDER BY updated_at DESC;"
```

Expected: one active row. Listed, Expired, and Closed—Outcome Unknown are non-pinging; Sale Confirmed and Purchase Confirmed are pinging.

- [ ] **Step 4: Verify shared cooldown behavior operationally**

When the first qualifying contract event arrives, confirm that one message is posted per subscription and at most one channel-wide mention occurs within five minutes across killmail and contract feeds. Subsequent messages must still post without a mention.

---

### Task 8: Monitor the first 24 hours and establish routine operations

**Files:**
- Read: `docs/contract-intelligence.md`
- No repository changes

**Interfaces:**
- Consumes: active production subscription
- Produces: signed deployment acceptance or rollback decision

- [ ] **Step 1: Monitor continuously for the first 30 minutes**

```sh
docker compose ps
docker compose logs --since=30m discordbot
docker compose logs --since=30m postgres
```

Expected: both containers healthy, killmail processing continues, contract cycles complete, and no restart loop appears.

- [ ] **Step 2: Check unresolved delivery and collection failures hourly for 24 hours**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT status, failure_kind, count(*) FROM contract_outbound_deliveries GROUP BY status, failure_kind ORDER BY status, failure_kind;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT id, channel_id, contract_id, event_kind, failure_kind, last_error, last_attempt_at FROM contract_outbound_deliveries WHERE failure_kind IS NOT NULL AND failure_resolved_at IS NULL ORDER BY last_attempt_at, id;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT failure_kind, region_id, contract_id, resource_key, count(*) FROM contract_collection_failures WHERE resolved_at IS NULL AND classification IS NULL GROUP BY failure_kind, region_id, contract_id, resource_key ORDER BY failure_kind, region_id, contract_id;"
```

Expected: prepared rows drain; transient/ambiguous failures resolve; permanent delivery failures identify an actionable Discord permission/channel problem.

- [ ] **Step 3: Track database growth daily for the first week**

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;"
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total_size FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;"
```

Expected: growth is understood and disk headroom remains adequate. Do not introduce ad hoc deletion: the only tested cleanup path is for successful `regional_observations` older than 90 days, and it is not scheduled automatically.

- [ ] **Step 4: Schedule backups without changing retention policy**

Take a daily custom-format `pg_dump` for the first week and retain the pre-deployment JSON/config archive. Define long-term backup and retention policy as a separate follow-up; it is not part of this deployment.

- [ ] **Step 5: Sign the production acceptance gate**

Accept the deployment only when all are true:

```text
[ ] Exact image is contract-intel-5cbfc2b
[ ] 11 SQLx migrations succeeded
[ ] Killmail feed remained healthy
[ ] Every discovered region has a silent baseline
[ ] No unresolved collection failure blocks progress
[ ] Canary produced no mentions
[ ] Production subscription has the intended actions
[ ] Prepared deliveries drain and permanent failures are actionable
[ ] Database backup validates
```

---

### Task 9: Rollback procedure

**Files:**
- Restore only if explicitly required: application image and `.env`
- Retain: PostgreSQL volume and `config/`

**Interfaces:**
- Consumes: rollback image tag and pre-deployment backups
- Produces: restored pre-contract application behavior without destroying collected evidence

- [ ] **Step 1: Stop notifications first when the defect is contract-only**

Run `/contract_unsubscribe id:supercap-global` in the production feed channel. This soft-deletes the active subscription while preserving delivery history. Confirm:

```sh
docker compose exec -T postgres psql -U killbot_contracts -d killbot_contracts -c "SELECT subscription_id, deleted_at FROM contract_subscriptions WHERE subscription_id = 'supercap-global';"
```

Expected: `deleted_at` is non-null and no new matching outbox rows are prepared.

- [ ] **Step 2: Roll back the bot image only when application health is affected**

```sh
docker image tag ocn-killbot-rust:rollback-pre-contract-2026-08-14 ocn-killbot-rust:latest
docker compose up -d --no-deps --force-recreate discordbot
docker compose logs --since=5m discordbot
```

Expected: the previous bot starts and resumes the existing killmail feed. Keep the PostgreSQL service and volume intact; the old image ignores contract tables.

- [ ] **Step 3: Do not run down-migrations**

The release has no down scripts. Do not drop tables, delete the Compose volume, or restore the pre-deployment dump merely to roll back application code.

- [ ] **Step 4: Restore PostgreSQL only for proven database corruption**

If corruption is proven, stop the contract-capable bot, preserve the failed database, restore the validated dump into a separate replacement database, verify it, then point `CONTRACT_DATABASE_URL` at the replacement. This is an incident-recovery operation requiring a separate reviewed runbook; it is not the normal rollback path.

- [ ] **Step 5: Re-deploy forward after remediation**

Retag `ocn-killbot-rust:contract-intel-5cbfc2b` as `latest`, recreate the single bot container, verify the existing 12 migrations remain successful, wait for a successful global cycle, and recreate the subscription with the approved action policy.

---

## Self-review

- Spec coverage: release identity, backups, configuration, all schema migrations, startup isolation, silent baseline, canary, production pings, observability, retention limitation, and rollback are covered.
- Placeholder scan: commands use exact release tags, dates, table names, subscription IDs, filters, action documents, and expected results. Runtime secrets and the dedicated Discord channel remain intentionally outside source control.
- Type and schema consistency: table, column, event, action, command, and migration names match commit `5cbfc2b`.
- Scope: no JSON migration, authenticated-contract rollout, counterparty data, provisional/edit machinery, horizontal scaling, or retention implementation is included.

## Primary operational references

- [PostgreSQL `pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html)
- [PostgreSQL `ALTER TABLE` locking behavior](https://www.postgresql.org/docs/current/sql-altertable.html)
- [Docker Compose `up`](https://docs.docker.com/reference/cli/docker/compose/up/)
- [Docker Compose `config`](https://docs.docker.com/reference/cli/docker/compose/config/)
- [EVE ESI caching and request guidance](https://developers.eveonline.com/docs/services/esi/best-practices/)
- [Discord permissions](https://docs.discord.com/developers/topics/permissions)
