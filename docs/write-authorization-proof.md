# Write authorization: implementation proof

Recorded 2026-11-17 for the operator-managed guild-grant change. This records local implementation and deployment evidence, not production verification. The [operator guide](write-authorization.md) describes adoption; [ADR 0011](adr/0011-enforce-writes-with-operator-managed-guild-grants.md) records the authority boundary.

## Durable decisions

- One immutable startup policy grants a single write capability to explicit user IDs or role IDs in the originating guild. No Administrator/Manage Server bypass, wildcards or global grants. Strict version-1 parsing rejects ambiguous/invalid authority configuration, including duplicate fields/guild keys, numeric IDs and the guild's everyone role.
- Shared request adapters used by production and offline raw-Serenity tests guard every write and standings continuation before business parsing/I/O. Unknown operations fail closed. Continuations bind owner and original guild/channel/Subscription ID, refresh membership within two seconds, and claim completing state once. Components acknowledge privately before awaiting membership.
- Subscription identity remains the existing tuple. Final completion uses a then-current matching Subscription and does not recreate one deleted during work. Separate standings and Subscription saves are not transactional. Grants are checked at request boundaries; immediate cancellation of an already-running operation is not promised.
- Missing/invalid policy supplies deny-all while read/Feed initialization continues. The default is `policy/write-policy.json`; `PICKET_WRITE_POLICY_PATH` is the operator override. Policy loads only on process startup, rejects symlink indirection and writable-state paths, and has no Discord editing API.
- Supplied Compose mounts `./policy:/app/policy:ro` separately from writable `./config:/app/config:delegated`. Operator provisioning must precede adoption. A path override also needs a matching separate read-only mount with no writable alias. Actual policy files are ignored by Git/build contexts; only the deny-all example is retained. Local build/review artifacts are excluded from Docker contexts.

## Behavior and repository checks

The policy implementation at `91679de` passed 11 offline policy tests. The request-boundary implementation at `b93b0f1` passed 23 offline authorization tests and 14 command-registration tests. Coverage includes user/role grants, cross-guild and Administrator denial, identity mismatch, all production command classifications, malformed/unknown operations, owner/binding checks, fresh-membership failure/revocation/timeout, acknowledgement ordering, replay/concurrent claims, missing-policy read/Feed startup, bounded secret-free diagnostics and final Subscription update behavior.

The recorded broad commands were `cargo fmt --check`, `cargo check --offline`, `cargo clippy --offline -- -W clippy::all`, `cargo test --offline` against a disposable local PostgreSQL database, explicit completion of the remaining test binaries after the broad run stopped, and `cargo test --offline --doc`. Formatting, compilation and Clippy passed; Clippy reported 17 existing warnings in untouched logic. No new warning in the authorization modules was reported. The documentation target contained zero tests.

Across the initial broad run and completion of its remaining binaries: **822 passed, 1 failed, 4 ignored**. This is aggregate evidence, not a passing single full-suite run. The failure was `acceptance_provenance_forward_migration_preserves_the_applied_ledger_and_quarantines_stale_replay`: an existing history assertion expected one confirmed sale and observed zero. The controller independently reproduced the same assertion on unchanged `origin/main`. A fixed August fixture falling outside the query's rolling 90-day window on November 17 is the source-based explanation; no history/migration logic was changed here.

First failures remain in local evidence: missing dependency/DNS setup failures; expected missing-module and behavioral red tests; registration assertions updated for the approved permission change; initial formatting; a parallel scoped-log-capture race corrected in the test harness; the broad history failure; and an initial remaining-binary compile failure caused by baseline library artifacts sharing the target directory. After the baseline run ended, a package-only cache clean repaired that artifact interference and the unexecuted targets were rebuilt. The failing history result remains in the total.

Final-diff limits: the broad run preceded a final diagnostic-only cleanup in SSO handling. The resulting source passed rebuilt remaining targets, including all 23 authorization and 11 policy tests, plus format/check/Clippy. The controller's subsequent test-only guild/channel fixture correction at `e98bf2e` passed its focused binding test and formatting. No full-suite run of that exact final tree is claimed. Task 3 changes documentation, Compose, ignores and the example only; it consumes those Rust results without repeating the suite.

An independent GPT-6.1 Sol review at explicit xhigh effort approved the runtime/tests/Cargo diff through `e98bf2e` in one round with no findings. This review did not assess the later deployment/documentation diff; whole-branch final review remains a separate step.

## Local deployment checks

All checks below passed on Docker Desktop's `desktop-linux` context, Engine `29.7.2`. The Compose render used `--env-file /dev/null`, forwarded only `HOME`/`PATH`, and supplied four dummy Discord/EVE variable values. No `.env` was read and no real credentials were used.

| Check | Observed result |
| --- | --- |
| `docker compose --env-file /dev/null config --format json` in sanitized environment | Default policy path preserved; bot user `501:20`; root `read_only: true`; `cap_drop: [ALL]`; `no-new-privileges:true`; exactly two distinct bot mounts: policy read-only, state writable. No second writable policy alias in supplied Compose. |
| Exact custom-path YAML from the operator guide, merged render | `/app/operator-policy/write-policy.json` selected; matching custom directory mount read-only, default policy mount read-only and state writable; containment preserved. This checks configuration, not a custom-path live mount. |
| Isolated shell container using already-local `postgres:16-alpine` with overridden entrypoint | No network, non-root `501:20`, read-only root, all capabilities dropped, no-new-privileges; only the two dummy bind mounts. No database or Picket process started. |
| Dummy policy read, append and file creation | Read succeeded; append and creation both failed with `Read-only file system`, despite writable host-side dummy permissions. Host readback confirmed unchanged policy and no new policy file. |
| Dummy state and root writes | State write succeeded and persisted to the dummy host directory; root write failed with `Read-only file system`. `/proc/self/status` reported zero effective capabilities and `NoNewPrivs: 1`. |
| Cleanup | Disposable shell container, its image-declared anonymous volume and temporary policy/state directories removed. Existing test database was not touched. |
| Git exclusion | Actual/default/alternate/nested policy paths ignored; example remains addable. |
| Actual Docker build-context export using `FROM scratch`, no running process | A dummy actual policy file was excluded; `policy/write-policy.example.json` remained and parsed as deny-all. Wildcard copy probes for `.scratch` and `.superpowers` exported neither directory. Only the example and public README were exported; temporary files removed. |

The container proof used temporary host directories and a shell from a utility image, not Picket's production binary/image. That image declares an additional anonymous database-data volume; it was unused and explicitly removed after the proof. Container inspect established the two bind mounts' `RW` flags as false/true. A rendered declaration and this local filesystem check do not establish the production host's mount layout or security.

The deny-all example is exactly `{ "version": 1, "guilds": {} }`. No production IDs or grants were fabricated. Deployment checks used bounded subprocess timeouts and retained their first outputs locally. The tracked summary contains decisions and observed results, not private policy contents or scratch documents.

## Unassessed boundaries

Credentialed Discord embed tests remained ignored. No live Discord/EVE behavior, production Compose stack, deployment, bot-token operation, host/kernel escape resistance, dependency audit or resource-exhaustion resistance was tested. Stolen approved identities and allowed-role administrators remain trusted by the chosen model. An already-compromised process need not obey its own policy decisions; read-only storage protects the prescribed on-disk mount, not arbitrary process behavior. Repository commit hooks/import enforcement/ontology were not introduced by this change.

Docker's [bind mount documentation](https://docs.docker.com/engine/storage/bind-mounts/) and [Compose service reference](https://docs.docker.com/reference/compose-file/services/) describe the read-only mount/root and containment declarations. The results above come from local render, inspect and filesystem observations.
