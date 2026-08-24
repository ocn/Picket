# 04 — Verify the integrated Recovery Deployment candidate

**What to build:** Establish one reviewed fixed point containing corrected health evidence, Retained Contract Manifest reuse, and single-owner Terminal Resolution Recovery, with complete pre-production evidence that the combined runtime is deployable.

**Blocked by:** 01 — Correct public-contract ESI health evidence; 02 — Reuse Retained Contract Manifests; 03 — Establish single-owner Terminal Resolution Recovery.

**Status:** claimed

- [ ] The three completed slices are present together without reintroducing sweep-coupled terminal probes, unnecessary retained-manifest requests, or false ESI-progress health.
- [ ] Focused cross-slice regressions prove collection, terminal recovery, health evaluation, Progressive Enrichment, and delivery remain compatible.
- [ ] The complete serial real-PostgreSQL contract suite passes from a clean temporary database.
- [ ] Migration compatibility passes against both a clean database and the current applied production ledger, confirming that this effort adds no schema migration.
- [ ] Safe library tests, formatting, typechecking, Clippy, diff checks, and quiet Docker Compose configuration validation pass on the same fixed point.
- [ ] Public ESI wire tests prove no Structure Authorization bearer token reaches regional discovery, manifests, lifecycle probes, universe names, affiliations, or health-related public operations.
- [ ] Authenticated structure wire tests prove player-structure requests still carry the required scoped bearer token.
- [ ] Existing five-region Contract Subscription filtering, lifecycle actions, ping eligibility, presentation revision, and Historical Enrichment Repair tests remain green.
- [ ] Documentation, domain language, the recovery ADR, and the executable rollout checklist describe the verified behavior without introducing a canary environment or automated production action.
- [ ] The exact reviewed revision and verification evidence are recorded for the Recovery Deployment handoff.
- [ ] Final Sol high Standards/Spec review of the full recovery range finds no blocking issue.

## Comments

- 2026-08-24 — Verification candidate: `fc19b5de6e845a0d0b734e24b7975f2499fdca0c`. Its contract source, tests, migrations, and Cargo manifests are unchanged from `cf617d047686d4c821277a1c6899ad116399cc8d`; the only intervening commit records Ticket 03's tracker resolution. The complete serial real-PostgreSQL suite was therefore retained as fixed-point evidence: 273/273 passed in 325.96s at `cf617d0`.
- 2026-08-24 — New bounded evidence on that candidate: clean/current migration compatibility; corrected ESI-progress health; retained-manifest request scaling; bounded sole-owner terminal recovery; public and authenticated ESI authorization wires; subscription/presentation; ping eligibility; and read-only historical repair dry-run tests all passed (10 focused tests). Safe library suite: 95 passed, 1 credentialed visual test ignored, with only the documented independently proven ESI-timeout test skipped. `cargo fmt --check`, `cargo check`, `git diff --check`, and `docker compose config --quiet` passed. `cargo clippy -- -W clippy::all` exited 0 with 87 pre-existing warnings outside the recovery diff range: 74 `uninlined_format_args`, 5 `too_many_arguments`, 3 `result_large_err`, 2 `type_complexity`, and one each of `large_enum_variant`, `needless_borrow`, and `needless_return`.
- 2026-08-24 — Documentation reconciles the Retained Contract Manifest, Terminal Resolution Recovery, corrected public-contract ESI progress, observational watchdog, Recovery Deployment, Production Verification, and historical-repair gate without creating a canary or automated production action. No deployment, service restart, production database or subscription mutation, Discord call, or historical queue action occurred. The protected unstaged `ON CONFLICT` test hunk remains untouched.
- 2026-08-24 — Status remains claimed pending final Sol high Standards/Spec review of `6b2b3ad1f2b8d3a6ad25a73b26d799bb78fbbbb8..fc19b5de6e845a0d0b734e24b7975f2499fdca0c`.
