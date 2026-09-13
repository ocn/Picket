# 04 — Rename code names at the next planned Deployment

**What to build:** Crate, binaries, Docker image, non-database containers, both User-Agent strings, and the startup log line say Picket. Ships with whatever the next planned Deployment carries; it does not get its own Deployment and must pass the Rollout Health Gate like anything else.

**Blocked by:** None. Scheduled by the next Deployment, not by another ticket.

**Status:** ready-for-agent

- [ ] `Cargo.toml` `name = "picket"`; `use killbot_rust::` becomes `use picket::` in `src/main.rs`, `src/bin/health_watchdog.rs`, `src/bin/structure_resolver_provision.rs`, and tests.
- [ ] `Dockerfile` copies and runs `picket` instead of `killbot-rust`; `docker-compose.yaml` image is `ocn-picket:latest`, containers are `picket` and `picket-health-watchdog`. The postgres container name and the `contract-postgres` volume are unchanged.
- [ ] `src/contract_intelligence.rs` User-Agent is `picket Global Public Contract Intelligence`; `src/feed/r2z2.rs` User-Agent is `picket R2Z2 feed consumer`. Check the contact email is still in whatever UA header helper builds these, since CCP's ESI guidance asks for one.
- [ ] `src/lib.rs` logs `Starting Picket...`.
- [ ] `.scratch/cache-fix-deployment-wizard.sh` and `.claude/skills/eve-esi-api/scripts/inspect-openapi.sh` reference the new binary or image name.
- [ ] `cargo test` and `cargo clippy -- -W clippy::all` green; `docker compose config` shows the new names; the existing Postgres volume mounts unchanged.
- [ ] `CHANGELOG.md` Unreleased notes the binary and image rename for self-hosters, with the old and new names side by side.
