# 09 — Validate the production contract feed

**What to build:** Complete release-level verification and operator documentation so Global Public Contract Intelligence can be deployed and evaluated without weakening the existing bot.

**Blocked by:** 06 — Enrich contract intelligence embeds; 07 — Make Discord delivery restart-safe; 08 — Recover contract intelligence without notification floods.

**Status:** ready-for-agent

- [ ] The highest contract-cycle test seam covers Regional Baseline, Listed, Awaiting Resolution, Sale Confirmed, Purchase Confirmed, Expired, Closed — Outcome Unknown, filtering, persistence, and prepared Discord deliveries end to end.
- [ ] Captured official ESI fixtures cover public contract summaries, item manifests, pagination/cache headers, `204`, `304`, `404`, and representative transient failures without requiring live network access in the standard suite.
- [ ] PostgreSQL migrations succeed against both a clean database and an already-current database, and database constraints enforce lifecycle and delivery idempotency.
- [ ] Application-level tests prove that PostgreSQL or contract-collector failure does not stop the killmail feed.
- [ ] Normal embed tests verify content and structure without Discord credentials, while a separate ignored integration test supports manual visual delivery to Discord.
- [ ] Operator documentation explains required database configuration, local container startup, contract commands, supported event actions, recovery behavior, retention, and the confirmed-only evidence limitation.
- [ ] Documentation identifies ESI cache latency as an external bound and does not promise unsupported real-time contract-status lookup.
- [ ] The release has no provisional alert, Discord edit/retraction, counterparty, authenticated-contract, persistence-migration, or horizontal-scaling machinery.
- [ ] Existing JSON-backed killmail subscriptions, caches, standings, and feed checkpoints remain readable and writable with unchanged behavior.
- [ ] Formatting, compilation, linting, unit tests, contract integration tests, and all existing non-ignored tests pass.
