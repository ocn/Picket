# 01 — Establish silent Global Public Contract Intelligence baselines

**What to build:** Add an isolated PostgreSQL-backed collector that can observe every region's public item-exchange contracts, retain their public facts and item manifests, and establish silent Regional Baselines without disrupting the existing killmail feed.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] PostgreSQL migrations create durable storage for Public Contract facts, item manifests, regional collection metadata, cache metadata, Presence Intervals, lifecycle evidence, and collection failures.
- [ ] Local container configuration provides PostgreSQL and a contract-database connection without changing the existing JSON-backed persistence used by killmails, metadata caches, standings, or feed checkpoints.
- [ ] The collector discovers all EVE regions and retains only public item-exchange contracts from each complete paginated regional response.
- [ ] Newly observed contracts have their structured Offered Items and Requested Items fetched and stored; contract titles are never used to infer contents.
- [ ] The first complete observation of each region is recorded as a silent Regional Baseline and emits no user-facing event.
- [ ] A regional observation is complete only when all expected pages succeed and their pagination/cache metadata is internally consistent; otherwise it is an Inconclusive Observation.
- [ ] Inconclusive Observations cannot create presence, absence, or terminal lifecycle transitions.
- [ ] Unchanged contract facts and item manifests are stored once while repeated complete observations extend Presence Intervals rather than duplicating fact rows.
- [ ] ESI requests identify the application, use conditional requests, respect advertised cache boundaries, and honor error-limit, rate-limit, and retry headers.
- [ ] PostgreSQL unavailability pauses and retries only contract collection; the existing killmail feed remains operational and no in-memory contract-state fallback is introduced.
- [ ] Automated tests drive the production collection seam with fake ESI responses and a temporary PostgreSQL database, covering complete baselines, inconsistent pagination, missing manifests, unchanged observations, and database failure isolation.

