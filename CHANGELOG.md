# Changelog

## [Unreleased]

Draft for the next public release after `v0.1.1`. The entries below currently
cover the recent feed workstreams and selected fixes; they are not yet a complete
account of changes since that release. Before publishing, reconcile
`v0.1.1..[release commit]`, include the Rust rewrite and intervening killfeed
changes, and assign the final version and release date. See the
[release-scope audit](docs/changelog-audit-2026-09-13.md).

### Changed

- Restrict subscription management, watchlist management, and standings-sync
  commands to Manage Server permissions by default, and disable them in DMs.
- Show ship type names for individually tracked ships in killmail fleet
  composition, while retaining group names for group-based tracking.
- Rename the project to Picket. Documentation and the repository change now;
  the crate, binaries, Docker image, and container names follow at the next
  planned deployment.

### Added

- Add public item-exchange contract subscriptions with `/contract_subscribe`
  and `/contract_unsubscribe`: new listings, confirmed sales and purchases,
  expirations, and closures whose outcome cannot be confirmed. A disappearing
  listing alone is not reported as a sale; private contracts and counterparties
  are outside this feed's scope.
- Filter contract alerts by offered/requested items, ship groups, ISK, issuer,
  location, and light-year range, with separate post/ping actions per event.
- Enrich contract alerts with issuer, item, ISK, and location context, including
  accessible player structures through an optional structure resolver. Unknown
  proximity produces a non-pinging alert that can be updated and promoted when
  its location is verified.
- Add sovereignty subscriptions with `/sov_subscribe` and `/sov_unsubscribe`,
  campaign discovery and configurable advance reminders, and `/sov_timers`
  for this channel's matching campaigns. Optional alerts identify Sovereignty
  Hub vulnerability windows shifting into a preferred timezone window.
- Add sovereignty reachability filters from a configured home system over
  stargates and an optional Wanderer wormhole map, with newly reachable
  campaign alerts, wormhole path risk, and chain freshness reporting.
- Add `/watch`, `/watch_subscribe`, and `/watch_unsubscribe` for corporation
  and alliance changes: corporation joins/leaves, alliance changes, member-count
  swings, war declarations, new war allies, retractions, and endings. Initial
  snapshots are silent; member-count alerts require about a week of history.
- Include killmail and contract summaries in pinging message content so phone
  notifications and channel previews provide context before opening the embed.
- Show the attacker credited with the final blow beside the victim in killmail
  embeds, using the same linked character and affiliation formatting.
- Support named Discord role pings for killmail subscriptions with explicit
  allowed-mention control and configurable maximum killmail age.
- Support configurable channel-wide killmail ping cooldowns, retaining the
  existing five-minute default while allowing longer per-subscription windows.
- Add the high-value `nano-activity` roaming feed for non-NPC low/null-security
  kills by subcapital attackers involving at most ten pilots within 8 LY of
  Turnur or Kurniainen, with a 15-minute ping-age limit and 30-minute
  `@Roamers` cooldown. Qualifying
  killmails still post when their ping is stale or cooldown-suppressed.

### Fixed

- Preserve contract context and enrichments through lifecycle changes and
  delivery retries; replay pending deliveries after restarts and recover from
  extended collection outages without flooding channels with old listings.
- Preserve omitted sovereignty subscription settings on partial updates.
- Resolve sovereignty alert system and region names before posting instead
  of displaying an unknown region.
- Replace killmail battle-report links with WarBeacon related battle reports.
- Skip missing historical R2Z2 sequence files one at a time instead of stalling
  or resyncing to the current feed head during checkpoint rewind recovery.

### Operator updates

- Add PostgreSQL-backed contract, sovereignty, and watchlist feeds, enabled by
  `CONTRACT_DATABASE_URL`; killmail subscriptions remain JSON-backed. See the
  [contract runbook](docs/contract-intelligence.md) and
  [sovereignty/watchlist runbook](docs/sov-and-watchlist-feeds.md) for setup.
- Add persisted health snapshots and an independent Discord watchdog with a
  persistent health view and consolidated operator incident alerts. Feed health
  includes delivery failures and collection progress, with watchlist thresholds
  sized to its hourly cadence.
- Share ESI caching and request limits across the new feeds, bound regional
  collection, and pace unresolved contract recovery to respect API limits.

## Published releases

- [v0.1.1 — 2024-09-30](https://github.com/ocn/Picket/releases/tag/v0.1.1)
- [v0.1.0 — 2024-02-24](https://github.com/ocn/Picket/releases/tag/v0.1.0)

[Unreleased]: https://github.com/ocn/Picket/compare/v0.1.1...HEAD
