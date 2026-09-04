# Changelog

## [Unreleased]

### Added

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

- Skip missing historical R2Z2 sequence files one at a time instead of stalling
  or resyncing to the current feed head during checkpoint rewind recovery.
