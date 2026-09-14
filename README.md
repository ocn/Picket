# Picket

Picket: EVE Online intel feeds for Discord.

[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)
<img alt="Github License" src="https://img.shields.io/github/license/ocn/Picket" />
<img alt="GitHub Last Commit" src="https://img.shields.io/github/last-commit/ocn/Picket" />
<img alt="GitHub Commit Activity (Month)" src="https://img.shields.io/github/commit-activity/m/ocn/Picket" />

<img alt="Picket: a small ship holding beside a stargate, wood-engraving style" src="docs/brand/banner.jpg" width="100%" />

Picket watches zKillboard killmails, ESI sov campaigns, public contracts, and corp or alliance rosters, and posts only what a channel's filters ask for.

The setup that gets used most: @here when a dread, carrier, FAX, or Rorqual lands on a killmail within a few light-years of your staging, @everyone when it's a super or titan, anyone you've set blue skipped, and no ping at all once the kill is more than five minutes old.

A picket is the ship you leave on a gate to call what comes through. That's the whole idea here. You say what counts as worth calling, per channel, and the bot holds the gate.

Picket is built and run by an EVE Online Partner. If it earns a place in your server, use creator code DRAC at eveonline.com/checkout.

<p align="center"><img alt="EVE Online Partner" src="docs/brand/eve-online-partner.png" height="120"></p>

| Loss (structure Subscription, victim side) | Kill (ship group Subscription, attacker side) |
| --- | --- |
| ![A Keepstar loss embed](docs/screenshots/01-killfeed-loss-structure.png) | ![Dreadnoughts killing a Phoenix](docs/screenshots/02-killfeed-kill-dreads.png) |

| Radar ping (light-year range, @here) | Type-Tracked Ship (title names the hull) |
| --- | --- |
| ![A radar ping with @here](docs/screenshots/03-killfeed-radar-ping.png) | ![A type-tracked Thanatos kill](docs/screenshots/04-killfeed-type-tracked.png) |

## Get it

**Invite the public instance.** It runs the Killfeed only. [Invite Picket](https://discordapp.com/api/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=149504&scope=bot), then run `/subscribe` in the channel that should receive kills.

**Or self-host.** Every Feed, on your own box:

```shell
git clone https://github.com/ocn/Picket && cd Picket
cp docs/env.sample .env        # add DISCORD_BOT_TOKEN and DISCORD_CLIENT_ID
docker compose up --build -d
```

The Killfeed works with just the two Discord variables. The Contract, Sov Timer, and Watchlist Feeds need PostgreSQL (`CONTRACT_DATABASE_URL`) and start only when it is set. Each Feed section below says what else it needs.

Every subscribe, unsubscribe, and watchlist command needs the Manage Server permission and cannot be used in DMs. Server admins can loosen that per role under Server Settings, Integrations, the bot, Command permissions.

## Feeds

A Feed is a kind of event Picket can watch. A Subscription is one filter and one action bound to one channel. A channel can hold as many Subscriptions as you like, across Feeds.

### Killfeed

Watches every killmail zKillboard publishes, live, and posts the ones a channel's Subscriptions match. A match on the attacker side is a kill (green bar); on the victim side, a loss (red bar). The message line above the embed states what died and where, so phone notifications read right without opening it.

| Command | Does |
| --- | --- |
| `/subscribe` | Creates or replaces a Subscription in this channel. `id` and `description` are required; every filter is optional and they AND together. |
| `/unsubscribe id:<id>` | Removes it. |
| `/diag` | Shows every Subscription active in this channel and what it matched recently. |
| `/find_unsubscribed` | Lists channels in this server with no Subscription at all. |
| `/sync_standings` | Authorises one character and attaches its corp or alliance contacts to a Subscription, so anyone set blue is skipped. |
| `/sync_remove id:<id>` | Detaches the standings from that Subscription. |
| `/sync_clear` | Deletes your saved character authorisation and contact lists. |

Filters: ISK value, region, system, security band, alliance, corporation, character, ship type, ship group, `target` (apply the entity and ship filters to attackers, the victim, or either), name fragment, pilot count, NPC or solo, UTC hour range, and light-year range from one or more systems. Pings: `@here`, `@everyone`, or one role, with a maximum killmail age and a per-channel cooldown.

```text
/subscribe id: caps-radar description: Capitals within 7 LY of staging ship_group_ids: 485,547,1538,4594,883 target: attacker ly_ranges_json: [{"system_id":YOUR_SYSTEM_ID,"range":7.0}] ping_type: Here max_ping_delay_minutes: 10
```

Needs: `DISCORD_BOT_TOKEN`, `DISCORD_CLIENT_ID`. Standings sync also needs `EVE_CLIENT_ID` and `EVE_CLIENT_SECRET`.
Full reference: [docs/killfeed.md](docs/killfeed.md) (every option, JSON config format, finding IDs, more examples).

### Contract Feed

Watches public item-exchange contracts across every region ESI exposes and posts the ones whose offered or requested ships match a channel's Subscription. Each contract has a lifecycle: listed, sale confirmed, purchase confirmed, expired, closed with the outcome unknown. You choose per action whether to ignore it, post it, or post and ping. Acceptance is only reported with public ESI evidence, never inferred from a listing vanishing.

| Command | Does |
| --- | --- |
| `/contract_subscribe` | Creates or replaces a contract Subscription in this channel: `id`, `description`, a JSON `filter`, and `event_actions`; optional `ly_ranges_json` and `ping_type`. |
| `/contract_unsubscribe id:<id>` | Removes it. |

```text
/contract_subscribe id:supercaps-near-home description:Public supercap contracts within 8 LY filter:{"root":{"or":[{"condition":{"ship_groups":{"ids":[30,659],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[30,659],"direction":"requested"}}}]}} event_actions:{"listed":"post","sale_confirmed":"post_and_ping","purchase_confirmed":"post_and_ping","expired":"post","closed_outcome_unknown":"post"} ly_ranges_json:[{"system_id":YOUR_SYSTEM_ID,"range":8.0}] ping_type:here
```

Needs: PostgreSQL via `CONTRACT_DATABASE_URL`. Player-structure locations resolve only with the optional structure resolver, which uses one operator character and no contract scopes.
Full reference: [docs/contract-intelligence.md](docs/contract-intelligence.md) (filter grammar, lifecycle evidence, structure resolver, recovery and retention).

### Sov Timer Feed

Watches public sovereignty campaigns and Sovereignty Hub vulnerability windows, and posts when a campaign appears, reaches a T-minus mark, becomes reachable from home, or when a hub's window shifts into your timezone. Reachability is computed over the stargate map and, if you connect a Wanderer map, the current wormhole chain; routes through wormholes show a path-risk line.

| Command | Does |
| --- | --- |
| `/sov_subscribe` | Creates or partially updates a sov Subscription in this channel by `name`: a JSON `filter` or the convenience options `region_id`, `defender_alliance_id`, `max_jumps`, `allow_frigate_holes`; `tminus_marks`, `tz_window`, `tz_shift_enabled`; a `role` or `user` to ping; `clear` to drop stored fields. |
| `/sov_unsubscribe name:<name>` | Removes it. |
| `/sov_timers` | Lists the live campaigns matching this channel's Subscriptions. Open to every member. |

```text
/sov_subscribe name: reachable-timers filter: {"root":{"condition":{"vulnerable_within":{"hours":12}}}} max_jumps: 8 tminus_marks: 120,30
```

Needs: PostgreSQL via `CONTRACT_DATABASE_URL`; `SOV_HOME_SYSTEM_ID` for your home system; optionally `WANDERER_BASE_URL`, `WANDERER_MAP`, and `WANDERER_MAP_API_KEY` for chain reachability. `config/stargates.json` ships with the repo.
Full reference: [docs/sov-and-watchlist-feeds.md](docs/sov-and-watchlist-feeds.md) (filter grammar, partial-update rules, timezone windows, refreshing the stargate graph).

### Watchlist Feed

Watches the alliances and corporations on a server's watchlist and posts when a corporation joins or leaves a watched alliance, a member count moves 10 % against a week ago, a watched corporation changes alliance, or a war is declared by or against a watched entity. Sov Subscriptions can use the same watchlist as their defender filter.

| Command | Does |
| --- | --- |
| `/watch add kind:<alliance\|corporation> ticker:<name>` | Adds an entity to this server's watchlist. Names resolve through ESI; a bare ticker only works once it is cached. |
| `/watch remove kind:<...> ticker:<...>` | Removes it. |
| `/watch list` | Shows the watchlist. |
| `/watch_subscribe name:<name>` | Subscribes this channel. `event_kinds` defaults to the four membership kinds; wars are opt-in (`all` selects every kind). Optional `role` and `user` to ping. Replaces the Subscription wholesale each time. |
| `/watch_unsubscribe name:<name>` | Removes it. |

```text
/watch add kind: alliance ticker: Snuffed Out
/watch_subscribe name: hostile-rosters event_kinds: all
```

Needs: PostgreSQL via `CONTRACT_DATABASE_URL`. Polls hourly through the shared ESI limiter; the first snapshot of any entity is a silent baseline.
Full reference: [docs/sov-and-watchlist-feeds.md](docs/sov-and-watchlist-feeds.md) (event kinds, war scanning, cost and retention).

## Operating it

| Command | Does |
| --- | --- |
| `/health` | Shows the persisted runtime health snapshot. Answers only the configured operator. |
| `/ping` | Checks the bot is up. |

The [contract runbook](docs/contract-intelligence.md) and the [sov and watchlist runbook](docs/sov-and-watchlist-feeds.md) cover start-up verification, recovery, and retention. Screenshots in this README are rendered from the bot's real payloads with `scripts/render-screenshots.mjs`.

## Development

Rust, Tokio, Serenity 0.11, sqlx. `cargo test` runs the unit and integration tests; the embed tests that post to Discord are `#[ignore]` and need `.env`. See `CLAUDE.md` for the module map and conventions.

## Contact

Picket is a derivative of [hazardous-killbot](https://github.com/SvenBrnn/hazardous-killbot). Questions to [this public email address](mailto:wands.larch.0y@icloud.com?subject=[Picket]).

## License

MIT. See [LICENSE.md](LICENSE.md).

"EVE", "EVE Online", "CCP", and all related logos and images are trademarks or registered trademarks of CCP hf. Picket is a third-party tool and is not endorsed by CCP.
