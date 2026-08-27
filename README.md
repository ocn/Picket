# zk-activity

[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)
<img alt="Github License" src="https://img.shields.io/github/license/ocn/zk-activity" />
<img alt="GitHub Last Commit" src="https://img.shields.io/github/last-commit/ocn/zk-activity" />
<img alt="GitHub Commit Activity (Month)" src="https://img.shields.io/github/commit-activity/m/ocn/zk-activity" />

<div style="display: flex; justify-content: center;">
  <img src="https://i.imgur.com/QmHC1Yx.png"  style="height: 100%; max-height: 150px;" /> 
</div>

**zk-activity** is a Discord bot, written in Rust, that brings EVE Online killmails from zkillboard.com into your Discord channels. It provides a powerful and flexible filtering system to ensure you only see the activity that matters to you.

The bot operates by subscribing to a data feed from zkillboard.com and processes incoming killmails against a set of rules you define. This allows for precise monitoring of specific regions, alliances, ship classes, or even fleet compositions.

<p float="left">
  <img src="https://i.imgur.com/gTIwRwx.png"  style="width: 49%; max-width: 500px;" />
  <img src="https://i.imgur.com/xFL3aoh.png"  style="width: 49%; max-width: 500px;" /> 
</p>

---

## Table of Contents

- [Usage](#usage)
- [Commands](#commands)
- [Advanced Examples](#advanced-examples)                                                                                                                                    
- [Manual Configuration](#manual-configuration)
- [Global Public Contract Intelligence](#global-public-contract-intelligence)
- [Development](#development)
- [Contact](#contact)
- [License](#license)

## Usage

The primary way to use the bot is by inviting it to your Discord server and using slash commands to create and manage subscriptions.

### 1. Invite the Bot
Use the following link to add the bot to your server. The owner of the bot will need to replace `YOUR_CLIENT_ID` with their bot's actual client ID.

[**Invite zk-activity Bot**](https://discordapp.com/api/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=149504&scope=bot) 

### 2. Create a Subscription
In the channel where you want to receive killmails, use the `/subscribe` command. This command allows you to combine multiple filter options to create a specific alert. All specified filters are combined with an "AND" logic—the killmail must match **all** of them to be posted.

**Example:** To track kills involving Dreadnoughts (group ID 485) and Marauders (group ID 547) in the Devoid region (ID 10000030) that are worth at least 1 billion ISK, you would use:
```
/subscribe id: cap-watch-devoid description: Capital and Marauder kills in Devoid region_ids: 10000030 ship_group_ids: 485,547 min_value: 1000000000
```

When you create your first subscription in a server, the bot will automatically generate a configuration file named `[your_server_id].json` (e.g., `123456789012345678.json`) on the host machine. All subsequent subscriptions for that server will be managed through in-Discord commands.

## Commands

### `/subscribe`
Creates or updates a killmail subscription for the current channel. All filter options are optional except for `id` and `description`.

-   `id` (Required): A unique name for the subscription (e.g., `my-first-filter`).
-   `description` (Required): A brief explanation of what the subscription does.
-   `min_value`: Minimum total ISK value.
-   `max_value`: Maximum total ISK value.
-   `region_ids`: Comma-separated list of region IDs.
-   `system_ids`: Comma-separated list of system IDs.
-   `alliance_ids`: Comma-separated list of alliance IDs.
-   `corp_ids`: Comma-separated list of corporation IDs.
-   `char_ids`: Comma-separated list of character IDs.
-   `ship_type_ids`: Comma-separated list of ship type IDs.
-   `ship_group_ids`: Comma-separated list of ship group IDs.
-   `security`: A security status range (e.g., `"-1.0..=0.4"` for low/nullsec).                                                                                              
-   `is_npc`: `True` for NPC-only kills, `False` for player-only.
-   `is_solo`: `True` for solo kills only.
-   `min_pilots`: Minimum number of pilots involved.
-   `max_pilots`: Maximum number of pilots involved.
-   `name_fragment`: A string that must appear in the ship's name.
-   `time_range_start` / `time_range_end`: A UTC hour range (0-23) for the kill.
-   `ly_ranges_json`: A JSON string for system ranges (e.g., `'[{"system_id":30000142, "range":10.0}]'`).
-   `ping_type`: Ping `@here` or `@everyone` for a match. Do not set this together with `role`.
-   `role`: Select one Discord role to ping for a match. This is a native Discord role selector; do not enter a role name or mention string. Do not set this together with `ping_type`.
-   `max_ping_delay_minutes`: The maximum age of a killmail (in minutes) to be eligible for a ping. Omit it, or set it to `0`, to allow pings for any killmail age. Stale matches are still posted without a ping.
-   `ping_cooldown_minutes`: The minimum time between pings in the current channel. It applies to `@here`, `@everyone`, and role pings. Omit it to use the five-minute default; a cooldown-suppressed match is still posted without a ping.

The cooldown is shared by every subscription in the same channel, but each channel has its own timer. Role pings mention only the selected role. The bot must be allowed to mention that role: make the role mentionable, or grant the bot Discord's `MENTION_EVERYONE` permission. See Discord's [allowed mentions documentation](https://docs.discord.com/developers/resources/message#allowed-mentions-object) for the permission behavior.

### `/unsubscribe`
Removes a subscription from the current channel.
-   `id` (Required): The unique ID of the subscription to remove.

### Contract commands

Contract subscriptions are PostgreSQL-backed and are separate from killmail subscriptions. Run these commands in the channel that should receive the contract feed.

| Command | Required arguments | Optional arguments | Result |
| --- | --- | --- | --- |
| `/contract_subscribe` | `id`, `description`, `filter`, `event_actions` | `ly_ranges_json`, `ping_type` | Creates or replaces this channel's public-contract subscription with the same `id`. |
| `/contract_unsubscribe` | `id` | None | Removes this channel's contract subscription with that `id`. |

`filter` is JSON with one `root` node. Nodes are `condition`, `and`, `or`, and `not`; the examples below use the deployed syntax. `event_actions` is JSON keyed by `listed`, `sale_confirmed`, `purchase_confirmed`, `expired`, and `closed_outcome_unknown`. Each value is `ignore`, `post`, or `post_and_ping`; omitted actions default to `ignore`.

`ly_ranges_json` is a non-empty array of system ranges, such as `[{"system_id":30002086,"range":8.0}]`. It is combined with `filter` using `AND`; multiple systems are alternatives. `ping_type:here` sends `@here` for every `post_and_ping` action in the subscription, while `ping_type:everyone` sends `@everyone`.

### `/diag`
Displays diagnostic information for all subscriptions active in the current channel.

### Finding IDs
To find the correct IDs for regions, systems, ships, and groups, you can use a third-party database site like [**EVE Ref**](https://everef.net/type) or Dotlan. For character, corporation, and alliance IDs, zKillboard is an excellent resource.

## Advanced Examples                                                                                                                                                         
                                                                                                                                                                             
### Pinging for Capital Kills Near a Staging System                                                                                                                       
                                                                                                                                                                             
This example creates a subscription that pings `@everyone` if a killmail involving Capital ships occurs within 7.0 light-years of Turnur (system ID 30002086). The              
ping will only be sent if the killmail is less than 10 minutes old.                                                                                                          
                                                                                                                                                                             
```                                                                                                                                                                          
/subscribe id: capitals-radar description: Capitals near Turnur ship_group_ids: 485 ly_ranges_json: [{"system_id":30002086, "range":7.0}] ping_type: Everyone
max_ping_delay_minutes: 10
```                                                                                                                                                                          
                                                                                                                                                                             
### Monitoring Nullsec for Specific Alliances                                                                                                                                
                                                                                                                                                                             
This example tracks activity in nullsec (`-1.0` to `0.0`) involving either Pandemic Horde (alliance ID 498125261) or Goonswarm Federation (alliance ID                   
1354830081) in the Curse (region ID 10000012) region.
                                                                                                                                                                             
```                                                                                                                                                                          
/subscribe id: nullsec-blocs description: Horde vs. Goons activity in nullsec security: "-1.0..=0.0" alliance_ids: 498125261,1354830081 region_ids: 10000012
```                                                                                                                                                                          


## Manual Configuration

For advanced users or for migrating configurations, you can manually edit the JSON files located in the `config/` directory. The bot automatically creates a file named `[guild_id].json` for each server where a subscription is made.

### Example `[guild_id].json`
This JSON structure is equivalent to the example `/subscribe` command shown above in step 2 of the usage guide.

```json
[
  {
    "id": "cap-watch-devoid",
    "description": "Capital and Marauder kills in Devoid",
    "action": {
      "channel_id": "YOUR_DISCORD_CHANNEL_ID"
    },
    "filter": {
      "And": [
        { "Condition": { "Region": [ 10000030 ] } },
        { "Condition": { "ShipGroup": [ 485, 547 ] } },
        { "Condition": { "TotalValue": { "min": 1000000000 } } }
      ]
    }
  }
]
```

## Global Public Contract Intelligence

The contract feed follows the same category model as the kill feeds: global feeds watch the configured class everywhere, and the Turnur/Kurniainen feed watches the configured class within range. It is an optional PostgreSQL-backed feed and does not migrate or replace the JSON-backed killmail configuration.

### What contract feeds monitor

These feeds do not show every contract listing. They monitor public `item_exchange` contracts whose offered or requested ship items match the configured ship groups, lifecycle action, and, where configured, location and light-year range. The source is the public [EVE Swagger Interface (ESI)](https://developers.eveonline.com/docs/services/esi/overview/).

They do not monitor private personal, corporation, or alliance contracts, or contract types other than public item exchanges. The collector scans the 114 region IDs returned by ESI and does not exclude wormhole regions. Public ESI does not provide a counterparty for these records, so the feed does not infer or display one.

The lifecycle actions are:

| Action | Meaning |
| --- | --- |
| `listed` | A matching public contract was observed. |
| `sale_confirmed` | Acceptance of an offered-item contract was confirmed with public ESI evidence. |
| `purchase_confirmed` | Acceptance of a requested-item contract was confirmed with public ESI evidence. |
| `expired` | The contract expired without confirmed acceptance evidence. |
| `closed_outcome_unknown` | The contract is no longer public, but the collector could not confirm why it closed. |

`sale_confirmed` and `purchase_confirmed` are not sent just because a listing disappears. The collector requires acceptance evidence. A `closed_outcome_unknown` post only says that the listing stopped being public.

### Live feed configurations

The following are the three active feeds. The JSON is the current deployed configuration, formatted for direct slash-command use.

#### Global supercaps

Posts all lifecycle actions for public titan and supercarrier contracts in every ESI region the collector scans. It does not ping.

```text
/contract_subscribe id:supercap-global description:Global public titan and supercarrier contracts filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"ids":[30,659],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[30,659],"direction":"requested"}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post","expired":"post","closed_outcome_unknown":"post"}
```

#### Global capitals

Posts all lifecycle actions for public contracts containing the configured capital ship groups in every ESI region the collector scans. It does not ping.

```text
/contract_subscribe id:capital-global description:Global public capital contracts filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"ids":[4594,485,1538,547,883,902,513],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[4594,485,1538,547,883,902,513],"direction":"requested"}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post","purchase_confirmed":"post","expired":"post","closed_outcome_unknown":"post"}
```

#### Supercaps within 8 LY of Turnur or Kurniainen

Posts the same lifecycle actions for public titan and supercarrier contracts within 8 LY of Turnur (30002086) or Kurniainen (30003089). Only confirmed sale and purchase actions ping `@here`.

```text
/contract_subscribe id:supercap-turnur-kurniainen-8ly description:Public supercapital contracts within 8 ly of Turnur or Kurniainen filter:{"root":{"and":[{"condition":{"event_kinds":["listed","sale_confirmed","purchase_confirmed","expired","closed_outcome_unknown"]}},{"or":[{"condition":{"ship_groups":{"ids":[30,659],"direction":"offered"}}},{"condition":{"ship_groups":{"ids":[30,659],"direction":"requested"}}}]}]}} event_actions:{"listed":"post","sale_confirmed":"post_and_ping","purchase_confirmed":"post_and_ping","expired":"post","closed_outcome_unknown":"post"} ly_ranges_json:[{"system_id":30002086,"range":8.0},{"system_id":30003089,"range":8.0}] ping_type:here
```

Use `/contract_unsubscribe id:<id>` in that feed's channel to stop one of these feeds. See [the operator runbook](docs/contract-intelligence.md) for the complete filter grammar and operations details.

## Sovereignty Timer Feed

The sov timer feed watches public sovereignty campaigns and posts to subscribed channels when one appears, when a configured T-minus mark is reached, and computes whether the campaign's system is reachable from a home system (Turnur by default) over stargates and, optionally, the current Wanderer wormhole chain. It shares the contract feed's PostgreSQL database and only starts when `CONTRACT_DATABASE_URL` is configured. Subscriptions use `/sov_subscribe` (with `max_jumps`, 1-11, and `allow_frigate_holes` convenience options) and `/sov_unsubscribe`; `/sov_timers` lists the live campaigns matching a channel's subscriptions on demand.

Reachability is computed by breadth-first search over `config/stargates.json`, a static undirected stargate adjacency map generated from the SDE `mapSolarSystemJumps` table and committed to the repository. The home system defaults to Turnur (30002086) and is overridden with `SOV_HOME_SYSTEM_ID`. If the file is missing or fails to parse, the bot logs an error and keeps running: `Reachable` filter leaves never match and `/sov_timers` reports the graph as unavailable, rather than the process failing to start.

### Wanderer chain reachability

When `WANDERER_BASE_URL`, `WANDERER_MAP`, and `WANDERER_MAP_API_KEY` are all set, the bot additionally polls the alliance's Wanderer map every two minutes (read-only: the client type has no write methods, even though the map API key itself is write-capable on Wanderer's side) and merges the current chain's traversable connections into the same stargate graph, so a campaign beyond gate range becomes reachable when the chain opens a door to it. Two routes are cached per chain snapshot -- with and without frigate-sized holes -- selected by the `Reachable` leaf's `allow_frigate_holes` flag. Critical-mass holes never traverse; every end-of-life bucket traverses (it is a risk fact, not a filter); gate/bridge connections tracked on the map traverse as one jump. When the route crosses at least one wormhole, the alert embed and `/sov_timers` show a Path Risk line (`worst hole: EOL <4h, mass <50%`); a pure gate route shows no such line. On a Wanderer error the last good snapshot keeps serving reachability; it is reported stale after ten minutes with no successful fetch (surfaced in `/sov_timers` as `chain: stale since <t>`, or `chain: not configured` when the three variables above are not all set), and in the Runtime Health Snapshot as `sov_chain_progress`. Any of the three variables missing simply keeps reachability stargate-only, exactly as before this feature existed.

### Sov timer subscription filter grammar

`/sov_subscribe filter:` accepts one JSON document: `{"root": <node>}`. A node is `{"condition": <leaf>}`, `{"and": [<node>, ...]}`, `{"or": [<node>, ...]}`, or `{"not": <node>}`. Leaf tag names are the `SovFilterCondition` variants exactly as serialized in `src/sov_feed/model.rs` (`#[serde(rename_all = "snake_case")]`):

| Leaf tag | Shape | Bounds |
| --- | --- | --- |
| `vulnerable_within` | `{"hours": N}` | 1-720 (30 days) |
| `defender` | `{"alliance_ids": [N, ...]}` | non-empty, positive IDs |
| `region` | `[N, ...]` | non-empty, positive region IDs |
| `system` | `[N, ...]` | non-empty, positive system IDs |
| `event_type` | `["tcu_defense" \| "ihub_defense" \| "station_defense" \| "station_freeport", ...]` | non-empty |
| `reachable` | `{"max_jumps": N, "allow_frigate_holes": bool}` | `max_jumps` 1-11; `allow_frigate_holes` defaults to `false` and, when `true`, admits frigate-sized wormhole connections on the Wanderer chain route |

Two complete examples (pinned by a round-trip unit test in `src/sov_feed/model.rs`, `readme_sov_subscribe_filter_examples_round_trip`):

Defense events starting within 12h in a system reachable within 8 jumps, including frigate-sized holes on the chain:

```text
{"root":{"and":[{"condition":{"vulnerable_within":{"hours":12}}},{"condition":{"reachable":{"max_jumps":8,"allow_frigate_holes":true}}}]}}
```

Campaigns in Jita that are not station freeports:

```text
{"root":{"and":[{"condition":{"system":[30000142]}},{"not":{"condition":{"event_type":["station_freeport"]}}}]}}
```

`/sov_subscribe` also takes convenience options instead of hand-writing the equivalent leaf: `region_id`, `defender_alliance_id`, `max_jumps`, and `allow_frigate_holes` (requires `max_jumps` also be set) are each ANDed onto the `filter` root as an extra `region`/`defender`/`reachable` leaf when supplied. `tminus_marks` is a separate per-subscription option (comma-separated T-minus minutes; default 120,30), not a filter leaf.

### Refreshing the stargate graph

Stargates essentially never move, so this only needs to run again after a rare SDE release that changes the map (e.g. a new region, a Pochven-style restructuring):

```shell
cargo run --bin build_stargate_graph
```

This fetches the [Fuzzwork SDE dump index page](https://www.fuzzwork.co.uk/dump/latest/) (to discover an SDE version stamp) and `mapSolarSystemJumps.csv` from the same mirror -- no other files -- then overwrites `config/stargates.json` with the parsed adjacency plus `sde_version` (the discovered stamp, or `unknown` if none was found), `source_last_modified` (the CSV response's `Last-Modified` header), `generated_at` (this run's timestamp), and the resulting `system_count`/`edge_count`. Review the diff -- `system_count`/`edge_count` should move by at most a handful of systems for an ordinary release -- and commit it like any other generated config file (`config/systems.json`, `config/ships.json`, ...).

## Development

This application is written in Rust and containerized using Docker.

### Requirements:

-   Docker
-   Docker Compose

### Setup:

1.  Clone this repository.
2.  Copy `docs/env.sample` to `.env` and add your Discord bot token and client ID.
3.  Run `docker-compose up --build -d` to start the application.

For the optional authenticated Contract Intelligence structure resolver, run `ENV_FILE=.env scripts/structure-resolver-provision.sh`. It performs dedicated-character consent, validates the returned authorization, probes known structures, and writes only the required runtime variables; see the [operator runbook](docs/contract-intelligence.md#optional-structure-resolver).

#### Example `.env` file

```shell
DISCORD_BOT_TOKEN=your_discord_bot_token
DISCORD_CLIENT_ID=your_discord_client_id
```

## Contact

This bot is a derivative of [hazardous-killbot](https://github.com/SvenBrnn/hazardous-killbot).

For any inquiries, please contact the developer at [this public email address](mailto:wands.larch.0y@icloud.com?subject=[GitHub]).

## License

This project is licensed under the MIT License. See the [LICENSE.md](LICENSE.md) file for details.
