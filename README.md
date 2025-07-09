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
-   `ping_type`: Ping `@here` or `@everyone` for a match.                                                                                                                    
-   `max_ping_delay_minutes`: The maximum age of a killmail (in minutes) to be eligible for a ping.

### `/unsubscribe`
Removes a subscription from the current channel.
-   `id` (Required): The unique ID of the subscription to remove.

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

## Development

This application is written in Rust and containerized using Docker.

### Requirements:

-   Docker
-   Docker Compose

### Setup:

1.  Clone this repository.
2.  Copy `docs/env.sample` to `.env` and add your Discord bot token and client ID.
3.  Run `docker-compose up --build -d` to start the application.

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
