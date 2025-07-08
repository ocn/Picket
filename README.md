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
- [Filtering System](#filtering-system)
- [Manual Configuration](#manual-configuration)
- [Development](#development)
- [Contact](#contact)
- [License](#license)

## Usage

The primary way to use the bot is by inviting it to your Discord server and using slash commands to create and manage subscriptions.

### 1. Invite the Bot
Use the following link to add the bot to your server:

[**Invite zk-activity Bot**](https://discordapp.com/api/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=149504&scope=bot) 
*(Note: The self-hosting owner will need to replace `YOUR_CLIENT_ID` with their bot's actual client ID).*

### 2. Create a Subscription
In the channel where you want to receive killmails, use the `/subscribe` command. This command allows you to build a filter using its various options.

**Example:** To track all kills of Dreadnoughts (group ID 485) and Marauders (group ID 547) in the Devoid region (ID 10000030), you would use:
```
/subscribe filter_json:{"Condition":{"And":[{"Condition":{"Region":[10000030]}},{"Condition":{"ShipGroup":[485,547]}}]}}
```

When you create your first subscription, the bot will automatically generate a configuration file on the host server named `[your_server_id].json` (e.g., `123456789012345678.json`). All subsequent subscriptions for that server will be managed through in-Discord commands.

## Commands

| Command         | Description                                                                                              |
| --------------- | -------------------------------------------------------------------------------------------------------- |
| `/subscribe`    | Creates a new killmail subscription for the current channel using a flexible JSON-based filter.          |
| `/unsubscribe`  | Removes a subscription from the current channel. You will be prompted to choose which one to remove.     |
| `/subscriptions`| Lists all active subscriptions for the current channel.                                                  |

The `/subscribe` command takes a single, powerful `filter_json` argument where you define your filter rules. See the [Filtering System](#filtering-system) section for details on how to construct this JSON.

## Filtering System

The filtering logic is built around `FilterNode` objects, which can be nested to create complex rules. You provide these rules in the `filter_json` argument of the `/subscribe` command.

### Filter Nodes

-   `And`: All child nodes must pass for the filter to be met.
-   `Or`: At least one child node must pass.
-   `Not`: Inverts the result of its child node.
-   `Condition`: A specific filter rule to evaluate.

### Available Conditions

| Condition      | Description                                                              | Example                                           |
| -------------- | ------------------------------------------------------------------------ | ------------------------------------------------- |
| `TotalValue`   | Filter by the total ISK value of the killmail.                           | `{ "TotalValue": { "min": 10000000 } }`            |
| `DroppedValue` | Filter by the value of items that dropped in the wreck.                  | `{ "DroppedValue": { "max": 5000000 } }`           |
| `Region`       | Match if the kill occurred in one of the specified region IDs.           | `{ "Region": [10000002, 10000043] }`               |
| `System`       | Match if the kill occurred in one of the specified system IDs.           | `{ "System": [30000142] }`                         |
| `Security`     | Match if the system's security status is within the inclusive range.     | `{ "Security": "0.1..=0.4" }`                      |
| `Alliance`     | Match if the victim or any attacker is in one of the specified alliances. | `{ "Alliance": [99005338] }`                       |
| `Corporation`  | Match if the victim or any attacker is in one of the specified corps.    | `{ "Corporation": [98389319] }`                    |
| `Character`    | Match if the victim or any attacker is one of the specified characters.  | `{ "Character": [2112625428] }`                   |
| `ShipType`     | Match if the victim's or an attacker's ship is one of the specified types. | `{ "ShipType": [19720, 17738] }`                   |
| `ShipGroup`    | Match if the ship belongs to one of the specified group IDs.             | `{ "ShipGroup": [485, 547] }`                      |
| `LyRangeFrom`  | Match if the kill is within a given light-year range of a system.        | `{ "LyRangeFrom": { "systems": [30000142], "range": 8.5 } }` |
| `IsNpc`        | Match if the kill was performed by NPCs (`true`) or players (`false`).   | `{ "IsNpc": false }`                              |
| `IsSolo`       | Match if the kill was a solo kill.                                       | `{ "IsSolo": true }`                              |
| `Pilots`       | Filter by the number of pilots involved (victim + attackers).            | `{ "Pilots": { "min": 10, "max": 50 } }`           |
| `NameFragment` | Match if a specified string appears in a ship's name.                    | `{ "NameFragment": "shuttle" }`                   |
| `TimeRange`    | Match if the kill occurred within a specific UTC hour range.             | `{ "TimeRange": { "start": 20, "end": 4 } }`       |

### Finding IDs

To find the correct IDs for regions, systems, ships, and groups, you can use a third-party database site like [**EVE Ref**](https://everef.net/type) or Dotlan. For character, corporation, and alliance IDs, zKillboard is an excellent resource.

## Manual Configuration

For advanced users or for migrating configurations, you can manually edit the JSON files located in the `config/` directory.

-   Each server (guild) gets its own configuration file named after its ID (e.g., `config/123456789012345678.json`).
-   This file contains an array of subscription objects.

### Example `[guild_id].json`

```json
[
  {
    "id": "capital-watch-devoid",
    "description": "Alerts for killmails valued over 5M ISK, involving specific capital or battleship groups, in the Devoid region (lowsec only).",
    "action": {
      "channel_id": "YOUR_DISCORD_CHANNEL_ID"
    },
    "filter": {
      "And": [
        { "Condition": { "TotalValue": { "min": 5000000 } } },
        { "Condition": { "Region": [ 10000030 ] } },
        { "Condition": { "ShipGroup": [ 485, 547, 1538, 30 ] } },
        { "Condition": { "Security": "0.0001..=0.4999" } }
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
2.  Copy `env.sample` to `.env` and add your Discord bot token and client ID.
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
