# zk-activity

[![TypeScript](https://badges.frapsoft.com/typescript/code/typescript.svg?v=101)](https://github.com/ellerbrock/typescript-badges/)
<img alt="Github License" src="https://img.shields.io/github/license/ocn/zk-activity" />
<img alt="GitHub Last Commit" src="https://img.shields.io/github/last-commit/ocn/zk-activity" />
<img alt="GitHub Commit Activity (Month)" src="https://img.shields.io/github/commit-activity/m/ocn/zk-activity" />
<img alt="GitHub Contributors" src="https://img.shields.io/github/contributors/ocn/zk-activity" />
<img alt="GitHub Issues" src="https://img.shields.io/github/issues/ocn/zk-activity" />

<div style="display: flex; justify-content: center;">
  <img src="https://i.imgur.com/QmHC1Yx.png"  style="height: 100%; max-height: 150px;" /> 
</div>

zk-activity is a bot that brings EVE Online killmails from zkillboard.com into your Discord channel. It provides a way to filter the incoming mails according to your preferences. 

The bot operates by subscribing to a data feed from zkillboard.com and processes the incoming data based on the filters that you configure. This allows for monitoring of specific regions of space for activity involving certain classes of ships or tracking a particular group of pilots.  

The bot's versatility allows for tracking of any killmails involving specific categories such as capitals, structures, structure modules, or fighters within a specific region and time frame. The filters can be combined to achieve nearly any sort of tracking of a person, corporation, alliance, ship-type, fleet size, or location that you desire.

<p float="left">
  <img src="https://i.imgur.com/gTIwRwx.png"  style="width: 49%; max-width: 500px;" />
  <img src="https://i.imgur.com/xFL3aoh.png"  style="width: 49%; max-width: 500px;" /> 
</p>

To use the bot, you invite it to your Discord server using a provided Discord invite URL link. Once the bot is on your server, you can set up feeds in a Discord channel. The bot will then start delivering killmails from EVE Online to your Discord channel based on the filters you've set up.

---

[Invite the zk-activity Discord bot to your Discord server here.](https://discordapp.com/api/oauth2/authorize?client_id=00000000000&permissions=149504&scope=bot)

---

This test bot normally runs the latest development branch build. There are no guarantees of uptime or stability. 

Data from this test bot is not often wiped, however there are no guarantees about data loss or recovery!

## Table of Contents

- [Getting Started](#getting-started)
- [Commands](#commands)
- [Examples](#examples)
- [Development](#development)
- [Contact](#contact)
- [License](#license)

## Getting Started

To use this bot, you can either host it yourself or contact the developer for hosting at a PLEX cost.

### Self-Hosting

If you choose to host the bot yourself, follow these steps:

1. Clone this repository to your local machine and `cd` into it.

#### Manual build
2. Install the required dependencies by running `yarn install` or `npm install`.
3. Copy the `env.sample` file to `.env` and fill out the required parameters.
4. Run `docker-compose up -d` to start the bot.

#### Docker + Docker Compose
2. Run `docker build -t ocn-killbot:latest . && docker-compose down && docker-compose up -d && sleep 2 && docker-compose logs`

## Commands

| key                          | description                                                                                                |
|------------------------------|------------------------------------------------------------------------------------------------------------|
| /zkill-subscribe public [id] | Subscribe to the public feed with various filtering options. Parameters:                                   |
|                              | - `id`: ID for public feed (required)                                                                      |
|                              | - `min_value`: Minimum ISK to show the entry (optional)                                                    |
|                              | - `limit_included_ship_ids`: Limit to certain ship IDs (comma-separated, optional)                         |
|                              | - `limit_excluded_ship_ids`: Exclude certain ship IDs (comma-separated, optional)                          |
|                              | - `limit_character_ids`: Limit to certain character IDs (comma-separated, optional)                        |
|                              | - `limit_corporation_ids`: Limit to certain corporation IDs (comma-separated, optional)                    |
|                              | - `limit_alliance_ids`: Limit to certain alliance IDs (comma-separated, optional)                          |
|                              | - `limit_faction_ids`: Limit to certain faction IDs (comma-separated, optional)                            |
|                              | - `limit_region_ids`: Limit to certain region IDs (comma-separated, optional)                              |
|                              | - `limit_security_max_inclusive`: Inclusive limit to a maximum security (optional)                         |
|                              | - `limit_security_max_exclusive`: Exclusive limit to a maximum security (optional)                         |
|                              | - `limit_security_min_inclusive`: Inclusive limit to a minimum security (optional)                         |
|                              | - `limit_security_min_exclusive`: Exclusive limit to a minimum security (optional)                         |
|                              | - `required_name_fragment`: Require a name fragment in the name of the matched type IDs (optional)         |
|                              | - `inclusion_limit_compares_attackers`: Consider attackers when sending mails (optional)                   |
|                              | - `inclusion_limit_compares_attacker_weapons`: Consider attackers' weapons when sending mails (optional)   |
|                              | - `exclusion_limit_compares_attackers`: Consider attackers when rejecting mails (optional)                 |
|                              | - `exclusion_limit_compares_attacker_weapons`: Consider attackers' weapons when rejecting mails (optional) |
| /zkill-unsubscribe all       | Make the bot not post any on this channel anymore                                                          |
| /zk-activity-diag            | Display the current channel's list of subscriptions                                                        |

## Examples

This bot allows you to set up feeds and limit types to track regional data for specific ships by their group identifiers. In this example, a subscription is set up for a public feed with an ID of 12345. The limit types are set to a minimum ISK value of 5000000, a region ID of 10000002 (The Forge), and a ship ID of 670 (Caldari Shuttle).

Here's how you can do it:

### Setting up a Feed

To set up a feed, you use the `/zkill-subscribe` command followed by the type of feed and the ID. For example, to subscribe to a public feed with an ID of 12345, you would use:

```
/zkill-subscribe public 12345
```

### Setting up Limit Types

You can also set up limit types when subscribing to a feed. Limit types allow you to filter the incoming mails according to your preferences. For example, to subscribe to a public feed with an ID of 12345 and set a minimum ISK value of 5000000, you would use:

```
/zkill-subscribe public 12345 min_value=5000000
```

### Tracking Regional Data for Specific Ships

To track regional data for specific ships, you can use the `limit_region_ids` and `limit_included_ship_ids` parameters. For example, to subscribe to a public feed with an ID of 12345, limit it to region ID 10000002 (The Forge), and include ship ID 670 (Caldari Shuttle), you would use:

```
/zkill-subscribe public 12345 limit_region_ids=10000002 limit_included_ship_ids=670
```


### Finding IDs for Tracking

To find the IDs for tracking specific ships, items, or groups, you can use the resources available on [everef.net](https://everef.net) and [zkillboard.com](https://zkillboard.com).

For ship and item IDs, navigate to [everef.net](https://everef.net). Here, you can browse through the Market and Ship groups to find the specific items or ships you want to track. Once you've found the item or ship, the ID can be found in the URL of the item or ship's page. This ID corresponds to the group ID for that particular ship or item.

For character, corporation, or alliance IDs, use the search feature on [zkillboard.com](https://zkillboard.com). Enter the name of the character, corporation, or alliance in the search bar. Once you've found the correct entity, the ID can be found at the end of the URL on the entity's page. This ID can be used to track activity related to that specific character, corporation, or alliance.

<img src="./docs/id.png" width=900>

Remember to replace the placeholders in the commands with the actual IDs you've found using these methods.

### Tracking Ship Categories

When you specify the ID of a particular ship from the game, the bot will track not only that specific ship but also the entire group to which that ship belongs. This means that if you specify a ship that belongs to a category such as Dreadnoughts or Control Towers, the bot will track all ships within that category, including racial and faction equivalents.

For example, if you specify the ID of a Moros (a Gallente Dreadnought), the bot will track all Dreadnoughts, including the Revelation (Amarr), Phoenix (Caldari), and Naglfar (Minmatar), as well as any faction Dreadnoughts.

To track a category of ships, you can use the `limit_included_ship_ids` parameter with the ID of any ship from the category. For example, to subscribe to a public feed with an ID of 12345, limit it to region ID 10000002 (The Forge), and track all Dreadnoughts, you would use:

```
/zkill-subscribe public 12345 limit_region_ids=10000002 limit_included_ship_ids=19720
```

In this example, 19720 is the ID of the Moros. Please replace the IDs in the example with the actual IDs and additional filter flags that you want to use.

### Using a JSON Config File

You can also use a JSON config file to load and store your subscriptions. Here's an example config file:

```json
{
  "subscriptions": [
    {
      "type": "public",
      "id": 12345,
      "limit_types": {
        "min_value": 5000000,
        "limit_region_ids": [10000002],
        "limit_included_ship_ids": [670]
      }
    }
  ]
}
```

To load this config file, you would use the `withConfig` method in your code:

```typescript
ZKillSubscriber.getInstance().withConfig('./path/to/your/config.json');
```

Replace `'./path/to/your/config.json'` with the actual path to your config file.

Please replace the IDs in the examples with the actual IDs and additional filter flags that you may want to use.

### Tracking Multiple Ships

To track multiple ships, you can use the `limit_included_ship_ids` parameter with multiple IDs separated by commas. For example, to subscribe to a public feed with an ID of 12345, limit it to region ID 10000002 (The Forge), and include ship IDs 670 (Caldari Shuttle) and 671 (Gallente Shuttle), you would use:

### Filtering by Name Fragment

You can filter the incoming mails by a name fragment. This can be useful if you want to track activity related to specific entities whose names contain a certain string. For example, to subscribe to a public feed with an ID of 12345 and require the name fragment "Caldari" in the name of the matched type IDs, you would use:

```
/zkill-subscribe public 12345 required_name_fragment=Caldari
```

### Filtering by Security Status

You can also filter the incoming mails by the security status of the solar system where the activity took place. This can be useful if you want to track activity in highsec, lowsec, or nullsec space. For example, to subscribe to a public feed with an ID of 12345 and set an inclusive limit to a maximum security of 0.5 (lowsec and nullsec only), you would use:

```
/zkill-subscribe public 12345 limit_security_max_inclusive=0.5
```

You will track all kills in lowsec and nullsec space.

To set an exclusive limit to a maximum security of 0.0 (lowsec), you would use:

```
/zkill-subscribe public 12345 limit_security_max_inclusive=0.5 limit_security_max_exclusive=0.0
```

You will track all kills only in lowsec space.

To filter only for kills in nullsec or highsec space, you can only use the inclusive upper limit to a minimum security of 0.0:

```
/zkill-subscribe public 12345 limit_security_max_inclusive=0.0
```

### Filtering by Character & Group affiliations

You can filter the incoming mails by character, corporation, or alliance. This can be useful if you want to track activity related to a specific group of pilots. 

For example, to subscribe to a public feed with an ID of 12345, limit it to region ID 10000002 (The Forge), and track kills by an alliance called BLACKFLAG, you would use:

```
/zkill-subscribe public 12345 limit_region_ids=10000002 limit_alliance_ids=99010015
```

Please replace the IDs and alliance name in the example with the actual IDs and alliance name that you want to use.

## Development

This application is written in TypeScript and utilizes the zkillboard webhook endpoint and discord.js. It is containerized using Docker, and orchestrated with Docker Compose for ease of development and deployment.

### Requirements:

- Docker
- Docker Compose

### Setup:

1. Clone this repository to your local machine.
2. Copy the `env.sample` file to `.env` and fill out the required parameters.
3. Run `docker-compose up -d` to start the application in detached mode.

### Building the Docker Image:

To build the Docker image for this application, run the following command:

```
docker build -t zk-activity:latest .
```

This will build a Docker image with the tag `zk-activity:latest`.

### Configuration:

Configuration for this application is handled through environment variables, which can be set in the `.env` file.

#### Environment Variables:

| Key                  | Description                        |
|----------------------|------------------------------------|
| DISCORD_BOT_TOKEN    | Your Discord bot token             |
| DISCORD_CLIENT_ID    | Your Discord application client ID |


#### Example .env file

```shell
DISCORD_BOT_TOKEN=your_discord_bot_token
DISCORD_CLIENT_ID=your_discord_client_id
```

Please replace the placeholders with your actual Discord bot token and application client ID.

## Contact

This bot is a derivative of [hazardous](https://github.com/SvenBrnn/hazardous-killbot).

For any inquiries or if you need assistance with hosting the bot, please contact the developer at [this public email address](mailto:wands.larch.0y@icloud.com?subject=[GitHub]).

## License 
Copyright 2024 ocn

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
