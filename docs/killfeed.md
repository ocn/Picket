# Killfeed reference

Everything the README summarises for the Killfeed, in full: every `/subscribe` option, the JSON configuration format, worked examples, and how to find IDs. Moved here from the README.

## Creating a Subscription

In the channel where you want to receive killmails, use the `/subscribe` command. This command allows you to combine multiple filter options to create a specific alert. All specified filters are combined with AND logic: the killmail must match **all** of them to be posted.

**Example:** To track kills involving Dreadnoughts (group ID 485) and Marauders (group ID 547) in the Devoid region (ID 10000030) that are worth at least 1 billion ISK, you would use:
```
/subscribe id: cap-watch-devoid description: Capital and Marauder kills in Devoid region_ids: 10000030 ship_group_ids: 485,547 min_value: 1000000000
```

When you create your first subscription in a server, the bot will automatically generate a configuration file named `[your_server_id].json` (e.g., `123456789012345678.json`) on the host machine. All subsequent subscriptions for that server will be managed through in-Discord commands.

## Commands

### `/subscribe`
Creates or updates a killmail subscription for the current channel. All filter options are optional except for `id` and `description`.

Both `/subscribe` and `/unsubscribe` require the Manage Server (`MANAGE_GUILD`) permission and cannot be used in DMs.

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

### `/diag`
Displays diagnostic information for all subscriptions active in the current channel.

### Finding IDs
To find the correct IDs for regions, systems, ships, and groups, you can use a third-party database site like [**EVE Ref**](https://everef.net/type) or Dotlan. For character, corporation, and alliance IDs, zKillboard is an excellent resource.

## Advanced Examples

### Pinging for Capital Kills Near a Staging System

This example creates a subscription that pings `@everyone` if a killmail involving Capital ships occurs within 7.0 light-years of your staging system (`YOUR_SYSTEM_ID`). The
ping will only be sent if the killmail is less than 10 minutes old.

```
/subscribe id: capitals-radar description: Capitals near staging ship_group_ids: 485 ly_ranges_json: [{"system_id":YOUR_SYSTEM_ID, "range":7.0}] ping_type: Everyone
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
