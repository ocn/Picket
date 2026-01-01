# zk-activity Development Overview

## Project Summary

**zk-activity** (killbot-rust) is a Discord bot that streams EVE Online killmails from zkillboard.com into Discord channels with a powerful filtering system.

## Architecture

```
                    ┌─────────────────┐
                    │   zkillboard    │
                    │     RedisQ      │
                    └────────┬────────┘
                             │ WebSocket/Long-polling
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                      killbot-rust                           │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │
│  │   redis_q    │──▶│  processor   │──▶│ discord_bot  │    │
│  │  (listener)  │   │  (filters)   │   │  (embeds)    │    │
│  └──────────────┘   └──────────────┘   └──────────────┘    │
│         │                  │                   │           │
│         ▼                  ▼                   ▼           │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │
│  │     esi      │   │    config    │   │   commands   │    │
│  │  (API calls) │   │ (JSON files) │   │   (slash)    │    │
│  └──────────────┘   └──────────────┘   └──────────────┘    │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Discord     │
                    │    Channels     │
                    └─────────────────┘
```

## Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Entry point, main loop, state initialization |
| `src/redis_q.rs` | zkillboard RedisQ listener |
| `src/processor.rs` | Killmail filtering logic |
| `src/discord_bot.rs` | Event handler, embeds, message sending |
| `src/esi.rs` | EVE ESI API client |
| `src/config.rs` | Configuration and subscription management |
| `src/models.rs` | Data structures |
| `src/commands/*.rs` | Discord slash commands |

## Data Flow

1. **RedisQ Listener** receives killmail from zkillboard
2. **ESI Client** fetches additional killmail data
3. **Processor** evaluates killmail against all subscriptions
4. **Discord Bot** sends embed to matching channels

## Configuration

- `config/[guild_id].json` - Per-server subscription files
- `data/systems.json` - Cached system data
- `data/ships.json` - Cached ship group mappings
- `data/names.json` - Cached entity names
- `.env` - Bot token and credentials

## Tech Stack

- **Language**: Rust (2021 edition)
- **Async Runtime**: Tokio
- **Discord Library**: Serenity 0.11
- **HTTP Client**: Reqwest (rustls-tls)
- **Caching**: Moka
- **Serialization**: Serde

## Workstreams

Active development areas are tracked in separate documents:

- [WORKSTREAM-FILTERS.md](WORKSTREAM-FILTERS.md) - Filter system improvements
- [WORKSTREAM-COMMANDS.md](WORKSTREAM-COMMANDS.md) - Discord command enhancements
- [WORKSTREAM-PERFORMANCE.md](WORKSTREAM-PERFORMANCE.md) - Performance optimizations
- [WORKSTREAM-FEATURES.md](WORKSTREAM-FEATURES.md) - New features and ideas
