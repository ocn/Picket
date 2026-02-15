# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**zk-activity** (`killbot-rust`) is a Rust Discord bot that streams EVE Online killmails from zkillboard RedisQ to Discord channels using configurable subscription filters.

At runtime, the bot:
1. Long-polls RedisQ for a killmail envelope.
2. Loads full killmail details from the ESI URL embedded in the RedisQ payload.
3. Evaluates subscriptions and filters.
4. Sends Discord embeds to matched channels.

## Build & Test Commands

```bash
cargo check                                # Fast compilation check
cargo build --release                      # Release build
cargo clippy -- -W clippy::all             # Lint
cargo test                                 # Unit tests + non-ignored tests
cargo test test_name                       # Specific test
cargo test --test test_killfeed_embeds -- --ignored --nocapture
cargo test --test test_tracking_embeds -- --ignored --nocapture
```

Notes:
- Integration embed tests are `#[ignore]` and do not run with plain `cargo test`.
- Integration embed tests use `.env` via `dotenvy` and require `DISCORD_BOT_TOKEN` and `DISCORD_CLIENT_ID`.
- Use `/build-and-fix` and `/test-and-fix` project commands for iterative error resolution (`.claude/commands/`).

Docker:
```bash
docker-compose up --build -d
```

## Architecture

```text
zkillboard RedisQ
  -> redis_q.rs (listen.php long-poll)
  -> esi.rs (load full killmail + metadata lookups)
  -> processor.rs (filter evaluation + veto logic)
  -> discord_bot.rs (embed assembly + message send)
```

In parallel, Serenity handles interactions:
- Global slash command registration on `ready`.
- Slash command dispatch and component callbacks on `interaction_create`.
- DM-based SSO callback URL handling in `message`.

## Key Modules

| Module | Role |
|--------|------|
| `src/lib.rs` | Startup, state construction, command map wiring, RedisQ processing loop |
| `src/redis_q.rs` | RedisQ HTTP listener with timeout + HTML/JSON guard |
| `src/esi.rs` | ESI client + Fuzzwork nearest celestial lookup + EVE SSO token/contact flow |
| `src/processor.rs` | Recursive filter engine and `IgnoreHighStanding` veto partitioning |
| `src/discord_bot.rs` | Serenity `EventHandler`, command registration/dispatch, embed rendering, message send |
| `src/config.rs` | App models, filter types, AppState, config load/save helpers |
| `src/models.rs` | Serde models for RedisQ and killmail payloads |
| `src/commands.rs` + `src/commands/*.rs` | `Command` trait and slash command implementations |

## Filter System

Subscriptions use a recursive `FilterNode` tree:
- `Condition(Filter)`
- `And(Vec<FilterNode>)`
- `Or(Vec<FilterNode>)`
- `Not(Box<FilterNode>)`

Filter categories:
- `SimpleFilter`: `TotalValue`, `DroppedValue`, `Region`, `System`, `Security`, `LyRangeFrom`, `IsNpc`, `IsSolo`, `Pilots`, `TimeRange`, `IgnoreHighStanding`
- `TargetedFilter` (`Target`: `Any`/`Attacker`/`Victim`): `Alliance`, `Corporation`, `Character`, `ShipType`, `ShipGroup`, `NameFragment`

Processing behavior:
- `IgnoreHighStanding` conditions are split into a veto tree.
- Primary matches and veto matches are evaluated separately.
- Final attacker matches are `primary - veto`; victim match is preserved from primary evaluation.

## State Management

`AppState` is shared as `Arc<AppState>` via Serenity `TypeMapKey` (`AppStateContainer`):
- Read-heavy maps: `Arc<RwLock<HashMap<...>>>` (systems, ships, names, tickers, group names, subscriptions, user standings)
- Write coordination: `tokio::sync::Mutex<()>` file locks and `last_ping_times`
- SSO state map: `Arc<Mutex<HashMap<String, SsoState>>>`

## Persistence

JSON-backed data in `config/`:
- `[guild_id].json`: per-guild subscription arrays
- `systems.json`, `ships.json`, `names.json`, `tickers.json`, `group_names.json`: cached EVE metadata
- `user_standings.json`: saved SSO tokens and contact lists

## Commands

Active slash commands registered in runtime command map:
- `ping`
- `subscribe`
- `unsubscribe`
- `diag`
- `sync_standings`
- `sync_remove`
- `sync_clear`
- `find_unsubscribed`

Notes:
- Command names use underscores (not hyphens).
- `src/commands/embed_mode.rs` exists but is not wired into `src/commands.rs` or runtime registration.

## Key Conventions

- Serenity version is `0.11` (`Cargo.toml`).
- Network/ESI/Fuzzwork failures are generally logged and skipped to keep processing alive.
- Startup exits on unrecoverable app config load failure (`load_app_config`).
- Ship display priority is hardcoded in `SHIP_GROUP_PRIORITY` and `GROUP_NAMES` in `src/discord_bot.rs`.
- Attacker identity keys are composite strings in `src/processor.rs`: `s{ship}:w{weapon}:c{char}:o{corp}:a{alliance}:f{faction}`.
- Group name resolution order: hardcoded display table -> `group_names.json` cache -> ESI fetch -> cache write.
- Test fixtures live in `resources/` and are loaded by `tests/common/mod.rs`.

## Environment Variables

```bash
DISCORD_BOT_TOKEN      # Required for runtime and integration embed tests
DISCORD_CLIENT_ID      # Required for runtime and integration embed tests
EVE_CLIENT_ID          # Required for /sync_standings SSO flow
EVE_CLIENT_SECRET      # Required for /sync_standings SSO flow
```

Setup:
- Copy `docs/env.sample` to `.env` for base Discord vars.
- Add `EVE_CLIENT_ID` and `EVE_CLIENT_SECRET` manually if using standings sync features.
