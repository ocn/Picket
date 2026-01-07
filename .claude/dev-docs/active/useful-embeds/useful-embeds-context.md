# Useful Embeds - Context Document

**Last Updated**: 2026-01-07

---

## Key Files

| File | Purpose | Key Functions |
|------|---------|---------------|
| `src/discord_bot.rs` | Embed building | `build_killmail_embed()`, `select_best_entity_for_display()`, `compute_fleet_composition()`, `FleetComposition`, `get_ticker()`, `get_most_common_attacker_group()`, `is_known_group()` |
| `src/models.rs` | Data structures | `Killmail`, `Victim`, `Attacker`, `ZkData` |
| `src/esi.rs` | ESI client | `get_name()`, `get_system()`, `get_ticker()` |
| `src/config.rs` | Subscription config | `Subscription`, `FilterNode`, `AppState` (with tickers cache), `FilterNode::contains_ship_filter()` |
| `tests/common/mod.rs` | Test helpers | `load_fixture()`, `create_app_state_with_subscriptions()` |
| `tests/test_tracking_embeds.rs` | Tracking embed tests | `send_tracking_embeds()` |
| `tests/test_killfeed_embeds.rs` | Killfeed embed tests | `send_killfeed_embeds()` |

---

## Data Available for Embeds

### From `zk_data.killmail.victim`

```rust
pub struct Victim {
    pub ship_type_id: u32,          // Ship type
    pub character_id: Option<u64>,   // Pilot (may be NPC/structure)
    pub corporation_id: Option<u64>, // Corp
    pub alliance_id: Option<u64>,    // Alliance
    pub faction_id: Option<u64>,     // NPC faction
    pub damage_taken: u32,           // Total damage
    pub position: Option<Position>,  // Coordinates
}
```

### From `zk_data.killmail.attackers[]`

```rust
pub struct Attacker {
    pub ship_type_id: Option<u32>,   // Ship (may be None for structures)
    pub weapon_type_id: Option<u32>, // Weapon used
    pub character_id: Option<u64>,   // Pilot
    pub corporation_id: Option<u64>, // Corp
    pub alliance_id: Option<u64>,    // Alliance
    pub faction_id: Option<u64>,     // NPC faction
    pub damage_done: u32,            // Damage dealt
    pub final_blow: bool,            // Got the kill
    pub security_status: f32,        // Sec status
}
```

### From `zk_data.zkb`

```rust
pub struct ZkbMeta {
    pub total_value: f64,       // ISK value
    pub dropped_value: f64,     // Dropped loot value
    pub destroyed_value: f64,   // Destroyed value
    pub fitted_value: f64,      // Fitting value
    pub npc: bool,              // NPC kill
    pub solo: bool,             // Solo kill
    pub awox: bool,             // Awox (friendly fire)
    pub location_id: Option<u64>, // Structure/celestial
    pub esi: String,            // ESI URL for full data
}
```

---

## Discord Embed Limits

| Element | Limit |
|---------|-------|
| Title | 256 characters |
| Description | 4096 characters |
| Fields | 25 max |
| Field name | 256 characters |
| Field value | 1024 characters |
| Footer text | 2048 characters |
| Author name | 256 characters |
| Total embed | 6000 characters |

---

## Current Embed Structure (After Redesign)

```
┌─────────────────────────────────────────────────────┐
│ [Ship Icon] Battle Report: Titan in X-7OMU (Deklein)│
│             Killmail posted 5 minutes ago           │
│             URL → Battle Report                     │
├─────────────────────────────────────────────────────┤
│ Title: "15x Titans killed a Nyx"                    │
│    or: "Nyx died to 15x Titans"     [Thumbnail: Nyx]│
│ URL → zkillboard                                    │
├─────────────────────────────────────────────────────┤
│ (3753) Attackers Involved                           │
│ 151x Titans, 188x Supers, 84x Dreads...             │
│ ```                                                 │
│ [CONDI] 2761                                        │
│  └ 139 Titans, 169 Supers                           │
│  └ 77 Dreads, 37 Carriers                           │
│  └ 796 BS, 338 HICs, +836                           │
│ others 492                                          │
│ ```                                                 │
├─────────────────────────────────────────────────────┤
│ Victim                                              │
│ [RAZOR] Player Name                                 │
├─────────────────────────────────────────────────────┤
│ in: System (Region)                                 │
│ on: Celestial, 150km away                           │
│ range: 5.2 LY from Turnur (Supers|FAX|Blops)        │
├─────────────────────────────────────────────────────┤
│ Value: 2.5B • EVETime: 01/06/2026, 14:30 [timestamp]│
└─────────────────────────────────────────────────────┘
```

---

## Ship Group Categories

```rust
const SUPER_GROUPS: &[u32] = &[30, 659]; // Titans, Supercarriers
const CAP_GROUPS: &[u32] = &[4594, 485, 1538, 547, 883, 902, 513];
// Lancers, Dreads, FAX, Carriers, Cap Industrial, Jump Freighters, Freighters
```

All other known groups are treated as subcaps. Unknown groups count toward the `+N` overflow.

---

## Decisions Log

### 2026-01-01
- **Decision**: Focus Phase 1 on information density before layout changes
- **Rationale**: Quick wins that don't require subscription config changes

### 2026-01-01
- **Decision**: Use footer icon for final blow ship
- **Rationale**: Footer icon is currently unused, provides visual context

### 2026-01-03
- **Decision**: Use static `GROUP_NAMES` mapping with abbreviations
- **Rationale**: Faster than ESI, covers all common ship groups
- **Implementation**: `const GROUP_NAMES: &[(u32, &str, &str)]` with (group_id, singular, plural)

### 2026-01-03
- **Decision**: Use compact `+N` format instead of "Nx Others"
- **Rationale**: Saves tokens/space, cleaner appearance
- **Example**: `2x Dreads, 3x HACs, +15` instead of `2x Dreads, 3x HACs, 15x Others`

### 2026-01-03
- **Decision**: Unknown ship groups go into +N count, not displayed as "Others"
- **Rationale**: Cleaner output, avoids confusion
- **Implementation**: `GROUP_UNKNOWN` sentinel (0) with debug logging for missing groups

### 2026-01-03
- **Decision**: Split integration tests into tracking/killfeed
- **Rationale**: Easier to test specific embed modes, faster iteration

### 2026-01-06
- **Decision**: Same embed layout for both Tracking and Killfeed modes
- **Rationale**: Simplifies code, consistent user experience
- **Implementation**: Removed planned `EmbedMode` enum, single layout for all

### 2026-01-06
- **Decision**: Category-based ship breakdown per alliance (supers/caps/subcaps)
- **Rationale**: Shows fleet composition at a glance with manageable line count
- **Implementation**: `format_category_line()` shows up to 3 types per category with `+N` overflow

### 2026-01-06
- **Decision**: Dynamic title based on kill/loss perspective
- **Rationale**: Immediately communicates context
- **Implementation**: Green = `"Nx Group killed a Victim"`, Red = `"Victim died to Nx Group"`

### 2026-01-06
- **Decision**: Ticker-based alliance display with ESI caching
- **Rationale**: Tickers are more recognizable than full names, fit better in compact format
- **Implementation**: `get_ticker()` in esi.rs with `AppState.tickers` cache

---

## Open Questions

1. ~~Should victim character name link to zkb or evewho?~~ (Resolved: shows name without link)
2. For compact mode, what's the minimum useful info? (Deferred)
3. ~~Should we show ISK dropped vs destroyed?~~ (Deferred)
4. How to handle structure kills (no character)? (Works correctly - shows "Structure")
