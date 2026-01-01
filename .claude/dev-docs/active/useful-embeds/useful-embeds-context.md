# Useful Embeds - Context Document

**Last Updated**: 2026-01-01

---

## Key Files

| File | Purpose | Key Functions |
|------|---------|---------------|
| `src/discord_bot.rs` | Embed building | `build_killmail_embed()`, `select_best_entity_for_display()` |
| `src/models.rs` | Data structures | `Killmail`, `Victim`, `Attacker`, `ZkData` |
| `src/esi.rs` | ESI client | `get_name()`, `get_system()` |
| `src/config.rs` | Subscription config | `Subscription`, `FilterNode` |

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

## Current Embed Structure

```
┌─────────────────────────────────────────────────────┐
│ [Author Icon] Author Name (ship + system + time)   │
│               Author URL → Battle Report           │
├─────────────────────────────────────────────────────┤
│ Title: "`Ship` destroyed" or "`Ship` died to X"   │
│ URL → zkillboard                   [Thumbnail 64px]│
├─────────────────────────────────────────────────────┤
│                                                     │
│              [Image 128px - "other" ship]          │
│                                                     │
├─────────────────────────────────────────────────────┤
│ (N) Attackers Involved                              │
│ ```                                                 │
│ Alliance Name       x15                             │
│ Other Alliance      x8                              │
│ ...others           x3                              │
│ ```                                                 │
│ victim: [Alliance Name](zkb)                        │
│ in: [System](dotlan) ([Region](dotlan))            │
│ range: 5.2 LY from Base ([Supers]|[FAX]|[Blops])   │
│ on: [Celestial](zkb), 150km away                   │
├─────────────────────────────────────────────────────┤
│ Value: 2.5B • EVETime: 01/01/2026, 14:30           │
│                                        [timestamp] │
└─────────────────────────────────────────────────────┘
```

---

## Decisions Log

### 2026-01-01
- **Decision**: Focus Phase 1 on information density before layout changes
- **Rationale**: Quick wins that don't require subscription config changes

### 2026-01-01
- **Decision**: Use footer icon for final blow ship
- **Rationale**: Footer icon is currently unused, provides visual context

---

## Open Questions

1. Should victim character name link to zkb or evewho?
2. For compact mode, what's the minimum useful info?
3. Should we show ISK dropped vs destroyed?
4. How to handle structure kills (no character)?
