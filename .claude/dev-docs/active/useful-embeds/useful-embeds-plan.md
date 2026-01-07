# Useful Embeds - Implementation Plan

**Last Updated**: 2026-01-07
**Status**: Complete (Phase 1 + Layout Redesign + Title Logic + NPC Handling Fixes)
**Estimated Effort**: M (Medium)

---

## Executive Summary

Enhance the killmail embed with **fleet composition by ship group** and a **redesigned layout** that provides better information density. The embed now shows dynamic titles based on kill/loss perspective, category-based ship breakdowns (supers/caps/subcaps), ticker-based alliance display, and victim information.

---

## Completed Work

### Phase 1: Fleet Composition by Ship Group ✅

**Goal**: Show attacker fleet broken down by ship group with category-based formatting.

**Implemented Format**:
```
151x Titans, 188x Supers, 84x Dreads, 49x Carriers, 1028x BS, 383x HICs, +1373
```

**Per-Alliance Breakdown**:
```
[CONDI] 2761
 └ 139 Titans, 169 Supers
 └ 77 Dreads, 37 Carriers
 └ 796 BS, 338 HICs, +836
[INIT] 500
 └ 12 Titans, 23 Supers
 └ 15 Dreads, 8 FAX
 └ 142 BS, 45 HICs, +255
others 492
```

**Key Features**:
- Ship groups organized into categories: Supers (Titans, Supercarriers), Caps (Dreads, FAX, Carriers, etc.), Subcaps (everything else)
- Up to 3 ship types per category line with `+N` overflow
- Up to 8 alliances shown, then "others N"
- Ticker-based alliance display with ESI fetch + caching

### Phase 1.5: Embed Layout Redesign ✅

**Goal**: Redesign entire embed structure for better information presentation.

**New Layout**:
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

**Key Changes from Original**:
1. **Dynamic Title**: Green (kill) = `"Nx Group killed a Victim"`, Red (loss) = `"Victim died to Nx Group"`
2. **Author Section**: Now shows `"Battle Report: {ship} in {system} ({region})\nKillmail posted {relative_time}"`
3. **Thumbnail**: Always victim ship
4. **New Victim Field**: Shows `[TICKER] Character Name`
5. **Attackers Field**: Combined overall fleet comp + category-based alliance breakdown

---

## Technical Implementation

### Ship Group Categories

```rust
const SUPER_GROUPS: &[u32] = &[30, 659]; // Titans, Supercarriers
const CAP_GROUPS: &[u32] = &[4594, 485, 1538, 547, 883, 902, 513]; // Lancers, Dreads, FAX, Carriers, Cap Indy, JF, Freighters
// Subcaps = everything else not in SUPER_GROUPS or CAP_GROUPS
```

### Key Functions Added

| Function | Location | Purpose |
|----------|----------|---------|
| `GROUP_NAMES` | `discord_bot.rs` | Static mapping of group_id → (singular, plural) |
| `get_group_name()` | `discord_bot.rs` | Resolve group_id to display name |
| `is_known_group()` | `discord_bot.rs` | Check if group_id is in GROUP_NAMES |
| `FleetComposition` | `discord_bot.rs` | Struct holding fleet aggregation data |
| `compute_fleet_composition()` | `discord_bot.rs` | Aggregate attackers by ship group |
| `format_overall()` | `discord_bot.rs` | Format overall fleet comp line |
| `format_category_line()` | `discord_bot.rs` | Format a category (supers/caps/subcaps) |
| `format_alliance_breakdown()` | `discord_bot.rs` | Format per-alliance breakdown |
| `get_most_common_attacker_group()` | `discord_bot.rs` | Find most numerous known ship group in attackers |
| `get_ticker()` | `esi.rs` | Fetch alliance/corp ticker from ESI |
| `get_ticker()` | `discord_bot.rs` | Wrapper with cache lookup |
| `contains_ship_filter()` | `config.rs` | Check if FilterNode contains ShipType/ShipGroup filters |

### Caching

- **Tickers**: `AppState.tickers` - HashMap<u64, String> persisted to `config/tickers.json`
- **Ships**: Existing `AppState.ships` cache for group_id lookups

---

## Phase 2: Layout Modes (Deferred/Cancelled)

Originally planned different modes (Tracking vs Killfeed). Decision made to use **same layout for both** - no separate modes needed.

---

## Phase 3: Future Ideas (Backlog)

- [ ] Fitting anomaly detection (unusual fits)
- [ ] Compact mode for high-volume feeds
- [ ] Battle grouping (multiple kills in same system/time)

---

## Success Metrics

1. ✅ Fleet composition visible at a glance
2. ✅ No significant latency increase (caching)
3. ✅ Embed remains readable on mobile
4. ✅ Dynamic title provides context (kill vs loss)
5. ✅ Victim information displayed

---

## Completion Notes

This task is functionally complete. The embed layout has been redesigned with:
- Fleet composition by ship group with category-based formatting
- Dynamic titles based on kill/loss perspective
- Ticker-based alliance/corp display
- New victim field showing character name

### Bug Fixes (2026-01-07)

1. **Title Ship Count Logic**: Fixed to count by ship GROUP not individual TYPE
   - Title now shows correct group count (e.g., "1028x BS" not "539x BS")

2. **Ship Tracking vs Entity Tracking**: Added `FilterNode::contains_ship_filter()` helper
   - Ship filters (ShipType/ShipGroup): Use matched ship group in title
   - Entity filters (Alliance/Corp): Use most common attacker group in title

3. **Category Line Sorting**: Fixed to select top 2 by count, display by GROUP_NAMES priority
   - BS now correctly appears before BC when both have high counts

4. **NPC/Unknown Ship Handling**: Fixed to include GROUP_UNKNOWN in subcaps
   - Unknown groups now display in alliance breakdown instead of being silently excluded

5. **Author Icon for Red Embeds**: Kept `most_common_ship_type()` for red (loss) embeds
   - Green: tracked ship icon
   - Red: most common attacker ship type icon

6. **`get_most_common_attacker_group()` Fix**: Now only counts known groups
   - Prevents "1x ships" title when unknown groups tie with known groups

Integration tests exist in `tests/test_tracking_embeds.rs` and `tests/test_killfeed_embeds.rs`.
Test fixture `resources/132461133_ceptor_npc_test.json` added for NPC ship testing.
