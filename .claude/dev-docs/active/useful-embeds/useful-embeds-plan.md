# Useful Embeds - Implementation Plan

**Last Updated**: 2026-01-01
**Status**: Planning
**Estimated Effort**: M (Medium)

---

## Executive Summary

Enhance the killmail embed to show **fleet composition by ship group** (Heavy Assault Cruisers, Interdictors, etc.) rather than just alliance counts. The goal is high information density that helps understand what happened at a glance.

---

## Current State

### What Works Well
- Alliance/corp grouping with counts
- Tracked entity ship in thumbnail
- "Other" ship in image slot
- Color coding (green=kill, red=loss)
- Range/jump links

### What's Missing
- **Ship group composition**: "30x HACs, 10x Dictors" instead of just "40 attackers"
- **Layout flexibility**: Different emphasis for kills vs losses
- Better visual density without clutter

---

## Proposed Changes

### Phase 1: Fleet Composition by Ship Group (Priority: HIGH)

**Goal**: Show attacker fleet broken down by ship group (not individual ship types)

**Current**:
```
BIGAB                  x25
Pandemic Horde         x15
...others              x5
```

**Proposed (Option B - Selected)**:
```
BIGAB x25 (15 HAC, 5 Dictor, 5 Logi)
Pandemic Horde x15 (10 HAC, 5 Dictor)
...others x5
```

**Alternative formats (for future consideration)**:
- Option A: Fleet summary separate from alliance counts
  ```
  Fleet: 25x HAC, 10x Dictor, 5x Logi
  BIGAB x25 | Pandemic Horde x15 | others x5
  ```
- Option C: Ship groups only, no alliance breakdown
  ```
  25x Heavy Assault Cruiser
  10x Interdictor
  5x Logistics
  ```

**Implementation**:
1. Already have `get_ship_group_id()` to map ship → group
2. Need group ID → group name mapping (ESI or static)
3. Aggregate attackers by (alliance, ship_group)
4. Format compactly

### Phase 2: Layout Modes (Priority: MEDIUM)

Defer detailed planning until Phase 1 is complete and we can see what information density looks like. The modes will likely be:

- **Standard**: Current layout with fleet comp added
- **Kill-focused**: Emphasize what tracked entity used
- **Loss-focused**: Emphasize what killed tracked entity

### Phase 3: Future Ideas (Backlog)

- Fitting anomaly detection (unusual fits)
- Compact mode for high-volume
- Battle grouping (multiple kills in same system/time)

---

## Technical Notes

### Ship Group ID → Name

EVE ship groups we care about:
| Group ID | Name |
|----------|------|
| 26 | Cruiser |
| 27 | Battleship |
| 28 | Industrial |
| 30 | Titan |
| 324 | Assault Frigate |
| 358 | Heavy Assault Cruiser |
| 419 | Combat Battlecruiser |
| 485 | Dreadnought |
| 513 | Freighter |
| 540 | Command Ship |
| 541 | Interdictor |
| 547 | Carrier |
| 659 | Supercarrier |
| 830 | Covert Ops |
| 831 | Interceptor |
| 832 | Logistics |
| 833 | Force Recon |
| 834 | Stealth Bomber |
| 893 | Electronic Attack Ship |
| 894 | Heavy Interdiction Cruiser |
| 898 | Black Ops |
| 900 | Marauder |
| 906 | Combat Recon |
| 1022 | Strategic Cruiser |
| 1201 | Attack Battlecruiser |
| 1527 | Logistics Frigate |
| 1534 | Command Destroyer |
| 1538 | Force Auxiliary |
| 1972 | Flag Cruiser |
| 4594 | Lancer Dreadnought |

Options:
1. **Static map**: Hardcode common group names
2. **ESI fetch**: `universe/groups/{id}/` (cacheable)
3. **Hybrid**: Static for common, ESI fallback

### Data Flow

```
attackers[]
  → ship_type_id
  → get_ship_group_id() [cached]
  → group_name [new: cached]
  → aggregate by (alliance, group)
  → format
```

---

## Success Metrics

1. Fleet composition visible at a glance
2. No significant latency increase (caching)
3. Embed remains readable on mobile

---

## Removed from Scope

- ~~Victim character name~~ (not needed)
- ~~Final blow info~~ (not needed)
- ~~Footer icon~~ (defer)
- ~~Damage breakdown~~ (not useful)
- ~~Ship fitting preview~~ (maybe later for anomaly detection)
