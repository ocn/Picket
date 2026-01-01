# Useful Embeds - Task Checklist

**Last Updated**: 2026-01-01

---

## Phase 1: Fleet Composition by Ship Group

### 1.1 Add Ship Group Name Resolution
- [ ] Create `get_group_name()` function in `esi.rs` or `discord_bot.rs`
- [ ] Decide: static map vs ESI fetch vs hybrid
- [ ] Add caching for group names (similar to ship names)
- [ ] Handle unknown group IDs gracefully
- **Acceptance**: Can resolve group_id → "Heavy Assault Cruiser"
- **Effort**: S
- **Dependencies**: None

### 1.2 Aggregate Attackers by Ship Group
- [ ] Modify attacker processing in `build_killmail_embed()`
- [ ] For each attacker: get ship_type_id → group_id → group_name
- [ ] Count by group (overall fleet composition)
- [ ] Optionally: count by (alliance, group) for detailed breakdown
- **Acceptance**: Can produce "15x HAC, 5x Dictor, 5x Logi"
- **Effort**: M
- **Dependencies**: 1.1

### 1.3 Format Fleet Composition in Embed
- [ ] Design compact format for fleet comp line
- [ ] Decide placement (new line before/after alliance list? replace?)
- [ ] Test with various fleet sizes (5, 20, 100+ attackers)
- [ ] Ensure fits within embed field limits
- **Acceptance**: Embed shows fleet composition at a glance
- **Effort**: S
- **Dependencies**: 1.2

### 1.4 Format Style
- **Decision**: Option B - Alliance with ship breakdown

**Selected format**:
```
BIGAB x15 (10 HAC, 5 Logi)
Horde x10 (5 HAC, 5 Dictor)
```

**Alternative formats (for future consideration)**:
- Option A: Fleet comp summary + alliance counts separate
  ```
  Fleet: 15x HAC, 5x Dictor, 5x Logi
  BIGAB x15 | Horde x10
  ```
- Option C: Ship groups only, no alliance breakdown
  ```
  15x Heavy Assault Cruiser
  5x Interdictor
  5x Logistics
  ```

- **Effort**: S

---

## Phase 2: Layout Modes (Deferred)

Planning deferred until Phase 1 complete. Likely modes:
- [ ] Standard mode (current + fleet comp)
- [ ] Kill-focused mode
- [ ] Loss-focused mode

---

## Phase 3: Future Ideas (Backlog)

- [ ] Fitting anomaly detection
- [ ] Compact mode
- [ ] Battle grouping

---

## Progress Log

### 2026-01-01
- Refined scope based on user feedback
- Removed: victim name, final blow info, damage breakdown
- Focus: Fleet composition by ship group
- Deferred: footer icon, layout modes

---

## Completed

- [x] Initial analysis
- [x] Scope refinement with user
- [x] Added "other" ship image (prior work)
