# Workstream: Filter System

## Overview

The filter system is the core of killbot - it determines which killmails get posted to which channels.

## Current Architecture

Filters are defined in `src/config.rs` and evaluated in `src/processor.rs`.

### Filter Types

```rust
pub enum SimpleFilter {
    Region(Vec<u32>),
    System(Vec<u32>),
    ShipGroup(Vec<u32>),
    ShipType(Vec<u32>),
    Alliance(Vec<u64>),
    Corporation(Vec<u64>),
    Character(Vec<u64>),
    TotalValue { min: Option<u64>, max: Option<u64> },
    Security { min: Option<f64>, max: Option<f64> },
    IsNpc(bool),
    IsSolo(bool),
    PilotCount { min: Option<u32>, max: Option<u32> },
    TimeRange { start: u8, end: u8 },
    IgnoreHighStanding { ... },
}
```

### Filter Composition

Filters can be combined using `And`, `Or`, and `Not` operators:

```rust
pub enum FilterNode {
    Condition(Filter),
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
}
```

---

## Backlog

### High Priority

- [ ] **Light-year range optimization** - Current implementation may be slow for many systems
- [ ] **Filter validation** - Validate IDs exist before saving subscription

### Medium Priority

- [ ] **Named filter groups** - Allow reusable filter definitions
- [ ] **Filter templates** - Common filter patterns (e.g., "all capitals")
- [ ] **Inverse matching** - "Notify when NOT in this region"

### Low Priority

- [ ] **Regex ship names** - Match ships by name pattern
- [ ] **Time-based filters** - Different filters for different times of day
- [ ] **Damage threshold** - Filter by damage dealt/received

---

## Technical Notes

### Filter Result Tracking

The processor returns which specific filters matched:

```rust
pub struct FilterResult {
    pub matched_victim: bool,
    pub matched_attackers: HashSet<AttackerKey>,
    pub min_pilots: Option<u32>,
    pub light_year_range: Option<SystemRange>,
}
```

This enables smart embed generation (showing matched entity prominently).

### Performance Considerations

- Filters are evaluated for every killmail against every subscription
- Ship group lookups may require ESI calls (cached after first fetch)
- Light-year calculations use precomputed system coordinates
