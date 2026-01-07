# Useful Embeds - Task Checklist

**Last Updated**: 2026-01-07

---

## Phase 1: Fleet Composition by Ship Group ✅ COMPLETE

### 1.1 Add Ship Group Name Resolution ✅
- [x] Create `get_group_name()` function in `discord_bot.rs`
- [x] Decide: static map vs ESI fetch vs hybrid → **Static map chosen**
- [x] Add `GROUP_NAMES` constant with (group_id, singular, plural)
- [x] Handle unknown group IDs gracefully → `GROUP_UNKNOWN` sentinel, debug logging
- **Acceptance**: ✅ Can resolve group_id → "HAC", "Dreads", etc.
- **Effort**: S
- **Dependencies**: None

### 1.2 Aggregate Attackers by Ship Group ✅
- [x] Create `FleetComposition` struct in `discord_bot.rs`
- [x] `compute_fleet_composition()` aggregates by ship group
- [x] Count by group (overall fleet composition)
- [x] Count by (alliance, group) for per-alliance breakdown
- **Acceptance**: ✅ Produces "15x HAC, 5x Dictor, 5x Logi"
- **Effort**: M
- **Dependencies**: 1.1

### 1.3 Format Fleet Composition in Embed ✅
- [x] `format_overall()` for overall fleet comp (single/multi-line)
- [x] `format_alliance_breakdown()` for per-alliance ship breakdown
- [x] Tested with various fleet sizes (5, 45, 77, 3753 attackers)
- [x] Fits within embed field limits with truncation to +N
- **Acceptance**: ✅ Embed shows fleet composition at a glance
- **Effort**: S
- **Dependencies**: 1.2

### 1.4 Format Style ✅
- **Decision**: Category-based formatting with ticker display
- **Implemented format**:
  ```
  [CONDI] 2761
   └ 139 Titans, 169 Supers
   └ 77 Dreads, 37 Carriers
   └ 796 BS, 338 HICs, +836
  ```

### 1.5 Polish & Bug Fixes ✅
- [x] Fixed pluralization ("1x Titans" → "1x Titan")
- [x] Changed "Nx Others" to compact "+N" format
- [x] Fixed T3C group ID (1022 → 963)
- [x] Added T3D group ID (1305)
- [x] Added Pod group ID (29)
- [x] Unknown groups count in +N instead of "Others"

### 1.6 Title Logic Fixes ✅
- [x] Fixed title to count by ship GROUP, not individual TYPE
- [x] Added `get_most_common_attacker_group()` function
- [x] Added `is_known_group()` helper
- [x] Fixed category line sorting (select by count, display by priority)
- [x] Added `FilterNode::contains_ship_filter()` helper in config.rs
- [x] Ship tracking filters use matched ship group in title
- [x] Entity tracking filters use most common attacker group in title
- **Acceptance**: ✅ Title shows correct group count and type
- **Effort**: S

### 1.7 NPC/Unknown Ship Handling ✅
- [x] Include GROUP_UNKNOWN in subcaps for alliance breakdown
- [x] Exclude unknown groups from title count calculations
- [x] Added test fixture `132461133_ceptor_npc_test.json`
- [x] Author icon for red embeds uses most common attacker TYPE
- **Acceptance**: ✅ NPC ships appear in fleet comp, don't affect title
- **Effort**: S

---

## Phase 1.5: Embed Layout Redesign ✅ COMPLETE

### 1.5.1 Ticker Support ✅
- [x] Add `get_ticker()` to `esi.rs` for ESI fetches
- [x] Add `tickers` cache to `AppState` in `config.rs`
- [x] Add `load_tickers()` and `save_tickers()` functions
- [x] Add `get_ticker()` wrapper in `discord_bot.rs` with cache lookup
- **Acceptance**: ✅ Can fetch and cache alliance/corp tickers
- **Effort**: S

### 1.5.2 Dynamic Title ✅
- [x] Implement color-based title format
- [x] Green (kill): `"{count}x {group} killed a {victim_ship}"`
- [x] Red (loss): `"{victim_ship} died to {count}x {group}"`
- **Acceptance**: ✅ Title reflects kill/loss perspective
- **Effort**: S

### 1.5.3 Author Section Redesign ✅
- [x] Change format to: `"Battle Report: {ship} in {system} ({region})"`
- [x] Add: `"\nKillmail posted {relative_time}"`
- **Acceptance**: ✅ Author section shows battle context
- **Effort**: S

### 1.5.4 Victim Field ✅
- [x] Add new "Victim" field to embed
- [x] Format as `[TICKER] Character Name`
- [x] Handle structure kills (no character)
- **Acceptance**: ✅ Victim info displayed with ticker
- **Effort**: S

### 1.5.5 Category-Based Alliance Breakdown ✅
- [x] Add `SUPER_GROUPS` constant (Titans, Supercarriers)
- [x] Add `CAP_GROUPS` constant (Dreads, FAX, Carriers, etc.)
- [x] Implement `format_category_line()` with 3-type limit + overflow
- [x] Supers line, Caps line, Subcaps line per alliance
- **Acceptance**: ✅ Alliance breakdown shows ship categories
- **Effort**: M

### 1.5.6 Integration Tests ✅
- [x] Split tests into `test_tracking_embeds.rs` and `test_killfeed_embeds.rs`
- [x] Remove `EmbedMode` references (same layout for both)
- [x] Add `dotenvy` dev dependency for test env loading
- **Acceptance**: ✅ Tests compile and can be run
- **Effort**: S

---

## Phase 2: Layout Modes (Cancelled)

Originally planned separate modes for Tracking vs Killfeed. Decision made to use **same layout for both**.

- [x] ~~Standard mode~~ → Single unified layout
- [x] ~~Kill-focused mode~~ → Cancelled
- [x] ~~Loss-focused mode~~ → Cancelled

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

### 2026-01-03
- **Completed Phase 1**: Fleet composition by ship group
- Implemented `FleetComposition` struct with formatting methods
- Added `GROUP_NAMES` static mapping with abbreviations
- Fixed pluralization bug in title
- Changed "Nx Others" → compact "+N" format
- Fixed incorrect group IDs (T3C: 1022→963, added T3D: 1305, Pod: 29)
- Added `GROUP_UNKNOWN` sentinel for unknown ship groups
- Split integration tests into `test_tracking_embeds.rs` and `test_killfeed_embeds.rs`
- All tests passing with 10 embeds sent successfully

### 2026-01-06
- **Completed Phase 1.5**: Embed layout redesign
- Added ticker support with ESI fetch + caching
- Implemented dynamic title based on kill/loss color
- Redesigned author section with battle report format
- Added victim field with ticker + character name
- Implemented category-based alliance breakdown (supers/caps/subcaps)
- Fixed test files to remove `EmbedMode` references
- Added `dotenvy` dev dependency
- Decision: Same layout for Tracking and Killfeed (cancelled separate modes)
- Verified implementation matches plan file requirements

### 2026-01-07
- **Fixed title ship count logic**: Changed from counting ship TYPE to counting by GROUP
  - "Keepstar died to 539x BS" → "Keepstar died to 1028x BS" (correct count)
- **Fixed category line sorting**: Select top 2 by count, display by GROUP_NAMES priority
  - BS now correctly appears before BC when both are numerous
- **Added ship tracking vs entity tracking distinction**:
  - Added `FilterNode::contains_ship_filter()` helper in config.rs
  - Ship filters use matched ship group in title
  - Entity filters use most common attacker group in title
- **Fixed NPC/unknown ship handling**:
  - Unknown groups (GROUP_UNKNOWN) now included in subcaps for alliance breakdown
  - Unknown groups excluded from `get_most_common_attacker_group()` to prevent "1x ships" title
- **Added author icon logic for red embeds**:
  - Green (kill): tracked ship icon
  - Red (loss): most common attacker TYPE icon
- **Added test fixture**: `resources/132461133_ceptor_npc_test.json` for NPC ship testing
- Updated killfeed tests with BIGAB alliance (Target::Any for kills AND losses)

---

## Completed

- [x] Initial analysis
- [x] Scope refinement with user
- [x] Added "other" ship image (prior work)
- [x] **Phase 1: Fleet composition by ship group** (2026-01-03)
- [x] Integration test restructuring (2026-01-03)
- [x] **Phase 1.5: Embed layout redesign** (2026-01-06)
- [x] Ticker support with caching (2026-01-06)
- [x] Dynamic title based on color (2026-01-06)
- [x] Category-based alliance breakdown (2026-01-06)
- [x] Dev-docs update to reflect current state (2026-01-06)
- [x] **Title logic fixes** (2026-01-07)
- [x] **NPC/unknown ship handling** (2026-01-07)
- [x] **Ship tracking vs entity tracking distinction** (2026-01-07)
- [x] Dev-docs update with 2026-01-07 fixes
