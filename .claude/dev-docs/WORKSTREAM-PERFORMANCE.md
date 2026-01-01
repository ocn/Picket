# Workstream: Performance

## Overview

Performance optimization for the killmail processing pipeline.

## Current Flow

```
RedisQ → ESI Fetch → Filter Evaluation → Embed Building → Discord Send
  ~1s      ~200ms         ~1-50ms           ~50-200ms        ~100ms
```

---

## Caching Layers

### 1. Persistent Cache (JSON files)

| Data | File | When Cached |
|------|------|-------------|
| Systems | `data/systems.json` | First request for system |
| Ship Groups | `data/ships.json` | First request for ship type |
| Entity Names | `data/names.json` | First request for ID |

### 2. In-Memory Cache (Moka)

```rust
pub celestial_cache: Cache<u32, Arc<Celestial>>  // TTL-based
```

### 3. Subscription Cache

Subscriptions are loaded into memory at startup and updated on changes.

---

## Backlog

### High Priority

- [ ] **Batch ESI calls** - Fetch multiple names in single POST
- [ ] **Pre-warm cache** - Load common data at startup
- [ ] **Connection pooling** - Reuse HTTP connections

### Medium Priority

- [ ] **Parallel filter evaluation** - Evaluate subscriptions concurrently
- [ ] **Lazy embed building** - Only build embed after confirming match
- [ ] **ESI rate limit handling** - Graceful handling of 429 responses

### Low Priority

- [ ] **Metrics collection** - Track processing times
- [ ] **Cache hit rate monitoring** - Identify cache efficiency
- [ ] **Memory profiling** - Identify memory-heavy operations

---

## Bottleneck Analysis

### ESI Calls per Killmail

For each killmail, we may need to fetch:
- System info (if not cached)
- Ship group ID for victim
- Ship group IDs for matched attackers
- Names for display
- Celestial info for location

**Optimization**: Pre-populate cache with common data:
- All solar systems
- Common ship type IDs (capitals, etc.)
- Major alliance/corp names

### Filter Evaluation

For N subscriptions with M filters each:
- Worst case: O(N * M) filter evaluations per killmail
- Most filters are simple ID lookups (O(1))
- Light-year range is O(K) where K = number of base systems

### Embed Building

Name resolution is the bottleneck:
- Need names for victim, attackers, ships, systems
- Each name may be an ESI call

**Optimization**: Batch name resolution with `universe/names` POST endpoint.
