# Workstream: Features

## Overview

New feature ideas and enhancements for killbot.

---

## Planned Features

### Statistics & Analytics

- [ ] **Kill statistics** - Track kills per channel/subscription
- [ ] **Monthly reports** - Summary embeds at month end
- [ ] **Top killers/victims** - Leaderboards for subscribed entities

### Notifications

- [ ] **Ping scheduling** - Different ping types for different times
- [ ] **Ping cooldown per entity** - Don't ping same alliance twice in 5 min
- [ ] **Quiet hours** - Disable pings during specified hours

### Integration

- [ ] **Webhook support** - Alternative to bot messages
- [ ] **API endpoint** - REST API for subscription management
- [ ] **Web dashboard** - Browser-based configuration

---

## Feature Ideas (Backlog)

### Enhanced Embeds

- [ ] **Battle summary** - When many kills in same system, summarize
- [ ] **Fleet composition** - Show attacker fleet breakdown
- [ ] **Timeline view** - Show kills in chronological thread
- [ ] **Compact mode** - Smaller embeds for high-volume channels

### Smart Filtering

- [ ] **Activity detection** - Alert on unusual activity patterns
- [ ] **Structure timers** - Integration with structure vulnerability
- [ ] **Jump fatigue calc** - Show fatigue from base system

### Standings Integration

- [ ] **Coalition tracking** - Define custom coalition groupings
- [ ] **Standing colors** - Color-code embeds by standing
- [ ] **Contact sync improvements** - Handle token refresh

---

## Recently Completed

- [x] Light-year range filtering
- [x] Ping rate limiting (5 min cooldown per channel)
- [x] Standings sync via EVE SSO
- [x] Kill relative time display ("5 minutes ago")
- [x] Channel cleanup on permission loss

---

## Implementation Priority

| Priority | Feature | Complexity | Impact |
|----------|---------|------------|--------|
| 1 | Subscription list command | Low | High |
| 2 | Better error messages | Low | High |
| 3 | Batch ESI calls | Medium | Medium |
| 4 | Statistics tracking | Medium | Medium |
| 5 | Web dashboard | High | High |
