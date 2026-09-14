# 05 — Recover battle collection without interrupting ordinary alerts

**What to build:** Show battle collection gaps during failures, keep ordinary alerts available, and reconcile recoverable missing observations without stale notification floods.

**Blocked by:** 04 — Deliver fresh, low-noise battle discoveries to Discord

**Status:** draft — awaiting breakdown approval

- [ ] Exercise slow processing, unavailable storage, rejected handoff, process restart and source outages; ordinary killfeed delivery continues while report/UI health exposes the affected coverage.
- [ ] Persist battle-specific recovery progress or equivalent reconciliation state independently of the ordinary source checkpoint; do not treat a volatile queue as durable evidence.
- [ ] Use bounded source replay where available to recover missing evidence idempotently; resume/retry without duplicate counts or notification obligations.
- [ ] Suspend quiet-based closure while coverage is unknown, and re-evaluate closure after reconciliation rather than interpreting outage duration as quiet.
- [ ] Recheck freshness before delayed discovery delivery; recovered historical records update reports and existing messages without fresh-alert floods.
- [ ] Leave gaps visible when upstream retention prevents recovery, and verify both recoverable and irrecoverable cases through the approved application seam.

**Spec coverage:** User Stories 27, 52, 53, 54, 55. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

