# 08 — Recover contract intelligence without notification floods

**What to build:** Make contract collection and delivery resume safely after database, ESI, or process outages without flooding users with stale expiry and unknown-closure notifications.

**Blocked by:** 05 — Report expired and unresolved closures safely; 07 — Make Discord delivery restart-safe.

**Status:** ready-for-agent

- [ ] Contract collection and delivery retry PostgreSQL and ESI failures with bounded backoff while killmail collection and delivery continue independently.
- [ ] After a material collection gap, each affected region re-establishes a complete recovery baseline before absence-driven transitions resume.
- [ ] Newly encountered live contracts during recovery are incorporated silently rather than emitted as stale Listed events.
- [ ] Outage-spanning Expired and Closed — Outcome Unknown events are retained as lifecycle facts but suppressed from Discord to prevent a recovery flood.
- [ ] A previously observed contract may still emit Sale Confirmed or Purchase Confirmed after recovery when fresh positive pre-expiry acceptance evidence supports it.
- [ ] Pending outbound deliveries resume from PostgreSQL without recreating successful deliveries under normal restart conditions.
- [ ] Successful high-volume scan diagnostics are retained for 90 days and can be cleaned without deleting contract facts, Presence Intervals, lifecycle evidence, or Issuer summaries.
- [ ] Unresolved collection and delivery failures remain available until they are resolved or explicitly classified.
- [ ] The collector assumes one bot instance and introduces no leader election, advisory-lock ownership, or other horizontal-scaling machinery.
- [ ] Tests simulate database loss, ESI outage, process restart, stale regional state, new live contracts, stale expirations, unknown closures, and fresh positive acceptance evidence.

