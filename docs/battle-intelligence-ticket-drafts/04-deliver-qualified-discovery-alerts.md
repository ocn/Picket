# 04 — Deliver fresh, low-noise battle discoveries to Discord

**What to build:** Rank observed reports and send qualified fresh discovery messages, with routine edits and explicit priority mentions.

**Blocked by:** 03 — Evolve separate Battle Reports with contextual losses

**Status:** draft — awaiting breakdown approval

- [ ] Rank the private report list by configured priority, freshness and observed scale, showing the ranking factors.
- [ ] Ordinary discovery requires two linked player-ship losses in ten minutes with at least five distinct attacking pilots; pods do not meet the loss threshold and NPC-only events do not trigger discovery.
- [ ] Explicit priority-source matches bypass corroboration but not the five-minute event-time freshness limit; source mention settings cannot silently set battle priority.
- [ ] Persist notification intent and destination/message state, deduplicating overlapping subscriptions per report/destination; normal losses edit the existing message and distinct qualifying encounters or new priority escalations can surface follow-ups.
- [ ] Ordinary notifications and revisions have no mentions; priority mentions require explicit role opt-in and respect a ten-minute destination-channel cooldown independently of message deduplication.
- [ ] Handle retries, ambiguous sends and stale delivery attempts without claiming unverified exactly-once transport behavior; verify intended sends/edits and mentions with captured delivery rather than live Discord.

**Spec coverage:** User Stories 3, 4, 25, 26, 27, 28, 29, 30, 31, 32, 55. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

