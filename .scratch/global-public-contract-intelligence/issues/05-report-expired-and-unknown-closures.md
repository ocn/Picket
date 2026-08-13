# 05 — Report expired and unresolved closures safely

**What to build:** Finish the public-contract lifecycle by classifying expiry and unresolvable disappearance without overstating what public evidence proves.

**Blocked by:** 04 — Confirm pure ship sales and purchases.

**Status:** ready-for-agent

- [ ] A contract that ceases to be public at its known expiration boundary without earlier acceptance evidence becomes Expired.
- [ ] A `204` observed at or after the known expiration boundary cannot establish Acceptance Confirmed or a financial event.
- [ ] A disappeared contract whose public evidence no longer distinguishes acceptance, cancellation, or delisting becomes Closed — Outcome Unknown.
- [ ] A `404` response never becomes Sale Confirmed or Purchase Confirmed solely because the contract disappeared before expiry.
- [ ] A contract that reappears while Awaiting Resolution returns to Observed without emitting a terminal event or corrective Discord message.
- [ ] Expired and Closed — Outcome Unknown each honor independent ignore, post, or post-and-ping actions.
- [ ] Terminal transitions and their subscription deliveries are locally idempotent across retries and restarts.
- [ ] User-facing messages use the canonical Expired and Closed — Outcome Unknown language and never imply an unavailable cancellation reason or counterparty.
- [ ] Tests cover pre-expiry `404`, expiry-boundary `204`, normal expiry, transient errors, regional reappearance, and duplicate lifecycle processing.

