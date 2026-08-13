# 03 — Support complete recursive Contract Subscription filtering

**What to build:** Expand Contract Subscriptions into the agreed recursive Boolean grammar so users can precisely select contract events by contents, value, geography, and Issuer facts without changing killmail filter semantics.

**Blocked by:** 02 — Notify on newly Listed ship contracts.

**Status:** ready-for-agent

- [ ] Contract filters support Condition, And, Or, and Not nodes with deterministic nested evaluation.
- [ ] Conditions cover event kind, Offered Items, Requested Items, item type, ship group, minimum and maximum relevant ISK, region, solar system, security range, raw location ID, Issuer character, Issuer corporation, Observed Affiliation alliance, personal versus corporation issuance, and case-insensitive title fragment.
- [ ] Offered ISK, Requested ISK, Offered Items, and Requested Items remain directionally distinct during filtering.
- [ ] Monetary comparisons reject non-finite or invalid values and use the ISK direction relevant to the matched contract event.
- [ ] The Discord management surface can express and validate the complete grammar without exceeding Discord's application-command option limit.
- [ ] Invalid filter trees, unknown event actions, invalid ranges, and malformed identifiers are rejected without partially updating the existing subscription.
- [ ] Any configured ship match makes the contract eligible while preserving the complete bundle for event and embed consumers.
- [ ] A multi-ship bundle produces one event per contract rather than one event per item.
- [ ] The primary display ship is selected deterministically from the bot's strategic ship-group priority with stable fallback ordering.
- [ ] Automated tests exercise nested Boolean combinations and every condition family through the highest contract-cycle seam rather than private filter helpers alone.
- [ ] Existing killmail filters and JSON subscription behavior remain unchanged.

