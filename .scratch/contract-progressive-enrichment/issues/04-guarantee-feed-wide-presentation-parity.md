# 04 — Guarantee feed-wide presentation parity

**What to build:** Every Contract Subscription feed presents equivalent Public Contract evidence with one current revision, one compact identity/location grammar, and one omission policy regardless of channel, filter shape, event kind, or ping action.

**Blocked by:** 03 — Preserve enrichment through terminal events.

**Status:** resolved

- [ ] A table-driven collector test is written red first and sends equivalent evidence through region, identity, item, and proximity-oriented Contract Subscriptions.
- [ ] Equivalent events across feeds produce the same Issuer information, Observed Location precedence, compact `in/on/range` grammar, and presentation revision.
- [ ] Solar-system, region, and current-location links use the established shared killfeed conventions.
- [ ] Issuer names use the contextual `issuer` role and established identity-link convention where supported; unavailable Acceptor identity is omitted everywhere.
- [ ] No feed exposes raw identifiers, evidence-class terminology, HTTP details, empty fields, or repetitive unknown placeholders.
- [ ] Listings and all terminal event kinds remain within the established embed field, line, and character budgets.
- [ ] Contract Subscription matching and Contract Event Actions remain authoritative and no non-acceptance event gains ping eligibility.
- [ ] The presentation revision is incremented once and is shared by live rendering and the later Historical Enrichment Repair slice.
- [ ] Existing killmail presentation behavior is unchanged.
- [ ] The approved production-shaped collector seam, focused renderer tests, and regular typechecking pass.
