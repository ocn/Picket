# 06 — Enrich contract intelligence embeds

**What to build:** Turn contract events into concise, tactically useful embeds containing the best public location and Issuer context plus compact locally observed history.

**Blocked by:** 03 — Support complete recursive Contract Subscription filtering; 04 — Confirm pure ship sales and purchases; 05 — Report expired and unresolved closures safely.

**Status:** ready-for-agent

- [ ] Observed Location resolves as precisely as public data permits and degrades through station or accessible structure name, solar system, security status, region, and raw location ID without losing known facts.
- [ ] Issuer character, corporation, and publicly resolvable alliance are snapshotted while the contract is observed.
- [ ] Observed Affiliation is explicitly rendered as observation-time context and is not presented as acceptance-time affiliation.
- [ ] No counterparty, buyer, or accepting corporation field appears because public ESI does not source one.
- [ ] The embed summarizes the complete Offered/Requested bundle while keeping the primary strategically ranked ship prominent in the title and thumbnail.
- [ ] Relevant Offered ISK or Requested ISK is compactly formatted without reversing transaction direction.
- [ ] Issuer and corporation history is limited to concise 90-day confirmed-sale, confirmed-purchase, and unknown-closure totals plus the most recent confirmed local event.
- [ ] Confirmed outcomes show the evidence interval and observed delivery latency; terminal outcomes avoid false timestamp precision.
- [ ] The footer includes the contract ID and compact timing context.
- [ ] The Contract Address is rendered in a dedicated one-line fenced code block containing only `contract:0//<contract-id>`.
- [ ] Contract-authored titles are sanitized against misleading formatting and unintended mentions while preserving readable text.
- [ ] Structural embed tests cover each event kind, location fallback level, single- and multi-ship bundles, missing affiliations, history truncation, evidence timing, and Contract Address isolation.

