# 04 — Confirm pure ship sales and purchases

**What to build:** Resolve disappeared public contracts using cache-aware item evidence and notify users when a pure ship-for-ISK sale or ISK-for-ship purchase is positively confirmed before expiry.

**Blocked by:** 02 — Notify on newly Listed ship contracts.

**Status:** ready-for-agent

- [ ] Absence from a complete regional observation moves a previously Observed contract to Awaiting Resolution without producing a sale or purchase claim.
- [ ] Awaiting Resolution records the final complete public observation and schedules item-evidence probes according to the endpoint's cache metadata.
- [ ] A `200` or conditional `304` response keeps resolution pending until the advertised cache boundary rather than treating cached items as current lifecycle evidence.
- [ ] A pre-expiry `204` response records Acceptance Confirmed with the evidence response and observation times needed to describe an acceptance interval.
- [ ] Sale Confirmed requires a matched ship among Offered Items, positive Requested ISK, and no Requested Items.
- [ ] Purchase Confirmed requires a matched ship among Requested Items, positive Offered ISK, and no Offered Items.
- [ ] Accepted mixed or barter contracts are retained but produce neither Sale Confirmed nor Purchase Confirmed in this release.
- [ ] Sale Confirmed and Purchase Confirmed each honor their independent ignore, post, or post-and-ping action.
- [ ] A confirmed event reports an evidence interval and observed notification latency rather than an invented exact acceptance timestamp.
- [ ] Positive acceptance evidence is terminal and is not invalidated by a later public observation that resembles relisting.
- [ ] No path emits “likely sold,” “likely purchased,” or another provisional financial claim.
- [ ] Tests cover the pre-expiry `204`, cached `200`/`304`, zero-ISK exchanges, malformed money, pure sale, pure purchase, multi-item barter, and repeated resolution attempts.

