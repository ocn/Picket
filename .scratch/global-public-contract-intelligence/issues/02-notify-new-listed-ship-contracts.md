# 02 — Notify on newly Listed ship contracts

**What to build:** Let Discord users create PostgreSQL-backed Contract Subscriptions that post a compact message when a newly observed public contract lists a configured ship for offer or request.

**Blocked by:** 01 — Establish silent Global Public Contract Intelligence baselines.

**Status:** ready-for-agent

- [ ] Contract Subscriptions are stored in PostgreSQL separately from existing killmail subscriptions and identify their guild, destination channel, stable subscription ID, description, filter, and per-event Contract Event Actions.
- [ ] A contract-specific subscribe command can create or replace a subscription for the invoking channel, and a corresponding removal command can delete it.
- [ ] Contract commands preserve the current bot's source-level permission behavior and produce ephemeral success or validation responses.
- [ ] The initial filter surface can distinguish Listed events, Offered Items versus Requested Items, item type IDs, and ship group IDs.
- [ ] Every configured event action supports ignore, post, or post-and-ping independently, even though this slice emits only Listed events.
- [ ] A new matching contract observed after the Regional Baseline produces one Listed event per matching Contract Subscription; baseline contracts do not.
- [ ] A contract without a successfully captured item manifest cannot match a ship condition or produce a ship listing notification.
- [ ] A persisted outbound-delivery record is created before Discord is called and is marked successful with the returned message ID.
- [ ] Overlapping subscriptions in the same channel produce separate messages; repeated processing of the same observation does not reproduce the same subscription/contract/event delivery.
- [ ] The initial embed identifies the event, matched ship or bundle, relevant ISK, best currently available location, Issuer, contract ID, and an isolated Contract Address without inventing a counterparty.
- [ ] User-authored contract text cannot create unintended Discord mentions.
- [ ] High-level tests cover command persistence, Offered/Requested direction, type/group matching, baseline suppression, one-message-per-subscription behavior, and idempotent repeated cycles.

