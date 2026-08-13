# Contract Intelligence

This context describes the bot's observation and reporting of publicly visible EVE Online item-exchange contracts across all regions. It excludes authenticated character-, corporation-, and alliance-specific contract monitoring.

## Language

**Global Public Contract Intelligence**:
Observation and analysis of public item-exchange contracts across all EVE regions, based solely on data available without contract-owner or contract-acceptor authorization.
_Avoid_: Private contract monitoring, corporation contract monitoring

**Public Contract**:
An item-exchange contract that EVE exposes to all players and through the public contract API while it remains publicly available.
_Avoid_: Global contract

**Offered Item**:
An item the issuer supplies through an item-exchange contract.
_Avoid_: Sold item

**Requested Item**:
An item the issuer requires from the accepting party through an item-exchange contract.
_Avoid_: Bought item

**Offered ISK**:
ISK the issuer supplies to the accepting party in exchange for Requested Items.
_Avoid_: Price, reward

**Requested ISK**:
ISK the issuer requires from the accepting party in exchange for Offered Items.
_Avoid_: Reward, offered price

**Issuer**:
The character that created a public contract; this is the only contract party identified by the global public data.
_Avoid_: Seller, buyer, party

**Observed Location**:
The most precise publicly resolvable place associated with a contract, degrading from station or structure name to solar system, region, and raw location ID as necessary.
_Avoid_: Exact location

**Observed Affiliation**:
The issuer's corporation and, when resolvable, alliance at observation time; it is not evidence of affiliation when the contract later disappears.
_Avoid_: Issuance affiliation, sale-time affiliation

**Contract Subscription**:
A recursive Boolean filter over contract events, items, value, location, issuer identity, and other public contract facts, paired with an alert action.
_Avoid_: Killmail subscription

**Contract Observation**:
A timestamped record of the public facts visible for a contract during one collection cycle.
_Avoid_: Contract state

**No Longer Public**:
The factual transition recorded when a previously observed public contract is absent from a complete subsequent regional observation.
_Avoid_: Sold, completed

**Awaiting Resolution**:
The internal lifecycle state of a Public Contract that is No Longer Public but whose terminal outcome has not yet been established. It never supports a user-facing sale or purchase claim.
_Avoid_: Likely sold, provisional sale

**Acceptance Confirmed**:
The terminal outcome established when public evidence reports that a contract was recently accepted before its known expiration time. The accepting party remains unknown.
_Avoid_: Acceptance inferred, absence confirmed

**Sale Confirmed**:
An Acceptance Confirmed event for a contract in which the issuer offered a matched ship, requested positive ISK, and requested no items.
_Avoid_: Likely sold, delisted

**Purchase Confirmed**:
An Acceptance Confirmed event for a contract in which the issuer requested a matched ship, offered positive ISK, and offered no items.
_Avoid_: Likely purchased, fulfilled listing

**Closed — Outcome Unknown**:
The terminal outcome of a contract that became No Longer Public but could not be classified as Acceptance Confirmed or Expired from public evidence.
_Avoid_: Sold, cancelled, delisted

**Expired**:
The terminal outcome of a contract that ceased to be public at its known expiration boundary without earlier evidence of acceptance.
_Avoid_: Unsold, cancelled

**Inconclusive Observation**:
A regional collection cycle that is incomplete or internally inconsistent and therefore cannot create, confirm, or retract a contract lifecycle transition.
_Avoid_: Missing contract

**Regional Baseline**:
The first complete observation of a region after collection begins, used to establish existing public contracts without producing listing alerts.
_Avoid_: Initial events

**Presence Interval**:
The bounded period between a contract's first and last complete public observations, retained without duplicating identical contract facts for every collection cycle.
_Avoid_: Snapshot history

**Contract Event Action**:
A subscription choice to ignore, post, or post-and-ping each supported contract event independently.
_Avoid_: Alert action, lifecycle action

**Contract Address**:
The raw `contract:0//<contract-id>` address that a user can paste into EVE to create an in-game contract link.
_Avoid_: Contract URL, Discord link

**Observation History**:
Retained contract observations used initially for embed enrichment and later for querying or analysis.
_Avoid_: Sales history
