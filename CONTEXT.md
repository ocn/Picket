# Contract Intelligence

This context describes the bot's observation and reporting of publicly visible EVE Online item-exchange contracts across all configured regions. One operator-managed character authorization may resolve accessible player structures, but the system does not request private character or corporation contract history.

## Product language

**Feed**:
A kind of EVE Online event the bot can watch and deliver to Discord, such as the Killfeed, the Contract Feed, the Sov Timer Feed, or the Watchlist Feed.
_Avoid_: Stream, tracker, module, monitor

**Subscription**:
One filter paired with one action, bound to one Discord channel within a Feed.
_Avoid_: Alert, rule, feed (for a single binding)

**Radar**:
A Subscription whose filter is bounded by a light-year range from one or more solar systems. A Radar is a shape of Subscription, not a Feed and not the product.
_Avoid_: Proximity feed, range alert, activity radar (as a Feed name)

**Intel**:
The user-facing word for what Feeds deliver. "Intelligence" remains only in the formal context name Contract Intelligence.
_Avoid_ (in user-facing copy): Intelligence, monitoring, tracking

## Language

**Global Public Contract Intelligence**:
Observation and analysis of public item-exchange contracts across all EVE regions, based solely on data available without contract-owner or contract-acceptor authorization.
_Avoid_: Private contract monitoring, corporation contract monitoring

**Structure Authorization**:
The operator-managed character authorization used only to resolve player structures that character may access. It does not expose private contract-party history and does not imply that every public contract location can be resolved.
_Avoid_: Contract authorization, universal structure access

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
The character that created a contract. Global public contract data always identifies the Issuer by character ID.
_Avoid_: Seller, buyer, party

**Acceptor**:
The character or corporation that accepted a contract. Global public contract data does not identify the Acceptor, and this system does not request private party contract history, so unavailable Acceptor identity is omitted from embeds.
_Avoid_: Purchaser, buyer, contractor

**Observed Location**:
The most precise evidenced place associated with a contract, degrading from structure or station to solar system and then human-readable region. Raw location and region IDs are retained internally but are not user-facing location labels.
_Avoid_: Exact location

**Last-Verified Location**:
A previously authenticated structure-to-system or system-to-region mapping whose verification has expired. It remains usable with its verification time stated and must not be represented as currently revalidated.
_Avoid_: Current location, stale location, inferred location

**Progressive Enrichment**:
Synchronous pre-delivery collection and presentation of the most specific supported facts available for an event while omitting unavailable optional facts. Once posted, an event is not dynamically rerendered when later evidence becomes available.
_Avoid_: Complete embed, best-effort guess

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

**Terminal Resolution Recovery**:
Bounded processing of due Awaiting Resolution cases independently of regional discovery. It preserves the existing public-evidence requirements and cannot force, infer, or discard a terminal outcome merely to reduce backlog.
_Avoid_: Backlog replay, bulk closure

**Resolution Throughput**:
The rate at which due Awaiting Resolution cases receive a durably committed terminal outcome. It excludes probe attempts and Discord deliveries.
_Avoid_: Request rate, alert throughput

**Error-Charged Probe**:
A Terminal Resolution Recovery probe whose HTTP response consumes ESI's shared legacy error allowance, even when that response establishes a valid terminal outcome.
_Avoid_: Failed probe, unsuccessful resolution

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
The raw `contract:0//<contract-id>` address that a user can paste into EVE to create an in-game contract link. Opening the link in the game client shows the contract's full details, including its status and Acceptor, to any character regardless of party involvement.
_Avoid_: Contract URL, Discord link

**Observation History**:
Retained contract observations used initially for embed enrichment and later for querying or analysis.
_Avoid_: Sales history

**Historical Enrichment Repair**:
An operator-initiated, bounded, non-pinging in-place migration of existing retained Discord deliveries to a newer presentation format or fuller retained evidence. It preserves original message identity, never recreates a deleted or retired alert, and is not a normal-operation retry path.
_Avoid_: Repost, historical replay

**Retained Enrichment Evidence**:
Identity or location facts preserved with their source and observation time after collection. Previously verified location evidence remains part of contract history after structure access changes and is retained until an explicit retention policy replaces indefinite storage.
_Avoid_: Authorization token, live lookup result

**Retained Contract Manifest**:
The complete Offered Item and Requested Item membership captured for one Contract ID. Because a submitted contract cannot be altered, a complete manifest remains authoritative; only an absent, incomplete, or previously failed manifest requires collection.
_Avoid_: Expired item cache, mutable contract contents

## Killfeed language

**Ship Type**:
A concrete hull identified by an EVE type ID (e.g., Sidewinder).
_Avoid_: Ship, hull class

**Ship Group**:
An EVE group ID naming a category of Ship Types (e.g., Covert Ops).
_Avoid_: Ship class, ship group type, group type

**Type-Tracked Ship**:
An attacker whose Ship Type appears in the matched subscription's ship-type filter list. A Type-Tracked Ship is displayed by its Ship Type name; all other ships are displayed by their Ship Group name. Tracking follows the hull an attacker flies, never the weapon they use.
_Avoid_: Matched ship, tracked hull

**Fleet Composition Tally**:
The per-kill summary of attacker ship kinds, rendered as a bounded list of Tally Entries plus an overflow count of unnamed remaining ships. Type-Tracked Ships are guaranteed a named entry ahead of count-based selection.
_Avoid_: Attacker summary, ship breakdown

**Tally Entry**:
One "Nx Name" element of a Fleet Composition Tally, counted per Ship Group — or per Ship Type for Type-Tracked Ships, which are counted apart from their Ship Group.
_Avoid_: Line item

**Final Blow**:
The finishing attack recorded on a killmail and attributed to one attacker.
_Avoid_: Final blow attacker, killer

**After-Action Recap**:
A generated visual summary of reported battle activity that helps readers catch up on a long chain of killfeed alerts. Its coverage is limited to the evidence included in the report.
_Avoid_: Complete battle record

**Encounter**:
A grouping of reported combat events treated as one engagement. Its boundaries are derived from available evidence and may change as additional reports arrive.
_Avoid_: Killmail, roaming session

**Battle Report**:
An evolving summary of the explicitly included killmails for one Encounter, with observed participants, losses, locations and timing. It remains separate from other reports even when comparisons show shared pilots.
_Avoid_: Persistent fleet, complete fleet roster

**Fleet Track**:
An explicitly qualified association between separate Encounters, supported by recurring pilots, timing and plausible travel. It does not merge their rosters or establish continuous fleet presence; shared affiliation alone is insufficient.
_Avoid_: Confirmed fleet roster, live fleet position

**Roaming Session**:
A collection of separate Encounters summarized together with the evidence for their association stated. It does not establish that one unchanged fleet traveled between the locations or remained assembled throughout.
_Avoid_: Single battle, channel digest

**Provisional Closure**:
An Encounter or Roaming Session's transition to recap after a configured interval without newly evidenced activity while collection remains healthy. It remains revisable and does not establish that participating pilots departed.
_Avoid_: Confirmed battle end, fleet departure

**System Activity**:
Reported movement and destruction counts for a solar system over a stated observation interval. These counts describe activity, not the number or identities of pilots currently present.
_Avoid_: Live population, fleet count

**Discovery Source**:
An existing killmail subscription explicitly enabled to seed Battle Reports. Its filter retains its existing meaning; optional battle priority is independent of its Discord mention policy.
_Avoid_: Discord message history, implicit priority feed

**Contextual Killmail**:
An observed killmail included under a Battle Report's stated membership rules even when it did not match a Discovery Source. Its inclusion adds report evidence without independently authorizing a discovery alert.
_Avoid_: New discovery, complete battle coverage

**Discovery Area**:
The configurable set of systems eligible for automatic new-Battle-Report discovery. Geographic predicates may use security classes, regions, systems and light-year ranges; overlapping coverage counts once. Named deployment areas are configuration presets, not a fixed product boundary.
_Avoid_: Source channel regions, universe-wide alerts

**Ship Cache**:
A stock of ships positioned for future fleet deployment or reshipping near an area of interest.
_Avoid_: Active fleet

## Release and communication

**Deployment**:
A change to the version running in production.
_Avoid_: Release

**Runtime Health Snapshot**:
The watchdog's persisted assessment of current operational evidence. It reports runtime conditions but does not itself authorize or prevent collection and delivery.
_Avoid_: Deployment gate

**Rollout Health Gate**:
The release-specific decision predicate that authorizes Deployment, Production Verification, or Historical Enrichment Repair from current operational evidence.
_Avoid_: Runtime Health Snapshot

**Recovery Deployment**:
A bounded Deployment whose reviewed purpose is to repair a condition preventing the Runtime Health Snapshot from recovering. It may pass the Rollout Health Gate under the documented recovery exception, but it cannot authorize Historical Enrichment Repair until post-deployment recovery is demonstrated.
_Avoid_: Healthy deployment, emergency bypass

**Production Verification**:
A bounded check of the deployed behavior in the sole production environment using one controlled non-pinging message in the testing channel. It is not a canary environment, partial deployment, traffic split, or authority for automated remediation.
_Avoid_: Canary deployment, staging environment

**GitHub Release**:
The tagged public record of a deployable version, with technical release notes.
_Avoid_: Deployment, announcement

**Release Announcement**:
A user-focused message that explains a release's practical effect and links to its GitHub Release.
_Avoid_: Release notes

**Release Window**:
A recurring four-week checkpoint for drafting and publishing a release when the Changelog contains user-visible changes. A window may be recorded as skipped.
_Avoid_: Release date

**Changelog**:
The agent-prepared, human-reviewed factual source of user-visible changes, accumulated under Unreleased and converted into a versioned section for a GitHub Release.
_Avoid_: Git log, generated release notes

**Operator**:
The recipient configured to receive a health incident.
_Avoid_: Guild Administrator

**Guild Administrator**:
The person who configures zk-activity for a Discord guild.
_Avoid_: Operator

**Public Interface**:
The slash commands, configuration formats, and deployment requirements on which Guild Administrators rely.
_Avoid_: Internal implementation

**Release Candidate**:
A proposed deployable version tied to one exact revision, with its GitHub Release draft and reviewed release material.
_Avoid_: Published release, Deployment

**Release Review**:
The single human review of a Release Candidate's Changelog, GitHub Release draft, and Release Announcement Draft before the first public action.
_Avoid_: Per-entry approval

**Release Announcement Draft**:
A non-public proposed Release Announcement prepared for every Release Candidate. It may remain unpublished.
_Avoid_: Release Announcement

**Skipped Release Window**:
A Release Window without an eligible user-visible, human-reviewed Changelog entry. It is recorded outside the Changelog.
_Avoid_: Empty release

**Manual Rollback**:
A human-authorized Deployment of an earlier verified production version after failed Production Verification. It never proceeds automatically.
_Avoid_: Automatic rollback

**Failed Release Candidate**:
A Release Candidate whose Production Verification fails. Its public release and announcement remain unpublished while a human chooses Manual Rollback, a fix-forward Deployment, or cancellation.
_Avoid_: Failed release

**Release Verification**:
The required successful build, test, Compose configuration, and production evidence for a Release Candidate. Credentialed ignored visual tests supplement this evidence but do not routinely block it.
_Avoid_: Release Review

**External EVE Post**:
A human-approved message for a specifically selected EVE community, used only when a release offers prospective users a concrete reason to care.
_Avoid_: Routine release announcement

**Stable Public Interface**:
The Public Interface declared at v1.0.0 and thereafter versioned for compatibility.
_Avoid_: Internal implementation

**Release Pilot**:
The first successful Release Candidate after playbook approval. It publishes v1.0.0.
_Avoid_: Test release

**Cancelled Release Candidate**:
A Failed Release Candidate that a human cancels. Its GitHub Release draft remains unpublished, its tag remains an abandoned record, and any correction uses a new patch version.
_Avoid_: Reused release

**Release Cadence Log**:
The version-controlled record of every Release Window, stating its date, published version or skip, and reason.
_Avoid_: Changelog

## Feed diagnostics

**Feed Diagnostic Snapshot**:
A view of feed configuration, effective behavior, and timestamped operational evidence for a selected channel or server scope. Missing evidence remains explicit, and silence alone does not establish a cause.
_Avoid_: Subscription Inventory, live feed state

**Subscription Inventory**:
The configuration portion of a Feed Diagnostic Snapshot, identifying the subscriptions and their configured rules and actions within its scope.
_Avoid_: Feed Diagnostic Snapshot, runtime health

**Subscription Platform Binding**:
The association of a subscription instance and configuration revision with its platform, server, channel, and platform resource identity, used to reconcile configuration and diagnostic evidence.
_Avoid_: Subscription name, Discord member

## Local chat narrative language

**Local Message History**:
The retained, sanitized record of messages observed from live EVE Local chat logs, preserving in-game speakers, subjects, and available attribution evidence.
_Avoid_: Observation History, Discord message history

**Local Narrative**:
A summary of who spoke about whom and what in Local, informed by timestamped observations of character arrivals and departures, Local count changes, and corporation and alliance composition changes when available. Reported claims remain distinct from observed events.
_Avoid_: Verified intelligence, raw transcript

**Downtime Session**:
The downtime-to-downtime observation period over which Local conversation and presence changes are considered together.
_Avoid_: Client login session, calendar day
