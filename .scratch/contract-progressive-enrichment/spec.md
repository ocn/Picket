# Synchronous Progressive Enrichment for Public Contract Feeds

**Status:** ready-for-agent

## Problem Statement

Global Public Contract Intelligence alerts do not consistently preserve or display the strongest identity and Observed Location evidence available to the bot. A listing may contain a useful player-structure or solar-system location, but a later terminal embed can fall back to a bare region after that structure becomes inaccessible. Some historical repairs have exposed raw region identifiers instead of the established human-readable killfeed location style. Different Contract Subscription feeds can therefore present different levels of detail for the same class of event.

The operator uses these feeds to track adversarial contracts that do not involve the operator's character, corporation, or alliance. ESI's public contract data exposes the Issuer but not the Acceptor, and authenticated character-contract history cannot reveal unrelated adversarial contracts. Adding contract authorization machinery would not recover the missing counterparty.

The operator needs every contract feed to synchronously collect, retain, and render as much supported Issuer and location evidence as possible before its initial post. Every final embed must contain a human-readable region or a more specific solar-system or structure location, without raw identifiers, fabricated precision, empty placeholders, or later enrichment-driven edits. Existing retained Discord messages should be migrated in place when their retained evidence supports the fuller format.

## Solution

Apply one Progressive Enrichment pipeline and one shared contract presentation contract to every Global Public Contract Intelligence feed. Before preparing a new Discord delivery, resolve independent public identity and location resources in parallel, use the existing Structure Authorization only for player-structure requests that require it, and wait within a 30-second optional-enrichment budget. Persist each supported fact with its source and observation time before rendering.

Render the strongest evidenced location in a compact `in/on` form shared with the main killfeed. Prefer a current station or authenticated structure and its solar system; otherwise reuse observation-time or Last-Verified Location evidence; otherwise render the contract's human-readable observed region. Raw region, solar-system, structure, character, corporation, and alliance identifiers remain durable internal evidence and never substitute for a user-facing name.

Render the Issuer's resolved character name and available corporation or alliance context. Omit unavailable optional identity or location lines. Do not request character or corporation contract-history scopes, do not claim an Acceptor identity, and do not add authorization commands or UI.

After an initial message is posted, normal operation does not edit it merely because later enrichment becomes available. Historical Enrichment Repair remains a separate, explicit, bounded operator migration. It reuses Retained Enrichment Evidence, edits the original Discord message without mentions, and never creates a replacement post.

Replace the existing Turnur-area proximity subscription in the designated regional contract channel with region filtering for Metropolis, Heimatar, The Forge, Etherium Reach, and Perrigen Falls. Acceptance Confirmed events in that feed are eligible for the configured ping; the enrichment and presentation contract remains identical to every other contract feed.

## User Stories

1. As a contract-feed reader, I want every Public Contract embed to contain a human-readable region or a more specific location, so that every alert has actionable geographic context.
2. As a contract-feed reader, I want a solar-system name when supported by evidence, so that I can determine where the contracted ship was located.
3. As a contract-feed reader, I want a player-structure name when it is currently available through Structure Authorization, so that I can identify the exact location when ESI permits it.
4. As a contract-feed reader, I want an NPC station name when the Public Contract identifies one, so that known public locations remain precise.
5. As a contract-feed reader, I want the solar system and region displayed together when both are known, so that local and regional context are immediately visible.
6. As a contract-feed reader, I want location formatting to match the approved main-killfeed `in/on` grammar, so that Discord feeds remain visually consistent.
7. As a contract-feed reader, I want solar systems and regions linked through the established map-link convention, so that I can inspect the location quickly.
8. As a contract-feed reader, I want a currently resolved structure linked through the established location-link convention, so that the structure is directly actionable.
9. As a contract-feed reader, I want observation-time location evidence retained through Acceptance Confirmed, so that a terminal embed does not lose information that was known while the contract was public.
10. As a contract-feed reader, I want Last-Verified Location evidence clearly dated, so that I can distinguish retained evidence from a current structure lookup.
11. As a contract-feed reader, I want a generic player-structure line only when retained evidence supports that context, so that the embed does not imply a currently resolved structure name.
12. As a contract-feed reader, I want a region-only fallback when no more specific evidence exists, so that the embed remains useful without inventing precision.
13. As a contract-feed reader, I never want a raw region identifier such as `10000042`, so that internal identifiers are not mistaken for location names.
14. As a contract-feed reader, I never want raw structure, solar-system, character, corporation, or alliance identifiers used as presentation labels, so that embeds remain readable.
15. As a contract-feed reader, I want unavailable optional facts omitted, so that embeds do not contain repetitive `Unknown`, `Unresolved`, or empty fields.
16. As a contract-feed reader, I want the Issuer identified by resolved character name, so that I can identify who created the contract.
17. As a contract-feed reader, I want available Issuer corporation or alliance context, so that I can associate the contract with an adversarial group.
18. As a contract-feed reader, I want identity labels to state the party's role, so that the Issuer is not confused with an Acceptor.
19. As a contract-feed reader, I want established identity links used when the presentation surface supports them, so that I can investigate the Issuer without exposing a raw ID.
20. As a contract-feed reader, I do not want an Acceptor field when ESI cannot identify that party, so that absence of evidence is not rendered as useless content.
21. As a contract-feed reader, I do not want the Issuer described as the buyer or purchaser, so that contract direction remains factual.
22. As a contract-feed reader, I want offered-item and requested-item semantics preserved, so that a Sale Confirmed is not confused with a Purchase Confirmed.
23. As a contract-feed reader, I want the existing observed acceptance or closure window retained, so that public evidence is not presented as an exact transfer timestamp.
24. As a contract-feed reader, I want the same enrichment in listing, Sale Confirmed, Purchase Confirmed, Expired, and Closed — Outcome Unknown embeds, so that terminal state does not reduce data quality.
25. As a subscriber to any Contract Subscription feed, I want the same presentation revision and evidence precedence, so that channel configuration cannot create inconsistent data fullness.
26. As a regional-feed subscriber, I want Metropolis, Heimatar, The Forge, Etherium Reach, and Perrigen Falls monitored instead of the former Turnur-area light-year filter, so that the feed covers the selected travel and staging regions.
27. As a regional-feed subscriber, I want Acceptance Confirmed events to use the configured ping action, so that completed contracts receive immediate attention.
28. As a regional-feed subscriber, I want non-acceptance events to gain no new ping eligibility from this change, so that enrichment does not broaden notification behavior.
29. As an operator, I want independent ESI enrichment calls parallelized, so that richer embeds do not add unnecessary serial latency.
30. As an operator, I want optional live enrichment bounded to 30 seconds before the best supported payload is prepared, so that Discord delivery cannot wait indefinitely.
31. As an operator, I want the required human-readable region invariant to defer an initial delivery rather than emit a raw identifier, so that timeouts never corrupt presentation.
32. As an operator, I want authenticated player-structure requests to carry the existing scoped bearer token, so that accessible structure and system evidence can be captured.
33. As an operator, I want public contract, map, affiliation, and name requests to remain public and never carry the structure resolver token, so that authorization stays narrowly scoped.
34. As an operator, I want denied or unavailable structure access to degrade only location precision, so that public collection, matching, and delivery continue.
35. As an operator, I want current, observation-time, and Last-Verified Location evidence retained with provenance, so that presentation remains auditable across lifecycle transitions and restarts.
36. As an operator, I want useful identity and location evidence retained indefinitely until an explicit retention policy is introduced, so that historical analysis and repair remain possible.
37. As an operator, I do not want normal messages dynamically rerendered when later enrichment appears, so that ordinary operation has one synchronous presentation decision.
38. As an operator, I want delivery retry and repair state to remain available for Discord transport failures, so that the no-dynamic-enrichment rule does not weaken delivery reliability.
39. As an operator, I want Historical Enrichment Repair to use only retained evidence and permitted public name lookup, so that historical migration cannot trigger a new authenticated structure crawl.
40. As an operator, I want historical embeds edited in place, so that their Discord message identifiers and existing links remain stable.
41. As an operator, I want historical edits to contain no allowed mentions, so that presentation migration cannot ping users or roles.
42. As an operator, I want deleted, retired, prepared, malformed, or permanently failed identities excluded from historical migration, so that old alerts cannot be recreated or corrupted.
43. As an operator, I want historical candidates rediscovered through a dry run immediately before queueing, so that live database state rather than an old count defines the bounded batch.
44. As an operator, I want a historical batch to stop at a permanent Discord failure or persisted Retry-After boundary, so that migration remains controlled.
45. As an operator, I want production canaries verified before the remaining bounded migration, so that message identity, data fullness, formatting, and absence of pings are proven live.
46. As an operator, I do not want new character-contract scopes, authorization profiles, slash commands, or UI, so that the system does not add machinery incapable of identifying unrelated adversarial Acceptors.
47. As an operator, I want the existing Structure Authorization left otherwise unchanged, so that this feature cannot disrupt its credential or access boundary.
48. As an operator, I want all presentation facts derived from public evidence, Structure Authorization, or Retained Enrichment Evidence, so that the bot never guesses who or where a contract involved.

## Implementation Decisions

- The change applies to every Global Public Contract Intelligence feed. Contract Subscription filtering and Contract Event Actions decide eligibility and ping behavior, but they do not alter enrichment depth or presentation format.
- One shared Progressive Enrichment service owns pre-delivery identity and Observed Location assembly for every event kind. Feed-specific renderers or enrichment policies are not introduced.
- Public regional contract observations remain the universal evidence baseline. The public summary supplies the Issuer identifier, Issuer corporation identifier, start and end location identifiers, observed region, contract dates, type, and financial facts.
- Acceptor identity is not available from the public regional contract record. The system will not request character or corporation contract-history scopes because those routes expose only contracts involving the authorizing party and therefore cannot reveal the unrelated adversarial Acceptors in scope.
- Structure Authorization remains limited to `esi-universe.read_structures.v1`. Only the player-structure operation receives its bearer token. Public discovery, manifests, lifecycle probes, universe names, affiliations, stations, systems, constellations, and regions never receive it.
- Independent enrichment work is executed concurrently where ESI ordering does not impose a dependency. Issuer affiliation/name resolution, item-name resolution, and the first location-resolution step may run in parallel. Dependent station-to-system-to-constellation-to-region requests remain ordered.
- Optional live enrichment has a 30-second wall-clock budget per event presentation. No initial Discord delivery is prepared before the enrichment barrier completes or expires.
- At the optional-enrichment deadline, rendering uses the strongest supported evidence already available. Missing optional identity, structure, range, or affiliation facts are omitted.
- A human-readable observed region is mandatory. Region names first use the loaded system catalog, then retained region-name evidence, then the unauthenticated region operation. If no human-readable name is available, initial delivery remains deferred; a raw region identifier is never rendered.
- Observed Location precedence is:
  1. a public NPC station and its public solar system;
  2. a currently authenticated player structure and its solar system;
  3. the most specific location retained from the Contract Observation while the contract was public;
  4. a Last-Verified Location, with its verification date shown;
  5. the human-readable region from the regional observation.
- A later lower-quality result cannot replace stronger Retained Enrichment Evidence. Evidence selection remains deterministic across retries and restarts.
- A listing requests presentation location even when its matching Contract Subscription uses only a region or identity filter. Matching must not suppress enrichment required by presentation.
- A terminal event begins with its persisted observation-time context. During its synchronous pre-delivery phase it may revalidate missing or expired location evidence, but failure cannot erase the retained snapshot.
- Successful current structure evidence may render the structure name. Last-Verified Location renders the supported solar system and region, uses `Player-owned structure` rather than a stale structure name, and adds a compact verification-date line.
- The presentation grammar is progressive:
  - current specific evidence: `in: <solar system> (<region>)`, followed by `on: <station or structure>`;
  - Last-Verified Location: `in: <solar system> (<region>)`, `on: Player-owned structure`, and `location verified: <date>`;
  - region fallback: `in: <region>`.
- The established linked `in/on/range` formatter is reused. Security status, emojis, raw evidence-class labels, raw HTTP results, and alternate location styles are not added.
- Issuer character, corporation, and alliance identifiers are retained internally with provenance. Presentation uses resolved names and the contextual `issuer` role; established identity links are used where the existing embed surface supports them.
- The Issuer affiliation is the Observed Affiliation from the public observation. It is not represented as an affiliation at the later acceptance time.
- No empty Acceptor, corporation, alliance, structure, solar-system, or verification fields are rendered. Optional lines exist only when they carry actionable information.
- Existing lifecycle semantics remain authoritative. Acceptance Confirmed does not become an exact acceptance timestamp, and the existing observation window remains visible.
- Live presentation becomes immutable with respect to later enrichment once Discord has accepted the initial post. The normal collector does not schedule enrichment-driven message edits.
- Discord transport retries, persisted Retry-After handling, idempotent delivery, and repair of actual delivery failures remain operational concerns and are not considered enrichment-driven rerendering.
- Historical Enrichment Repair remains an operator-only, capability-protected, channel-scoped operation with dry-run and bounded selection. It queues through the durable repair worker and never calls Discord directly from the operator interface.
- Historical reconstruction uses the retained event, Contract Observation, Retained Enrichment Evidence, structure cache, system catalog, and unauthenticated region-name lookup. It does not make a new authenticated structure request.
- Historical selection includes only sent, retained, outdated deliveries with an existing Discord message identifier and valid retained event/subscription data. It excludes deleted, retired, prepared, malformed, active-repair, and permanently failed identities.
- Historical repairs preserve the Contract Event identity, delivery nonce, Discord message identifier, ping eligibility, and original channel. The edit payload permits no mentions and cannot create a replacement post.
- Historical candidate state is live. Every mutation is preceded by a fresh dry run; the operator's bounded approval is not silently expanded when the eligible set changes.
- The designated regional subscription replaces its Turnur-area light-year condition with a union of Metropolis, Heimatar, The Forge, Etherium Reach, and Perrigen Falls. Sale Confirmed and Purchase Confirmed use the approved acceptance ping action. Other event actions remain explicit and gain no new ping behavior from this change.
- Retained Enrichment Evidence is stored with source, observation or verification time, location identifiers, resolved names, and evidence class. It is retained indefinitely until an explicit retention policy replaces that decision.
- PostgreSQL remains authoritative for Contract Observations, Retained Enrichment Evidence, lifecycle events, subscriptions, prepared deliveries, and Historical Enrichment Repair state, consistent with the persistence ADR.
- No additional token encryption, multi-user authorization, per-channel consent, security workflow, or credential UI is introduced. Existing credential validation and storage behavior remain unchanged.
- The next presentation change increments the contract presentation revision. All live event kinds and Historical Enrichment Repair use the same revision.
- Deployment follows the existing health and rollout gate: validate Compose configuration, deploy the reviewed revision, verify collector and resolver health, dry-run historical scope, edit representative canaries, then execute only the explicitly approved bounded remainder while monitoring failures and Retry-After state.

## Testing Decisions

- Tests assert external behavior rather than private helper calls, SQL statement shape, or internal task scheduling.
- The primary and only new seam is a production-shaped Contract Collector cycle using controlled ESI HTTP responses, real temporary PostgreSQL, Contract Subscription evaluation, a controlled Structure Authorization resolver, serialized Discord delivery inspection, and retained-state inspection.
- The primary seam extends the existing collector-level compact-contract-embed test rather than introducing a parallel renderer seam.
- Table-driven cases at the primary seam cover each lifecycle kind and every location tier: public NPC station, current authenticated player structure, retained observation-time structure/system, Last-Verified Location, and region-only fallback.
- The primary seam verifies that all Contract Subscription variants receive the same presentation revision and enrichment depth for equivalent evidence.
- The primary seam verifies synchronous behavior by holding independent controlled ESI responses, proving that no Discord delivery is prepared before the enrichment barrier, and then releasing them in an order that demonstrates safe parallelism.
- The primary seam verifies the optional-enrichment deadline by withholding an optional response and asserting that the delivery contains the best supported facts without an empty placeholder.
- A required-region case withholds every human-readable region source and verifies that the initial delivery is deferred instead of rendering a raw identifier.
- Structure wire assertions verify that the scoped bearer token appears only on the authenticated structure request. Public contract, name, affiliation, station, system, constellation, and region requests must not carry it.
- Identity cases verify resolved Issuer character, corporation, and alliance presentation, retained numeric identifiers, role labeling, omission when names are unavailable, and absence of an Acceptor field.
- Terminal cases verify that observation-time and Last-Verified Location evidence survives disappearance, lower-quality current evidence cannot overwrite it, and the public observation window remains unchanged.
- Location presentation assertions use independent expected `in/on` strings and links; they do not call production formatting helpers to construct their expected values.
- Historical cases use the existing operator rerender and Discord-wire seams as supporting regression coverage. They verify fresh dry-run selection, retained-only reconstruction, stable message identifiers, empty allowed mentions, PATCH without replacement POST, revision fencing, bounded queueing, Retry-After persistence, and permanent-failure isolation.
- A historical authenticated-wire assertion proves that rerendering performs no structure request while still allowing unauthenticated lookup of a missing human-readable region name before queueing.
- Migration coverage starts from both a clean database and the current production migration ledger, then verifies backward-compatible retained payloads and evidence provenance through public store behavior.
- Existing location-evidence ordering, structure-resolver authorization, collection rate-limit, restart, terminal lifecycle, compact renderer, and repair tests are prior art and remain part of the regression suite.
- Automated tests use loopback HTTP fixtures and temporary PostgreSQL and require no live ESI, Discord, or SSO credentials.
- Final verification includes the focused primary seam, the complete serial contract suite, safe library tests, formatting, typechecking, Clippy, and quiet Docker Compose configuration validation.
- Production validation re-queries health rather than relying on historical counts. It verifies regional progress, deferred-backlog movement, resolver readiness, prepared delivery count, unresolved permanent failures, Discord errors, duplicate-post indicators, and rate-limit failures before historical work.
- Live canary acceptance verifies the original Discord message identifiers, no mentions, correct Issuer presentation, human-readable region, strongest retained specific location, no raw identifiers, no replacement posts, and the approved compact style.

## Out of Scope

- Discovering or displaying an Acceptor for unrelated adversarial Public Contracts.
- Requesting `esi-contracts.read_character_contracts.v1`, corporation contract-history scopes, or alliance-specific contract access.
- Adding authorization profiles, multi-user authorization, slash commands, consent UI, credential dashboards, or per-channel authorization.
- Redesigning credential storage, encryption, rotation, or security policy.
- Claiming that Structure Authorization can resolve structures outside the authorized character's ACL or docking access.
- Requiring a player-structure name when only solar-system or regional evidence is available.
- Rendering raw identifiers as a fallback for unresolved names.
- Guessing a location, Acceptor, affiliation, acceptance time, or terminal outcome.
- Replacing the observed acceptance window with a synthetic exact timestamp.
- Enrichment-driven edits to already-posted messages during normal operation.
- Creating replacement Discord posts when historical edits fail.
- Rewriting deleted, retired, malformed, prepared, active-repair, or permanently failed historical deliveries.
- Triggering new authenticated structure collection during Historical Enrichment Repair.
- Changing contract lifecycle classification, Contract Event identity, item direction, financial semantics, history aggregation, or the suppression of unsupported want-to-buy listing alerts.
- Broadening ping behavior beyond the explicitly configured Acceptance Confirmed actions in the designated regional feed.
- Changing the killmail feed's presentation, filtering, or enrichment behavior.
- Introducing a runtime retention job; indefinite retention remains until it becomes an operational problem and a separate policy is approved.

## Further Notes

- The ESI public regional contract schema exposes `issuer_id`, `issuer_corporation_id`, and location identifiers but no `acceptor_id`: https://esi.evetech.net/meta/openapi.json
- CCP documents that authenticated ESI access is limited to the character and explicitly granted scopes: https://developers.eveonline.com/docs/services/sso/
- CCP's ESI overview distinguishes public and authenticated routes and makes route scopes authoritative: https://developers.eveonline.com/docs/services/esi/overview/
- The authenticated structure operation is access-qualified; an inaccessible structure returns Forbidden: https://developers.eveonline.com/api-explorer#/operations/GetUniverseStructuresStructureId
- ESI clients must respect cache and rate-limit boundaries while parallelizing requests: https://developers.eveonline.com/docs/services/esi/best-practices/ and https://developers.eveonline.com/docs/services/esi/rate-limiting/
- Discord edit payloads and allowed-mention behavior remain governed by: https://docs.discord.com/developers/resources/message#edit-message
- Discord rate limits and Retry-After behavior remain governed by: https://docs.discord.com/developers/topics/rate-limits
- This specification builds on the existing Accessible Contract Lifecycle Embeds specification and its deployment/canary issue. It does not silently replace their evidence-safe lifecycle semantics or rollout protections.
- The domain decision is recorded by ADR 0003: Global Public Contract Intelligence remains public and structure-only; authenticated party-history enrichment is deliberately excluded.
