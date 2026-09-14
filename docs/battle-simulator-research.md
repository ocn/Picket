# EVE battle visualization: research and design interview

Research date: 2026-09-04. Design updated: 2026-09-05. This records findings and settled/open decisions, not an approved implementation plan.

## Existing tools

| Tool | Documented overlap |
| --- | --- |
| [God's Eye View](https://github.com/bilawalsidhu/gods-eye-view) | Reference experience: navigable 3D spatial intelligence with live contacts and contextual layers. |
| [Ashy's pyBattleVis](https://ashy.vargur.dev/zkillboard-3d-br-viewer-v1-0/) | Imports a zKillboard battle report and renders ship-loss markers using ESI positions. The author describes a static scene, not continuous ship telemetry. |
| [Derelict](https://git.ignore.pl/derelict/derelict/) | 3D post-battle wreckage-field visualization. |
| [EO-Map](https://eo-map.com/live-kills/) | Live reported kills on a 3D New Eden map, event history and adjustable-speed replay. This is system-scale activity. |
| [killmail.app](https://killmail.app/) | Interactive battle reports, scrollable timelines, overviews and live killmail feeds. Its indexed site description establishes these features; a moving 3D battle grid was not verified. |
| [WarBeacon](https://forums.eveonline.com/t/warbeacon-battle-report-tool-focused-on-advanced-analysis/497464) | Battle analysis with a loss timeline and editable alliance sides. |

These establish substantial partial overlap. This search did not establish an existing product combining automatic ongoing-battle detection with a ship-level 3D reconstruction. Absence from the search does not establish nonexistence. Tools were researched through their documentation, not functionally tested.

## Evidence boundaries

[CCP's killmail documentation](https://support.eveonline.com/hc/en-us/articles/11730655033884-Killmails) describes loss events, participants, destroyed-ship equipment, time/location and damage totals. It explicitly states that third-party sharing is incomplete. [zKillboard's feature documentation](https://zkillboard.com/information/features/) also documents configurable publication delays.

Design implication: recent feed arrival is not proof that a fight is still ongoing. A rendered reconstruction must distinguish reported facts from assumptions about motion, presence and combat between losses.

The current [killmail models](../src/models.rs) retain optional victim death coordinates, attacker identities/types/weapons and aggregate damage, but no attacker coordinates, movement histories, per-shot timestamps, arrival/departure times or health histories. Exact motion cannot be recovered uniquely from these records. [EVE's map guide](https://developers.eveonline.com/docs/guides/map-data/) establishes that killmail coordinates are system-relative, in meters; universe and system coordinate conventions differ.

## Existing repository capabilities

The current checkout has R2Z2 and RedisQ adapters under `src/feed/`, enrichment/processing in `src/pipeline.rs`, ESI metadata and nearest-celestial lookups, subscription filtering and Discord reporting. The older AGENTS.md architecture description does not reflect the current feed paths. The activity dashboard document under `docs/superpowers/plans/` is a plan; no `src/activity` implementation exists in this checkout.

[Current zKillboard API documentation](https://zkillboard.com/api/docs/) describes R2Z2 records and possible repeated killmails. A prospective battle history would need deduplication and distinct event/publication/receipt times; current `R2z2KillmailResponse` does not retain the documented publication timestamp.

## Settled product direction

Primary audience: the user's EVE alliance leadership and FC team, studying how activity changes and deciding whether to bring a fleet, position Ship Caches, or install clones nearby. The product serves tactical discovery and longer-term deployment planning. General alliance membership or membership in the source Discord guild does not by itself grant UI access.

Primary entry: automatic discovery of unfolding battles and newly observed fleets, including their encounters elsewhere. A live dashboard and Discord alerts serve the same discovery purpose. After-Action Recaps remain in scope.

Historical backfill is useful. Public archives exist, but their completeness and suitability for evaluating historical alert timing require investigation.

Accepted 2026-09-05:

- Q2, reconstruction: start with observed positions, encounter timelines and explicitly inferred connections between sightings. Estimated ship motion is deferred until it supports a specific tactical question.
- Q4, recap boundaries: individual Encounter reports plus Roaming Session recaps linking encounters across locations.
- Q5, fleet continuity: recurring pilots, elapsed time and plausible travel support a provisional Fleet Track; affiliation alone is insufficient.
- Q6, initial geography: use the area configured for [Discord channel 1117504110611157034](https://discord.com/channels/888224317991706685/1117504110611157034) in guild 888224317991706685. Resolve its actual geographic predicate rather than assuming that the user's word "regions" means entire EVE administrative regions. Retain outside-area encounters for linked groups, as previously proposed.

See [ADR 0007](adr/0007-preserve-observations-in-battle-reconstruction.md) for the reconstruction evidence boundary.

Also accepted 2026-09-05:

- Q7: provisional candidates appear immediately on the dashboard; Discord discovery notifications require corroboration or an explicitly prioritized event. Initial eligibility is settled by Q11 below.
- Q8: configurable provisional closure after 20 minutes of quiet for an Encounter and 60 minutes for a Roaming Session. Collection outages do not establish quiet; closure does not prove that pilots left.
- Q9: late killmails revise the existing recap image and report with a visible revision time. Historical backfill does not generate fresh battle alerts.
- Q10: initial deployment planning shows activity trends, recurring groups and travel distance around chosen staging locations. Private ship-cache inventory and installed-clone tracking are deferred.

Q11–Q15 accepted with the user's access correction:

- Q11: initial Discord corroboration requires two linked player-ship losses within ten minutes involving at least five distinct attacking pilots. The initial capital bypass is subsequently expressed through explicit priority-source configuration in Q25, rather than a separate built-in capital list. Pods do not satisfy the two-loss requirement, and NPC-only losses do not trigger discovery notifications. These remain configurable heuristics to evaluate, not validated detection rates.
- Q12: design for a dedicated fleet-intelligence channel. Ordinary discoveries and recaps have no mentions; prioritized discoveries may mention an opted-in role under a cooldown. Channel creation and message delivery are future rollout work.
- Q13: regional 3D activity map alongside a ranked encounter list, with system/encounter drill-down and a scrub-able timeline. Recaps use a loss timeline and composition summary, with PNG previews and SVG/full-resolution exports.
- Q14: the UI audience is alliance leadership and the FC team only. Discord users do not receive UI access until hardening is worked out later. Authentication, exact role mappings and shared UI release are deferred to that work; source-guild membership is not a sufficient access rule.
- Q15: initial backfill covers 90 days across the selected discovery area (expanded by Q23 and subsequently clarified as configurable), comparing similar weekdays/hours and displaying coverage gaps. This sets an initial import horizon, not an automatic deletion policy.

Latest Q16–Q19 responses:

- Q16 settled by the latest clarification: keep Encounters separate and avoid implicit assumptions. Comparisons may show their supporting evidence, but cannot automatically merge rosters or silently assert persistent fleet identity, movement or continuous presence.
- Q17 accepted with correction: a supporting combat event must be no more than **five minutes** old at live-alert evaluation. Capital priority does not bypass freshness; older arrivals update history/recaps. This age limit is separate from the ten-minute corroboration window and 20/60-minute closure thresholds.
- Q18 accepted with an explicit low-noise requirement: distinct corroborated encounters and newly observed capital escalations may produce follow-ups; routine losses edit existing messages. Retain the proposed ten-minute destination-channel role-mention cooldown and no mentions for revisions/recaps. A role-mention cooldown alone does not control message volume; deduplication/coalescing must be evaluated too.
- Q19 accepted: use an evolving Battle Report for each separate Encounter, with explicit comparisons between reports. This replaces presumed fleet lifetimes; neither the original 24-hour horizon nor the later size-dependent 30-minute/two-hour expiry applies. Outside-region notification relevance still needs a specific rule. See [ADR 0008](adr/0008-track-encounters-through-evolving-battle-reports.md).

Latest Q20–Q22 responses:

- Q20: group by time and external battle-report modeling. The proposed bespoke spatial/participant-clustering policy was not accepted as the primary boundary model. Inspect established report implementations and make the chosen time/system selection model explicit; external report membership is a model/source assertion, not proof of one fleet. Reference implementations have been inspected below; the local single-system/healthy-quiet adaptation was accepted in Q28 on 2026-09-06.
- Q21: qualify a report as a new BR. The proposed five-attacker/50-percent-overlap/one-hour rule was not accepted as an additional alert gate. The user subsequently expanded geography to western/northern lowsec, the specifically named nullsec regions, and an additional eight-light-year area around 3G-LFX. A report independently qualifies within the selected area; this is not authorization for unfiltered universe-wide Discord alerts.
- Q22 settled: twenty minutes of healthy quiet triggers a provisional after-action recap. Team/outcome assessments appear only at that stage, are labeled when inferred, and leave the winner undetermined when evidence is insufficient. Loss efficiency alone does not prove tactical victory; new evidence can revise the recap.
- Q23 settled: all lowsec, full Etherium Reach and Perrigen Falls, plus the additional low/nullsec area within eight light-years of 3G-LFX, with highsec excluded. This replaces the earlier five-region-only discovery scope.

## UI distribution boundary

The user explicitly defers access to the UI through Discord until a later hardening phase. Initial Discord alerts and image recaps must be usable without dashboard access and omit links/buttons to an unreleased dashboard or internal report/download service. The dashboard can be designed and developed privately; neither UI exposure nor an authentication rollout is authorized by this design session. Standalone recap attachments remain part of the Discord design. Restricting future UI access does not itself specify which Discord roles may read a recap channel; exact audience mapping belongs to rollout/hardening.

## Original channel geography (reference, subsequently expanded)

The channel's active production subscription was inspected through a read-only database transaction at 2026-09-05T04:22:05Z. Its actual geographic filter covers these complete regions:

| Region | EVE region ID |
| --- | --- |
| Metropolis | 10000042 |
| Heimatar | 10000030 |
| The Forge | 10000002 |
| Etherium Reach | 10000027 |
| Perrigen Falls | 10000066 |

The active subscription's ID is `supercap-turnur-kurniainen-8ly`, but its filter has no light-year predicate; its last update was 2026-08-19T20:12:57Z. Older deployment notes and Discord category names refer to eight light-years around Turnur/Kurniainen and must not replace the current filter. The user selected this channel's geography, not its supercapital-contract content filters, and then broadened the area as below. No source-channel configuration has been changed.

## Configurable discovery geography and initial preset

Coverage must be configurable, not hardcoded to named regions or an anchor system. The previously accepted selection is an initial deployment preset: **all lowsec**, **full Etherium Reach** (10000027), **full Perrigen Falls** (10000066), and **the additional low/nullsec area within eight light-years of 3G-LFX**, with highsec excluded. The radius is an additional area, not an intersection restricting those entire regions. The user's "all" resolves the earlier western/northern lowsec ambiguity.

Anchor verified through [ESI](https://esi.evetech.net/latest/universe/systems/30002302/): 3G-LFX, system 30002302, constellation PGPJ-8 (20000338), region Etherium Reach, raw security −0.1453966349363327. Geographic distance uses system coordinates, not stargate hops or an assertion of actual ship jump capability. [EVE's map guide](https://developers.eveonline.com/docs/guides/map-data/) defines one light-year as 9,460,000,000,000,000 meters for game jump calculations. The existing processor's conversion uses 9,460,730,472,580,800; boundary consistency must be addressed in implementation, not silently copied or changed during design.

Initial preset predicate: lowsec OR nullsec in Etherium Reach/Perrigen Falls OR nullsec within eight light-years of 3G-LFX. Lowsec inside the sphere is already included by the first branch. Highsec is excluded. De-duplicate overlap between branches. The accepted ninety-day backfill horizon applies to the configured discovery area. Numeric security alone must not accidentally expand the sphere branch to wormhole or abyssal space.

The [system-security guide](https://developers.eveonline.com/docs/guides/system-security/) defines lowsec by raw security 0 < x < 0.45; do not use a naive raw <0.5 cutoff. Scope and security classification must also distinguish special-space systems from ordinary nullsec.

Provisional radius enumeration from [zKillboard's 2026-05-23 map snapshot](https://github.com/zKillboard/zKillboard/blob/16df61a297c269cd3ade341834ed88a696077a99/public/data/mapSystems.jsonl): 257 systems within the inclusive eight-light-year sphere, all classified nullsec in that snapshot—Etherium Reach 91, Malpais 60, The Kalevala Expanse 47, The Spire 28, Oasa 16, Perrigen Falls 15. This establishes a concrete provisional footprint; regenerate against current official static data before implementing the final filter. The runtime systems cache is incomplete and cannot establish exhaustive coverage. Full Etherium Reach/Perrigen Falls coverage remains in addition to this sphere; the sphere's rows must not replace those full-region branches.

## Added scope: after-action recaps

User-requested scope: design Discord embed alerts containing generated battle-report image summaries, at high resolution or as SVG, to recap long chains of alerts. This complements ongoing discovery and covers both individual Encounters and Roaming Sessions.

Accepted presentation direction: a concise embed with a readable image preview and standalone exports. Links to the detailed internal report/replay are deferred until the UI hardening phase permits access. Candidate image content includes system and event-time window, observed participants and affiliations, ship losses and ISK lost, a loss timeline, major losses, and a spatial overview where positions are available. Side assignments, spatial reconstruction and coverage limits must follow the eventual evidence model.

Format direction: generate an SVG master and a high-resolution PNG preview/export from the same report snapshot. Discord documents PNG among supported embed attachment formats; SVG should be offered as a standalone file rather than relied upon for inline display. Source: [Discord file-upload documentation](https://docs.discord.com/developers/reference#uploading-files). Layout must remain readable at normal Discord display size; additional resolution alone does not solve dense labels.

Subsequent Q24–Q32 settle recap eligibility for every notified encounter, local deduplication, contextual killmail inclusion, retention and fresh action after provisional closure. Quiet thresholds and in-place correction of delayed evidence are settled above. Discord uses standalone attachments; hosted download access remains deferred with UI hardening. A general channel digest combining unrelated activity has not been requested or accepted.

## Activity landscape and historical data

The user explicitly requests ESI system activity as an additional mapping layer. Proposed views combine recent encounters with system activity trends so users can distinguish a short spike from recurring activity near potential staging locations.

Verified against the [official ESI OpenAPI schema](https://esi.evetech.net/meta/openapi.json):

| Endpoint | Evidence supplied |
| --- | --- |
| `/universe/system_kills/` | `system_id`, `ship_kills` (player ships), `pod_kills`, `npc_kills`. |
| `/universe/system_jumps/` | `system_id`, `ship_jumps`. No pilot identities, origin/destination pairs or fleet membership. |

Both describe the last hour ending at the response's Last-Modified timestamp, exclude wormhole space, list only nonzero systems, and specify a 3600-second cache lifetime. The schema does not establish whether ship_jumps includes particular movement mechanisms; do not label it gate-only traffic without further evidence. Do not treat repeated cached responses as independent hours or absent wormhole entries as zero activity. These aggregates support activity baselines; killmails supply the available participant evidence for encounter discovery.

[EVE Ref's dataset catalog](https://docs.everef.net/datasets/) lists downloadable killmails, system-jumps and system-kills archives with machine-readable directory indexes. Its [system-kill history](https://data.everef.net/system-kills/history/) and [system-jump history](https://data.everef.net/system-jumps/history/) contain multi-year data, with continuity still to be assessed. [zKillboard's current API documentation](https://zkillboard.com/api/docs/) documents daily historical killmail IDs/hashes, full raw daily archives, and per-day totals. Backfill therefore has identified sources; exact coverage, missing intervals, ingestion cost and publication-time availability remain to be measured.

Historical event-time replay and replay of what an observer could have known at the time are different evaluation tasks. A killmail archive does not automatically establish its original feed delivery time. Historical replay must not leak later evidence into an evaluation of live discovery.

The earlier Discord activity-dashboard plan explicitly excluded correlation and external UI. This new design includes both as requested; it is not an instruction to execute or silently rewrite that earlier implementation plan. Existing contract-specific ADRs do not define the scope of this battle-intelligence design.

## Configurable feeds and local report identity (Q24–Q25)

The user clarified that coverage must be configurable and proposed reusing existing feed configurations as the source of discovery events. Reuse the current subscription predicates rather than introducing a competing filter language or requiring another capital-class list. The earlier Q25 capital-class recommendation was not accepted. Q25 follow-up is accepted: existing subscriptions opt into discovery and may explicitly designate priority matches that bypass corroboration. Existing mention policies do not establish battle priority. Actual source IDs are deployment configuration.

Repository inspection supports this integration: `src/pipeline.rs:85` constructs the complete enriched killmail and `:91` evaluates subscriptions. Matched results retain the guild, cloned subscription and filter match alongside the full killmail (`:113`). `src/config.rs:461` defines subscriptions with an ID, description, recursive filter and destination/ping action. These internal matches are available independently of Discord delivery. Missing ping settings, mention cooldown and ping age limits do not remove the match. Completely unmatched events lose their full payload at `src/pipeline.rs:95`.

Accepted integration direction: subscribe the battle detector to internal match results for selected existing feeds; preserve source subscription identity and evaluated configuration with the observation. Discord delivery success must not determine discovery eligibility. Multiple matches of one killmail add provenance, not duplicate losses. A small source-selection and battle-output configuration may still be needed; this is distinct from duplicating existing event filters. Existing recursive geography/ship/value/standing predicates must retain their semantics, rather than stripping out apparent geographic nodes from arbitrary And/Or/Not trees. How to supply a geographic activity-map scope when a feed mixes geographic and non-geographic alternatives remains an explicit configuration design question.

Q27 is accepted: source matches seed discovery, while other observed killmails inside the report's explicit membership window supply labeled context, even when they do not match a selected source feed. Supporting that requires access to enriched events before the unmatched payload is discarded, plus retained or backfilled observations where available. Including such context must not automatically make those events eligible for independent discovery alerts. Preserve both the original feed match and each contextual inclusion reason. This does not promise complete coverage of an actual battle.

For Q24, the user supports using related-report URLs as potential deduplication evidence while preferring local capability independent of upstream. Proposed local identity: store each killmail once by ID, keep a stable local report ID with explicit time/system membership and revisions, and retain external URLs as references with their source definitions. Matching URLs and overlapping killmail sets identify candidate duplicate coverage; neither automatically merges separate encounters. Adjacent windows, split reports and partial coverage require preserving ambiguity. Record per-destination notification state so overlapping subscriptions do not duplicate a recap in the same destination. A fresh qualifying encounter after closure versus a historical correction still needs an explicit membership rule.

The current embed constructs `https://warbeacon.net/br/related/{system}/{timestamp}/` locally (`src/discord_bot.rs:2171`). This is a navigational reference, not verified upstream report membership. No local battle ledger, report identity store or durable Discord-message outcome store exists yet. The prior activity-dashboard plan remains unimplemented; its proposed observation after sends would also miss unmatched context. These findings establish an integration seam, not implementation completion.

## Q25–Q27 accepted

- Q25: existing subscriptions opt into discovery with optional explicit priority designation; reuse their filters. Priority bypasses corroboration, not five-minute freshness. Source ping policy does not implicitly determine battle priority.
- Q26: order live reports by configured priority, freshness and observed scale, with ranking factors visible. Historical activity remains a separate view/layer. Every notified encounter receives a recap, including small engagements.
- Q27: feed matches seed reports; other observed losses within the explicit report scope may contribute labeled context without independently triggering discovery alerts. Preserve inclusion reasons and source-match provenance.

The current consolidated design is in [battle-simulator-design.md](battle-simulator-design.md). Q28–Q32 were accepted on 2026-09-06: single-system reports with twenty-minute healthy-quiet closure; prospective feed edits with historical provenance preserved; explicit map coverage for ambiguous mixed filters; configurable ninety-day detailed evidence and one-year compact reports/activity retention subject to a storage estimate; and ordinary-feed availability with visible battle gaps and independent recovery. Product interview decisions are settled; concrete engineering wiring and rollout configuration belong to implementation planning. The accepted 60-minute Roaming Session closure is retained for recap collections; it is not evidence of fleet persistence. UI exposure and authentication rollout remain deferred to later hardening.

## Battle reports as a tracking unit

[zKillboard's related-report endpoint](https://zkillboard.com/api/docs/) returns report data selected by solar system and UTC time and has a documented ten-minute server cache. It provides grouped evidence, not independent fleet telemetry. Accepted direction: construct evolving reports from the live killmail feed; use third-party reports for import, comparison and links as appropriate. Relying exclusively on polling a ten-minute-cached report endpoint would not support the accepted five-minute alert-freshness objective reliably. The same documentation states that the killmail search API withholds killmails younger than five minutes, further supporting the live-feed integration. Report membership and team assignment remain explicit interpretation choices; a shared system/time window alone does not establish that unrelated losses belong to one battle.

Archive detail confirmed from [EVE Ref's killmail documentation](https://docs.everef.net/datasets/killmails): daily archives contain raw ESI killmails and may be updated as older kills are discovered. zKillboard enrichment is absent, so historical ISK summaries and publication-time evaluation require separate evidence; missing values must not silently become zero.

## External BR modeling: inspected behavior

- [andimiller/br input handling](https://github.com/andimiller/br/blob/cb0a8ae1ac763550e3fb9f3a7cd515c0801578c8/src/main-brcat.js#L157) accepts user-entered system/start/end rows, including multiple rows, with a default 90-minute duration. These are selected report windows, not an automatic incident detector. [Team assignment](https://github.com/andimiller/br/blob/cb0a8ae1ac763550e3fb9f3a7cd515c0801578c8/src/parser-brcat.js#L460) supports manual corp/alliance assignment to up to four sides and saved/static presets.
- [zKillboard's inspected filter](https://github.com/zKillboard/zKillboard/blob/405c6ff5c0b46e6b295a2f53a9025bb8f36bd17c/classes/MongoFilter.php#L173) selects inclusive [T−1h, T+2h] for the related report's fixed exHours=1; the report timestamp is hour-aligned. Adjacent hourly reports overlap two hours. A newly observed external report URL therefore does not establish a new battle.
- [zKillboard's side model](https://github.com/zKillboard/zKillboard/blob/405c6ff5c0b46e6b295a2f53a9025bb8f36bd17c/classes/Related.php#L232) is a two-side hostility heuristic with explicit overrides, not observed coalition membership.
- [Report readiness](https://github.com/zKillboard/zKillboard/blob/405c6ff5c0b46e6b295a2f53a9025bb8f36bd17c/classes/RelatedReport.php#L78): complete=true means computation completed, not combat concluded. It must not satisfy the user's post-action conclusion rule.

These source findings support reusing explicit time/system report modeling while retaining local deduplication, event freshness and provisional quiet closure. They do not establish a tactical winner or authorize implicit fleet merging. br.evetools.org's actual algorithm was not verified through available sources.

## Persistence and failure-isolation findings

`src/lib.rs:469` and `src/sov_feed/store.rs:185` show the existing PostgreSQL connection reused by separate stores. Dedicated battle tables can follow that pattern without migrating JSON subscription configuration; ADR 0001 preserves the existing ownership split. Contract delivery preparation (`src/contract_intelligence.rs:7356`) and revision-checked repair completion (`:7497`) supply patterns for durable outbound state and preventing stale report renders from clearing newer work. These are available patterns, not an existing battle implementation.

The prior activity plan's bounded `try_send` may drop observations. Current R2Z2 checkpoint persistence precedes returning a killmail to processing (`src/feed/r2z2.rs:348`), so a restart alone cannot recover those observations. A separate battle recovery position/reconciliation or acknowledged durable handoff is necessary. Accepted Q32 protects ordinary feed availability with explicit gaps and recovery, rather than silently claiming lossless ingestion. Independent background tasks isolate control flow, but still share CPU, memory and database capacity.
