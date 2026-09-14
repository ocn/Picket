# EVE battle intelligence design

Status: product decisions accepted through Q32 on 2026-09-06. This document consolidates the agreed design; [research notes](battle-simulator-research.md) retain sources, repository findings and the interview history. No implementation or deployment is implied.

## Purpose and evidence

Provide alliance leadership and FCs with automatic discovery of unfolding combat, observed battle reconstruction, and regional activity history to support hunting and staging decisions. Present a private 3D regional map, ranked encounter list, system drill-down and timeline scrubber. Discord receives independent discovery embeds and after-action image recaps. UI access through Discord and shared UI exposure await later hardening.

Public killmails establish reported losses, attacker participation and optional victim positions. They cannot reconstruct continuous ship motion, a complete fleet roster or current presence. Start with observed positions and timelines. Preserve separate Encounters and Battle Reports; cross-report comparisons disclose overlapping pilots, locations and times without implicitly merging fleets. Outcome assessments appear only after provisional closure, remain qualified and may leave the winner unknown.

Existing tools cover portions of this experience, including static 3D loss reconstruction and live system maps. Research has not verified one product combining every requested capability. See the linked research for inspected products and evidence limits.

## Discovery and configurable coverage

Existing killmail subscriptions explicitly opt into battle discovery. Evaluate their existing filter trees unchanged and consume the internal matches with their complete killmails. Record which subscription and evaluated configuration supplied each match. Optional battle-priority designation is independent of source Discord ping settings. Multiple matching feeds contribute provenance to one observation.

Coverage is configurable. The initial geographic preset is all lowsec, full Etherium Reach and Perrigen Falls, and additional nullsec within an inclusive eight light-years of 3G-LFX; highsec is excluded from this preset. These are configuration values, not product constants. Current incomplete metadata caches cannot enumerate every system; use complete static map data and the EVE distance convention. For complex feed predicates, Q30 defines an explicit map/history scope using existing geography filter types.

Source matches seed reports. Other observed losses inside a report's explicit membership may supply labeled contextual evidence even when they do not match a selected feed. Store the inclusion reason. Contextual inclusion alone does not authorize an independent discovery alert. Collection remains incomplete because public killmail publication is incomplete or delayed.

## Reports and notifications

| Behavior | Accepted direction |
| --- | --- |
| Tracking unit | Separate evolving Battle Report per Encounter, with stable local identity and visible revisions. |
| Duplicate observations | Deduplicate killmail IDs; retain source provenance and external report references. |
| External related URLs | Supporting association evidence; no upstream dependency for local identity or automatic merging based on a URL. |
| Dashboard discovery | Show provisional candidates immediately. |
| Discord corroboration | Initial heuristic: two linked player-ship losses in ten minutes, with at least five distinct attacking pilots. Pods do not satisfy the loss threshold; NPC-only losses do not trigger discovery. |
| Priority | Explicit priority-source matches may bypass corroboration; freshness still applies. |
| Freshness | Supporting combat event at most five minutes old at live-alert evaluation. |
| Ranking | Configured priority, freshness and observed scale; expose the factors. |
| Routine updates | Edit existing encounter messages; avoid a new message for each loss. |
| Follow-ups | Distinct qualifying encounters and newly observed priority escalations; deduplicate per destination. |
| Mentions | Ordinary discoveries and recaps have none. Priority may mention an opted-in role, with a ten-minute destination-channel mention cooldown. |
| Encounter closure | Provisional recap after twenty minutes of healthy collection without new evidenced activity. A collection outage does not establish quiet. |
| Roaming recap | Collection of separate associated encounters; sixty-minute quiet closure, without asserting a persistent fleet. |
| Late evidence | Revise the report and existing recap with a visible revision time. Backfill does not create fresh alerts. |
| Recap eligibility | Every notified encounter, including small engagements. |

Generate an SVG master and readable PNG preview from the same report revision, with high-resolution PNG and downloadable SVG exports. Include location/time window, observed participation and ship losses, loss timeline, available valuation and spatial evidence. Unknown valuations or missing positions remain missing, not zero or fabricated. Discord attachments remain usable without links to the private dashboard. Quiet closure supports a provisional after-action assessment, not proof of departure or a tactical winner.

Local reports use the accepted single-system time-window rule in Q28 below, including the distinction between renewed action and delayed evidence. External report windows remain source-defined, and their overlaps cannot silently combine separate encounters.

## Activity history

Import an initial ninety days where archives permit, visibly marking gaps. Show hourly system-jump and player-ship, pod and NPC-loss counts alongside recurring combat patterns and comparisons with equivalent weekday/hour periods. ESI counts are hourly aggregates, not identified fleets or live population. Store observation interval and source freshness so cached responses are not counted repeatedly.

Preserve event time, receipt time and publication time when available. Event-time archive playback must not masquerade as a historical test of what an observer knew live. Historical loss values require a documented source and valuation time; raw ESI archives do not include zKillboard valuations. Private ship-cache inventories and actual clone installation records are deferred.

## Engineering consequences to carry into implementation planning

The current pipeline already supplies enriched killmails with subscription matches. It discards the full unmatched payload, so contextual report evidence requires observation before that discard. Discord sends currently lack durable structured message outcomes; revisable recaps need persisted destination/message identity and delivery state. No battle ledger or battle-report module exists in this checkout. These are required additions, not existing capabilities.

Keep battle processing and rendering isolated from ordinary killfeed delivery. Persist enough evidence, configuration provenance and report revisions to recover without duplicate counting. Record collection gaps and recheck freshness before delayed delivery. Preserve existing subscription configuration ownership; choosing battle storage does not imply moving JSON subscription files. The existing PostgreSQL connection is already used by separate contract, sovereignty and watchlist stores, so dedicated battle tables can reuse that deployment while subscriptions remain JSON-backed. Existing durable delivery and revision-checked message repair provide reusable patterns. The current R2Z2 checkpoint advances before downstream processing; a bounded in-memory handoff alone cannot guarantee battle recovery. The accepted availability and recovery policy is Q32 below; concrete wiring belongs to the engineering plan.

## Report lifecycle and data policy — accepted 2026-09-06

- **Q28 — Local report windows.** Accepted: one system per automatically discovered report, starting at the first included combat observation and extending until twenty minutes of healthy quiet. Treat this as explicit system/time grouping, which may include unrelated combat; allow later boundary corrections while preserving revisions. Fresh qualifying combat beyond the closed window starts another report; delayed events inside it revise the old report. External imports retain their original windows. This is a local adaptation of time/system BR modeling, not a claim that external services implement the same quiet rule.
- **Q29 — Feed edits.** Accepted: apply source-filter and priority edits to future discovery evaluations; retain configuration provenance for past matches and do not rewrite historical alert eligibility. Existing reports keep their stated context membership. Explicit replay may recompute a historical view without generating fresh alerts.
- **Q30 — Activity-map coverage.** Accepted: reuse an unambiguous existing geographic scope where possible. For complex mixed predicates, require an explicit geographic scope using the existing geography filter types, with the resolved systems visible; do not silently guess a scope by stripping ship/value/standing conditions. This scope controls map/history collection, while full source filters govern event eligibility.
- **Q31 — Retention.** Accepted configurable starting policy: ninety days of detailed killmail evidence, one year of compact report summaries and hourly activity. Older retained summaries explicitly lose detailed replay/correction capability when evidence expires. This is distinct from the accepted ninety-day initial backfill and requires a storage estimate before enabling deletion.

- **Q32 — Battle-worker failure.** Accepted: keep ordinary killfeed delivery running when battle processing or storage is unavailable, display a collection gap and suspend quiet-based closure, and recover battle evidence through a separate persisted recovery position or archive reconciliation. Recovery must recheck five-minute freshness and must not flood Discord with historical discoveries. If upstream retention cannot cover a gap, preserve that gap explicitly. This favors ordinary-feed availability over guaranteed immediate battle observation; it does not treat an in-memory queue as durable.

These product policies are accepted. Implementation planning must specify storage schema, recovery reconciliation, report-window boundary conventions, valuation sources, render layouts and verification before rollout. Source IDs, role/channel mappings, numerical ranking weights and deployment sizing can remain configuration or measured implementation choices; they do not grant UI access or permission to publish.

## Acceptance scenarios for implementation planning

| Scenario | Required observable behavior |
| --- | --- |
| One killmail matches several opted-in feeds or is replayed | Count one loss, retain all match provenance, and avoid duplicate encounter notifications in the same destination. |
| A source match discovers combat with additional nonmatching losses | Include losses within the explicit report scope as labeled context; context alone cannot create independent discovery alerts. |
| A priority event arrives more than five minutes after its combat time | Update applicable evidence/history; do not issue a fresh discovery alert or mention. |
| An encounter reaches twenty minutes of healthy quiet | Produce a provisional recap even for a small notified encounter; outcome may remain unknown. |
| Delayed evidence falls within a closed report window | Revise that report and its existing recap with a visible revision time. |
| Fresh qualifying combat occurs beyond a closed report window | Create a separate local report; an overlapping external URL does not collapse the two. |
| A source subscription changes | Future evaluations use the new configuration; previous matches retain their original rationale. |
| Battle processing fails while the ordinary feed operates | Continue ordinary delivery, expose a coverage gap, suspend quiet-based closure, and reconcile evidence without historical alert floods. |
| Detailed evidence expires under the configured retention policy | Retained compact summaries remain readable and clearly indicate unavailable detailed replay. |
| A recap is delivered to Discord | PNG preview and standalone exports work without access to the private UI. |

The next artifact is an engineering implementation plan against this design. Production source selection, Discord destinations/roles and UI access hardening remain rollout work; this design does not change running configuration.
