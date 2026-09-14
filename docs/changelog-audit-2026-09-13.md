# Changelog audit — 2026-09-13

## Next public release boundary

GitHub Releases was checked on 2026-09-13. The latest published release is
[v0.1.1](https://github.com/ocn/Picket/releases/tag/v0.1.1), published
2024-09-30, at local tag commit `850931f`. The earlier
[v0.1.0](https://github.com/ocn/Picket/releases/tag/v0.1.0) was published
2024-02-24. The repository now resolves to `ocn/Picket`.

The next public release must account for `v0.1.1..[release commit]`, not just
the recent feed commits reviewed below. This audit and its suggested public
copy are partial drafting material, not final release notes. Keep entries under
Unreleased until the release version, date, and included commit are settled.

At the next public push/release, reconcile that full range, including the 2025
Rust rewrite, configuration/command migration, standings and filtering changes,
and the 2026 killfeed presentation, R2Z2, and processing improvements. Compare
the final behavior with the published v0.1.1 notes, including their courier
planner claims, to identify removals and upgrade steps. Recheck deployment and
rename status, include newly completed work, and update the public copy before
moving the finalized entries into a dated version section. Preserve the existing
GitHub release history; a production deployment or public push alone does not
establish a new published release.

The local `v0.1.1` tag is not an ancestor of the current HEAD; they share merge
base `f7d90a8`. Review both the release-to-candidate tree diff and divergent
history when reconciling scope, rather than treating every commit in the range
as a newly introduced feature.

## Recent workstream audit

Scope: the new-feed work from `3701e49` (2026-08-13) through `ed6a296`
(2026-09-13), including follow-up fixes and rollout records. The existing
April R2Z2 recovery entry is retained. This is not a retrospective audit of
the January–March killfeed changes or the 2025 Rust rewrite.

Evidence is committed history, committed README/runbooks, and selected source
diffs at `ed6a296`, not the working tree's unfinished features. The pre-existing
uncommitted final-blow changelog entry was preserved and verified against
`52a707d` and `b67820d`. Entries stay under Unreleased because commit dates do
not establish a release version or universal deployment date.

The selection follows [Keep a Changelog](https://keepachangelog.com/): curate
notable changes for readers rather than reproduce the commit log.

| Workstream | Previous coverage | Evidence commits | Audience and sharing assessment |
| --- | --- | --- | --- |
| Picket rename and product vocabulary | Rename covered | `ed6a296`, `6c1fe5d` | Public headline. Describe Picket and its feeds; crate/binary/container rename is still deferred. |
| Public contract feed and filters | Missing | `3701e49`, `a34099b`, `a0d2c2a`, `74677d8`, `77b169e`, `f084b18` | Public headline: listings and evidence-backed lifecycle alerts, with configurable filters/actions. State public item-exchange scope. |
| Contract location and presentation context | Missing | `c27dae0`, `db3de38`, `077311d`, `8ecbb28`, `1fff8c9`, `618fd3b`, `2b80992`, `e144e2f` | Public supporting detail: richer issuer/item/location context, explicit unresolved proximity, later verified updates. Structure access is conditional. |
| Contract delivery and recovery | Missing | `378376b`, `975a959`, `fc2e022`, `6c77ee9`, `570d8cc`, `73696cb` | Changelog reliability detail. Avoid an exactly-once guarantee: the runbook documents a rare duplicate after an ambiguous send outside Discord's nonce window. |
| Sovereignty timers | Missing | `9f24837`, `6b0ba79`, `02f46a5`, `b2d2636`, `6a1089c`, `5cc9fc3` | Public feature, qualified as pilot in deployment announcements: discovery, advance reminders, timer board, timezone shifts, preserved settings, resolved place names. |
| Stargate/Wanderer route context | Missing | `02f46a5`, `a3aa986`, `7c85f6c` | User-facing capability: reachable campaigns and path risk. Wanderer is optional and was disabled in the recorded deployment. |
| Corporation/alliance watchlist | Missing | `e481b8a`, `09ac1d2`, `07df3f0` | Public headline: organization changes and wars. Explain silent initial baselines and the history requirement for member-count alerts. |
| Killmail and contract notification summaries | Missing | `793eba8`, `cab6a35` | Public headline: phone banners and previews now say what happened. Applies to pinging messages. |
| Killmail presentation and links | Final blow present only in working tree | `6b2b3ad`, `52a707d`, `b67820d`, `e662161` | User-facing supporting detail: tracked ship names, final-blow attacker, WarBeacon links. |
| Role pings, cooldowns, nano subscription | Covered | `cf6d78d`, `5f70748`, `9256ce9`, `2a9b493` | Role/cooldown controls are general features. Nano is a configured subscription; share its exact coverage with that channel's audience. |
| Command permissions | Missing | `c4e3144` | Admin-facing behavior change; include in changelog because existing command access can change. |
| Health, caching, collection throughput, recovery pacing | Missing | `d4bdfa2`, `82caf07`, `dfe0061`, `eae50d9`, `7dbc368`, `4739b45`, `c4e3144`, `11ef81b` | Operator notes. Schema, limiter, scheduling, and watchdog details do not need a general-user announcement. |
| Context/domain documentation, tests, rollout tooling | Not listed | `3701e49`, `6c1fe5d`, `3427edb`, `a91f222`, `2a48066` | Internal evidence and maintenance; no standalone public feature claim. |

“Context mapping” has two distinct meanings here: implemented location and
route context belongs in user-facing notes; the domain model in
[CONTEXT.md](../CONTEXT.md) is internal documentation. Proposed channel-topic
subscription bindings, battle reconstruction, activity dashboards, and message
narrative analysis in uncommitted plans/ADRs are not completed features in this
commit range. Uncommitted alert-stage, mention, embed, and deployment changes
also need their own verification before inclusion.

## Deployment evidence and announcement boundary

The committed [pilot record](rollouts/esi-intel-feeds-pilot.md) reports live sov
samples in a sandbox, production `#corp-watch` configuration, silent watchlist
baselines, eight explicitly historical watchlist samples, and a day-2 restart
check with no duplicate deliveries. It does not establish live delivery of
every watchlist event kind. Sovereignty remained sandbox-only pending feedback;
Wanderer was disabled. These are the last recorded observations, not a fresh
production check.

For a general announcement, lead with Picket's rename, public contract intel,
corporation/alliance watchlists, and informative notification previews. Describe
sovereignty timers as a pilot and Wanderer routing as an optional capability.
Use location context, final-blow attribution, and role/cooldown controls as short
supporting details. Keep database setup, health operations, internal context
modeling, tests, and rollout machinery in the changelog/runbooks.

## Suggested public copy

Picket is the new name for the bot. Alongside killmails, it now supports public
contract feeds and corporation/alliance watchlists, covering new listings,
confirmed contract outcomes, organization changes, and wars. Pinging killmail
and contract alerts now include a summary in notification previews; killmail
embeds also identify the final-blow attacker. Role pings and configurable
cooldowns give channels more control over notifications.

Sovereignty timers are in pilot, with advance reminders, an on-demand timer
board, and reachability filters. Optional Wanderer integration adds wormhole
route context and path risk. Contract alerts include richer location context
and explicitly identify proximity that has not yet been verified.
