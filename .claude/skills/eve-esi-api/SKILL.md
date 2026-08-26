---
name: eve-esi-api
description: Use when changing or debugging EVE ESI or zKillboard R2Z2/RedisQ interactions, including public-contract lifecycle, conditional caching, shared rate limits, SSO/JWKS, player structures, ESI health, killmails, or typed EVE identifiers
---

# EVE ESI API

## Workflow

1. Select every affected branch and read only its reference:
   - public contracts, lifecycle, delivery, or backlog metrics → [CONTRACTS.md](CONTRACTS.md);
   - rate limits, conditional caching, or ESI health → [LIMITER-CACHE-HEALTH.md](LIMITER-CACHE-HEALTH.md);
   - player structures, SSO, JWT, or JWKS → [STRUCTURES-AUTH.md](STRUCTURES-AUTH.md);
   - R2Z2, RedisQ, zKillboard metadata, or killmail flow → [KILLMAIL.md](KILLMAIL.md);
   - EVE identifiers or identity/observation clocks → [IDENTIFIERS.md](IDENTIFIERS.md).
2. For each affected ESI HTTP operation:
   - record method/path, authentication scope, runtime compatibility header, and typed identifiers;
   - run `.claude/skills/eve-esi-api/scripts/inspect-openapi.sh '<path>' [method]`;
   - treat OpenAPI as the latest schema and observed headers/body as the actual request; compare operation `x-compatibility-date` with runtime `X-Compatibility-Date`, noting that an unversioned request receives the oldest compatible behavior;
   - record official schema, observed wire behavior, and project fixtures separately; when they differ, require an explicit tested compatibility rule;
   - complete the five-axis evidence note: **Wire**, **Cache**, **Limiter**, **Domain**, and **State**. Mark an inapplicable axis `N/A` with its reason.
3. For a feed, model, or identifier change without an ESI request, record source authority, typed input/output, permitted transformation or checkpoint transition, preserved provenance, and retry/failure boundary.
4. Apply every verification item from each selected branch.

## Completion criteria

- An ESI HTTP change is complete when every axis cites wire/project evidence or a justified `N/A`, and tests cover each permitted transition and failure boundary.
- A feed, model, or identifier change is complete when source authority, typed transformation, state/checkpoint invariant, provenance, and retry/failure behavior are explicit and verified.
- A multi-branch change satisfies both criteria and every selected branch's verification section.
