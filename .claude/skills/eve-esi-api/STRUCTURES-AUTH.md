# Structures and authorization

## Evidence boundaries

Public contracts may contain NPC station IDs or player-structure IDs. Resolving a player structure requires `esi-universe.read_structures.v1` and access through the authorized character's ACL.

Classify outcomes at the narrowest supported boundary:

- token refresh, discovery, JWKS, JWT, client, or character failure is global authorization evidence;
- a structure lookup forbidden/not-found result is per-location access evidence;
- a successful lookup is Access-Qualified Evidence for name, solar system, owner, and optional position;
- previously verified structure evidence remains historical evidence after access changes.

## JWT and JWKS

1. Trust configured discovery and JWKS origins.
2. Require the token's `kid`.
3. Select the matching supported key and algorithm from a heterogeneous JWKS.
4. Validate issuer, audience, expiration, subject/character, and required scope.
5. Keep refresh/access tokens memory-only unless an explicit provisioning path rotates them durably.

## Verification

Cover one Bearer header on the structure route, absent authorization on public routes, missing `kid`, mixed RSA/EC keys, unsupported algorithms, issuer/audience/scope failures, refresh failure, per-structure denial, successful Access-Qualified Evidence, retained historical evidence, and global-backoff restart behavior. Error and debug output must remain credential-free.

Project source: `src/structure_resolver.rs` and its integration seams in `src/contract_intelligence.rs`.

Official sources: [EVE SSO and JWT validation](https://developers.eveonline.com/docs/services/sso/) and [Structure lookup API Explorer](https://developers.eveonline.com/api-explorer#/operations/GetUniverseStructuresStructureId).
