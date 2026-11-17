# Write authorization

Picket requires an explicit operator-managed grant for every User-Initiated Write: Subscriptions in every Feed, watched entities, saved standings, and standings button/SSO continuations. A verified member matches an allowed user ID OR an allowed role ID in the originating guild. Grants in another guild confer no access. Manage Server is not required, and Administrator grants no bypass. Discord commands cannot edit the policy.

## Provision before adoption

Before starting this version or replacing a running version, create the host-side policy directory and copy the deny-all example:

```sh
mkdir -p policy
cp policy/write-policy.example.json policy/write-policy.json
```

The example is exactly `{ "version": 1, "guilds": {} }`; it grants nobody access. On an existing installation, preserve and edit the existing policy instead of copying over it. Add the intended guilds, users and roles before adoption. Actual files under `policy/` are excluded from Git and Docker build contexts; the example is retained. Keep policy backups outside the repository and build context.

The default file is `policy/write-policy.json`, relative to the application's working directory (`/app` in the image). The host operator must make the directory traversable and the file readable by the configured container UID/GID (`KILLBOT_UID:KILLBOT_GID`, default `501:20`). Keep policy editing under operator control. Supplied Compose mounts `./policy:/app/policy:ro`, separately from writable `./config:/app/config:delegated`; it retains the non-root user, read-only root filesystem, dropped capabilities and no-new-privileges setting. Do not expose the host policy directory, an ancestor or the policy file through another writable mount, including through `config/`.

## Version 1 schema

`guilds` maps guild ID strings to objects with optional `allowed_user_ids` and `allowed_role_ids` string arrays. Empty or absent guild maps/lists grant nothing. Each ID must be a canonical positive decimal `u64` string: digits only, no leading zeros, whitespace, signs, zero, overflow or JSON numbers. Obtain actual IDs with Discord Developer Mode's Copy ID; the placeholders below must be replaced and are intentionally invalid:

```json
{
  "version": 1,
  "guilds": {
    "GUILD_ID": {
      "allowed_user_ids": ["USER_ID"],
      "allowed_role_ids": ["ROLE_ID"]
    }
  }
}
```

The guild's `@everyone` role ID (equal to the guild ID) is forbidden. There are no wildcards, global user grants, per-command permissions or deny/allow precedence rules. Unknown versions/fields, duplicate object fields or guild keys, wrong types, invalid IDs and invalid JSON reject the whole policy. The loader requires a regular UTF-8 file, rejects symlink components (even ones normalized away by `..`), and rejects paths inside writable bot state, including resolved state aliases. Use a real directory outside writable state.

## Restart and migration

Policy is loaded once into an immutable startup snapshot. Host-side edits affect the next bot process; there is no hot reload or policy administration command. After editing grants, the operator restarts the bot with `docker compose restart discordbot`. Pending SSO states are process-local and clear on restart; users must start a new standings flow. A role change is checked at the next continuation boundary, not guaranteed to cancel an operation already in flight.

A missing, unreadable or malformed policy, or a rejected path, logs a fixed startup error category and disables all User-Initiated Writes. Query handling, collection and existing automated Feed delivery remain enabled, subject to their normal configuration and service health. An empty policy directory therefore safely denies writes; it does not grant emergency access. Guild denials are ephemeral: `Not authorized to change Picket in this server.` DM continuation denials are private and generic.

Old Discord integration or channel command overrides may still hide or restrict commands. Review Server Settings → Integrations → Picket → Command permissions when making commands available to intended writers. These overrides cannot grant application write authority. `/watch` no longer defaults to Manage Server, so `/watch list` is guild-visible; `/watch add` and `/watch remove` still require grants. `/find_unsubscribed` keeps its existing registration restriction, and `/health` keeps its separate operator-only check. Unknown commands/subcommands fail closed.

Standings continuations require the initiating user, the original guild/channel/Subscription ID tuple and a fresh current-member lookup in that guild, bounded to two seconds; missing membership, removed grants/roles, errors or timeout deny. Completing state is claimed once after preflight; reauthorization does not consume it. A missing Subscription is never recreated by completion. If an approved writer changes or recreates the same tuple after preflight, completion uses that then-current Subscription. Standings and Subscription saves remain separate; they are not one transaction. `/sync_clear` still clears only the caller's own global saved standings, requiring a grant in the invoking guild.

## Path overrides

`PICKET_WRITE_POLICY_PATH` is an operator-only startup override. For a directly run process it names a readable regular file outside `config/`, without symlink indirection. Compose forwards the variable, but changing it does not create a mount. Supply a corresponding separate read-only mount. For example, after provisioning a real host directory `./operator-policy` outside `config/`, an override file can contain:

```yaml
services:
  discordbot:
    environment:
      PICKET_WRITE_POLICY_PATH: /app/operator-policy/write-policy.json
    volumes:
      - ./operator-policy:/app/operator-policy:ro
```

Set the same path in the runtime environment used to start the bot. If using an additional host policy directory, exclude its actual files from Git and Docker contexts before storing grants there. Render the combined Compose files and inspect every mount: the new mount must be read-only, distinct from writable state, with no writable alias. Never point the override into `/app/config` or weaken containment to make a rejected path load. Recreate the bot container to apply changed mount/environment configuration; a plain restart only reloads the existing container configuration.

## Limits and evidence

This boundary addresses an ordinary unapproved Discord member, including one holding that member's stolen token. A stolen approved identity inherits its grants; someone able to assign an allowed role can confer authority. Bot-token theft permits API requests outside these handlers. A compromised process can disregard in-memory authorization even if the policy mount stays read-only. Direct host execution has the filesystem permissions supplied by its operator. Dependency vulnerabilities, resource exhaustion, Docker/kernel escapes and production host configuration are outside this proof.

See [the implementation proof](write-authorization-proof.md) for offline request coverage, the retained baseline failure and the isolated dummy container check. The local proof does not establish production containment or live Discord/EVE behavior. Docker documents [read-only bind mounts](https://docs.docker.com/engine/storage/bind-mounts/) and [Compose containment declarations](https://docs.docker.com/reference/compose-file/services/); those declarations and runtime filesystem observations are separate evidence.
