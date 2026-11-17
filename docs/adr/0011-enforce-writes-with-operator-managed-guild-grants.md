# Enforce writes with operator-managed guild grants

Picket authorizes User-Initiated Writes against explicit operator-managed user or role grants for the originating guild, including every delayed standings continuation; Discord command defaults and Administrator status do not grant authority. Commands remain available to members without Manage Server so that the application's decision, rather than a broad Discord permission, governs configuration changes after an ordinary member account is compromised.

The policy loads on restart from a separate read-only deployment mount, while bot state remains writable; Discord commands cannot edit their own authority source. Missing or invalid policy disables user-requested mutations while automated Feeds continue. This restricts application authority and protects the policy mount; it does not establish host isolation or distinguish an approved identity from an attacker using that identity's token.

References: [Discord command permissions](https://github.com/discord/discord-api-docs/blob/main/developers/interactions/application-commands.mdx#permissions), [OWASP authorization](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html), [Docker security](https://docs.docker.com/engine/security/).
