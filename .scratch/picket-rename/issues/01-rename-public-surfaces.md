# 01 — Rename the public surfaces

**What to build:** Every human-readable surface in the repo says Picket, with no code or deployment change.

**Blocked by:** None. The badge URLs are the exception: they change in 02.

**Status:** resolved

- [x] `docs/TERMS.md` and `docs/PRIVACY.md` name the bot Picket and the operator "ocn", with the existing public email as the contact; "Hazardous Killbot" no longer appears.
- [x] `AGENTS.md` and `CLAUDE.md` project lines say Picket (`killbot-rust` stays in parentheses until 04 lands).
- [x] `README.md` has no remaining "zk-activity" outside the shield URLs; the `## Contact` section keeps the hazardous-killbot lineage line.
- [x] `docs/announcement.md` is either rewritten under the new name or given a one-line note at the top that it predates the rename (decide in the map's "Not yet specified").
- [x] `CHANGELOG.md` Unreleased gets a `### Changed` entry: the project is renamed to Picket; binaries and images follow at the next Deployment.
- [x] `grep -ri 'zk-activity\|hazardous killbot' README.md docs/*.md AGENTS.md CLAUDE.md` returns only shield URLs and the lineage line.

## Answer

Done in the working tree. TERMS and PRIVACY now name Picket and the operator ocn; PRIVACY also lists what is actually stored (SSO tokens and contact lists from `/sync_standings`, Postgres feed state) and no longer points at the upstream author. `docs/announcement.md` kept as history with a note. `AGENTS.md`/`CLAUDE.md` project line updated.
