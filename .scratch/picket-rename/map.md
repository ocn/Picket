# Wayfinder Map — Picket rename

## Destination

The product is called Picket everywhere a user or a developer can see it, with the public surfaces renamed now and the code names renamed at the next planned Deployment, without spending a Deployment on cosmetics.

## Status

Clear for implementation. 01 is resolved. 04 and 05 can start now. 02 is a human action (repo rename). 03 waits on 02 and 05.

## Outputs

- Brand definition: [docs/brand.md](../../docs/brand.md)
- Vocabulary: `CONTEXT.md` "Product language" (Feed, Subscription, Radar, Intel)
- Tickets: [01 public surfaces](issues/01-rename-public-surfaces.md), [02 repo rename](issues/02-rename-github-repo.md) (`ready-for-human`), [03 announce](issues/03-forum-thread-and-listing.md), [04 code names](issues/04-rename-code-names-at-next-deployment.md), [05 logo](issues/05-generate-and-place-logo.md)

## Decisions so far

- Name: Picket. Rejected: Wyrmsight (unintuitive), eve-esi-bot / esi-monitor / esi-feeds-bot (name the plumbing, lead with a CCP mark), Radar / Sonar (feature vocabulary and per-server nicknames, not ownable).
- Distribution: a public hosted instance and self-hosting, independently. Two Discord applications, two deployments, no shared state. Public instance is Killfeed-only at launch. SaaS pricing is parked.
- Marketing makes no "public data only" claim; the standings veto reads synced contacts.
- Feed / Subscription / Radar / Intel codified in `CONTEXT.md`.
- Rename timing: public surfaces now; crate, binaries, image, containers (postgres container and volume excluded), both User-Agent strings, and the startup log line at the next planned Deployment.
- Operator in TERMS and PRIVACY is "ocn". Creator name appears once, in the README credit; affiliation is carried by the partner line and creator code DRAC.
- Discord verification only when server count forces it.
- GitHub description and topics change in the same sitting as the repo rename.
- Announcement is a new forum thread, not a renamed one. `docs/announcement.md` stays as history with a note.
- Public instance gets a new Discord application named Picket; the alliance's existing application and token are untouched.
- Brand style: 1860s wood engraving (Homer's picket plate as reference), ink on paper, monochrome; embed state colours untouched.

## Not yet specified

- The public instance invite link (exists once its Discord application is created).
