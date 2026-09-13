# Picket brand

The bot has had three names (`hazardous-killbot`, `zk-activity`, `killbot-rust`) and all three describe where the data came from or one feature. Every time the scope grew, the name was wrong again. This file exists so the fourth name is the last one.

## The name

A picket is the ship you park on a gate to call what comes through. That is the whole product in one word: you decide what counts as worth calling, per channel, and the bot holds the gate. It stretches to sov timers, contracts, and rosters without strain, because all of those are "something showed up, tell the channel."

Rules that follow from the name:

- The product is **Picket**. The repo, crate, Docker image, and Discord application use it. Alliance servers can nickname their instance whatever they like (one calls it Radar); that never leaks into code or docs.
- **Radar** stays the name of a Subscription shape (a light-year range around a system). It is not the product and not a Feed. See `CONTEXT.md`.
- No CCP mark in the name. "EVE Online" appears in the tagline and description, never as the first word of anything we own. Keep the CCP trademark notice in the README.
- Feeds are **intel**, not "intelligence", "monitoring", or "tracking", in anything a user reads.

## Tagline and description

Tagline (Discord About, forum thread title, first line under the README title):

> Picket: EVE Online intel feeds for Discord.

GitHub description:

> Picket is an EVE Online intel bot for Discord. Killmails, sov timers, public contracts, and watchlists, filtered per channel.

One-line summary when something needs a sentence rather than a slogan:

> Picket watches zKillboard killmails, ESI sov campaigns, public contracts, and corp or alliance rosters, and posts only what a channel's filters ask for.

## What we do not say

- No staging system, alliance, or corporation names in copy. Examples say "your staging" and "your space". A distance ("within 8 LY") is fine; a distance plus a system name identifies a group.
- No claim that the bot only reads public data. It posts public events, but the standings veto reads a synced contact list. Just don't make the claim.
- No "powerful", "flexible", "blazing", "real-time". Say what the filter does instead.
- The creator's name appears once, in the README credit, and not in the bot. The creator code does the affiliation work:

> Picket is built and run by an EVE Online Partner. If it earns a place in your server, use creator code DRAC at eveonline.com/checkout.

## Style

The reference is Winslow Homer's *The Army of the Potomac: A Sharp-Shooter on Picket Duty* (Harper's Weekly, 1862). Wood engraving: black ink on cream paper, dense parallel hatching for shadow, cross-hatching for the darkest areas, white left as paper. It is public domain, so a crop can sit in the README or the forum post as a reference image with the caption intact.

We borrow the technique, not the subject. Homer's picket is aiming a rifle. Ours holds a gate and calls what comes through, so the ship in our mark never has a weapon drawn and there is no crosshair or reticle anywhere in the brand.

Rules that follow from the style:

- Two tones only: ink and paper. No colour in the mark, the banner, or the avatar.
- Hatching carries all shading. No flat fills, no gradients, no glow.
- A caption band under any banner, small caps serif, the way Harper's captioned its plates.
- The avatar is a simplified cut of the mark, not a shrink of it. Fine hatching does not survive a 32 px circle.

## Colour

The embeds already use colour to mean something, and the brand must not fight that:

| Role | Hex | Where it is used today |
| --- | --- | --- |
| Kill (a matched attacker) | Serenity `DARK_GREEN` | killfeed embed bar |
| Loss (a matched victim) | Serenity `RED` | killfeed embed bar |
| Contract event | `#E67E22` | contract embed bar |

The brand itself is monochrome, which sidesteps the problem entirely:

| Role | Hex | Notes |
| --- | --- | --- |
| Ink | `#1A1714` | warm black, the engraved line |
| Paper | `#F3EBD9` | aged cream; the avatar disc, so it stands out on Discord's dark UI |
| Mid hatch | `#6B615A` | only for digital reproductions of hatching at small sizes |

Never recolour the kill, loss, or contract bars. If a non-state embed ever needs a brand colour (a `/health` reply), use the ink value.

## Logo brief

Concept: one small ship holding position beside an EVE Online stargate, engraved. The ship is at rest, the gate is the big shape, the dark of space is hatched rather than filled. Think of it as a plate from an 1860s weekly that happened to be about a stargate.

An EVE stargate is not a ring. It is an open industrial frame, kilometres long, made of girders, plating, antenna masts, and docking arms, bracketing a bright swirling jump vortex. The four faction designs, from in-game screenshots (kept out of the repo, they're CCP's):

| Faction | Shape to describe |
| --- | --- |
| Minmatar | two tall angular pylons rising from a boxy base, rust-brown, rough plating, the vortex between the pylons |
| Caldari | a rectangular frame of straight beams with lit rectangular panels, the vortex inside the frame |
| Gallente | an asymmetric structure with tall curved fins and long spars, green-lit |
| Amarr | a row of stacked open cylinders on a spine, gold, the vortex glowing through the cylinders |

The mark stays faction-neutral: pylons plus vortex, no faction's exact silhouette. That keeps it recognisable as EVE to a player without copying a specific CCP model.

Deliverables, in order of need:

1. Banner, about 1280 x 400, the full engraving with a caption band reading "PICKET" in small caps serif, for the README and the forum post.
2. Mark, 1024 x 1024, the same ship and gate cut down to a handful of lines on a paper-coloured disc, for the Discord avatar. Discord crops to a circle; keep the mark inside the middle 70 %.
3. Mark on transparent background, for badges.

### Prompt for image models

Paste as-is. Change only the bracketed line between runs.

```
A 19th-century wood engraving in the manner of 1860s Harper's Weekly illustrations.
Subject: a single small spacecraft holding position beside an enormous stargate in the style of EVE Online, seen slightly from above, at rest, no weapons visible. The stargate is not a ring: it is an open industrial frame of two tall angular pylons and cross-beams built from girders, armour plating, antenna masts, and docking arms, bracketing a bright swirling jump vortex between them. The vortex is bare paper with fine radiating hatching. The frame is kilometres long and the ship is tiny against it. The dark of space is rendered with dense parallel hatching and cross-hatching, not solid black. Stars are small untouched flecks of paper.
Black ink on aged cream paper. Two tones only. Every shadow is drawn with engraved lines, every highlight is bare paper.
No colour, no gradients, no glow, no digital smoothing, no photorealism, no 3D.
Composition: the gate frame fills most of the image, the ship is small and off-centre near one pylon, plenty of hatched sky.
[Variant: BANNER, landscape 16:5, with a plain caption band at the bottom reading PICKET in small capitals serif type]
```

Variants for the last line:

- `BANNER`: as written above.
- `MARK`: replace the last line with "Variant: MARK, square 1:1, simplified to the fewest engraved lines that still read as a ship beside a ring, on a circular cream disc, wide margin, no caption, no text." Expect to redraw this by hand from the best output; models overdraw at this size.
- `PLATE`: replace the last line with "Variant: PLATE, landscape 3:2, no caption, richer detail in the girders and hatching." For the forum post and any hero image.

Gate-shape variants, appended after the variant line if the neutral gate keeps coming out as a ring:

- `MINMATAR`: "The stargate is two tall angular pylons rising from a boxy base, rough riveted plating, the vortex between the pylons."
- `CALDARI`: "The stargate is a rectangular frame of straight beams with rows of lit rectangular panels, the vortex inside the frame."
- `GALLENTE`: "The stargate is an asymmetric structure with tall curved fins and long spars."
- `AMARR`: "The stargate is a row of stacked open cylinders on a spine, the vortex glowing through them."

Use these to steer, then pick the output that reads as "a gate" to an EVE player without being any one faction's model.

Negative prompt, for models that take one:

```
colour, gradient, glow, bloom, 3D render, photorealistic, digital painting, smooth shading, solid black fill, closed ring, torus, circular portal, Stargate SG-1, crosshair, reticle, rifle, gun, laser, explosion, skull, radar sweep, text (except the caption band when requested)
```

"Rifle" and "crosshair" are excluded because of the Homer reference. "Closed ring", "torus", and "Stargate SG-1" are excluded because the first run produced a ring, which is the wrong franchise. "Radar sweep" is excluded because Radar is a feature name we keep separate.

## Assets

**Provisional.** Generated 2026-09-13 with the first version of the prompt, before the gate description was corrected; the gate in these is a closed ring, which is not an EVE stargate. They hold the README slot until the prompt above is rerun. The chosen output was the 1376 x 768 plate; two 3:1 outputs from the same run were rejected because the ship was too small to read.

| File | Size | Use |
| --- | --- | --- |
| `docs/brand/plate.jpg` | 1376 x 768 | source, keep untouched |
| `docs/brand/banner.jpg` | 1280 x 714 | README header, forum post (the only image committed; the PNGs below stay local until the rerun) |
| `docs/brand/banner.png` | 1280 x 714 | lossless copy, not committed |
| `docs/brand/banner-wide.png` | 1280 x 400 | anywhere that wants a strip; the top of the ring is cropped |
| `docs/brand/mark.png` | 1024 x 1024 | Discord avatar; ship sits lower-left, inside the circle crop |

Still owed: a hand-simplified mark that reads at 32 px, and a transparent-background mark. The current `mark.png` is a crop of the plate and goes muddy below about 64 px.

## Screenshots

README screenshots are renders of the bot's real payloads, not client captures: `cargo test --lib screenshot_payloads -- --ignored` dumps the JSON, `scripts/render-screenshots.mjs` draws it with Discord-styled web components in headless Chrome. Keep them under `docs/screenshots/`, 2x scale, 680 px wide viewport, dark theme, and re-run both steps whenever an embed changes. State colours (green kill, red loss, orange contract) are the bot's own and must never be recoloured.

## Where the brand shows up

| Surface | Uses |
| --- | --- |
| README title and first paragraph | name, tagline, banner (deliverable 1), one-line summary, creator code line |
| GitHub repo description and topics | description above; topics `eve-online`, `discord-bot`, `esi`, `zkillboard`, `rust` |
| Discord application | name Picket, avatar from deliverable 2, About = tagline + repo link |
| Forum thread (Third-Party Developers) | title = tagline, body = README intro |
| Partner badge | `docs/brand/eve-online-partner.png` under the partner line in the README. It is CCP's asset, unmodified; the repo copy only bakes in the ink background because the original is white-on-transparent and vanishes on GitHub's light theme. The transparent original sits beside it. Always paired with the creator-code sentence, never used as Picket's own mark. |
| Embeds | unchanged; state colours only |
