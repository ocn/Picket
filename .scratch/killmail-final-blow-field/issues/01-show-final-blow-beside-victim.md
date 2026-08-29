# 01 — Show Final Blow beside Victim

**What to build:** Make every killmail embed identify the attacker credited with the Final Blow beside the Victim using the same compact participant formatting and zKillboard links, while preserving the existing full-width Victim display when no attacker is marked.

**Blocked by:** None — can start immediately.

**Status:** claimed

- [ ] A marked character credited with the Final Blow renders immediately after Victim as a linked `[alliance ticker, else corporation ticker] Character` field.
- [ ] Victim and Final Blow are inline only when both fields are present; Discord remains responsible for responsive wrapping.
- [ ] A marked attacker without a character renders its linked available affiliation followed by its unlinked Ship Type.
- [ ] No marked attacker omits Final Blow and leaves Victim non-inline.
- [ ] Multiple marked attackers deterministically render the first in killmail order.
- [ ] Existing killmail titles, attacker summaries, links, matching, notification behavior, models, configuration, and persistence remain unchanged.
- [ ] Victim and the credited attacker share only the minimal participant-formatting code needed to keep their presentation consistent.
- [ ] Three credential-free tests at the public embed-builder seam cover normal linked output, non-character fallback, and zero/multiple marker behavior.
- [ ] Focused tests, the full Rust test suite, formatting, compilation, and Clippy pass.
