# 02 — Make Production Verification preflight canonical and non-sending

**What to build:** Make the controlled Production Verification prove the current terminal presentation through an automated non-sending preflight before any Discord transport is constructed. The ignored live seam must reuse the same fixture and assertions so presentation expectations cannot drift.

**Blocked by:** None — can start immediately

**Status:** resolved

- [x] A non-ignored preflight renders the current canonical terminal presentation without constructing a Discord client or issuing a request.
- [x] The preflight verifies title, description, Issuer, strongest supported human-readable location, presentation revision, thumbnail, history, and empty-placeholder behavior.
- [x] The preflight verifies no raw identifier labels and no unexpected mention content.
- [x] The controlled delivery remains non-pinging with empty allowed mentions.
- [x] The ignored live Discord seam reuses the same canonical fixture and preflight rather than duplicating presentation expectations.
- [x] A stale or failing preflight makes zero Discord requests and cannot create a replacement post.
- [x] The current obsolete title assertions are replaced with the canonical terminal title without changing production rendering behavior.
- [x] Focused presentation, embed-budget, mention, nonce, and delivery-wire regressions pass.
- [x] No production, database, subscription, credential, historical-repair, or Discord mutation occurs while implementing and testing this ticket.

## Implementation evidence

- RED: the ignored manual seam read its token, then stopped at its stale title assertion before constructing an HTTP client or sending a Discord request.
- GREEN: `cargo test --lib contract_intelligence::embed_tests::manual_contract_embed_visual_preflight_matches_current_presentation_and_outbound_policy -- --exact` passed (1 test), including the pure outbound builder's empty content, empty allowed mentions, nonce, and non-pinging policy.
- GREEN: `cargo test --lib contract_intelligence::embed_tests::failed_manual_contract_embed_visual_preflight_makes_no_post_attempt -- --exact` passed (1 test); a stale title stops before the post-attempt boundary.
- Adjacent safe tests passed for terminal embed structure and budget, nonce, suppressed edit mentions, and explicit non-pinging delivery allowed mentions.
- `cargo fmt --check`, `cargo check`, and `git diff --check` passed. The ignored sending seam was not rerun.

## Answer

Resolved in `c90e9d2` and `83ae9f9`. RED stopped before HTTP/send; GREEN covered the canonical render, outbound policy, and zero-post failure boundary. Focused presentation, embed-budget, mention, nonce, delivery-wire, formatting, typecheck, and diff checks passed. Sol high Spec: PASS (0 findings). Sol high Standards: PASS; accepted low judgment calls on redundant description raw-label checks and `post_attempts` naming. No production, Discord, database, subscription, credential, or historical-repair mutation occurred.
