# 07 — Make Discord delivery restart-safe

**What to build:** Harden outbound contract notifications so confirmed intelligence survives restarts and transient Discord failures while preserving the bot's existing per-channel ping behavior.

**Blocked by:** 03 — Support complete recursive Contract Subscription filtering; 04 — Confirm pure ship sales and purchases; 05 — Report expired and unresolved closures safely.

**Status:** ready-for-agent

- [ ] The durable delivery identity is unique per Contract Subscription, contract, and event kind while allowing different matching subscriptions in one channel to each receive a message.
- [ ] Pending delivery state is committed before Discord is called, and a successful response records the returned Discord message ID.
- [ ] Immediate retries use a stable deterministic nonce and request Discord nonce enforcement within the platform's supported recent-message window.
- [ ] Ambiguous deliveries remain retryable after the nonce window; the documented policy accepts a rare duplicate in preference to losing tactical intelligence.
- [ ] Definite permanent Discord failures are retained with actionable error context rather than retried indefinitely without classification.
- [ ] Contract and killmail notifications share one process-local five-minute ping limiter keyed by destination channel.
- [ ] The first eligible message may carry the configured `@here` or `@everyone`; all subsequent matching embeds are still posted without a mention during the window.
- [ ] Allowed-mention configuration permits only the bot-generated configured ping and prevents contract-authored text from notifying users or roles.
- [ ] A delivery batch is ordered by strategic ship-group priority, descending relevant ISK, then oldest confirmation time before ping eligibility is evaluated.
- [ ] The shared ping behavior does not change existing killmail notification semantics.
- [ ] Tests use controllable time and a fake Discord boundary to cover success, transient failure, ambiguous response, restart, nonce reuse, post-window retry, overlapping subscriptions, shared ping suppression, and batch priority.

