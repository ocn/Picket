# R2Z2 Feed Support

## What changed

zKillboard is sunsetting RedisQ on May 31, 2026. This release adds R2Z2 as a
replacement killmail feed provider and makes it the default for new deployments.

The update also adds HTTP timeouts to all outbound requests (ESI, Fuzzwork,
feed polling) and a per-killmail processing watchdog. These fixes prevent the
main loop from hanging indefinitely on a stalled HTTP call -- the root cause
of a 48-hour production outage in February 2026.

## Switching to R2Z2

Set one environment variable:

```
KILLMAIL_FEED_PROVIDER=r2z2
```

Or in `docker-compose.yaml`:

```yaml
environment:
  KILLMAIL_FEED_PROVIDER: r2z2
```

To stay on RedisQ until it shuts down, set `KILLMAIL_FEED_PROVIDER=redisq`
(or omit the variable -- RedisQ is still the compile-time default).

## How R2Z2 works

R2Z2 provides killmails as a numbered sequence of JSON files hosted at
`https://r2z2.zkillboard.com/ephemeral/`. Each killmail gets a monotonically
increasing sequence number.

The feed operates as a simple state machine:

1. **Startup** -- Read `config/r2z2_sequence.json` for a persisted checkpoint.
   If no checkpoint exists, fetch the latest sequence number from
   `/ephemeral/sequence.json` and start from there.

2. **Poll** -- Request `/ephemeral/{sequence}.json`.
   - **200 OK** -- Parse the killmail, advance the sequence, persist the
     checkpoint, and hand the killmail to the processing pipeline.
   - **404 Not Found** -- The next killmail hasn't been published yet. Wait
     `R2Z2_POLL_INTERVAL_SECS` (default 6s) and retry the same sequence.
   - **429 Too Many Requests** -- Wait 10-20s (jittered) and retry.
   - **5xx / transport error** -- Exponential backoff (1s to 60s, jittered)
     and retry.

3. **Resync** -- If the feed hits `R2Z2_MAX_CONSECUTIVE_404S` (default 10) or
   stays in the 404 loop for longer than `R2Z2_RESYNC_TIMEOUT_SECS` (default
   300s), it re-fetches `/ephemeral/sequence.json` to jump to the current head.
   This handles gaps in the sequence caused by the ephemeral retention window.

4. **Checkpoint** -- After each successfully processed killmail, the next
   sequence number is atomically written to `config/r2z2_sequence.json`
   (write-to-tmp then rename). On restart the feed resumes exactly where it
   left off.

## Configuration

All settings have sensible defaults. Override via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KILLMAIL_FEED_PROVIDER` | `redisq` | `redisq` or `r2z2` |
| `R2Z2_CONNECT_TIMEOUT_SECS` | 10 | TCP connect timeout |
| `R2Z2_REQUEST_TIMEOUT_SECS` | 15 | Per-request timeout |
| `R2Z2_POLL_INTERVAL_SECS` | 6 | Delay between polls on 404 |
| `R2Z2_MAX_CONSECUTIVE_404S` | 10 | 404s before resync |
| `R2Z2_RESYNC_TIMEOUT_SECS` | 300 | Max time in 404 loop before resync |
| `ESI_HTTP_TIMEOUT_SECS` | 15 | ESI/Fuzzwork client timeout |
| `KILLMAIL_PROCESS_TIMEOUT_SECS` | 60 | Watchdog: max time per killmail |

## Timeout hardening (applies to both providers)

- **ESI client**: All ESI, Fuzzwork, and SSO HTTP calls now have a configurable
  timeout (default 15s). Previously the client had no timeout, which allowed a
  single stalled request to block the feed permanently.

- **Killmail watchdog**: Each killmail is processed inside a
  `tokio::time::timeout` wrapper (default 60s). If ESI loading or Discord
  message sending stalls, the killmail is skipped and the feed continues.

- **RedisQ client**: Now uses separate connect and request timeouts instead of
  a per-request `.timeout()` call.

## Files

| Path | Role |
|------|------|
| `src/feed/mod.rs` | `KillmailFeed` trait and `FeedError` enum |
| `src/feed/redisq.rs` | RedisQ feed implementation |
| `src/feed/r2z2.rs` | R2Z2 feed implementation |
| `config/r2z2_sequence.json` | Persisted sequence checkpoint (created at runtime) |
| `docs/env.sample` | All environment variables with defaults |
