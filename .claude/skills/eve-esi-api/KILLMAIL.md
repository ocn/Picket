# zKillboard feeds and killmail boundary

Keep feed transport, third-party metadata, and ESI representations source-typed:

| Representation | Authority |
|---|---|
| R2Z2 sequence/head document | Current third-party feed position |
| R2Z2 sequence entry | Killmail ID/hash, zKillboard metadata, and optional inline ESI-shaped killmail |
| RedisQ package | Legacy long-poll delivery and zKillboard metadata |
| Embedded ESI reference | Identity of a direct official killmail fetch |
| Direct ESI killmail response | Official response schema and wire semantics |

Validate an R2Z2 inline killmail's schema and ID before using it; fetch the embedded ESI reference when inline data is absent. Keep zKillboard valuation/location hints outside ESI models and authorization facts.

## R2Z2 sequence state

- Startup resumes the persisted next sequence or reads the current head when no valid checkpoint exists.
- A valid `200` entry advances the in-memory sequence, then attempts an atomic temp-file write and rename of the next sequence before pipeline handoff. Serialization, write, or rename failure is logged and handoff still proceeds.
- A null placeholder advances the sequence without producing a killmail.
- A historical `404` below the current head advances past the gap; a head/ahead `404` retains the sequence and may resync after the configured bound.
- `429` uses jittered delay; server/transport failures use bounded exponential backoff and retain the sequence.
- Restart resumes the last successfully persisted sequence. Downstream processing after handoff does not rewind it; a failed checkpoint save can therefore replay the entry after restart.

## RedisQ boundary

RedisQ returns a nullable package through a long poll. A null package produces no work; malformed HTML/JSON is a feed error. RedisQ metadata and its embedded ESI reference retain their separate authority.

## Verification

Cover checkpoint load/corruption, checkpoint serialization/write/rename failure, current-head initialization, valid entry, null placeholder, historical/head `404`, resync, `429`, server/transport retry, restart/replay after save failure, inline-killmail validation, fallback ESI fetch, RedisQ timeout/null/malformed responses, and source-specific diagnostics.

Project sources: `src/feed/r2z2.rs`, `src/feed/redisq.rs`, `src/feed/mod.rs`, `src/models.rs`, `src/pipeline.rs`, and `docs/r2z2-migration.md`.

Third-party source: [zKillboard RedisQ and R2Z2 transition](https://github.com/zKillboard/RedisQ).
