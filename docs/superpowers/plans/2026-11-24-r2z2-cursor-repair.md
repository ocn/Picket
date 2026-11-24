# Prevent R2Z2 resync replay

Status: approved for implementation by Sean on 2026-11-24.
Delivery: reviewed pull request, open and unmerged; no Deployment.
Local tracker: `.scratch/duplicate-killmail-posts/issues/01-prevent-resync-replay.md`.

## Problem and evidence

A guild with one Killfeed Subscription reported repeated killmail messages.
Retained diagnostic facts and runtime logs show distinct confirmed Discord
messages for the same killmail following repeated R2Z2 resyncs to one discovery
pointer. An offline reproduction of the real adapter emits the consumed
killmail again after the next sequence returns 404.

The [upstream API documentation](https://zkillboard.com/api/docs/) explains that
the discovery pointer updates every 51 killmails. It can lag a consumer that
has already fetched subsequent sequences. Replacing the pending cursor with
that pointer therefore replays consumed sequences.

## Approved behavior

- Retain the pending cursor when a 404's discovery pointer is stale or equal.
- Reset count/time resync tracking without refetching the pointer or rewinding.
- Preserve the configured polling delay through resync.
- Preserve checkpoint startup, valid killmails, null placeholders, and forward
  recovery past missing historical sequences.
- Use the discovery value already fetched for the 404 decision. A second read
  must not jump over a pending sequence that becomes available meanwhile.

## Verification

Exercise `KillmailFeed::next` through the real R2Z2 HTTP adapter with a local
HTTP fixture and temporary checkpoint. Demonstrate the original duplicate
before the fix and verify count/time resync, repeated 404s, polling delay,
checkpoint reconstruction, forward recovery, and subsequent new data.

## Boundaries and limits

This is an adapter consistency repair within the existing Killfeed, using its
existing mutex and checkpoint format. It adds no domain objects, schema, or
published interface. No ontology adoption or program process is declared.
No configured commit checks exist; enforcement setup remains separate work.

This repair does not deduplicate a killmail republished by zKillboard under a
different sequence ID, coordinate independent processes, guarantee recovery
after checkpoint write failure, or delete existing duplicate messages. Filters,
Subscription bindings, Discord delivery, and other Feeds keep their behavior.

The investigation started from local main, which contained unrelated
unpublished work. Implementation and publication use fetched origin/main so
the PR contains only this repair. Production configuration and the original
dirty checkout remain untouched.
