# 13 — Backfill historical combat into revisable Battle Reports

**What to build:** Import an initial ninety days of available killmail evidence into inspectable report history with gaps, revision handling and no fresh discovery alerts.

**Blocked by:** 05 — Recover battle collection without interrupting ordinary alerts; 06 — Configure and inspect geographic coverage; 10 — Compare external report definitions and correct local boundaries

**Status:** draft — awaiting breakdown approval

- [ ] Use documented archive sources with persisted import progress, idempotent killmail handling and configurable geographic scope; an operator can resume a partial import.
- [ ] Recover available historical report evidence and reconcile changed archive records through the existing membership, revision and external-source comparison contracts.
- [ ] Expose history and import coverage in report queries/UI, preserving unavailable intervals and valuation enrichment limits instead of fabricating completeness.
- [ ] Keep event-time replay separate from as-known-at-the-time replay; absent publication or original receipt evidence must not be synthesized.
- [ ] Historical imports do not create fresh alerts or mentions; changes to already-notified reports use their existing correction path.
- [ ] Verify resumed imports, duplicate and changed daily records, missing files, late evidence, unavailable prices and source retention limits through fixture-backed sources and real temporary storage.

**Spec coverage:** User Stories 40, 46, 47, 48, 55. All accepted evidence, privacy and ordinary-feed-availability constraints apply.

