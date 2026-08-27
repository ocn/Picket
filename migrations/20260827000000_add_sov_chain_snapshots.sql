-- Wanderer chain snapshots (ticket 05: wanderer chain reachability and
-- path risk). One row per successful poll of the Wanderer map's systems
-- and connections (spec "Chain Snapshot"; `src/sov_feed/chain.rs`).
--
-- Retention: bounded to the last 24 hours, always keeping at least the
-- single newest row even if it is itself older than that (see
-- `SovStore::prune_chain_snapshots` in `src/sov_feed/store.rs`, called
-- after every successful insert by the chain poll loop in `src/lib.rs`).
-- At the spec's two-minute poll cadence this keeps at most ~720 rows.
--
-- `fetched_at` doubles as this feed's health-check progress signal
-- (`sov_chain_progress` in `src/contract_intelligence.rs`: `max(fetched_at)`
-- across this table) -- a row is only ever inserted after a successful
-- fetch, so its presence and freshness is exactly "last successful fetch",
-- with no separate collector-state table needed.
CREATE TABLE IF NOT EXISTS sov_chain_snapshots (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL,
    systems JSONB NOT NULL,
    connections JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sov_chain_snapshots_fetched_at_idx
    ON sov_chain_snapshots (fetched_at DESC);
