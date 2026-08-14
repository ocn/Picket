ALTER TABLE contract_outbound_deliveries
    ADD COLUMN IF NOT EXISTS failure_resolved_at TIMESTAMPTZ;

ALTER TABLE contract_collection_failures
    ADD COLUMN IF NOT EXISTS contract_id BIGINT,
    ADD COLUMN IF NOT EXISTS resource_key TEXT;

CREATE INDEX IF NOT EXISTS contract_collection_failures_unresolved_scope_idx
    ON contract_collection_failures (failure_kind, region_id, contract_id, resource_key)
    WHERE resolved_at IS NULL AND classification IS NULL;

CREATE INDEX IF NOT EXISTS contract_outbound_deliveries_unresolved_failure_idx
    ON contract_outbound_deliveries (last_attempt_at, id)
    WHERE failure_kind IS NOT NULL AND failure_resolved_at IS NULL;
