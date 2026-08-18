ALTER TABLE contract_outbound_deliveries
    ADD COLUMN IF NOT EXISTS desired_message JSONB,
    ADD COLUMN IF NOT EXISTS repair_status TEXT NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS repair_prepared_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS repair_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_repair_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_repair_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS repair_next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS repair_failure_kind TEXT,
    ADD COLUMN IF NOT EXISTS repair_last_error TEXT,
    ADD COLUMN IF NOT EXISTS repair_failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS repair_failure_resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS repair_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS repair_claim_token TEXT,
    ADD COLUMN IF NOT EXISTS repair_claimed_revision BIGINT,
    ADD COLUMN IF NOT EXISTS repair_claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS repair_lease_until TIMESTAMPTZ;

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_outbound_deliveries_repair_status_check
    CHECK (repair_status IN ('none', 'pending', 'permanent'));

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_outbound_deliveries_repair_failure_kind_check
    CHECK (repair_failure_kind IS NULL OR repair_failure_kind IN ('transient', 'ambiguous', 'permanent'));

CREATE INDEX IF NOT EXISTS contract_outbound_deliveries_pending_repair_idx
    ON contract_outbound_deliveries (repair_prepared_at, id)
    WHERE status = 'sent' AND repair_status = 'pending';

CREATE TABLE contract_delivery_audit (
    id BIGSERIAL PRIMARY KEY,
    delivery_id BIGINT NOT NULL REFERENCES contract_outbound_deliveries (id),
    action TEXT NOT NULL CHECK (action IN ('requeued')),
    actor TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    detail TEXT NOT NULL
);
