ALTER TABLE contract_outbound_deliveries
    DROP CONSTRAINT IF EXISTS contract_outbound_deliveries_status_check;

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_outbound_deliveries_status_check
    CHECK (status IN ('prepared', 'sent', 'failed'));

ALTER TABLE contract_outbound_deliveries
    ADD COLUMN IF NOT EXISTS delivery_nonce TEXT,
    ADD COLUMN IF NOT EXISTS strategic_priority BIGINT NOT NULL DEFAULT 9223372036854775807,
    ADD COLUMN IF NOT EXISTS relevant_isk DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS nonce_window_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS failure_kind TEXT,
    ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;

UPDATE contract_outbound_deliveries
SET delivery_nonce = 'ci-' || id
WHERE delivery_nonce IS NULL;

ALTER TABLE contract_outbound_deliveries
    ALTER COLUMN delivery_nonce SET NOT NULL;

CREATE INDEX IF NOT EXISTS contract_outbound_deliveries_prepared_order_idx
    ON contract_outbound_deliveries (strategic_priority, relevant_isk DESC, confirmed_at, id)
    WHERE status = 'prepared';
