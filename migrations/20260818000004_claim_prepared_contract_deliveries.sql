ALTER TABLE contract_outbound_deliveries
    ADD COLUMN IF NOT EXISTS delivery_claim_token TEXT,
    ADD COLUMN IF NOT EXISTS delivery_claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS delivery_lease_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS contract_outbound_deliveries_prepared_claim_idx
    ON contract_outbound_deliveries (delivery_lease_until, strategic_priority, relevant_isk DESC, confirmed_at, id)
    WHERE status = 'prepared';
