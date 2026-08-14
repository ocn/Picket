ALTER TABLE contract_outbound_deliveries
    ADD COLUMN IF NOT EXISTS ping_type TEXT NOT NULL DEFAULT 'here';

ALTER TABLE contract_outbound_deliveries
    DROP CONSTRAINT IF EXISTS contract_outbound_deliveries_ping_type_check;

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_outbound_deliveries_ping_type_check
    CHECK (ping_type IN ('here', 'everyone'));
