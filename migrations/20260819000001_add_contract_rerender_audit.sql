ALTER TABLE contract_delivery_audit
    DROP CONSTRAINT IF EXISTS contract_delivery_audit_action_check;

ALTER TABLE contract_delivery_audit
    ADD CONSTRAINT contract_delivery_audit_action_check
    CHECK (action IN ('requeued', 'rerendered'));
