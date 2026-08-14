ALTER TABLE contract_subscriptions
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

DO $$
DECLARE
    foreign_key_name TEXT;
BEGIN
    SELECT delivery_constraint.constraint_name
    INTO foreign_key_name
    FROM information_schema.table_constraints AS delivery_constraint
    JOIN information_schema.referential_constraints AS reference
      ON reference.constraint_schema = delivery_constraint.constraint_schema
     AND reference.constraint_name = delivery_constraint.constraint_name
    JOIN information_schema.table_constraints AS subscription_constraint
      ON subscription_constraint.constraint_schema = reference.unique_constraint_schema
     AND subscription_constraint.constraint_name = reference.unique_constraint_name
    WHERE delivery_constraint.table_schema = current_schema()
      AND delivery_constraint.table_name = 'contract_outbound_deliveries'
      AND delivery_constraint.constraint_type = 'FOREIGN KEY'
      AND subscription_constraint.table_schema = current_schema()
      AND subscription_constraint.table_name = 'contract_subscriptions'
    LIMIT 1;

    IF foreign_key_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE contract_outbound_deliveries DROP CONSTRAINT %I',
            foreign_key_name
        );
    END IF;
END $$;

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_delivery_subscription_history_fkey
    FOREIGN KEY (guild_id, channel_id, subscription_id)
    REFERENCES contract_subscriptions (guild_id, channel_id, subscription_id)
    ON DELETE RESTRICT;

CREATE TABLE contract_deferred_subscription_matches (
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_id TEXT NOT NULL,
    contract_id BIGINT NOT NULL,
    event_kind TEXT NOT NULL,
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id, subscription_id, contract_id, event_kind)
);
