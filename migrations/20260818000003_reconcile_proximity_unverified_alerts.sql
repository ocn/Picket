CREATE INDEX IF NOT EXISTS contract_deferred_proximity_location_idx
    ON contract_deferred_subscription_matches ((event #>> '{contract,start_location_id}'))
    WHERE proximity_unverified;

ALTER TABLE contract_outbound_deliveries
    ADD COLUMN IF NOT EXISTS delivery_kind TEXT NOT NULL DEFAULT 'event',
    ADD COLUMN IF NOT EXISTS proximity_promotion_ping BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE contract_outbound_deliveries AS delivery
SET proximity_promotion_ping = CASE delivery.event_kind
    WHEN 'listed' THEN subscriptions.event_actions ->> 'listed' IN ('post_and_ping', 'post_and_ping_everyone')
    WHEN 'sale_confirmed' THEN subscriptions.event_actions ->> 'sale_confirmed' IN ('post_and_ping', 'post_and_ping_everyone')
    WHEN 'purchase_confirmed' THEN subscriptions.event_actions ->> 'purchase_confirmed' IN ('post_and_ping', 'post_and_ping_everyone')
    WHEN 'expired' THEN subscriptions.event_actions ->> 'expired' IN ('post_and_ping', 'post_and_ping_everyone')
    WHEN 'closed_outcome_unknown' THEN subscriptions.event_actions ->> 'closed_outcome_unknown' IN ('post_and_ping', 'post_and_ping_everyone')
    ELSE FALSE
END
FROM contract_subscriptions AS subscriptions
JOIN contract_deferred_subscription_matches AS deferred
  ON deferred.guild_id = subscriptions.guild_id
 AND deferred.channel_id = subscriptions.channel_id
 AND deferred.subscription_id = subscriptions.subscription_id
WHERE delivery.guild_id = subscriptions.guild_id
  AND delivery.channel_id = subscriptions.channel_id
  AND delivery.subscription_id = subscriptions.subscription_id
  AND deferred.contract_id = delivery.contract_id
  AND deferred.event_kind = delivery.event_kind
  AND delivery.delivery_kind = 'event'
  AND deferred.proximity_unverified;

DO $$
DECLARE
    existing_constraint TEXT;
BEGIN
    SELECT constraint_name
    INTO existing_constraint
    FROM information_schema.table_constraints AS constraints
    WHERE constraints.table_schema = current_schema()
      AND constraints.table_name = 'contract_outbound_deliveries'
      AND constraints.constraint_type = 'UNIQUE'
      AND constraints.constraint_name <> 'contract_outbound_deliveries_identity_unique'
      AND ARRAY(
          SELECT attributes.attname::TEXT
          FROM pg_constraint AS delivery_constraint
          JOIN LATERAL unnest(delivery_constraint.conkey) WITH ORDINALITY AS keys(attnum, ordinal)
            ON TRUE
          JOIN pg_attribute AS attributes
            ON attributes.attrelid = delivery_constraint.conrelid
           AND attributes.attnum = keys.attnum
          WHERE delivery_constraint.conrelid = 'contract_outbound_deliveries'::regclass
            AND delivery_constraint.conname = constraints.constraint_name
          ORDER BY keys.ordinal
      ) = ARRAY[
          'guild_id',
          'channel_id',
          'subscription_id',
          'contract_id',
          'event_kind'
      ]
    LIMIT 1;

    IF existing_constraint IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE contract_outbound_deliveries DROP CONSTRAINT %I',
            existing_constraint
        );
    END IF;
END $$;

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_outbound_deliveries_identity_unique
    UNIQUE (guild_id, channel_id, subscription_id, contract_id, event_kind, delivery_kind);

ALTER TABLE contract_outbound_deliveries
    ADD CONSTRAINT contract_outbound_deliveries_delivery_kind_check
    CHECK (delivery_kind IN ('event', 'proximity_promotion'));
