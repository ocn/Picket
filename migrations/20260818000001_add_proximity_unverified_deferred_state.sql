ALTER TABLE contract_deferred_subscription_matches
    ADD COLUMN proximity_unverified BOOLEAN NOT NULL DEFAULT FALSE;
