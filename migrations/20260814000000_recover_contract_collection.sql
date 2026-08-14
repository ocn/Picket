ALTER TABLE regional_collection_metadata
    ADD COLUMN IF NOT EXISTS recovery_baseline_at TIMESTAMPTZ;

ALTER TABLE contract_resolution_cases
    ADD COLUMN IF NOT EXISTS suppresses_nonfinancial_notification BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE contract_collection_failures
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS classification TEXT;
