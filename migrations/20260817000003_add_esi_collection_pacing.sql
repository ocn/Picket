ALTER TABLE esi_collection_limiter_state
    ADD COLUMN IF NOT EXISTS next_request_at TIMESTAMPTZ;

ALTER TABLE esi_collection_limiter_state
    ADD COLUMN IF NOT EXISTS pacing_active BOOLEAN NOT NULL DEFAULT FALSE;
