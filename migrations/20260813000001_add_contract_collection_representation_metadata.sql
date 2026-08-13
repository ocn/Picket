ALTER TABLE esi_cache_metadata
    ADD COLUMN IF NOT EXISTS last_modified TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS esi_collection_limiter_state (
    limiter_scope BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (limiter_scope),
    pause_until TIMESTAMPTZ,
    error_limit_remain BIGINT,
    error_limit_reset BIGINT,
    rate_limit_group TEXT,
    rate_limit_limit TEXT,
    rate_limit_remaining BIGINT,
    rate_limit_used BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
