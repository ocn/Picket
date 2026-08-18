CREATE TABLE structure_resolver_runtime (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    enabled BOOLEAN NOT NULL,
    credential_revision TEXT NOT NULL CHECK (btrim(credential_revision) <> ''),
    status TEXT NOT NULL CHECK (status IN ('disabled', 'ready', 'degraded')),
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE structure_resolution_state (
    structure_id BIGINT PRIMARY KEY CHECK (structure_id > 0),
    credential_revision TEXT NOT NULL CHECK (btrim(credential_revision) <> ''),
    etag TEXT,
    solar_system_id BIGINT CHECK (solar_system_id > 0),
    observed_at TIMESTAMPTZ,
    cache_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    first_denied_at TIMESTAMPTZ,
    last_denied_at TIMESTAMPTZ,
    denied_attempts INTEGER NOT NULL DEFAULT 0 CHECK (denied_attempts >= 0),
    parked_at TIMESTAMPTZ,
    transient_failures INTEGER NOT NULL DEFAULT 0 CHECK (transient_failures >= 0),
    last_error TEXT,
    last_failure_kind TEXT CHECK (last_failure_kind IN ('access_denied', 'transient', 'degraded')),
    last_success_at TIMESTAMPTZ,
    attempt_generation BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (parked_at IS NULL OR first_denied_at IS NOT NULL)
);

CREATE INDEX structure_resolution_state_due_idx
    ON structure_resolution_state (next_attempt_at)
    WHERE parked_at IS NULL;

CREATE INDEX structure_resolution_state_lease_idx
    ON structure_resolution_state (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;

CREATE TABLE structure_resolver_backoff (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    credential_revision TEXT NOT NULL CHECK (btrim(credential_revision) <> ''),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
