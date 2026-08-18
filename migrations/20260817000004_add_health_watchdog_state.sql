ALTER TABLE health_discord_views
    ADD COLUMN last_error TEXT,
    ADD COLUMN failure_kind TEXT CHECK (failure_kind IN ('transient', 'permanent')),
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD COLUMN config_revision TEXT NOT NULL DEFAULT '1';

CREATE TABLE health_discord_incidents (
    incident_key TEXT PRIMARY KEY CHECK (incident_key = 'current'),
    channel_id BIGINT NOT NULL,
    message_id TEXT,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    degradation JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_error TEXT,
    failure_kind TEXT CHECK (failure_kind IN ('transient', 'permanent')),
    next_attempt_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    config_revision TEXT NOT NULL DEFAULT '1',
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE health_watchdog_runtime (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    started_at TIMESTAMPTZ NOT NULL
);
