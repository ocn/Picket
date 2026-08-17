CREATE TABLE bot_heartbeats (
    component TEXT PRIMARY KEY CHECK (component = 'bot'),
    observed_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE health_snapshots (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'critical')),
    observed_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE health_check_results (
    check_key TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'critical')),
    observed_at TIMESTAMPTZ NOT NULL,
    evidence TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0)
);

CREATE TABLE health_transitions (
    transition_identity TEXT PRIMARY KEY,
    check_key TEXT NOT NULL,
    prior_status TEXT CHECK (prior_status IN ('healthy', 'degraded', 'critical')),
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'critical')),
    observed_at TIMESTAMPTZ NOT NULL,
    evidence TEXT NOT NULL
);

CREATE INDEX health_transitions_check_observed_idx
    ON health_transitions (check_key, observed_at DESC);

CREATE TABLE health_discord_views (
    view_key TEXT PRIMARY KEY CHECK (view_key = 'primary'),
    channel_id BIGINT NOT NULL,
    message_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE health_feed_telemetry (
    source TEXT PRIMARY KEY CHECK (source IN ('r2z2_progress', 'feed_validation')),
    last_progress_at TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE health_backlog_metrics (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    due_count BIGINT NOT NULL CHECK (due_count >= 0),
    peak_due_count BIGINT NOT NULL CHECK (peak_due_count >= 0),
    growth_detected BOOLEAN NOT NULL DEFAULT FALSE,
    oldest_due_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL
);
