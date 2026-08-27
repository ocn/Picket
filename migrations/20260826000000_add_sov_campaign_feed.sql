-- Sovereignty campaign feed (ticket 02: "appeared" alert stage only).
--
-- Schema is kept forward-compatible for later tickets in the same effort
-- (.scratch/esi-intel-feeds/spec.md): `sov_subscriptions.options` is an
-- opaque JSONB document for future per-subscription settings (T-minus
-- marks, timezone window) and `sov_alert_deliveries.stage` is a free-form
-- text column so later Alert Stages (`tminus:<minutes>`, `reachable:<n>`,
-- `tz_window_entered`) need no further migration.

CREATE TABLE IF NOT EXISTS sov_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    event_type TEXT NOT NULL,
    structure_id BIGINT NOT NULL,
    solar_system_id BIGINT NOT NULL,
    constellation_id BIGINT NOT NULL,
    defender_id BIGINT,
    defender_score DOUBLE PRECISION,
    attackers_score DOUBLE PRECISION,
    start_time TIMESTAMPTZ NOT NULL,
    is_baseline BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ
);

-- Singleton marker: present once the first successful campaigns listing has
-- been fully processed (Sov Baseline). Its absence means every campaign in
-- the next successful listing is baseline and produces no `appeared` alert.
CREATE TABLE IF NOT EXISTS sov_collector_baseline (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    established_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS sov_subscriptions (
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    filter JSONB NOT NULL,
    options JSONB NOT NULL DEFAULT '{}'::jsonb,
    role_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id, name)
);

CREATE TABLE IF NOT EXISTS sov_alert_deliveries (
    id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_name TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id BIGINT NOT NULL,
    stage TEXT NOT NULL,
    message JSONB NOT NULL,
    role_id BIGINT,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'sent', 'failed')),
    failure_kind TEXT CHECK (failure_kind IN ('transient', 'permanent')),
    last_error TEXT,
    failed_at TIMESTAMPTZ,
    delivery_nonce TEXT,
    nonce_window_until TIMESTAMPTZ,
    delivery_claim_token TEXT,
    delivery_claimed_at TIMESTAMPTZ,
    delivery_lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    discord_message_id TEXT,
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    UNIQUE (guild_id, channel_id, subscription_name, subject_kind, subject_id, stage),
    FOREIGN KEY (guild_id, channel_id, subscription_name)
        REFERENCES sov_subscriptions (guild_id, channel_id, name)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS sov_alert_deliveries_prepared_claim_idx
    ON sov_alert_deliveries (delivery_lease_until, prepared_at, id)
    WHERE status = 'prepared';
