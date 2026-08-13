CREATE TABLE contract_subscriptions (
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_id TEXT NOT NULL,
    description TEXT NOT NULL,
    filter JSONB NOT NULL,
    event_actions JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id, subscription_id)
);

CREATE TABLE contract_outbound_deliveries (
    id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_id TEXT NOT NULL,
    contract_id BIGINT NOT NULL,
    event_kind TEXT NOT NULL,
    event JSONB NOT NULL,
    message JSONB NOT NULL,
    ping BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'sent')),
    discord_message_id TEXT,
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    UNIQUE (guild_id, channel_id, subscription_id, contract_id, event_kind),
    FOREIGN KEY (guild_id, channel_id, subscription_id)
        REFERENCES contract_subscriptions (guild_id, channel_id, subscription_id)
        ON DELETE CASCADE
);
