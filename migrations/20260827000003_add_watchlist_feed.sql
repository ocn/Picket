-- Watchlist feed (ticket 08: registry + corporation join/leave alerts).
--
-- A guild maintains a per-guild registry of watched alliances and
-- corporations (`watchlist_entities`), an hourly collector diffs each
-- watched alliance's corporation list against a global membership snapshot
-- (`watchlist_alliance_corps`), and emits `corp_joined` / `corp_left`
-- events that fan out to the guild's channel subscriptions
-- (`watchlist_subscriptions`) through a durable, lease-claimed delivery
-- path (`watchlist_deliveries`) sharing the same restart-safe columns as
-- `sov_alert_deliveries` (see
-- `migrations/20260826000000_add_sov_campaign_feed.sql`). Later tickets
-- (09 member deltas / corp alliance change, 10 wars) extend the same
-- tables and the free-form `event_kind` column with no further migration.

-- The per-guild registry of Watched Entities (spec "Watchlist feed":
-- "Registry per guild: kind (alliance or corporation), entity id, ticker,
-- name, who added it, when. Unique per guild/kind/entity."). `ticker` and
-- `name` are resolved once at `/watch add` time and stored verbatim so the
-- collector and embeds need no re-resolution for the alliance side.
CREATE TABLE IF NOT EXISTS watchlist_entities (
    guild_id BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('alliance', 'corporation')),
    entity_id BIGINT NOT NULL,
    ticker TEXT,
    name TEXT,
    added_by BIGINT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, kind, entity_id)
);

CREATE INDEX IF NOT EXISTS watchlist_entities_alliance_idx
    ON watchlist_entities (entity_id)
    WHERE kind = 'alliance';

-- The global alliance -> corporation membership snapshot the hourly
-- collector diffs against (spec "Hourly collectors: watched alliance
-- corporation lists diffed against the last snapshot"). One row per
-- (alliance, corporation) ever observed; `left_at` is set when a
-- corporation disappears from a fresh listing (a Corporation Left event)
-- and cleared again on a later rejoin (a fresh Corporation Joined). The
-- snapshot is global rather than per-guild because the corporation list of
-- an alliance is the same regardless of which guild watches it; events fan
-- out to every guild that watches the alliance at delivery time.
CREATE TABLE IF NOT EXISTS watchlist_alliance_corps (
    alliance_id BIGINT NOT NULL,
    corporation_id BIGINT NOT NULL,
    corporation_name TEXT,
    corporation_ticker TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    left_at TIMESTAMPTZ,
    PRIMARY KEY (alliance_id, corporation_id)
);

CREATE INDEX IF NOT EXISTS watchlist_alliance_corps_alliance_idx
    ON watchlist_alliance_corps (alliance_id);

-- Per-alliance Sov Baseline marker (spec user story 53: "silent baseline
-- on the first watchlist snapshot ... adding an alliance does not announce
-- all its existing corporations"). Present once the first successful
-- corporation listing for an alliance has been fully processed; its
-- absence means every corporation in the next successful listing for that
-- alliance is baseline and produces no event.
CREATE TABLE IF NOT EXISTS watchlist_alliance_baseline (
    alliance_id BIGINT PRIMARY KEY,
    established_at TIMESTAMPTZ NOT NULL
);

-- Per-channel subscriptions choosing which event kinds to receive and an
-- optional role ping (spec "Subscriptions per channel choose event kinds
-- and a role ping"). `event_kinds` is a free-form TEXT[] so later tickets
-- add kinds without a migration; `options` is an opaque forward-compatible
-- JSONB document mirroring `sov_subscriptions.options`.
CREATE TABLE IF NOT EXISTS watchlist_subscriptions (
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    event_kinds TEXT[] NOT NULL DEFAULT '{}'::text[],
    role_id BIGINT,
    options JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id, name)
);

-- Durable, lease-claimed delivery rows (same restart-safe columns as
-- `sov_alert_deliveries`): one embed per event per subscription, deduped by
-- the unique constraint on (subscription, event_kind, entity_id,
-- evidence_key). `entity_id` is the Watched Entity the event is about (the
-- alliance whose membership changed); `evidence_key` is the corporation id
-- plus the transition date, so a corporation that leaves and later rejoins
-- produces distinct, separately-deduped deliveries.
CREATE TABLE IF NOT EXISTS watchlist_deliveries (
    id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_name TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    evidence_key TEXT NOT NULL,
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
    UNIQUE (guild_id, channel_id, subscription_name, event_kind, entity_id, evidence_key),
    FOREIGN KEY (guild_id, channel_id, subscription_name)
        REFERENCES watchlist_subscriptions (guild_id, channel_id, name)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS watchlist_deliveries_prepared_claim_idx
    ON watchlist_deliveries (delivery_lease_until, prepared_at, id)
    WHERE status = 'prepared';
