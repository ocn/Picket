-- Sovereignty Hub vulnerability windows and the sovereignty ownership map
-- (ticket 07: vulnerability window shift into the guild timezone).
--
-- `sov_structures` persists `GET /sovereignty/structures/`, filtered at
-- collection time to Sovereignty Hubs only (`src/sov_feed/model.rs`'s
-- `SOV_HUB_STRUCTURE_TYPE_IDS`; legacy TCUs, if the wire ever returns one
-- again, are never inserted). `is_baseline` mirrors `sov_campaigns.is_baseline`
-- for forward compatibility (no alert in this ticket reads it -- the
-- `tz_window_entered` stage's own silent-first-observation rule lives in
-- `sov_tz_window_state` below, per (subscription, structure) exactly like
-- ticket 06's `sov_reachability_state`). A hub missing from a later
-- listing (destroyed, or sov lost) is removed outright by
-- `SovStore::remove_structures` rather than soft-ended like a campaign --
-- there is no "ended" fact worth keeping for a hub, only "currently
-- exists" -- which cascades into `sov_tz_window_state` via the foreign key
-- below.
CREATE TABLE IF NOT EXISTS sov_structures (
    structure_id BIGINT PRIMARY KEY,
    structure_type_id BIGINT NOT NULL,
    alliance_id BIGINT,
    solar_system_id BIGINT NOT NULL,
    vulnerability_occupancy_level DOUBLE PRECISION,
    vulnerable_start_time TIMESTAMPTZ,
    vulnerable_end_time TIMESTAMPTZ,
    is_baseline BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

-- Singleton marker, mirroring `sov_collector_baseline`: present once the
-- first successful structures listing has been fully processed.
CREATE TABLE IF NOT EXISTS sov_structures_baseline (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    established_at TIMESTAMPTZ NOT NULL
);

-- `GET /sovereignty/map/`: one row per system with sovereignty, holding
-- whichever of alliance/corporation/faction the wire reports (a system may
-- carry any subset of the three simultaneously -- alliance and
-- corporation together for player sov, faction alone for FW/NPC space).
-- Replaced wholesale each successful poll by `SovStore::replace_map`
-- (`ON CONFLICT` upsert plus a delete of rows absent from the fresh
-- listing, in one transaction) since the feed has no use for map history,
-- only "the current owner" (spec "Embed": "current owner from the
-- sovereignty map").
CREATE TABLE IF NOT EXISTS sov_map (
    solar_system_id BIGINT PRIMARY KEY,
    alliance_id BIGINT,
    corporation_id BIGINT,
    faction_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Per-(subscription, structure) `tz_window_entered` transition state,
-- mirroring `sov_reachability_state`'s shape and every one of its
-- invariants exactly (see that table's migration comment for the full
-- rationale): `in_window` tracks the overlap fact alone, independent of
-- whether the subscription's Defender/Region/System leaves also match;
-- `transition_sequence` increments only on an observed false->true flip
-- and is folded into the `tz_window_entered:<seq>` Alert Stage key stored
-- in `sov_alert_deliveries.stage`; no prior row means "first observation"
-- (Sov Baseline: the initial INSERT never counts as a transition, already
-- `announced = TRUE`); `announced` decouples "a transition was observed"
-- from "the alert has actually been prepared", re-checked every
-- stage-evaluation pass against the full (Reachable/VulnerableWithin-
-- ignoring) filter while `reachable`... err, `in_window = TRUE AND
-- announced = FALSE`, so a subscription combining the window with e.g. a
-- `Region` leaf that does not yet match does not lose the announcement.
-- Only subscriptions with `tz_shift_enabled = true` ever get a row here
-- (spec: "Only subscriptions with tz_shift_enabled participate").
--
-- Pruned automatically when its structure is removed (`ON DELETE CASCADE`
-- on `structure_id`) -- unlike campaigns, hubs are hard-deleted by
-- `SovStore::remove_structures` rather than soft-ended, so a plain
-- cascade suffices with no separate transactional DELETE needed (contrast
-- `SovStore::mark_ended`, which soft-ends campaigns and therefore prunes
-- `sov_reachability_state` explicitly).
CREATE TABLE IF NOT EXISTS sov_tz_window_state (
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_name TEXT NOT NULL,
    structure_id BIGINT NOT NULL,
    in_window BOOLEAN NOT NULL,
    since TIMESTAMPTZ NOT NULL,
    transition_sequence BIGINT NOT NULL DEFAULT 0,
    announced BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id, subscription_name, structure_id),
    FOREIGN KEY (guild_id, channel_id, subscription_name)
        REFERENCES sov_subscriptions (guild_id, channel_id, name)
        ON DELETE CASCADE,
    FOREIGN KEY (structure_id)
        REFERENCES sov_structures (structure_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS sov_tz_window_state_structure_id_idx
    ON sov_tz_window_state (structure_id);
