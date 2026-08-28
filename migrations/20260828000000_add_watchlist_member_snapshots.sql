-- Watchlist feed, ticket 09: member-count deltas and corporation alliance
-- changes.
--
-- The hourly collector fetches public corporation info
-- (`GET /corporations/{id}/`: `member_count`, `alliance_id`, `name`,
-- `ticker`) for every watched corporation and every current member
-- corporation of each watched alliance, and persists timestamped member
-- snapshots. From those snapshots it derives two new event kinds on the
-- existing free-form `watchlist_deliveries.event_kind` column (no delivery
-- migration needed): `member_delta` (a watched entity's member count moving
-- more than ten percent against the snapshot nearest seven days ago) and
-- `corp_changed_alliance` (a watched corporation whose current alliance
-- differs from its previous snapshot). Ticket 10 (wars) reuses the same
-- delivery table and adds its own free-form kinds with no further schema
-- change.

-- Timestamped member-count snapshots, one stream keyed by
-- (entity_kind, entity_id). This folds the ticket's "corporation info
-- snapshot" and the alliance aggregate into a single table so the
-- member-delta reference lookup (the snapshot nearest seven days ago) is one
-- uniform query for both entity kinds rather than a corporation table plus a
-- computed historical sum:
--   * `corporation` rows carry the corporation's own `member_count` and its
--     current `alliance_id` (nullable; NULL when unaffiliated). These are the
--     "corporation info snapshots" and also drive `corp_changed_alliance`
--     (comparing a fresh `alliance_id` against the previous snapshot's).
--   * `alliance` rows carry the alliance's aggregate member count for a
--     cycle (the sum over its current member corporations' latest snapshots);
--     `alliance_id` is always NULL for these rows.
-- One row per (kind, entity, observed_at); the collector observes at most
-- once per hour, so this is effectively per-hour. Retention: the collector
-- prunes rows older than 30 days every cycle (see
-- `WatchlistStore::prune_member_snapshots`). Thirty days comfortably exceeds
-- the seven-day reference window (with margin for gaps and the ~6.5-day
-- minimum-history rule) while bounding growth to roughly
-- 24 * 30 = 720 rows per watched/member entity.
CREATE TABLE IF NOT EXISTS watchlist_member_snapshots (
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('alliance', 'corporation')),
    entity_id BIGINT NOT NULL,
    member_count BIGINT NOT NULL,
    alliance_id BIGINT,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_kind, entity_id, observed_at)
);

-- The reference lookup orders one entity's snapshots by proximity to
-- `now - 7 days`, and the latest-snapshot and pruning queries scan by
-- observed_at; this composite index covers all three.
CREATE INDEX IF NOT EXISTS watchlist_member_snapshots_entity_time_idx
    ON watchlist_member_snapshots (entity_kind, entity_id, observed_at);

-- Prune queries scan purely by age across entities.
CREATE INDEX IF NOT EXISTS watchlist_member_snapshots_observed_at_idx
    ON watchlist_member_snapshots (observed_at);

-- Per-entity member-delta band state. The band is +/-10 % of the reference
-- count; `member_delta` fires once per crossing and re-arms only after the
-- count returns inside the band (spec user story 46). `last_fired_direction`
-- is the armed flag: NULL means armed (the count is inside the band, or the
-- entity has never fired), 'up'/'down' means the last crossing fired in that
-- direction and the entity is disarmed until it returns inside the band.
-- `reference_count` / `reference_observed_at` record the reference used at
-- the last evaluation for observability. `crossing_sequence` is a
-- monotonically increasing per-entity counter incremented once per fired
-- crossing; the delivery evidence key (entity + direction + crossing_sequence)
-- is what makes one crossing a single restart-safe delivery. Unlike the
-- earlier `reference_observed_at`-derived key (which slid as the seven-day
-- window advanced and so could mint a fresh key across a crash between send
-- and disarm, double-posting), the sequence is stable for a crossing: the
-- disarm (sequence bump + `last_fired_direction`) and the delivery-row inserts
-- commit in one transaction, so a crash anywhere afterwards yields exactly one
-- post through the lease path.
CREATE TABLE IF NOT EXISTS watchlist_member_band_state (
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('alliance', 'corporation')),
    entity_id BIGINT NOT NULL,
    reference_count BIGINT,
    reference_observed_at TIMESTAMPTZ,
    last_fired_direction TEXT CHECK (last_fired_direction IN ('up', 'down')),
    crossing_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_kind, entity_id)
);

-- Watch-scoped member-delta baseline marker. `watchlist_member_snapshots`
-- rows are shared across roles (a corporation snapshotted hourly as a member
-- of a watched alliance) and outlive a watch (30-day retention), so the mere
-- ABSENCE of prior snapshot rows is not a watch-scoped baseline: adding a
-- corporation that was already being snapshotted as an alliance member, or
-- re-adding an entity within the retention window, would otherwise fire
-- `member_delta` / `corp_changed_alliance` against pre-watch history. This
-- marker is established by the member-snapshot pass on the first successful
-- evaluation AFTER the entity became watched (and cleared + re-established by
-- `add_entity` when no guild currently watches the entity). `member_delta`
-- only considers reference snapshots observed at/after `established_at` (so it
-- naturally takes ~7 days of post-add history before it can fire), and
-- `corp_changed_alliance` for a watched corporation only compares against a
-- previous snapshot observed at/after it.
CREATE TABLE IF NOT EXISTS watchlist_member_baseline (
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('alliance', 'corporation')),
    entity_id BIGINT NOT NULL,
    established_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_kind, entity_id)
);

-- Consecutive corporation-info fetch failures, so one persistently failing
-- member corporation does not block its alliance's aggregate member count
-- forever. `current_alliance_member_count` normally requires every current
-- member to have a snapshot; a member whose fetch has failed continuously for
-- at least three cycles (`consecutive_failures >= 3`) is excluded from that
-- requirement (its last known snapshot is summed if one exists, otherwise it
-- is skipped) and surfaced as an excluded member. The counter resets on any
-- success (`200`) or `304`.
CREATE TABLE IF NOT EXISTS watchlist_corp_fetch_failures (
    corporation_id BIGINT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
