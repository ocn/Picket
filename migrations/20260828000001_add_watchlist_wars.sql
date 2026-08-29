-- Watchlist feed, ticket 10: war declarations for watched entities.
--
-- The hourly collector head-scans the id-descending war list
-- (`GET /wars/`) above a persisted high-water mark, fetches details
-- (`GET /wars/{id}/`) for each new war id, and persists EVERY inspected war
-- (whether or not a watched party is currently involved). "Watched" is a
-- query-time derivation (a war is watched iff any of its parties matches a
-- `watchlist_entities` row of the same kind, any guild), so an entity added
-- after a war was stored begins re-fetching that war on the next cycle
-- (fixes the "late-added entity never re-fetches its pre-existing wars" bug)
-- and an entity that is unwatched simply stops re-fetching (bounding the
-- re-fetch quota). `war_declared` is only emitted the cycle a war is first
-- seen above the mark; a war stored before its party was watched is treated
-- as that party's silent war baseline (no retroactive `war_declared`). Each
-- OPEN war (`finished IS NULL`) whose derivation is currently watched is
-- re-fetched every cycle (conditional, bounded, least-recently-fetched) to
-- detect allies joining, retraction, and finish. The first scan is a silent
-- baseline (wars already open are stored without events); it spans as many
-- cycles as its detail-fetch cap requires to inspect the whole head page.
-- Events reuse the existing free-form `watchlist_deliveries.event_kind`
-- column and its dedup unique constraint (`war_declared`, `war_ally_joined`,
-- `war_retracted`, `war_finished`) with no delivery migration.

-- The single-row head-scan cursor. `high_water_mark` is the highest war id
-- ever scanned (the id-descending list means new wars have larger ids);
-- `baseline_established` flips true only once the silent baseline has
-- inspected the entire head page. Because the head page holds up to 2000 ids
-- but each cycle inspects at most the detail-fetch cap, the baseline spans
-- several cycles: `baseline_head_max` pins the head max observed on the very
-- first baseline cycle (the mark set when the baseline completes) and
-- `baseline_cursor` records the lowest war id inspected so far, so each cycle
-- resumes strictly below it by paging DOWNWARD (`?max_war_id=cursor`) instead
-- of re-reading the head, which drifts as new wars are declared each hour --
-- ids below the cursor are always reachable regardless of head drift.
-- `baseline_floor` pins the bottom id of the ORIGINAL head page so the
-- baseline inspects exactly the ids that existed at start (`[floor, head_max]`)
-- and stops once the cursor reaches the floor; ids strictly below the original
-- window are out of scope by design. All three are NULL before the baseline
-- starts and irrelevant afterward. Advanced only after a page's details have been
-- processed, so a crash mid-cycle re-scans the same ids and the delivery
-- dedup keys prevent double posts. `singleton` pins one row, mirroring
-- `esi_collection_limiter_state.limiter_scope`.
CREATE TABLE IF NOT EXISTS watchlist_war_scan_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    high_water_mark BIGINT NOT NULL DEFAULT 0,
    baseline_established BOOLEAN NOT NULL DEFAULT FALSE,
    baseline_established_at TIMESTAMPTZ,
    baseline_head_max BIGINT,
    baseline_cursor BIGINT,
    baseline_floor BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per inspected war (every war the head-scan or baseline fetched a
-- detail for, watched or not). `aggressor`/`defender` each carry a kind
-- (alliance|corporation) and id, as the wire returns exactly one of
-- `alliance_id`/`corporation_id` on each side. `finished IS NULL` means the
-- war is still open and is re-fetched every cycle when a party is currently
-- watched; `last_fetched_at` orders the bounded least-recently-fetched
-- re-fetch. Finished wars older than the retention window are pruned so the
-- table stays bounded to the open-war population plus recently-finished wars.
CREATE TABLE IF NOT EXISTS watchlist_wars (
    war_id BIGINT PRIMARY KEY,
    aggressor_kind TEXT NOT NULL CHECK (aggressor_kind IN ('alliance', 'corporation')),
    aggressor_id BIGINT NOT NULL,
    defender_kind TEXT NOT NULL CHECK (defender_kind IN ('alliance', 'corporation')),
    defender_id BIGINT NOT NULL,
    declared TIMESTAMPTZ,
    started TIMESTAMPTZ,
    finished TIMESTAMPTZ,
    retracted TIMESTAMPTZ,
    mutual BOOLEAN NOT NULL DEFAULT FALSE,
    open_for_allies BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_fetched_at TIMESTAMPTZ NOT NULL
);

-- The bounded re-fetch pass orders open wars by `last_fetched_at`.
CREATE INDEX IF NOT EXISTS watchlist_wars_open_fetch_idx
    ON watchlist_wars (last_fetched_at)
    WHERE finished IS NULL;

-- The ally list of a stored war, as a child table so a newly-appearing ally
-- is detected by its absence here and a `war_ally_joined` event keyed by the
-- ally fires once. `first_seen_at` records when the ally was first observed.
CREATE TABLE IF NOT EXISTS watchlist_war_allies (
    war_id BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('alliance', 'corporation')),
    ally_id BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (war_id, kind, ally_id),
    FOREIGN KEY (war_id) REFERENCES watchlist_wars (war_id) ON DELETE CASCADE
);

-- Per-war-id detail-fetch failure ledger for the head-scan (bounded skip).
-- A war id the head-scan cannot resolve otherwise freezes the high-water mark
-- below it forever (`GET /wars/{id}/` 404s for ids that no longer resolve, and
-- a persistently-failing id would block every later declaration). A 404 is a
-- permanent absence: recorded and skipped immediately (the mark advances past
-- it). Any other error is skipped after
-- `WAR_FETCH_FAILURE_SKIP_THRESHOLD` consecutive failed cycles (the mark
-- advances past it, logged and counted). A successful fetch clears the row.
CREATE TABLE IF NOT EXISTS watchlist_war_fetch_attempts (
    war_id BIGINT PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    first_failed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
