-- Reachability transition state (ticket 06: "became reachable"
-- transitions). Reachability is subscription-relative -- whether a
-- campaign counts as "reachable" depends on the subscription's own
-- `Reachable { max_jumps, allow_frigate_holes }` leaf -- so state is
-- persisted per (subscription, campaign), not per system alone. Only
-- subscriptions whose filter contains a `Reachable` leaf ever get a row
-- here (`SovFilterNode::has_reachable_leaf`); `reachable` tracks that leaf
-- alone, independent of whether the subscription's full filter matches
-- (see `src/sov_feed/collector.rs`'s `evaluate_stage_cycle` doc comment).
--
-- `transition_sequence` increments only on an observed false->true flip
-- and is folded into the `reachable:<seq>` Alert Stage key stored in
-- `sov_alert_deliveries.stage` (that table's existing free-form TEXT
-- column and unique constraint need no migration of their own -- see
-- `migrations/20260826000000_add_sov_campaign_feed.sql`'s doc comment).
-- No prior row means "first observation": the initial `INSERT` never
-- counts as a transition, so a campaign reachable at baseline never fires
-- (spec "Reachability": "only an observed unreachable-to-reachable
-- transition emits an event").
--
-- `announced` (fix round finding 2) decouples "a false->true transition
-- was observed" from "the reachable:<seq> alert has been fired for it":
-- the *leaf* can flip to reachable well before the subscription's *full*
-- filter matches (e.g. `And(Reachable, VulnerableWithin(12h))` with the
-- campaign still 20h out), and the fix round requires that alert not be
-- lost if the leaf never flips again. `announced` starts `FALSE` on a
-- fresh false->true transition and is set `TRUE` only once the collector
-- has actually prepared the `reachable:<seq>` delivery (see
-- `SovStore::mark_reachability_announced`); every stage-evaluation pass
-- re-checks any row with `reachable = TRUE AND announced = FALSE`
-- against the full filter, not only the pass that produced the
-- transition. A baseline row (first observation) is inserted already
-- `announced = TRUE`, since a baseline observation must never fire
-- regardless of when the full filter later matches. A true->false flip
-- also resets `announced = TRUE`: there is nothing pending to announce
-- while unreachable, and the next false->true flip starts a fresh
-- transition (and sequence) with its own `announced = FALSE`.
--
-- Pruned alongside its campaign: `SovStore::mark_ended` deletes every
-- state row for a campaign the moment it is marked ended, in the same
-- transaction as the `ended_at` update (ticket: "Ended campaigns are
-- skipped and their state may be pruned with the campaign"; fix round
-- finding 4).
CREATE TABLE IF NOT EXISTS sov_reachability_state (
    guild_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    subscription_name TEXT NOT NULL,
    campaign_id BIGINT NOT NULL,
    reachable BOOLEAN NOT NULL,
    since TIMESTAMPTZ NOT NULL,
    transition_sequence BIGINT NOT NULL DEFAULT 0,
    announced BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, channel_id, subscription_name, campaign_id),
    FOREIGN KEY (guild_id, channel_id, subscription_name)
        REFERENCES sov_subscriptions (guild_id, channel_id, name)
        ON DELETE CASCADE
);

-- Supports `SovStore::mark_ended`'s `DELETE ... WHERE campaign_id =
-- ANY($1)` (fix round finding 5); the table has no other index usable for
-- that predicate since the primary key leads with `guild_id`.
CREATE INDEX IF NOT EXISTS sov_reachability_state_campaign_id_idx
    ON sov_reachability_state (campaign_id);
