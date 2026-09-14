-- Direct Discord user mentions for the sov and watchlist feeds (ticket 20).
--
-- Both feeds already carry an optional `role_id` role ping on their
-- per-channel subscriptions. Ticket 20 adds a second, independent mention
-- target: a specific Discord user, pinged beside (or instead of) the role.
-- `ping_user_id` is a nullable BIGINT holding the Discord user snowflake;
-- absent (NULL) means "no user ping", exactly as a NULL `role_id` means "no
-- role ping". Neither column is added to the delivery tables: a delivery's
-- role ping is already snapshotted into `*.role_id`, and the user ping is
-- read back from the subscription at claim time (see the delivery-claim
-- subqueries in the sov/watchlist stores), so no delivery-row migration is
-- needed and every already-stored delivery keeps rendering unchanged.

ALTER TABLE sov_subscriptions
    ADD COLUMN IF NOT EXISTS ping_user_id BIGINT;

ALTER TABLE watchlist_subscriptions
    ADD COLUMN IF NOT EXISTS ping_user_id BIGINT;
