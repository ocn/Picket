# Use PostgreSQL for contract intelligence state

The contract-intelligence subsystem will use PostgreSQL for observations, lifecycle resolution, subscription matches, and Discord delivery state because independently cached public ESI resources must be reconciled durably and confirmed alerts must remain restart-safe and deduplicated. Existing JSON-backed configuration, caches, standings, and feed checkpoints remain unchanged; migrating them is a separate future project, accepting a bounded temporary persistence split to avoid coupling contract delivery to an unrelated cutover.
