CREATE TABLE regional_recovery_epochs (
    id BIGSERIAL PRIMARY KEY,
    region_id BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    initial_complete_at TIMESTAMPTZ,
    initialization_source TEXT NOT NULL,
    UNIQUE (id, region_id)
);

CREATE INDEX regional_recovery_epochs_region_started_idx
    ON regional_recovery_epochs (region_id, started_at, id);

ALTER TABLE regional_collection_metadata
    ADD COLUMN IF NOT EXISTS current_recovery_epoch_id BIGINT,
    ADD COLUMN IF NOT EXISTS requires_silent_baseline BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE regional_collection_metadata
    ADD CONSTRAINT regional_collection_metadata_current_recovery_epoch_fkey
    FOREIGN KEY (current_recovery_epoch_id, region_id)
    REFERENCES regional_recovery_epochs (id, region_id);

CREATE TABLE regional_observation_batches (
    id BIGSERIAL PRIMARY KEY,
    region_id BIGINT NOT NULL,
    recovery_epoch_id BIGINT,
    attempt_started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('complete', 'inconclusive')),
    expected_pages INTEGER,
    pages_attempted INTEGER NOT NULL DEFAULT 0,
    pages_observed INTEGER NOT NULL DEFAULT 0,
    observed_contract_count BIGINT NOT NULL DEFAULT 0,
    resolved_contract_count BIGINT NOT NULL DEFAULT 0,
    manifest_failure_count BIGINT NOT NULL DEFAULT 0,
    consistency_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_classification TEXT,
    failure_detail TEXT,
    retry_after TIMESTAMPTZ,
    CHECK (completed_at >= attempt_started_at),
    CHECK (expected_pages IS NULL OR expected_pages >= 0),
    CHECK (pages_attempted >= 0 AND pages_observed >= 0),
    CHECK (observed_contract_count >= 0 AND resolved_contract_count >= 0 AND manifest_failure_count >= 0),
    CHECK (
        (outcome = 'complete'
            AND recovery_epoch_id IS NOT NULL
            AND expected_pages IS NOT NULL
            AND expected_pages > 0)
        OR (outcome = 'inconclusive' AND failure_classification IS NOT NULL)
    ),
    FOREIGN KEY (recovery_epoch_id, region_id)
        REFERENCES regional_recovery_epochs (id, region_id)
);

CREATE INDEX regional_observation_batches_region_epoch_complete_idx
    ON regional_observation_batches (region_id, recovery_epoch_id, completed_at DESC, id DESC)
    WHERE outcome = 'complete';

CREATE INDEX regional_observation_batches_retry_after_idx
    ON regional_observation_batches (retry_after)
    WHERE retry_after IS NOT NULL;

CREATE TABLE regional_epoch_contract_presence (
    recovery_epoch_id BIGINT NOT NULL,
    region_id BIGINT NOT NULL,
    contract_id BIGINT NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (recovery_epoch_id, region_id, contract_id),
    FOREIGN KEY (recovery_epoch_id, region_id)
        REFERENCES regional_recovery_epochs (id, region_id)
);

WITH trustworthy_metadata AS (
    SELECT region_id, baseline_at, last_complete_at
    FROM regional_collection_metadata
    WHERE baseline_at IS NOT NULL
      AND last_complete_at IS NOT NULL
      AND complete_observations > 0
), initialized_epochs AS (
    INSERT INTO regional_recovery_epochs (
        region_id,
        started_at,
        initial_complete_at,
        initialization_source
    )
    SELECT region_id, baseline_at, last_complete_at, 'migration_trustworthy_metadata'
    FROM trustworthy_metadata
    RETURNING id, region_id
)
UPDATE regional_collection_metadata AS metadata
SET current_recovery_epoch_id = initialized_epochs.id,
    requires_silent_baseline = FALSE
FROM initialized_epochs
WHERE metadata.region_id = initialized_epochs.region_id;

INSERT INTO regional_epoch_contract_presence (
    recovery_epoch_id,
    region_id,
    contract_id,
    last_observed_at
)
SELECT
    metadata.current_recovery_epoch_id,
    final_snapshot_contracts.region_id,
    final_snapshot_contracts.contract_id,
    final_snapshot_contracts.last_observed_at
FROM (
    SELECT
        presence.region_id,
        presence.contract_id,
        presence.last_observed_at
    FROM presence_intervals AS presence
    JOIN regional_collection_metadata AS metadata
      ON metadata.region_id = presence.region_id
    WHERE metadata.current_recovery_epoch_id IS NOT NULL
      AND presence.last_observed_at = metadata.last_complete_at

    UNION

    SELECT
        pending.region_id,
        pending.contract_id,
        pending.last_observed_at
    FROM contract_manifest_pending AS pending
    JOIN regional_collection_metadata AS metadata
      ON metadata.region_id = pending.region_id
    WHERE metadata.current_recovery_epoch_id IS NOT NULL
      AND pending.last_observed_at = metadata.last_complete_at
) AS final_snapshot_contracts
JOIN regional_collection_metadata AS metadata
  ON metadata.region_id = final_snapshot_contracts.region_id
ON CONFLICT (recovery_epoch_id, region_id, contract_id)
DO UPDATE SET last_observed_at = EXCLUDED.last_observed_at;

UPDATE regional_collection_metadata
SET requires_silent_baseline = TRUE
WHERE current_recovery_epoch_id IS NULL;
