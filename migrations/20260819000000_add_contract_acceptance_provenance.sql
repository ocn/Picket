ALTER TABLE contract_resolution_cases
    ADD COLUMN IF NOT EXISTS acceptance_provenance TEXT;

ALTER TABLE contract_resolution_cases
    ADD CONSTRAINT contract_resolution_cases_acceptance_provenance_check
    CHECK (acceptance_provenance IS NULL OR acceptance_provenance IN (
        'accepted_by_player',
        'no_content'
    ));

WITH evidence AS (
    SELECT DISTINCT ON (region_id, contract_id)
        region_id,
        contract_id,
        detail
    FROM contract_lifecycle_evidence
    WHERE evidence_kind = 'acceptance_confirmed'
    ORDER BY region_id, contract_id, observed_at DESC, id DESC
)
UPDATE contract_resolution_cases AS resolution
SET acceptance_provenance = CASE evidence.detail ->> 'response'
    WHEN '403_accepted_by_player' THEN 'accepted_by_player'
    WHEN '204_no_content' THEN 'no_content'
    ELSE NULL
END
FROM evidence
WHERE resolution.region_id = evidence.region_id
  AND resolution.contract_id = evidence.contract_id
  AND resolution.state = 'acceptance_confirmed'
  AND resolution.acceptance_provenance IS NULL;

UPDATE contract_resolution_cases
SET state = 'closed_outcome_unknown',
    notification_pending = FALSE,
    updated_at = now()
WHERE state = 'acceptance_confirmed'
  AND acceptance_provenance IS DISTINCT FROM 'accepted_by_player';
