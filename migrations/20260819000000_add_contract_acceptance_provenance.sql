ALTER TABLE contract_resolution_cases
    ADD COLUMN IF NOT EXISTS acceptance_provenance TEXT;

ALTER TABLE contract_resolution_cases
    ADD CONSTRAINT contract_resolution_cases_acceptance_provenance_check
    CHECK (acceptance_provenance IS NULL OR acceptance_provenance IN (
        'accepted_by_player',
        'no_content'
    )) NOT VALID;

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

UPDATE contract_outbound_deliveries AS delivery
SET event = CASE
        WHEN delivery.event ? 'acceptance_evidence' THEN jsonb_set(
            delivery.event,
            '{acceptance_evidence,provenance}',
            '"accepted_by_player"'::jsonb,
            TRUE
        )
        ELSE jsonb_set(
            delivery.event,
            '{acceptance_evidence}',
            jsonb_build_object(
                'last_public_observed_at', resolution.last_public_observed_at,
                'absence_observed_at', resolution.absence_observed_at,
                'evidence_response_at', resolution.acceptance_evidence_at,
                'provenance', 'accepted_by_player'
            ),
            TRUE
        )
    END
FROM contract_resolution_cases AS resolution
WHERE delivery.contract_id = resolution.contract_id
  AND (delivery.event ->> 'region_id' IS NULL OR delivery.event ->> 'region_id' = resolution.region_id::text)
  AND delivery.event_kind IN ('sale_confirmed', 'purchase_confirmed')
  AND resolution.state = 'acceptance_confirmed'
  AND resolution.acceptance_provenance = 'accepted_by_player';

UPDATE contract_outbound_deliveries AS delivery
SET event_kind = 'closed_outcome_unknown',
    event = jsonb_set(
        delivery.event - 'acceptance_evidence',
        '{kind}',
        '"closed_outcome_unknown"'::jsonb,
        TRUE
    ),
    message = delivery.message - 'presentation_revision'
FROM contract_resolution_cases AS resolution
WHERE delivery.contract_id = resolution.contract_id
  AND (delivery.event ->> 'region_id' IS NULL OR delivery.event ->> 'region_id' = resolution.region_id::text)
  AND delivery.event_kind IN ('sale_confirmed', 'purchase_confirmed')
  AND resolution.state = 'closed_outcome_unknown';

UPDATE contract_outbound_deliveries AS delivery
SET event_kind = 'closed_outcome_unknown',
    event = jsonb_set(
        delivery.event - 'acceptance_evidence',
        '{kind}',
        '"closed_outcome_unknown"'::jsonb,
        TRUE
    ),
    message = delivery.message - 'presentation_revision'
WHERE delivery.event_kind IN ('sale_confirmed', 'purchase_confirmed')
  AND COALESCE(delivery.event #>> '{acceptance_evidence,provenance}', '') <> 'accepted_by_player'
  AND NOT EXISTS (
      SELECT 1
      FROM contract_resolution_cases AS resolution
      WHERE resolution.contract_id = delivery.contract_id
        AND (delivery.event ->> 'region_id' IS NULL OR delivery.event ->> 'region_id' = resolution.region_id::text)
        AND resolution.state = 'acceptance_confirmed'
        AND resolution.acceptance_provenance = 'accepted_by_player'
  );

ALTER TABLE contract_resolution_cases
    VALIDATE CONSTRAINT contract_resolution_cases_acceptance_provenance_check;
