UPDATE contract_resolution_cases
SET acceptance_evidence_at = absence_observed_at,
    updated_at = now()
WHERE state IN ('expired', 'closed_outcome_unknown')
  AND acceptance_evidence_at IS NULL;

UPDATE contract_outbound_deliveries AS delivery
SET status = 'failed',
    failure_kind = 'permanent',
    last_error = 'quarantined stale legacy acceptance delivery after provenance reclassification',
    failed_at = now(),
    failure_resolved_at = now(),
    ping = FALSE,
    delivery_claim_token = NULL,
    delivery_claimed_at = NULL,
    delivery_lease_until = NULL,
    desired_message = NULL,
    repair_status = 'none',
    repair_prepared_at = NULL,
    repair_attempt_count = 0,
    first_repair_attempt_at = NULL,
    last_repair_attempt_at = NULL,
    repair_next_attempt_at = NULL,
    repair_failure_kind = NULL,
    repair_last_error = NULL,
    repair_failed_at = NULL,
    repair_failure_resolved_at = NULL,
    repair_claim_token = NULL,
    repair_claimed_revision = NULL,
    repair_claimed_at = NULL,
    repair_lease_until = NULL
WHERE delivery.status = 'prepared'
  AND delivery.event_kind = 'closed_outcome_unknown'
  AND delivery.event ->> 'kind' = 'closed_outcome_unknown'
  AND (
      COALESCE(delivery.message ->> 'title', '') ILIKE '%contract accepted%'
      OR COALESCE(delivery.message ->> 'title', '') ILIKE '%sale confirmed%'
      OR COALESCE(delivery.message ->> 'title', '') ILIKE '%purchase confirmed%'
  )
  AND EXISTS (
      SELECT 1
      FROM contract_resolution_cases AS resolution
      WHERE resolution.contract_id = delivery.contract_id
        AND (delivery.event ->> 'region_id' IS NULL OR delivery.event ->> 'region_id' = resolution.region_id::text)
        AND resolution.state = 'closed_outcome_unknown'
        AND resolution.acceptance_provenance IS DISTINCT FROM 'accepted_by_player'
  );

UPDATE contract_outbound_deliveries AS delivery
SET desired_message = NULL,
    repair_status = 'none',
    repair_prepared_at = NULL,
    repair_attempt_count = 0,
    first_repair_attempt_at = NULL,
    last_repair_attempt_at = NULL,
    repair_next_attempt_at = NULL,
    repair_failure_kind = NULL,
    repair_last_error = NULL,
    repair_failed_at = NULL,
    repair_failure_resolved_at = NULL,
    repair_claim_token = NULL,
    repair_claimed_revision = NULL,
    repair_claimed_at = NULL,
    repair_lease_until = NULL
WHERE delivery.status = 'sent'
  AND delivery.event_kind = 'closed_outcome_unknown'
  AND delivery.event ->> 'kind' = 'closed_outcome_unknown'
  AND (
      COALESCE(delivery.desired_message ->> 'title', '') ILIKE '%contract accepted%'
      OR COALESCE(delivery.desired_message ->> 'title', '') ILIKE '%sale confirmed%'
      OR COALESCE(delivery.desired_message ->> 'title', '') ILIKE '%purchase confirmed%'
  )
  AND EXISTS (
      SELECT 1
      FROM contract_resolution_cases AS resolution
      WHERE resolution.contract_id = delivery.contract_id
        AND (delivery.event ->> 'region_id' IS NULL OR delivery.event ->> 'region_id' = resolution.region_id::text)
        AND resolution.state = 'closed_outcome_unknown'
        AND resolution.acceptance_provenance IS DISTINCT FROM 'accepted_by_player'
  );

UPDATE contract_outbound_deliveries AS delivery
SET event = jsonb_set(
        jsonb_set(
            delivery.event,
            '{kind}',
            to_jsonb(resolution.state),
            TRUE
        ),
        '{acceptance_evidence}',
        jsonb_strip_nulls(jsonb_build_object(
            'last_public_observed_at', resolution.last_public_observed_at,
            'absence_observed_at', resolution.absence_observed_at,
            'evidence_response_at', resolution.acceptance_evidence_at,
            'provenance', resolution.acceptance_provenance
        )),
        TRUE
    )
FROM contract_resolution_cases AS resolution
WHERE delivery.contract_id = resolution.contract_id
  AND (delivery.event ->> 'region_id' IS NULL OR delivery.event ->> 'region_id' = resolution.region_id::text)
  AND delivery.event_kind = resolution.state
  AND resolution.state IN ('expired', 'closed_outcome_unknown');
