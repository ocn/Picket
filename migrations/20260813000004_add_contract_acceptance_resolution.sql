CREATE TABLE contract_resolution_cases (
    region_id BIGINT NOT NULL,
    contract_id BIGINT NOT NULL,
    contract JSONB NOT NULL,
    manifest JSONB NOT NULL,
    last_public_observed_at TIMESTAMPTZ NOT NULL,
    absence_observed_at TIMESTAMPTZ NOT NULL,
    next_probe_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    state TEXT NOT NULL CHECK (state IN ('awaiting_resolution', 'acceptance_confirmed')),
    acceptance_evidence_at TIMESTAMPTZ,
    acceptance_response_metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (region_id, contract_id)
);

CREATE INDEX contract_resolution_cases_pending_probe_idx
    ON contract_resolution_cases (next_probe_at)
    WHERE state = 'awaiting_resolution';
