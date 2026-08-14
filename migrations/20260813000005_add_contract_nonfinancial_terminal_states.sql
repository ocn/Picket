ALTER TABLE contract_resolution_cases
    DROP CONSTRAINT contract_resolution_cases_state_check;

ALTER TABLE contract_resolution_cases
    ADD CONSTRAINT contract_resolution_cases_state_check
    CHECK (state IN (
        'awaiting_resolution',
        'acceptance_confirmed',
        'expired',
        'closed_outcome_unknown'
    ));
