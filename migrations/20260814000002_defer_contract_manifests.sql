CREATE TABLE contract_manifest_pending (
    region_id BIGINT NOT NULL,
    contract_id BIGINT NOT NULL,
    fact_hash TEXT NOT NULL REFERENCES public_contract_facts(fact_hash),
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    notify_listed_on_resolution BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (region_id, contract_id)
);

CREATE INDEX contract_manifest_pending_last_observed_idx
    ON contract_manifest_pending (region_id, last_observed_at);
