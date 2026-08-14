CREATE TABLE contract_observed_embed_contexts (
    region_id BIGINT NOT NULL,
    contract_id BIGINT NOT NULL,
    context JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (region_id, contract_id)
);
