CREATE TABLE IF NOT EXISTS regional_collection_metadata (
    region_id BIGINT PRIMARY KEY,
    baseline_at TIMESTAMPTZ,
    complete_observations BIGINT NOT NULL DEFAULT 0,
    last_complete_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS esi_cache_metadata (
    resource_key TEXT PRIMARY KEY,
    etag TEXT,
    expires_at TIMESTAMPTZ,
    expected_pages INTEGER,
    error_limit_remain BIGINT,
    error_limit_reset BIGINT,
    retry_after TIMESTAMPTZ,
    response JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public_contract_facts (
    fact_hash TEXT PRIMARY KEY,
    contract_id BIGINT NOT NULL,
    facts JSONB NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contract_item_manifests (
    manifest_hash TEXT PRIMARY KEY,
    manifest JSONB NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS presence_intervals (
    region_id BIGINT NOT NULL,
    contract_id BIGINT NOT NULL,
    fact_hash TEXT NOT NULL REFERENCES public_contract_facts(fact_hash),
    manifest_hash TEXT NOT NULL REFERENCES contract_item_manifests(manifest_hash),
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (region_id, contract_id, fact_hash, manifest_hash)
);

CREATE TABLE IF NOT EXISTS regional_observations (
    id BIGSERIAL PRIMARY KEY,
    region_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('complete', 'inconclusive')),
    expected_pages INTEGER,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS contract_lifecycle_evidence (
    id BIGSERIAL PRIMARY KEY,
    region_id BIGINT NOT NULL,
    contract_id BIGINT NOT NULL,
    evidence_kind TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS contract_collection_failures (
    id BIGSERIAL PRIMARY KEY,
    region_id BIGINT,
    observed_at TIMESTAMPTZ NOT NULL,
    failure_kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    retry_after TIMESTAMPTZ
);
