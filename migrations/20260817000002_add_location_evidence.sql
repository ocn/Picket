CREATE TABLE location_evidence (
    id BIGSERIAL PRIMARY KEY,
    location_id BIGINT NOT NULL CHECK (location_id > 0),
    evidence_class TEXT NOT NULL CHECK (evidence_class IN ('public_npc', 'access_qualified', 'operator')),
    structure_id BIGINT CHECK (structure_id IS NULL OR structure_id > 0),
    station_id BIGINT CHECK (station_id IS NULL OR station_id > 0),
    solar_system_id BIGINT NOT NULL CHECK (solar_system_id > 0),
    region_id BIGINT CHECK (region_id IS NULL OR region_id > 0),
    provenance TEXT NOT NULL CHECK (btrim(provenance) <> ''),
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    expired_at TIMESTAMPTZ,
    expired_by TEXT,
    superseded_at TIMESTAMPTZ,
    superseded_by_actor TEXT,
    supersedes_id BIGINT REFERENCES location_evidence(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expires_at IS NULL OR expires_at > observed_at),
    CHECK (evidence_class <> 'operator' OR expires_at IS NOT NULL),
    CHECK (station_id IS NULL OR station_id = location_id),
    CHECK (structure_id IS NULL OR structure_id = location_id),
    CHECK (station_id IS NULL OR structure_id IS NULL),
    CHECK (
        station_id IS NOT NULL
        OR structure_id IS NOT NULL
        OR location_id = solar_system_id
    ),
    CHECK (
        (expired_at IS NULL AND expired_by IS NULL)
        OR (expired_at IS NOT NULL AND expired_by IS NOT NULL AND btrim(expired_by) <> '')
    ),
    CHECK (
        (superseded_at IS NULL AND superseded_by_actor IS NULL)
        OR (
            superseded_at IS NOT NULL
            AND superseded_by_actor IS NOT NULL
            AND btrim(superseded_by_actor) <> ''
        )
    )
);

CREATE UNIQUE INDEX location_evidence_current_class_unique
    ON location_evidence (location_id, evidence_class)
    WHERE superseded_at IS NULL AND expired_at IS NULL;

CREATE UNIQUE INDEX location_evidence_one_replacement_per_predecessor
    ON location_evidence (supersedes_id)
    WHERE supersedes_id IS NOT NULL;

CREATE INDEX location_evidence_location_precedence_idx
    ON location_evidence (location_id, evidence_class, observed_at DESC, id DESC);

CREATE TABLE location_evidence_audit (
    id BIGSERIAL PRIMARY KEY,
    location_evidence_id BIGINT NOT NULL REFERENCES location_evidence(id),
    action TEXT NOT NULL CHECK (action IN ('added', 'expired', 'superseded')),
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    provenance TEXT NOT NULL CHECK (btrim(provenance) <> ''),
    occurred_at TIMESTAMPTZ NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX location_evidence_audit_evidence_idx
    ON location_evidence_audit (location_evidence_id, id);
