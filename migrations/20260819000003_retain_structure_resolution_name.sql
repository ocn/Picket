ALTER TABLE structure_resolution_state
    ADD COLUMN structure_name TEXT
    CHECK (structure_name IS NULL OR btrim(structure_name) <> '');
