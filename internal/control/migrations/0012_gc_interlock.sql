-- +eventglass Up
-- Only the coordinated backup verifier may refresh this attestation. No
-- startup default, environment override or empty backup inventory authorizes GC.
ALTER TABLE installations
    ADD COLUMN dedupe_retention_days INTEGER NOT NULL DEFAULT 30 CHECK (dedupe_retention_days BETWEEN 1 AND 3650),
    ADD COLUMN gc_safe_before TIMESTAMPTZ,
    ADD COLUMN gc_verified_until TIMESTAMPTZ,
    ADD CONSTRAINT installations_gc_attestation_check CHECK (
        (gc_safe_before IS NULL) = (gc_verified_until IS NULL)
    );

-- Preserve existing promises while making installation policy authoritative.
UPDATE installations SET dedupe_retention_days=GREATEST(retention_days,
    COALESCE((SELECT max(retention_days) FROM projects),30));

-- +eventglass Down
ALTER TABLE installations
    DROP CONSTRAINT installations_gc_attestation_check,
    DROP COLUMN gc_safe_before,
    DROP COLUMN gc_verified_until,
    DROP COLUMN dedupe_retention_days;
