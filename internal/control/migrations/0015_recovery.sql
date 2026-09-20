-- +eventglass Up
ALTER TABLE installations ADD COLUMN last_restore_at TIMESTAMPTZ;

CREATE TABLE recovery_verifications (
    verification_id UUID PRIMARY KEY,
    backup_id UUID NOT NULL REFERENCES backup_sets(backup_id),
    installation_id UUID NOT NULL,
    source_generation BIGINT NOT NULL CHECK (source_generation > 0),
    report_sha256 CHAR(64) NOT NULL CHECK (report_sha256 ~ '^[0-9a-f]{64}$'),
    inventory_sha256 CHAR(64) NOT NULL CHECK (inventory_sha256 ~ '^[0-9a-f]{64}$'),
    referenced_objects BIGINT NOT NULL CHECK (referenced_objects >= 0),
    referenced_bytes BIGINT NOT NULL CHECK (referenced_bytes >= 0),
    recovery_lsn TEXT NOT NULL CHECK (char_length(recovery_lsn) BETWEEN 3 AND 64),
    state TEXT NOT NULL CHECK (state IN ('verified','activated','failed')),
    failure_code TEXT CHECK (failure_code IS NULL OR char_length(failure_code) BETWEEN 1 AND 64),
    verified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    activated_at TIMESTAMPTZ,
    UNIQUE (backup_id,report_sha256),
    CHECK ((state='failed') = (failure_code IS NOT NULL)),
    CHECK ((state='activated') = (activated_at IS NOT NULL))
);
CREATE INDEX recovery_verifications_state_idx ON recovery_verifications(state,verified_at);

-- +eventglass Down
DROP TABLE recovery_verifications;
ALTER TABLE installations DROP COLUMN last_restore_at;
