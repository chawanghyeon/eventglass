-- +eventglass Up
ALTER TABLE maintenance_tasks
    ADD COLUMN retention_floor_us BIGINT;

UPDATE maintenance_tasks
SET retention_floor_us=(SELECT retention_floor_us FROM installations WHERE singleton)
WHERE kind='retain';

ALTER TABLE maintenance_tasks
    ADD CONSTRAINT maintenance_tasks_retention_floor_check CHECK (
        (kind='retain') = (retention_floor_us IS NOT NULL)
        AND (retention_floor_us IS NULL OR retention_floor_us >= 0)
    ),
    DROP CONSTRAINT maintenance_tasks_check3,
    ADD CONSTRAINT maintenance_tasks_output_required_check CHECK (
        state NOT IN ('prepared','completed')
        OR output_manifest IS NOT NULL
        OR (kind='retain' AND state='completed')
    );

CREATE TABLE ingest_batch_retirement_summaries (
    tenant_id BIGINT NOT NULL,
    lane_id INTEGER NOT NULL CHECK (lane_id >= 0 AND lane_id < 16),
    batch_seq BIGINT NOT NULL CHECK (batch_seq > 0),
    batch_id UUID NOT NULL UNIQUE,
    request_count INTEGER NOT NULL CHECK (request_count > 0),
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    accepted_count INTEGER NOT NULL CHECK (accepted_count >= 0),
    duplicate_count INTEGER NOT NULL CHECK (duplicate_count >= 0),
    conflict_count INTEGER NOT NULL CHECK (conflict_count >= 0),
    journal_sha256 CHAR(64) NOT NULL CHECK (journal_sha256 ~ '^[0-9a-f]{64}$'),
    receipt_set_sha256 CHAR(64) NOT NULL CHECK (receipt_set_sha256 ~ '^[0-9a-f]{64}$'),
    retired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,lane_id,batch_seq),
    FOREIGN KEY (tenant_id,lane_id,batch_seq) REFERENCES ingest_batches(tenant_id,lane_id,batch_seq)
);

ALTER TABLE object_intents
    ADD COLUMN gc_marked_at TIMESTAMPTZ,
    ADD COLUMN gc_confirmed_at TIMESTAMPTZ,
    ADD COLUMN gc_attempt INTEGER NOT NULL DEFAULT 0 CHECK (gc_attempt >= 0);

UPDATE object_intents
SET gc_marked_at=COALESCE(retired_at,updated_at,created_at),
    gc_confirmed_at=CASE
        WHEN state='deleted' THEN COALESCE(retired_at,updated_at,created_at)
        ELSE NULL
    END
WHERE state IN ('deleting','deleted');

ALTER TABLE object_intents
    ADD CONSTRAINT object_intents_gc_state_check CHECK (
        (state IN ('deleting','deleted')) = (gc_marked_at IS NOT NULL)
        AND (state='deleted') = (gc_confirmed_at IS NOT NULL)
    );
CREATE INDEX object_intents_gc_sweep_idx ON object_intents(state,gc_confirmed_at,gc_marked_at,intent_id);

-- +eventglass Down
DROP INDEX object_intents_gc_sweep_idx;
ALTER TABLE object_intents
    DROP CONSTRAINT object_intents_gc_state_check,
    DROP COLUMN gc_attempt,
    DROP COLUMN gc_confirmed_at,
    DROP COLUMN gc_marked_at;
DROP TABLE ingest_batch_retirement_summaries;
ALTER TABLE maintenance_tasks
    DROP CONSTRAINT maintenance_tasks_output_required_check,
    DROP CONSTRAINT maintenance_tasks_retention_floor_check,
    DROP COLUMN retention_floor_us,
    ADD CONSTRAINT maintenance_tasks_check3 CHECK (
        state NOT IN ('prepared','completed') OR output_manifest IS NOT NULL
    );
