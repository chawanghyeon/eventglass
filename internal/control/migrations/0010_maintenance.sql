-- +eventglass Up
CREATE TABLE maintenance_tasks (
    task_id UUID PRIMARY KEY,
    tenant_id BIGINT REFERENCES tenants(tenant_id),
    lane_id INTEGER CHECK (lane_id >= 0 AND lane_id < 16),
    kind TEXT NOT NULL CHECK (kind IN ('compact','retain','gc','backup_verify')),
    input_identity CHAR(64) NOT NULL CHECK (input_identity ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL CHECK (state IN ('queued','running','prepared','completed','failed')),
    storage_generation BIGINT NOT NULL CHECK (storage_generation > 0),
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    owner TEXT,
    lease_until TIMESTAMPTZ,
    retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    output_manifest BYTEA CHECK (output_manifest IS NULL OR octet_length(output_manifest) BETWEEN 2 AND 1048576),
    output_sha256 CHAR(64) CHECK (output_sha256 IS NULL OR output_sha256 ~ '^[0-9a-f]{64}$'),
    last_error_code TEXT CHECK (last_error_code IS NULL OR char_length(last_error_code) BETWEEN 1 AND 64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (task_id,tenant_id),
    UNIQUE (tenant_id, kind, input_identity),
    CHECK ((tenant_id IS NULL) = (lane_id IS NULL)),
    CHECK ((state='running') = (owner IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK ((output_manifest IS NULL) = (output_sha256 IS NULL)),
    CHECK (state NOT IN ('prepared','completed') OR output_manifest IS NOT NULL),
    CHECK (output_manifest IS NULL OR state IN ('running','prepared','completed'))
);
CREATE INDEX maintenance_tasks_claim_idx ON maintenance_tasks(kind,state,retry_at,lease_until,created_at);
CREATE UNIQUE INDEX maintenance_tasks_active_lane_idx ON maintenance_tasks(tenant_id,lane_id)
    WHERE state IN ('queued','running','prepared');

ALTER TABLE bundles ADD COLUMN reserved_by UUID REFERENCES maintenance_tasks(task_id);
CREATE UNIQUE INDEX bundles_active_reservation_idx ON bundles(bundle_id) WHERE reserved_by IS NOT NULL;

CREATE TABLE maintenance_inputs (
    task_id UUID NOT NULL,
    tenant_id BIGINT NOT NULL,
    bundle_id UUID NOT NULL,
    expected_valid_from_generation BIGINT NOT NULL CHECK (expected_valid_from_generation > 0),
    expected_identity_sha256 CHAR(64) NOT NULL CHECK (expected_identity_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (task_id,bundle_id),
    FOREIGN KEY (task_id,tenant_id) REFERENCES maintenance_tasks(task_id,tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id,bundle_id) REFERENCES bundles(tenant_id,bundle_id)
);

ALTER TABLE object_intents DROP CONSTRAINT object_intents_producer_family_check;
ALTER TABLE object_intents DROP CONSTRAINT object_intents_producer_authority_check;
ALTER TABLE object_intents
    ADD COLUMN maintenance_task_id UUID REFERENCES maintenance_tasks(task_id),
    ADD CONSTRAINT object_intents_producer_family_check CHECK (
        (conversion_job_id IS NOT NULL)::integer + (query_id IS NOT NULL)::integer + (maintenance_task_id IS NOT NULL)::integer <= 1
    ),
    ADD CONSTRAINT object_intents_producer_authority_check CHECK (
        (conversion_job_id IS NOT NULL OR query_id IS NOT NULL OR maintenance_task_id IS NOT NULL) = (producer_generation IS NOT NULL)
    );
CREATE INDEX object_intents_maintenance_producer_idx ON object_intents(tenant_id,maintenance_task_id)
    WHERE maintenance_task_id IS NOT NULL;

ALTER TABLE ingest_batches
    ADD COLUMN recovery_state TEXT NOT NULL DEFAULT 'live' CHECK (recovery_state IN ('live','retired')),
    ADD COLUMN journal_retired_at TIMESTAMPTZ,
    ALTER COLUMN journal_intent_id DROP NOT NULL,
    DROP CONSTRAINT ingest_batches_tenant_id_journal_intent_id_fkey,
    ADD CONSTRAINT ingest_batches_journal_intent_fk FOREIGN KEY (tenant_id,journal_intent_id) REFERENCES object_intents(tenant_id,intent_id),
    ADD CONSTRAINT ingest_batches_recovery_consistency_check CHECK (
        (recovery_state='live' AND journal_intent_id IS NOT NULL AND journal_retired_at IS NULL)
        OR (recovery_state='retired' AND state='published' AND journal_intent_id IS NULL AND journal_retired_at IS NOT NULL)
    );

CREATE TABLE backup_sets (
    backup_id UUID PRIMARY KEY,
    external_tool_id TEXT NOT NULL UNIQUE CHECK (char_length(external_tool_id) BETWEEN 1 AND 256),
    installation_id UUID NOT NULL,
    storage_generation BIGINT NOT NULL CHECK (storage_generation > 0),
    base_start TIMESTAMPTZ NOT NULL,
    base_end TIMESTAMPTZ NOT NULL CHECK (base_end >= base_start),
    earliest_recoverable_time TIMESTAMPTZ NOT NULL,
    latest_recoverable_time TIMESTAMPTZ NOT NULL CHECK (latest_recoverable_time >= earliest_recoverable_time),
    state TEXT NOT NULL CHECK (state IN ('pending','verified','expired','failed')),
    protected_until TIMESTAMPTZ NOT NULL,
    verified_at TIMESTAMPTZ,
    inventory_object_key TEXT,
    inventory_sha256 CHAR(64) CHECK (inventory_sha256 IS NULL OR inventory_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((inventory_object_key IS NULL) = (inventory_sha256 IS NULL)),
    CHECK ((state IN ('verified','expired')) = (verified_at IS NOT NULL))
);

-- +eventglass Down
DROP TABLE backup_sets;
ALTER TABLE ingest_batches
    DROP CONSTRAINT ingest_batches_recovery_consistency_check,
    DROP CONSTRAINT ingest_batches_journal_intent_fk,
    ADD CONSTRAINT ingest_batches_tenant_id_journal_intent_id_fkey FOREIGN KEY (tenant_id,journal_intent_id) REFERENCES object_intents(tenant_id,intent_id),
    ALTER COLUMN journal_intent_id SET NOT NULL,
    DROP COLUMN journal_retired_at,
    DROP COLUMN recovery_state;
DROP INDEX object_intents_maintenance_producer_idx;
ALTER TABLE object_intents
    DROP CONSTRAINT object_intents_producer_authority_check,
    DROP CONSTRAINT object_intents_producer_family_check,
    DROP COLUMN maintenance_task_id,
    ADD CONSTRAINT object_intents_producer_family_check CHECK ((conversion_job_id IS NOT NULL)::integer + (query_id IS NOT NULL)::integer <= 1),
    ADD CONSTRAINT object_intents_producer_authority_check CHECK ((conversion_job_id IS NOT NULL OR query_id IS NOT NULL) = (producer_generation IS NOT NULL));
DROP TABLE maintenance_inputs;
DROP INDEX bundles_active_reservation_idx;
ALTER TABLE bundles DROP COLUMN reserved_by;
DROP TABLE maintenance_tasks;
