-- +eventglass Up
CREATE TABLE tenants (
    tenant_id BIGINT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE projects (
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    project_id BIGINT NOT NULL UNIQUE CHECK (project_id > 0),
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'disabled')),
    auth_revision BIGINT NOT NULL DEFAULT 1 CHECK (auth_revision > 0),
    scrub_revision INTEGER NOT NULL CHECK (scrub_revision > 0),
    default_service TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, project_id)
);

CREATE TABLE project_keys (
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    key_hash BYTEA NOT NULL CHECK (octet_length(key_hash) = 32),
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'revoked')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id, key_hash),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id)
);

CREATE TABLE lanes (
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    lane_id INTEGER NOT NULL CHECK (lane_id >= 0 AND lane_id < 16),
    accepted_seq BIGINT NOT NULL DEFAULT 0 CHECK (accepted_seq >= 0),
    published_seq BIGINT NOT NULL DEFAULT 0 CHECK (published_seq >= 0 AND published_seq <= accepted_seq),
    catalog_generation BIGINT NOT NULL DEFAULT 0 CHECK (catalog_generation >= 0),
    last_received_time_us BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, lane_id)
);

CREATE TABLE object_intents (
    intent_id UUID PRIMARY KEY,
    installation_id UUID NOT NULL,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    storage_generation BIGINT NOT NULL CHECK (storage_generation > 0),
    object_key TEXT NOT NULL UNIQUE CHECK (object_key <> '' AND left(object_key, 1) <> '/' AND position('..' in object_key) = 0 AND position(chr(92) in object_key) = 0),
    kind TEXT NOT NULL CHECK (kind IN ('journal', 'analytics', 'payload', 'temporary')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'uploaded', 'referenced', 'deleting', 'deleted')),
    owner TEXT NOT NULL,
    fence BIGINT NOT NULL DEFAULT 1 CHECK (fence > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    expected_bytes BIGINT NOT NULL CHECK (expected_bytes >= 0),
    expected_sha256 CHAR(64) NOT NULL CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
    uploaded_bytes BIGINT,
    uploaded_sha256 CHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((uploaded_bytes IS NULL) = (uploaded_sha256 IS NULL)),
    UNIQUE (tenant_id, intent_id)
);
CREATE INDEX object_intents_gc_idx ON object_intents(state, expires_at, intent_id);

CREATE TABLE ingest_batches (
    tenant_id BIGINT NOT NULL,
    lane_id INTEGER NOT NULL,
    batch_seq BIGINT NOT NULL CHECK (batch_seq > 0),
    batch_id UUID NOT NULL UNIQUE,
    journal_intent_id UUID NOT NULL UNIQUE,
    request_count INTEGER NOT NULL CHECK (request_count > 0 AND request_count <= 1000),
    received_time_us BIGINT NOT NULL,
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    accepted_count INTEGER NOT NULL CHECK (accepted_count >= 0 AND accepted_count <= record_count),
    duplicate_count INTEGER NOT NULL CHECK (duplicate_count >= 0),
    conflict_count INTEGER NOT NULL CHECK (conflict_count >= 0),
    journal_sha256 CHAR(64) NOT NULL CHECK (journal_sha256 ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'accepted' CHECK (state IN ('accepted', 'published', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, lane_id, batch_seq),
    FOREIGN KEY (tenant_id, lane_id) REFERENCES lanes(tenant_id, lane_id),
    FOREIGN KEY (tenant_id, journal_intent_id) REFERENCES object_intents(tenant_id, intent_id),
    CHECK (accepted_count + duplicate_count + conflict_count = record_count)
);

CREATE TABLE receipts (
    acceptance_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    lane_id INTEGER NOT NULL,
    batch_seq BIGINT NOT NULL,
    content_sha256 CHAR(64) NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    request_index INTEGER NOT NULL CHECK (request_index >= 0),
    selection_json JSONB NOT NULL,
    selection_sha256 CHAR(64) NOT NULL CHECK (selection_sha256 ~ '^[0-9a-f]{64}$'),
    ordinal_first INTEGER NOT NULL,
    ordinal_last INTEGER NOT NULL,
    accepted_count INTEGER NOT NULL CHECK (accepted_count >= 0),
    duplicate_count INTEGER NOT NULL CHECK (duplicate_count >= 0),
    conflict_count INTEGER NOT NULL CHECK (conflict_count >= 0),
    unsupported_count INTEGER NOT NULL CHECK (unsupported_count >= 0),
    received_time_us BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, project_id, acceptance_id),
    UNIQUE (tenant_id, lane_id, batch_seq, request_index),
    FOREIGN KEY (tenant_id, lane_id, batch_seq) REFERENCES ingest_batches(tenant_id, lane_id, batch_seq),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    CHECK (ordinal_first >= 0 AND ordinal_last >= ordinal_first - 1),
    CHECK (accepted_count + duplicate_count + conflict_count = ordinal_last - ordinal_first + 1)
);

CREATE TABLE event_dedupe (
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('error', 'transaction')),
    source_event_id TEXT NOT NULL,
    record_id CHAR(64) NOT NULL,
    receipt_acceptance_id UUID NOT NULL,
    payload_sha256 CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id, kind, source_event_id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    FOREIGN KEY (tenant_id, project_id, receipt_acceptance_id)
        REFERENCES receipts(tenant_id, project_id, acceptance_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX event_dedupe_expiry_idx ON event_dedupe(expires_at);

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('convert')),
    tenant_id BIGINT NOT NULL,
    lane_id INTEGER NOT NULL,
    batch_seq BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'failed')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    storage_generation BIGINT NOT NULL CHECK (storage_generation > 0),
    owner TEXT,
    lease_until TIMESTAMPTZ,
    retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (kind, tenant_id, lane_id, batch_seq),
    FOREIGN KEY (tenant_id, lane_id, batch_seq) REFERENCES ingest_batches(tenant_id, lane_id, batch_seq),
    CHECK ((state = 'running') = (owner IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX jobs_claim_idx ON jobs(kind, state, retry_at, lease_until, created_at);

CREATE TABLE sdk_outcomes (
    acceptance_id UUID NOT NULL REFERENCES receipts(acceptance_id) ON DELETE CASCADE,
    item_ordinal INTEGER NOT NULL,
    category TEXT NOT NULL,
    reason TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    approximate BOOLEAN NOT NULL,
    PRIMARY KEY (acceptance_id, item_ordinal, category, reason)
);

-- +eventglass Down
DROP TABLE sdk_outcomes;
DROP TABLE jobs;
DROP TABLE event_dedupe;
DROP TABLE receipts;
DROP TABLE ingest_batches;
DROP TABLE object_intents;
DROP TABLE lanes;
DROP TABLE project_keys;
DROP TABLE projects;
DROP TABLE tenants;
