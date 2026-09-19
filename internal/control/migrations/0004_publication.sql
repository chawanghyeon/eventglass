-- +eventglass Up
CREATE TABLE job_outputs (
    output_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    job_id UUID NOT NULL,
    prepare_fence BIGINT NOT NULL CHECK (prepare_fence > 0),
    manifest_version INTEGER NOT NULL CHECK (manifest_version > 0),
    header_json BYTEA NOT NULL CHECK (octet_length(header_json) BETWEEN 2 AND 65536),
    manifest_sha256 CHAR(64) NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    occurrence_sha256 CHAR(64) NOT NULL CHECK (occurrence_sha256 ~ '^[0-9a-f]{64}$'),
    selected_record_count INTEGER NOT NULL CHECK (selected_record_count >= 0),
    selected_error_count INTEGER NOT NULL CHECK (selected_error_count BETWEEN 0 AND selected_record_count),
    state TEXT NOT NULL CHECK (state IN ('prepared', 'published', 'discarded')),
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id, prepare_fence),
    UNIQUE (tenant_id, output_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs(tenant_id, job_id)
);

ALTER TABLE jobs
    ADD COLUMN prepared_output_id UUID,
    ADD CONSTRAINT jobs_prepared_output_fk FOREIGN KEY (tenant_id, prepared_output_id) REFERENCES job_outputs(tenant_id, output_id);

CREATE TABLE job_output_parts (
    output_id UUID NOT NULL REFERENCES job_outputs(output_id) ON DELETE CASCADE,
    part_index INTEGER NOT NULL CHECK (part_index >= 0),
    metadata_json BYTEA NOT NULL CHECK (octet_length(metadata_json) BETWEEN 2 AND 65536),
    metadata_sha256 CHAR(64) NOT NULL CHECK (metadata_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (output_id, part_index)
);

CREATE TABLE job_output_occurrences (
    output_id UUID NOT NULL,
    record_id CHAR(64) NOT NULL CHECK (record_id ~ '^[0-9a-f]{64}$'),
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    acceptance_id UUID NOT NULL,
    lane_id INTEGER NOT NULL CHECK (lane_id >= 0 AND lane_id < 16),
    batch_seq BIGINT NOT NULL CHECK (batch_seq > 0),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    event_time_us BIGINT NOT NULL,
    event_time_ns INTEGER NOT NULL CHECK (event_time_ns BETWEEN 0 AND 999),
    received_time_us BIGINT NOT NULL,
    release_json TEXT CHECK (release_json IS NULL OR (octet_length(release_json) BETWEEN 2 AND 1048576 AND jsonb_typeof(release_json::jsonb) = 'string')),
    issue_id CHAR(64) NOT NULL CHECK (issue_id ~ '^[0-9a-f]{64}$'),
    grouping_version INTEGER NOT NULL CHECK (grouping_version > 0),
    fingerprint_sha256 CHAR(64) NOT NULL CHECK (fingerprint_sha256 ~ '^[0-9a-f]{64}$'),
    title_json TEXT NOT NULL CHECK (octet_length(title_json) BETWEEN 2 AND 6146 AND jsonb_typeof(title_json::jsonb) = 'string' AND char_length(title_json::jsonb #>> '{}') <= 512),
    PRIMARY KEY (output_id, record_id),
    CHECK (issue_id = fingerprint_sha256),
    FOREIGN KEY (tenant_id, output_id) REFERENCES job_outputs(tenant_id, output_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    FOREIGN KEY (tenant_id, project_id, acceptance_id) REFERENCES receipts(tenant_id, project_id, acceptance_id),
    FOREIGN KEY (tenant_id, lane_id, batch_seq) REFERENCES ingest_batches(tenant_id, lane_id, batch_seq)
);
CREATE INDEX job_output_occurrences_scope_idx ON job_output_occurrences(tenant_id, project_id, issue_id, record_id);

CREATE TABLE bundles (
    bundle_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    lane_id INTEGER NOT NULL CHECK (lane_id >= 0 AND lane_id < 16),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    grouping_version INTEGER NOT NULL CHECK (grouping_version > 0),
    event_day DATE NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('error', 'log', 'transaction')),
    input_seq_min BIGINT NOT NULL CHECK (input_seq_min > 0),
    input_seq_max BIGINT NOT NULL CHECK (input_seq_max >= input_seq_min),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    identity_sha256 CHAR(64) NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
    valid_from_generation BIGINT NOT NULL CHECK (valid_from_generation > 0),
    valid_to_generation BIGINT CHECK (valid_to_generation > valid_from_generation),
    retired_at TIMESTAMPTZ,
    UNIQUE (tenant_id, bundle_id),
    FOREIGN KEY (tenant_id, lane_id) REFERENCES lanes(tenant_id, lane_id),
    CHECK ((valid_to_generation IS NULL) = (retired_at IS NULL))
);
CREATE INDEX bundles_catalog_idx ON bundles(tenant_id, lane_id, valid_from_generation, valid_to_generation);

CREATE TABLE files (
    file_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bundle_id UUID NOT NULL,
    intent_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('analytics', 'payload')),
    bytes BIGINT NOT NULL CHECK (bytes > 0),
    full_sha256 CHAR(64) NOT NULL CHECK (full_sha256 ~ '^[0-9a-f]{64}$'),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    min_event_time_us BIGINT NOT NULL,
    max_event_time_us BIGINT NOT NULL CHECK (max_event_time_us >= min_event_time_us),
    min_received_time_us BIGINT NOT NULL,
    max_received_time_us BIGINT NOT NULL CHECK (max_received_time_us >= min_received_time_us),
    min_batch_seq BIGINT NOT NULL CHECK (min_batch_seq > 0),
    max_batch_seq BIGINT NOT NULL CHECK (max_batch_seq >= min_batch_seq),
    UNIQUE (tenant_id, file_id),
    UNIQUE (tenant_id, intent_id),
    UNIQUE (bundle_id, role),
    FOREIGN KEY (tenant_id, bundle_id) REFERENCES bundles(tenant_id, bundle_id),
    FOREIGN KEY (tenant_id, intent_id) REFERENCES object_intents(tenant_id, intent_id)
);

CREATE TABLE file_blocks (
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    sha256 CHAR(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (file_id, block_index)
);

CREATE TABLE bundle_projects (
    tenant_id BIGINT NOT NULL,
    bundle_id UUID NOT NULL,
    project_id BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, bundle_id, project_id),
    FOREIGN KEY (tenant_id, bundle_id) REFERENCES bundles(tenant_id, bundle_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id)
);
CREATE INDEX bundle_projects_project_idx ON bundle_projects(tenant_id, project_id, bundle_id);

CREATE TABLE issues (
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    issue_id CHAR(64) NOT NULL CHECK (issue_id ~ '^[0-9a-f]{64}$'),
    grouping_version INTEGER NOT NULL CHECK (grouping_version > 0),
    fingerprint_sha256 CHAR(64) NOT NULL CHECK (fingerprint_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('unresolved', 'resolved', 'ignored')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    occurrence_count BIGINT NOT NULL CHECK (occurrence_count > 0),
    first_event_time_us BIGINT NOT NULL,
    first_event_time_ns INTEGER NOT NULL CHECK (first_event_time_ns BETWEEN 0 AND 999),
    first_record_id CHAR(64) NOT NULL CHECK (first_record_id ~ '^[0-9a-f]{64}$'),
    first_release_json TEXT CHECK (first_release_json IS NULL OR (octet_length(first_release_json) BETWEEN 2 AND 1048576 AND jsonb_typeof(first_release_json::jsonb) = 'string')),
    last_event_time_us BIGINT NOT NULL,
    last_event_time_ns INTEGER NOT NULL CHECK (last_event_time_ns BETWEEN 0 AND 999),
    last_record_id CHAR(64) NOT NULL CHECK (last_record_id ~ '^[0-9a-f]{64}$'),
    last_release_json TEXT CHECK (last_release_json IS NULL OR (octet_length(last_release_json) BETWEEN 2 AND 1048576 AND jsonb_typeof(last_release_json::jsonb) = 'string')),
    last_received_time_us BIGINT NOT NULL,
    title_json TEXT NOT NULL CHECK (octet_length(title_json) BETWEEN 2 AND 6146 AND jsonb_typeof(title_json::jsonb) = 'string' AND char_length(title_json::jsonb #>> '{}') <= 512),
    resolved_cut JSONB,
    PRIMARY KEY (tenant_id, project_id, issue_id),
    UNIQUE (issue_id),
    UNIQUE (project_id, grouping_version, fingerprint_sha256),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    CHECK (issue_id = fingerprint_sha256),
    CHECK ((status = 'resolved') = (resolved_cut IS NOT NULL)),
    CHECK (resolved_cut IS NULL OR (jsonb_typeof(resolved_cut) = 'array' AND jsonb_array_length(resolved_cut) = 16))
);
CREATE INDEX issues_list_idx ON issues(tenant_id, project_id, status, last_received_time_us DESC, issue_id DESC);

CREATE TABLE issue_occurrences (
    record_id CHAR(64) PRIMARY KEY CHECK (record_id ~ '^[0-9a-f]{64}$'),
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    issue_id CHAR(64) NOT NULL,
    acceptance_id UUID NOT NULL,
    lane_id INTEGER NOT NULL CHECK (lane_id >= 0 AND lane_id < 16),
    batch_seq BIGINT NOT NULL CHECK (batch_seq > 0),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    event_time_us BIGINT NOT NULL,
    event_time_ns INTEGER NOT NULL CHECK (event_time_ns BETWEEN 0 AND 999),
    received_time_us BIGINT NOT NULL,
    release_json TEXT CHECK (release_json IS NULL OR (octet_length(release_json) BETWEEN 2 AND 1048576 AND jsonb_typeof(release_json::jsonb) = 'string')),
    UNIQUE (tenant_id, project_id, issue_id, record_id),
    FOREIGN KEY (tenant_id, project_id, issue_id) REFERENCES issues(tenant_id, project_id, issue_id),
    FOREIGN KEY (tenant_id, project_id, acceptance_id) REFERENCES receipts(tenant_id, project_id, acceptance_id),
    FOREIGN KEY (tenant_id, lane_id, batch_seq) REFERENCES ingest_batches(tenant_id, lane_id, batch_seq)
);
CREATE INDEX issue_occurrences_issue_idx ON issue_occurrences(tenant_id, project_id, issue_id, event_time_us DESC, event_time_ns DESC, record_id DESC);
CREATE INDEX issue_occurrences_received_idx ON issue_occurrences(tenant_id, received_time_us, record_id);

CREATE TABLE issue_transitions (
    transition_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    issue_id CHAR(64) NOT NULL,
    issue_revision BIGINT NOT NULL CHECK (issue_revision > 0),
    type TEXT NOT NULL CHECK (type IN ('created', 'regressed', 'resolved', 'ignored', 'reopened')),
    received_time_us BIGINT NOT NULL,
    record_id CHAR(64),
    actor_user_id BIGINT,
    UNIQUE (issue_id, issue_revision),
    FOREIGN KEY (tenant_id, project_id, issue_id) REFERENCES issues(tenant_id, project_id, issue_id),
    FOREIGN KEY (tenant_id, project_id, issue_id, record_id) REFERENCES issue_occurrences(tenant_id, project_id, issue_id, record_id)
);

-- +eventglass Down
DROP TABLE issue_transitions;
DROP TABLE issue_occurrences;
DROP TABLE issues;
DROP TABLE bundle_projects;
DROP TABLE file_blocks;
DROP TABLE files;
DROP TABLE bundles;
DROP TABLE job_output_occurrences;
DROP TABLE job_output_parts;
ALTER TABLE jobs DROP CONSTRAINT jobs_prepared_output_fk, DROP COLUMN prepared_output_id;
DROP TABLE job_outputs;
