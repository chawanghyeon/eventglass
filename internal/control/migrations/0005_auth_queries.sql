-- +eventglass Up
ALTER TABLE installations
    ADD COLUMN setup_state TEXT NOT NULL DEFAULT 'ready' CHECK (setup_state IN ('uninitialized', 'provisioning', 'ready')),
    ADD COLUMN setup_attempt UUID,
    ADD COLUMN setup_request_fingerprint BYTEA CHECK (setup_request_fingerprint IS NULL OR octet_length(setup_request_fingerprint) = 32),
    ADD COLUMN setup_marker_key TEXT,
    ADD COLUMN setup_marker_sha256 CHAR(64) CHECK (setup_marker_sha256 IS NULL OR setup_marker_sha256 ~ '^[0-9a-f]{64}$'),
    ADD COLUMN setup_owner TEXT,
    ADD COLUMN setup_fence BIGINT NOT NULL DEFAULT 0 CHECK (setup_fence >= 0),
    ADD COLUMN setup_lease_until TIMESTAMPTZ,
    ADD COLUMN bootstrap_token_hash BYTEA CHECK (bootstrap_token_hash IS NULL OR octet_length(bootstrap_token_hash) = 32),
    ADD COLUMN setup_completed_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    ADD COLUMN retention_floor_us BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN retention_tick_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN recovery_state TEXT NOT NULL DEFAULT 'ready' CHECK (recovery_state IN ('ready', 'restoring', 'verification_required')),
    ADD COLUMN alerts_paused BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE installations SET setup_completed_at = created_at WHERE setup_state = 'ready';
ALTER TABLE installations ADD CONSTRAINT installations_setup_authority_check CHECK (
        (setup_state = 'ready' AND setup_attempt IS NULL AND setup_owner IS NULL AND setup_lease_until IS NULL AND bootstrap_token_hash IS NULL AND setup_completed_at IS NOT NULL)
        OR (setup_state = 'uninitialized' AND setup_attempt IS NULL AND setup_request_fingerprint IS NULL AND setup_marker_key IS NULL AND setup_marker_sha256 IS NULL AND setup_owner IS NULL AND setup_lease_until IS NULL AND bootstrap_token_hash IS NOT NULL AND setup_completed_at IS NULL)
        OR (setup_state = 'provisioning' AND setup_attempt IS NOT NULL AND setup_request_fingerprint IS NOT NULL AND setup_marker_key IS NOT NULL AND setup_marker_sha256 IS NOT NULL AND setup_owner IS NOT NULL AND setup_lease_until IS NOT NULL AND bootstrap_token_hash IS NOT NULL AND setup_completed_at IS NULL)
    );

CREATE SEQUENCE eventglass_tenant_id_seq AS BIGINT;
SELECT setval('eventglass_tenant_id_seq', COALESCE((SELECT max(tenant_id) FROM tenants), 0) + 1, false);
ALTER TABLE tenants ALTER COLUMN tenant_id SET DEFAULT nextval('eventglass_tenant_id_seq');
CREATE SEQUENCE eventglass_project_id_seq AS BIGINT;
SELECT setval('eventglass_project_id_seq', COALESCE((SELECT max(project_id) FROM projects), 0) + 1, false);
ALTER TABLE projects ALTER COLUMN project_id SET DEFAULT nextval('eventglass_project_id_seq');
CREATE SEQUENCE eventglass_user_id_seq AS BIGINT;

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY DEFAULT nextval('eventglass_user_id_seq') CHECK (user_id > 0),
    email_normalized TEXT NOT NULL UNIQUE CHECK (char_length(email_normalized) BETWEEN 3 AND 320 AND email_normalized = lower(btrim(email_normalized))),
    password_phc TEXT NOT NULL CHECK (octet_length(password_phc) BETWEEN 32 AND 1024),
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'disabled')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    auth_revision BIGINT NOT NULL DEFAULT 1 CHECK (auth_revision > 0),
    credential_revision BIGINT NOT NULL DEFAULT 1 CHECK (credential_revision > 0),
    is_installation_admin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE memberships (
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    role TEXT NOT NULL CHECK (role IN ('admin', 'member')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id)
);
CREATE INDEX memberships_user_idx ON memberships(user_id, tenant_id);

CREATE TABLE project_grants (
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('operator', 'viewer')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id, user_id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    FOREIGN KEY (tenant_id, user_id) REFERENCES memberships(tenant_id, user_id) ON DELETE CASCADE
);
CREATE INDEX project_grants_user_idx ON project_grants(tenant_id, user_id, project_id);

CREATE TABLE sessions (
    token_hash BYTEA PRIMARY KEY CHECK (octet_length(token_hash) = 32),
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    csrf_hash BYTEA NOT NULL CHECK (octet_length(csrf_hash) = 32),
    credential_revision BIGINT NOT NULL CHECK (credential_revision > 0),
    storage_generation BIGINT NOT NULL CHECK (storage_generation > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    CHECK (expires_at > created_at)
);
CREATE INDEX sessions_user_idx ON sessions(user_id, expires_at);
CREATE INDEX sessions_expiry_idx ON sessions(expires_at) WHERE revoked_at IS NULL;

CREATE TABLE login_limits (
    bucket_hash BYTEA NOT NULL CHECK (octet_length(bucket_hash) = 32),
    window_start TIMESTAMPTZ NOT NULL,
    count INTEGER NOT NULL CHECK (count > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bucket_hash, window_start),
    CHECK (expires_at > window_start AND expires_at <= window_start + interval '1 hour')
);
CREATE INDEX login_limits_expiry_idx ON login_limits(expires_at);

CREATE TABLE audit_events (
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    audit_id UUID NOT NULL,
    actor_user_id BIGINT REFERENCES users(user_id),
    action TEXT NOT NULL CHECK (action IN ('setup', 'login', 'logout', 'password_changed', 'project_created', 'project_updated', 'key_created', 'key_revoked', 'user_created', 'membership_updated', 'issue_status_changed')),
    target_type TEXT NOT NULL CHECK (char_length(target_type) BETWEEN 1 AND 64),
    target_id TEXT NOT NULL CHECK (char_length(target_id) BETWEEN 1 AND 128),
    target_revision BIGINT CHECK (target_revision IS NULL OR target_revision > 0),
    request_id UUID NOT NULL,
    operation_id UUID,
    operation_sha256 CHAR(64) CHECK (operation_sha256 IS NULL OR operation_sha256 ~ '^[0-9a-f]{64}$'),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, audit_id)
);
CREATE UNIQUE INDEX audit_events_operation_idx ON audit_events(tenant_id, actor_user_id, operation_id) WHERE operation_id IS NOT NULL;
CREATE INDEX audit_events_list_idx ON audit_events(tenant_id, occurred_at DESC, audit_id DESC);

ALTER TABLE issue_transitions
    ADD COLUMN operation_id UUID,
    ADD CONSTRAINT issue_transitions_actor_fk FOREIGN KEY (actor_user_id) REFERENCES users(user_id);
CREATE UNIQUE INDEX issue_transitions_operation_idx ON issue_transitions(tenant_id, actor_user_id, operation_id) WHERE operation_id IS NOT NULL;

CREATE TABLE query_snapshots (
    snapshot_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    user_id BIGINT REFERENCES users(user_id),
    principal_kind TEXT NOT NULL CHECK (principal_kind IN ('user', 'alert')),
    principal_ref TEXT NOT NULL CHECK (char_length(principal_ref) BETWEEN 1 AND 128),
    auth_revision BIGINT NOT NULL CHECK (auth_revision > 0),
    storage_generation BIGINT NOT NULL CHECK (storage_generation > 0),
    dataset_hash CHAR(64) NOT NULL CHECK (dataset_hash ~ '^[0-9a-f]{64}$'),
    dataset_bytes BYTEA NOT NULL CHECK (octet_length(dataset_bytes) BETWEEN 2 AND 32768),
    retention_floor_us BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    max_until TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'released')),
    UNIQUE (tenant_id, snapshot_id),
    CHECK ((principal_kind = 'user') = (user_id IS NOT NULL)),
    CHECK (expires_at > created_at AND max_until >= expires_at)
);
CREATE INDEX query_snapshots_owner_idx ON query_snapshots(tenant_id, user_id, state, expires_at);

CREATE TABLE snapshot_projects (
    snapshot_id UUID NOT NULL REFERENCES query_snapshots(snapshot_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, project_id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    FOREIGN KEY (tenant_id, snapshot_id) REFERENCES query_snapshots(tenant_id, snapshot_id)
);

CREATE TABLE snapshot_lanes (
    snapshot_id UUID NOT NULL REFERENCES query_snapshots(snapshot_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL,
    lane_id INTEGER NOT NULL CHECK (lane_id >= 0 AND lane_id < 16),
    cut_seq BIGINT NOT NULL CHECK (cut_seq >= 0),
    catalog_generation BIGINT NOT NULL CHECK (catalog_generation >= 0),
    PRIMARY KEY (snapshot_id, tenant_id, lane_id),
    FOREIGN KEY (tenant_id, lane_id) REFERENCES lanes(tenant_id, lane_id),
    FOREIGN KEY (tenant_id, snapshot_id) REFERENCES query_snapshots(tenant_id, snapshot_id)
);
CREATE INDEX snapshot_lanes_generation_idx ON snapshot_lanes(tenant_id, lane_id, catalog_generation);

CREATE TABLE query_jobs (
    query_id UUID PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    user_id BIGINT REFERENCES users(user_id),
    principal_ref TEXT NOT NULL CHECK (char_length(principal_ref) BETWEEN 1 AND 128),
    snapshot_id UUID NOT NULL REFERENCES query_snapshots(snapshot_id),
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('search', 'aggregate', 'detail', 'related', 'alert')),
    operation_hash CHAR(64) NOT NULL CHECK (operation_hash ~ '^[0-9a-f]{64}$'),
    operation_bytes BYTEA NOT NULL CHECK (octet_length(operation_bytes) BETWEEN 2 AND 65536),
    state TEXT NOT NULL CHECK (state IN ('planning', 'queued', 'running', 'succeeded', 'failed', 'canceled')),
    deadline TIMESTAMPTZ NOT NULL,
    coordinator_owner TEXT,
    coordinator_fence BIGINT NOT NULL DEFAULT 0 CHECK (coordinator_fence >= 0),
    lease_until TIMESTAMPTZ,
    result_intent_id UUID REFERENCES object_intents(intent_id),
    result_sha256 CHAR(64) CHECK (result_sha256 IS NULL OR result_sha256 ~ '^[0-9a-f]{64}$'),
    result_bytes BIGINT CHECK (result_bytes IS NULL OR result_bytes >= 0),
    error_code TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    sealed_plan_sha CHAR(64) CHECK (sealed_plan_sha IS NULL OR sealed_plan_sha ~ '^[0-9a-f]{64}$'),
    plan_file_count INTEGER NOT NULL DEFAULT 0 CHECK (plan_file_count BETWEEN 0 AND 32768),
    plan_scan_count INTEGER NOT NULL DEFAULT 0 CHECK (plan_scan_count BETWEEN 0 AND 4096),
    plan_bytes BIGINT NOT NULL DEFAULT 0 CHECK (plan_bytes BETWEEN 0 AND 16777216),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, query_id),
    FOREIGN KEY (tenant_id, snapshot_id) REFERENCES query_snapshots(tenant_id, snapshot_id),
    FOREIGN KEY (tenant_id, result_intent_id) REFERENCES object_intents(tenant_id, intent_id),
    CHECK ((coordinator_owner IS NULL) = (lease_until IS NULL)),
    CHECK ((result_sha256 IS NULL) = (result_bytes IS NULL)),
    CHECK (expires_at > created_at)
);
CREATE INDEX query_jobs_claim_idx ON query_jobs(state, deadline, query_id);
CREATE INDEX query_jobs_owner_idx ON query_jobs(tenant_id, user_id, expires_at, query_id);

CREATE TABLE query_tasks (
    query_id UUID NOT NULL REFERENCES query_jobs(query_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('scan', 'reduce')),
    level INTEGER NOT NULL CHECK (level >= 0),
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'canceled')),
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    owner TEXT,
    lease_until TIMESTAMPTZ,
    manifest_json BYTEA NOT NULL CHECK (octet_length(manifest_json) BETWEEN 2 AND 1048576),
    result_intent_id UUID REFERENCES object_intents(intent_id),
    result_sha256 CHAR(64) CHECK (result_sha256 IS NULL OR result_sha256 ~ '^[0-9a-f]{64}$'),
    result_rows BIGINT CHECK (result_rows IS NULL OR result_rows >= 0),
    result_bytes BIGINT CHECK (result_bytes IS NULL OR result_bytes >= 0),
    retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    error_code TEXT,
    PRIMARY KEY (query_id, stage, level, partition_id),
    UNIQUE (tenant_id, query_id, stage, level, partition_id),
    FOREIGN KEY (tenant_id, query_id) REFERENCES query_jobs(tenant_id, query_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, result_intent_id) REFERENCES object_intents(tenant_id, intent_id),
    CHECK ((state = 'running') = (owner IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK ((result_sha256 IS NULL) = (result_bytes IS NULL))
);
CREATE INDEX query_tasks_claim_idx ON query_tasks(state, retry_at, query_id, stage, level, partition_id);

CREATE TABLE query_task_inputs (
    query_id UUID NOT NULL,
    tenant_id BIGINT NOT NULL,
    consumer_stage TEXT NOT NULL,
    consumer_level INTEGER NOT NULL,
    consumer_partition_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 7),
    producer_stage TEXT NOT NULL,
    producer_level INTEGER NOT NULL,
    producer_partition_id INTEGER NOT NULL,
    PRIMARY KEY (query_id, consumer_stage, consumer_level, consumer_partition_id, ordinal),
    UNIQUE (query_id, consumer_stage, consumer_level, consumer_partition_id, producer_stage, producer_level, producer_partition_id),
    FOREIGN KEY (tenant_id, query_id, consumer_stage, consumer_level, consumer_partition_id) REFERENCES query_tasks(tenant_id, query_id, stage, level, partition_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, query_id, producer_stage, producer_level, producer_partition_id) REFERENCES query_tasks(tenant_id, query_id, stage, level, partition_id) ON DELETE CASCADE,
    CHECK (consumer_stage = 'reduce' AND producer_level < consumer_level)
);

CREATE TABLE query_level_budgets (
    query_id UUID NOT NULL REFERENCES query_jobs(query_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL,
    level INTEGER NOT NULL CHECK (level >= 0),
    reserved_bytes BIGINT NOT NULL DEFAULT 0 CHECK (reserved_bytes BETWEEN 0 AND 67108864),
    committed_bytes BIGINT NOT NULL DEFAULT 0 CHECK (committed_bytes BETWEEN 0 AND reserved_bytes),
    PRIMARY KEY (query_id, level),
    FOREIGN KEY (tenant_id, query_id) REFERENCES query_jobs(tenant_id, query_id) ON DELETE CASCADE
);

-- +eventglass Down
DROP TABLE query_level_budgets;
DROP TABLE query_task_inputs;
DROP TABLE query_tasks;
DROP TABLE query_jobs;
DROP TABLE snapshot_lanes;
DROP TABLE snapshot_projects;
DROP TABLE query_snapshots;
DROP INDEX issue_transitions_operation_idx;
ALTER TABLE issue_transitions DROP CONSTRAINT issue_transitions_actor_fk, DROP COLUMN operation_id;
DROP TABLE audit_events;
DROP TABLE login_limits;
DROP TABLE sessions;
DROP TABLE project_grants;
DROP TABLE memberships;
DROP TABLE users;
ALTER TABLE projects ALTER COLUMN project_id DROP DEFAULT;
ALTER TABLE tenants ALTER COLUMN tenant_id DROP DEFAULT;
DROP SEQUENCE eventglass_user_id_seq;
DROP SEQUENCE eventglass_project_id_seq;
DROP SEQUENCE eventglass_tenant_id_seq;
ALTER TABLE installations
    DROP CONSTRAINT installations_setup_authority_check,
    DROP COLUMN setup_state,
    DROP COLUMN setup_attempt,
    DROP COLUMN setup_request_fingerprint,
    DROP COLUMN setup_marker_key,
    DROP COLUMN setup_marker_sha256,
    DROP COLUMN setup_owner,
    DROP COLUMN setup_fence,
    DROP COLUMN setup_lease_until,
    DROP COLUMN bootstrap_token_hash,
    DROP COLUMN setup_completed_at,
    DROP COLUMN retention_floor_us,
    DROP COLUMN retention_tick_at,
    DROP COLUMN recovery_state,
    DROP COLUMN alerts_paused;
