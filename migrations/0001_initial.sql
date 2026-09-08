CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'member')),
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    expires_at_us INTEGER NOT NULL,
    created_at_us INTEGER NOT NULL,
    last_seen_at_us INTEGER NOT NULL
);
CREATE INDEX sessions_user ON sessions(user_id);

CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    platform TEXT,
    default_environment TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE project_keys (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    public_key TEXT NOT NULL UNIQUE,
    created_at_us INTEGER NOT NULL,
    revoked_at_us INTEGER
);

CREATE TABLE shards (
    id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    format_version TEXT NOT NULL,
    tokenizer_version INTEGER NOT NULL DEFAULT 1,
    min_received_at_us INTEGER,
    max_received_at_us INTEGER,
    first_record_received_at_us INTEGER,
    state TEXT NOT NULL CHECK (
        state IN ('active', 'local', 'remote_verified', 'remote_only')
    ),
    min_timestamp_us INTEGER,
    max_timestamp_us INTEGER,
    min_ingest_seq INTEGER,
    max_ingest_seq INTEGER,
    last_applied_inbox_id INTEGER NOT NULL DEFAULT 0,
    record_count INTEGER NOT NULL DEFAULT 0,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    remote_archive_key TEXT,
    archive_sha256 TEXT,
    recovery_checkpoint_id TEXT,
    created_at_us INTEGER NOT NULL,
    sealed_at_us INTEGER,
    last_accessed_at_us INTEGER
);
CREATE UNIQUE INDEX shards_one_active ON shards(state) WHERE state = 'active';
CREATE INDEX shards_time_range ON shards(min_timestamp_us, max_timestamp_us);

CREATE TABLE runtime_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    installation_id TEXT NOT NULL,
    storage_generation TEXT NOT NULL,
    next_ingest_seq INTEGER NOT NULL CHECK (next_ingest_seq > 0),
    last_applied_inbox_id INTEGER NOT NULL DEFAULT 0,
    last_applied_ingest_seq INTEGER NOT NULL DEFAULT 0,
    authorization_epoch INTEGER NOT NULL DEFAULT 0,
    inbox_bytes INTEGER NOT NULL DEFAULT 0 CHECK (inbox_bytes >= 0),
    inbox_records INTEGER NOT NULL DEFAULT 0 CHECK (inbox_records >= 0),
    active_shard_id TEXT REFERENCES shards(id)
);

CREATE TABLE inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    acceptance_id TEXT NOT NULL,
    chunk_no INTEGER NOT NULL,
    first_ingest_seq INTEGER NOT NULL,
    last_ingest_seq INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    received_at_us INTEGER NOT NULL,
    normalizer_version INTEGER NOT NULL,
    payload BLOB NOT NULL,
    UNIQUE (acceptance_id, chunk_no)
);

CREATE TABLE issues (
    id TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    fingerprint TEXT NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    title TEXT NOT NULL,
    culprit TEXT,
    level TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('unresolved', 'resolved', 'ignored')),
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    occurrence_count INTEGER NOT NULL,
    first_seen_ingest_seq INTEGER NOT NULL,
    last_seen_ingest_seq INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    first_release TEXT,
    last_release TEXT,
    resolved_at_us INTEGER,
    resolved_through_ingest_seq INTEGER,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL,
    UNIQUE(project_id, fingerprint_version, fingerprint)
);
CREATE INDEX issues_listing ON issues(project_id, status, last_seen_us DESC, id);

CREATE TABLE issue_occurrences (
    event_key TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    issue_id TEXT NOT NULL REFERENCES issues(id),
    record_id TEXT NOT NULL UNIQUE,
    shard_id TEXT NOT NULL REFERENCES shards(id),
    source_event_id TEXT,
    ingest_seq INTEGER NOT NULL UNIQUE,
    occurred_at_us INTEGER NOT NULL
);
CREATE INDEX occurrence_listing
    ON issue_occurrences(issue_id, occurred_at_us DESC, ingest_seq DESC);

CREATE TABLE alerts (
    id INTEGER PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id),
    name TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    deleted_at_us INTEGER,
    last_evaluation_error TEXT,
    last_evaluation_watermark INTEGER,
    pending_evaluation_end_us INTEGER,
    pending_cut_seq INTEGER,
    condition_type TEXT NOT NULL,
    condition_json TEXT NOT NULL,
    destination_type TEXT NOT NULL,
    destination_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_evaluated_at_us INTEGER,
    last_triggered_at_us INTEGER,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE alert_deliveries (
    id TEXT PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(id),
    dedupe_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'sent', 'failed', 'cancelled')),
    sent_at_us INTEGER,
    last_status_code INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at_us INTEGER NOT NULL,
    created_at_us INTEGER NOT NULL,
    last_error TEXT
);
CREATE INDEX delivery_due ON alert_deliveries(state, next_retry_at_us);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE backup_snapshots (
    id TEXT PRIMARY KEY,
    cut_ingest_seq INTEGER NOT NULL,
    cut_inbox_id INTEGER NOT NULL,
    object_key TEXT,
    sha256 TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'failed')),
    created_at_us INTEGER NOT NULL,
    completed_at_us INTEGER,
    last_error TEXT
);

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at_us INTEGER NOT NULL
);

CREATE INDEX shards_received_range ON shards(min_received_at_us, max_received_at_us);
