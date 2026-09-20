-- +eventglass Up
ALTER TABLE installations
    ADD COLUMN alert_encryption_key_id TEXT CHECK (alert_encryption_key_id IS NULL OR char_length(alert_encryption_key_id) BETWEEN 1 AND 128);

ALTER TABLE audit_events DROP CONSTRAINT audit_events_action_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_check CHECK (action IN (
    'setup', 'login', 'logout', 'password_changed', 'project_created', 'project_updated',
    'key_created', 'key_revoked', 'user_created', 'membership_updated', 'issue_status_changed',
    'destination_created', 'destination_updated', 'alert_created', 'alert_updated'
));

CREATE TABLE alert_destinations (
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    destination_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128 AND name=btrim(name)),
    url TEXT NOT NULL CHECK (octet_length(url) BETWEEN 1 AND 2048),
    secret_ciphertext BYTEA,
    encryption_key_id TEXT,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, destination_id),
    UNIQUE (destination_id),
    CHECK ((secret_ciphertext IS NULL) = (encryption_key_id IS NULL)),
    CHECK (secret_ciphertext IS NULL OR (octet_length(secret_ciphertext) BETWEEN 29 AND 4130 AND char_length(encryption_key_id) BETWEEN 1 AND 128))
);
CREATE INDEX alert_destinations_list_idx ON alert_destinations(tenant_id, created_at, destination_id);

CREATE TABLE alerts (
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    alert_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128 AND name=btrim(name)),
    kind TEXT NOT NULL CHECK (kind IN ('issue', 'threshold')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    rule_bytes BYTEA NOT NULL CHECK (octet_length(rule_bytes) BETWEEN 2 AND 32768),
    rule_sha256 CHAR(64) NOT NULL CHECK (rule_sha256 ~ '^[0-9a-f]{64}$'),
    destination_id UUID NOT NULL,
    cooldown_seconds INTEGER NOT NULL CHECK (cooldown_seconds BETWEEN 0 AND 86400),
    enabled_from_public_cut BIGINT[] NOT NULL,
    enabled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    first_window_end_us BIGINT,
    last_completed_end_us BIGINT,
    last_fired_end_us BIGINT,
    last_issue_fired_at_us BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, alert_id),
    UNIQUE (alert_id),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    FOREIGN KEY (tenant_id, destination_id) REFERENCES alert_destinations(tenant_id, destination_id),
    CHECK (cardinality(enabled_from_public_cut)=16 AND array_lower(enabled_from_public_cut,1)=1),
    CHECK ((kind='threshold') = (first_window_end_us IS NOT NULL)),
    CHECK (last_completed_end_us IS NULL OR kind='threshold'),
    CHECK (last_fired_end_us IS NULL OR kind='threshold'),
    CHECK (last_issue_fired_at_us IS NULL OR kind='issue')
);
CREATE INDEX alerts_list_idx ON alerts(tenant_id, project_id, created_at, alert_id);
CREATE INDEX alerts_scheduler_idx ON alerts(enabled, kind, tenant_id, alert_id);

CREATE TABLE alert_evaluations (
    tenant_id BIGINT NOT NULL,
    alert_id UUID NOT NULL,
    alert_revision BIGINT NOT NULL CHECK (alert_revision > 0),
    evaluation_id UUID NOT NULL,
    window_start_us BIGINT NOT NULL,
    window_end_us BIGINT NOT NULL CHECK (window_end_us > window_start_us),
    cut BIGINT[] NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('waiting','queued','running','succeeded','failed','canceled')),
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    owner TEXT,
    lease_until TIMESTAMPTZ,
    snapshot_id UUID REFERENCES query_snapshots(snapshot_id),
    observed_count BIGINT,
    fired BOOLEAN,
    error_code TEXT CHECK (error_code IS NULL OR char_length(error_code) BETWEEN 1 AND 64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, evaluation_id),
    UNIQUE (evaluation_id),
    UNIQUE (alert_id, alert_revision, window_end_us),
    FOREIGN KEY (tenant_id, alert_id) REFERENCES alerts(tenant_id, alert_id),
    CHECK (cardinality(cut)=16 AND array_lower(cut,1)=1),
    CHECK ((state='running') = (owner IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK ((state='succeeded') = (observed_count IS NOT NULL AND fired IS NOT NULL)),
    CHECK (observed_count IS NULL OR observed_count >= 0)
);
CREATE INDEX alert_evaluations_claim_idx ON alert_evaluations(state, window_end_us, evaluation_id);
CREATE UNIQUE INDEX query_jobs_alert_snapshot_idx ON query_jobs(snapshot_id) WHERE user_id IS NULL;

CREATE TABLE deliveries (
    tenant_id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    delivery_id UUID NOT NULL,
    alert_id UUID NOT NULL,
    alert_revision BIGINT NOT NULL CHECK (alert_revision > 0),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    destination_id UUID NOT NULL,
    destination_revision BIGINT NOT NULL CHECK (destination_revision > 0),
    destination_url TEXT NOT NULL CHECK (octet_length(destination_url) BETWEEN 1 AND 2048),
    destination_secret_ciphertext BYTEA,
    destination_encryption_key_id TEXT,
    dedupe_key TEXT NOT NULL CHECK (char_length(dedupe_key) BETWEEN 1 AND 256),
    body_bytes BYTEA NOT NULL CHECK (octet_length(body_bytes) BETWEEN 2 AND 65536),
    body_sha256 CHAR(64) NOT NULL CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','succeeded','failed','canceled')),
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    owner TEXT,
    lease_until TIMESTAMPTZ,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt BETWEEN 0 AND 12),
    retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_status INTEGER CHECK (last_status BETWEEN 100 AND 599),
    error_code TEXT CHECK (error_code IS NULL OR char_length(error_code) BETWEEN 1 AND 64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, delivery_id),
    UNIQUE (delivery_id),
    UNIQUE (tenant_id, dedupe_key),
    FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, project_id),
    FOREIGN KEY (tenant_id, alert_id) REFERENCES alerts(tenant_id, alert_id),
    CHECK ((destination_secret_ciphertext IS NULL) = (destination_encryption_key_id IS NULL)),
    CHECK ((state='running') = (owner IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX deliveries_claim_idx ON deliveries(state, retry_at, delivery_id);
CREATE INDEX deliveries_list_idx ON deliveries(tenant_id, project_id, created_at DESC, delivery_id DESC);

CREATE TABLE issue_alert_evaluations (
    tenant_id BIGINT NOT NULL,
    alert_id UUID NOT NULL,
    alert_revision BIGINT NOT NULL CHECK (alert_revision > 0),
    transition_id UUID NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('sent','cooldown','disabled')),
    delivery_id UUID,
    evaluated_at_us BIGINT NOT NULL,
    PRIMARY KEY (alert_id, alert_revision, transition_id),
    FOREIGN KEY (tenant_id, alert_id) REFERENCES alerts(tenant_id, alert_id),
    FOREIGN KEY (transition_id) REFERENCES issue_transitions(transition_id),
    FOREIGN KEY (tenant_id, delivery_id) REFERENCES deliveries(tenant_id, delivery_id),
    CHECK ((decision='sent') = (delivery_id IS NOT NULL))
);
CREATE INDEX issue_alert_evaluations_scan_idx ON issue_alert_evaluations(tenant_id, alert_id, alert_revision, transition_id);

-- Alert snapshots have no browser user. Their authority is the live alert row.
ALTER TABLE query_snapshots DROP CONSTRAINT query_snapshots_check;
ALTER TABLE query_snapshots ADD CONSTRAINT query_snapshots_principal_check CHECK (
    (principal_kind='user' AND user_id IS NOT NULL) OR (principal_kind='alert' AND user_id IS NULL)
);

-- +eventglass Down
ALTER TABLE query_snapshots DROP CONSTRAINT query_snapshots_principal_check;
ALTER TABLE query_snapshots ADD CONSTRAINT query_snapshots_check CHECK ((principal_kind='user') = (user_id IS NOT NULL));
DROP TABLE issue_alert_evaluations;
DROP TABLE deliveries;
DROP TABLE alert_evaluations;
DROP INDEX query_jobs_alert_snapshot_idx;
DROP TABLE alerts;
DROP TABLE alert_destinations;
ALTER TABLE audit_events DROP CONSTRAINT audit_events_action_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_check CHECK (action IN ('setup', 'login', 'logout', 'password_changed', 'project_created', 'project_updated', 'key_created', 'key_revoked', 'user_created', 'membership_updated', 'issue_status_changed'));
ALTER TABLE installations DROP COLUMN alert_encryption_key_id;
