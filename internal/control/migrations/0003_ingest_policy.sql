-- +eventglass Up
ALTER TABLE installations
    ADD COLUMN global_scrub_policy_sha CHAR(64) NOT NULL DEFAULT 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', -- pragma: allowlist secret -- SHA-256 of the deterministic empty policy
    ADD COLUMN global_scrub_policy_revision BIGINT NOT NULL DEFAULT 1,
    ADD CONSTRAINT installations_global_scrub_sha_check CHECK (global_scrub_policy_sha ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT installations_global_scrub_revision_check CHECK (global_scrub_policy_revision > 0);

ALTER TABLE tenants
    ADD COLUMN auth_revision BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN name TEXT NOT NULL DEFAULT 'Tenant',
    ADD CONSTRAINT tenants_auth_revision_check CHECK (auth_revision > 0),
    ADD CONSTRAINT tenants_name_check CHECK (char_length(name) BETWEEN 1 AND 128);

ALTER TABLE projects
    ADD COLUMN name TEXT NOT NULL DEFAULT 'Project',
    ADD COLUMN allowed_origins JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN scrub_rules JSONB NOT NULL DEFAULT '{"version":1,"rules":[]}'::jsonb,
    ADD COLUMN config_revision BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN retention_days INTEGER NOT NULL DEFAULT 30,
    ADD COLUMN retention_revision BIGINT NOT NULL DEFAULT 1,
    ADD CONSTRAINT projects_name_check CHECK (char_length(name) BETWEEN 1 AND 128),
    ADD CONSTRAINT projects_allowed_origins_check CHECK (jsonb_typeof(allowed_origins) = 'array' AND jsonb_array_length(allowed_origins) <= 100 AND octet_length(allowed_origins::text) <= 65536),
    ADD CONSTRAINT projects_scrub_rules_check CHECK (jsonb_typeof(scrub_rules) = 'object' AND scrub_rules ? 'version' AND octet_length(scrub_rules::text) <= 65536),
    ADD CONSTRAINT projects_config_revision_check CHECK (config_revision > 0),
    ADD CONSTRAINT projects_retention_days_check CHECK (retention_days BETWEEN 1 AND 3650),
    ADD CONSTRAINT projects_retention_revision_check CHECK (retention_revision > 0);

ALTER TABLE project_keys
    ADD COLUMN key_id UUID,
    ADD COLUMN label TEXT NOT NULL DEFAULT '',
    ADD COLUMN key_prefix TEXT;
UPDATE project_keys
SET key_id = (
        substring(encode(sha256(convert_to(tenant_id::text || ':' || project_id::text || ':' || encode(key_hash, 'hex'), 'UTF8')), 'hex') from 1 for 8) || '-' ||
        substring(encode(sha256(convert_to(tenant_id::text || ':' || project_id::text || ':' || encode(key_hash, 'hex'), 'UTF8')), 'hex') from 9 for 4) || '-' ||
        substring(encode(sha256(convert_to(tenant_id::text || ':' || project_id::text || ':' || encode(key_hash, 'hex'), 'UTF8')), 'hex') from 13 for 4) || '-' ||
        substring(encode(sha256(convert_to(tenant_id::text || ':' || project_id::text || ':' || encode(key_hash, 'hex'), 'UTF8')), 'hex') from 17 for 4) || '-' ||
        substring(encode(sha256(convert_to(tenant_id::text || ':' || project_id::text || ':' || encode(key_hash, 'hex'), 'UTF8')), 'hex') from 21 for 12)
    )::uuid,
    key_prefix = left(encode(key_hash, 'hex'), 8);
ALTER TABLE project_keys
    ALTER COLUMN key_id SET NOT NULL,
    ALTER COLUMN key_prefix SET NOT NULL,
    ADD CONSTRAINT project_keys_key_id_key UNIQUE (key_id),
    ADD CONSTRAINT project_keys_label_check CHECK (char_length(label) <= 128),
    ADD CONSTRAINT project_keys_key_prefix_check CHECK (key_prefix ~ '^[0-9a-f]{8}$');

ALTER TABLE receipts
    ADD COLUMN policy_revision INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN normalizer_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN grouping_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN dedupe_hash_version INTEGER NOT NULL DEFAULT 1,
    ADD CONSTRAINT receipts_policy_revision_check CHECK (policy_revision > 0),
    ADD CONSTRAINT receipts_normalizer_version_check CHECK (normalizer_version > 0),
    ADD CONSTRAINT receipts_grouping_version_check CHECK (grouping_version > 0),
    ADD CONSTRAINT receipts_dedupe_hash_version_check CHECK (dedupe_hash_version > 0);

CREATE TABLE receipt_unsupported (
    acceptance_id UUID NOT NULL REFERENCES receipts(acceptance_id) ON DELETE CASCADE,
    item_ordinal INTEGER NOT NULL CHECK (item_ordinal >= 0),
    item_type_json TEXT NOT NULL CHECK (octet_length(item_type_json) BETWEEN 2 AND 770),
    byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
    PRIMARY KEY (acceptance_id, item_ordinal)
);

ALTER TABLE jobs DROP CONSTRAINT jobs_state_check;
ALTER TABLE jobs DROP CONSTRAINT jobs_check;
ALTER TABLE jobs
    ADD CONSTRAINT jobs_state_check CHECK (state IN ('queued', 'running', 'prepared', 'completed', 'failed')),
    ADD CONSTRAINT jobs_lease_state_check CHECK ((state = 'running') = (owner IS NOT NULL AND lease_until IS NOT NULL)),
    ADD CONSTRAINT jobs_tenant_job_key UNIQUE (tenant_id, job_id);
CREATE INDEX jobs_prepared_idx ON jobs(tenant_id, lane_id, batch_seq) WHERE state = 'prepared';

ALTER TABLE object_intents
    ADD COLUMN retired_at TIMESTAMPTZ,
    ADD COLUMN protect_until TIMESTAMPTZ,
    ADD COLUMN conversion_job_id UUID,
    ADD COLUMN producer_generation BIGINT,
    ADD COLUMN producer_fence BIGINT,
    ADD CONSTRAINT object_intents_conversion_job_fk FOREIGN KEY (tenant_id, conversion_job_id) REFERENCES jobs(tenant_id, job_id),
    ADD CONSTRAINT object_intents_producer_pair_check CHECK ((producer_generation IS NULL) = (producer_fence IS NULL)),
    ADD CONSTRAINT object_intents_producer_job_check CHECK ((conversion_job_id IS NULL) = (producer_generation IS NULL)),
    ADD CONSTRAINT object_intents_producer_values_check CHECK (producer_generation IS NULL OR (producer_generation > 0 AND producer_fence > 0));

ALTER TABLE sdk_outcomes
    ADD COLUMN category_json TEXT,
    ADD COLUMN reason_json TEXT,
    ADD COLUMN category_sha256 BYTEA,
    ADD COLUMN reason_sha256 BYTEA;
UPDATE sdk_outcomes
SET category_json = to_json(category)::text,
    reason_json = to_json(reason)::text;
UPDATE sdk_outcomes
SET category_sha256 = sha256(convert_to(category_json, 'UTF8')),
    reason_sha256 = sha256(convert_to(reason_json, 'UTF8'));
ALTER TABLE sdk_outcomes
    ALTER COLUMN category_json SET NOT NULL,
    ALTER COLUMN reason_json SET NOT NULL,
    ALTER COLUMN category_sha256 SET NOT NULL,
    ALTER COLUMN reason_sha256 SET NOT NULL,
    DROP CONSTRAINT sdk_outcomes_pkey,
    DROP COLUMN category,
    DROP COLUMN reason,
    ADD CONSTRAINT sdk_outcomes_category_json_check CHECK (octet_length(category_json) BETWEEN 2 AND 125829122),
    ADD CONSTRAINT sdk_outcomes_reason_json_check CHECK (octet_length(reason_json) BETWEEN 2 AND 125829122),
    ADD CONSTRAINT sdk_outcomes_category_sha_check CHECK (octet_length(category_sha256) = 32),
    ADD CONSTRAINT sdk_outcomes_reason_sha_check CHECK (octet_length(reason_sha256) = 32),
    ADD CONSTRAINT sdk_outcomes_pkey PRIMARY KEY (acceptance_id, item_ordinal, category_sha256, reason_sha256);

-- +eventglass Down
ALTER TABLE sdk_outcomes
    ADD COLUMN category TEXT,
    ADD COLUMN reason TEXT;
UPDATE sdk_outcomes
SET category = category_json::jsonb #>> '{}',
    reason = reason_json::jsonb #>> '{}';
ALTER TABLE sdk_outcomes
    ALTER COLUMN category SET NOT NULL,
    ALTER COLUMN reason SET NOT NULL,
    DROP CONSTRAINT sdk_outcomes_pkey,
    DROP CONSTRAINT sdk_outcomes_category_json_check,
    DROP CONSTRAINT sdk_outcomes_reason_json_check,
    DROP CONSTRAINT sdk_outcomes_category_sha_check,
    DROP CONSTRAINT sdk_outcomes_reason_sha_check,
    DROP COLUMN category_json,
    DROP COLUMN reason_json,
    DROP COLUMN category_sha256,
    DROP COLUMN reason_sha256,
    ADD CONSTRAINT sdk_outcomes_pkey PRIMARY KEY (acceptance_id, item_ordinal, category, reason);

ALTER TABLE object_intents
    DROP CONSTRAINT object_intents_conversion_job_fk,
    DROP CONSTRAINT object_intents_producer_pair_check,
    DROP CONSTRAINT object_intents_producer_job_check,
    DROP CONSTRAINT object_intents_producer_values_check,
    DROP COLUMN retired_at,
    DROP COLUMN protect_until,
    DROP COLUMN conversion_job_id,
    DROP COLUMN producer_generation,
    DROP COLUMN producer_fence;

DROP INDEX jobs_prepared_idx;
ALTER TABLE jobs
    DROP CONSTRAINT jobs_state_check,
    DROP CONSTRAINT jobs_lease_state_check,
    DROP CONSTRAINT jobs_tenant_job_key,
    ADD CONSTRAINT jobs_state_check CHECK (state IN ('queued', 'running', 'completed', 'failed')),
    ADD CONSTRAINT jobs_check CHECK ((state = 'running') = (owner IS NOT NULL AND lease_until IS NOT NULL));

DROP TABLE receipt_unsupported;

ALTER TABLE receipts
    DROP CONSTRAINT receipts_policy_revision_check,
    DROP CONSTRAINT receipts_normalizer_version_check,
    DROP CONSTRAINT receipts_grouping_version_check,
    DROP CONSTRAINT receipts_dedupe_hash_version_check,
    DROP COLUMN policy_revision,
    DROP COLUMN normalizer_version,
    DROP COLUMN grouping_version,
    DROP COLUMN dedupe_hash_version;

ALTER TABLE project_keys
    DROP CONSTRAINT project_keys_key_id_key,
    DROP CONSTRAINT project_keys_label_check,
    DROP CONSTRAINT project_keys_key_prefix_check,
    DROP COLUMN key_id,
    DROP COLUMN label,
    DROP COLUMN key_prefix;

ALTER TABLE projects
    DROP CONSTRAINT projects_name_check,
    DROP CONSTRAINT projects_allowed_origins_check,
    DROP CONSTRAINT projects_scrub_rules_check,
    DROP CONSTRAINT projects_config_revision_check,
    DROP CONSTRAINT projects_retention_days_check,
    DROP CONSTRAINT projects_retention_revision_check,
    DROP COLUMN name,
    DROP COLUMN allowed_origins,
    DROP COLUMN scrub_rules,
    DROP COLUMN config_revision,
    DROP COLUMN retention_days,
    DROP COLUMN retention_revision;

ALTER TABLE tenants
    DROP CONSTRAINT tenants_auth_revision_check,
    DROP CONSTRAINT tenants_name_check,
    DROP COLUMN auth_revision,
    DROP COLUMN name;

ALTER TABLE installations
    DROP CONSTRAINT installations_global_scrub_sha_check,
    DROP CONSTRAINT installations_global_scrub_revision_check,
    DROP COLUMN global_scrub_policy_sha,
    DROP COLUMN global_scrub_policy_revision;
