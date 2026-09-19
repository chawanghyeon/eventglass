-- +eventglass Up
ALTER TABLE installations
    ADD COLUMN retention_days INTEGER NOT NULL DEFAULT 30 CHECK (retention_days BETWEEN 1 AND 3650),
    ADD COLUMN retention_revision BIGINT NOT NULL DEFAULT 1 CHECK (retention_revision > 0);

ALTER TABLE query_snapshots ADD COLUMN tenant_auth_revision BIGINT;
UPDATE query_snapshots s SET tenant_auth_revision=t.auth_revision
FROM tenants t WHERE t.tenant_id=s.tenant_id;
ALTER TABLE query_snapshots
    ALTER COLUMN tenant_auth_revision SET NOT NULL,
    ADD CONSTRAINT query_snapshots_tenant_auth_revision_check CHECK (tenant_auth_revision > 0);

ALTER TABLE snapshot_projects ADD COLUMN project_auth_revision BIGINT;
UPDATE snapshot_projects s SET project_auth_revision=p.auth_revision
FROM projects p WHERE p.tenant_id=s.tenant_id AND p.project_id=s.project_id;
ALTER TABLE snapshot_projects
    ALTER COLUMN project_auth_revision SET NOT NULL,
    ADD CONSTRAINT snapshot_projects_auth_revision_check CHECK (project_auth_revision > 0);

-- +eventglass Down
ALTER TABLE snapshot_projects
    DROP CONSTRAINT snapshot_projects_auth_revision_check,
    DROP COLUMN project_auth_revision;
ALTER TABLE query_snapshots
    DROP CONSTRAINT query_snapshots_tenant_auth_revision_check,
    DROP COLUMN tenant_auth_revision;
ALTER TABLE installations
    DROP COLUMN retention_revision,
    DROP COLUMN retention_days;
