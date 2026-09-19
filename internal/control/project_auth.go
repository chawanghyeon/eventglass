package control

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProjectAuthorization struct {
	Snapshot       AuthorizationSnapshot
	DefaultService string
	AllowedOrigins []string
}

func LoadProjectOrigins(ctx context.Context, pool *pgxpool.Pool, tenantID, projectID int64) ([]string, error) {
	if pool == nil || tenantID <= 0 || projectID <= 0 {
		return nil, ErrAuthorizationStale
	}
	var tenantState, projectState, originsJSON string
	err := pool.QueryRow(ctx, `SELECT t.state,p.state,p.allowed_origins::text FROM tenants t
		JOIN projects p ON p.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND p.project_id=$2`, tenantID, projectID).
		Scan(&tenantState, &projectState, &originsJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrProjectDisabled
	}
	if err != nil {
		return nil, err
	}
	if tenantState != "active" || projectState != "active" {
		return nil, ErrProjectDisabled
	}
	var origins []string
	if err := json.Unmarshal([]byte(originsJSON), &origins); err != nil {
		return nil, err
	}
	return origins, nil
}

// LoadProjectAuthorization resolves current ingest policy without returning or
// persisting the public key. Accept later locks and revalidates this snapshot.
func LoadProjectAuthorization(ctx context.Context, pool *pgxpool.Pool, tenantID, projectID int64, keyHash [32]byte) (ProjectAuthorization, error) {
	if pool == nil || tenantID <= 0 || projectID <= 0 {
		return ProjectAuthorization{}, ErrAuthorizationStale
	}
	var result ProjectAuthorization
	var tenantState, projectState, keyState, originsJSON string
	err := pool.QueryRow(ctx, `SELECT t.state,t.auth_revision,p.state,p.auth_revision,p.scrub_revision,p.config_revision,
		p.default_service,p.allowed_origins::text,k.state,k.revision
		FROM tenants t JOIN projects p ON p.tenant_id=t.tenant_id
		JOIN project_keys k ON k.tenant_id=p.tenant_id AND k.project_id=p.project_id
		WHERE t.tenant_id=$1 AND p.project_id=$2 AND k.key_hash=$3`, tenantID, projectID, keyHash[:]).Scan(
		&tenantState, &result.Snapshot.TenantRevision, &projectState, &result.Snapshot.ProjectRevision,
		&result.Snapshot.ScrubRevision, &result.Snapshot.ConfigRevision, &result.DefaultService, &originsJSON,
		&keyState, &result.Snapshot.KeyRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProjectAuthorization{}, ErrKeyRevoked
	}
	if err != nil {
		return ProjectAuthorization{}, err
	}
	if tenantState != "active" {
		return ProjectAuthorization{}, ErrTenantDisabled
	}
	if projectState != "active" {
		return ProjectAuthorization{}, ErrProjectDisabled
	}
	if keyState != "active" {
		return ProjectAuthorization{}, ErrKeyRevoked
	}
	if err := json.Unmarshal([]byte(originsJSON), &result.AllowedOrigins); err != nil {
		return ProjectAuthorization{}, err
	}
	result.Snapshot.KeyHash = keyHash
	return result, nil
}
