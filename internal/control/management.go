package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type ProjectKeyMetadata struct {
	KeyID     string
	Label     string
	KeyPrefix string
	State     string
	Revision  int64
	CreatedAt time.Time
}

type CreateProjectKeyCommand struct {
	TenantID    int64
	ProjectID   int64
	KeyID       string
	Label       string
	KeyPrefix   string
	KeyHash     [32]byte
	ActorUserID int64
	RequestID   string
	AuditID     string
}

func (operations *AuthOperations) CreateProjectKey(ctx context.Context, command CreateProjectKeyCommand) (ProjectKeyMetadata, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.KeyID == "" || len(command.Label) > 128 || len(command.KeyPrefix) != 8 || command.KeyHash == ([32]byte{}) || command.ActorUserID <= 0 || command.RequestID == "" || command.AuditID == "" {
		return ProjectKeyMetadata{}, errors.New("invalid project key")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return ProjectKeyMetadata{}, err
	}
	defer tx.Rollback(ctx)
	var tenantState, projectState string
	if err := tx.QueryRow(ctx, `SELECT t.state,p.state FROM tenants t JOIN projects p ON p.tenant_id=t.tenant_id
		WHERE t.tenant_id=$1 AND p.project_id=$2 FOR SHARE OF t,p`, command.TenantID, command.ProjectID).Scan(&tenantState, &projectState); err != nil {
		return ProjectKeyMetadata{}, ErrForbidden
	}
	if tenantState != "active" || projectState != "active" {
		return ProjectKeyMetadata{}, ErrForbidden
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM project_keys WHERE tenant_id=$1`, command.TenantID).Scan(&count); err != nil {
		return ProjectKeyMetadata{}, err
	}
	if count >= 1000 {
		return ProjectKeyMetadata{}, errors.New("project key limit exceeded")
	}
	var result ProjectKeyMetadata
	err = tx.QueryRow(ctx, `INSERT INTO project_keys(tenant_id,project_id,key_hash,key_id,label,key_prefix)
		VALUES($1,$2,$3,$4,$5,$6) RETURNING key_id::text,label,key_prefix,state,revision,created_at`,
		command.TenantID, command.ProjectID, command.KeyHash[:], command.KeyID, strings.TrimSpace(command.Label), command.KeyPrefix).Scan(
		&result.KeyID, &result.Label, &result.KeyPrefix, &result.State, &result.Revision, &result.CreatedAt)
	if err != nil {
		return ProjectKeyMetadata{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE projects SET auth_revision=auth_revision+1 WHERE tenant_id=$1 AND project_id=$2`, command.TenantID, command.ProjectID); err != nil {
		return ProjectKeyMetadata{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'key_created','project_key',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, result.KeyID, result.Revision, command.RequestID); err != nil {
		return ProjectKeyMetadata{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectKeyMetadata{}, err
	}
	return result, nil
}

func (operations *AuthOperations) ListProjectKeys(ctx context.Context, tenantID, projectID int64) ([]ProjectKeyMetadata, error) {
	rows, err := operations.pool.Query(ctx, `SELECT key_id::text,label,key_prefix,state,revision,created_at
		FROM project_keys WHERE tenant_id=$1 AND project_id=$2 ORDER BY created_at,key_id LIMIT 1001`, tenantID, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ProjectKeyMetadata, 0)
	for rows.Next() {
		var key ProjectKeyMetadata
		if err := rows.Scan(&key.KeyID, &key.Label, &key.KeyPrefix, &key.State, &key.Revision, &key.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, key)
	}
	if len(result) > 1000 {
		return nil, errors.New("project key limit exceeded")
	}
	return result, rows.Err()
}

type RevokeProjectKeyCommand struct {
	TenantID         int64
	ProjectID        int64
	KeyID            string
	ExpectedRevision int64
	ActorUserID      int64
	RequestID        string
	AuditID          string
}

func (operations *AuthOperations) RevokeProjectKey(ctx context.Context, command RevokeProjectKeyCommand) error {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.KeyID == "" || command.ExpectedRevision <= 0 || command.ActorUserID <= 0 || command.RequestID == "" || command.AuditID == "" {
		return errors.New("invalid key revocation")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT project_id FROM projects WHERE tenant_id=$1 AND project_id=$2 FOR UPDATE`, command.TenantID, command.ProjectID); err != nil {
		return ErrForbidden
	}
	result, err := tx.Exec(ctx, `UPDATE project_keys SET state='revoked',revision=revision+1
		WHERE tenant_id=$1 AND project_id=$2 AND key_id=$3 AND revision=$4 AND state='active'`, command.TenantID, command.ProjectID, command.KeyID, command.ExpectedRevision)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		var state string
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT state,revision FROM project_keys WHERE tenant_id=$1 AND project_id=$2 AND key_id=$3`, command.TenantID, command.ProjectID, command.KeyID).Scan(&state, &revision); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrForbidden
			}
			return err
		}
		if state != "revoked" || revision != command.ExpectedRevision+1 {
			return ErrRevisionConflict
		}
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE projects SET auth_revision=auth_revision+1 WHERE tenant_id=$1 AND project_id=$2`, command.TenantID, command.ProjectID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'key_revoked','project_key',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, command.KeyID, command.ExpectedRevision+1, command.RequestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
