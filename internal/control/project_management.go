package control

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

type Project struct {
	TenantID, ProjectID                   int64
	Name, State, DefaultService           string
	AllowedOrigins                        []string
	ScrubRules                            []byte
	Revision, AuthRevision, ScrubRevision int64
}

type ProjectPageCommand struct {
	TenantID, ActorUserID, AfterProjectID int64
	Limit                                 int
}

func (operations *AuthOperations) ListProjectPage(ctx context.Context, command ProjectPageCommand) ([]Project, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.AfterProjectID < 0 || command.Limit < 1 || command.Limit > 1000 {
		return nil, errors.New("invalid project page")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	admin, err := tenantAccess(ctx, tx, command.TenantID, command.ActorUserID, 0, false)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT p.tenant_id,p.project_id,p.name,p.state,p.default_service,p.allowed_origins::text,
		CASE WHEN $4 THEN p.scrub_rules::text ELSE NULL END,p.config_revision,p.auth_revision,p.scrub_revision
		FROM projects p LEFT JOIN project_grants g ON g.tenant_id=p.tenant_id AND g.project_id=p.project_id AND g.user_id=$2
		WHERE p.tenant_id=$1 AND p.project_id>$3 AND ($4 OR (g.user_id IS NOT NULL AND p.state='active'))
		ORDER BY p.project_id LIMIT $5`, command.TenantID, command.ActorUserID, command.AfterProjectID, admin, command.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Project, 0, command.Limit+1)
	for rows.Next() {
		var value Project
		var origins string
		var scrub *string
		if err := rows.Scan(&value.TenantID, &value.ProjectID, &value.Name, &value.State, &value.DefaultService, &origins, &scrub, &value.Revision, &value.AuthRevision, &value.ScrubRevision); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(origins), &value.AllowedOrigins); err != nil {
			return nil, err
		}
		if value.AllowedOrigins == nil {
			value.AllowedOrigins = []string{}
		}
		if scrub != nil {
			value.ScrubRules = []byte(*scrub)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

type CreateProjectCommand struct {
	TenantID, ActorUserID int64
	Name, DefaultService  string
	AllowedOrigins        []string
	RequestID, AuditID    string
}

func (operations *AuthOperations) CreateProject(ctx context.Context, command CreateProjectCommand) (Project, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || strings.TrimSpace(command.Name) == "" || len(strings.TrimSpace(command.Name)) > 128 || len(command.DefaultService) > 256 || len(command.AllowedOrigins) > 100 || command.RequestID == "" || command.AuditID == "" {
		return Project{}, errors.New("invalid project creation")
	}
	if command.AllowedOrigins == nil {
		command.AllowedOrigins = []string{}
	}
	origins, err := json.Marshal(command.AllowedOrigins)
	if err != nil {
		return Project{}, err
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return Project{}, err
	}
	var result Project
	var originsJSON string
	var scrubJSON string
	err = tx.QueryRow(ctx, `INSERT INTO projects(tenant_id,name,default_service,allowed_origins,scrub_rules,scrub_revision)
		VALUES($1,$2,$3,$4::jsonb,'{"version":1,"redact_keys":[],"redact_paths":[],"body_patterns":[]}'::jsonb,1)
		RETURNING tenant_id,project_id,name,state,default_service,allowed_origins::text,scrub_rules::text,config_revision,auth_revision,scrub_revision`,
		command.TenantID, strings.TrimSpace(command.Name), command.DefaultService, string(origins)).Scan(&result.TenantID, &result.ProjectID, &result.Name, &result.State, &result.DefaultService, &originsJSON, &scrubJSON, &result.Revision, &result.AuthRevision, &result.ScrubRevision)
	if err != nil {
		return Project{}, err
	}
	if err := json.Unmarshal([]byte(originsJSON), &result.AllowedOrigins); err != nil {
		return Project{}, err
	}
	result.ScrubRules = []byte(scrubJSON)
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'project_created','project',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, strconv.FormatInt(result.ProjectID, 10), result.Revision, command.RequestID); err != nil {
		return Project{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Project{}, err
	}
	return result, nil
}

type UpdateProjectCommand struct {
	TenantID, ProjectID, ActorUserID, ExpectedRevision int64
	Name, State, DefaultService                        *string
	AllowedOrigins                                     *[]string
	ScrubRules                                         []byte
	RequestID, AuditID                                 string
}

func (operations *AuthOperations) UpdateProject(ctx context.Context, command UpdateProjectCommand) (Project, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.ActorUserID <= 0 || command.ExpectedRevision <= 0 || command.RequestID == "" || command.AuditID == "" || command.Name == nil && command.State == nil && command.DefaultService == nil && command.AllowedOrigins == nil && len(command.ScrubRules) == 0 {
		return Project{}, errors.New("invalid project update")
	}
	if command.Name != nil && (strings.TrimSpace(*command.Name) == "" || len(strings.TrimSpace(*command.Name)) > 128) || command.State != nil && *command.State != "active" && *command.State != "disabled" || command.DefaultService != nil && len(*command.DefaultService) > 256 || command.AllowedOrigins != nil && len(*command.AllowedOrigins) > 100 {
		return Project{}, errors.New("invalid project update fields")
	}
	var origins any
	if command.AllowedOrigins != nil {
		encoded, err := json.Marshal(*command.AllowedOrigins)
		if err != nil {
			return Project{}, err
		}
		origins = string(encoded)
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return Project{}, err
	}
	var current int64
	if err := tx.QueryRow(ctx, `SELECT config_revision FROM projects WHERE tenant_id=$1 AND project_id=$2 FOR UPDATE`, command.TenantID, command.ProjectID).Scan(&current); errors.Is(err, pgx.ErrNoRows) {
		return Project{}, ErrForbidden
	} else if err != nil {
		return Project{}, err
	}
	if current != command.ExpectedRevision {
		return Project{}, ErrRevisionConflict
	}
	var result Project
	var originsJSON, scrubJSON string
	err = tx.QueryRow(ctx, `UPDATE projects SET
		name=coalesce($3,name),state=coalesce($4,state),default_service=coalesce($5,default_service),
		allowed_origins=coalesce($6::jsonb,allowed_origins),scrub_rules=coalesce($7::jsonb,scrub_rules),
		config_revision=config_revision+1,auth_revision=auth_revision+CASE WHEN $4 IS NULL THEN 0 ELSE 1 END,
		scrub_revision=scrub_revision+CASE WHEN $7 IS NULL THEN 0 ELSE 1 END
		WHERE tenant_id=$1 AND project_id=$2
		RETURNING tenant_id,project_id,name,state,default_service,allowed_origins::text,scrub_rules::text,config_revision,auth_revision,scrub_revision`,
		command.TenantID, command.ProjectID, trimmedText(command.Name), command.State, command.DefaultService, origins, nullableJSON(command.ScrubRules)).Scan(&result.TenantID, &result.ProjectID, &result.Name, &result.State, &result.DefaultService, &originsJSON, &scrubJSON, &result.Revision, &result.AuthRevision, &result.ScrubRevision)
	if err != nil {
		return Project{}, err
	}
	if err := json.Unmarshal([]byte(originsJSON), &result.AllowedOrigins); err != nil {
		return Project{}, err
	}
	result.ScrubRules = []byte(scrubJSON)
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'project_updated','project',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, strconv.FormatInt(result.ProjectID, 10), result.Revision, command.RequestID); err != nil {
		return Project{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Project{}, err
	}
	return result, nil
}

func trimmedText(value *string) any {
	if value == nil {
		return nil
	}
	return strings.TrimSpace(*value)
}

func nullableJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return string(value)
}
