package control

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type ManagedUser struct {
	UserID        int64
	Email         string
	State         string
	Revision      int64
	Role          *string
	ProjectGrants []ProjectGrant
}

type UserPageCommand struct {
	TenantID, ActorUserID, AfterUserID int64
	Limit                              int
}

func (operations *AuthOperations) ListUserPage(ctx context.Context, command UserPageCommand) ([]ManagedUser, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.AfterUserID < 0 || command.Limit < 1 || command.Limit > 1000 {
		return nil, errors.New("invalid user page")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT u.user_id,u.email_normalized,u.state,u.revision,m.role
		FROM memberships m JOIN users u ON u.user_id=m.user_id
		WHERE m.tenant_id=$1 AND u.user_id>$2 ORDER BY u.user_id LIMIT $3`, command.TenantID, command.AfterUserID, command.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ManagedUser, 0, command.Limit+1)
	for rows.Next() {
		var value ManagedUser
		var role string
		if err := rows.Scan(&value.UserID, &value.Email, &value.State, &value.Revision, &role); err != nil {
			return nil, err
		}
		value.Role = &role
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	userIDs := make([]int64, len(result))
	byUser := make(map[int64]*ManagedUser, len(result))
	for index := range result {
		userIDs[index] = result[index].UserID
		byUser[result[index].UserID] = &result[index]
	}
	grantRows, err := tx.Query(ctx, `SELECT user_id,project_id,role FROM project_grants WHERE tenant_id=$1 AND user_id=ANY($2) ORDER BY user_id,project_id`, command.TenantID, userIDs)
	if err != nil {
		return nil, err
	}
	for grantRows.Next() {
		var userID int64
		var grant ProjectGrant
		if err := grantRows.Scan(&userID, &grant.ProjectID, &grant.Role); err != nil {
			grantRows.Close()
			return nil, err
		}
		if user := byUser[userID]; user != nil {
			user.ProjectGrants = append(user.ProjectGrants, grant)
		}
	}
	if err := grantRows.Err(); err != nil {
		grantRows.Close()
		return nil, err
	}
	grantRows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

type CreateUserCommand struct {
	TenantID, ActorUserID    int64
	Email, PasswordPHC, Role string
	ProjectGrants            []ProjectGrant
	RequestID, AuditID       string
}

func (operations *AuthOperations) CreateUser(ctx context.Context, command CreateUserCommand) (ManagedUser, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.PasswordPHC == "" || command.RequestID == "" || command.AuditID == "" || command.Role != "admin" && command.Role != "member" {
		return ManagedUser{}, errors.New("invalid user creation")
	}
	email, err := NormalizeEmail(command.Email)
	if err != nil {
		return ManagedUser{}, err
	}
	if err := validateManagedGrants(command.Role, command.ProjectGrants); err != nil {
		return ManagedUser{}, err
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return ManagedUser{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return ManagedUser{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id=$1 FOR UPDATE`, command.TenantID); err != nil {
		return ManagedUser{}, err
	}
	var existing int64
	err = tx.QueryRow(ctx, `SELECT user_id FROM users WHERE email_normalized=$1`, email).Scan(&existing)
	if err == nil {
		return ManagedUser{}, ErrRevisionConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ManagedUser{}, err
	}
	var result ManagedUser
	if err := tx.QueryRow(ctx, `INSERT INTO users(email_normalized,password_phc) VALUES($1,$2)
		RETURNING user_id,email_normalized,state,revision`, email, command.PasswordPHC).Scan(&result.UserID, &result.Email, &result.State, &result.Revision); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ManagedUser{}, ErrRevisionConflict
		}
		return ManagedUser{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,$3)`, command.TenantID, result.UserID, command.Role); err != nil {
		return ManagedUser{}, err
	}
	if err := replaceProjectGrants(ctx, tx, command.TenantID, result.UserID, command.Role, command.ProjectGrants); err != nil {
		return ManagedUser{}, err
	}
	result.Role = &command.Role
	result.ProjectGrants = append([]ProjectGrant(nil), command.ProjectGrants...)
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'user_created','user',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, strconv.FormatInt(result.UserID, 10), result.Revision, command.RequestID); err != nil {
		return ManagedUser{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedUser{}, err
	}
	return result, nil
}

type UpdateUserCommand struct {
	TenantID, ActorUserID, TargetUserID, ExpectedRevision int64
	State, Role                                           *string
	RoleSet, GrantsSet                                    bool
	ProjectGrants                                         []ProjectGrant
	PasswordPHC                                           *string
	RequestID, AuditID                                    string
}

func (operations *AuthOperations) UpdateUser(ctx context.Context, command UpdateUserCommand) (ManagedUser, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.TargetUserID <= 0 || command.ExpectedRevision <= 0 || command.RequestID == "" || command.AuditID == "" {
		return ManagedUser{}, errors.New("invalid user update")
	}
	if command.State != nil && *command.State != "active" && *command.State != "disabled" {
		return ManagedUser{}, errors.New("invalid user state")
	}
	if command.RoleSet && command.Role != nil && *command.Role != "admin" && *command.Role != "member" {
		return ManagedUser{}, errors.New("invalid membership role")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return ManagedUser{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return ManagedUser{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id=$1 FOR UPDATE`, command.TenantID); err != nil {
		return ManagedUser{}, err
	}
	var result ManagedUser
	var installationAdmin bool
	if err := tx.QueryRow(ctx, `SELECT user_id,email_normalized,state,revision,is_installation_admin FROM users WHERE user_id=$1 FOR UPDATE`, command.TargetUserID).Scan(&result.UserID, &result.Email, &result.State, &result.Revision, &installationAdmin); errors.Is(err, pgx.ErrNoRows) {
		return ManagedUser{}, ErrForbidden
	} else if err != nil {
		return ManagedUser{}, err
	}
	if result.Revision != command.ExpectedRevision {
		return ManagedUser{}, ErrRevisionConflict
	}
	var currentRole string
	if err := tx.QueryRow(ctx, `SELECT role FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, command.TenantID, command.TargetUserID).Scan(&currentRole); errors.Is(err, pgx.ErrNoRows) {
		return ManagedUser{}, ErrForbidden
	} else if err != nil {
		return ManagedUser{}, err
	}
	newRole := &currentRole
	if command.RoleSet {
		newRole = command.Role
	}
	grants := command.ProjectGrants
	if !command.GrantsSet {
		grants, err = loadProjectGrants(ctx, tx, command.TenantID, command.TargetUserID)
		if err != nil {
			return ManagedUser{}, err
		}
	}
	if newRole == nil {
		grants = nil
	}
	if newRole != nil {
		if err := validateManagedGrants(*newRole, grants); err != nil {
			return ManagedUser{}, err
		}
	}
	if currentRole == "admin" && (newRole == nil || *newRole != "admin" || command.State != nil && *command.State == "disabled") {
		var admins int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM memberships m JOIN users u ON u.user_id=m.user_id WHERE m.tenant_id=$1 AND m.role='admin' AND u.state='active'`, command.TenantID).Scan(&admins); err != nil {
			return ManagedUser{}, err
		}
		if admins <= 1 {
			return ManagedUser{}, ErrLastAdmin
		}
	}
	if command.State != nil || command.PasswordPHC != nil { // pragma: allowlist secret -- PHC is an Argon2id hash
		if command.PasswordPHC != nil && command.TargetUserID == command.ActorUserID { // pragma: allowlist secret -- PHC is an Argon2id hash
			return ManagedUser{}, ErrForbidden
		}
		var memberships int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE user_id=$1`, command.TargetUserID).Scan(&memberships); err != nil {
			return ManagedUser{}, err
		}
		if memberships != 1 {
			return ManagedUser{}, ErrForbidden
		}
	}
	if installationAdmin && command.State != nil && *command.State == "disabled" {
		return ManagedUser{}, ErrForbidden
	}
	if newRole == nil {
		if _, err := tx.Exec(ctx, `DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, command.TenantID, command.TargetUserID); err != nil {
			return ManagedUser{}, err
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE memberships SET role=$3,revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND user_id=$2`, command.TenantID, command.TargetUserID, *newRole); err != nil {
			return ManagedUser{}, err
		}
		if err := replaceProjectGrants(ctx, tx, command.TenantID, command.TargetUserID, *newRole, grants); err != nil {
			return ManagedUser{}, err
		}
	}
	if command.State != nil {
		result.State = *command.State
	}
	credentialBump := command.PasswordPHC != nil // pragma: allowlist secret -- PHC is an Argon2id hash
	if credentialBump {
		if _, err := tx.Exec(ctx, `UPDATE users SET state=$2,password_phc=$3,credential_revision=credential_revision+1,auth_revision=auth_revision+1,revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1`, command.TargetUserID, result.State, *command.PasswordPHC); err != nil {
			return ManagedUser{}, err
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE users SET state=$2,auth_revision=auth_revision+1,revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1`, command.TargetUserID, result.State); err != nil {
			return ManagedUser{}, err
		}
	}
	if command.State != nil && *command.State == "disabled" || credentialBump {
		if _, err := tx.Exec(ctx, `UPDATE sessions SET revoked_at=coalesce(revoked_at,clock_timestamp()) WHERE user_id=$1`, command.TargetUserID); err != nil {
			return ManagedUser{}, err
		}
	}
	result.Revision++
	result.Role, result.ProjectGrants = newRole, grants
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'membership_updated','user',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, strconv.FormatInt(command.TargetUserID, 10), result.Revision, command.RequestID); err != nil {
		return ManagedUser{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedUser{}, err
	}
	return result, nil
}

func validateManagedGrants(role string, grants []ProjectGrant) error {
	if role == "admin" && len(grants) != 0 {
		return errors.New("admin membership cannot carry project grants")
	}
	seen := map[int64]bool{}
	for _, grant := range grants {
		if grant.ProjectID <= 0 || seen[grant.ProjectID] || grant.Role != "operator" && grant.Role != "viewer" {
			return errors.New("invalid project grants")
		}
		seen[grant.ProjectID] = true
	}
	return nil
}

func replaceProjectGrants(ctx context.Context, tx pgx.Tx, tenantID, userID int64, role string, grants []ProjectGrant) error {
	if _, err := tx.Exec(ctx, `DELETE FROM project_grants WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		return err
	}
	for _, grant := range grants {
		result, err := tx.Exec(ctx, `INSERT INTO project_grants(tenant_id,project_id,user_id,role)
			SELECT $1,project_id,$2,$3 FROM projects WHERE tenant_id=$1 AND project_id=$4`, tenantID, userID, grant.Role, grant.ProjectID)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrForbidden
		}
	}
	return nil
}

func loadProjectGrants(ctx context.Context, tx pgx.Tx, tenantID, userID int64) ([]ProjectGrant, error) {
	rows, err := tx.Query(ctx, `SELECT project_id,role FROM project_grants WHERE tenant_id=$1 AND user_id=$2 ORDER BY project_id`, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []ProjectGrant{}
	for rows.Next() {
		var grant ProjectGrant
		if err := rows.Scan(&grant.ProjectID, &grant.Role); err != nil {
			return nil, err
		}
		result = append(result, grant)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ProjectID < result[j].ProjectID })
	return result, rows.Err()
}
