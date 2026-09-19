package control

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrSetupComplete    = errors.New("installation setup is complete")
	ErrSetupInProgress  = errors.New("installation setup is in progress")
	ErrSetupConflict    = errors.New("installation setup request conflicts with the reserved request")
	ErrInvalidBootstrap = errors.New("invalid bootstrap token")
	ErrUnauthenticated  = errors.New("session is not authenticated")
	ErrForbidden        = errors.New("principal is forbidden")
	ErrLoginLimited     = errors.New("login rate limit exceeded")
	ErrLastAdmin        = errors.New("last active tenant admin cannot be removed")
	ErrRevisionConflict = errors.New("resource revision conflicts with current state")
)

type SetupState string

const (
	SetupUninitialized SetupState = "uninitialized"
	SetupProvisioning  SetupState = "provisioning"
	SetupReady         SetupState = "ready"
)

type AuthOperations struct{ pool *pgxpool.Pool }

func NewAuthOperations(pool *pgxpool.Pool) (*AuthOperations, error) {
	if pool == nil {
		return nil, errors.New("PostgreSQL pool is required")
	}
	return &AuthOperations{pool: pool}, nil
}

type SetupStatus struct {
	State      SetupState
	RetryAfter time.Duration
}

type SetupDescriptor struct {
	InstallationID  string
	StorageIdentity string
}

type SetupAuthority struct {
	Attempt        string
	Owner          string
	Fence          int64
	InstallationID string
	MarkerKey      string
	MarkerSHA256   string
}

type SetupReservation struct {
	Authority         SetupAuthority
	StorageIdentity   string
	StorageGeneration int64
}

type SetupFinalize struct {
	Authority        SetupAuthority
	EmailNormalized  string
	PasswordPHC      string
	TenantName       string
	SessionTokenHash [32]byte
	CSRFHash         [32]byte
	SessionExpiresAt time.Time
	RequestID        string
	AuditID          string
}

type SessionTenant struct {
	TenantID      int64
	Name          string
	Role          string
	Revision      int64
	ProjectGrants []ProjectGrant
}

type ProjectGrant struct {
	ProjectID int64
	Role      string
}

type SessionPrincipal struct {
	UserID             int64
	Email              string
	AuthRevision       int64
	CredentialRevision int64
	InstallationAdmin  bool
	ExpiresAt          time.Time
	CSRFHash           [32]byte
	Tenants            []SessionTenant
}

type LoginCredential struct {
	UserID             int64
	PasswordPHC        string
	CredentialRevision int64
	Active             bool
}

func NormalizeEmail(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if len(normalized) < 3 || len(normalized) > 320 || strings.Count(normalized, "@") != 1 {
		return "", errors.New("invalid email")
	}
	return normalized, nil
}

func (operations *AuthOperations) EnsureSetupInstallation(ctx context.Context, installationID, storageIdentity string, bootstrapHash [32]byte) error {
	if operations == nil || operations.pool == nil || installationID == "" || storageIdentity == "" || bootstrapHash == ([32]byte{}) {
		return errors.New("invalid setup installation")
	}
	result, err := operations.pool.Exec(ctx, `INSERT INTO installations(
		singleton,installation_id,storage_generation,schema_version,storage_identity,setup_state,bootstrap_token_hash,setup_completed_at)
		VALUES(true,$1,1,$2,$3,'uninitialized',$4,NULL) ON CONFLICT(singleton) DO NOTHING`,
		installationID, model.SchemaVersion, storageIdentity, bootstrapHash[:])
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var existingIdentity string
	var state SetupState
	var existingHash []byte
	if err := operations.pool.QueryRow(ctx, `SELECT storage_identity,setup_state,bootstrap_token_hash FROM installations WHERE singleton`).Scan(&existingIdentity, &state, &existingHash); err != nil {
		return err
	}
	if existingIdentity != storageIdentity {
		return errors.New("configured S3 storage identity does not match PostgreSQL authority")
	}
	if state != SetupReady && subtle.ConstantTimeCompare(existingHash, bootstrapHash[:]) != 1 {
		return ErrInvalidBootstrap
	}
	return nil
}

func (operations *AuthOperations) SetupStatus(ctx context.Context) (SetupStatus, error) {
	var result SetupStatus
	var seconds int64
	err := operations.pool.QueryRow(ctx, `SELECT setup_state,
		CASE WHEN setup_state='provisioning' THEN greatest(0,ceil(extract(epoch FROM setup_lease_until-clock_timestamp())))::bigint ELSE 0 END
		FROM installations WHERE singleton`).Scan(&result.State, &seconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrInvalidBootstrap
	}
	result.RetryAfter = time.Duration(seconds) * time.Second
	return result, err
}

func (operations *AuthOperations) SetupDescriptor(ctx context.Context) (SetupDescriptor, error) {
	var result SetupDescriptor
	err := operations.pool.QueryRow(ctx, `SELECT installation_id::text,storage_identity FROM installations WHERE singleton`).Scan(&result.InstallationID, &result.StorageIdentity)
	return result, err
}

func (operations *AuthOperations) ReserveSetup(ctx context.Context, bootstrapHash, fingerprint [32]byte, attempt, owner, markerKey, markerSHA string, lease time.Duration) (SetupReservation, error) {
	if attempt == "" || owner == "" || markerKey == "" || !validSHA(markerSHA) || lease <= 0 || lease > time.Minute {
		return SetupReservation{}, errors.New("invalid setup reservation")
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return SetupReservation{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return SetupReservation{}, err
	}
	var state SetupState
	var installationID, storageIdentity string
	var storedAttempt *string
	var storageGeneration, fence int64
	var storedBootstrap, storedFingerprint []byte
	var leaseLive bool
	err = tx.QueryRow(ctx, `SELECT setup_state,installation_id::text,storage_identity,storage_generation,setup_fence,
		setup_attempt::text,bootstrap_token_hash,setup_request_fingerprint,coalesce(setup_lease_until > clock_timestamp(),false)
		FROM installations WHERE singleton FOR UPDATE`).Scan(&state, &installationID, &storageIdentity, &storageGeneration, &fence, &storedAttempt, &storedBootstrap, &storedFingerprint, &leaseLive)
	if err != nil {
		return SetupReservation{}, err
	}
	if state == SetupReady {
		return SetupReservation{}, ErrSetupComplete
	}
	if subtle.ConstantTimeCompare(storedBootstrap, bootstrapHash[:]) != 1 {
		return SetupReservation{}, ErrInvalidBootstrap
	}
	if state == SetupProvisioning {
		if subtle.ConstantTimeCompare(storedFingerprint, fingerprint[:]) != 1 {
			return SetupReservation{}, ErrSetupConflict
		}
		if leaseLive {
			return SetupReservation{}, ErrSetupInProgress
		}
		if storedAttempt == nil || *storedAttempt == "" {
			return SetupReservation{}, ErrSetupConflict
		}
		attempt = *storedAttempt
		fence++
	} else {
		fence = 1
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET setup_state='provisioning',setup_attempt=$1,
		setup_request_fingerprint=$2,setup_marker_key=$3,setup_marker_sha256=$4,setup_owner=$5,
		setup_fence=$6,setup_lease_until=clock_timestamp()+$7::interval WHERE singleton`,
		attempt, fingerprint[:], markerKey, markerSHA, owner, fence, fmt.Sprintf("%f seconds", lease.Seconds())); err != nil {
		return SetupReservation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SetupReservation{}, err
	}
	return SetupReservation{Authority: SetupAuthority{Attempt: attempt, Owner: owner, Fence: fence, InstallationID: installationID, MarkerKey: markerKey, MarkerSHA256: markerSHA}, StorageIdentity: storageIdentity, StorageGeneration: storageGeneration}, nil
}

func (operations *AuthOperations) HeartbeatSetup(ctx context.Context, authority SetupAuthority, lease time.Duration) error {
	if authority.Attempt == "" || authority.Owner == "" || authority.Fence <= 0 || lease <= 0 || lease > time.Minute {
		return errors.New("invalid setup heartbeat")
	}
	result, err := operations.pool.Exec(ctx, `UPDATE installations SET setup_lease_until=clock_timestamp()+$4::interval
		WHERE singleton AND setup_state='provisioning' AND setup_attempt=$1 AND setup_owner=$2 AND setup_fence=$3
		AND setup_lease_until > clock_timestamp()`, authority.Attempt, authority.Owner, authority.Fence, fmt.Sprintf("%f seconds", lease.Seconds()))
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrSetupConflict
	}
	return nil
}

func (operations *AuthOperations) FinalizeSetup(ctx context.Context, command SetupFinalize) (SessionPrincipal, error) {
	if command.Authority.Attempt == "" || command.Authority.Owner == "" || command.Authority.Fence <= 0 || command.EmailNormalized == "" || command.PasswordPHC == "" || strings.TrimSpace(command.TenantName) == "" || command.SessionExpiresAt.IsZero() || command.RequestID == "" || command.AuditID == "" {
		return SessionPrincipal{}, errors.New("invalid setup finalization")
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return SessionPrincipal{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return SessionPrincipal{}, err
	}
	var generation int64
	err = tx.QueryRow(ctx, `SELECT storage_generation FROM installations WHERE singleton AND setup_state='provisioning'
		AND setup_attempt=$1 AND setup_owner=$2 AND setup_fence=$3 AND setup_lease_until > clock_timestamp() FOR UPDATE`,
		command.Authority.Attempt, command.Authority.Owner, command.Authority.Fence).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return SessionPrincipal{}, ErrSetupConflict
	}
	if err != nil {
		return SessionPrincipal{}, err
	}
	var userID, tenantID int64
	if err := tx.QueryRow(ctx, `INSERT INTO users(email_normalized,password_phc,is_installation_admin)
		VALUES($1,$2,true) RETURNING user_id`, command.EmailNormalized, command.PasswordPHC).Scan(&userID); err != nil {
		return SessionPrincipal{}, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO tenants(name) VALUES($1) RETURNING tenant_id`, strings.TrimSpace(command.TenantName)).Scan(&tenantID); err != nil {
		return SessionPrincipal{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'admin')`, tenantID, userID); err != nil {
		return SessionPrincipal{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO lanes(tenant_id,lane_id) SELECT $1,generate_series(0,15)`, tenantID); err != nil {
		return SessionPrincipal{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,csrf_hash,credential_revision,storage_generation,expires_at)
		VALUES($1,$2,$3,1,$4,$5)`, command.SessionTokenHash[:], userID, command.CSRFHash[:], generation, command.SessionExpiresAt); err != nil {
		return SessionPrincipal{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id)
		VALUES($1,$2,$3,'setup','installation',$4,1,$5)`, tenantID, command.AuditID, userID, command.Authority.InstallationID, command.RequestID); err != nil {
		return SessionPrincipal{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET setup_state='ready',setup_attempt=NULL,setup_request_fingerprint=NULL,
		setup_owner=NULL,setup_lease_until=NULL,bootstrap_token_hash=NULL,setup_completed_at=clock_timestamp(),
		retention_tick_at=clock_timestamp() WHERE singleton`); err != nil {
		return SessionPrincipal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SessionPrincipal{}, err
	}
	return SessionPrincipal{UserID: userID, Email: command.EmailNormalized, AuthRevision: 1, CredentialRevision: 1, InstallationAdmin: true, ExpiresAt: command.SessionExpiresAt, CSRFHash: command.CSRFHash, Tenants: []SessionTenant{{TenantID: tenantID, Name: strings.TrimSpace(command.TenantName), Role: "admin", Revision: 1, ProjectGrants: []ProjectGrant{}}}}, nil
}

func (operations *AuthOperations) LoadLoginCredential(ctx context.Context, email string) (LoginCredential, bool, error) {
	var credential LoginCredential
	var state string
	err := operations.pool.QueryRow(ctx, `SELECT user_id,password_phc,credential_revision,state FROM users WHERE email_normalized=$1`, email).Scan(&credential.UserID, &credential.PasswordPHC, &credential.CredentialRevision, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return LoginCredential{}, false, nil
	}
	credential.Active = state == "active"
	return credential, true, err
}

func (operations *AuthOperations) LoadUserCredential(ctx context.Context, userID int64) (LoginCredential, error) {
	var credential LoginCredential
	var state string
	err := operations.pool.QueryRow(ctx, `SELECT user_id,password_phc,credential_revision,state FROM users WHERE user_id=$1`, userID).Scan(&credential.UserID, &credential.PasswordPHC, &credential.CredentialRevision, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return LoginCredential{}, ErrUnauthenticated
	}
	credential.Active = state == "active"
	return credential, err
}

func (operations *AuthOperations) ChangePassword(ctx context.Context, userID, observedCredentialRevision int64, passwordPHC string) error {
	if userID <= 0 || observedCredentialRevision <= 0 || passwordPHC == "" {
		return errors.New("invalid password change")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var current int64
	var state string
	if err := tx.QueryRow(ctx, `SELECT credential_revision,state FROM users WHERE user_id=$1 FOR UPDATE`, userID).Scan(&current, &state); err != nil {
		return ErrUnauthenticated
	}
	if state != "active" || current != observedCredentialRevision {
		return ErrRevisionConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE users SET password_phc=$2,credential_revision=credential_revision+1,
		auth_revision=auth_revision+1,revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1`, userID, passwordPHC); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET revoked_at=coalesce(revoked_at,clock_timestamp()) WHERE user_id=$1`, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (operations *AuthOperations) ConsumeLoginLimits(ctx context.Context, buckets [][32]byte, limits []int) error {
	if len(buckets) == 0 || len(buckets) != len(limits) {
		return errors.New("invalid login limits")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM login_limits WHERE ctid IN (SELECT ctid FROM login_limits WHERE expires_at <= clock_timestamp() LIMIT 1000)`); err != nil {
		return err
	}
	limited := false
	for index, bucket := range buckets {
		if limits[index] <= 0 {
			return errors.New("invalid login limit")
		}
		var count int
		err := tx.QueryRow(ctx, `INSERT INTO login_limits(bucket_hash,window_start,count,expires_at)
			VALUES($1,date_trunc('minute',clock_timestamp()),1,date_trunc('minute',clock_timestamp())+interval '1 hour')
			ON CONFLICT(bucket_hash,window_start) DO UPDATE SET count=login_limits.count+1 RETURNING count`, bucket[:]).Scan(&count)
		if err != nil {
			return err
		}
		limited = limited || count > limits[index]
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if limited {
		return ErrLoginLimited
	}
	return nil
}

func (operations *AuthOperations) CreateSession(ctx context.Context, credential LoginCredential, tokenHash, csrfHash [32]byte, expiresAt time.Time) (SessionPrincipal, error) {
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return SessionPrincipal{}, err
	}
	defer tx.Rollback(ctx)
	var generation int64
	var setupState, recoveryState string
	if err := tx.QueryRow(ctx, `SELECT storage_generation,setup_state,recovery_state FROM installations WHERE singleton FOR SHARE`).Scan(&generation, &setupState, &recoveryState); err != nil {
		return SessionPrincipal{}, err
	}
	if setupState != string(SetupReady) || recoveryState != "ready" {
		return SessionPrincipal{}, ErrUnauthenticated
	}
	var revision int64
	var state string
	if err := tx.QueryRow(ctx, `SELECT credential_revision,state FROM users WHERE user_id=$1 FOR UPDATE`, credential.UserID).Scan(&revision, &state); err != nil {
		return SessionPrincipal{}, ErrUnauthenticated
	}
	if state != "active" || revision != credential.CredentialRevision {
		return SessionPrincipal{}, ErrUnauthenticated
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,csrf_hash,credential_revision,storage_generation,expires_at)
		VALUES($1,$2,$3,$4,$5,$6)`, tokenHash[:], credential.UserID, csrfHash[:], revision, generation, expiresAt); err != nil {
		return SessionPrincipal{}, err
	}
	principal, err := loadPrincipal(ctx, tx, tokenHash[:], false)
	if err != nil {
		return SessionPrincipal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SessionPrincipal{}, err
	}
	return principal, nil
}

func (operations *AuthOperations) AuthenticateSession(ctx context.Context, tokenHash [32]byte) (SessionPrincipal, error) {
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return SessionPrincipal{}, err
	}
	defer tx.Rollback(ctx)
	principal, err := loadPrincipal(ctx, tx, tokenHash[:], true)
	if err != nil {
		return SessionPrincipal{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET last_seen_at=clock_timestamp() WHERE token_hash=$1`, tokenHash[:]); err != nil {
		return SessionPrincipal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SessionPrincipal{}, err
	}
	return principal, nil
}

type authQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadPrincipal(ctx context.Context, queryer authQueryer, tokenHash []byte, lock bool) (SessionPrincipal, error) {
	var principal SessionPrincipal
	var csrf []byte
	query := `SELECT u.user_id,u.email_normalized,u.auth_revision,u.credential_revision,u.is_installation_admin,s.expires_at,s.csrf_hash
		FROM sessions s JOIN users u ON u.user_id=s.user_id JOIN installations i ON i.singleton
		WHERE s.token_hash=$1 AND s.revoked_at IS NULL AND s.expires_at>clock_timestamp() AND u.state='active'
		AND s.credential_revision=u.credential_revision AND s.storage_generation=i.storage_generation
		AND i.setup_state='ready' AND i.recovery_state='ready'`
	if lock {
		query += ` FOR UPDATE OF s`
	}
	if err := queryer.QueryRow(ctx, query, tokenHash).Scan(&principal.UserID, &principal.Email, &principal.AuthRevision, &principal.CredentialRevision, &principal.InstallationAdmin, &principal.ExpiresAt, &csrf); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SessionPrincipal{}, ErrUnauthenticated
		}
		return SessionPrincipal{}, err
	}
	copy(principal.CSRFHash[:], csrf)
	rows, err := queryer.Query(ctx, `SELECT m.tenant_id,t.name,m.role,m.revision,g.project_id,g.role
		FROM memberships m JOIN tenants t ON t.tenant_id=m.tenant_id
		LEFT JOIN project_grants g ON g.tenant_id=m.tenant_id AND g.user_id=m.user_id
		WHERE m.user_id=$1 AND t.state='active' ORDER BY m.tenant_id,g.project_id`, principal.UserID)
	if err != nil {
		return SessionPrincipal{}, err
	}
	defer rows.Close()
	byTenant := map[int64]int{}
	for rows.Next() {
		var tenantID, revision int64
		var name, role string
		var projectID *int64
		var projectRole *string
		if err := rows.Scan(&tenantID, &name, &role, &revision, &projectID, &projectRole); err != nil {
			return SessionPrincipal{}, err
		}
		index, exists := byTenant[tenantID]
		if !exists {
			index = len(principal.Tenants)
			byTenant[tenantID] = index
			principal.Tenants = append(principal.Tenants, SessionTenant{TenantID: tenantID, Name: name, Role: role, Revision: revision, ProjectGrants: []ProjectGrant{}})
		}
		if projectID != nil {
			principal.Tenants[index].ProjectGrants = append(principal.Tenants[index].ProjectGrants, ProjectGrant{ProjectID: *projectID, Role: *projectRole})
		}
	}
	return principal, rows.Err()
}

func (operations *AuthOperations) RevokeSession(ctx context.Context, tokenHash [32]byte) error {
	result, err := operations.pool.Exec(ctx, `UPDATE sessions SET revoked_at=coalesce(revoked_at,clock_timestamp()) WHERE token_hash=$1`, tokenHash[:])
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrUnauthenticated
	}
	return nil
}

type ProjectScope struct {
	TenantID     int64
	UserID       int64
	AuthRevision int64
	ProjectIDs   []int64
}

func (operations *AuthOperations) AuthorizeProjects(ctx context.Context, tokenHash [32]byte, tenantID int64, projectIDs []int64, requireOperator bool) (ProjectScope, error) {
	principal, err := operations.AuthenticateSession(ctx, tokenHash)
	if err != nil {
		return ProjectScope{}, err
	}
	requested := append([]int64(nil), projectIDs...)
	sort.Slice(requested, func(i, j int) bool { return requested[i] < requested[j] })
	for index, id := range requested {
		if id <= 0 || (index > 0 && requested[index-1] == id) {
			return ProjectScope{}, ErrForbidden
		}
	}
	var membership *SessionTenant
	for index := range principal.Tenants {
		if principal.Tenants[index].TenantID == tenantID {
			membership = &principal.Tenants[index]
			break
		}
	}
	if membership == nil {
		return ProjectScope{}, ErrForbidden
	}
	if membership.Role != "admin" {
		roles := make(map[int64]string, len(membership.ProjectGrants))
		for _, grant := range membership.ProjectGrants {
			roles[grant.ProjectID] = grant.Role
		}
		for _, projectID := range requested {
			role, ok := roles[projectID]
			if !ok || (requireOperator && role != "operator") {
				return ProjectScope{}, ErrForbidden
			}
		}
	}
	if len(requested) > 0 {
		var count int
		if err := operations.pool.QueryRow(ctx, `SELECT count(*) FROM projects WHERE tenant_id=$1 AND state='active' AND project_id=ANY($2)`, tenantID, requested).Scan(&count); err != nil {
			return ProjectScope{}, err
		}
		if count != len(requested) {
			return ProjectScope{}, ErrForbidden
		}
	}
	return ProjectScope{TenantID: tenantID, UserID: principal.UserID, AuthRevision: principal.AuthRevision, ProjectIDs: requested}, nil
}

type MembershipUpdate struct {
	TenantID         int64
	TargetUserID     int64
	ExpectedRevision int64
	Role             *string
	ProjectGrants    []ProjectGrant
}

func (operations *AuthOperations) ReplaceMembership(ctx context.Context, command MembershipUpdate) (int64, error) {
	if command.TenantID <= 0 || command.TargetUserID <= 0 || command.ExpectedRevision <= 0 {
		return 0, errors.New("invalid membership update")
	}
	if command.Role != nil && *command.Role != "admin" && *command.Role != "member" {
		return 0, errors.New("invalid membership role")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id=$1 FOR UPDATE`, command.TenantID); err != nil {
		return 0, err
	}
	var currentRole string
	var revision int64
	if err := tx.QueryRow(ctx, `SELECT role,revision FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, command.TenantID, command.TargetUserID).Scan(&currentRole, &revision); err != nil {
		return 0, err
	}
	if revision != command.ExpectedRevision {
		return 0, ErrRevisionConflict
	}
	if currentRole == "admin" && (command.Role == nil || *command.Role != "admin") {
		var admins int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM memberships m JOIN users u ON u.user_id=m.user_id WHERE m.tenant_id=$1 AND m.role='admin' AND u.state='active'`, command.TenantID).Scan(&admins); err != nil {
			return 0, err
		}
		if admins <= 1 {
			return 0, ErrLastAdmin
		}
	}
	if command.Role == nil {
		if _, err := tx.Exec(ctx, `DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, command.TenantID, command.TargetUserID); err != nil {
			return 0, err
		}
	} else {
		if *command.Role == "admin" && len(command.ProjectGrants) != 0 {
			return 0, errors.New("admin membership cannot carry project grants")
		}
		if _, err := tx.Exec(ctx, `DELETE FROM project_grants WHERE tenant_id=$1 AND user_id=$2`, command.TenantID, command.TargetUserID); err != nil {
			return 0, err
		}
		seen := map[int64]bool{}
		for _, grant := range command.ProjectGrants {
			if grant.ProjectID <= 0 || seen[grant.ProjectID] || (grant.Role != "operator" && grant.Role != "viewer") {
				return 0, errors.New("invalid project grants")
			}
			seen[grant.ProjectID] = true
			result, err := tx.Exec(ctx, `INSERT INTO project_grants(tenant_id,project_id,user_id,role)
				SELECT $1,p.project_id,$2,$3 FROM projects p WHERE p.tenant_id=$1 AND p.project_id=$4`, command.TenantID, command.TargetUserID, grant.Role, grant.ProjectID)
			if err != nil {
				return 0, err
			}
			if result.RowsAffected() != 1 {
				return 0, ErrForbidden
			}
		}
		revision++
		if _, err := tx.Exec(ctx, `UPDATE memberships SET role=$3,revision=$4,updated_at=clock_timestamp() WHERE tenant_id=$1 AND user_id=$2`, command.TenantID, command.TargetUserID, *command.Role, revision); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE users SET auth_revision=auth_revision+1,revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1`, command.TargetUserID); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return revision, nil
}
