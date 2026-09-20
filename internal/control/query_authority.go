package control

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
)

type durableQueryScope struct {
	SnapshotID        string
	TenantID          int64
	UserID            int64
	PrincipalHash     string
	AuthRevision      int64
	TenantRevision    int64
	StorageGeneration int64
	ExpiresAt         time.Time
	MaxUntil          time.Time
	ProjectIDs        []int64
	ProjectRevisions  []int64
}

// lockDurableQueryScope is the worker-side authorization linearization point.
// It needs no browser secret: a user snapshot must still correspond to one live
// session, while an alert snapshot must still correspond to its enabled rule.
// Every captured authority revision is rechecked under shared locks before a
// task result can become authoritative.
func lockDurableQueryScope(ctx context.Context, tx pgx.Tx, tenantID int64, snapshotID string, expectedGeneration int64) (durableQueryScope, error) {
	var scope durableQueryScope
	var setupState, recoveryState string
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT storage_generation,setup_state,recovery_state,clock_timestamp()
		FROM installations WHERE singleton FOR SHARE`).Scan(&scope.StorageGeneration, &setupState, &recoveryState, &now); err != nil {
		return scope, err
	}
	if scope.StorageGeneration != expectedGeneration || setupState != "ready" || recoveryState != "ready" {
		return scope, ErrStorageGeneration
	}
	var state string
	var principalKind string
	var userID *int64
	if err := tx.QueryRow(ctx, `SELECT snapshot_id::text,tenant_id,user_id,principal_kind,principal_ref,auth_revision,tenant_auth_revision,
		storage_generation,expires_at,max_until,state FROM query_snapshots
		WHERE tenant_id=$1 AND snapshot_id=$2`, tenantID, snapshotID).Scan(
		&scope.SnapshotID, &scope.TenantID, &userID, &principalKind, &scope.PrincipalHash, &scope.AuthRevision,
		&scope.TenantRevision, &scope.StorageGeneration, &scope.ExpiresAt, &scope.MaxUntil, &state); err != nil {
		return scope, errors.Join(ErrSnapshotExpired, err)
	}
	if state != "active" || !scope.ExpiresAt.After(now) || scope.StorageGeneration != expectedGeneration {
		return scope, ErrSnapshotExpired
	}
	var tenantState, userState, role string
	var tenantRevision, userRevision int64
	if err := tx.QueryRow(ctx, `SELECT state,auth_revision FROM tenants WHERE tenant_id=$1 FOR SHARE`, tenantID).Scan(&tenantState, &tenantRevision); err != nil {
		return scope, errors.Join(ErrForbidden, err)
	}
	if tenantState != "active" || tenantRevision != scope.TenantRevision {
		return scope, ErrForbidden
	}
	if principalKind == "user" {
		if userID == nil {
			return scope, ErrForbidden
		}
		scope.UserID = *userID
		if err := tx.QueryRow(ctx, `SELECT u.state,u.auth_revision,m.role
		FROM tenants t JOIN memberships m ON m.tenant_id=t.tenant_id
		JOIN users u ON u.user_id=m.user_id
		WHERE t.tenant_id=$1 AND u.user_id=$2 FOR SHARE OF t,u,m`, tenantID, scope.UserID).Scan(
			&userState, &userRevision, &role); err != nil {
			return scope, errors.Join(ErrForbidden, err)
		}
		if userState != "active" || userRevision != scope.AuthRevision {
			return scope, ErrForbidden
		}
		sessionRows, err := tx.Query(ctx, `SELECT token_hash FROM sessions
		WHERE user_id=$1 AND revoked_at IS NULL AND expires_at>clock_timestamp()
		AND storage_generation=$2 FOR SHARE`, scope.UserID, expectedGeneration)
		if err != nil {
			return scope, err
		}
		principalLive, sessionCount := false, 0
		for sessionRows.Next() {
			var tokenHash []byte
			if err := sessionRows.Scan(&tokenHash); err != nil {
				sessionRows.Close()
				return scope, err
			}
			sessionCount++
			if len(tokenHash) == 32 {
				var fixed [32]byte
				copy(fixed[:], tokenHash)
				principalLive = principalLive || model.QueryPrincipalHash(scope.UserID, fixed) == scope.PrincipalHash
			}
			if sessionCount > 100 {
				sessionRows.Close()
				return scope, ErrQueryLimitExceeded
			}
		}
		if err := sessionRows.Err(); err != nil {
			sessionRows.Close()
			return scope, err
		}
		sessionRows.Close()
		if !principalLive {
			return scope, ErrForbidden
		}
	} else if principalKind == "alert" {
		if userID != nil {
			return scope, ErrForbidden
		}
	} else {
		return scope, ErrForbidden
	}
	rows, err := tx.Query(ctx, `SELECT sp.project_id,sp.project_auth_revision,p.auth_revision,p.state
		FROM snapshot_projects sp JOIN projects p ON p.tenant_id=sp.tenant_id AND p.project_id=sp.project_id
		WHERE sp.tenant_id=$1 AND sp.snapshot_id=$2 ORDER BY sp.project_id FOR SHARE OF p`, tenantID, snapshotID)
	if err != nil {
		return scope, err
	}
	for rows.Next() {
		var projectID, capturedRevision, currentRevision int64
		var projectState string
		if err := rows.Scan(&projectID, &capturedRevision, &currentRevision, &projectState); err != nil {
			rows.Close()
			return scope, err
		}
		if projectState != "active" || capturedRevision != currentRevision {
			rows.Close()
			return scope, ErrForbidden
		}
		scope.ProjectIDs = append(scope.ProjectIDs, projectID)
		scope.ProjectRevisions = append(scope.ProjectRevisions, capturedRevision)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return scope, err
	}
	rows.Close()
	if len(scope.ProjectIDs) == 0 {
		return scope, ErrForbidden
	}
	if principalKind == "alert" {
		var revision int64
		var enabled, paused bool
		if err := tx.QueryRow(ctx, `SELECT a.revision,a.enabled,i.alerts_paused FROM alerts a JOIN installations i ON i.singleton WHERE a.tenant_id=$1 AND a.alert_id::text=$2 FOR SHARE OF a`, tenantID, scope.PrincipalHash).Scan(&revision, &enabled, &paused); err != nil {
			return scope, errors.Join(ErrForbidden, err)
		}
		if !enabled || paused || revision != scope.AuthRevision {
			return scope, ErrForbidden
		}
	}
	if principalKind == "user" && role != "admin" {
		grantRows, err := tx.Query(ctx, `SELECT project_id FROM project_grants
			WHERE tenant_id=$1 AND user_id=$2 AND project_id=ANY($3::bigint[])
			ORDER BY project_id FOR SHARE`, tenantID, scope.UserID, scope.ProjectIDs)
		if err != nil {
			return scope, err
		}
		granted := 0
		for grantRows.Next() {
			var projectID int64
			if err := grantRows.Scan(&projectID); err != nil || granted >= len(scope.ProjectIDs) || projectID != scope.ProjectIDs[granted] {
				grantRows.Close()
				return scope, ErrForbidden
			}
			granted++
		}
		if err := grantRows.Err(); err != nil {
			grantRows.Close()
			return scope, err
		}
		grantRows.Close()
		if granted != len(scope.ProjectIDs) {
			return scope, ErrForbidden
		}
	}
	var finalState string
	var finalPrincipal string
	var finalUser *int64
	var finalKind string
	var finalAuth, finalTenantAuth, finalGeneration int64
	var finalExpires time.Time
	if err := tx.QueryRow(ctx, `SELECT user_id,principal_kind,principal_ref,auth_revision,tenant_auth_revision,storage_generation,expires_at,state
		FROM query_snapshots WHERE tenant_id=$1 AND snapshot_id=$2 FOR SHARE`, tenantID, snapshotID).Scan(
		&finalUser, &finalKind, &finalPrincipal, &finalAuth, &finalTenantAuth, &finalGeneration, &finalExpires, &finalState); err != nil {
		return scope, err
	}
	if finalState != "active" || !finalExpires.After(now) || finalKind != principalKind || principalKind == "user" && (finalUser == nil || *finalUser != scope.UserID) || principalKind == "alert" && finalUser != nil || finalPrincipal != scope.PrincipalHash || finalAuth != scope.AuthRevision || finalTenantAuth != scope.TenantRevision || finalGeneration != expectedGeneration {
		return scope, ErrForbidden
	}
	return scope, nil
}
