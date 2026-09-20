package control

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AlertOperations struct{ pool *pgxpool.Pool }

func NewAlertOperations(pool *pgxpool.Pool) (*AlertOperations, error) {
	if pool == nil {
		return nil, errors.New("PostgreSQL pool is required")
	}
	return &AlertOperations{pool: pool}, nil
}

func (operations *AlertOperations) BindEncryptionKey(ctx context.Context, keyID string) error {
	if keyID == "" {
		return errors.New("alert encryption key ID is required")
	}
	result, err := operations.pool.Exec(ctx, `UPDATE installations SET alert_encryption_key_id=$1 WHERE singleton AND alert_encryption_key_id IS NULL`, keyID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var stored string
	if err := operations.pool.QueryRow(ctx, `SELECT alert_encryption_key_id FROM installations WHERE singleton`).Scan(&stored); err != nil {
		return err
	}
	if stored != keyID {
		return errors.New("configured alert encryption key does not match PostgreSQL authority")
	}
	return nil
}

type AlertDestination struct {
	TenantID        int64
	DestinationID   string
	Name            string
	URL             string
	Revision        int64
	Enabled         bool
	HasSecret       bool
	Secret          []byte
	EncryptionKeyID string
	CreatedAt       time.Time
}

type CreateDestinationCommand struct {
	TenantID, ActorUserID    int64
	DestinationID, Name, URL string
	SecretCiphertext         []byte
	EncryptionKeyID          string
	RequestID, AuditID       string
}

func (operations *AlertOperations) CreateDestination(ctx context.Context, command CreateDestinationCommand) (AlertDestination, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.DestinationID == "" || strings.TrimSpace(command.Name) == "" || len(command.Name) > 128 || command.URL == "" || len(command.URL) > 2048 || command.RequestID == "" || command.AuditID == "" || (len(command.SecretCiphertext) == 0) != (command.EncryptionKeyID == "") {
		return AlertDestination{}, errors.New("invalid alert destination")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return AlertDestination{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return AlertDestination{}, err
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM alert_destinations WHERE tenant_id=$1`, command.TenantID).Scan(&count); err != nil {
		return AlertDestination{}, err
	}
	if count >= 1000 {
		return AlertDestination{}, errors.New("destination limit exceeded")
	}
	var result AlertDestination
	err = tx.QueryRow(ctx, `INSERT INTO alert_destinations(tenant_id,destination_id,name,url,secret_ciphertext,encryption_key_id)
		VALUES($1,$2,$3,$4,$5,$6) RETURNING tenant_id,destination_id::text,name,url,revision,enabled,
		secret_ciphertext IS NOT NULL,coalesce(secret_ciphertext,''::bytea),coalesce(encryption_key_id,''),created_at`,
		command.TenantID, command.DestinationID, strings.TrimSpace(command.Name), command.URL, nullableBytes(command.SecretCiphertext), nullableText(command.EncryptionKeyID)).Scan(
		&result.TenantID, &result.DestinationID, &result.Name, &result.URL, &result.Revision, &result.Enabled, &result.HasSecret, &result.Secret, &result.EncryptionKeyID, &result.CreatedAt)
	if err != nil {
		return AlertDestination{}, err
	}
	if err := insertAlertAudit(ctx, tx, command.TenantID, command.AuditID, command.ActorUserID, "destination_created", "destination", result.DestinationID, result.Revision, command.RequestID); err != nil {
		return AlertDestination{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AlertDestination{}, err
	}
	return result, nil
}

type UpdateDestinationCommand struct {
	TenantID, ActorUserID, ExpectedRevision int64
	DestinationID                           string
	Name, URL                               *string
	Enabled                                 *bool
	SecretSet                               bool
	SecretCiphertext                        []byte
	EncryptionKeyID                         string
	RequestID, AuditID                      string
}

func (operations *AlertOperations) UpdateDestination(ctx context.Context, command UpdateDestinationCommand) (AlertDestination, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.ExpectedRevision <= 0 || command.DestinationID == "" || command.RequestID == "" || command.AuditID == "" || command.Name != nil && (strings.TrimSpace(*command.Name) == "" || len(*command.Name) > 128) || command.URL != nil && (*command.URL == "" || len(*command.URL) > 2048) || command.SecretSet && (len(command.SecretCiphertext) == 0) != (command.EncryptionKeyID == "") {
		return AlertDestination{}, errors.New("invalid destination update")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return AlertDestination{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return AlertDestination{}, err
	}
	var current AlertDestination
	if err := tx.QueryRow(ctx, `SELECT tenant_id,destination_id::text,name,url,revision,enabled,secret_ciphertext IS NOT NULL,
		coalesce(secret_ciphertext,''::bytea),coalesce(encryption_key_id,''),created_at FROM alert_destinations
		WHERE tenant_id=$1 AND destination_id=$2 FOR UPDATE`, command.TenantID, command.DestinationID).Scan(
		&current.TenantID, &current.DestinationID, &current.Name, &current.URL, &current.Revision, &current.Enabled, &current.HasSecret, &current.Secret, &current.EncryptionKeyID, &current.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AlertDestination{}, ErrForbidden
		}
		return AlertDestination{}, err
	}
	if current.Revision != command.ExpectedRevision {
		return AlertDestination{}, ErrRevisionConflict
	}
	if command.Name != nil {
		current.Name = strings.TrimSpace(*command.Name)
	}
	if command.URL != nil {
		current.URL = *command.URL
	}
	if command.Enabled != nil {
		current.Enabled = *command.Enabled
	}
	if command.SecretSet {
		current.Secret, current.EncryptionKeyID = command.SecretCiphertext, command.EncryptionKeyID
		current.HasSecret = len(current.Secret) > 0
	}
	err = tx.QueryRow(ctx, `UPDATE alert_destinations SET name=$3,url=$4,enabled=$5,secret_ciphertext=$6,encryption_key_id=$7,
		revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND destination_id=$2
		RETURNING revision`, command.TenantID, command.DestinationID, current.Name, current.URL, current.Enabled, nullableBytes(current.Secret), nullableText(current.EncryptionKeyID)).Scan(&current.Revision)
	if err != nil {
		return AlertDestination{}, err
	}
	if !current.Enabled {
		if _, err := tx.Exec(ctx, `UPDATE deliveries SET state='canceled',owner=NULL,lease_until=NULL,error_code='destination_disabled',updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND destination_id=$2 AND state IN ('queued','running')`, command.TenantID, command.DestinationID); err != nil {
			return AlertDestination{}, err
		}
	}
	if err := insertAlertAudit(ctx, tx, command.TenantID, command.AuditID, command.ActorUserID, "destination_updated", "destination", current.DestinationID, current.Revision, command.RequestID); err != nil {
		return AlertDestination{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AlertDestination{}, err
	}
	return current, nil
}

func (operations *AlertOperations) ListDestinations(ctx context.Context, tenantID, actorUserID int64) ([]AlertDestination, bool, error) {
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	admin, err := tenantAccess(ctx, tx, tenantID, actorUserID, 0, false)
	if err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(ctx, `SELECT tenant_id,destination_id::text,name,url,revision,enabled,secret_ciphertext IS NOT NULL,created_at
		FROM alert_destinations WHERE tenant_id=$1 ORDER BY created_at,destination_id LIMIT 1001`, tenantID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	result := make([]AlertDestination, 0)
	for rows.Next() {
		var value AlertDestination
		if err := rows.Scan(&value.TenantID, &value.DestinationID, &value.Name, &value.URL, &value.Revision, &value.Enabled, &value.HasSecret, &value.CreatedAt); err != nil {
			return nil, false, err
		}
		if !admin {
			value.URL = ""
			if !value.Enabled {
				continue
			}
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(result) > 1000 {
		return nil, false, errors.New("destination limit exceeded")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return result, admin, nil
}

type AlertRule struct {
	TenantID, ProjectID                int64
	AlertID, Name                      string
	Kind                               string
	Revision                           int64
	Enabled                            bool
	RuleBytes                          []byte
	RuleSHA256, DestinationID          string
	CooldownSeconds                    int
	EnabledFromPublicCut               [model.LaneCount]int64
	EnabledAt                          time.Time
	FirstWindowEndUS                   int64
	LastCompletedEndUS, LastFiredEndUS *int64
	CreatedAt                          time.Time
}

type CreateAlertCommand struct {
	TenantID, ProjectID, ActorUserID int64
	AlertID, Name, DestinationID     string
	Kind                             string
	RuleBytes                        []byte
	RuleSHA256                       string
	CooldownSeconds                  int
	RequestID, AuditID               string
}

func (operations *AlertOperations) CreateAlert(ctx context.Context, command CreateAlertCommand) (AlertRule, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.ActorUserID <= 0 || command.AlertID == "" || strings.TrimSpace(command.Name) == "" || len(command.Name) > 128 || command.DestinationID == "" || command.CooldownSeconds < 0 || command.CooldownSeconds > 86400 || command.RequestID == "" || command.AuditID == "" || len(command.RuleBytes) < 2 || len(command.RuleBytes) > 32<<10 || !validSHA(command.RuleSHA256) || command.Kind != "issue" && command.Kind != "threshold" {
		return AlertRule{}, errors.New("invalid alert")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return AlertRule{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, false, command.ProjectID, true); err != nil {
		return AlertRule{}, err
	}
	var destinationEnabled bool
	if err := tx.QueryRow(ctx, `SELECT enabled FROM alert_destinations WHERE tenant_id=$1 AND destination_id=$2 FOR SHARE`, command.TenantID, command.DestinationID).Scan(&destinationEnabled); err != nil || !destinationEnabled {
		return AlertRule{}, errors.Join(ErrForbidden, err)
	}
	cut, err := lockPublishedCut(ctx, tx, command.TenantID)
	if err != nil {
		return AlertRule{}, err
	}
	var firstEnd any
	if command.Kind == "threshold" {
		var rule struct {
			WindowSeconds int `json:"window_seconds"`
		}
		if json.Unmarshal(command.RuleBytes, &rule) != nil || !validAlertWindow(rule.WindowSeconds) {
			return AlertRule{}, errors.New("invalid threshold window")
		}
		var nowUS int64
		if err := tx.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&nowUS); err != nil {
			return AlertRule{}, err
		}
		firstEnd = (nowUS/60_000_000+1)*60_000_000 + int64(rule.WindowSeconds)*1_000_000
	}
	var result AlertRule
	err = tx.QueryRow(ctx, `INSERT INTO alerts(tenant_id,project_id,alert_id,name,kind,rule_bytes,rule_sha256,destination_id,cooldown_seconds,enabled_from_public_cut,first_window_end_us)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING revision,enabled,enabled_at,created_at`, command.TenantID, command.ProjectID, command.AlertID, strings.TrimSpace(command.Name), command.Kind, command.RuleBytes, command.RuleSHA256, command.DestinationID, command.CooldownSeconds, cut[:], firstEnd).Scan(&result.Revision, &result.Enabled, &result.EnabledAt, &result.CreatedAt)
	if err != nil {
		return AlertRule{}, err
	}
	result.TenantID, result.ProjectID, result.AlertID, result.Name, result.Kind, result.RuleBytes, result.RuleSHA256, result.DestinationID, result.CooldownSeconds, result.EnabledFromPublicCut = command.TenantID, command.ProjectID, command.AlertID, strings.TrimSpace(command.Name), command.Kind, command.RuleBytes, command.RuleSHA256, command.DestinationID, command.CooldownSeconds, cut
	if value, ok := firstEnd.(int64); ok {
		result.FirstWindowEndUS = value
	}
	if err := insertAlertAudit(ctx, tx, command.TenantID, command.AuditID, command.ActorUserID, "alert_created", "alert", result.AlertID, result.Revision, command.RequestID); err != nil {
		return AlertRule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AlertRule{}, err
	}
	return result, nil
}

type UpdateAlertCommand struct {
	TenantID, ProjectID, ActorUserID, ExpectedRevision int64
	AlertID                                            string
	Name                                               *string
	Enabled                                            *bool
	DestinationID                                      *string
	CooldownSeconds                                    *int
	RuleBytes                                          []byte
	RuleSHA256                                         string
	RequestID, AuditID                                 string
}

func (operations *AlertOperations) UpdateAlert(ctx context.Context, command UpdateAlertCommand) (AlertRule, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.ActorUserID <= 0 || command.ExpectedRevision <= 0 || command.AlertID == "" || command.RequestID == "" || command.AuditID == "" || command.Name != nil && (strings.TrimSpace(*command.Name) == "" || len(*command.Name) > 128) || command.CooldownSeconds != nil && (*command.CooldownSeconds < 0 || *command.CooldownSeconds > 86400) || (len(command.RuleBytes) > 0 && (!validSHA(command.RuleSHA256) || len(command.RuleBytes) > 32<<10)) {
		return AlertRule{}, errors.New("invalid alert update")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return AlertRule{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, false, command.ProjectID, true); err != nil {
		return AlertRule{}, err
	}
	cut, err := lockPublishedCut(ctx, tx, command.TenantID)
	if err != nil {
		return AlertRule{}, err
	}
	current, err := loadAlertForUpdate(ctx, tx, command.TenantID, command.ProjectID, command.AlertID)
	if err != nil {
		return AlertRule{}, err
	}
	if current.Revision != command.ExpectedRevision {
		return AlertRule{}, ErrRevisionConflict
	}
	if command.Name != nil {
		current.Name = strings.TrimSpace(*command.Name)
	}
	if command.Enabled != nil {
		current.Enabled = *command.Enabled
	}
	if command.DestinationID != nil {
		current.DestinationID = *command.DestinationID
	}
	if command.CooldownSeconds != nil {
		current.CooldownSeconds = *command.CooldownSeconds
	}
	if len(command.RuleBytes) > 0 {
		current.RuleBytes = command.RuleBytes
		current.RuleSHA256 = command.RuleSHA256
	}
	var destinationEnabled bool
	if err := tx.QueryRow(ctx, `SELECT enabled FROM alert_destinations WHERE tenant_id=$1 AND destination_id=$2 FOR SHARE`, command.TenantID, current.DestinationID).Scan(&destinationEnabled); err != nil || current.Enabled && !destinationEnabled {
		return AlertRule{}, errors.Join(ErrForbidden, err)
	}
	current.EnabledFromPublicCut = cut
	var firstEnd any
	if current.Kind == "threshold" {
		var rule struct {
			WindowSeconds int `json:"window_seconds"`
		}
		if json.Unmarshal(current.RuleBytes, &rule) != nil || !validAlertWindow(rule.WindowSeconds) {
			return AlertRule{}, errors.New("threshold revision requires first window end")
		}
		var nowUS int64
		if err := tx.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&nowUS); err != nil {
			return AlertRule{}, err
		}
		current.FirstWindowEndUS = (nowUS/60_000_000+1)*60_000_000 + int64(rule.WindowSeconds)*1_000_000
		firstEnd = current.FirstWindowEndUS
	}
	err = tx.QueryRow(ctx, `UPDATE alerts SET name=$4,enabled=$5,rule_bytes=$6,rule_sha256=$7,destination_id=$8,cooldown_seconds=$9,
		enabled_from_public_cut=$10,enabled_at=clock_timestamp(),first_window_end_us=$11,last_completed_end_us=NULL,last_fired_end_us=NULL,last_issue_fired_at_us=NULL,
		revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND alert_id=$3
		RETURNING revision,enabled_at`, command.TenantID, command.ProjectID, command.AlertID, current.Name, current.Enabled, current.RuleBytes, current.RuleSHA256, current.DestinationID, current.CooldownSeconds, cut[:], firstEnd).Scan(&current.Revision, &current.EnabledAt)
	if err != nil {
		return AlertRule{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE alert_evaluations SET state='canceled',owner=NULL,lease_until=NULL,error_code='alert_revised',updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND alert_id=$2 AND state IN ('waiting','queued','running')`, command.TenantID, command.AlertID); err != nil {
		return AlertRule{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE deliveries SET state='canceled',owner=NULL,lease_until=NULL,error_code='alert_revised',updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND alert_id=$2 AND state IN ('queued','running')`, command.TenantID, command.AlertID); err != nil {
		return AlertRule{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_tasks SET state='canceled',owner=NULL,lease_until=NULL,error_code='alert_revised' WHERE tenant_id=$1 AND query_id IN (SELECT q.query_id FROM query_jobs q JOIN query_snapshots s ON s.snapshot_id=q.snapshot_id WHERE q.tenant_id=$1 AND s.principal_kind='alert' AND s.principal_ref=$2) AND state IN ('queued','running')`, command.TenantID, command.AlertID); err != nil {
		return AlertRule{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_jobs SET state='canceled',coordinator_owner=NULL,lease_until=NULL,error_code='alert_revised',updated_at=clock_timestamp() WHERE tenant_id=$1 AND snapshot_id IN (SELECT snapshot_id FROM query_snapshots WHERE tenant_id=$1 AND principal_kind='alert' AND principal_ref=$2) AND state IN ('planning','queued','running')`, command.TenantID, command.AlertID); err != nil {
		return AlertRule{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_snapshots SET state='released',expires_at=LEAST(expires_at,clock_timestamp()) WHERE tenant_id=$1 AND principal_kind='alert' AND principal_ref=$2 AND state='active'`, command.TenantID, command.AlertID); err != nil {
		return AlertRule{}, err
	}
	if err := insertAlertAudit(ctx, tx, command.TenantID, command.AuditID, command.ActorUserID, "alert_updated", "alert", current.AlertID, current.Revision, command.RequestID); err != nil {
		return AlertRule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AlertRule{}, err
	}
	return current, nil
}

func (operations *AlertOperations) ListAlertPage(ctx context.Context, tenantID, projectID, actorUserID int64, limit int, afterAlertID string) ([]AlertRule, error) {
	if limit < 1 || limit > 1000 || afterAlertID != "" && uuid.Validate(afterAlertID) != nil {
		return nil, errors.New("invalid alert page")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tenantAccess(ctx, tx, tenantID, actorUserID, projectID, false); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT tenant_id,project_id,alert_id::text,name,kind,revision,enabled,rule_bytes,rule_sha256,destination_id::text,cooldown_seconds,
		enabled_from_public_cut,enabled_at,coalesce(first_window_end_us,0),last_completed_end_us,last_fired_end_us,created_at
		FROM alerts WHERE tenant_id=$1 AND project_id=$2 AND ($3='' OR alert_id>NULLIF($3,'')::uuid) ORDER BY alert_id LIMIT $4`, tenantID, projectID, afterAlertID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]AlertRule, 0)
	for rows.Next() {
		value, scanErr := scanAlert(rows)
		if scanErr != nil {
			return nil, scanErr
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

func (operations *AlertOperations) GetAlert(ctx context.Context, tenantID, projectID, actorUserID int64, alertID string) (AlertRule, error) {
	if tenantID <= 0 || projectID <= 0 || actorUserID <= 0 || uuid.Validate(alertID) != nil {
		return AlertRule{}, ErrForbidden
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return AlertRule{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tenantAccess(ctx, tx, tenantID, actorUserID, projectID, false); err != nil {
		return AlertRule{}, err
	}
	row := tx.QueryRow(ctx, `SELECT tenant_id,project_id,alert_id::text,name,kind,revision,enabled,rule_bytes,rule_sha256,destination_id::text,cooldown_seconds,
		enabled_from_public_cut,enabled_at,coalesce(first_window_end_us,0),last_completed_end_us,last_fired_end_us,created_at FROM alerts WHERE tenant_id=$1 AND project_id=$2 AND alert_id=$3`, tenantID, projectID, alertID)
	result, err := scanAlert(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return AlertRule{}, ErrForbidden
	}
	if err != nil {
		return AlertRule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AlertRule{}, err
	}
	return result, nil
}

func (operations *AlertOperations) ListEnabledAlertPage(ctx context.Context, limit int, afterTenant int64, afterAlertID string) ([]AlertRule, error) {
	if limit < 1 || limit > 1000 || afterTenant < 0 || afterAlertID != "" && uuid.Validate(afterAlertID) != nil {
		return nil, errors.New("invalid scheduler alert limit")
	}
	rows, err := operations.pool.Query(ctx, `SELECT a.tenant_id,a.project_id,a.alert_id::text,a.name,a.kind,a.revision,a.enabled,a.rule_bytes,a.rule_sha256,a.destination_id::text,a.cooldown_seconds,a.enabled_from_public_cut,a.enabled_at,coalesce(a.first_window_end_us,0),a.last_completed_end_us,a.last_fired_end_us,a.created_at FROM alerts a JOIN installations i ON i.singleton WHERE a.enabled AND NOT i.alerts_paused AND (a.tenant_id>$2 OR (a.tenant_id=$2 AND a.alert_id>NULLIF($3,'')::uuid)) ORDER BY a.tenant_id,a.alert_id LIMIT $1`, limit, afterTenant, afterAlertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []AlertRule{}
	for rows.Next() {
		value, scanErr := scanAlert(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

type rowScanner interface{ Scan(...any) error }

func scanAlert(row rowScanner) (AlertRule, error) {
	var value AlertRule
	var cut []int64
	err := row.Scan(&value.TenantID, &value.ProjectID, &value.AlertID, &value.Name, &value.Kind, &value.Revision, &value.Enabled, &value.RuleBytes, &value.RuleSHA256, &value.DestinationID, &value.CooldownSeconds, &cut, &value.EnabledAt, &value.FirstWindowEndUS, &value.LastCompletedEndUS, &value.LastFiredEndUS, &value.CreatedAt)
	if err != nil {
		return value, err
	}
	if len(cut) != model.LaneCount {
		return value, errors.New("stored alert cut is invalid")
	}
	copy(value.EnabledFromPublicCut[:], cut)
	return value, nil
}
func loadAlertForUpdate(ctx context.Context, tx pgx.Tx, tenantID, projectID int64, alertID string) (AlertRule, error) {
	row := tx.QueryRow(ctx, `SELECT tenant_id,project_id,alert_id::text,name,kind,revision,enabled,rule_bytes,rule_sha256,destination_id::text,cooldown_seconds,
		enabled_from_public_cut,enabled_at,coalesce(first_window_end_us,0),last_completed_end_us,last_fired_end_us,created_at FROM alerts WHERE tenant_id=$1 AND project_id=$2 AND alert_id=$3 FOR UPDATE`, tenantID, projectID, alertID)
	value, err := scanAlert(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return value, ErrForbidden
	}
	return value, err
}

func lockPublishedCut(ctx context.Context, tx pgx.Tx, tenantID int64) ([model.LaneCount]int64, error) {
	var cut [model.LaneCount]int64
	rows, err := tx.Query(ctx, `SELECT lane_id,published_seq FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR UPDATE`, tenantID)
	if err != nil {
		return cut, err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var lane int
		if index >= model.LaneCount || rows.Scan(&lane, &cut[index]) != nil || lane != index {
			return cut, errors.New("tenant lane topology is invalid")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return cut, err
	}
	if index != model.LaneCount {
		return cut, errors.New("tenant lane topology is incomplete")
	}
	return cut, nil
}

func requireTenantRole(ctx context.Context, tx pgx.Tx, tenantID, userID int64, admin bool, projectID int64, operator bool) error {
	_, err := tenantAccess(ctx, tx, tenantID, userID, projectID, operator)
	if err != nil {
		return err
	}
	if admin {
		var role string
		if err := tx.QueryRow(ctx, `SELECT role FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR SHARE`, tenantID, userID).Scan(&role); err != nil || role != "admin" {
			return ErrForbidden
		}
	}
	return nil
}
func tenantAccess(ctx context.Context, tx pgx.Tx, tenantID, userID, projectID int64, operator bool) (bool, error) {
	var tenantState, userState, role string
	if err := tx.QueryRow(ctx, `SELECT t.state,u.state,m.role FROM tenants t JOIN memberships m ON m.tenant_id=t.tenant_id JOIN users u ON u.user_id=m.user_id WHERE t.tenant_id=$1 AND u.user_id=$2 FOR SHARE OF t,m,u`, tenantID, userID).Scan(&tenantState, &userState, &role); err != nil || tenantState != "active" || userState != "active" {
		return false, errors.Join(ErrForbidden, err)
	}
	if projectID > 0 {
		var state string
		if role == "admin" {
			if err := tx.QueryRow(ctx, `SELECT state FROM projects WHERE tenant_id=$1 AND project_id=$2 FOR SHARE`, tenantID, projectID).Scan(&state); err != nil || state != "active" {
				return false, errors.Join(ErrForbidden, err)
			}
		} else {
			var grant string
			if err := tx.QueryRow(ctx, `SELECT p.state,g.role FROM projects p JOIN project_grants g ON g.tenant_id=p.tenant_id AND g.project_id=p.project_id WHERE p.tenant_id=$1 AND p.project_id=$2 AND g.user_id=$3 FOR SHARE OF p,g`, tenantID, projectID, userID).Scan(&state, &grant); err != nil || state != "active" || operator && grant != "operator" {
				return false, errors.Join(ErrForbidden, err)
			}
		}
	}
	return role == "admin", nil
}

func insertAlertAudit(ctx context.Context, tx pgx.Tx, tenantID int64, auditID string, actor int64, action, targetType, targetID string, revision int64, requestID string) error {
	_, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, tenantID, auditID, actor, action, targetType, targetID, revision, requestID)
	return err
}
func nullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}
func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func validAlertWindow(value int) bool {
	return value == 60 || value == 300 || value == 900 || value == 3600
}
