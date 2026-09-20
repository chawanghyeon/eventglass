package control

import (
	"context"
	"encoding/json"
	"errors"
)

type SDKOutcome struct {
	SDKName, Category, Reason    string
	CategorySHA256, ReasonSHA256 string
	Count                        int64
	Approximate                  bool
}

type SDKOutcomePageCommand struct {
	TenantID, ActorUserID int64
	StartUS, EndUS        int64
	Limit                 int
	AfterCategorySHA256   string
	AfterReasonSHA256     string
}

func (operations *AuthOperations) ListSDKOutcomePage(ctx context.Context, command SDKOutcomePageCommand) ([]SDKOutcome, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.StartUS >= command.EndUS || command.Limit < 1 || command.Limit > 1000 || (command.AfterCategorySHA256 == "") != (command.AfterReasonSHA256 == "") || command.AfterCategorySHA256 != "" && (!validSHA(command.AfterCategorySHA256) || !validSHA(command.AfterReasonSHA256)) {
		return nil, errors.New("invalid SDK outcome page")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT encode(o.category_sha256,'hex'),encode(o.reason_sha256,'hex'),o.category_json,o.reason_json,sum(o.quantity)::bigint,bool_or(o.approximate)
		FROM sdk_outcomes o JOIN receipts r ON r.acceptance_id=o.acceptance_id
		WHERE r.tenant_id=$1 AND r.received_time_us>=$2 AND r.received_time_us<$3
		AND (($4='' AND $5='') OR (o.category_sha256,o.reason_sha256)>(decode($4,'hex'),decode($5,'hex')))
		GROUP BY o.category_sha256,o.reason_sha256,o.category_json,o.reason_json ORDER BY o.category_sha256,o.reason_sha256 LIMIT $6`,
		command.TenantID, command.StartUS, command.EndUS, command.AfterCategorySHA256, command.AfterReasonSHA256, command.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]SDKOutcome, 0, command.Limit+1)
	for rows.Next() {
		var value SDKOutcome
		var categoryJSON, reasonJSON string
		if err := rows.Scan(&value.CategorySHA256, &value.ReasonSHA256, &categoryJSON, &reasonJSON, &value.Count, &value.Approximate); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(categoryJSON), &value.Category); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(reasonJSON), &value.Reason); err != nil {
			return nil, err
		}
		value.SDKName = "unknown"
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
