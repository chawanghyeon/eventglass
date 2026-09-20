//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/alerts"
	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestPublicQueryEndToEndAlertEvaluation(t *testing.T) {
	env := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "EVENTGLASS_TEST_BINARY")
	fixture := setupAlertFixture(t, 989)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rule := testRule(t, alerts.KindThreshold, `{"expression":"true","kinds":["error"],"window_seconds":60,"metric":"count","operator":"ge","threshold":"0"}`)
	created, err := fixture.operations.CreateAlert(ctx, control.CreateAlertCommand{TenantID: fixture.tenant, ProjectID: fixture.project, ActorUserID: fixture.user, AlertID: fixture.uuid(), Name: "empty-count", DestinationID: fixture.destination, Kind: "threshold", RuleBytes: rule.Bytes, RuleSHA256: rule.SHA256, CooldownSeconds: 0, RequestID: fixture.uuid(), AuditID: fixture.uuid()})
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	if err := fixture.pool.QueryRow(ctx, `SELECT (floor(extract(epoch FROM clock_timestamp())/60)*60*1000000)::bigint`).Scan(&end); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE alerts SET first_window_end_us=$3 WHERE tenant_id=$1 AND alert_id=$2`, fixture.tenant, created.AlertID, end); err != nil {
		t.Fatal(err)
	}
	queryOps, _ := control.NewQueryOperations(fixture.pool)
	store := integrationStore(t, "alert-query-989")
	var installationID string
	if err := fixture.pool.QueryRow(ctx, `SELECT installation_id::text FROM installations WHERE singleton`).Scan(&installationID); err != nil {
		t.Fatal(err)
	}
	worker := &query.Workflow{Disk: resource.NewBudget(4 << 30), Control: queryOps, Store: store, Runner: app.ProcessQueryRunner{BinaryPath: env["EVENTGLASS_TEST_BINARY"]}, InstallationID: installationID, ScratchDir: filepath.Join(t.TempDir(), "worker")}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() {
		for {
			task, claimErr := queryOps.ClaimQueryTask(workerCtx, installationID, 1, "alert-e2e-worker")
			if claimErr != nil {
				if workerCtx.Err() != nil {
					workerDone <- nil
				} else {
					workerDone <- claimErr
				}
				return
			}
			if task != nil {
				if executeErr := worker.Execute(workerCtx, *task); executeErr != nil && !errors.Is(executeErr, control.ErrQueryTerminal) {
					workerDone <- executeErr
					return
				}
				continue
			}
			select {
			case <-workerCtx.Done():
				workerDone <- nil
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	defer func() {
		stopWorker()
		if err := <-workerDone; err != nil {
			t.Errorf("alert worker: %v", err)
		}
	}()
	evaluator := &alerts.Evaluator{Alerts: fixture.operations, Queries: queryOps, Objects: store, Results: app.AlertCountReader{Store: store, Exporter: app.ProcessQueryExportRunner{BinaryPath: env["EVENTGLASS_TEST_BINARY"]}, ScratchDir: filepath.Join(t.TempDir(), "results")}, Owner: "alert-e2e-scheduler", PublicURL: "https://events.example.invalid", StorageGeneration: 1}
	if err := evaluator.EvaluateOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var state string
	var body []byte
	if err := fixture.pool.QueryRow(ctx, `SELECT state,body_bytes FROM deliveries WHERE tenant_id=$1 AND alert_id=$2`, fixture.tenant, created.AlertID).Scan(&state, &body); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Version   int `json:"version"`
		Threshold struct {
			Observed string `json:"observed"`
		} `json:"threshold"`
	}
	if json.Unmarshal(body, &decoded) != nil || state != "queued" || decoded.Version != 1 || decoded.Threshold.Observed != "0" {
		t.Fatalf("state=%s body=%s", state, body)
	}
}
