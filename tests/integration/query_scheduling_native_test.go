//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/google/uuid"
)

// Real Parquet/S3 fixtures, ordinary authorized Submission/Awaiter and the
// actual product worker. This is query correctness/fairness evidence, not a
// durable-ingest or multi-worker throughput benchmark.
func TestPublicQueryEndToEndTenantTurnsActualWorker(t *testing.T) {
	checkQueryTenantTurnsActualWorkers(t, 1)
}

func TestPublicQueryEndToEndTenantTurnsActualWorkers(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) {
			checkQueryTenantTurnsActualWorkers(t, workers)
		})
	}
}

func checkQueryTenantTurnsActualWorkers(t *testing.T, workerCount int) {
	t.Helper()
	fixture := setupAcceptFixture(t, 1811)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	secondTenant := int64(1812)
	if workerCount > 1 {
		// Extra principals use tenant*100+100+ordinal in the existing helper;
		// leave a gap so they cannot collide with the other tenant's base user.
		secondTenant = 1831
	}
	second := addSchedulingTenant(t, ctx, fixture, secondTenant)
	ops, token := setupQueryPrincipal(t, fixture)
	_, secondToken := setupQueryPrincipal(t, second)
	prefix := "query-turns-" + uuid.NewString()
	store := integrationStore(t, prefix)
	insertPublicQueryBundle(t, ctx, fixture, store)
	insertPublicQueryBundle(t, ctx, second, store)
	codec, err := query.NewTokenCodec(query.SigningKey{ID: "query-turns", Secret: sha256.Sum256([]byte("isolated query scheduling token key"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	binary := requiredEnvironment(t, "EVENTGLASS_TEST_BINARY")["EVENTGLASS_TEST_BINARY"]
	service := &api.QueryAdapter{Control: ops, Store: store, Tokens: codec,
		Exporter: app.ProcessQueryExportRunner{BinaryPath: binary}, Working: resource.NewBudget(query.PlanningWorkingBytes),
		ScratchDir: filepath.Join(t.TempDir(), "results"), InstallationID: acceptInstallationID, StorageGeneration: 1}
	var workers []*maintenanceRuntime
	var resume func()
	if workerCount > 1 {
		workers, resume = startPausedTenantWorkers(t, ctx, fixture, prefix, workerCount)
	}
	type submitted struct {
		tenant, project int64
		token           [32]byte
		principal       control.SessionPrincipal
		queryID         string
	}
	var pending []submitted
	for index, target := range []*acceptFixture{fixture, second} {
		currentToken, count := token, 2
		if index == 1 {
			currentToken, count = secondToken, 1
		}
		if workerCount > 1 {
			count = 8 // Four actual principals, preserving two active queries/user.
		}
		filter := &query.Node{Op: "constant", Constant: true}
		canonical, err := query.CanonicalFilter(filter)
		if err != nil {
			t.Fatal(err)
		}
		spec := model.DatasetSpec{TenantID: target.tenantID, ProjectIDs: []int64{target.projectID}, Kinds: []model.Kind{model.KindLog},
			TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 10_000_000, Filter: canonical}
		digest, encoded, err := query.DatasetHash(spec)
		if err != nil {
			t.Fatal(err)
		}
		principal := control.SessionPrincipal{UserID: target.tenantID*100 + 1}
		for ordinal := range count {
			if ordinal >= 2 && ordinal%2 == 0 {
				_, currentToken = addQueryPrincipal(t, target, ordinal/2)
				principal.UserID = target.tenantID*100 + 100 + int64(ordinal/2)
			}
			response, err := service.Search(ctx, principal, currentToken, query.PublicSearchRequest{
				Dataset: query.PublicDataset{Spec: spec, Filter: filter, SHA256: digest, EncodedBytes: encoded}, Limit: 10, Sort: "event_desc", Mode: query.ModeAsync})
			if err != nil || response.Job == nil || response.Result != nil {
				t.Fatalf("asynchronous submission: job=%v error=%v", response.Job, err)
			}
			pending = append(pending, submitted{target.tenantID, target.projectID, currentToken, principal, response.Job.QueryId.String()})
		}
	}
	if service.Working.Used() != 0 {
		t.Fatal("submission leaked planning reservation")
	}
	if _, err := fixture.pool.Exec(ctx, `CREATE TABLE test_query_claim_order (
		ordinal BIGSERIAL PRIMARY KEY CHECK(ordinal<=64),tenant_id BIGINT NOT NULL,query_id UUID NOT NULL,owner TEXT NOT NULL);
		CREATE FUNCTION test_record_query_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		  IF NEW.state='running' AND NEW.attempt>OLD.attempt THEN
		    INSERT INTO test_query_claim_order(tenant_id,query_id,owner) VALUES(NEW.tenant_id,NEW.query_id,NEW.owner);
		  END IF;
		  RETURN NEW;
		END $$;
		CREATE TRIGGER test_query_claim_order AFTER UPDATE ON query_tasks FOR EACH ROW EXECUTE FUNCTION test_record_query_claim()`); err != nil {
		t.Fatal(err)
	}
	if workerCount == 1 {
		workers = []*maintenanceRuntime{startMaintenanceRuntime(t, ctx, fixture, prefix)}
	} else {
		resume()
	}
	awaiter := query.Awaiter{Control: ops}
	for _, request := range pending {
		if err := awaiter.Execute(ctx, request.token, request.tenant, request.queryID); err != nil {
			t.Fatalf("actual worker query=%s: %v", request.queryID, err)
		}
	}
	for _, worker := range workers {
		worker.stop()
	}
	var turns []int64
	var owners int
	if err := fixture.pool.QueryRow(ctx, `SELECT array_agg(tenant_id ORDER BY ordinal),count(DISTINCT owner) FROM test_query_claim_order`).Scan(&turns, &owners); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual query workers=%d tenant order=%v", workerCount, turns)
	for _, request := range pending {
		response, err := service.Job(ctx, request.principal, request.token, request.tenant, request.queryID)
		if err != nil || response.State != "succeeded" || response.Result == nil {
			t.Fatalf("query result state=%s error=%v", response.State, err)
		}
		var wire struct {
			Rows []struct {
				RecordID  string `json:"record_id"`
				ProjectID string `json:"project_id"`
			} `json:"rows"`
		}
		decodeWire(t, response.Result, &wire)
		var actual []string
		for _, row := range wire.Rows {
			if row.ProjectID != strconv.FormatInt(request.project, 10) {
				t.Fatalf("cross-project result: tenant=%d project=%s", request.tenant, row.ProjectID)
			}
			actual = append(actual, row.RecordID)
		}
		expected := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)}
		if !slices.Equal(actual, expected) {
			t.Fatalf("tenant=%d result IDs=%v expected=%v", request.tenant, actual, expected)
		}
	}
	foreign := pending[len(pending)-1]
	if _, err := service.Job(ctx, pending[0].principal, pending[0].token, foreign.tenant, foreign.queryID); err == nil {
		t.Fatal("different tenant/session read a scheduled result")
	}
	if workerCount > 1 {
		checkMultipleWorkerTenantTurns(t, workerCount, turns, fixture.tenantID, second.tenantID, 8, 8, owners)
		return
	}
	want := []int64{fixture.tenantID, second.tenantID, fixture.tenantID}
	if !slices.Equal(turns, want) {
		t.Fatalf("older tenant hid ready search: turns=%v expected=%v", turns, want)
	}
}
