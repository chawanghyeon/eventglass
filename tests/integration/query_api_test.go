//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestPublicQueryEndToEnd(t *testing.T) {
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "EVENTGLASS_TEST_BINARY")
	fixture := setupAcceptFixture(t, 980)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	operations, tokenHash := setupQueryPrincipal(t, fixture)
	store := integrationStore(t, "query-api-980")
	insertPublicQueryBundle(t, ctx, fixture, store)
	codec, err := query.NewTokenCodec(query.SigningKey{ID: "integration", Secret: sha256.Sum256([]byte("query API integration signing key"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := &app.DurableQueryWorkflow{
		Control: operations, Store: store, Runner: app.ProcessQueryRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"]},
		InstallationID: acceptInstallationID, ScratchDir: filepath.Join(t.TempDir(), "worker"),
	}
	syncExecutor := &app.DurableQuerySyncExecutor{
		Control: operations, Workflow: worker, InstallationID: acceptInstallationID, StorageGeneration: 1, Owner: "sync-integration",
	}
	service := &app.PublicQueryService{
		Control: operations, Store: store, Tokens: codec, Exporter: app.ProcessQueryExportRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"]},
		Sync: syncExecutor, ScratchDir: filepath.Join(t.TempDir(), "results"), InstallationID: acceptInstallationID, StorageGeneration: 1,
	}
	principal := control.SessionPrincipal{UserID: fixture.tenantID*100 + 1}
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, _ := query.CanonicalFilter(filter)
	spec := model.DatasetSpec{
		TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindLog},
		TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 10_000_000, Filter: canonical,
	}
	digest, encoded, err := query.DatasetHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	dataset := query.PublicDataset{Spec: spec, Filter: filter, SHA256: digest, EncodedBytes: encoded}

	before := store.OperationCounts()
	coldStarted := time.Now()
	search, err := service.Search(ctx, principal, tokenHash, query.PublicSearchRequest{Dataset: dataset, Limit: 1, Sort: "event_desc", Mode: query.ModeSync})
	if err != nil || search.Result == nil || search.Job != nil {
		t.Fatalf("search=%#v err=%v", search, err)
	}
	var searchWire struct {
		Rows       []map[string]any `json:"rows"`
		ReadToken  string           `json:"read_token"`
		NextCursor *string          `json:"next_cursor"`
	}
	decodeWire(t, search.Result, &searchWire)
	if len(searchWire.Rows) != 1 || searchWire.ReadToken == "" || searchWire.NextCursor == nil || searchWire.Rows[0]["message_truncated"] != true {
		t.Fatalf("search wire=%#v", searchWire)
	}
	coldFinished, coldElapsed := store.OperationCounts(), time.Since(coldStarted)
	warmStarted := time.Now()
	warm, err := service.Search(ctx, principal, tokenHash, query.PublicSearchRequest{Dataset: dataset, ReadToken: searchWire.ReadToken, Limit: 1, Sort: "event_desc", Mode: query.ModeSync})
	if err != nil || warm.Result == nil {
		t.Fatalf("warm search=%#v err=%v", warm, err)
	}
	warmFinished, warmElapsed := store.OperationCounts(), time.Since(warmStarted)

	aggregate, err := service.Aggregate(ctx, principal, tokenHash, query.PublicAggregateRequest{
		Dataset: dataset, ReadToken: searchWire.ReadToken, Mode: query.ModeSync,
		Metrics: []query.AggregateMetric{{Name: "events", Op: "count"}}, Histogram: &query.AggregateHistogram{IntervalUS: 1_000_000, EmptyBuckets: true},
		Top: 100, Order: query.AggregateOrder{Metric: "count", Direction: "desc"},
	})
	if err != nil || aggregate.Result == nil {
		t.Fatalf("aggregate=%#v err=%v", aggregate, err)
	}
	var aggregateWire struct {
		Groups []json.RawMessage `json:"groups"`
	}
	decodeWire(t, aggregate.Result, &aggregateWire)
	if len(aggregateWire.Groups) != 10 {
		t.Fatalf("histogram groups=%d", len(aggregateWire.Groups))
	}

	recordID := strings.Repeat("a", 64)
	detail, err := service.Record(ctx, principal, tokenHash, fixture.tenantID, fixture.projectID, recordID, searchWire.ReadToken)
	if err != nil {
		t.Fatal(err)
	}
	var detailWire struct {
		Record    map[string]any `json:"record"`
		Raw       map[string]any `json:"raw"`
		ReadToken string         `json:"read_token"`
	}
	decodeWire(t, detail, &detailWire)
	if detailWire.ReadToken == "" || detailWire.Record["record_id"] != recordID || detailWire.Raw["message"] != strings.Repeat("가", 1100) {
		t.Fatalf("detail=%#v", detailWire)
	}
	_, otherToken := addQueryPrincipal(t, fixture, 44)
	otherPrincipal := control.SessionPrincipal{UserID: fixture.tenantID*100 + 144}
	if _, err := service.Record(ctx, otherPrincipal, otherToken, fixture.tenantID, fixture.projectID, recordID, searchWire.ReadToken); !errors.Is(err, query.ErrTokenForbidden) {
		t.Fatalf("another user read token error=%v", err)
	}

	async, err := service.Search(ctx, principal, tokenHash, query.PublicSearchRequest{Dataset: dataset, ReadToken: searchWire.ReadToken, Limit: 2, Sort: "event_desc", Mode: query.ModeAsync})
	if err != nil || async.Job == nil || async.Result != nil {
		t.Fatalf("async=%#v err=%v", async, err)
	}
	if err := syncExecutor.Execute(ctx, tokenHash, fixture.tenantID, async.Job.QueryId.String()); err != nil {
		t.Fatal(err)
	}
	job, err := service.Job(ctx, principal, tokenHash, fixture.tenantID, async.Job.QueryId.String())
	if err != nil || job.State != "succeeded" || job.Result == nil {
		t.Fatalf("job=%#v err=%v", job, err)
	}

	oversized, err := service.Search(ctx, principal, tokenHash, query.PublicSearchRequest{Dataset: dataset, ReadToken: searchWire.ReadToken, Limit: 2, Sort: "event_desc", Mode: query.ModeAsync})
	if err != nil || oversized.Job == nil {
		t.Fatalf("oversized submission=%#v err=%v", oversized, err)
	}
	if err := syncExecutor.Execute(ctx, tokenHash, fixture.tenantID, oversized.Job.QueryId.String()); err != nil {
		t.Fatal(err)
	}
	limitedService := *service
	limitedService.Exporter = limitExportRunner{}
	limited, err := limitedService.Job(ctx, principal, tokenHash, fixture.tenantID, oversized.Job.QueryId.String())
	if err != nil || limited.State != "failed" || limited.Error == nil || limited.Error.Code != "result_limit_exceeded" {
		t.Fatalf("limited job=%#v err=%v", limited, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE query_jobs SET created_at=clock_timestamp()-interval '2 seconds',expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND query_id=$2`, fixture.tenantID, async.Job.QueryId); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Job(ctx, principal, tokenHash, fixture.tenantID, async.Job.QueryId.String()); !errors.Is(err, api.ErrPublicQueryGone) {
		t.Fatalf("expired job error=%v", err)
	}

	currentCtx, stopCurrent := context.WithCancel(ctx)
	currentEvents := []string{}
	err = service.Live(currentCtx, principal, tokenHash, query.PublicLiveRequest{
		TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindLog}, Filter: filter, Canonical: canonical,
	}, "", func(event query.LiveEvent) error {
		currentEvents = append(currentEvents, event.Type)
		stopCurrent()
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(currentEvents) != 1 || currentEvents[0] != "checkpoint" {
		t.Fatalf("current-cut live events=%v err=%v", currentEvents, err)
	}

	catchup := time.Now().Add(-5 * time.Minute)
	liveCtx, stopLive := context.WithTimeout(ctx, 15*time.Second)
	defer stopLive()
	var liveRows []map[string]any
	err = service.Live(liveCtx, principal, tokenHash, query.PublicLiveRequest{
		TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindLog}, Filter: filter, Canonical: canonical, CatchupStart: &catchup,
	}, "", func(event query.LiveEvent) error {
		if event.Type != "rows" {
			return nil
		}
		var wire struct {
			Rows []map[string]any `json:"rows"`
		}
		decodeWire(t, event.Data, &wire)
		liveRows = append(liveRows, wire.Rows...)
		stopLive()
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(liveRows) != 1 || liveRows[0]["record_id"] != strings.Repeat("c", 64) {
		t.Fatalf("catchup live rows=%#v err=%v", liveRows, err)
	}

	revokedCtx, stopRevoked := context.WithTimeout(ctx, 10*time.Second)
	defer stopRevoked()
	revokedEvents := []string{}
	err = service.Live(revokedCtx, principal, tokenHash, query.PublicLiveRequest{
		TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindLog}, Filter: filter, Canonical: canonical,
	}, "", func(event query.LiveEvent) error {
		revokedEvents = append(revokedEvents, event.Type)
		if event.Type == "checkpoint" && len(revokedEvents) == 1 {
			if _, updateErr := fixture.pool.Exec(ctx, `UPDATE sessions SET revoked_at=clock_timestamp() WHERE token_hash=$1`, tokenHash[:]); updateErr != nil {
				return updateErr
			}
		}
		if event.Type == "error" {
			stopRevoked()
		}
		return nil
	})
	if err != nil || len(revokedEvents) != 2 || revokedEvents[0] != "checkpoint" || revokedEvents[1] != "error" {
		t.Fatalf("revoked live events=%v err=%v", revokedEvents, err)
	}
	after := store.OperationCounts()
	t.Logf("query evidence cold: HEAD=%d full_GET=%d bytes=%d elapsed_ms=%d", coldFinished.HeadRequests-before.HeadRequests, coldFinished.FullGetRequests-before.FullGetRequests, coldFinished.FullGetBytes-before.FullGetBytes, coldElapsed.Milliseconds())
	t.Logf("query evidence warm: HEAD=%d full_GET=%d bytes=%d elapsed_ms=%d", warmFinished.HeadRequests-coldFinished.HeadRequests, warmFinished.FullGetRequests-coldFinished.FullGetRequests, warmFinished.FullGetBytes-coldFinished.FullGetBytes, warmElapsed.Milliseconds())
	t.Logf("query evidence total: HEAD=%d full_GET=%d bytes=%d", after.HeadRequests-before.HeadRequests, after.FullGetRequests-before.FullGetRequests, after.FullGetBytes-before.FullGetBytes)
}

type limitExportRunner struct{}

func (limitExportRunner) Export(context.Context, engine.QueryExportRequest) (engine.QueryExportSummary, error) {
	return engine.QueryExportSummary{}, engine.ErrQueryExecutionLimit
}

func insertPublicQueryBundle(t *testing.T, ctx context.Context, fixture *acceptFixture, store *storage.S3Store) {
	t.Helper()
	root := t.TempDir()
	analyticsPath, payloadPath := filepath.Join(root, "analytics.parquet"), filepath.Join(root, "payload.parquet")
	recordA, recordB, recordC := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	liveEventUS := time.Now().Add(-time.Minute).UnixMicro()
	message := strings.Repeat("가", 1100)
	writeIntegrationParquet(t, ctx, analyticsPath, `SELECT * FROM (VALUES
		(`+wireAnalyticsRow(fixture, recordA, 1_200_000, message, 0)+`),(`+wireAnalyticsRow(fixture, recordB, 1_100_000, "short", 1)+`),
		(`+wireAnalyticsRowTimes(fixture, recordC, 1_050_000, liveEventUS, "live", 2)+`))
		t(tenant_id,project_id,record_id,kind,event_time_us,event_time_ns_remainder,arrival_time_us,received_time_us,lane_id,batch_seq,record_ordinal,level,severity_number,message,service,environment,release,trace_id,issue_id,grouping_version)`)
	metadata := func(id, value string, event int64) string {
		object := map[string]any{"tenant_id": fixture.tenantID, "project_id": fixture.projectID, "record_id": id, "kind": "log", "event_time_us": event, "event_time_ns_remainder": 0, "arrival_time_us": event, "level": "info", "message": value, "attrs": []any{}, "search_values": []string{value}, "warnings": []string{}, "schema_version": 1, "normalizer_version": 1, "scrub_version": 1}
		encoded, _ := json.Marshal(object)
		return strings.ReplaceAll(string(encoded), "'", "''")
	}
	rawA, _ := json.Marshal(map[string]any{"message": message})
	rawB, _ := json.Marshal(map[string]any{"message": "short"})
	rawC, _ := json.Marshal(map[string]any{"message": "live"})
	writeIntegrationParquet(t, ctx, payloadPath, `SELECT * FROM (VALUES
		('`+recordA+`','`+strings.ReplaceAll(string(rawA), "'", "''")+`',NULL,'[]','`+metadata(recordA, message, 1_200_000)+`'),
		('`+recordB+`','`+strings.ReplaceAll(string(rawB), "'", "''")+`',NULL,'[]','`+metadata(recordB, "short", 1_100_000)+`'),
		('`+recordC+`','`+strings.ReplaceAll(string(rawC), "'", "''")+`',NULL,'[]','`+metadata(recordC, "live", 1_050_000)+`'))
		t(record_id,raw_json,envelope_sdk_json,normalization_warnings_json,canonical_metadata_json)`)
	analyticsBytes, _ := os.ReadFile(analyticsPath)
	payloadBytes, _ := os.ReadFile(payloadPath)
	analyticsInfo, err := store.Put(ctx, "bundles/analytics.parquet", analyticsBytes)
	if err != nil {
		t.Fatal(err)
	}
	payloadInfo, err := store.Put(ctx, "bundles/payload.parquet", payloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	analyticsEvidence, _ := storage.InspectFile(analyticsPath)
	payloadEvidence, _ := storage.InspectFile(payloadPath)
	insertCatalogObjects(t, ctx, fixture, analyticsInfo, payloadInfo, analyticsEvidence, payloadEvidence, liveEventUS)
}

func wireAnalyticsRow(fixture *acceptFixture, id string, event int64, message string, ordinal int) string {
	return wireAnalyticsRowTimes(fixture, id, event, event, message, ordinal)
}

func wireAnalyticsRowTimes(fixture *acceptFixture, id string, event, received int64, message string, ordinal int) string {
	encoded, _ := json.Marshal(message)
	return fmt.Sprintf("%d::BIGINT,%d::BIGINT,'%s','log',%d::BIGINT,0::INTEGER,%d::BIGINT,%d::BIGINT,0::INTEGER,5::BIGINT,%d::INTEGER,'info',9::SMALLINT,'%s','svc',NULL,NULL,NULL,NULL,NULL", fixture.tenantID, fixture.projectID, id, event, received, received, ordinal, strings.ReplaceAll(string(encoded[1:len(encoded)-1]), "'", "''"))
}

func writeIntegrationParquet(t *testing.T, ctx context.Context, path, statement string) {
	t.Helper()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `COPY (`+statement+`) TO '`+strings.ReplaceAll(path, "'", "''")+`' (FORMAT PARQUET)`); err != nil {
		t.Fatal(err)
	}
}

func insertCatalogObjects(t *testing.T, ctx context.Context, fixture *acceptFixture, analytics, payload storage.ObjectInfo, analyticsEvidence, payloadEvidence storage.FileEvidence, maxReceivedUS int64) {
	t.Helper()
	ids := []string{snapshotUUID(fixture.tenantID, 701), snapshotUUID(fixture.tenantID, 702), snapshotUUID(fixture.tenantID, 703), snapshotUUID(fixture.tenantID, 704), snapshotUUID(fixture.tenantID, 705)}
	for index, object := range []storage.ObjectInfo{analytics, payload} {
		kind := []string{"analytics", "payload"}[index]
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256)
			VALUES($1,$2,$3,1,$4,$5,'referenced','query-fixture',1,clock_timestamp()+interval '1 hour',$6,$7,$6,$7)`, ids[index], acceptInstallationID, fixture.tenantID, object.Key, kind, object.Size, object.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundles(bundle_id,tenant_id,lane_id,schema_version,grouping_version,event_day,kind,input_seq_min,input_seq_max,row_count,identity_sha256,valid_from_generation)
		VALUES($1,$2,0,1,1,'2026-09-20','log',1,5,3,$3,7)`, ids[2], fixture.tenantID, strings.Repeat("1", 64)); err != nil {
		t.Fatal(err)
	}
	for index, evidence := range []storage.FileEvidence{analyticsEvidence, payloadEvidence} {
		role := []string{"analytics", "payload"}[index]
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
			VALUES($1,$2,$3,$4,$5,$6,$7,3,1050000,1200000,1050000,$8,1,5)`, ids[3+index], fixture.tenantID, ids[2], ids[index], role, evidence.Bytes, evidence.SHA256, maxReceivedUS); err != nil {
			t.Fatal(err)
		}
		for block, checksum := range evidence.BlockSHA256 {
			if _, err := fixture.pool.Exec(ctx, `INSERT INTO file_blocks(file_id,block_index,sha256) VALUES($1,$2,$3)`, ids[3+index], block, checksum); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, fixture.tenantID, ids[2], fixture.projectID); err != nil {
		t.Fatal(err)
	}
}

func decodeWire(t *testing.T, value, target any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil || json.Unmarshal(encoded, target) != nil {
		t.Fatalf("decode wire %s: %v", encoded, err)
	}
}
