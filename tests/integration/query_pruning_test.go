//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestPublicQueryEndToEndFirstPagePruning(t *testing.T) {
	environment := requiredEnvironment(t, "EVENTGLASS_TEST_BINARY")
	fixture := setupAcceptFixture(t, 1981)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ops, token := setupQueryPrincipal(t, fixture)
	store := integrationStore(t, "first-page-pruning")
	ingestOps, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := control.NewPublicationOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	converter := ingest.DurableConversionWorkflow{Control: publication, Store: store, Runner: app.ProcessConversionRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"]}, InstallationID: acceptInstallationID, ScratchDir: filepath.Join(t.TempDir(), "convert"), Disk: resource.NewBudget(4 << 30)}
	publisher := ingest.DurablePublicationWorkflow{Control: publication, Store: store}
	workflow := integrationWorkflow(t, fixture, ingestOps, store, "pruning")
	for batch := range 4 {
		command := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(0), "unused")
		var items []sdk.Item
		for row := range 2 {
			items = append(items, sdk.Item{Ordinal: row, Type: "event", Value: map[string]any{"event_id": fmt.Sprintf("%032x", batch*2+row+1), "message": fmt.Sprintf("batch-%d-row-%d", batch, row), "timestamp": time.Unix(100+int64(batch), 0).UTC().Format(time.RFC3339)}})
		}
		command.Request, err = ingest.NormalizeEnvelope(sdk.Envelope{Items: items}, ingest.NormalizeOptions{TenantID: fixture.tenantID, ProjectID: fixture.projectID, AcceptanceID: command.Request.AcceptanceID, ArrivalTime: time.Unix(100+int64(batch), 0)})
		if err != nil {
			t.Fatal(err)
		}
		accepted := workflow.Process(ctx, []ingest.Command{command})
		if len(accepted) != 1 || accepted[0].Err != nil || accepted[0].Receipt.AcceptedCount != 2 {
			t.Fatalf("accept=%+v", accepted)
		}
		job, err := publication.ClaimConversion(ctx, acceptInstallationID, 1, "pruning", time.Minute)
		if err != nil || job == nil {
			t.Fatalf("claim=%+v %v", job, err)
		}
		if err := converter.ConvertAndPrepare(ctx, *job); err != nil {
			t.Fatal(err)
		}
		pending, err := publication.ClaimPublication(ctx, acceptInstallationID, 1, fixture.tenantID, 0, "pruning", time.Minute)
		if err != nil || pending == nil {
			t.Fatalf("publish claim=%+v %v", pending, err)
		}
		if _, err := publisher.VerifyAndPublish(ctx, *pending); err != nil {
			t.Fatal(err)
		}
	}
	disk := resource.NewBudget(4 << 30)
	cache, err := storage.NewBlockCache(filepath.Join(t.TempDir(), "cache"), storage.DefaultCacheBytes, disk)
	if err != nil {
		t.Fatal(err)
	}
	worker := &query.Workflow{Control: ops, Store: store, Disk: disk, Cache: cache, Runner: app.ProcessQueryRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"]}, InstallationID: acceptInstallationID, ScratchDir: filepath.Join(t.TempDir(), "worker")}
	startIndependentQueryWorker(t, ctx, ops, worker)
	codec, err := query.NewTokenCodec(query.SigningKey{ID: "pruning", Secret: sha256.Sum256([]byte("isolated pruning test signing key"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	service := &api.QueryAdapter{Control: ops, Store: store, Tokens: codec, Exporter: app.ProcessQueryExportRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"]}, Working: resource.NewBudget(query.PlanningWorkingBytes), ScratchDir: filepath.Join(t.TempDir(), "results"), InstallationID: acceptInstallationID, StorageGeneration: 1}
	principal := control.SessionPrincipal{UserID: fixture.tenantID*100 + 1}
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, _ := query.CanonicalFilter(filter)
	spec := model.DatasetSpec{TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindError}, TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 1_000_000_000, Filter: canonical}
	digest, encoded, err := query.DatasetHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	dataset := query.PublicDataset{Spec: spec, SHA256: digest, EncodedBytes: encoded, Filter: filter}
	for _, order := range []string{"event_desc", "received_desc"} {
		t.Run(order, func(t *testing.T) {
			readToken, cursor := "", ""
			seen := map[string]bool{}
			for page := 0; ; page++ {
				before := store.OperationCounts()
				result, err := service.Search(ctx, principal, token, query.PublicSearchRequest{Dataset: dataset, ReadToken: readToken, Cursor: cursor, Limit: 1, Sort: order, Mode: query.ModeSync})
				if err != nil || result.Result == nil {
					t.Fatalf("page=%d result=%+v err=%v", page, result, err)
				}
				var wire struct {
					Rows []struct {
						Message string `json:"message"`
					} `json:"rows"`
					ReadToken  string  `json:"read_token"`
					NextCursor *string `json:"next_cursor"`
				}
				decodeWire(t, result.Result, &wire)
				if len(wire.Rows) != 1 || seen[wire.Rows[0].Message] {
					t.Fatalf("lost/duplicate page=%d wire=%+v", page, wire)
				}
				if !strings.HasPrefix(wire.Rows[0].Message, fmt.Sprintf("batch-%d-", 3-page/2)) {
					t.Fatalf("page=%d is not the next independently expected batch: %+v", page, wire)
				}
				seen[wire.Rows[0].Message] = true
				var planned int
				if err := fixture.pool.QueryRow(ctx, `SELECT COALESCE(sum(jsonb_array_length(convert_from(manifest_json,'UTF8')::jsonb->'files')),0) FROM query_tasks WHERE query_id=(SELECT query_id FROM query_jobs WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 1) AND stage='scan'`, fixture.tenantID).Scan(&planned); err != nil {
					t.Fatal(err)
				}
				wantFiles := 4
				if page == 0 {
					wantFiles = 1
				}
				if planned != wantFiles {
					t.Fatalf("page=%d planned=%d want=%d", page, planned, wantFiles)
				}
				if store.OperationCounts().HeadRequests-before.HeadRequests < 8 {
					t.Fatal("pruning bypassed full paired catalog HEAD verification")
				}
				readToken = wire.ReadToken
				if wire.NextCursor == nil {
					break
				}
				cursor = *wire.NextCursor
				if page >= 8 {
					t.Fatal("unbounded pagination")
				}
			}
			for batch := range 4 {
				for row := range 2 {
					if !seen[fmt.Sprintf("batch-%d-row-%d", batch, row)] {
						t.Fatal("independent expected row missing")
					}
				}
			}
			if len(seen) != 8 {
				t.Fatalf("rows=%d", len(seen))
			}
		})
	}
	// Even an older file that the first-page proof would prune must pass the
	// unchanged catalog integrity check. Delete only this isolated test object.
	var oldKey string
	if err := fixture.pool.QueryRow(ctx, `SELECT oi.object_key FROM files f JOIN object_intents oi ON oi.intent_id=f.intent_id WHERE f.tenant_id=$1 AND f.role='analytics' ORDER BY f.min_received_time_us LIMIT 1`, fixture.tenantID).Scan(&oldKey); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, []string{oldKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Search(ctx, principal, token, query.PublicSearchRequest{Dataset: dataset, Limit: 1, Sort: "event_desc", Mode: query.ModeSync}); !errors.Is(err, query.ErrCatalogObjectMissing) {
		t.Fatalf("pruned missing object did not fail closed: %v", err)
	}
	// Revocation still rejects before catalog access or plan creation.
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, fixture.tenantID, principal.UserID); err != nil {
		t.Fatal(err)
	}
	before := store.OperationCounts()
	if _, err := service.Search(ctx, principal, token, query.PublicSearchRequest{Dataset: dataset, Limit: 1, Sort: "event_desc", Mode: query.ModeSync}); !errors.Is(err, control.ErrUnauthenticated) {
		t.Fatalf("revoked search=%v", err)
	}
	if store.OperationCounts().HeadRequests != before.HeadRequests {
		t.Fatal("revoked query reached S3")
	}
}
