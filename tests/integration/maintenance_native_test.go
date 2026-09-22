//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/maintenance"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

// Use durable ingestion and real native children, not synthetic catalog/output
// manifests: the former retention tests never reached the one-input loader.
func TestMaintenanceEndToEndRetentionAndPinnedSnapshot(t *testing.T) {
	runNativeRetention(t, 2, 1, false)
}

func TestMaintenanceEndToEndLargeBundleRetention(t *testing.T) {
	// Exercise both the formerly failing64-event conversion and512-event
	// compaction through actual durable ingestion, S3 and joined native children.
	runNativeRetention(t, 8, 64, true)
}

func runNativeRetention(t *testing.T, batchCount, perBatch int, large bool) {
	t.Helper()
	environment := requiredEnvironment(t, "EVENTGLASS_TEST_BINARY")
	fixture := setupAcceptFixture(t, 1780)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store := integrationStore(t, "maintenance-native-"+uuid.NewString())
	ingestOps, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := control.NewPublicationOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := control.NewMaintenanceOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	gate := app.NewNativeTaskGate()
	scratch := filepath.Join(t.TempDir(), "work")
	converter := ingest.DurableConversionWorkflow{Control: publication, Store: store,
		Runner: app.ProcessConversionRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"], Gate: gate}, InstallationID: acceptInstallationID, ScratchDir: scratch, Disk: resource.NewBudget(4 << 30)}
	publisher := ingest.DurablePublicationWorkflow{Control: publication, Store: store}
	workflow := integrationWorkflow(t, fixture, ingestOps, store, "native-retention")
	received := make([]int64, batchCount)
	random := rand.New(rand.NewSource(1780))
	for index := range batchCount {
		command := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(0), "unused")
		items := make([]sdk.Item, perBatch)
		for ordinal := range items {
			value := map[string]any{"event_id": fmt.Sprintf("%032x", index*perBatch+ordinal+1), "message": fmt.Sprintf("native retention %d/%d", index, ordinal)}
			if large {
				// Deterministic high-entropy payload avoids a fake large file
				// made of compressible zeroes. Each journal stays below24MiB.
				blob := make([]byte, 72<<10)
				if _, err := random.Read(blob); err != nil {
					t.Fatal(err)
				}
				value["extra"] = map[string]any{"blob": base64.StdEncoding.EncodeToString(blob)}
			}
			items[ordinal] = sdk.Item{Ordinal: ordinal, Type: "event", Value: value}
		}
		command.Request, err = ingest.NormalizeEnvelope(sdk.Envelope{Items: items}, ingest.NormalizeOptions{TenantID: fixture.tenantID, ProjectID: fixture.projectID, AcceptanceID: command.Request.AcceptanceID, ArrivalTime: time.Unix(1, 0)})
		if err != nil {
			t.Fatal(err)
		}
		results := workflow.Process(ctx, []ingest.Command{command})
		if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptedCount != perBatch {
			t.Fatalf("accept: %+v", results)
		}
		received[index] = results[0].Receipt.ReceivedTimeUS
		job, err := publication.ClaimConversion(ctx, acceptInstallationID, 1, "native-converter", time.Minute)
		if err != nil || job == nil {
			t.Fatalf("conversion claim: %+v %v", job, err)
		}
		if err := converter.ConvertAndPrepare(ctx, *job); err != nil {
			t.Fatalf("convert batch %d: %v", index, err)
		}
		publish, err := publication.ClaimPublication(ctx, acceptInstallationID, 1, fixture.tenantID, 0, "native-publisher", time.Minute)
		if err != nil || publish == nil {
			t.Fatalf("publication claim: %+v %v", publish, err)
		}
		if _, err := publisher.VerifyAndPublish(ctx, *publish); err != nil {
			t.Fatal(err)
		}
	}
	if received[0] >= received[batchCount-1] {
		t.Fatalf("expected distinct durable receive times: %v", received)
	}
	var inputs []string
	if err := fixture.pool.QueryRow(ctx, `SELECT array_agg(bundle_id::text ORDER BY bundle_id) FROM bundles WHERE tenant_id=$1 AND valid_to_generation IS NULL`, fixture.tenantID).Scan(&inputs); err != nil {
		t.Fatal(err)
	}
	if len(inputs) != batchCount {
		t.Fatalf("published inputs: %v", inputs)
	}
	_, err = ops.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: inputs})
	if err != nil {
		t.Fatal(err)
	}
	disk := resource.NewBudget(4 << 30)
	runner := app.ProcessCompactionRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"], Gate: gate}
	compactor := maintenance.Workflow{Control: ops, Store: store, Runner: runner, InstallationID: acceptInstallationID, ScratchDir: scratch, Disk: disk}
	task, err := ops.ClaimCompaction(ctx, acceptInstallationID, "native-compactor", time.Minute)
	if err != nil || task == nil {
		t.Fatalf("compaction claim: %+v %v", task, err)
	}
	if err := compactor.CompactAndPrepare(ctx, *task); err != nil {
		t.Fatalf("compact %d batches: %v", batchCount, err)
	}
	prepared, err := ops.ClaimCompaction(ctx, acceptInstallationID, "native-swap", time.Minute)
	if err != nil || prepared == nil || !prepared.Prepared {
		t.Fatalf("prepared claim: %+v %v", prepared, err)
	}
	if _, err := compactor.Swap(ctx, *prepared); err != nil {
		t.Fatal(err)
	}
	queryOps, token := addQueryPrincipal(t, fixture, 1)
	filter, err := query.CanonicalFilter(&query.Node{Op: "constant", Constant: true})
	if err != nil {
		t.Fatal(err)
	}
	spec := model.DatasetSpec{TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindError}, TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 2_000_000, Filter: filter}
	digest, encoded, err := query.DatasetHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := queryOps.CreateSnapshot(ctx, control.CreateSnapshotCommand{SnapshotID: uuid.NewString(), SessionTokenHash: token, TenantID: fixture.tenantID, ProjectIDs: spec.ProjectIDs, DatasetSHA256: digest, DatasetBytes: encoded})
	if err != nil {
		t.Fatal(err)
	}
	catalog := control.CatalogCommand{SessionTokenHash: token, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, DatasetSHA256: digest, DatasetBytes: encoded, TimeBasis: spec.TimeBasis, StartUS: spec.StartUS, EndUS: spec.EndUS, Kinds: spec.Kinds, Limit: 256}
	original, err := queryOps.CatalogPage(ctx, catalog)
	if err != nil || len(original) != 1 || original[0].RowCount != int64(batchCount*perBatch) {
		t.Fatalf("original catalog: %+v %v", original, err)
	}
	if large && (original[0].PayloadBytes <= storage.MaxJournalBytes || original[0].PayloadBytes > engine.MaxBundleFileBytes) {
		t.Fatalf("fixture did not produce a real large bundle: %+v", original[0])
	}
	t.Logf("real compacted analytics_bytes=%d payload_bytes=%d records=%d", original[0].Bytes, original[0].PayloadBytes, original[0].RowCount)
	// Independently read both actual S3 Parquet files through the pinned engine.
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, setting := range []string{"SET memory_limit='64MiB'", "SET max_temp_directory_size='0B'"} {
		if _, err := db.ExecContext(ctx, setting); err != nil {
			t.Fatal(err)
		}
	}
	readPair := func(file model.CatalogFile) []string {
		t.Helper()
		var identities [2][]string
		for index, object := range []struct {
			key, sha string
			bytes    int64
		}{{file.ObjectKey, file.SHA256, file.Bytes}, {file.PayloadObjectKey, file.PayloadSHA256, file.PayloadBytes}} {
			path := filepath.Join(t.TempDir(), "pair.parquet")
			if err := store.DownloadToFile(ctx, object.key, path, object.bytes, object.sha, engine.MaxBundleFileBytes); err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryContext(ctx, `SELECT record_id FROM read_parquet(?) ORDER BY record_id`, path)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				identities[index] = append(identities[index], id)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(identities[0], identities[1]) {
			t.Fatalf("actual pair differs: %v", identities)
		}
		return identities[0]
	}
	originalIDs := readPair(original[0])
	if len(originalIDs) != batchCount*perBatch {
		t.Fatalf("original rows: %v", originalIDs)
	}
	var retainedIDs []string
	rows, err := fixture.pool.Query(ctx, `SELECT record_id FROM issue_occurrences WHERE tenant_id=$1 AND received_time_us=$2 ORDER BY record_id`, fixture.tenantID, received[batchCount-1])
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		retainedIDs = append(retainedIDs, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(retainedIDs) != perBatch {
		t.Fatalf("retained identities=%d err=%v", len(retainedIDs), err)
	}
	retainer := maintenance.RetentionWorkflow{Control: ops, Store: store, Runner: runner, InstallationID: acceptInstallationID, ScratchDir: scratch, Disk: disk}
	current := original[0]
	for index, floor := range []int64{received[batchCount-1], received[batchCount-1] + 1} {
		if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=$1,retention_tick_at=clock_timestamp()`, floor); err != nil {
			t.Fatal(err)
		}
		_, err := ops.ReserveRetention(ctx, control.ReserveRetentionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleID: current.BundleID})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := ops.ClaimRetention(ctx, acceptInstallationID, "native-retainer", time.Minute)
		if err != nil || claim == nil {
			t.Fatalf("retention claim: %+v %v", claim, err)
		}
		canceled, stop := context.WithCancel(ctx)
		stop()
		if err := retainer.Execute(canceled, *claim); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled retention: %v", err)
		}
		stale := *claim
		stale.Authority.Fence++
		if err := retainer.Execute(ctx, stale); !errors.Is(err, control.ErrMaintenanceFence) {
			t.Fatalf("stale retention: %v", err)
		}
		before := store.OperationCounts()
		if err := retainer.Execute(ctx, *claim); err != nil {
			t.Fatal(err)
		}
		if index == 1 && store.OperationCounts() != before {
			t.Fatal("fully expired retirement performed unnecessary S3 I/O")
		}
		if index == 0 {
			prepared, err := ops.ClaimRetention(ctx, acceptInstallationID, "native-retention-swap", time.Minute)
			if err != nil || prepared == nil || !prepared.Prepared {
				t.Fatalf("retention prepared: %+v %v", prepared, err)
			}
			if err := retainer.Execute(ctx, *prepared); err != nil {
				t.Fatal(err)
			}
			if err := retainer.Execute(ctx, *prepared); err != nil {
				t.Fatalf("completed retry: %v", err)
			}
		}
		fresh, err := queryOps.CreateSnapshot(ctx, control.CreateSnapshotCommand{SnapshotID: uuid.NewString(), SessionTokenHash: token, TenantID: fixture.tenantID, ProjectIDs: spec.ProjectIDs, DatasetSHA256: digest, DatasetBytes: encoded})
		if err != nil {
			t.Fatal(err)
		}
		freshCatalog := catalog
		freshCatalog.SnapshotID = fresh.SnapshotID
		files, err := queryOps.CatalogPage(ctx, freshCatalog)
		if err != nil || len(files) != 1-index {
			t.Fatalf("retained catalog: %+v %v", files, err)
		}
		if index == 0 {
			current = files[0]
			if current.RowCount != int64(perBatch) || !reflect.DeepEqual(readPair(current), retainedIDs) {
				t.Fatalf("half-open retention boundary: %+v", current)
			}
		}
		pinned, err := queryOps.CatalogPage(ctx, catalog)
		if err != nil || !reflect.DeepEqual(pinned, original) || !reflect.DeepEqual(readPair(pinned[0]), originalIDs) {
			t.Fatalf("pinned catalog changed: %+v %v", pinned, err)
		}
		if disk.Used() != 0 {
			t.Fatalf("disk permit leaked: %d", disk.Used())
		}
		entries, err := os.ReadDir(scratch)
		if err != nil || len(entries) != 0 {
			t.Fatalf("native scratch leaked: %v %v", entries, err)
		}
	}
	var published, currentRows, deleted int64
	if err := fixture.pool.QueryRow(ctx, `SELECT published_seq,(SELECT count(*) FROM bundles WHERE tenant_id=$1 AND valid_to_generation IS NULL),(SELECT count(*) FROM object_intents WHERE tenant_id=$1 AND state='deleted') FROM lanes WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID).Scan(&published, &currentRows, &deleted); err != nil {
		t.Fatal(err)
	}
	if published != int64(batchCount) || currentRows != 0 || deleted != 0 {
		t.Fatalf("retirement changed cut or physically deleted: %d %d %d", published, currentRows, deleted)
	}
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM memberships WHERE tenant_id=$1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := queryOps.CatalogPage(ctx, catalog); !errors.Is(err, control.ErrUnauthenticated) {
		t.Fatalf("revoked pinned read: %v", err)
	}
	t.Logf("real ingest/publication/compaction/mixed+expired retention passed; pinned paired identities stable; no physical GC; S3=%+v", store.OperationCounts())
}
