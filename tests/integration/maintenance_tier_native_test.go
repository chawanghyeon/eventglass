//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/maintenance"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/google/uuid"
)

// Actual durable ingestion and paired Parquet/S3 bytes, not a catalog-size stub.
// Timed selection/reservation/prepare/swap excludes fixture creation and the
// independent post-swap identity oracle. Fresh IDs/timestamps mean the five
// logical repetitions are not byte-identical native-input benchmarks.
func TestCompactionSizeCohortNativeRewrite(t *testing.T) {
	for sample := range 5 {
		t.Run(fmt.Sprint(sample), func(t *testing.T) {
			binary := requiredEnvironment(t, "EVENTGLASS_TEST_BINARY")["EVENTGLASS_TEST_BINARY"]
			fixture := setupAcceptFixture(t, 1774)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			store := integrationStore(t, "native-size-cohort-"+uuid.NewString())
			ingestion, err := control.NewIngestOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			publication, err := control.NewPublicationOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			operations, err := control.NewMaintenanceOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			resources, err := app.ResourcesForRoles(map[app.Role]bool{app.RoleWorker: true})
			if err != nil {
				t.Fatal(err)
			}
			gate, scratch := app.NewNativeTaskGate(), filepath.Join(t.TempDir(), "work")
			accept := integrationWorkflow(t, fixture, ingestion, store, "size-cohort")
			converter := ingest.DurableConversionWorkflow{Control: publication, Store: store,
				Runner: app.ProcessConversionRunner{BinaryPath: binary, Gate: gate}, InstallationID: acceptInstallationID, ScratchDir: scratch, Disk: resources.Disk}
			publisher := ingest.DurablePublicationWorkflow{Control: publication, Store: store}
			random := rand.New(rand.NewSource(1774))
			for batch := range 9 {
				command := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(0), "unused")
				count := 1
				if batch == 0 {
					count = 4
				}
				items := make([]sdk.Item, count)
				for ordinal := range items {
					value := map[string]any{"event_id": fmt.Sprintf("%032x", batch*4+ordinal+1), "message": fmt.Sprintf("size cohort %d/%d", batch, ordinal)}
					if batch == 0 {
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
				results := accept.Process(ctx, []ingest.Command{command})
				if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptedCount != count {
					t.Fatalf("durable ACK: %+v", results)
				}
				job, err := publication.ClaimConversion(ctx, acceptInstallationID, 1, "cohort-converter", time.Minute)
				if err != nil || job == nil {
					t.Fatalf("conversion: %v %v", job, err)
				}
				if err := converter.ConvertAndPrepare(ctx, *job); err != nil {
					t.Fatal(err)
				}
				publish, err := publication.ClaimPublication(ctx, acceptInstallationID, 1, fixture.tenantID, 0, "cohort-publisher", time.Minute)
				if err != nil || publish == nil {
					t.Fatalf("publication: %v %v", publish, err)
				}
				if _, err := publisher.VerifyAndPublish(ctx, *publish); err != nil {
					t.Fatal(err)
				}
			}
			var largeID string
			var largeBytes, tinyBytes int64
			if err := fixture.pool.QueryRow(ctx, `SELECT b.bundle_id::text,sum(f.bytes) FROM bundles b JOIN files f USING(bundle_id)
				WHERE b.tenant_id=$1 AND b.valid_from_generation=1 GROUP BY b.bundle_id`, fixture.tenantID).Scan(&largeID, &largeBytes); err != nil {
				t.Fatal(err)
			}
			if err := fixture.pool.QueryRow(ctx, `SELECT sum(f.bytes) FROM bundles b JOIN files f USING(bundle_id)
				WHERE b.tenant_id=$1 AND b.valid_from_generation>1`, fixture.tenantID).Scan(&tinyBytes); err != nil {
				t.Fatal(err)
			}
			if largeBytes < 1<<20 || tinyBytes >= 128<<10 {
				t.Fatalf("fixture does not cover separate real size cohorts: large=%d tiny=%d", largeBytes, tinyBytes)
			}
			compactor := maintenance.Workflow{Control: operations, Store: store,
				Runner: app.ProcessCompactionRunner{BinaryPath: binary, Gate: gate}, InstallationID: acceptInstallationID, ScratchDir: scratch, Disk: resources.Disk}
			// Keep fixture CPU quota consumption out of the timed workflow.
			time.Sleep(150 * time.Millisecond)
			beforeIO := store.OperationCounts()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			started := time.Now()
			candidate, err := operations.FindCompactionCandidate(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1,
				TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: candidate.BundleIDs})
			if err != nil {
				t.Fatal(err)
			}
			task, err := operations.ClaimCompaction(ctx, acceptInstallationID, "cohort-compactor", time.Minute)
			if err != nil || task == nil {
				t.Fatalf("compaction claim: %v %v", task, err)
			}
			if err := compactor.CompactAndPrepare(ctx, *task); err != nil {
				t.Fatal(err)
			}
			prepared, err := operations.ClaimCompaction(ctx, acceptInstallationID, "cohort-swap", time.Minute)
			if err != nil || prepared == nil || !prepared.Prepared {
				t.Fatalf("swap claim: %v %v", prepared, err)
			}
			if _, err := compactor.Swap(ctx, *prepared); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			runtime.ReadMemStats(&after)
			afterIO := store.OperationCounts()
			t.Logf("sample=%d selected=%d large_input_bytes=%d tiny_input_bytes=%d workflow_ns=%d go_bytes=%d go_allocs=%d S3_GET=%d S3_GET_bytes=%d S3_PUT=%d S3_PUT_bytes=%d S3_HEAD=%d S3_Range=%d", sample, len(candidate.BundleIDs), largeBytes, tinyBytes, elapsed.Nanoseconds(), after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs,
				afterIO.FullGetRequests-beforeIO.FullGetRequests, afterIO.FullGetBytes-beforeIO.FullGetBytes, afterIO.PutRequests-beforeIO.PutRequests, afterIO.PutBytes-beforeIO.PutBytes, afterIO.HeadRequests-beforeIO.HeadRequests, afterIO.RangeRequests-beforeIO.RangeRequests)
			// Independent persisted occurrence IDs versus both actual current
			// Parquet roles: the quiet large pair must remain queryable too.
			var expected []string
			if err := fixture.pool.QueryRow(ctx, `SELECT array_agg(record_id ORDER BY record_id) FROM issue_occurrences WHERE tenant_id=$1`, fixture.tenantID).Scan(&expected); err != nil {
				t.Fatal(err)
			}
			if len(expected) != 12 {
				t.Fatalf("accepted identity count=%d", len(expected))
			}
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
			for _, role := range []string{"analytics", "payload"} {
				rows, err := fixture.pool.Query(ctx, `SELECT oi.object_key,f.bytes,f.full_sha256 FROM files f JOIN bundles b USING(bundle_id)
					JOIN object_intents oi ON oi.tenant_id=f.tenant_id AND oi.intent_id=f.intent_id AND oi.state='referenced'
					WHERE b.tenant_id=$1 AND b.valid_to_generation IS NULL AND f.role=$2 ORDER BY b.bundle_id`, fixture.tenantID, role)
				if err != nil {
					t.Fatal(err)
				}
				var paths []string
				for rows.Next() {
					var key, sha string
					var bytes int64
					if err := rows.Scan(&key, &bytes, &sha); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					path := filepath.Join(t.TempDir(), "oracle.parquet")
					if err := store.DownloadToFile(ctx, key, path, bytes, sha, engine.MaxBundleFileBytes); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					paths = append(paths, path)
				}
				err = rows.Err()
				rows.Close()
				if err != nil {
					t.Fatal(err)
				}
				actualRows, err := db.QueryContext(ctx, `SELECT record_id FROM read_parquet(?) ORDER BY record_id`, paths)
				if err != nil {
					t.Fatal(err)
				}
				var actual []string
				for actualRows.Next() {
					var id string
					if err := actualRows.Scan(&id); err != nil {
						actualRows.Close()
						t.Fatal(err)
					}
					actual = append(actual, id)
				}
				err = actualRows.Err()
				actualRows.Close()
				if err != nil || !slices.Equal(actual, expected) {
					t.Fatalf("%s actual IDs=%v expected=%v error=%v", role, actual, expected, err)
				}
				for _, path := range paths {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				}
			}
			var usage syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
				t.Fatal(err)
			}
			t.Logf("parent_maxrss=%d OS=%s (includes fixture and oracle; excludes child RSS)", usage.Maxrss, runtime.GOOS)
			if slices.Contains(candidate.BundleIDs, largeID) || len(candidate.BundleIDs) != 8 {
				t.Fatalf("quiet large pair was unnecessarily rewritten: selected=%d", len(candidate.BundleIDs))
			}
		})
	}
}
