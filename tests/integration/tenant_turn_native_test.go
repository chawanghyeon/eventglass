//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/google/uuid"
)

// Real product worker, not a model dispatcher. One older twelve-job tenant and
// one four-job tenant are ready before startup. The private-schema trigger
// records actual conversion claims only; it is bounded test instrumentation,
// not a product hook or a throughput measurement.
func TestConversionTenantTurnsActualWorker(t *testing.T) {
	fixture := setupAcceptFixture(t, 1791)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	second := addSchedulingTenant(t, ctx, fixture, 1792)
	prefix := "tenant-turn-native-" + uuid.NewString()
	store := integrationStore(t, prefix)
	ops, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[int64][]string{}
	for index, target := range []*acceptFixture{fixture, second} {
		workflow := integrationWorkflow(t, target, ops, store, fmt.Sprintf("tenant-turn-%d", index))
		count := 12
		if index == 1 {
			count = 4
		}
		for batch := range count {
			command := integrationCommand(t, target, target.projectID, target.auth, target.uuidForLane(batch%16), fmt.Sprintf("tenant turn %d/%d", index, batch))
			command.Request, err = ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
				"event_id": fmt.Sprintf("%032x", index*16+batch+1), "message": fmt.Sprintf("tenant turn %d/%d", index, batch),
			}}}}, ingest.NormalizeOptions{TenantID: target.tenantID, ProjectID: target.projectID, AcceptanceID: command.Request.AcceptanceID, ArrivalTime: time.Unix(1, 0)})
			if err != nil {
				t.Fatal(err)
			}
			results := workflow.Process(ctx, []ingest.Command{command})
			if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptedCount != 1 {
				t.Fatalf("durable accept: %+v", results)
			}
			expected[target.tenantID] = append(expected[target.tenantID], command.Request.Records[0].RecordID)
		}
		slices.Sort(expected[target.tenantID])
	}
	if _, err := fixture.pool.Exec(ctx, `CREATE TABLE test_claim_order (
		ordinal BIGSERIAL PRIMARY KEY CHECK(ordinal<=64),tenant_id BIGINT NOT NULL,job_id UUID NOT NULL,attempt INTEGER NOT NULL);
		CREATE FUNCTION test_record_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		  IF NEW.state='running' AND NEW.prepared_output_id IS NULL AND NEW.attempt>OLD.attempt THEN
		    INSERT INTO test_claim_order(tenant_id,job_id,attempt) VALUES(NEW.tenant_id,NEW.job_id,NEW.attempt);
		  END IF;
		  RETURN NEW;
		END $$;
		CREATE TRIGGER test_claim_order AFTER UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION test_record_claim()`); err != nil {
		t.Fatal(err)
	}
	worker := startMaintenanceRuntime(t, ctx, fixture, prefix)
	defer worker.stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var completed int
		if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state='completed'`).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 16 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("publication incomplete: %d/16: %v", completed, ctx.Err())
		case <-worker.done:
			t.Fatalf("worker stopped early: %v", worker.err)
		case <-ticker.C:
		}
	}
	worker.stop()
	var turns []int64
	if err := fixture.pool.QueryRow(ctx, `SELECT array_agg(tenant_id ORDER BY ordinal) FROM test_claim_order`).Scan(&turns); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual single-worker conversion claim order=%v", turns)
	if len(turns) != 16 {
		t.Fatalf("unexpected retries/claims=%d", len(turns))
	}
	// Read every actual current analytics and payload file after shutdown. The
	// expected IDs came from submitted normalized records, not catalog counts.
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
	for tenant, identities := range expected {
		for _, role := range []string{"analytics", "payload"} {
			rows, err := fixture.pool.Query(ctx, `SELECT oi.object_key,f.bytes,f.full_sha256 FROM files f JOIN bundles b USING(bundle_id)
				JOIN object_intents oi ON oi.tenant_id=f.tenant_id AND oi.intent_id=f.intent_id AND oi.state='referenced'
				WHERE b.tenant_id=$1 AND b.valid_to_generation IS NULL AND f.role=$2 ORDER BY b.bundle_id`, tenant, role)
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
				path := filepath.Join(t.TempDir(), "pair.parquet")
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
			result, err := db.QueryContext(ctx, `SELECT record_id FROM read_parquet(?) ORDER BY record_id`, paths)
			if err != nil {
				t.Fatal(err)
			}
			var actual []string
			for result.Next() {
				var id string
				if err := result.Scan(&id); err != nil {
					result.Close()
					t.Fatal(err)
				}
				actual = append(actual, id)
			}
			err = result.Err()
			result.Close()
			if err != nil || !slices.Equal(actual, identities) {
				t.Fatalf("tenant=%d role=%s actual=%v expected=%v error=%v", tenant, role, actual, identities, err)
			}
		}
	}
	for index := range 8 {
		want := fixture.tenantID
		if index%2 == 1 {
			want = second.tenantID
		}
		if turns[index] != want {
			t.Fatalf("ready tenant displaced at turn=%d: actual=%v", index, turns)
		}
	}
}
