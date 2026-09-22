package crash_test

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestPublicationLatencyAndFileSizeDistribution(t *testing.T) {
	fixture := setupCrashFixture(t, 740)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}
	ingestOperations, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := ingest.NewWorkflow(ingest.WorkflowConfig{
		Control: ingestOperations, Store: fixture.store, InstallationID: crashInstallationID, StorageGeneration: 1,
		ProcessID: crashUUID(fixture.tenantID, 800), TempDir: filepath.Join(t.TempDir(), "producer"), SpoolBudget: resource.NewBudget(4 * storage.MaxJournalBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	publication, err := control.NewPublicationOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	converter := ingest.DurableConversionWorkflow{
		Control: publication, Store: fixture.store, Runner: app.ProcessConversionRunner{BinaryPath: "/out/eventglass-go"},
		InstallationID: crashInstallationID, ScratchDir: filepath.Join(t.TempDir(), "converter"), Disk: resource.NewBudget(4 << 30),
	}
	publisher := ingest.DurablePublicationWorkflow{Control: publication, Store: fixture.store}
	latencies := make([]time.Duration, 0, 5)
	for index := 0; index < cap(latencies); index++ {
		acceptanceID := crashUUID(fixture.tenantID, 100+index)
		normalized, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
			"event_id": fmt.Sprintf("%032x", fixture.tenantID*100+int64(index)), "message": fmt.Sprintf("publication sample %d", index),
		}}}}, ingest.NormalizeOptions{TenantID: fixture.tenantID, ProjectID: fixture.projectID, AcceptanceID: acceptanceID, ArrivalTime: time.Unix(int64(index+10), 0)})
		if err != nil {
			t.Fatal(err)
		}
		results := producer.Process(ctx, []ingest.Command{{Request: normalized, Authorization: ingest.Authorization{
			TenantID: fixture.tenantID, ProjectID: fixture.projectID, KeyHash: fixture.keyHash,
			TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1,
		}}})
		if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptedCount != 1 {
			t.Fatalf("sample %d ACK=%#v", index, results)
		}
		ackedAt := time.Now()
		job, err := publication.ClaimConversion(ctx, crashInstallationID, 1, fmt.Sprintf("converter-%d", index), time.Minute)
		if err != nil || job == nil {
			t.Fatalf("sample %d conversion=%#v err=%v", index, job, err)
		}
		if err := converter.ConvertAndPrepare(ctx, *job); err != nil {
			t.Fatalf("sample %d convert=%v", index, err)
		}
		prepared, err := publication.ClaimPublication(ctx, crashInstallationID, 1, fixture.tenantID, results[0].Receipt.LaneID, fmt.Sprintf("publisher-%d", index), time.Minute)
		if err != nil || prepared == nil {
			t.Fatalf("sample %d publication=%#v err=%v", index, prepared, err)
		}
		if _, err := publisher.VerifyAndPublish(ctx, *prepared); err != nil {
			t.Fatalf("sample %d publish=%v", index, err)
		}
		latencies = append(latencies, time.Since(ackedAt))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95Index := int(math.Ceil(float64(len(latencies))*0.95)) - 1
	p95 := latencies[p95Index]
	if p95 > 5*time.Second {
		t.Fatalf("ACK-to-visible p95=%s exceeds 5s", p95)
	}
	rows, err := fixture.pool.Query(ctx, `SELECT role,bytes FROM files WHERE tenant_id=$1 ORDER BY bytes,role`, fixture.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	fileCount := 0
	var minimum, maximum int64
	for rows.Next() {
		var role string
		var size int64
		if err := rows.Scan(&role, &size); err != nil {
			t.Fatal(err)
		}
		if size <= 0 || size > 128<<20 || (role != "analytics" && role != "payload") {
			t.Fatalf("invalid published file role=%s bytes=%d", role, size)
		}
		if minimum == 0 || size < minimum {
			minimum = size
		}
		if size > maximum {
			maximum = size
		}
		fileCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fileCount != len(latencies)*2 {
		t.Fatalf("file count=%d samples=%d", fileCount, len(latencies))
	}
	t.Logf("G03 ACK-to-visible samples=%d p95=%s file_bytes_min=%d file_bytes_max=%d files=%d", len(latencies), p95, minimum, maximum, fileCount)
}
