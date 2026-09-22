//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/google/uuid"
)

// A real native child and real durable ACK must obey the same bounded-output
// contract as the conversion interface. A callback invoked only after every
// partition has been written does not provide upload backpressure.
func TestConversionEndToEndOneOutstandingPair(t *testing.T) {
	testConversionEventDays(t, 8)
}

func TestConversionEndToEndTenThousandEventDays(t *testing.T) {
	testConversionEventDays(t, 10_000)
}

func testConversionEventDays(t *testing.T, days int) {
	t.Helper()
	environment := requiredEnvironment(t, "EVENTGLASS_TEST_BINARY")
	fixture := setupAcceptFixture(t, 1790)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	store := integrationStore(t, "conversion-stream-"+uuid.NewString())
	ingestOps, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := control.NewPublicationOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	command := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(0), "unused")
	eventClock := time.Now().UTC()
	var items []sdk.Item
	if days > 1000 {
		// One legal log container, not ten thousand envelope items (whose
		// separate protocol limit is1000). Received time remains current.
		logs := make([]any, days)
		for index := range logs {
			logs[index] = map[string]any{"body": fmt.Sprintf("streamed day %d", index),
				"timestamp": eventClock.AddDate(0, 0, -index).Format(time.RFC3339Nano), "level": "info"}
		}
		items = []sdk.Item{{Ordinal: 0, Type: "log", Header: map[string]any{"item_count": json.Number(fmt.Sprint(days))},
			Value: map[string]any{"items": logs}}}
	} else {
		items = make([]sdk.Item, days)
		for index := range items {
			items[index] = sdk.Item{Ordinal: index, Type: "event", Value: map[string]any{
				"event_id": fmt.Sprintf("%032x", index+1), "message": fmt.Sprintf("streamed day %d", index),
				"timestamp": eventClock.AddDate(0, 0, -index).Format(time.RFC3339Nano),
			}}
		}
	}
	command.Request, err = ingest.NormalizeEnvelope(sdk.Envelope{Items: items}, ingest.NormalizeOptions{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID,
		AcceptanceID: command.Request.AcceptanceID, ArrivalTime: eventClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptor := integrationWorkflow(t, fixture, ingestOps, store, "streaming-conversion")
	results := acceptor.Process(ctx, []ingest.Command{command})
	if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptedCount != days {
		t.Fatalf("durable ACK: %+v", results)
	}
	job, err := publication.ClaimConversion(ctx, acceptInstallationID, 1, "streaming-converter", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("conversion claim: %+v %v", job, err)
	}
	expected := make(map[string]string, days)
	for _, record := range command.Request.Records {
		day := time.UnixMicro(record.EventTimeUS).UTC().Format("2006-01-02")
		expected[day] = fmt.Sprintf("%x", sha256.Sum256([]byte(record.RecordID+"\n")))
	}
	observed := &pairBoundRunner{native: app.ProcessConversionRunner{BinaryPath: environment["EVENTGLASS_TEST_BINARY"], Gate: app.NewNativeTaskGate()}, expected: expected, log: t.Logf}
	scratch := filepath.Join(t.TempDir(), "conversion")
	resources, err := app.ResourcesForRoles(map[app.Role]bool{app.RoleWorker: true})
	if err != nil {
		t.Fatal(err)
	}
	converter := ingest.DurableConversionWorkflow{Control: publication, Store: store, Runner: observed,
		InstallationID: acceptInstallationID, ScratchDir: scratch, Disk: resources.Disk}
	if err := conversionWithHeartbeat(ctx, publication, job.Authority, func(taskCtx context.Context) error {
		return converter.ConvertAndPrepare(taskCtx, *job)
	}); err != nil {
		t.Fatalf("convert acknowledged multi-day batch: %v", err)
	}
	if observed.pairs != days || len(observed.expected) != 0 {
		t.Fatalf("observed pairs=%d, want %d", observed.pairs, days)
	}
	publish, err := publication.ClaimPublication(ctx, acceptInstallationID, 1, fixture.tenantID, 0, "streaming-publisher", time.Minute)
	if err != nil || publish == nil {
		t.Fatalf("publication claim: %+v %v", publish, err)
	}
	publisher := ingest.DurablePublicationWorkflow{Control: publication, Store: store}
	if err := conversionWithHeartbeat(ctx, publication, publish.Authority, func(taskCtx context.Context) error {
		_, err := publisher.VerifyAndPublish(taskCtx, *publish)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var bundles, rows, eventDays int64
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*),COALESCE(sum(row_count),0),count(DISTINCT event_day)
		FROM bundles WHERE tenant_id=$1 AND valid_to_generation IS NULL`, fixture.tenantID).Scan(&bundles, &rows, &eventDays); err != nil {
		t.Fatal(err)
	}
	if bundles != int64(days) || rows != int64(days) || eventDays != int64(days) {
		t.Fatalf("published partition coverage bundles=%d rows=%d days=%d, want %d", bundles, rows, eventDays, days)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 || resources.Disk.Used() != 0 {
		t.Fatalf("joined conversion retained scratch: entries=%d reserved=%d err=%v", len(entries), resources.Disk.Used(), err)
	}
	t.Logf("real durable ACK/publication: days=%d exact per-day record identity; one outstanding pair; zero residual disk reservation; S3=%+v", days, store.OperationCounts())
}

func conversionWithHeartbeat(ctx context.Context, operations *control.PublicationOperations, authority control.JobAuthority, run func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := operations.Heartbeat(ctx, authority, time.Minute); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := run(ctx)
	cancel()
	<-done
	return err
}

type pairBoundRunner struct {
	native   app.ProcessConversionRunner
	pairs    int
	expected map[string]string
	log      func(string, ...any)
}

func (runner *pairBoundRunner) Run(ctx context.Context, request engine.ConversionRequest, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	return runner.native.Run(ctx, request, func(bundle engine.ConvertedBundle) error {
		entries, err := os.ReadDir(request.OutputDirectory)
		if err != nil {
			return err
		}
		if len(entries) != 2 {
			return fmt.Errorf("before upload of pair %d: %d output files, require exactly one outstanding pair", bundle.Index, len(entries))
		}
		if expected, exists := runner.expected[bundle.EventDay]; !exists || bundle.RowCount != 1 || bundle.IdentitySHA256 != expected {
			return fmt.Errorf("partition %s has an unexpected record identity or duplicate output", bundle.EventDay)
		}
		if err := emit(bundle); err != nil {
			return err
		}
		delete(runner.expected, bundle.EventDay)
		runner.pairs++
		if runner.pairs%1000 == 0 {
			runner.log("verified and uploaded %d single-record day pairs", runner.pairs)
		}
		return nil
	})
}
