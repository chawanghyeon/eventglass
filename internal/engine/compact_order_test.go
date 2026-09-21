package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

// DESIGN5.2 and Convert sort paired files by project, service(NULLS FIRST),
// event time and record ID. Receipt/lane order is not that physical contract.
func TestCompactionPreservesCanonicalPhysicalOrder(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	serviceA, serviceZ, serviceEmpty := "a", "z", ""
	const eventTime = int64(1_700_000_000_000_000)
	records := make([]StageRecord, 6)
	for index := range records {
		records[index] = conversionRecord(string(rune('a'+index)), model.KindLog, eventTime, index%3)
		records[index].BatchSeq = int64(4 + index/3)
		records[index].ReceivedTimeUS = eventTime + 1000 + int64(index/3)
	}
	records[0].Record.ProjectID, records[0].Record.Service = 20, &serviceA
	records[1].Record.Service = &serviceZ
	records[2].Record.EventTimeUS = eventTime + 30 // NULL service still sorts first.
	records[3].Record.Service, records[3].Record.EventTimeUS = &serviceEmpty, eventTime+20
	records[4].Record.Service, records[4].Record.EventTimeUS = &serviceA, eventTime+10
	records[5].Record.Service, records[5].Record.EventTimeUS = &serviceA, eventTime+20

	db, err := Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	physicalIDs := func(path string) []string {
		t.Helper()
		rows, err := db.QueryContext(ctx, `SELECT record_id FROM read_parquet(?,file_row_number=true) ORDER BY file_row_number`, path)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id[:1])
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	request := CompactionRequest{Version: CompactionProtocolVersion, TenantID: 1, LaneID: 3, SchemaVersion: 1, GroupingVersion: 1,
		EventDay: "2023-11-14", Kind: model.KindLog, OutputDirectory: filepath.Join(root, "merged"), SpillDirectory: filepath.Join(root, "spill"), NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20}
	for batch := range 2 {
		dir := filepath.Join(root, fmt.Sprint(batch))
		if err := ensureEngineDirectory(dir); err != nil {
			t.Fatal(err)
		}
		input := records[batch*3 : (batch+1)*3]
		_, err := Convert(ctx, ConversionRequest{Version: ConversionProtocolVersion, TenantID: 1, LaneID: 3, BatchSeq: input[0].BatchSeq, BatchID: input[0].BatchID, SelectedRecords: 3,
			StagePath: writeConversionStage(t, dir, input), OutputDirectory: filepath.Join(dir, "output"), SpillDirectory: filepath.Join(dir, "spill"), NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20}, func(bundle ConvertedBundle) error {
			want := []string{"c", "b", "a"}
			if batch == 1 {
				want = []string{"d", "e", "f"}
			}
			for _, path := range []string{bundle.Analytics.Path, bundle.Payload.Path} {
				if got := physicalIDs(path); !reflect.DeepEqual(got, want) {
					t.Fatalf("conversion physical order=%v want=%v", got, want)
				}
			}
			request.Inputs = append(request.Inputs, CompactionInput{BundleID: fmt.Sprint(batch), AnalyticsPath: bundle.Analytics.Path, PayloadPath: bundle.Payload.Path, IdentitySHA256: bundle.IdentitySHA256})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := Compact(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split("c d e f b a", " ")
	for _, path := range []string{result.Bundle.Analytics.Path, result.Bundle.Payload.Path} {
		if got := physicalIDs(path); !reflect.DeepEqual(got, want) {
			t.Errorf("%s physical order=%v want=%v", filepath.Base(path), got, want)
		}
	}
}
