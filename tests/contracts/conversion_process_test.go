package contracts

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestEngineChildConversionProtocol(t *testing.T) {
	binary := eventglassBinary(t)
	root := t.TempDir()
	stagePath := filepath.Join(root, "selected.jsonl")
	stage, err := os.OpenFile(stagePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	record := model.Record{
		TenantID: 1, ProjectID: 2, RecordID: strings.Repeat("a", 64), AcceptanceID: "00000000-0000-4000-8000-000000000111",
		Kind: model.KindLog, EventTimeUS: 1_700_000_000_000_000, ArrivalTimeUS: 1_700_000_000_000_001,
		Message: "child", Raw: json.RawMessage(`{"body":"child"}`), Attrs: []model.Attribute{}, SearchValues: []string{"child"}, Warnings: []string{},
		SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
	}
	staged := engine.StageRecord{
		Version: 1, GlobalOrdinal: 0, BatchID: "00000000-0000-4000-8000-000000000222", LaneID: 0, BatchSeq: 1,
		ReceivedTimeUS: 1_700_000_000_000_002, GroupingVersion: 1, Record: record,
	}
	if err := json.NewEncoder(stage).Encode(staged); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	request := engine.ConversionRequest{
		Version: 1, StagePath: stagePath, OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 0, BatchSeq: 1, BatchID: staged.BatchID, SelectedRecords: 1,
		NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20,
	}
	var bundles []engine.ConvertedBundle
	summary, err := (app.ProcessConversionRunner{BinaryPath: binary}).Run(context.Background(), request, func(bundle engine.ConvertedBundle) error {
		bundles = append(bundles, bundle)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(summary.DuckDBVersion, "v2.0.0-dev84020") || summary.BundleCount != 1 || len(bundles) != 1 || bundles[0].RowCount != 1 {
		t.Fatalf("summary=%#v bundles=%#v", summary, bundles)
	}
}
