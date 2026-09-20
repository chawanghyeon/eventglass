package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestCompactPreservesExactPairedIdentityAndBounds(t *testing.T) {
	root := t.TempDir()
	first := conversionRecord("a", model.KindLog, 1_700_000_000_000_000, 0)
	second := conversionRecord("b", model.KindLog, 1_700_000_001_000_000, 0)
	second.BatchSeq = 5
	second.ReceivedTimeUS++

	convertOne := func(name string, staged StageRecord) ConvertedBundle {
		directory := filepath.Join(root, name)
		if err := ensureEngineDirectory(directory); err != nil {
			t.Fatal(err)
		}
		request := ConversionRequest{
			Version: ConversionProtocolVersion, StagePath: writeConversionStage(t, directory, []StageRecord{staged}),
			OutputDirectory: filepath.Join(directory, "output"), SpillDirectory: filepath.Join(directory, "spill"),
			TenantID: 1, LaneID: 3, BatchSeq: staged.BatchSeq, BatchID: staged.BatchID, SelectedRecords: 1,
		}
		var bundle ConvertedBundle
		if _, err := Convert(context.Background(), request, func(value ConvertedBundle) error { bundle = value; return nil }); err != nil {
			t.Fatal(err)
		}
		return bundle
	}
	left, right := convertOne("left", first), convertOne("right", second)
	request := CompactionRequest{
		Version: CompactionProtocolVersion, TenantID: 1, LaneID: 3, SchemaVersion: 1, GroupingVersion: 1,
		EventDay: left.EventDay, Kind: model.KindLog,
		Inputs: []CompactionInput{
			{BundleID: "left", AnalyticsPath: left.Analytics.Path, PayloadPath: left.Payload.Path, IdentitySHA256: left.IdentitySHA256},
			{BundleID: "right", AnalyticsPath: right.Analytics.Path, PayloadPath: right.Payload.Path, IdentitySHA256: right.IdentitySHA256},
		},
		OutputDirectory: filepath.Join(root, "compact-output"), SpillDirectory: filepath.Join(root, "compact-spill"),
		NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20,
	}
	result, err := Compact(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256([]byte(strings.Repeat("a", 64) + "\n" + strings.Repeat("b", 64) + "\n"))
	if !strings.HasPrefix(result.DuckDBVersion, "v2.0.0-dev84020") || result.Bundle.RowCount != 2 || result.Bundle.IdentitySHA256 != hex.EncodeToString(expected[:]) || result.Bundle.Analytics.MinBatchSeq != 4 || result.Bundle.Analytics.MaxBatchSeq != 5 {
		t.Fatalf("result=%#v", result)
	}
	if err := storage.VerifyFile(result.Bundle.Analytics.Path, result.Bundle.Analytics.Evidence); err != nil {
		t.Fatal(err)
	}
	if err := storage.VerifyFile(result.Bundle.Payload.Path, result.Bundle.Payload.Evidence); err != nil {
		t.Fatal(err)
	}

	request.OutputDirectory = filepath.Join(root, "mismatch-output")
	request.SpillDirectory = filepath.Join(root, "mismatch-spill")
	request.Inputs[0].PayloadPath = right.Payload.Path
	if _, err := Compact(context.Background(), request); err == nil {
		t.Fatal("mismatched analytics/payload pair was compacted")
	}
}

func ensureEngineDirectory(path string) error {
	return os.MkdirAll(path, 0o700)
}
