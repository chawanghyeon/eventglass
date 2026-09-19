package api

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

func TestFinalizeDetailRejectsMissingDuplicateAndRestoresTypedPayload(t *testing.T) {
	recordID := strings.Repeat("a", 64)
	line := `{"record_id":"` + recordID + `","raw_json":"{\"message\":\"full\"}","envelope_sdk_json":null,` +
		`"normalization_warnings_json":"[\"normalized\"]","canonical_metadata_json":"{\"record_id\":\"` + recordID + `\",\"tenant_id\":1,\"project_id\":2,\"event_time_us\":3,\"arrival_time_us\":4}",` +
		`"received_time_us":5,"lane_id":0,"batch_seq":6,"record_ordinal":7,"issue_id":null,"grouping_version":null}`
	path := filepath.Join(t.TempDir(), "detail.jsonl")
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := finalizeDetail(path, engine.QueryResultPlan{Kind: "detail", Limit: 1, RecordID: recordID}, "read-token")
	if err != nil {
		t.Fatal(err)
	}
	if result.ReadToken != "read-token" || result.EnvelopeSDK != nil || result.Raw["message"] != "full" || result.Record["tenant_id"] != "1" || result.Record["received_time_us"] != "5" {
		t.Fatalf("result=%#v", result)
	}
	if warnings, ok := result.Record["warnings"].([]any); !ok || len(warnings) != 1 || warnings[0] != "normalized" {
		t.Fatalf("warnings=%#v", result.Record["warnings"])
	}
	empty := filepath.Join(t.TempDir(), "empty.jsonl")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := finalizeDetail(empty, engine.QueryResultPlan{RecordID: recordID}, "token"); err != ErrPublicQueryNotFound {
		t.Fatalf("empty err=%v", err)
	}
	if err := os.WriteFile(path, []byte(line+"\n"+line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := finalizeDetail(path, engine.QueryResultPlan{RecordID: recordID}, "token"); err == nil {
		t.Fatal("duplicate record detail accepted")
	}
}

func TestDecodeJSONLineRejectsTrailingJSON(t *testing.T) {
	if _, err := decodeJSONLine([]byte(`{"ok":true} {"extra":true}`)); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}
