package issues

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func errorRecord(t *testing.T, projectID int64, raw map[string]any, message string, template *string) model.Record {
	t.Helper()
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	return model.Record{Kind: model.KindError, ProjectID: projectID, Raw: encoded, Message: message, MessageTemplate: template}
}

func TestGroupingWorkedExampleAndCustomDefault(t *testing.T) {
	template := "failed {id}"
	record := errorRecord(t, 42, map[string]any{"message": "failed 19"}, "failed 19", &template)
	group, err := GroupRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	want := `["eventglass-grouping-v1","42",["message","failed {id}"]]`
	if string(group.CanonicalBytes) != want || group.IssueID != "83cc729a4dd842d933612b45ee1d535afe02381588ebc011a0a678581801f334" || group.FingerprintSHA != group.IssueID { // pragma: allowlist secret -- public SHA-256 golden
		t.Fatalf("group=%#v", group)
	}
	record.Raw = json.RawMessage(`{"message":"failed 19","fingerprint":["tenant-rule","{{default}}"]}`)
	group, err = GroupRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	want = `["eventglass-grouping-v1","42",["custom",[["literal","tenant-rule"],["default",["message","failed {id}"]]]]]`
	if string(group.CanonicalBytes) != want || group.IssueID != "4a4d2bc55b587ca00842eecd40395accbb80e3acbc60ed6715b457b82b26ad53" { // pragma: allowlist secret -- public SHA-256 golden
		t.Fatalf("custom group=%s id=%s", group.CanonicalBytes, group.IssueID)
	}
}

func TestGroupingStackChainLastEightInAppAndSlashNormalization(t *testing.T) {
	frames := make([]any, 12)
	for index := range frames {
		frames[index] = map[string]any{
			"module": "module", "function": "fn" + string(rune('a'+index)), "filename": `C:\src\file.go`,
			"lineno": index + 10, "in_app": index >= 2,
		}
	}
	raw := map[string]any{"exception": map[string]any{"values": []any{
		map[string]any{"type": "Outer", "value": "outer"},
		map[string]any{"type": "Inner", "value": "inner", "stacktrace": map[string]any{"frames": frames}},
	}}}
	group, err := GroupRecord(errorRecord(t, 9, raw, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	var encoded []any
	if err := json.Unmarshal(group.CanonicalBytes, &encoded); err != nil {
		t.Fatal(err)
	}
	components := encoded[2].([]any)
	chain := components[1].([]any)
	selected := components[2].([]any)
	if components[0] != "stack" || len(chain) != 2 || chain[0] != "Outer" || chain[1] != "Inner" || len(selected) != 8 {
		t.Fatalf("components=%#v", components)
	}
	first := selected[0].([]any)
	last := selected[7].([]any)
	if first[1] != "fne" || last[1] != "fnl" || first[2] != "C:/src/file.go" || group.Title != "Inner: inner" {
		t.Fatalf("frames=%#v title=%q", selected, group.Title)
	}
	raw["exception"].(map[string]any)["values"].([]any)[1].(map[string]any)["stacktrace"].(map[string]any)["frames"].([]any)[4].(map[string]any)["lineno"] = 999
	changed, err := GroupRecord(errorRecord(t, 9, raw, "", nil))
	if err != nil || changed.IssueID != group.IssueID {
		t.Fatalf("line number changed group: %s/%s err=%v", group.IssueID, changed.IssueID, err)
	}
}

func TestGroupingEscapingMarkersEmptyAndNonError(t *testing.T) {
	record := errorRecord(t, 7, map[string]any{"fingerprint": []any{"<literal>&", "{{  default  }}"}}, "<title>&", nil)
	group, err := GroupRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(group.CanonicalBytes), `\u003c`) || !strings.Contains(string(group.CanonicalBytes), `"<literal>&"`) || !strings.Contains(string(group.CanonicalBytes), `"{{  default  }}"`) {
		t.Fatalf("escaping or marker expansion changed: %s", group.CanonicalBytes)
	}
	record.Raw = json.RawMessage(`{"fingerprint":[]}`)
	group, err = GroupRecord(record)
	if err != nil || string(group.CanonicalBytes) != `["eventglass-grouping-v1","7",["message","<title>&"]]` {
		t.Fatalf("empty fingerprint=%s err=%v", group.CanonicalBytes, err)
	}
	record.Raw = json.RawMessage(`{"fingerprint":["{{ default }}","{{default}}"]}`)
	group, err = GroupRecord(record)
	if err != nil || strings.Count(string(group.CanonicalBytes), `"default"`) != 2 {
		t.Fatalf("default marker spellings=%s err=%v", group.CanonicalBytes, err)
	}
	record = errorRecord(t, 7, map[string]any{"exception": map[string]any{"values": []any{map[string]any{}}}}, "", nil)
	group, err = GroupRecord(record)
	if err != nil || string(group.CanonicalBytes) != `["eventglass-grouping-v1","7",["exception","",""]]` {
		t.Fatalf("empty exception=%s err=%v", group.CanonicalBytes, err)
	}
	record.Kind = model.KindLog
	if _, err := GroupRecord(record); !errors.Is(err, ErrNotError) {
		t.Fatalf("log grouping=%v", err)
	}
	record = errorRecord(t, 7, map[string]any{}, strings.Repeat("가", 513), nil)
	group, err = GroupRecord(record)
	if err != nil || len([]rune(group.Title)) != 512 {
		t.Fatalf("title runes=%d err=%v", len([]rune(group.Title)), err)
	}
}

func TestMalformedOptionalFingerprintAndFramesFallBackWithoutPoisoning(t *testing.T) {
	record := errorRecord(t, 7, map[string]any{"fingerprint": "not-an-array"}, "safe", nil)
	group, err := GroupRecord(record)
	if err != nil || string(group.CanonicalBytes) != `["eventglass-grouping-v1","7",["message","safe"]]` {
		t.Fatalf("malformed fingerprint group=%s err=%v", group.CanonicalBytes, err)
	}
	record = errorRecord(t, 7, map[string]any{
		"fingerprint": []any{"custom", 1},
		"exception": map[string]any{"values": []any{map[string]any{
			"type": "Failure", "value": "bad", "stacktrace": map[string]any{"frames": []any{"bad-frame"}},
		}}},
	}, "", nil)
	group, err = GroupRecord(record)
	if err != nil || string(group.CanonicalBytes) != `["eventglass-grouping-v1","7",["exception","Failure","bad"]]` {
		t.Fatalf("malformed optional values group=%s err=%v", group.CanonicalBytes, err)
	}
}
