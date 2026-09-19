package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/query"
)

func TestParseLiveRequestCanonicalizesScopeAndResume(t *testing.T) {
	request := httptest.NewRequest("GET", "/v1/live?tenant_id=7&project_ids=9,2&kinds=log&kinds=error&expression=service+%3D%3D+%27api%27&catchup_start_us=1&resume_token=checkpoint", nil)
	request.Header.Set("Last-Event-ID", "checkpoint")
	result, resume, err := parseLiveRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.TenantID != 7 || len(result.ProjectIDs) != 2 || result.ProjectIDs[0] != 2 || result.ProjectIDs[1] != 9 || len(result.Kinds) != 2 || result.CatchupStart == nil || resume != "checkpoint" {
		t.Fatalf("request=%#v resume=%q", result, resume)
	}
	if !strings.Contains(string(result.Canonical), "api") {
		t.Fatalf("canonical filter=%q", result.Canonical)
	}
}

func TestParseLiveRequestRejectsAmbiguousCheckpointAndDuplicateScope(t *testing.T) {
	request := httptest.NewRequest("GET", "/v1/live?tenant_id=7&project_ids=2&resume_token=one", nil)
	request.Header.Set("Last-Event-ID", "two")
	if _, _, err := parseLiveRequest(request); err == nil {
		t.Fatal("mismatched resume checkpoints accepted")
	}
	request = httptest.NewRequest("GET", "/v1/live?tenant_id=7&project_ids=2,2", nil)
	if _, _, err := parseLiveRequest(request); err == nil {
		t.Fatal("duplicate project accepted")
	}
}

func TestEncodeSSEUsesCheckpointAsIDAndEnforcesBound(t *testing.T) {
	encoded, err := encodeSSE(query.LiveEvent{Type: "checkpoint", ID: "token", Data: map[string]string{"resume_token": "token"}})
	if err != nil || string(encoded) != "id: token\nevent: checkpoint\ndata: {\"resume_token\":\"token\"}\n\n" {
		t.Fatalf("encoded=%q err=%v", encoded, err)
	}
	if _, err := encodeSSE(query.LiveEvent{Type: "unknown", Data: struct{}{}}); err == nil {
		t.Fatal("unknown SSE event accepted")
	}
	if _, err := encodeSSE(query.LiveEvent{Type: "rows", Data: strings.Repeat("x", query.LiveMaximumPending)}); err == nil {
		t.Fatal("oversized SSE event accepted")
	}
}
