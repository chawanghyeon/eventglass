package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/sdk"
)

func TestNormalizeEventUsesEnvelopeIDExactNegativeTimeAndScrubs(t *testing.T) {
	envelope := sdk.Envelope{
		Header: map[string]any{
			"event_id": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
			"sdk":      map[string]any{"name": "sentry.go", "version": "0.49.0"},
			"sent_at":  "2026-01-02T03:04:05.123456789Z",
		},
		Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
			"event_id":  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"timestamp": json.Number("-0.000000001"),
			"message":   "payment failed",
			"level":     "critical",
			"request": map[string]any{
				"headers": []any{[]any{"Authorization", "Bearer fixture-secret"}},
				"url":     "https://example.invalid/path?token=fixture-secret&x=1",
			},
			"extra": map[string]any{
				"password": "fixture-secret", "safe": "안녕하세요 👋",
				"a.b": "literal", "a": map[string]any{"b": "nested"},
			},
		}}},
	}
	batch, err := NormalizeEnvelope(envelope, NormalizeOptions{
		TenantID: 2, ProjectID: 7, AcceptanceID: "acceptance-one",
		ArrivalTime: time.Unix(10, 0), DefaultService: "payments", ForbiddenValue: "fixture-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("records=%d", len(batch.Records))
	}
	record := batch.Records[0]
	if record.EventTimeUS != -1 || record.EventTimeNSRemainder != 999 {
		t.Fatalf("negative timestamp was not floor-normalized: %d/%d", record.EventTimeUS, record.EventTimeNSRemainder)
	}
	if record.SourceEventID == nil || *record.SourceEventID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("envelope event ID did not win: %#v", record.SourceEventID)
	}
	if record.Level != "fatal" || !contains(record.Warnings, "event_id_conflict") {
		t.Fatalf("level/warnings=%s %#v", record.Level, record.Warnings)
	}
	if strings.Contains(string(record.Raw), "fixture-secret") || !strings.Contains(string(record.Raw), filteredValue) {
		t.Fatalf("raw payload was not scrubbed: %s", record.Raw)
	}
	if record.Service == nil || *record.Service != "payments" || record.SDKName == nil || *record.SDKName != "sentry.go" {
		t.Fatalf("promotions missing: %#v", record)
	}
	if !strings.Contains(string(record.EnvelopeSDKJSON), "sentry.go") || string(record.SentAtJSON) != `"2026-01-02T03:04:05.123456789Z"` {
		t.Fatalf("envelope metadata missing: sdk=%s sent_at=%s", record.EnvelopeSDKJSON, record.SentAtJSON)
	}
	if findAttribute(record.Attrs, "/a.b") == nil || findAttribute(record.Attrs, "/a/b") == nil {
		t.Fatalf("dotted and nested paths were not distinct: %#v", record.Attrs)
	}
}

func TestNormalizeVersionedAndVersionlessLogsPreservesTypedValues(t *testing.T) {
	log := map[string]any{
		"timestamp":       "2026-01-02T03:04:05.123456789Z",
		"level":           "warn",
		"body":            "order 42",
		"trace_id":        "11111111111111111111111111111111",
		"span_id":         "2222222222222222",
		"severity_number": json.Number("14"),
		"attributes": map[string]any{
			"sentry.message.template": map[string]any{"type": "string", "value": "order %s"},
			"service.name":            map[string]any{"type": "string", "value": "checkout"},
			"a.b":                     map[string]any{"type": "integer", "value": json.Number("9223372036854775807"), "unit": "item"},
			"huge":                    map[string]any{"type": "integer", "value": json.Number("123456789012345678901234567890123456789")},
			"mixed":                   map[string]any{"type": "array", "value": []any{json.Number("1"), "1", true, nil}},
			"mismatch":                map[string]any{"type": "integer", "value": "not-an-integer"},
		},
	}
	for _, version := range []any{nil, json.Number("1"), json.Number("2")} {
		container := map[string]any{"items": []any{log}}
		if version != nil {
			container["version"] = version
		}
		batch, err := NormalizeEnvelope(sdk.Envelope{Header: map[string]any{}, Items: []sdk.Item{{Ordinal: 1, Type: "log", Header: map[string]any{"item_count": json.Number("1")}, Value: container}}}, NormalizeOptions{
			TenantID: 1, ProjectID: 1, AcceptanceID: "acceptance-log", ArrivalTime: time.Unix(0, 0),
		})
		if err != nil {
			t.Fatalf("version %v: %v", version, err)
		}
		record := batch.Records[0]
		if record.Kind != model.KindLog || record.Level != "warning" || record.SeverityNumber == nil || *record.SeverityNumber != 14 {
			t.Fatalf("bad log projection: %#v", record)
		}
		if record.EventTimeUS != 1767323045123456 || record.EventTimeNSRemainder != 789 {
			t.Fatalf("lost timestamp precision: %d/%d", record.EventTimeUS, record.EventTimeNSRemainder)
		}
		integer := findAttribute(record.Attrs, "/a.b")
		if integer == nil || integer.IntegerValue == nil || *integer.IntegerValue != "9223372036854775807" || integer.Unit == nil || *integer.Unit != "item" {
			t.Fatalf("integer attribute lost: %#v", integer)
		}
		huge := findAttribute(record.Attrs, "/huge")
		if huge == nil || huge.ValueType != "big_integer" {
			t.Fatalf("large integer was not explicit: %#v", huge)
		}
		mismatch := findAttribute(record.Attrs, "/mismatch")
		if mismatch == nil || mismatch.ValueType != "invalid" {
			t.Fatalf("declared type mismatch was not retained as invalid: %#v", mismatch)
		}
	}
}

func TestNormalizeExponentTimestampTraceScopeAndAttributeOrder(t *testing.T) {
	log := map[string]any{
		"timestamp": json.Number("1.767323045123456789e9"),
		"body":      "trace",
		"trace_id":  "00000000000000000000000000000000",
		"span_id":   "not-a-span",
		"attributes": map[string]any{
			"z": json.Number("1"),
			"a": "first",
		},
	}
	batch, err := NormalizeEnvelope(sdk.Envelope{Header: map[string]any{"trace": map[string]any{"trace_id": "11111111111111111111111111111111"}}, Items: []sdk.Item{{
		Ordinal: 0, Type: "log", Header: map[string]any{}, Value: map[string]any{
			"version": json.Number("1"), "ingest_settings": map[string]any{"infer_ip": "always"}, "items": []any{log},
		},
	}}}, NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: "exponent", ArrivalTime: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	record := batch.Records[0]
	if record.EventTimeUS != 1_767_323_045_123_456 || record.EventTimeNSRemainder != 789 {
		t.Fatalf("exponent timestamp lost precision: %d/%d", record.EventTimeUS, record.EventTimeNSRemainder)
	}
	if record.TraceID != nil || record.SpanID != nil || !contains(record.Warnings, "invalid_trace_id") || !contains(record.Warnings, "invalid_span_id") || !contains(record.Warnings, "inference_disabled") {
		t.Fatalf("trace scope/diagnostics incorrect: %#v", record)
	}
	if len(record.Attrs) != 2 || record.Attrs[0].Path != "/a" || record.Attrs[1].Path != "/z" {
		t.Fatalf("attributes are not deterministic: %#v", record.Attrs)
	}
}

func TestTimestampUsesInt64MicrosecondRange(t *testing.T) {
	farFuture := "9999-12-31T23:59:59.999999999Z"
	micros, remainder, _, err := parseTimestamp(farFuture)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := time.Parse(time.RFC3339Nano, farFuture)
	if micros != parsed.Unix()*1_000_000+999_999 || remainder != 999 {
		t.Fatalf("far future timestamp=%d/%d", micros, remainder)
	}
	micros, remainder, _, err = parseTimestamp(json.Number("9223372036854.775807"))
	if err != nil || micros != int64(9_223_372_036_854_775_807) || remainder != 0 {
		t.Fatalf("large decimal timestamp=%d/%d err=%v", micros, remainder, err)
	}
}

func TestNormalizeRejectsAttributeAndCanonicalRecordLimits(t *testing.T) {
	attrs := make(map[string]any, MaxTypedAttributes+1)
	for index := 0; index <= MaxTypedAttributes; index++ {
		attrs[fmt.Sprintf("a.%04d", index)] = "value"
	}
	_, err := NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "log", Value: map[string]any{
		"items": []any{map[string]any{"body": "too wide", "attributes": attrs}},
	}}}}, NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: "wide", ArrivalTime: time.Now()})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("wide attributes returned %v", err)
	}

	_, err = NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
		"message": strings.Repeat("x", MaxRecordBytes),
	}}}}, NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: "large", ArrivalTime: time.Now()})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("large canonical record returned %v", err)
	}
}

func TestNormalizeRejectsUnknownLogVersionAtomically(t *testing.T) {
	_, err := NormalizeEnvelope(sdk.Envelope{Header: map[string]any{}, Items: []sdk.Item{
		{Ordinal: 0, Type: "event", Value: map[string]any{"message": "valid"}},
		{Ordinal: 1, Type: "log", Value: map[string]any{"version": json.Number("3"), "items": []any{}}},
	}}, NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: "atomic", ArrivalTime: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "unsupported log version") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnsupportedTypeDiagnosticIsBoundedAndStable(t *testing.T) {
	longType := strings.Repeat("가", 45)
	got := normalizeUnsupportedType(longType)
	if !strings.HasPrefix(got, strings.Repeat("가", 33)+"#") || len(got) != 116 || !utf8.ValidString(got) {
		t.Fatalf("unexpected diagnostic %q bytes=%d", got, len(got))
	}
	if got != normalizeUnsupportedType(longType) || normalizeUnsupportedType(strings.Repeat("x", 128)) != strings.Repeat("x", 128) {
		t.Fatal("unsupported type normalization is not stable at the boundary")
	}
}

func TestIdentityDomainAndClientReport(t *testing.T) {
	first := recordID("1", "error", "id")
	if first == recordID("1", "log", "id") || len(first) != 64 {
		t.Fatal("record identity is not domain separated")
	}
	acceptance, err := NewAcceptanceID()
	if err != nil || len(acceptance) != 36 || acceptance[14] != '4' {
		t.Fatalf("bad UUIDv4: %q %v", acceptance, err)
	}
	batch, err := NormalizeEnvelope(sdk.Envelope{Header: map[string]any{}, Items: []sdk.Item{{Ordinal: 3, Type: "client_report", Value: map[string]any{
		"discarded_events": []any{map[string]any{"reason": "queue_overflow", "category": "log_item", "quantity": json.Number("7")}},
	}}}}, NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: acceptance, ArrivalTime: time.Now()})
	if err != nil || len(batch.Outcomes) != 1 || batch.Outcomes[0].Quantity != 7 || !batch.Outcomes[0].Approximate {
		t.Fatalf("bad outcomes: %#v %v", batch.Outcomes, err)
	}
}

func TestSourceIdentityIsSeparateFromOccurrenceIdentity(t *testing.T) {
	envelope := sdk.Envelope{Items: []sdk.Item{{Type: "event", Value: map[string]any{
		"event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "message": "same source",
	}}}}
	normalize := func(acceptance string) model.Record {
		batch, err := NormalizeEnvelope(envelope, NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: acceptance, ArrivalTime: time.Unix(1, 0)})
		if err != nil {
			t.Fatal(err)
		}
		return batch.Records[0]
	}
	first, retry, later := normalize("first"), normalize("first"), normalize("later")
	if first.RecordID != retry.RecordID || first.RecordID == later.RecordID {
		t.Fatal("occurrence identity is not retry-stable and expiry-safe")
	}
	if *first.SourceEventID != *later.SourceEventID {
		t.Fatal("source dedupe identity changed")
	}
}

func findAttribute(attributes []model.Attribute, path string) *model.Attribute {
	for index := range attributes {
		if attributes[index].Path == path {
			return &attributes[index]
		}
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
