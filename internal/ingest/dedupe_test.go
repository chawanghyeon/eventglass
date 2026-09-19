package ingest

import (
	"encoding/json"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestDedupeHashIgnoresArrival(t *testing.T) {
	sourceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	base := model.Record{
		Kind: model.KindError, SourceEventID: &sourceID,
		Raw:          json.RawMessage(`{"z":1.0,"event_id":"first","nested":{"b":"<&","a":1}}`),
		AcceptanceID: "first", ArrivalTimeUS: 1, RecordID: "occurrence-one", Warnings: []string{"one"},
	}
	want, ok, err := DedupePayloadSHA256(base)
	if err != nil || !ok {
		t.Fatalf("hash=%s ok=%v err=%v", want, ok, err)
	}
	retry := base
	retry.AcceptanceID, retry.ArrivalTimeUS, retry.RecordID = "second", 999, "occurrence-two"
	retry.Warnings = []string{"different"}
	retry.Raw = json.RawMessage(`{"nested":{"a":1,"b":"<&"},"event_id":"different","z":1.0}`)
	got, ok, err := DedupePayloadSHA256(retry)
	if err != nil || !ok || got != want {
		t.Fatalf("retry hash=%s want=%s ok=%v err=%v", got, want, ok, err)
	}

	mutated := retry
	mutated.Raw = json.RawMessage(`{"nested":{"a":1,"b":"<&"},"z":1}`)
	changed, _, err := DedupePayloadSHA256(mutated)
	if err != nil || changed == want {
		t.Fatalf("numeric lexical mutation hash=%s want-different-from=%s err=%v", changed, want, err)
	}

	logRecord := retry
	logRecord.Kind = model.KindLog
	if digest, eligible, err := DedupePayloadSHA256(logRecord); err != nil || eligible || digest != "" {
		t.Fatalf("log unexpectedly deduped: %q %v %v", digest, eligible, err)
	}
	missing := retry
	missing.SourceEventID = nil
	if digest, eligible, err := DedupePayloadSHA256(missing); err != nil || eligible || digest != "" {
		t.Fatalf("missing source unexpectedly deduped: %q %v %v", digest, eligible, err)
	}
	invalidID := retry
	invalid := "not-an-event-id"
	invalidID.SourceEventID = &invalid
	if digest, eligible, err := DedupePayloadSHA256(invalidID); err != nil || eligible || digest != "" {
		t.Fatalf("invalid source unexpectedly deduped: %q %v %v", digest, eligible, err)
	}
}
