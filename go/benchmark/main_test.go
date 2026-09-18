package main

import (
	"encoding/json"
	"testing"
)

func TestGeneratedDatasetMatchesRustBenchmarkShape(t *testing.T) {
	const records = 10_000
	base := int64(1_700_000_000_000_000)
	sizes := make([]uint64, 0, records)
	errors := 0
	late := 0
	for index := 0; index < records; index++ {
		value := makeRecord(index, base)
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal record %d: %v", index, err)
		}
		sizes = append(sizes, uint64(len(encoded)))
		if value.Kind == errorKind {
			errors++
		}
		if index%97 == 0 {
			late++
		}
	}
	if errors != 477 {
		t.Fatalf("errors=%d, want 477", errors)
	}
	if late != 104 {
		t.Fatalf("late=%d, want 104", late)
	}
	distribution := makeDistribution(sizes)
	for field, expected := range map[string]uint64{
		"p50": 1550,
		"p95": 1568,
		"p99": 1889,
		"max": 1893,
	} {
		actual := map[string]uint64{
			"p50": distribution.P50,
			"p95": distribution.P95,
			"p99": distribution.P99,
			"max": distribution.Max,
		}[field]
		if actual != expected {
			t.Fatalf("%s=%d, want %d", field, actual, expected)
		}
	}
}

func TestSerializeChunkRoundTripsCanonicalPayload(t *testing.T) {
	value := makeRecord(0, 1_700_000_000_000_000)
	value.IngestSeq = 1
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	chunk := serializeChunk([][]byte{encoded}, 1, 1, value.ReceivedAtUS)
	var payload inboxPayload
	if err := decodePayload(chunk.bytes, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Version != 1 || len(payload.Records) != 1 || payload.Records[0].IngestSeq != 1 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	reencoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(reencoded) != string(chunk.bytes) {
		t.Fatalf("chunk is not canonical JSON: original=%s reencoded=%s", chunk.bytes, reencoded)
	}
}

func TestPercentileUsesCeilingRank(t *testing.T) {
	values := []uint64{4, 1, 3, 2}
	distribution := makeDistribution(values)
	if distribution.P50 != 2 || distribution.P95 != 4 || distribution.P99 != 4 {
		t.Fatalf("unexpected distribution: %+v", distribution)
	}
}
