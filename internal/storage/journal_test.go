package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/klauspost/compress/zstd"
)

func journalFixture(records int) model.JournalBatch {
	batch := model.JournalBatch{BatchID: "00000000-0000-4000-8000-000000000000", TenantID: 1}
	for candidate := 0; len(batch.Requests) < 3; candidate++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012x", candidate)
		lane, _ := model.LaneForAcceptance(id)
		if len(batch.Requests) == 0 {
			batch.LaneID = lane
		}
		if lane != batch.LaneID {
			continue
		}
		request := model.NormalizedRequest{TenantID: 1, ProjectID: int64(len(batch.Requests) + 1), AcceptanceID: id}
		if len(batch.Requests) != 1 { // Empty diagnostics request between data requests.
			for i := range records {
				request.Records = append(request.Records, model.Record{
					TenantID: 1, ProjectID: request.ProjectID, AcceptanceID: id,
					RecordID: fmt.Sprintf("%064x", candidate*10000+i), Kind: model.KindLog,
					RecordOrdinal: i, Message: "hello 안녕하세요", Raw: json.RawMessage(`{"body":"hello"}`),
					SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
				})
			}
		} else {
			request.Outcomes = []model.Outcome{{ItemOrdinal: 0, Category: "log_item", Reason: "queue_overflow", Quantity: 3, Approximate: true}}
			request.Unsupported = 1
			request.UnsupportedItems = []model.UnsupportedItem{{ItemOrdinal: 1, Type: "attachment", Bytes: 50}}
		}
		batch.Requests = append(batch.Requests, request)
	}
	return batch
}

func TestJournalMultipleProjectsEmptyRangesAndExactReplay(t *testing.T) {
	batch := journalFixture(2)
	var output bytes.Buffer
	info, err := WriteJournal(&output, batch)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	header, err := ReplayJournal(bytes.NewReader(output.Bytes()), info, func(request JournalRequest, ordinal int, record model.Record) error {
		if ordinal != count || request.ProjectID != record.ProjectID || request.AcceptanceID != record.AcceptanceID {
			t.Fatal("scope/order mismatch")
		}
		expected := batch.Requests[0].Records[count%2]
		if count >= 2 {
			expected = batch.Requests[2].Records[count%2]
		}
		got, _ := json.Marshal(record)
		want, _ := json.Marshal(expected)
		if !bytes.Equal(got, want) {
			t.Fatal("canonical data changed")
		}
		count++
		return nil
	})
	if err != nil || count != 4 || header.RequestCount != 3 {
		t.Fatalf("replay: %v records=%d header=%+v", err, count, header)
	}
}

func TestJournalRejectsCorruptionBeforeCallbacksAndInvalidStructure(t *testing.T) {
	var output bytes.Buffer
	info, err := WriteJournal(&output, journalFixture(2))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), output.Bytes()...)
	corrupt[len(corrupt)/2] ^= 1
	called := false
	if _, err := ReplayJournal(bytes.NewReader(corrupt), info, func(JournalRequest, int, model.Record) error { called = true; return nil }); err == nil || called {
		t.Fatal("corrupt object reached consumer")
	}
	decoder, _ := zstd.NewReader(nil)
	plain, err := decoder.DecodeAll(output.Bytes(), nil)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	for name, changed := range map[string][]byte{
		"version":  bytes.Replace(plain, []byte(`"format_version":1`), []byte(`"format_version":99`), 1),
		"scope":    bytes.Replace(plain, []byte(`"tenant_id":1`), []byte(`"tenant_id":2`), 1),
		"counts":   bytes.Replace(plain, []byte(`"records":2`), []byte(`"records":3`), 1),
		"footer":   plain[:bytes.LastIndex(plain[:len(plain)-1], []byte("\n"))+1],
		"trailing": append(append([]byte(nil), plain...), []byte("{}\n")...),
	} {
		t.Run(name, func(t *testing.T) {
			encoder, _ := zstd.NewWriter(nil, zstd.WithWindowSize(1<<20))
			data := encoder.EncodeAll(changed, nil)
			encoder.Close()
			digest := sha256.Sum256(data)
			if _, err := ReplayJournal(bytes.NewReader(data), JournalInfo{int64(len(data)), hex.EncodeToString(digest[:])}, nil); err == nil {
				t.Fatal("invalid journal accepted")
			}
		})
	}
	sentinel := errors.New("consumer canceled")
	if _, err := ReplayJournal(bytes.NewReader(output.Bytes()), info, func(JournalRequest, int, model.Record) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestJournalRejectsMixedTenantLaneDuplicateAndHugeLine(t *testing.T) {
	for _, mutate := range []func(*model.JournalBatch){
		func(b *model.JournalBatch) { b.Requests[0].TenantID = 2 },
		func(b *model.JournalBatch) { b.LaneID = (b.LaneID + 1) % 16 },
		func(b *model.JournalBatch) { b.Requests[1] = b.Requests[0] },
		func(b *model.JournalBatch) { b.Requests[0].Records[1] = b.Requests[0].Records[0] },
		func(b *model.JournalBatch) { b.Requests[0].Records[0].Message = strings.Repeat("x", maxJournalLine) },
	} {
		batch := journalFixture(2)
		mutate(&batch)
		if _, err := WriteJournal(io.Discard, batch); err == nil {
			t.Fatal("invalid batch accepted")
		}
	}
}

func TestRequestChecksumSurvivesRebatching(t *testing.T) {
	digests := func(batch model.JournalBatch) map[string]string {
		t.Helper()
		var buffer bytes.Buffer
		info, err := WriteJournal(&buffer, batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ReplayJournal(bytes.NewReader(buffer.Bytes()), info, nil); err != nil {
			t.Fatal(err)
		}
		decoder, err := zstd.NewReader(bytes.NewReader(buffer.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		defer decoder.Close()
		scanner := bufio.NewScanner(decoder)
		result := map[string]string{}
		var current JournalRequest
		for scanner.Scan() {
			var line journalLine
			if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
				t.Fatal(err)
			}
			if line.Type == "request" {
				if err := json.Unmarshal(line.Value, &current); err != nil {
					t.Fatal(err)
				}
			}
			if line.Type == "end_request" {
				var footer journalCount
				if err := json.Unmarshal(line.Value, &footer); err != nil {
					t.Fatal(err)
				}
				result[current.AcceptanceID] = footer.SHA256
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	batch := journalFixture(2)
	original := digests(batch)
	lastID := batch.Requests[2].AcceptanceID
	batch.Requests = batch.Requests[2:]
	batch.BatchID = "00000000-0000-4000-8000-000000000099"
	if digests(batch)[lastID] != original[lastID] {
		t.Fatal("physical batch placement changed receipt identity")
	}
	batch.Requests[0].Records[0].Message = "different payload"
	if digests(batch)[lastID] == original[lastID] {
		t.Fatal("content mutation retained receipt hash")
	}
}

func BenchmarkJournal(b *testing.B) {
	for _, count := range []int{10, 1000, 5000} {
		b.Run(fmt.Sprint(count*2), func(b *testing.B) {
			batch := journalFixture(count)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := WriteJournal(io.Discard, batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
