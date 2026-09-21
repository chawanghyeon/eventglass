//go:build comparison

package comparison

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// This hashes actual request bodies offered to the HTTP client, including
// admission retries and deliberate duplicates, in locked submission order.
// It is not TCP arrival order or an ACK/published-data checksum. Only the hash
// and counters are retained, never DSN keys or the complete workload in memory.
type submittedInput struct {
	Framing          string
	SHA256           string
	Envelopes, Bytes int64
}

type inputEvidence struct {
	mu               sync.Mutex
	digest           hash.Hash
	envelopes, bytes int64
}

func newInputEvidence() *inputEvidence {
	digest := sha256.New()
	_, _ = digest.Write([]byte("eventglass-submitted-envelope-v1\x00"))
	return &inputEvidence{digest: digest}
}

func (input *inputEvidence) record(body []byte) {
	input.mu.Lock()
	defer input.mu.Unlock()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(body)))
	_, _ = input.digest.Write(length[:])
	_, _ = input.digest.Write(body)
	input.envelopes++
	input.bytes += int64(len(body))
}

func (input *inputEvidence) snapshot() submittedInput {
	input.mu.Lock()
	defer input.mu.Unlock()
	return submittedInput{Framing: "eventglass-submitted-envelope-v1:BE64-length+body", SHA256: hex.EncodeToString(input.digest.Sum(nil)), Envelopes: input.envelopes, Bytes: input.bytes}
}

// Count the entire received-time workload through the public query API and
// actual worker/native engine, not receipts or a second SQL count of metadata.
// This independently checks the known generator's 100 logs + 5 errors/cycle;
// it does not replace the four-size fixture's typed/filter/row identity oracle.
func publishedCounts(ctx context.Context, client *http.Client, baseURL string, state comparisonState, csrf string, start, end time.Time) (_ map[string]int64, resultErr error) {
	body := map[string]any{
		"tenant_id": strconv.FormatInt(state.TenantID, 10), "project_ids": []string{strconv.FormatInt(state.ProjectID, 10)},
		"start_us": strconv.FormatInt(start.Add(-time.Minute).UnixMicro(), 10), "end_us": strconv.FormatInt(end.Add(time.Minute).UnixMicro(), 10),
		"time_basis": "received", "kinds": []string{"log", "error"}, "filter": map[string]any{"op": "constant", "value": true},
		"metrics": []map[string]any{{"name": "events", "op": "count"}}, "group_by": []map[string]any{{"op": "field", "name": "kind"}},
		"top": 10, "order": map[string]any{"metric": "events", "direction": "desc"}, "mode": "sync",
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/aggregate", bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", comparisonOrigin)
	request.Header.Set("X-CSRF-Token", csrf)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("search status=%d body=%s", response.StatusCode, data)
	}
	var wire struct {
		SnapshotID string `json:"snapshot_id"`
		Complete   bool   `json:"complete"`
		Groups     []struct {
			Keys    []struct{ Type, Value string }          `json:"keys"`
			Metrics map[string]struct{ Type, Value string } `json:"metrics"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	if wire.SnapshotID == "" {
		return nil, errors.New("successful query omitted snapshot_id")
	}
	defer func() {
		resultErr = errors.Join(resultErr, releaseComparisonSnapshot(ctx, client, baseURL, state.TenantID, wire.SnapshotID, csrf))
	}()
	if !wire.Complete || len(wire.Groups) != 2 {
		return nil, errors.New("published oracle requires exactly two complete kind groups")
	}
	counts := make(map[string]int64, 2)
	for _, group := range wire.Groups {
		if len(group.Keys) != 1 || group.Keys[0].Type != "string" || (group.Keys[0].Value != "log" && group.Keys[0].Value != "error") {
			return nil, errors.New("invalid published kind group")
		}
		metric, ok := group.Metrics["events"]
		count, err := strconv.ParseInt(metric.Value, 10, 64)
		if !ok || metric.Type != "integer" || err != nil || count < 0 {
			return nil, errors.New("invalid published integer count")
		}
		kind := group.Keys[0].Value
		if _, duplicate := counts[kind]; duplicate {
			return nil, errors.New("duplicate published kind group")
		}
		counts[kind] = count
	}
	return counts, nil
}
