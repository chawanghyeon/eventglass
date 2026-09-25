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
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Keep planned logical envelopes separate from actual HTTP attempts. Retries
// affect attempt provenance, not whether two installations ran the same input.
// Only digests and counters are retained, never full bodies or DSN keys.
type submittedInput struct {
	Framing, SHA256, WorkloadFraming, WorkloadSHA256   string
	Envelopes, Bytes, WorkloadEnvelopes, WorkloadBytes int64
	Complete                                           bool
}

const maxSubmittedInputEnvelopes = 100_000

type submittedEnvelopeDigest struct {
	length uint64
	digest [sha256.Size]byte
}

type inputEvidence struct {
	mu                                                 sync.Mutex
	attempts, workload                                 []submittedEnvelopeDigest
	envelopes, bytes, workloadEnvelopes, workloadBytes int64
}

func newInputEvidence() *inputEvidence {
	return &inputEvidence{}
}

func (input *inputEvidence) recordAttempt(body []byte) error {
	input.mu.Lock()
	defer input.mu.Unlock()
	if len(input.attempts)+len(input.workload) >= maxSubmittedInputEnvelopes {
		return errors.New("comparison submitted-input evidence limit exceeded")
	}
	input.attempts = append(input.attempts, submittedEnvelopeDigest{length: uint64(len(body)), digest: sha256.Sum256(body)})
	input.envelopes++
	input.bytes += int64(len(body))
	return nil
}

func (input *inputEvidence) recordWorkload(body []byte, publicKey string) error {
	digest, err := comparisonWorkloadDigest(body, publicKey)
	if err != nil {
		return err
	}
	length := len(body)
	if publicKey != "" {
		length += len("comparison-generated-key") - len(publicKey)
	}
	input.mu.Lock()
	defer input.mu.Unlock()
	if len(input.attempts)+len(input.workload) >= maxSubmittedInputEnvelopes {
		return errors.New("comparison submitted-input evidence limit exceeded")
	}
	input.workload = append(input.workload, submittedEnvelopeDigest{length: uint64(length), digest: digest})
	input.workloadEnvelopes++
	input.workloadBytes += int64(length)
	return nil
}

func comparisonWorkloadDigest(body []byte, publicKey string) ([sha256.Size]byte, error) {
	if publicKey == "" {
		return sha256.Sum256(body), nil
	}
	headerEnd := bytes.IndexByte(body, '\n')
	if headerEnd < 0 {
		return [sha256.Size]byte{}, errors.New("comparison envelope has no DSN header")
	}
	key := []byte(publicKey)
	keyAt := bytes.Index(body[:headerEnd], key)
	if keyAt < 0 {
		return [sha256.Size]byte{}, errors.New("comparison envelope DSN key is absent")
	}
	digest := sha256.New()
	_, _ = digest.Write(body[:keyAt])
	_, _ = digest.Write([]byte("comparison-generated-key"))
	_, _ = digest.Write(body[keyAt+len(key):])
	var sum [sha256.Size]byte
	copy(sum[:], digest.Sum(nil))
	return sum, nil
}

func (input *inputEvidence) snapshot() submittedInput {
	input.mu.Lock()
	attempts := append([]submittedEnvelopeDigest(nil), input.attempts...)
	workload := append([]submittedEnvelopeDigest(nil), input.workload...)
	envelopes, totalBytes := input.envelopes, input.bytes
	workloadEnvelopes, workloadBytes := input.workloadEnvelopes, input.workloadBytes
	input.mu.Unlock()
	sort.Slice(attempts, func(i, j int) bool {
		if attempts[i].length != attempts[j].length {
			return attempts[i].length < attempts[j].length
		}
		return bytes.Compare(attempts[i].digest[:], attempts[j].digest[:]) < 0
	})
	sort.Slice(workload, func(i, j int) bool {
		if workload[i].length != workload[j].length {
			return workload[i].length < workload[j].length
		}
		return bytes.Compare(workload[i].digest[:], workload[j].digest[:]) < 0
	})
	digest := hashEnvelopeSet("eventglass-submitted-envelope-set-v2\x00", attempts)
	workloadDigest := hashEnvelopeSet("eventglass-comparison-logical-workload-set-v1\x00", workload)
	return submittedInput{
		Framing: "eventglass-submitted-envelope-set-v2:sorted-BE64-length+SHA256(body)",
		SHA256:  hex.EncodeToString(digest), WorkloadFraming: "eventglass-comparison-logical-workload-set-v1:DSN-key-normalized+sorted-BE64-length+SHA256(body)",
		WorkloadSHA256: hex.EncodeToString(workloadDigest), Envelopes: envelopes, Bytes: totalBytes,
		WorkloadEnvelopes: workloadEnvelopes, WorkloadBytes: workloadBytes,
		Complete: int64(len(attempts)) == envelopes && int64(len(workload)) == workloadEnvelopes,
	}
}

func hashEnvelopeSet(domain string, entries []submittedEnvelopeDigest) []byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	var length [8]byte
	for _, entry := range entries {
		binary.BigEndian.PutUint64(length[:], entry.length)
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(entry.digest[:])
	}
	return hash.Sum(nil)
}

func TestSubmittedInputEvidenceHasAFixedMemoryCeiling(t *testing.T) {
	input := newInputEvidence()
	if err := input.recordWorkload([]byte("planned"), ""); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxSubmittedInputEnvelopes-1; index++ {
		if err := input.recordAttempt([]byte("bounded")); err != nil {
			t.Fatalf("record envelope %d: %v", index, err)
		}
	}
	if err := input.recordWorkload([]byte("overflow"), ""); err == nil {
		t.Fatal("submitted-input evidence accepted a planned envelope past its combined cap")
	}
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
