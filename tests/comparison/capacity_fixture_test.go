//go:build comparison

package comparison

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestCapacityFixtureHasFixedIndependentWork(t *testing.T) {
	const cycles = 16
	var logs, errors int
	seen := map[string]bool{}
	for sequence := 1; sequence <= cycles; sequence++ {
		state := comparisonState{TenantID: 1, ProjectID: int64((sequence-1)%4 + 1), PublicKey: "capacity-fixture"}
		bodies, _ := comparisonEnvelopes(state, int64(sequence), time.Unix(1_789_977_600, 0).UTC())
		if len(bodies) != 6 {
			t.Fatal("capacity input no longer has exactly six jobs per cycle")
		}
		for index, body := range bodies {
			lines := bytes.Split(body, []byte{'\n'})
			if len(lines) != 4 {
				t.Fatal("invalid SDK envelope framing")
			}
			if index == 0 {
				var payload struct {
					Items []json.RawMessage `json:"items"`
				}
				if err := json.Unmarshal(lines[2], &payload); err != nil || len(payload.Items) != 100 {
					t.Fatalf("log batch: %v", err)
				}
				logs += len(payload.Items)
			} else {
				var event struct {
					EventID string `json:"event_id"`
				}
				if err := json.Unmarshal(lines[2], &event); err != nil {
					t.Fatal(err)
				}
				key := fmt.Sprintf("%d:%s", state.ProjectID, event.EventID)
				if len(event.EventID) != 32 || seen[key] {
					t.Fatal("capacity fixture contains a duplicate source event")
				}
				seen[key] = true
				errors++
			}
		}
	}
	if logs != 1600 || errors != 80 {
		t.Fatalf("capacity logical input logs=%d errors=%d", logs, errors)
	}
	if a, b := capacityDefinition(t, cycles), capacityDefinition(t, cycles); a != b || len(a) != 64 || a == capacityDefinition(t, cycles+4) {
		t.Fatal("capacity definition is not deterministic and size-bound")
	}
}
