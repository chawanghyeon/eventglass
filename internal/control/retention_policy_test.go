package control

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"
)

func TestRetentionFloorAfterChangeNeverMovesBackward(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/implementation/contract-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			ID       string `json:"id"`
			Now      string `json:"now_us"`
			OldFloor string `json:"old_floor_us"`
			Expected string `json:"expected_floor_us"`
			OldDays  int    `json:"old_days"`
			NewDays  int    `json:"new_days"`
		} `json:"retention_change_cases"`
	}
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		t.Run(test.ID, func(t *testing.T) {
			now, nowErr := strconv.ParseInt(test.Now, 10, 64)
			floor, floorErr := strconv.ParseInt(test.OldFloor, 10, 64)
			want, wantErr := strconv.ParseInt(test.Expected, 10, 64)
			got, err := RetentionFloorAfterChange(now, floor, test.OldDays, test.NewDays)
			if err != nil || nowErr != nil || floorErr != nil || wantErr != nil || got != want {
				t.Fatalf("floor=%d want=%d err=%v parse=(%v,%v,%v)", got, want, err, nowErr, floorErr, wantErr)
			}
		})
	}
}
