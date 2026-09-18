package model

import "testing"

func TestLaneUsesCanonicalUUIDAndRejectsMalformed(t *testing.T) {
	id := "00000000-0000-4000-8000-000000000000"
	a, err := LaneForAcceptance(id)
	if err != nil || a < 0 || a >= LaneCount {
		t.Fatal(a, err)
	}
	b, _ := LaneForAcceptance(id)
	if a != b {
		t.Fatal("unstable lane")
	}
	for _, invalid := range []string{"", "not-a-uuid", "00000000-0000-1000-8000-000000000000", "00000000-0000-4000-0000-000000000000", "-------- ---- ---- ---- ------------", "00000000-0000-4000-8000-00000000000A"} {
		if _, err := LaneForAcceptance(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}
