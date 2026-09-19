package model

import "testing"

func TestReceiptSelectionPartition(t *testing.T) {
	selection, err := NewReceiptSelection([]ReceiptClass{
		ReceiptAccepted, ReceiptAccepted, ReceiptDuplicate, ReceiptAccepted,
		ReceiptConflict, ReceiptConflict, ReceiptDuplicate,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := selection.CanonicalJSON(7)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":1,"accepted":[[0,1],[3,3]],"duplicate":[[2,2],[6,6]],"conflict":[[4,5]]}`
	if string(encoded) != want {
		t.Fatalf("selection=%s want=%s", encoded, want)
	}
	if digest, err := selection.SHA256(7); err != nil || digest != "4e8fce07c4f0a6c3c92a7f13b2a0726c7f9da4a4e22e6060b3070ab91e5addba" {
		t.Fatalf("selection digest=%s err=%v", digest, err)
	}

	empty, err := NewReceiptSelection(nil)
	if err != nil {
		t.Fatal(err)
	}
	if encoded, err := empty.CanonicalJSON(0); err != nil || string(encoded) != `{"version":1,"accepted":[],"duplicate":[],"conflict":[]}` {
		t.Fatalf("empty selection=%s err=%v", encoded, err)
	}

	invalid := []ReceiptSelection{
		{Version: 1, Accepted: []ReceiptRange{{0, 0}}, Duplicate: []ReceiptRange{}, Conflict: []ReceiptRange{}},
		{Version: 1, Accepted: []ReceiptRange{{0, 0}, {1, 1}}, Duplicate: []ReceiptRange{}, Conflict: []ReceiptRange{}},
		{Version: 1, Accepted: []ReceiptRange{{0, 1}}, Duplicate: []ReceiptRange{{1, 1}}, Conflict: []ReceiptRange{}},
	}
	for _, candidate := range invalid {
		if err := candidate.Validate(2); err == nil {
			t.Fatalf("accepted invalid selection: %#v", candidate)
		}
	}
}
