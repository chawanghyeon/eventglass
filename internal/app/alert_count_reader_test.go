package app

import "testing"

func TestDecodeAlertCountRequiresOneCompleteNonnegativeRow(t *testing.T) {
	value, err := decodeAlertCount([]byte("{\"m0_valid\":7,\"m0_excluded\":0}\n"))
	if err != nil || value != 7 {
		t.Fatalf("value=%d err=%v", value, err)
	}
	for _, raw := range []string{"", "{\"m0_valid\":-1}\n", "{\"m0_valid\":1}\n{\"m0_valid\":2}\n", "{\"other\":1}\n"} {
		if _, err := decodeAlertCount([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
