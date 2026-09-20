package api

import (
	"strings"
	"testing"
)

func TestAlertListCursorBindsScopeAndPrincipal(t *testing.T) {
	key := [32]byte{1}
	handler := &ManagementHandler{config: ManagementConfig{LoginBucketKey: key}}
	alertID := "00000000-0000-4000-8000-000000000123"
	token, err := handler.encodeAlertCursor(11, 22, 33, alertID)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := handler.decodeAlertCursor(token, 11, 22, 33)
	if err != nil || decoded != alertID {
		t.Fatalf("round trip = %q, %v", decoded, err)
	}
	for _, scope := range [][3]int64{{12, 22, 33}, {11, 23, 33}, {11, 22, 34}} {
		if _, err := handler.decodeAlertCursor(token, scope[0], scope[1], scope[2]); err == nil {
			t.Fatalf("cursor accepted for foreign scope %v", scope)
		}
	}
	tampered := token[:len(token)-1] + strings.Map(func(r rune) rune {
		if r == 'A' {
			return 'B'
		}
		return 'A'
	}, token[len(token)-1:])
	if _, err := handler.decodeAlertCursor(tampered, 11, 22, 33); err == nil {
		t.Fatal("tampered cursor accepted")
	}
}

func TestAlertListLimit(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want int
		ok   bool
	}{{"", 100, true}, {"1", 1, true}, {"1000", 1000, true}, {"0", 0, false}, {"1001", 1001, false}, {"x", 0, false}} {
		got, ok := parseAlertListLimit(test.raw)
		if got != test.want || ok != test.ok {
			t.Fatalf("parseAlertListLimit(%q) = %d, %v", test.raw, got, ok)
		}
	}
}
