package query

import "testing"

func TestDecodeCanonicalFilterRoundTrip(t *testing.T) {
	root := &Node{Op: "and", Args: []*Node{
		{Op: "eq", Left: &Node{Op: "field", Name: "kind"}, Right: &Node{Op: "literal", Type: StringType, Literal: &Literal{Type: StringType, String: "log"}}},
		{Op: "array_contains", Namespace: "attributes", Path: "/flags", Literal: &Literal{Type: BooleanType, Boolean: true}},
	}}
	encoded, err := CanonicalFilter(root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCanonicalFilter(encoded)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := CanonicalFilter(decoded)
	if string(encoded) != string(again) {
		t.Fatal("canonical filter changed on decode")
	}
	if _, err := DecodeCanonicalFilter(append(encoded, 0)); err == nil {
		t.Fatal("trailing filter bytes accepted")
	}
}
