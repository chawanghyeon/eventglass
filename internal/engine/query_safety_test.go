package engine

import "testing"

func TestSafeQueryStatementUsesKeywordBoundaries(t *testing.T) {
	if !safeQueryStatement("SELECT * FROM input_rows JOIN input_payload USING(record_id)", true) {
		t.Fatal("payload identifier was mistaken for LOAD")
	}
	for _, statement := range []string{
		"SELECT * FROM input_rows; LOAD json",
		"SELECT * FROM input_rows COPY x",
		"SELECT * FROM read_parquet('x')",
	} {
		if safeQueryStatement(statement, true) {
			t.Fatalf("unsafe statement accepted: %s", statement)
		}
	}
}

func TestValidGatewayInputOnlyAcceptsExplicitIPv4LoopbackCapabilities(t *testing.T) {
	if !validGatewayInput("http://127.0.0.1:38123/objects/550e8400-e29b-41d4-a716-446655440000") {
		t.Fatal("valid loopback capability was rejected")
	}
	for _, value := range []string{
		"http://127.0.0.1/objects/capability",
		"https://127.0.0.1:38123/objects/capability",
		"http://localhost:38123/objects/capability",
		"http://[::1]:38123/objects/capability",
		"http://127.0.0.2:38123/objects/capability",
		"http://user@127.0.0.1:38123/objects/capability",
		"http://127.0.0.1:38123/objects/capability/extra",
		"http://127.0.0.1:38123/objects/capability?query=1",
		"http://127.0.0.1:38123/objects/capability#fragment",
		"http://127.0.0.1:38123/not-objects/capability",
	} {
		if validGatewayInput(value) {
			t.Fatalf("unsafe gateway input accepted: %s", value)
		}
	}
}
