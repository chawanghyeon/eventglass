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
