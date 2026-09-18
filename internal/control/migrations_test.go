package control

import (
	"strings"
	"testing"
)

func TestMigrationManifestIsOrderedAndComplete(t *testing.T) {
	manifest, err := MigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest) == 0 {
		t.Fatal("migration manifest is empty")
	}
	for i, migration := range manifest {
		if len(migration.SHA256) != 64 || migration.UpSQL == "" || migration.DownSQL == "" {
			t.Fatalf("invalid migration manifest entry: %+v", migration)
		}
		if i > 0 && manifest[i-1].Version >= migration.Version {
			t.Fatalf("manifest is not strictly ordered: %+v", manifest)
		}
	}
}

func TestSplitMigrationRejectsMissingRollback(t *testing.T) {
	_, _, err := splitMigration("-- +eventglass Up\nSELECT 1")
	if err == nil || !strings.Contains(err.Error(), "markers") {
		t.Fatalf("expected marker error, got %v", err)
	}
}

func TestMigrationLedgerRejectsFutureGapAndDrift(t *testing.T) {
	manifest := []Migration{{Version: 1, SHA256: "first"}, {Version: 2, SHA256: "second"}}
	for _, ledger := range []map[int]string{{3: "future"}, {2: "second"}, {1: "changed"}} {
		if err := validateMigrationLedger(manifest, ledger); err == nil {
			t.Fatalf("accepted ledger %v", ledger)
		}
	}
	for _, ledger := range []map[int]string{{}, {1: "first"}, {1: "first", 2: "second"}} {
		if err := validateMigrationLedger(manifest, ledger); err != nil {
			t.Fatal(err)
		}
	}
}
