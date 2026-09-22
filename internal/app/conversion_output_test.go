package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestChildBundleRejectsSymlinkDespiteMatchingContentEvidence(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "output")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("verified bytes outside the assigned output directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	evidence, err := storage.InspectFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(output, "analytics.parquet")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(output, "payload.parquet")
	if err := os.WriteFile(payload, []byte("verified bytes outside the assigned output directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle := engine.ConvertedBundle{
		RowCount: 1, ProjectIDs: []int64{1}, IdentitySHA256: strings.Repeat("a", 64),
		Analytics: engine.ConvertedFile{Path: link, Evidence: evidence, RowCount: 1},
		Payload:   engine.ConvertedFile{Path: payload, Evidence: evidence, RowCount: 1},
	}
	if err := verifyChildBundle(output, bundle); err == nil {
		t.Fatal("child output followed a symlink outside its assigned directory")
	}
}
