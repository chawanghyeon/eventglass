package model

import (
	"strings"
	"testing"
)

func manifestRootFixture() OutputManifestRoot {
	return OutputManifestRoot{
		Version: 1,
		Header: OutputManifestHeader{
			Version: 1, OutputID: "00000000-0000-4000-8000-000000000001", JobID: "00000000-0000-4000-8000-000000000002",
			TenantID: 1, LaneID: 2, BatchSeq: 3, JournalSHA256: strings.Repeat("a", 64), ReceiptSetSHA256: strings.Repeat("b", 64),
			SelectedIdentitySHA256: strings.Repeat("c", 64), OccurrenceSummarySHA256: strings.Repeat("d", 64), GroupingVersion: 1,
			SelectedRecordCount: 2, SelectedErrorCount: 1, BundleCount: 1,
		},
		Parts: []OutputManifestPartRef{{Index: 0, SHA256: strings.Repeat("e", 64)}},
	}
}

func TestOutputManifestRootCanonicalAndOrdered(t *testing.T) {
	root := manifestRootFixture()
	encoded, err := root.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(string(encoded), "\n") || strings.Contains(string(encoded), " ") {
		t.Fatalf("manifest is not compact: %q", encoded)
	}
	first, err := root.SHA256()
	if err != nil || len(first) != 64 {
		t.Fatalf("manifest SHA=%q err=%v", first, err)
	}
	root.Parts[0].Index = 1
	if _, err := root.CanonicalJSON(); err == nil {
		t.Fatal("non-contiguous manifest parts were accepted")
	}
}

func TestEmptyOutputManifestHasNoParts(t *testing.T) {
	root := manifestRootFixture()
	root.Header.SelectedRecordCount = 0
	root.Header.SelectedErrorCount = 0
	root.Header.BundleCount = 0
	root.Parts = nil
	if _, err := root.CanonicalJSON(); err != nil {
		t.Fatal(err)
	}
	root.Parts = []OutputManifestPartRef{{Index: 0, SHA256: strings.Repeat("e", 64)}}
	if _, err := root.CanonicalJSON(); err == nil {
		t.Fatal("empty output manifest accepted a part")
	}
}
