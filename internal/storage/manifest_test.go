package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func manifestBundle(index int, titleBytes int) model.BundleManifest {
	blocks := []model.FileBlockManifest{{Index: 0, SHA256: strings.Repeat("a", 64)}}
	file := func(role string) model.FileManifest {
		return model.FileManifest{
			FileID: strings.Repeat(role[:1], 36) + strings.Repeat("x", titleBytes), IntentID: role + "-intent", Role: role,
			Bytes: 1, SHA256: strings.Repeat("b", 64), RowCount: 1,
			MinEventTimeUS: 1, MaxEventTimeUS: 1, MinReceivedTimeUS: 2, MaxReceivedTimeUS: 2, MinBatchSeq: 1, MaxBatchSeq: 1, Blocks: blocks,
		}
	}
	return model.BundleManifest{
		BundleID: "bundle-" + strings.Repeat("x", titleBytes) + string(rune('a'+index%26)), EventDay: "2026-01-01", Kind: model.KindLog,
		InputSeqMin: 1, InputSeqMax: 1, RowCount: 1, IdentitySHA256: strings.Repeat("c", 64), ProjectIDs: []int64{1},
		Analytics: file("analytics"), Payload: file("payload"),
	}
}

func TestOutputManifestPagerBoundsPartsAndRoot(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "manifests")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	pager, err := NewOutputManifestPager(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Cleanup()
	for index := 0; index < 20; index++ {
		if err := pager.Append(manifestBundle(index, 2500)); err != nil {
			t.Fatal(err)
		}
	}
	header := model.OutputManifestHeader{
		Version: 1, OutputID: "output", JobID: "job", TenantID: 1, LaneID: 0, BatchSeq: 1,
		JournalSHA256: strings.Repeat("a", 64), ReceiptSetSHA256: strings.Repeat("b", 64), SelectedIdentitySHA256: strings.Repeat("c", 64), OccurrenceSummarySHA256: strings.Repeat("d", 64),
		GroupingVersion: 1, SelectedRecordCount: 20, BundleCount: 20,
	}
	manifest, err := pager.Finish(header)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Parts) < 2 || len(manifest.RootJSON) > model.MaxManifestHeaderBytes || len(manifest.RootSHA256) != 64 {
		t.Fatalf("parts=%d root=%d sha=%q", len(manifest.Parts), len(manifest.RootJSON), manifest.RootSHA256)
	}
	for index, part := range manifest.Parts {
		info, err := os.Stat(part.Path)
		if err != nil || part.Index != index || info.Size() != part.Bytes || part.Bytes > model.MaxManifestPartBytes {
			t.Fatalf("part=%#v info=%v err=%v", part, info, err)
		}
	}
}

func TestOutputManifestPagerRejectsUntrustedDigestAndProjectOrder(t *testing.T) {
	for name, mutate := range map[string]func(*model.BundleManifest){
		"identity non-hex":      func(bundle *model.BundleManifest) { bundle.IdentitySHA256 = strings.Repeat("z", 64) },
		"file digest uppercase": func(bundle *model.BundleManifest) { bundle.Analytics.SHA256 = strings.Repeat("A", 64) },
		"block digest non-hex":  func(bundle *model.BundleManifest) { bundle.Payload.Blocks[0].SHA256 = strings.Repeat("z", 64) },
		"projects unsorted":     func(bundle *model.BundleManifest) { bundle.ProjectIDs = []int64{2, 1} },
		"projects duplicated":   func(bundle *model.BundleManifest) { bundle.ProjectIDs = []int64{1, 1} },
		"invalid event day":     func(bundle *model.BundleManifest) { bundle.EventDay = "2026-02-30" },
	} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "manifests")
			pager, err := NewOutputManifestPager(directory)
			if err != nil {
				t.Fatal(err)
			}
			bundle := manifestBundle(0, 0)
			mutate(&bundle)
			if err := pager.Append(bundle); err == nil {
				t.Fatal("invalid bundle was accepted")
			}
		})
	}
}
