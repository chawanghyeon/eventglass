package engine

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCompactionPartManifestBoundsAndNumericOrder(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"chunk=10", "chunk=2", "chunk=0"} {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "data_0.parquet"), []byte("test"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{filepath.Join(root, "chunk=0/data_0.parquet"), filepath.Join(root, "chunk=2/data_0.parquet"), filepath.Join(root, "chunk=10/data_0.parquet")}
	got, err := compactionPartPaths(root, 3, 12)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("parts=%v err=%v", got, err)
	}
	for _, limit := range []struct{ count, bytes int64 }{{2, 12}, {4, 12}, {3, 11}, {-1, 12}, {1025, 12}, {3, -1}} {
		if _, err := compactionPartPaths(root, limit.count, limit.bytes); err == nil {
			t.Fatalf("accepted bound %+v", limit)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "chunk=0/unexpected"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := compactionPartPaths(root, 3, 100); err == nil {
		t.Fatal("accepted uncounted file")
	}
}

func TestCompactionPartManifestRejectsSymlinksAndInvalidNames(t *testing.T) {
	for _, name := range []string{"chunk=-1", "chunk=bad", "other=0", "chunk=0"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(dir, "data_0.parquet")); err != nil {
				t.Fatal(err)
			}
			if _, err := compactionPartPaths(root, 1, 100); err == nil {
				t.Fatal("invalid partition admitted")
			}
		})
	}
	if paths, err := compactionPartPaths(t.TempDir(), 0, 0); err != nil || len(paths) != 0 {
		t.Fatalf("empty=%v %v", paths, err)
	}
}
