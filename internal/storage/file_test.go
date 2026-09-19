package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectAndVerifyFileBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "finished.parquet")
	data := make([]byte, DefaultBlockSize+17)
	for index := range data {
		data[index] = byte(index * 31)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	evidence, err := InspectFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Bytes != int64(len(data)) || evidence.BlockSize != DefaultBlockSize || len(evidence.BlockSHA256) != 2 || len(evidence.SHA256) != 64 {
		t.Fatalf("evidence=%#v", evidence)
	}
	if err := VerifyFile(path, evidence); err != nil {
		t.Fatal(err)
	}
	data[len(data)-1]++
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFile(path, evidence); err == nil {
		t.Fatal("changed file matched its evidence")
	}
}
