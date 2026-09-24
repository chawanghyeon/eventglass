//go:build duckdb_use_static_lib

package main

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/klauspost/compress/zstd"
)

// Keep every StageRecord field except those already stored in the compact
// index. Fixed-size compressed pages allow point reads without whole-file decode.
func measureProductCompactResidual(t *testing.T, stage, payload string, index []byte, rows int, pairBytes int64) {
	t.Helper()
	core, _, err := compactDecodeCore(index, rows, true)
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	decoder := json.NewDecoder(input)
	path := filepath.Join(filepath.Dir(stage), "residual.bin")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	compressor, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer compressor.Close()
	const pageRows = 128
	var offsets []uint64
	var lengths []uint64
	for start := 0; start < rows; start += pageRows {
		page := make([]engine.StageRecord, min(pageRows, rows-start))
		for i := range page {
			if err := decoder.Decode(&page[i]); err != nil {
				t.Fatal(err)
			}
			page[i].Record.ProjectID = 0
			page[i].Record.SeverityNumber = nil
			page[i].Record.Service = nil
			page[i].Record.Attrs = nil
		}
		raw, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		compressed := compressor.EncodeAll(raw, nil)
		offset, err := output.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, uint64(offset))
		lengths = append(lengths, uint64(len(compressed)))
		if _, err := output.Write(compressed); err != nil {
			t.Fatal(err)
		}
	}
	var extra engine.StageRecord
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected stage tail: %v", err)
	}
	directoryOffset, err := output.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	var directory []byte
	for i := range offsets {
		directory = binary.LittleEndian.AppendUint64(directory, offsets[i])
		directory = binary.LittleEndian.AppendUint64(directory, lengths[i])
	}
	if _, err := output.Write(directory); err != nil {
		t.Fatal(err)
	}
	footer := binary.LittleEndian.AppendUint64(nil, uint64(len(offsets)))
	footer = binary.LittleEndian.AppendUint64(footer, uint64(directoryOffset))
	footer = append(footer, "RSD1"...)
	if _, err := output.Write(footer); err != nil {
		t.Fatal(err)
	}
	if err := output.Sync(); err != nil {
		t.Fatal(err)
	}
	info, err := output.Stat()
	if err != nil {
		t.Fatal(err)
	}
	verify, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	defer verify.Close()
	wantDecoder := json.NewDecoder(verify)
	var readFooter [20]byte
	if _, err := output.ReadAt(readFooter[:], info.Size()-20); err != nil || string(readFooter[16:]) != "RSD1" || binary.LittleEndian.Uint64(readFooter[:8]) != uint64(len(offsets)) {
		t.Fatalf("invalid residual footer: %v", err)
	}
	dirOffset := binary.LittleEndian.Uint64(readFooter[8:16])
	if dirOffset != uint64(directoryOffset) || int64(dirOffset)+int64(len(directory))+20 != info.Size() {
		t.Fatal("invalid residual directory bounds")
	}
	readDirectory := make([]byte, len(directory))
	if _, err := output.ReadAt(readDirectory, int64(dirOffset)); err != nil {
		t.Fatal(err)
	}
	uncompressor, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer uncompressor.Close()
	seen := 0
	for pageIndex := range offsets {
		offset := binary.LittleEndian.Uint64(readDirectory[pageIndex*16:])
		length := binary.LittleEndian.Uint64(readDirectory[pageIndex*16+8:])
		if offset != offsets[pageIndex] || length != lengths[pageIndex] || offset+length > dirOffset {
			t.Fatal("invalid residual page directory")
		}
		compressed := make([]byte, length)
		if _, err := output.ReadAt(compressed, int64(offset)); err != nil {
			t.Fatal(err)
		}
		raw, err := uncompressor.DecodeAll(compressed, nil)
		if err != nil {
			t.Fatal(err)
		}
		var page []engine.StageRecord
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		if len(page) != min(pageRows, rows-seen) {
			t.Fatal("residual page row count mismatch")
		}
		for _, record := range page {
			var want engine.StageRecord
			if err := wantDecoder.Decode(&want); err != nil {
				t.Fatal(err)
			}
			row := core[seen]
			record.Record.ProjectID = row.project
			severity := int16(row.severity)
			record.Record.SeverityNumber = &severity
			record.Record.Service = &row.service
			if err := json.Unmarshal(row.attrs, &record.Record.Attrs); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(record, want) {
				t.Fatalf("residual roundtrip row=%d id=%s", seen, want.Record.RecordID)
			}
			seen++
		}
	}
	if seen != rows || wantDecoder.Decode(&extra) != io.EOF {
		t.Fatal("residual record count mismatch")
	}
	t.Logf("compact_residual rows=%d source_page_rows=%d pages=%d source_bytes=%d index_bytes=%d total_bytes=%d pair_bytes=%d delta_bytes=%d", rows, pageRows, len(offsets), info.Size(), len(index), info.Size()+int64(len(index)), pairBytes, info.Size()+int64(len(index))-pairBytes)
	compound := filepath.Join(filepath.Dir(stage), "compound.bin")
	packed, err := os.Create(compound)
	if err != nil {
		t.Fatal(err)
	}
	defer packed.Close()
	if _, err := packed.Write(index); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	copied, copyErr := io.Copy(packed, source)
	closeErr := source.Close()
	if copyErr != nil || closeErr != nil || copied != info.Size() {
		t.Fatalf("compound source copy bytes=%d err=%v close=%v", copied, copyErr, closeErr)
	}
	compoundFooter := append([]byte("CRS1"), binary.LittleEndian.AppendUint64(nil, uint64(len(index)))...)
	compoundFooter = binary.LittleEndian.AppendUint64(compoundFooter, uint64(info.Size()))
	compoundFooter = binary.LittleEndian.AppendUint64(compoundFooter, binary.LittleEndian.Uint64(index[:8]))
	compoundFooter = binary.LittleEndian.AppendUint32(compoundFooter, crc32.ChecksumIEEE(compoundFooter))
	if _, err := packed.Write(compoundFooter); err != nil {
		t.Fatal(err)
	}
	if err := packed.Sync(); err != nil {
		t.Fatal(err)
	}
	compoundInfo, err := packed.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if compoundInfo.Size() != int64(len(index))+info.Size()+32 {
		t.Fatalf("compound size=%d", compoundInfo.Size())
	}
	t.Logf("compact_compound rows=%d object_bytes=%d pair_bytes=%d delta_bytes=%d", rows, compoundInfo.Size(), pairBytes, compoundInfo.Size()-pairBytes)
	if os.Getenv("EVENTGLASS_PRODUCT_COMPACT_COMPOUND_MINIO") == "1" {
		measureProductCompactCompoundMinIO(t, compound, payload, rows, stage)
	}
}
