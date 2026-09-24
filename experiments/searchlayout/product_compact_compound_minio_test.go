//go:build duckdb_use_static_lib

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/klauspost/compress/zstd"
)

func measureProductCompactCompoundMinIO(t *testing.T, compound, payload string, rows int, stage string) {
	t.Helper()
	ctx := context.Background()
	endpoint, bucket := os.Getenv("EVENTGLASS_PRODUCT_MINIO_ENDPOINT"), os.Getenv("EVENTGLASS_PRODUCT_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Fatal("EVENTGLASS_PRODUCT_MINIO_ENDPOINT and EVENTGLASS_PRODUCT_MINIO_BUCKET are required")
	}
	store, err := storage.NewS3Store(ctx, storage.S3Config{
		Endpoint: endpoint, Region: "us-east-1", Bucket: bucket,
		Prefix:      fmt.Sprintf("searchlayout-compound-%d", time.Now().UnixNano()),
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{"compound": compound, "payload": payload}
	var manifests []storage.ObjectManifest
	for _, key := range []string{"compound", "payload"} {
		evidence, err := storage.InspectFile(paths[key])
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(paths[key])
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.PutStream(ctx, key, file, evidence.Bytes, evidence.SHA256)
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, storage.ObjectManifest{Capability: key, ObjectKey: key, Size: evidence.Bytes, SHA256: evidence.SHA256, BlockSize: evidence.BlockSize, BlockSHA256: evidence.BlockSHA256})
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Delete(cleanupCtx, []string{"compound", "payload"}); err != nil {
			t.Error(err)
		}
	})
	upload := store.OperationCounts()
	t.Logf("compact_compound_upload put=%d put_bytes=%d head=%d verify_get=%d verify_bytes=%d", upload.PutRequests, upload.PutBytes, upload.HeadRequests, upload.FullGetRequests, upload.FullGetBytes)
	meter := &productS3RangeMeter{S3Store: store, counts: make(map[string]productRangeCount)}
	gateway, err := storage.NewGateway(meter, manifests)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gateway)
	defer server.Close()
	input, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	decoder := json.NewDecoder(input)
	const target = 236
	var want engine.StageRecord
	for i := 0; i <= target; i++ {
		if err := decoder.Decode(&want); err != nil {
			t.Fatal(err)
		}
	}
	compoundSize := manifests[0].Size
	readRange := func(offset, length int64) []byte {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/objects/compound", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusPartialContent || int64(len(body)) != length {
			t.Fatalf("compound Range offset=%d length=%d status=%d bytes=%d read=%v close=%v", offset, length, response.StatusCode, len(body), readErr, closeErr)
		}
		return body
	}
	decodeSource, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer decodeSource.Close()
	runCompound := func() {
		pages := uint64((rows + 127) / 128)
		tailSize := int64(pages*16 + 20 + 32)
		if tailSize > compoundSize {
			t.Fatal("invalid compound tail size")
		}
		tail := readRange(compoundSize-tailSize, tailSize)
		footer := tail[len(tail)-32:]
		if string(footer[:4]) != "CRS1" || crc32.ChecksumIEEE(footer[:28]) != binary.LittleEndian.Uint32(footer[28:]) {
			t.Fatal("invalid compound footer")
		}
		indexSize := binary.LittleEndian.Uint64(footer[4:12])
		sourceSize := binary.LittleEndian.Uint64(footer[12:20])
		coreSize := binary.LittleEndian.Uint64(footer[20:28])
		if indexSize < 8 || indexSize+sourceSize+32 != uint64(compoundSize) || coreSize > indexSize-8 {
			t.Fatal("invalid compound ranges")
		}
		sourceFooter := tail[len(tail)-52 : len(tail)-32]
		directory := binary.LittleEndian.Uint64(sourceFooter[8:16])
		if string(sourceFooter[16:]) != "RSD1" || binary.LittleEndian.Uint64(sourceFooter[:8]) != pages || directory+pages*16+20 != sourceSize || indexSize+directory != uint64(compoundSize-tailSize) {
			t.Fatal("invalid source directory")
		}
		corePacked := readRange(8, int64(coreSize))
		coreBody := binary.LittleEndian.AppendUint64(nil, coreSize)
		coreBody = append(coreBody, corePacked...)
		core, _, err := compactDecodeCore(coreBody, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		entry := tail[(target/128)*16:][:16]
		pageOffset, pageSize := binary.LittleEndian.Uint64(entry[:8]), binary.LittleEndian.Uint64(entry[8:])
		if pageSize == 0 || pageOffset+pageSize > directory {
			t.Fatal("invalid source page")
		}
		compressed := readRange(int64(indexSize+pageOffset), int64(pageSize))
		raw, err := decodeSource.DecodeAll(compressed, nil)
		if err != nil {
			t.Fatal(err)
		}
		var page []engine.StageRecord
		if err := json.Unmarshal(raw, &page); err != nil || len(page) <= target%128 {
			t.Fatalf("invalid source page rows=%d err=%v", len(page), err)
		}
		got := page[target%128]
		row := core[target]
		got.Record.ProjectID = row.project
		severity := int16(row.severity)
		got.Record.SeverityNumber = &severity
		got.Record.Service = &row.service
		if err := json.Unmarshal(row.attrs, &got.Record.Attrs); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("compound detail mismatch id=%s err=%v", want.Record.RecordID, err)
		}
	}
	runPayload := func() {
		db, err := engine.Open(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		var id, raw, metadata string
		err = db.QueryRowContext(ctx, "SELECT record_id,raw_json,canonical_metadata_json FROM read_parquet(?) WHERE record_id=?", server.URL+"/objects/payload", want.Record.RecordID).Scan(&id, &raw, &metadata)
		closeErr := db.Close()
		if err != nil || closeErr != nil || id != want.Record.RecordID || raw != string(want.Record.Raw) {
			t.Fatalf("payload detail id=%q err=%v close=%v", id, err, closeErr)
		}
		var reconstructed model.Record
		if err := json.Unmarshal([]byte(metadata), &reconstructed); err != nil {
			t.Fatal(err)
		}
		reconstructed.Raw = json.RawMessage(raw)
		reconstructed.EnvelopeSDKJSON = want.Record.EnvelopeSDKJSON
		if !reflect.DeepEqual(reconstructed, want.Record) {
			t.Fatal("payload metadata differs from original record")
		}
	}
	cpuUS := func() int64 {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		return usage.Utime.Sec*1_000_000 + usage.Utime.Usec + usage.Stime.Sec*1_000_000 + usage.Stime.Usec
	}
	var wall [2][]int64
	var cpu [2][]int64
	var gets [2][]int64
	var readBytes [2][]int64
	for i := range 30 {
		for turn := range 2 {
			choice := (i + turn) % 2
			key := []string{"compound", "payload"}[choice]
			meter.take(key)
			startCPU, start := cpuUS(), time.Now()
			if choice == 0 {
				runCompound()
			} else {
				runPayload()
			}
			wall[choice] = append(wall[choice], time.Since(start).Microseconds())
			cpu[choice] = append(cpu[choice], cpuUS()-startCPU)
			reads := meter.take(key)
			gets[choice] = append(gets[choice], reads.gets)
			readBytes[choice] = append(readBytes[choice], reads.bytes)
		}
	}
	for choice, label := range []string{"compound", "payload"} {
		for _, values := range [][]int64{wall[choice], cpu[choice], gets[choice], readBytes[choice]} {
			slices.Sort(values)
		}
		if choice == 0 && (gets[choice][0] != 3 || gets[choice][29] != 3 || readBytes[choice][0] != readBytes[choice][29]) {
			t.Fatal("compound detail did not use three stable verified Range reads")
		}
		t.Logf("compact_compound_minio source=%s rows=%d reps=30 p50_us=%d p95_us=%d p99_us=%d cpu_p50_us=%d cpu_p95_us=%d get_p50=%d bytes_p50=%d", label, rows, wall[choice][15], wall[choice][28], wall[choice][29], cpu[choice][15], cpu[choice][28], gets[choice][15], readBytes[choice][15])
	}
	after := store.OperationCounts()
	t.Logf("compact_compound_minio_query range_get=%d range_bytes=%d", after.RangeRequests-upload.RangeRequests, after.RangeBytes-upload.RangeBytes)
}
