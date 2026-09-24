//go:build duckdb_use_static_lib

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func measureProductMinIO(t *testing.T, ctx context.Context, root string, db *sql.DB, analytics, payload, single string, aggregate, detail engine.QueryOperation, firstRaw []byte, rows int) {
	t.Helper()
	endpoint, bucket := os.Getenv("EVENTGLASS_PRODUCT_MINIO_ENDPOINT"), os.Getenv("EVENTGLASS_PRODUCT_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Fatal("EVENTGLASS_PRODUCT_MINIO_ENDPOINT and EVENTGLASS_PRODUCT_MINIO_BUCKET are required")
	}
	store, err := storage.NewS3Store(ctx, storage.S3Config{
		Endpoint: endpoint, Region: "us-east-1", Bucket: bucket,
		Prefix:      fmt.Sprintf("searchlayout-%d", time.Now().UnixNano()),
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{"analytics": analytics, "payload": payload, "single": single}
	keys := []string{"analytics", "payload", "single"}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Delete(cleanupCtx, keys); err != nil {
			t.Error(err)
		}
	})
	var manifests []storage.ObjectManifest
	for _, key := range keys {
		evidence, err := storage.InspectFile(files[key])
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(files[key])
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
		manifest := storage.ObjectManifest{
			ObjectKey: key, Size: evidence.Bytes, SHA256: evidence.SHA256,
			BlockSize: evidence.BlockSize, BlockSHA256: evidence.BlockSHA256,
		}
		if key == "single" {
			manifest.Capability = "single-analytics"
			manifests = append(manifests, manifest)
			manifest.Capability = "single-payload"
			manifests = append(manifests, manifest)
		} else {
			manifest.Capability = "pair-" + key
			manifests = append(manifests, manifest)
		}
	}
	upload := store.OperationCounts()
	t.Logf("product_minio_upload rows=%d put=%d put_bytes=%d head=%d verify_get=%d verify_bytes=%d", rows, upload.PutRequests, upload.PutBytes, upload.HeadRequests, upload.FullGetRequests, upload.FullGetBytes)
	meter := &productS3RangeMeter{S3Store: store, counts: make(map[string]productRangeCount)}
	measureProductGatewayStore(t, ctx, root, db, "minio", meter, manifests, aggregate, detail, firstRaw, rows)
	after := store.OperationCounts()
	t.Logf("product_minio_query rows=%d range_get=%d range_bytes=%d", rows, after.RangeRequests-upload.RangeRequests, after.RangeBytes-upload.RangeBytes)
}
