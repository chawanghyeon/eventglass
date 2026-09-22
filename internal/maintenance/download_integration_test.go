//go:build integration

package maintenance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

func TestMaintenanceDownloadsS3FailureAndRetry(t *testing.T) {
	endpoint, err := url.Parse(os.Getenv("EVENTGLASS_S3_ENDPOINT"))
	if err != nil || endpoint == nil || endpoint.Scheme != "http" || (endpoint.Hostname() != "minio" && endpoint.Hostname() != "127.0.0.1" && endpoint.Hostname() != "localhost") {
		t.Fatal("explicit disposable localhost/MinIO endpoint required")
	}
	for _, name := range []string{"EVENTGLASS_S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		if os.Getenv(name) == "" {
			t.Fatalf("missing explicit fixture setting %s", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store, err := storage.NewS3Store(ctx, storage.S3Config{Endpoint: endpoint.String(), Region: "us-east-1", PathStyle: true,
		Bucket: os.Getenv("EVENTGLASS_S3_BUCKET"), Prefix: "maintenance-download-test-" + uuid.NewString(),
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY")})
	if err != nil {
		t.Fatal(err)
	}
	work := downloadWork(8)
	var keys []string
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := store.Delete(cleanup, keys); err != nil {
			t.Error(err)
		}
	}()
	for index := range work.Inputs {
		for _, file := range []*control.CompactionFile{&work.Inputs[index].Analytics, &work.Inputs[index].Payload} {
			data := []byte(file.ObjectKey)
			digest := sha256.Sum256(data)
			file.Bytes, file.SHA256 = int64(len(data)), hex.EncodeToString(digest[:])
			keys = append(keys, file.ObjectKey)
			if _, err := store.Put(ctx, file.ObjectKey, data); err != nil {
				t.Fatal(err)
			}
		}
	}
	var active atomic.Int32
	observed := downloadStore{download: func(ctx context.Context, key, path string, size int64, checksum string, limit int64) error {
		active.Add(1)
		defer active.Add(-1)
		return store.DownloadToFile(ctx, key, path, size, checksum, limit)
	}}
	key := work.Inputs[1].Payload.ObjectKey
	for _, phase := range []string{"missing", "retry", "corrupt", "retry"} {
		if phase == "missing" {
			err = store.Delete(ctx, []string{key})
		} else if phase == "corrupt" {
			data := []byte(key)
			data[0] ^= 1 // Same length, different actual bytes and metadata.
			_, err = store.Put(ctx, key, data)
		} else {
			_, err = store.Put(ctx, key, []byte(key))
		}
		if err != nil {
			t.Fatal(err)
		}
		before := store.OperationCounts()
		inputs, err := downloadInputs(ctx, observed, t.TempDir(), work)
		if active.Load() != 0 || (err == nil) != (phase == "retry") {
			t.Fatalf("phase=%s active=%d error=%v", phase, active.Load(), err)
		}
		if phase != "retry" {
			continue
		}
		after := store.OperationCounts()
		if len(inputs) != 8 || after.FullGetRequests-before.FullGetRequests != 16 || after.HeadRequests != before.HeadRequests || after.RangeRequests != before.RangeRequests {
			t.Fatalf("unexpected retry requests before=%+v after=%+v inputs=%d", before, after, len(inputs))
		}
		for index, input := range inputs {
			for role, path := range []string{input.AnalyticsPath, input.PayloadPath} {
				expected := []control.CompactionFile{work.Inputs[index].Analytics, work.Inputs[index].Payload}[role]
				data, err := os.ReadFile(path)
				if err != nil || string(data) != expected.ObjectKey {
					t.Fatalf("phase=%s input=%d role=%d data=%q error=%v", phase, index, role, data, err)
				}
			}
		}
	}
}
