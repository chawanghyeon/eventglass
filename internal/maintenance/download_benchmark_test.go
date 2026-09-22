package maintenance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

// Real provider/full-SHA download cost only. Deterministic opaque bytes are not
// a Parquet/native rewrite or an end-to-end maintenance/SLO oracle. The runner
// owns a disposable MinIO installation; never fall back to ambient credentials.
func BenchmarkMaintenanceDownloads(b *testing.B) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" {
		b.Fatal("benchmark requires Linux ARM64")
	}
	endpoint, err := url.Parse(os.Getenv("EVENTGLASS_S3_ENDPOINT"))
	if err != nil || endpoint == nil || endpoint.Scheme != "http" || (endpoint.Hostname() != "minio" && endpoint.Hostname() != "127.0.0.1" && endpoint.Hostname() != "localhost") {
		b.Fatal("explicit disposable localhost/MinIO endpoint required")
	}
	for _, name := range []string{"EVENTGLASS_S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		if os.Getenv(name) == "" {
			b.Fatalf("missing explicit fixture setting %s", name)
		}
	}
	for _, pairCount := range []int{16, 128} {
		for _, fileBytes := range []int{8 << 10, 256 << 10} {
			b.Run(fmt.Sprintf("pairs-%d/bytes-%d", pairCount, fileBytes), func(b *testing.B) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				store, err := storage.NewS3Store(ctx, storage.S3Config{
					Endpoint: endpoint.String(), Region: "us-east-1", Bucket: os.Getenv("EVENTGLASS_S3_BUCKET"),
					Prefix: "maintenance-download-benchmark-" + uuid.NewString(), PathStyle: true,
					AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
				})
				if err != nil {
					b.Fatal(err)
				}
				keys := make([]string, 0, pairCount*2)
				b.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					if len(keys) > 0 {
						if err := store.Delete(cleanup, keys); err != nil {
							b.Error(err)
						}
					}
				})
				work := control.CompactionWork{Inputs: make([]control.CompactionWorkInput, pairCount)}
				fixtureHash := sha256.New()
				for index := range work.Inputs {
					input := &work.Inputs[index]
					input.BundleID = fmt.Sprintf("bundle-%d", index)
					for role, file := range []*control.CompactionFile{&input.Analytics, &input.Payload} {
						data := make([]byte, fileBytes)
						for offset := range data {
							data[offset] = byte((offset*17 + index*31 + role*97) % 251)
						}
						fixtureHash.Write(data)
						checksum := sha256.Sum256(data)
						file.ObjectKey = fmt.Sprintf("input-%03d-%d", index, role)
						file.Bytes, file.SHA256 = int64(fileBytes), hex.EncodeToString(checksum[:])
						keys = append(keys, file.ObjectKey)
						if _, err := store.Put(ctx, file.ObjectKey, data); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.Logf("fixture pairs=%d bytes=%d sha256=%x", pairCount, pairCount*2*fileBytes, fixtureHash.Sum(nil))
				root := filepath.Join(b.TempDir(), "inputs")
				run := func(verify bool) {
					if err := os.Mkdir(root, 0o700); err != nil {
						b.Fatal(err)
					}
					inputs, err := downloadInputs(ctx, store, root, work)
					if err != nil || len(inputs) != pairCount {
						b.Fatalf("inputs=%d error=%v", len(inputs), err)
					}
					if verify {
						for index, input := range inputs {
							for role, path := range []string{input.AnalyticsPath, input.PayloadPath} {
								evidence, err := storage.InspectFile(path)
								expected := []control.CompactionFile{work.Inputs[index].Analytics, work.Inputs[index].Payload}[role]
								if err != nil || evidence.Bytes != expected.Bytes || evidence.SHA256 != expected.SHA256 || input.BundleID != work.Inputs[index].BundleID {
									b.Fatalf("download identity index=%d role=%d error=%v", index, role, err)
								}
							}
						}
					}
					if err := os.RemoveAll(root); err != nil {
						b.Fatal(err)
					}
				}
				run(true)
				before := store.OperationCounts()
				b.ReportAllocs()
				b.SetBytes(int64(pairCount * 2 * fileBytes))
				b.ResetTimer()
				for range b.N {
					run(false)
				}
				b.StopTimer()
				after := store.OperationCounts()
				requests, transferred := after.FullGetRequests-before.FullGetRequests, after.FullGetBytes-before.FullGetBytes
				if requests != uint64(b.N*pairCount*2) || transferred != uint64(b.N*pairCount*2*fileBytes) || after.PutRequests != before.PutRequests || after.HeadRequests != before.HeadRequests || after.RangeRequests != before.RangeRequests {
					b.Fatalf("unexpected provider I/O before=%+v after=%+v", before, after)
				}
				b.ReportMetric(float64(requests)/float64(b.N), "S3-GET/op")
				b.ReportMetric(float64(transferred)/float64(b.N), "S3-B/op")
				status, err := os.ReadFile("/proc/self/status")
				if err != nil {
					b.Fatal(err)
				}
				for _, line := range strings.Split(string(status), "\n") {
					fields := strings.Fields(line)
					if len(fields) == 3 && (fields[0] == "VmRSS:" || fields[0] == "VmHWM:") {
						value, err := strconv.ParseUint(fields[1], 10, 64)
						if err != nil {
							b.Fatal(err)
						}
						b.ReportMetric(float64(value*1024), strings.TrimSuffix(fields[0], ":")+"-B")
					}
				}
			})
		}
	}
}
