package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestUniversalRangeLatency(t *testing.T) {
	endpoint := os.Getenv("EVENTGLASS_UNIFIED_MINIO")
	if endpoint == "" {
		t.Skip("local MinIO endpoint required")
	}
	docs := universalFixture(10000)
	path := filepath.Join(t.TempDir(), "universal.bin")
	_, size, err := writeUniversalObject(path, docs)
	if err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("eventglass-unified-baseline-%d", os.Getpid())
	client, err := newMinIOClient(context.Background(), endpoint, bucket)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("universal.bin"), Body: f})
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("universal.bin")})
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	remote := s3RangeSource{client: client, bucket: bucket}
	for _, tc := range []struct {
		name string
		p    universalPredicate
	}{{"rare", universalPredicate{op: "term", text: "fatal"}}, {"broad", universalPredicate{op: "term", text: "request"}}, {"regex", universalPredicate{op: "regex", text: "timeout|한글"}}} {
		want, wantDetails := oracleUniversalRange(docs, tc.p, "tags/region")
		var micros []int64
		var calls int
		var bytes int64
		for range 30 {
			meter := &measuredSource{src: remote}
			start := time.Now()
			got, details, err := runUniversalRange(context.Background(), meter, size, tc.p, "tags/region")
			micros = append(micros, time.Since(start).Microseconds())
			if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(details, wantDetails) {
				t.Fatalf("query=%s result mismatch: %v", tc.name, err)
			}
			if calls != 0 && (calls != meter.calls || bytes != meter.bytes) {
				t.Fatalf("non-deterministic S3 I/O for %s", tc.name)
			}
			calls, bytes = meter.calls, meter.bytes
		}
		slices.Sort(micros)
		t.Logf("minio query=%s GET=%d bytes=%d p50_us=%d p95_us=%d p99_us=%d reps=%d", tc.name, calls, bytes, micros[15], micros[28], micros[29], len(micros))
	}
	t.Logf("objectBytes=%d", size)
}
