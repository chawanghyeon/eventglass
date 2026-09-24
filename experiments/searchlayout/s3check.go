package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3RangeSource struct {
	client *s3.Client
	bucket string
	prefix string
}

func (s s3RangeSource) Get(ctx context.Context, name string, offset, size int64) ([]byte, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + name),
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	wantRange := fmt.Sprintf("bytes %d-%d/", offset, offset+size-1)
	if resp.ContentRange == nil || len(*resp.ContentRange) < len(wantRange) || (*resp.ContentRange)[:len(wantRange)] != wantRange {
		return nil, fmt.Errorf("unexpected S3 Content-Range: %v", resp.ContentRange)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

// runMinIO uploads fresh immutable segment files to an isolated local bucket,
// then runs the same query through signed S3 Range GET requests.
func newMinIOClient(ctx context.Context, endpoint, bucket string) (*s3.Client, error) {
	access := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if access == "" || secret == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")
	}
	cfg := aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return nil, err
	}
	return client, nil
}

func uploadSegments(ctx context.Context, client *s3.Client, bucket, prefix, dir string, c corpus) error {
	for _, seg := range c.Segs {
		f, err := os.Open(filepath.Join(dir, seg.Name))
		if err != nil {
			return err
		}
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(prefix + seg.Name), Body: f,
		})
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func runMinIO(ctx context.Context, endpoint, bucket, dir string, c corpus, q query, repetitions int) (measurement, error) {
	client, err := newMinIOClient(ctx, endpoint, bucket)
	if err != nil {
		return measurement{}, err
	}
	if err := uploadSegments(ctx, client, bucket, "", dir, c); err != nil {
		return measurement{}, err
	}
	want, err := run(ctx, localSource{dir: dir}, c, q, denseColumns)
	if err != nil {
		return measurement{}, err
	}
	_, m, err := measure(repetitions, func() (result, int, int64, error) {
		src := &measuredSource{src: s3RangeSource{client: client, bucket: bucket}}
		value, err := run(ctx, src, c, q, denseColumns)
		if err == nil && !equivalent(value, want) {
			err = fmt.Errorf("S3 Range result differed from local result")
		}
		return value, src.calls, src.bytes, err
	})
	return m, err
}
