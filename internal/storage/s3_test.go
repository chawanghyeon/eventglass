package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) Do(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestS3ObjectKeyStaysInsideConfiguredPrefix(t *testing.T) {
	store := &S3Store{prefix: "installation/live"}
	for _, invalid := range []string{"", "/absolute", "../escape", "a/../../escape", `a\\b`} {
		if _, err := store.objectKey(invalid); err == nil {
			t.Fatalf("objectKey(%q) succeeded", invalid)
		}
	}
	key, err := store.objectKey("journals/tenant/lane/object")
	if err != nil {
		t.Fatal(err)
	}
	if key != "installation/live/journals/tenant/lane/object" {
		t.Fatalf("key = %q", key)
	}
}

func TestS3RequiresExplicitRegionAndBucket(t *testing.T) {
	if _, err := NewS3Store(context.Background(), S3Config{}); err == nil {
		t.Fatal("empty S3 configuration succeeded")
	}
}

func TestPutStreamRejectsCorrectHeadMetadataWithWrongStoredBytes(t *testing.T) {
	good := []byte("good")
	digest := sha256.Sum256(good)
	checksum := hex.EncodeToString(digest[:])
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		header := make(http.Header)
		body := io.NopCloser(strings.NewReader(""))
		switch request.Method {
		case http.MethodPut:
			actual, err := io.ReadAll(request.Body)
			if err != nil || !bytes.Equal(actual, good) {
				t.Fatalf("uploaded bytes=%q err=%v", actual, err)
			}
			header.Set("ETag", `"test"`)
		case http.MethodHead:
			header.Set("Content-Length", "4")
			header.Set("X-Amz-Meta-Eventglass-Sha256", checksum)
			header.Set("ETag", `"test"`)
		case http.MethodGet:
			header.Set("Content-Length", "4")
			body = io.NopCloser(strings.NewReader("evil"))
		default:
			t.Fatalf("unexpected S3 method %s", request.Method)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: header, Body: body, Request: request}, nil
	})
	client := awss3.NewFromConfig(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: transport,
	}, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String("https://s3.invalid")
		options.UsePathStyle = true
	})
	store := &S3Store{client: client, bucket: "bucket"}
	_, err := store.PutStream(context.Background(), "journal", bytes.NewReader(good), int64(len(good)), checksum)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("wrong stored bytes were accepted: %v", err)
	}
	counts := store.OperationCounts()
	if counts.PutRequests != 1 || counts.PutBytes != 4 || counts.HeadRequests != 1 || counts.FullGetRequests != 1 || counts.FullGetBytes != 4 {
		t.Fatalf("readback counts=%#v", counts)
	}
}

func TestVerifiedDownloadRejectsLimitsBeforeCreatingFilesOrRequestingS3(t *testing.T) {
	store := &S3Store{} // Any attempted S3 request is a test failure/panic.
	for _, test := range []struct{ size, limit int64 }{
		{0, MaxJournalBytes}, {-1, MaxJournalBytes}, {MaxJournalBytes + 1, MaxJournalBytes},
		{1, 0}, {1, -1}, {1, model.MaxBundleFileBytes + 1}, {model.MaxBundleFileBytes + 1, model.MaxBundleFileBytes},
	} {
		path := filepath.Join(t.TempDir(), "must-not-exist")
		if err := store.DownloadToFile(context.Background(), "object", path, test.size, strings.Repeat("0", 64), test.limit); err == nil {
			t.Fatalf("invalid bound admitted: %+v", test)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid bound created file: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.DownloadToFile(ctx, "object", filepath.Join(t.TempDir(), "canceled"), 1, strings.Repeat("0", 64), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission: %v", err)
	}
}

type closingDownloadBody struct {
	ctx                       context.Context
	started, closing, release chan struct{}
	first                     bool
}

func (body *closingDownloadBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if !body.first {
		body.first = true
		buffer[0] = 'v'
		return 1, nil
	}
	close(body.started)
	<-body.ctx.Done()
	return 0, body.ctx.Err()
}
func (body *closingDownloadBody) Close() error {
	close(body.closing)
	<-body.release
	return nil
}

func TestVerifiedDownloadJoinsResponseCloseBeforeRemovingPartialFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &closingDownloadBody{ctx: ctx, started: make(chan struct{}), closing: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(body.release) }) }
	defer release()
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", ContentLength: 2, Header: http.Header{"Content-Length": []string{"2"}}, Body: body, Request: request}, nil
	})
	client := awss3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: transport}, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String("https://s3.invalid")
		options.UsePathStyle = true
	})
	store := &S3Store{client: client, bucket: "bucket"}
	path := filepath.Join(t.TempDir(), "partial")
	done := make(chan error, 1)
	go func() { done <- store.DownloadToFile(ctx, "object", path, 2, strings.Repeat("0", 64), 2) }()
	select {
	case <-body.started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	cancel()
	select {
	case <-body.closing:
	case <-time.After(5 * time.Second):
		t.Fatal("response close did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before response close: %v", err)
	default:
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 1 {
		t.Errorf("partial removed while response still owned: %v %v", info, err)
	}
	release()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial survived joined cancellation: %v", err)
	}
}
