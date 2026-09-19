package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"

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
	if counts.PutRequests != 1 || counts.HeadRequests != 1 || counts.FullGetRequests != 1 || counts.FullGetBytes != 4 {
		t.Fatalf("readback counts=%#v", counts)
	}
}
