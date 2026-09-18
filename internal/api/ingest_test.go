package api

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/klauspost/compress/zstd"
)

func TestIngestSupportsBoundedCompressionAndAuthenticates(t *testing.T) {
	body := []byte("{\"dsn\":\"http://fixturePublicKey@127.0.0.1:8123/1\"}\n{\"type\":\"event\"}\n{\"event_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"message\":\"hello\"}")
	for _, encoding := range []string{"identity", "gzip", "deflate", "br", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			sink := &MemorySink{}
			handler := newTestHandler(t, sink, nil)
			request := httptest.NewRequest(http.MethodPost, "/api/1/envelope/", bytes.NewReader(encodeBody(t, encoding, body)))
			request.Header.Set("Content-Encoding", encoding)
			request.Header.Set("X-Sentry-Auth", "Sentry sentry_version=7, sentry_key=fixturePublicKey")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			batches := sink.Batches()
			if len(batches) != 1 || len(batches[0].Records) != 1 || batches[0].Records[0].Message != "hello" {
				t.Fatalf("unexpected batch: %#v", batches)
			}
			if response.Header().Get("X-Eventglass-Receipt") == "" {
				t.Fatal("receipt header missing")
			}
		})
	}
}

func TestIngestRejectsConflictingIdentityAndIsAtomic(t *testing.T) {
	sink := &MemorySink{}
	handler := newTestHandler(t, sink, nil)
	body := "{\"dsn\":\"http://otherKey@127.0.0.1:8123/1\"}\n{\"type\":\"event\"}\n{\"message\":\"no\"}"
	request := httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || len(sink.Batches()) != 0 {
		t.Fatalf("identity conflict status=%d batches=%d", response.Code, len(sink.Batches()))
	}
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey&sentry_key=otherKey", strings.NewReader("{}"))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || len(sink.Batches()) != 0 {
		t.Fatalf("duplicate query identity status=%d batches=%d", response.Code, len(sink.Batches()))
	}

	body = "{}\n{\"type\":\"event\"}\n{\"message\":\"valid-first\"}\n{\"type\":\"log\"}\n{\"version\":3,\"items\":[]}"
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", bytes.NewBufferString(body))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(sink.Batches()) != 0 {
		t.Fatalf("atomic validation status=%d batches=%d body=%s", response.Code, len(sink.Batches()), response.Body.String())
	}
}

func TestIngestCORSUnknownItemsRateLimitAndDependencyFailure(t *testing.T) {
	sink := &MemorySink{}
	rateLimited := false
	handler := newTestHandler(t, sink, func() bool { return rateLimited })
	preflight := httptest.NewRequest(http.MethodOptions, "/api/1/envelope/", nil)
	preflight.Header.Set("Origin", "https://fixture.invalid")
	preflight.Header.Set("Access-Control-Request-Headers", "content-type, x-sentry-auth")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, preflight)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "https://fixture.invalid" || response.Header().Get("Vary") != "Origin" {
		t.Fatalf("bad preflight: %d %#v", response.Code, response.Header())
	}

	body := "{}\n{\"type\":\"attachment\",\"length\":4}\n\x00\n\xff\x01"
	request := httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", bytes.NewBufferString(body))
	request.Header.Set("Origin", "https://fixture.invalid")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Eventglass-Unsupported-Items") != "1" {
		t.Fatalf("unknown item status=%d headers=%#v body=%s", response.Code, response.Header(), response.Body.String())
	}
	unsupportedBatch := sink.Batches()[0]
	if len(unsupportedBatch.UnsupportedItems) != 1 || unsupportedBatch.UnsupportedItems[0].Type != "attachment" || unsupportedBatch.UnsupportedItems[0].Bytes != 4 {
		t.Fatalf("unknown item diagnostics=%#v", unsupportedBatch.UnsupportedItems)
	}

	rateLimited = true
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", bytes.NewBufferString("{}"))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") == "" || response.Header().Get("X-Sentry-Rate-Limits") == "" {
		t.Fatalf("bad rate limit response: %d %#v", response.Code, response.Header())
	}

	rateLimited = false
	sink.SetError(context.DeadlineExceeded)
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", bytes.NewBufferString("{}"))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("dependency status=%d", response.Code)
	}
}

func TestStoreResponseAndMalformedInputs(t *testing.T) {
	sink := &MemorySink{}
	handler := newTestHandler(t, sink, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/1/store/?sentry_key=fixturePublicKey", bytes.NewBufferString(`{"event_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","message":"legacy"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var result map[string]string
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result["id"] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("bad store response: %d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/api/1/store/?sentry_key=fixturePublicKey", strings.NewReader(`{"message":"server id"}`))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result["id"]) != 32 {
		t.Fatalf("missing-id store response: %d %s", response.Code, response.Body.String())
	}

	legacyJSON := []byte(`{"event_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","message":"legacy zlib"}`)
	var compressed bytes.Buffer
	zlibWriter := zlib.NewWriter(&compressed)
	if _, err := zlibWriter.Write(legacyJSON); err != nil {
		t.Fatal(err)
	}
	if err := zlibWriter.Close(); err != nil {
		t.Fatal(err)
	}
	form := url.Values{"sentry_data": {base64.StdEncoding.EncodeToString(compressed.Bytes())}}.Encode()
	request = httptest.NewRequest(http.MethodPost, "/api/1/store/?sentry_key=fixturePublicKey", strings.NewReader(form))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
		t.Fatalf("legacy base64+zlib store response: %d %s", response.Code, response.Body.String())
	}

	for _, test := range []struct {
		encoding, body string
		status         int
	}{
		{"snappy", "{}", http.StatusUnsupportedMediaType},
		{"identity", "{\"a\":1,\"a\":2}", http.StatusBadRequest},
	} {
		request = httptest.NewRequest(http.MethodPost, "/api/1/store/?sentry_key=fixturePublicKey", bytes.NewBufferString(test.body))
		request.Header.Set("Content-Encoding", test.encoding)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("encoding=%s status=%d body=%s", test.encoding, response.Code, response.Body.String())
		}
	}
}

func TestMixedEventAndLogsItemCountAndCanonicalLimits(t *testing.T) {
	sink := &MemorySink{}
	handler := newTestHandler(t, sink, nil)
	body := "{}\n{\"type\":\"event\"}\n{\"message\":\"event\"}\n" +
		"{\"type\":\"log\",\"item_count\":2}\n{\"version\":2,\"items\":[{\"body\":\"one\"},{\"body\":\"two\"}]}"
	request := httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(sink.Batches()) != 1 || len(sink.Batches()[0].Records) != 3 {
		t.Fatalf("mixed envelope status=%d batches=%#v body=%s", response.Code, sink.Batches(), response.Body.String())
	}

	badCount := "{}\n{\"type\":\"log\",\"item_count\":2}\n{\"version\":2,\"items\":[{\"body\":\"one\"}]}"
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", strings.NewReader(badCount))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(sink.Batches()) != 1 {
		t.Fatalf("item_count mismatch was not atomic: status=%d batches=%d", response.Code, len(sink.Batches()))
	}

	oversized := "{}\n{\"type\":\"event\"}\n{\"message\":\"" + strings.Repeat("x", 1<<20) + "\"}"
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=fixturePublicKey", strings.NewReader(oversized))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || len(sink.Batches()) != 1 {
		t.Fatalf("canonical limit status=%d batches=%d body=%s", response.Code, len(sink.Batches()), response.Body.String())
	}
}

func TestDecoderConcurrencyAndByteAdmission(t *testing.T) {
	handler, err := NewIngestHandler(Config{
		TenantID: 1, ProjectID: 1, PublicKey: "fixturePublicKey", Sink: &MemorySink{},
		DecoderSlots: 2, IngressBytes: 41 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, ok := handler.acquireDecoder(1)
	if !ok {
		t.Fatal("first decoder was not admitted")
	}
	second, ok := handler.acquireDecoder(1)
	if !ok {
		first()
		t.Fatal("second decoder was not admitted")
	}
	if _, ok := handler.acquireDecoder(1); ok {
		t.Fatal("third concurrent decoder was admitted")
	}
	second()
	first()

	handler.admission.limit = 30 << 20
	if _, ok := handler.acquireDecoder(MaxWireBytes); ok {
		t.Fatal("request exceeding byte admission was admitted")
	}
}

func newTestHandler(t *testing.T, sink BatchSink, rateLimited func() bool) *IngestHandler {
	t.Helper()
	handler, err := NewIngestHandler(Config{
		TenantID: 1, ProjectID: 1, PublicKey: "fixturePublicKey", Sink: sink,
		AllowedOrigins: []string{"https://fixture.invalid"}, DefaultService: "fixture",
		Now: func() time.Time { return time.Unix(1_767_323_045, 0) }, RateLimited: rateLimited,
		ForbiddenFixtureValue: "eventglass-live-scrub-sentinel",
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func encodeBody(t *testing.T, encoding string, body []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	var writer io.WriteCloser
	switch encoding {
	case "identity":
		return body
	case "gzip":
		writer = gzip.NewWriter(&output)
	case "deflate":
		writer = zlib.NewWriter(&output)
	case "br":
		writer = brotli.NewWriter(&output)
	case "zstd":
		encoder, err := zstd.NewWriter(&output, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		writer = encoder
	default:
		t.Fatalf("unsupported test encoding %s", encoding)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

var _ BatchSink = (*recordingSink)(nil)

type recordingSink struct{ batches []model.Batch }

func (sink *recordingSink) Accept(_ context.Context, batch model.Batch) error {
	sink.batches = append(sink.batches, batch)
	return nil
}
