package api

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/sha256"
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
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/testkit"
	"github.com/klauspost/compress/zstd"
)

func TestIngestSupportsBoundedCompressionAndAuthenticates(t *testing.T) {
	body := []byte("{\"dsn\":\"http://fixturePublicKey@127.0.0.1:8123/1\"}\n{\"type\":\"event\"}\n{\"event_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"message\":\"hello\"}")
	for _, encoding := range []string{"identity", "gzip", "deflate", "br", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			sink := &testkit.MemorySink{}
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
	sink := &testkit.MemorySink{}
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
	sink := &testkit.MemorySink{}
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
	sink := &testkit.MemorySink{}
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
	sink := &testkit.MemorySink{}
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
		TenantID: 1, ProjectID: 1, PublicKey: "fixturePublicKey", Sink: &testkit.MemorySink{},
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

	handler.admission = resource.NewBudget(30 << 20)
	if _, ok := handler.acquireDecoder(MaxWireBytes); ok {
		t.Fatal("request exceeding byte admission was admitted")
	}
}

func newTestHandler(t *testing.T, sink Acceptor, rateLimited func() bool) *IngestHandler {
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

var _ Acceptor = (*recordingSink)(nil)

type recordingSink struct {
	batches  []model.NormalizedRequest
	lastAuth ingest.Authorization
}

func (sink *recordingSink) Accept(_ context.Context, command ingest.Command) (control.ReceiptResult, error) {
	sink.lastAuth = command.Authorization
	sink.batches = append(sink.batches, command.Request)
	return control.ReceiptResult{AcceptanceID: command.Request.AcceptanceID}, nil
}

func TestAuthenticationSnapshotDoesNotEnterCanonicalData(t *testing.T) {
	sink := &recordingSink{}
	handler, err := NewIngestHandler(Config{TenantID: 2, ProjectID: 7, PublicKey: "fixturePublicKey",
		Sink: sink, ProjectRevision: 9, KeyRevision: 3, ScrubRevision: 5})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/api/7/envelope/?sentry_key=fixturePublicKey", strings.NewReader("{}"))
	request.Header.Set("User-Agent", "private-transport-marker")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	if sink.lastAuth != (ingest.Authorization{TenantID: 2, ProjectID: 7, KeyHash: sha256.Sum256([]byte("fixturePublicKey")), TenantRevision: 1, ProjectRevision: 9, KeyRevision: 3, ScrubRevision: 5, ConfigRevision: 1}) {
		t.Fatal("auth snapshot was lost")
	}
	encoded, err := json.Marshal(sink.batches[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("fixturePublicKey")) || bytes.Contains(encoded, []byte("private-transport-marker")) || bytes.Contains(encoded, []byte("KeyHash")) {
		t.Fatal("transport/auth data entered canonical request")
	}
}

func TestDynamicAuthorizationControlsPolicyAndReceipt(t *testing.T) {
	sink := &recordingSink{}
	var resolvedHash [32]byte
	handler, err := NewIngestHandler(Config{
		TenantID: 2, ProjectID: 7, Sink: sink,
		ResolveAuthorization: func(_ context.Context, tenantID, projectID int64, keyHash [32]byte) (control.ProjectAuthorization, error) {
			if tenantID != 2 || projectID != 7 {
				t.Fatal("resolver scope changed")
			}
			resolvedHash = keyHash
			return control.ProjectAuthorization{
				Snapshot:       control.AuthorizationSnapshot{TenantRevision: 4, ProjectRevision: 9, KeyRevision: 3, ScrubRevision: 5, ConfigRevision: 6, KeyHash: keyHash},
				DefaultService: "dynamic-service", AllowedOrigins: []string{"https://dynamic.invalid"},
			}, nil
		},
		ResolveOrigins: func(context.Context, int64, int64) ([]string, error) { return []string{"https://dynamic.invalid"}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/api/7/envelope/?sentry_key=dynamic-key", strings.NewReader("{}\n{\"type\":\"event\"}\n{\"message\":\"dynamic\"}"))
	request.Header.Set("Origin", "https://dynamic.invalid")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Eventglass-Receipt") == "" || resolvedHash != sha256.Sum256([]byte("dynamic-key")) {
		t.Fatalf("dynamic response=%d headers=%#v hash=%x", response.Code, response.Header(), resolvedHash)
	}
	if sink.lastAuth.TenantRevision != 4 || sink.lastAuth.ConfigRevision != 6 || sink.lastAuth.KeyHash != resolvedHash || len(sink.batches) != 1 || sink.batches[0].Records[0].Service == nil || *sink.batches[0].Records[0].Service != "dynamic-service" {
		t.Fatalf("dynamic auth/policy lost: auth=%#v batches=%#v", sink.lastAuth, sink.batches)
	}
	preflight := httptest.NewRequest(http.MethodOptions, "/api/7/envelope/", nil)
	preflight.Header.Set("Origin", "https://dynamic.invalid")
	preflight.Header.Set("Access-Control-Request-Headers", "content-type")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, preflight)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "https://dynamic.invalid" {
		t.Fatalf("dynamic preflight=%d %#v", response.Code, response.Header())
	}

	denied, err := NewIngestHandler(Config{
		TenantID: 2, ProjectID: 7, Sink: sink,
		ResolveAuthorization: func(context.Context, int64, int64, [32]byte) (control.ProjectAuthorization, error) {
			return control.ProjectAuthorization{}, control.ErrKeyRevoked
		},
		ResolveOrigins: func(context.Context, int64, int64) ([]string, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	denied.ServeHTTP(response, httptest.NewRequest("POST", "/api/7/envelope/?sentry_key=revoked", strings.NewReader("{}")))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("revoked dynamic key status=%d", response.Code)
	}
}

func TestWorkingBudgetRejectsBeforeParsingAndReleasesOnErrors(t *testing.T) {
	budget := resource.NewBudget(50 << 20)
	sink := &testkit.MemorySink{}
	handler, err := NewIngestHandler(Config{TenantID: 1, ProjectID: 1, PublicKey: "fixture", Sink: sink, WorkingBudget: budget})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/api/1/envelope/?sentry_key=fixture", strings.NewReader(strings.Repeat("x", 100000)))
	handler.ServeHTTP(response, request)
	if response.Code != 429 || budget.Used() != 0 || handler.admission.Used() != 0 {
		t.Fatal("working admission failed")
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("POST", "/api/1/envelope/?sentry_key=fixture", strings.NewReader("invalid")))
	if response.Code != 400 || budget.Used() != 0 || handler.admission.Used() != 0 {
		t.Fatal("parse failure leaked permit")
	}
	if err := budget.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("POST", "/api/1/envelope/?sentry_key=fixture", strings.NewReader("{}")))
	if response.Code != 429 || len(sink.Batches()) != 0 {
		t.Fatal("draining budget admitted work")
	}
}
