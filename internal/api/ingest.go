package api

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/klauspost/compress/zstd"
)

const (
	MaxWireBytes         = 20 << 20
	MaxDecompressedBytes = 20 << 20
	DefaultIngressBytes  = 64 << 20
	DefaultDecoderSlots  = 2
)

var errStorePayloadTooLarge = errors.New("legacy store payload limit exceeded")

type BatchSink interface {
	Accept(context.Context, model.Batch) error
}

type Config struct {
	TenantID              int64
	ProjectID             int64
	PublicKey             string
	AllowedOrigins        []string
	DefaultService        string
	Sink                  BatchSink
	Now                   func() time.Time
	RateLimited           func() bool
	ForbiddenFixtureValue string
	DecoderSlots          int
	IngressBytes          int64
}

type IngestHandler struct {
	config        Config
	origins       map[string]struct{}
	decoderTokens chan struct{}
	admission     byteAdmission
}

type byteAdmission struct {
	mu    sync.Mutex
	used  int64
	limit int64
}

func NewIngestHandler(config Config) (*IngestHandler, error) {
	if config.TenantID == 0 || config.ProjectID == 0 || config.PublicKey == "" || config.Sink == nil {
		return nil, errors.New("tenant, project, public key, and sink are required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.DecoderSlots <= 0 {
		config.DecoderSlots = DefaultDecoderSlots
	}
	if config.IngressBytes <= 0 {
		config.IngressBytes = DefaultIngressBytes
	}
	handler := &IngestHandler{
		config: config, origins: make(map[string]struct{}),
		decoderTokens: make(chan struct{}, config.DecoderSlots),
		admission:     byteAdmission{limit: config.IngressBytes},
	}
	for _, origin := range config.AllowedOrigins {
		handler.origins[origin] = struct{}{}
	}
	return handler, nil
}

func (handler *IngestHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	if origin := request.Header.Get("Origin"); origin != "" {
		writer.Header().Add("Vary", "Origin")
		if _, allowed := handler.origins[origin]; allowed {
			writer.Header().Set("Access-Control-Allow-Origin", origin)
			writer.Header().Set("Access-Control-Expose-Headers", "Retry-After, X-Sentry-Rate-Limits, X-Eventglass-Receipt")
		} else {
			writeError(writer, http.StatusForbidden, "origin_not_allowed")
			return
		}
	}
	if request.Method == http.MethodOptions {
		handler.options(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if handler.config.RateLimited != nil && handler.config.RateLimited() {
		writer.Header().Set("Retry-After", "1")
		writer.Header().Set("X-Sentry-Rate-Limits", "1::organization:quota_exceeded")
		writeError(writer, http.StatusTooManyRequests, "rate_limited")
		return
	}
	projectID, endpoint, ok := parseIngestPath(request.URL.Path)
	if !ok || projectID != handler.config.ProjectID {
		writeError(writer, http.StatusNotFound, "not_found")
		return
	}
	release, ok := handler.acquireDecoder(request.ContentLength)
	if !ok {
		writeRateLimit(writer, "admission_limited")
		return
	}
	defer release()
	body, status, reason := decodeRequestBody(writer, request)
	if status != 0 {
		writeError(writer, status, reason)
		return
	}

	var envelope sdk.Envelope
	var err error
	if endpoint == "store" {
		body, err = decodeStorePayload(body, request.Header.Get("Content-Type"))
		if err != nil {
			if errors.Is(err, errStorePayloadTooLarge) {
				writeError(writer, http.StatusRequestEntityTooLarge, "store_payload_too_large")
				return
			}
			writeError(writer, http.StatusBadRequest, "malformed_store_payload")
			return
		}
		payload, decodeErr := sdk.DecodeObject(body, sdk.DefaultJSONLimits)
		if decodeErr != nil {
			if errors.Is(decodeErr, sdk.ErrLimitExceeded) {
				writeError(writer, http.StatusRequestEntityTooLarge, "store_payload_limit_exceeded")
				return
			}
			writeError(writer, http.StatusBadRequest, "malformed_json")
			return
		}
		item := sdk.Item{Ordinal: 0, Type: "event", Header: map[string]any{"type": "event"}, Payload: body, Value: payload}
		if sdk.HasLoneSurrogateEscape(body) {
			item.Warnings = append(item.Warnings, "lone_surrogate_replaced")
		}
		envelope = sdk.Envelope{Header: map[string]any{}, Items: []sdk.Item{item}}
	} else {
		envelope, err = sdk.ParseEnvelope(body)
		if err != nil {
			if errors.Is(err, sdk.ErrLimitExceeded) {
				writeError(writer, http.StatusRequestEntityTooLarge, "envelope_limit_exceeded")
				return
			}
			writeError(writer, http.StatusBadRequest, "malformed_envelope")
			return
		}
	}
	if !handler.authorized(request, envelope.Header, projectID) {
		writeError(writer, http.StatusUnauthorized, "invalid_key")
		return
	}
	acceptanceID, err := ingest.NewAcceptanceID()
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "random_unavailable")
		return
	}
	batch, err := ingest.NormalizeEnvelope(envelope, ingest.NormalizeOptions{
		TenantID: handler.config.TenantID, ProjectID: projectID, AcceptanceID: acceptanceID,
		ArrivalTime: handler.config.Now(), DefaultService: handler.config.DefaultService,
		ForbiddenValue: handler.config.ForbiddenFixtureValue,
	})
	if err != nil {
		if errors.Is(err, ingest.ErrLimitExceeded) {
			writeError(writer, http.StatusRequestEntityTooLarge, "canonical_limit_exceeded")
			return
		}
		writeError(writer, http.StatusBadRequest, "normalization_failed")
		return
	}
	if err := handler.config.Sink.Accept(request.Context(), batch); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "dependency_unavailable")
		return
	}
	writer.Header().Set("X-Eventglass-Receipt", acceptanceID)
	if batch.Unsupported > 0 {
		writer.Header().Set("X-Eventglass-Unsupported-Items", strconv.Itoa(batch.Unsupported))
	}
	writer.WriteHeader(http.StatusOK)
	if endpoint == "store" {
		responseID := strings.ReplaceAll(acceptanceID, "-", "")
		if len(batch.Records) > 0 && batch.Records[0].SourceEventID != nil {
			responseID = *batch.Records[0].SourceEventID
		}
		_ = json.NewEncoder(writer).Encode(map[string]string{"id": responseID})
		return
	}
	_, _ = writer.Write([]byte("{}"))
}

func (handler *IngestHandler) acquireDecoder(contentLength int64) (func(), bool) {
	select {
	case handler.decoderTokens <- struct{}{}:
	default:
		return nil, false
	}
	wireReservation := contentLength
	if wireReservation <= 0 || wireReservation > MaxWireBytes {
		wireReservation = MaxWireBytes
	}
	reservation := wireReservation + MaxDecompressedBytes
	if !handler.admission.acquire(reservation) {
		<-handler.decoderTokens
		return nil, false
	}
	return func() {
		handler.admission.release(reservation)
		<-handler.decoderTokens
	}, true
}

func (admission *byteAdmission) acquire(bytes int64) bool {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if bytes < 0 || admission.used+bytes > admission.limit {
		return false
	}
	admission.used += bytes
	return true
}

func (admission *byteAdmission) release(bytes int64) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	admission.used -= bytes
}

func (handler *IngestHandler) options(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Origin") == "" {
		writeError(writer, http.StatusBadRequest, "missing_origin")
		return
	}
	requested := strings.ToLower(request.Header.Get("Access-Control-Request-Headers"))
	for _, header := range strings.Split(requested, ",") {
		header = strings.TrimSpace(header)
		if header != "" && header != "content-type" && header != "x-sentry-auth" {
			writeError(writer, http.StatusForbidden, "cors_header_not_allowed")
			return
		}
	}
	writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Sentry-Auth")
	writer.Header().Set("Access-Control-Max-Age", "600")
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *IngestHandler) authorized(request *http.Request, header map[string]any, projectID int64) bool {
	identities := make([]string, 0, 4)
	if authHeaders := request.Header.Values("X-Sentry-Auth"); len(authHeaders) > 0 {
		for _, authHeader := range authHeaders {
			keys := authKeys(authHeader)
			if len(keys) == 0 {
				return false
			}
			identities = append(identities, keys...)
		}
	}
	if keys, supplied := request.URL.Query()["sentry_key"]; supplied {
		if len(keys) == 0 {
			return false
		}
		for _, key := range keys {
			if key == "" {
				return false
			}
			identities = append(identities, key)
		}
	}
	if rawDSN, _ := header["dsn"].(string); rawDSN != "" {
		parsed, err := url.Parse(rawDSN)
		if err != nil || parsed.User == nil || parsed.User.Username() == "" {
			return false
		}
		project, err := strconv.ParseInt(strings.Trim(parsed.Path, "/"), 10, 64)
		if err != nil || project != projectID {
			return false
		}
		identities = append(identities, parsed.User.Username())
	}
	if len(identities) == 0 {
		return false
	}
	for _, identity := range identities {
		if identity != handler.config.PublicKey {
			return false
		}
	}
	return true
}

func authKeys(header string) []string {
	if space := strings.IndexByte(header, ' '); space >= 0 {
		header = header[space+1:]
	}
	var keys []string
	for _, part := range strings.Split(header, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if found && strings.EqualFold(key, "sentry_key") {
			keys = append(keys, value)
		}
	}
	return keys
}

func parseIngestPath(path string) (int64, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "api" || (parts[2] != "envelope" && parts[2] != "store") {
		return 0, "", false
	}
	project, err := strconv.ParseInt(parts[1], 10, 64)
	return project, parts[2], err == nil
}

func decodeRequestBody(writer http.ResponseWriter, request *http.Request) ([]byte, int, string) {
	wire, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, MaxWireBytes+1))
	if err != nil || len(wire) > MaxWireBytes {
		return nil, http.StatusRequestEntityTooLarge, "wire_too_large"
	}
	var reader io.Reader = bytes.NewReader(wire)
	var closer io.Closer
	switch strings.ToLower(strings.TrimSpace(request.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		decoded, err := gzip.NewReader(reader)
		if err != nil {
			return nil, http.StatusBadRequest, "invalid_compression"
		}
		reader, closer = decoded, decoded
	case "deflate":
		decoded, err := zlib.NewReader(reader)
		if err != nil {
			return nil, http.StatusBadRequest, "invalid_compression"
		}
		reader, closer = decoded, decoded
	case "br":
		reader = brotli.NewReader(reader)
	case "zstd":
		decoded, err := zstd.NewReader(reader, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(32<<20))
		if err != nil {
			return nil, http.StatusBadRequest, "invalid_compression"
		}
		reader, closer = decoded, decoded.IOReadCloser()
	default:
		return nil, http.StatusUnsupportedMediaType, "unsupported_encoding"
	}
	if closer != nil {
		defer closer.Close()
	}
	decoded, err := io.ReadAll(io.LimitReader(reader, MaxDecompressedBytes+1))
	if err != nil {
		return nil, http.StatusBadRequest, "invalid_compression"
	}
	if len(decoded) > MaxDecompressedBytes {
		return nil, http.StatusRequestEntityTooLarge, "decompressed_too_large"
	}
	return decoded, 0, ""
}

func decodeStorePayload(body []byte, contentType string) ([]byte, error) {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if contentType == "" {
		mediaType = "application/json"
	} else if err != nil {
		return nil, err
	}
	if mediaType != "application/x-www-form-urlencoded" {
		return body, nil
	}
	values, err := url.ParseQuery(string(body))
	if err != nil || values.Get("sentry_data") == "" {
		return nil, errors.New("missing sentry_data")
	}
	compressed, err := base64.StdEncoding.DecodeString(values.Get("sentry_data"))
	if err != nil {
		return nil, err
	}
	reader, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, MaxDecompressedBytes+1))
	if len(decoded) > MaxDecompressedBytes {
		return nil, errStorePayloadTooLarge
	}
	if err != nil {
		return nil, errors.New("invalid legacy zlib payload")
	}
	return decoded, nil
}

func writeRateLimit(writer http.ResponseWriter, reason string) {
	writer.Header().Set("Retry-After", "1")
	writer.Header().Set("X-Sentry-Rate-Limits", "1::organization:quota_exceeded")
	writeError(writer, http.StatusTooManyRequests, reason)
}

func writeError(writer http.ResponseWriter, status int, reason string) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": reason})
}

type MemorySink struct {
	mu      sync.Mutex
	batches []model.Batch
	err     error
}

func (sink *MemorySink) Accept(_ context.Context, batch model.Batch) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.err != nil {
		return sink.err
	}
	sink.batches = append(sink.batches, batch)
	return nil
}

func (sink *MemorySink) Batches() []model.Batch {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]model.Batch(nil), sink.batches...)
}

func (sink *MemorySink) SetError(err error) { sink.mu.Lock(); defer sink.mu.Unlock(); sink.err = err }
