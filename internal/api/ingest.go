package api

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/klauspost/compress/zstd"
)

const (
	MaxWireBytes         = 20 << 20
	MaxDecompressedBytes = 20 << 20
	DefaultIngressBytes  = 64 << 20
	DefaultDecoderSlots  = 2
	DefaultWorkingBytes  = 256 << 20
)

var errStorePayloadTooLarge = errors.New("legacy store payload limit exceeded")

type Acceptor interface {
	Accept(context.Context, ingest.Command) (control.ReceiptResult, error)
}

type Config struct {
	TenantID              int64
	ProjectID             int64
	PublicKey             string
	AllowedOrigins        []string
	DefaultService        string
	Sink                  Acceptor
	Now                   func() time.Time
	RateLimited           func() bool
	ForbiddenFixtureValue string
	DecoderSlots          int
	IngressBytes          int64
	ProjectRevision       int64
	KeyRevision           int64
	ScrubRevision         int
	TenantRevision        int64
	ConfigRevision        int64
	ResolveProject        func(context.Context, int64) (int64, error)
	ResolveAuthorization  func(context.Context, int64, int64, [32]byte) (control.ProjectAuthorization, error)
	ResolveOrigins        func(context.Context, int64, int64) ([]string, error)
	// App injects process-wide budgets when multiple handlers/roles coexist.
	IngressBudget *resource.Budget
	WorkingBudget *resource.Budget
}

type IngestHandler struct {
	config        Config
	origins       map[string]struct{}
	decoderTokens chan struct{}
	admission     *resource.Budget
}

func NewIngestHandler(config Config) (*IngestHandler, error) {
	dynamicProject := config.ResolveProject != nil
	if config.Sink == nil || (!dynamicProject && (config.TenantID <= 0 || config.ProjectID <= 0)) || (dynamicProject && (config.TenantID != 0 || config.ProjectID != 0)) || (config.PublicKey == "" && config.ResolveAuthorization == nil) {
		return nil, errors.New("tenant, project, public key, and sink are required")
	}
	if (config.ResolveAuthorization == nil) != (config.ResolveOrigins == nil) {
		return nil, errors.New("dynamic authorization and origin resolvers must be configured together")
	}
	if dynamicProject && config.ResolveAuthorization == nil {
		return nil, errors.New("dynamic project routing requires dynamic authorization")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.ProjectRevision == 0 {
		config.ProjectRevision = 1
	}
	if config.KeyRevision == 0 {
		config.KeyRevision = 1
	}
	if config.ScrubRevision == 0 {
		config.ScrubRevision = 1
	}
	if config.TenantRevision == 0 {
		config.TenantRevision = 1
	}
	if config.ConfigRevision == 0 {
		config.ConfigRevision = 1
	}
	if config.ProjectRevision < 1 || config.KeyRevision < 1 || config.ScrubRevision < 1 || config.TenantRevision < 1 || config.ConfigRevision < 1 {
		return nil, errors.New("authorization revisions must be positive")
	}
	if config.DecoderSlots <= 0 {
		config.DecoderSlots = DefaultDecoderSlots
	}
	if config.IngressBytes <= 0 {
		config.IngressBytes = DefaultIngressBytes
	}
	if config.IngressBudget == nil {
		config.IngressBudget = resource.NewBudget(config.IngressBytes)
	}
	if config.WorkingBudget == nil {
		config.WorkingBudget = resource.NewBudget(DefaultWorkingBytes)
	}
	handler := &IngestHandler{
		config: config, origins: make(map[string]struct{}),
		decoderTokens: make(chan struct{}, config.DecoderSlots),
		admission:     config.IngressBudget,
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
		if handler.config.ResolveAuthorization == nil {
			if _, allowed := handler.origins[origin]; !allowed {
				writeError(writer, http.StatusForbidden, "origin_not_allowed")
				return
			}
			setCORSResponse(writer, origin)
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
	if !ok || (handler.config.ProjectID != 0 && projectID != handler.config.ProjectID) {
		writeError(writer, http.StatusNotFound, "not_found")
		return
	}
	tenantID := handler.config.TenantID
	if handler.config.ResolveProject != nil {
		var err error
		tenantID, err = handler.config.ResolveProject(request.Context(), projectID)
		if err != nil {
			handler.writeAuthorizationError(writer, err)
			return
		}
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
	// Reserve before allocating a parsed object graph. This conservative policy
	// accounts for maps/strings/projections and canonical/codec scratch; it is
	// not a claim that Go heap or RSS is bounded by the byte counter.
	working, err := handler.config.WorkingBudget.Acquire(int64(len(body))*32 + 2*ingest.MaxCanonicalBytes + (8 << 20))
	if err != nil {
		writeRateLimit(writer, "working_memory_limited")
		return
	}
	defer working.Release()

	var envelope sdk.Envelope
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
		// Legacy form decompression can expand a second time.
		additional, reserveErr := handler.config.WorkingBudget.Acquire(int64(len(body))*32 + 1)
		if reserveErr != nil {
			writeRateLimit(writer, "working_memory_limited")
			return
		}
		defer additional.Release()
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
	identity, ok := handler.ingestIdentity(request, envelope.Header, projectID)
	if !ok {
		writeError(writer, http.StatusUnauthorized, "invalid_key")
		return
	}
	authorization := ingest.Authorization{
		TenantID: tenantID, ProjectID: projectID, KeyHash: sha256.Sum256([]byte(identity)),
		TenantRevision: handler.config.TenantRevision, ProjectRevision: handler.config.ProjectRevision,
		KeyRevision: handler.config.KeyRevision, ScrubRevision: handler.config.ScrubRevision, ConfigRevision: handler.config.ConfigRevision,
	}
	defaultService := handler.config.DefaultService
	if handler.config.ResolveAuthorization != nil {
		resolved, err := handler.config.ResolveAuthorization(request.Context(), tenantID, projectID, authorization.KeyHash)
		if err != nil {
			handler.writeAuthorizationError(writer, err)
			return
		}
		authorization.TenantRevision = resolved.Snapshot.TenantRevision
		authorization.ProjectRevision = resolved.Snapshot.ProjectRevision
		authorization.KeyRevision = resolved.Snapshot.KeyRevision
		authorization.ScrubRevision = resolved.Snapshot.ScrubRevision
		authorization.ConfigRevision = resolved.Snapshot.ConfigRevision
		defaultService = resolved.DefaultService
		if origin := request.Header.Get("Origin"); origin != "" && !containsString(resolved.AllowedOrigins, origin) {
			writeError(writer, http.StatusForbidden, "origin_not_allowed")
			return
		} else if origin != "" {
			setCORSResponse(writer, origin)
		}
	} else if identity != handler.config.PublicKey {
		writeError(writer, http.StatusUnauthorized, "invalid_key")
		return
	}
	acceptanceID, err := ingest.NewAcceptanceID()
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "random_unavailable")
		return
	}
	batch, err := ingest.NormalizeEnvelope(envelope, ingest.NormalizeOptions{
		TenantID: tenantID, ProjectID: projectID, AcceptanceID: acceptanceID,
		ArrivalTime: handler.config.Now(), DefaultService: defaultService,
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
	command := ingest.Command{Request: batch, Authorization: authorization}
	receipt, err := handler.config.Sink.Accept(request.Context(), command)
	if err != nil {
		if errors.Is(err, ingest.ErrProjectDisabled) {
			writeError(writer, http.StatusForbidden, "project_disabled")
			return
		}
		if errors.Is(err, ingest.ErrKeyRevoked) {
			writeError(writer, http.StatusUnauthorized, "invalid_key")
			return
		}
		if errors.Is(err, ingest.ErrAdmissionLimited) || errors.Is(err, ingest.ErrDraining) {
			writeRateLimit(writer, "admission_limited")
			return
		}
		writeError(writer, http.StatusServiceUnavailable, "dependency_unavailable")
		return
	}
	if receipt.AcceptanceID != acceptanceID {
		writeError(writer, http.StatusServiceUnavailable, "receipt_mismatch")
		return
	}
	writer.Header().Set("X-Eventglass-Receipt", receipt.AcceptanceID)
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
	permit, err := handler.admission.Acquire(reservation)
	if err != nil {
		<-handler.decoderTokens
		return nil, false
	}
	return func() {
		permit.Release()
		<-handler.decoderTokens
	}, true
}

func (handler *IngestHandler) options(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Origin") == "" {
		writeError(writer, http.StatusBadRequest, "missing_origin")
		return
	}
	projectID, _, ok := parseIngestPath(request.URL.Path)
	if !ok || (handler.config.ProjectID != 0 && projectID != handler.config.ProjectID) {
		writeError(writer, http.StatusNotFound, "not_found")
		return
	}
	tenantID := handler.config.TenantID
	if handler.config.ResolveProject != nil {
		var err error
		tenantID, err = handler.config.ResolveProject(request.Context(), projectID)
		if err != nil {
			handler.writeAuthorizationError(writer, err)
			return
		}
	}
	origin := request.Header.Get("Origin")
	if handler.config.ResolveOrigins != nil {
		origins, err := handler.config.ResolveOrigins(request.Context(), tenantID, projectID)
		if err != nil {
			handler.writeAuthorizationError(writer, err)
			return
		}
		if !containsString(origins, origin) {
			writeError(writer, http.StatusForbidden, "origin_not_allowed")
			return
		}
		setCORSResponse(writer, origin)
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

func (handler *IngestHandler) ingestIdentity(request *http.Request, header map[string]any, projectID int64) (string, bool) {
	identities := make([]string, 0, 4)
	if authHeaders := request.Header.Values("X-Sentry-Auth"); len(authHeaders) > 0 {
		for _, authHeader := range authHeaders {
			keys := authKeys(authHeader)
			if len(keys) == 0 {
				return "", false
			}
			identities = append(identities, keys...)
		}
	}
	if keys, supplied := request.URL.Query()["sentry_key"]; supplied {
		if len(keys) == 0 {
			return "", false
		}
		for _, key := range keys {
			if key == "" {
				return "", false
			}
			identities = append(identities, key)
		}
	}
	if rawDSN, _ := header["dsn"].(string); rawDSN != "" {
		parsed, err := url.Parse(rawDSN)
		if err != nil || parsed.User == nil || parsed.User.Username() == "" {
			return "", false
		}
		project, err := strconv.ParseInt(strings.Trim(parsed.Path, "/"), 10, 64)
		if err != nil || project != projectID {
			return "", false
		}
		identities = append(identities, parsed.User.Username())
	}
	if len(identities) == 0 {
		return "", false
	}
	identity := identities[0]
	for _, candidate := range identities[1:] {
		if candidate != identity {
			return "", false
		}
	}
	return identity, true
}

func (handler *IngestHandler) writeAuthorizationError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, control.ErrTenantDisabled), errors.Is(err, control.ErrProjectDisabled):
		writeError(writer, http.StatusForbidden, "project_disabled")
	case errors.Is(err, control.ErrKeyRevoked):
		writeError(writer, http.StatusUnauthorized, "invalid_key")
	default:
		writeError(writer, http.StatusServiceUnavailable, "dependency_unavailable")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func setCORSResponse(writer http.ResponseWriter, origin string) {
	writer.Header().Set("Access-Control-Allow-Origin", origin)
	writer.Header().Set("Access-Control-Expose-Headers", "Retry-After, X-Sentry-Rate-Limits, X-Eventglass-Receipt")
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
