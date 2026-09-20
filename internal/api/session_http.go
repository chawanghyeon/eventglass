package api

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/google/uuid"
)

func (handler *ManagementHandler) originAndJSON(writer http.ResponseWriter, request *http.Request) bool {
	if request.Header.Get("Origin") != handler.config.PublicOrigin {
		handler.error(writer, request, http.StatusForbidden, "csrf_failed", false)
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0]))
	if contentType != "application/json" {
		handler.error(writer, request, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return false
	}
	return true
}

func (handler *ManagementHandler) decodeObject(writer http.ResponseWriter, request *http.Request, limit int64, allowed ...string) (map[string]any, bool) {
	request.Body = http.MaxBytesReader(writer, request.Body, limit)
	data, err := io.ReadAll(request.Body)
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) || int64(len(data)) > limit {
		handler.error(writer, request, http.StatusRequestEntityTooLarge, "payload_too_large", false)
		return nil, false
	}
	if err != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return nil, false
	}
	object, err := sdk.DecodeObject(data, sdk.JSONLimits{MaxDepth: 16, MaxNodes: 512})
	if err != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return nil, false
	}
	allowedSet := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = true
	}
	for key := range object {
		if !allowedSet[key] {
			handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
			return nil, false
		}
	}
	return object, true
}

func decodeKnownObject(object map[string]any, destination any) bool {
	data, err := json.Marshal(object)
	return err == nil && json.Unmarshal(data, destination) == nil
}

func newSessionSecrets() (string, [32]byte, string, [32]byte, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", [32]byte{}, "", [32]byte{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(secret)
	tokenHash := sha256.Sum256(secret)
	csrfToken, csrfHash := csrfForSecret(secret)
	return encoded, tokenHash, csrfToken, csrfHash, nil
}

func csrfForSecret(secret []byte) (string, [32]byte) {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(csrfPurpose))
	token := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return token, sha256.Sum256([]byte(token))
}

func sessionDTO(principal control.SessionPrincipal, csrfToken string) generated.Session {
	tenants := make([]generated.SessionTenant, 0, len(principal.Tenants))
	for _, tenant := range principal.Tenants {
		grants := make([]generated.ProjectGrant, 0, len(tenant.ProjectGrants))
		for _, grant := range tenant.ProjectGrants {
			grants = append(grants, generated.ProjectGrant{ProjectId: strconv.FormatInt(grant.ProjectID, 10), Role: generated.ProjectGrantRole(grant.Role)})
		}
		tenants = append(tenants, generated.SessionTenant{TenantId: strconv.FormatInt(tenant.TenantID, 10), Name: tenant.Name, Role: generated.SessionTenantRole(tenant.Role), ProjectGrants: grants})
	}
	return generated.Session{UserId: strconv.FormatInt(principal.UserID, 10), Email: principal.Email, Tenants: tenants, ExpiresAt: principal.ExpiresAt, CsrfToken: csrfToken, IsInstallationAdmin: principal.InstallationAdmin}
}

func (handler *ManagementHandler) setCookie(writer http.ResponseWriter, value string, expires time.Time) {
	http.SetCookie(writer, &http.Cookie{Name: handler.config.CookieName, Value: value, Path: "/", Secure: handler.config.SecureCookie, HttpOnly: true, SameSite: http.SameSiteLaxMode, Expires: expires, MaxAge: int(sessionLifetime.Seconds())})
}

func (handler *ManagementHandler) clearCookie(writer http.ResponseWriter) {
	http.SetCookie(writer, &http.Cookie{Name: handler.config.CookieName, Path: "/", Secure: handler.config.SecureCookie, HttpOnly: true, SameSite: http.SameSiteLaxMode, Expires: time.Unix(1, 0), MaxAge: -1})
}

func (handler *ManagementHandler) loginBucket(kind, value string) [32]byte {
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	mac.Write([]byte(kind))
	mac.Write([]byte{0})
	mac.Write([]byte(value))
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func directClientIP(remoteAddress string) string {
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		return remoteAddress
	}
	return host
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newUUID() string { return uuid.NewString() }

func requestID(request *http.Request) string {
	value := strings.ToLower(request.Header.Get("X-Request-ID"))
	parsed, err := uuid.Parse(value)
	if err == nil && parsed.String() == value {
		return value
	}
	return newUUID()
}

func noStore(writer http.ResponseWriter) { writer.Header().Set("Cache-Control", "no-store") }

func (handler *ManagementHandler) json(writer http.ResponseWriter, status int, value any) {
	noStore(writer)
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func (handler *ManagementHandler) error(writer http.ResponseWriter, request *http.Request, status int, code string, retryable bool) {
	message := strings.ReplaceAll(code, "_", " ")
	handler.json(writer, status, generated.Error{Code: code, Message: message, Retryable: retryable, RequestId: uuid.MustParse(requestID(request))})
}

func (handler *ManagementHandler) passwordError(writer http.ResponseWriter, request *http.Request, err error) {
	if errors.Is(err, errInvalidPassword) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "budget") || strings.Contains(err.Error(), "limited") {
		writer.Header().Set("Retry-After", "1")
		handler.error(writer, request, http.StatusTooManyRequests, "admission_limited", true)
		return
	}
	handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
}

func (handler *ManagementHandler) setupError(writer http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, control.ErrSetupComplete):
		handler.error(writer, request, http.StatusConflict, "setup_complete", false)
	case errors.Is(err, control.ErrSetupInProgress):
		writer.Header().Set("Retry-After", "60")
		handler.error(writer, request, http.StatusConflict, "setup_in_progress", true)
	case errors.Is(err, control.ErrSetupConflict):
		handler.error(writer, request, http.StatusConflict, "revision_conflict", false)
	case errors.Is(err, control.ErrInvalidBootstrap):
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
	default:
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
	}
}

func (handler *ManagementHandler) authError(writer http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, control.ErrUnauthenticated):
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
	case errors.Is(err, control.ErrForbidden):
		handler.error(writer, request, http.StatusForbidden, "forbidden", false)
	case errors.Is(err, control.ErrRevisionConflict), errors.Is(err, control.ErrIssueRevisionStale):
		handler.error(writer, request, http.StatusConflict, "revision_conflict", false)
	default:
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
	}
}
