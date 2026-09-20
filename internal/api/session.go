package api

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/alerts"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

const (
	secureSessionCookie = "__Host-eventglass_session"
	developmentCookie   = "eventglass_dev_session"
	managementBodyLimit = 64 << 10
	authBodyLimit       = 8 << 10
	sessionLifetime     = 24 * time.Hour
	setupLease          = 60 * time.Second
	setupHeartbeat      = 15 * time.Second
	csrfPurpose         = "eventglass-csrf-v1"
)

type MarkerBuilder func(installationID, storageIdentity string) (body []byte, key, sha256 string, err error)
type MarkerStore func(context.Context, string, []byte, string) error

type ManagementConfig struct {
	Auth            *control.AuthOperations
	StoreMarker     MarkerStore
	Passwords       *PasswordHasher
	PublicOrigin    string
	CookieName      string
	SecureCookie    bool
	LoginBucketKey  [32]byte
	BuildMarker     MarkerBuilder
	OnSetupComplete func()
	Now             func() time.Time
	Queries         PublicQueryService
	Alerts          *control.AlertOperations
	AlertCipher     *alerts.SecretCipher
}

type ManagementHandler struct {
	config    ManagementConfig
	liveSlots chan struct{}
}

func NewManagementHandler(config ManagementConfig) (*ManagementHandler, error) {
	if config.Auth == nil || config.StoreMarker == nil || config.Passwords == nil || config.BuildMarker == nil || config.LoginBucketKey == ([32]byte{}) { // pragma: allowlist secret
		return nil, errors.New("management auth, store, password admission, marker builder, and bucket key are required")
	}
	origin, err := url.Parse(config.PublicOrigin)
	if err != nil || origin.Scheme == "" || origin.Host == "" || origin.Path != "" && origin.Path != "/" || origin.RawQuery != "" || origin.Fragment != "" {
		return nil, errors.New("public origin must contain only scheme and authority")
	}
	config.PublicOrigin = origin.Scheme + "://" + origin.Host
	if config.CookieName == "" {
		if config.SecureCookie {
			config.CookieName = secureSessionCookie
		} else {
			config.CookieName = developmentCookie
		}
	}
	if config.SecureCookie && config.CookieName != secureSessionCookie {
		return nil, errors.New("secure management cookie must use the __Host- name")
	}
	if !config.SecureCookie && (origin.Scheme != "http" || !isLoopbackHost(origin.Hostname()) || config.CookieName != developmentCookie) {
		return nil, errors.New("insecure cookie mode is restricted to explicit loopback HTTP")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &ManagementHandler{config: config, liveSlots: make(chan struct{}, 32)}, nil
}

func (handler *ManagementHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/setup", handler.getSetup)
	mux.HandleFunc("POST /v1/setup", handler.postSetup)
	mux.HandleFunc("POST /v1/sessions", handler.postSession)
	mux.HandleFunc("GET /v1/session", handler.getSession)
	mux.HandleFunc("DELETE /v1/session", handler.deleteSession)
	mux.HandleFunc("POST /v1/session/password", handler.changePassword)
	mux.HandleFunc("GET /v1/projects/{id}/keys", handler.listProjectKeys)
	mux.HandleFunc("POST /v1/projects/{id}/keys", handler.createProjectKey)
	mux.HandleFunc("DELETE /v1/projects/{id}/keys/{key_id}", handler.revokeProjectKey)
	if handler.config.Alerts != nil && handler.config.AlertCipher != nil {
		handler.registerAlertRoutes(mux)
	}
	if handler.config.Queries != nil {
		handler.registerQueryRoutes(mux)
	}
}

func (handler *ManagementHandler) getSetup(writer http.ResponseWriter, request *http.Request) {
	status, err := handler.config.Auth.SetupStatus(request.Context())
	if err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	state := generated.Complete
	if status.State == control.SetupUninitialized {
		state = generated.Required
	} else if status.State == control.SetupProvisioning {
		state = generated.InProgress
		if status.RetryAfter > 0 {
			writer.Header().Set("Retry-After", strconv.Itoa(max(1, int(status.RetryAfter.Seconds()))))
		}
	}
	handler.json(writer, http.StatusOK, generated.SetupState{State: state})
}

func (handler *ManagementHandler) postSetup(writer http.ResponseWriter, request *http.Request) {
	if !handler.originAndJSON(writer, request) {
		return
	}
	object, ok := handler.decodeObject(writer, request, authBodyLimit, "bootstrap_token", "email", "password", "tenant_name")
	if !ok {
		return
	}
	var body generated.SetupRequest
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	bootstrap, err := hex.DecodeString(body.BootstrapToken)
	if err != nil || len(bootstrap) != 32 {
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
		return
	}
	email, err := control.NormalizeEmail(body.Email)
	if err != nil || strings.TrimSpace(body.TenantName) == "" || len(strings.TrimSpace(body.TenantName)) > 128 {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	passwordPHC, err := handler.config.Passwords.Hash(body.Password)
	if err != nil {
		handler.passwordError(writer, request, err)
		return
	}
	descriptor, err := handler.config.Auth.SetupDescriptor(request.Context())
	if err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	marker, markerKey, markerSHA, err := handler.config.BuildMarker(descriptor.InstallationID, descriptor.StorageIdentity)
	if err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	canonical, _ := json.Marshal(struct {
		Email      string `json:"email"`
		Password   string `json:"password"`
		TenantName string `json:"tenant_name"`
	}{Email: email, Password: body.Password, TenantName: strings.TrimSpace(body.TenantName)})
	fingerprintMAC := hmac.New(sha256.New, bootstrap)
	fingerprintMAC.Write(canonical)
	var fingerprint [32]byte
	copy(fingerprint[:], fingerprintMAC.Sum(nil))
	bootstrapHash := sha256.Sum256(bootstrap)
	attempt, owner := newUUID(), newUUID()
	reservation, err := handler.config.Auth.ReserveSetup(request.Context(), bootstrapHash, fingerprint, attempt, owner, markerKey, markerSHA, setupLease)
	if err != nil {
		handler.setupError(writer, request, err)
		return
	}
	if err := handler.storeSetupMarker(request.Context(), reservation.Authority, markerKey, marker, markerSHA); err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	secret, tokenHash, csrfToken, csrfHash, err := newSessionSecrets()
	if err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	expires := handler.config.Now().Add(sessionLifetime)
	principal, err := handler.config.Auth.FinalizeSetup(request.Context(), control.SetupFinalize{
		Authority: reservation.Authority, EmailNormalized: email, PasswordPHC: passwordPHC, TenantName: strings.TrimSpace(body.TenantName),
		SessionTokenHash: tokenHash, CSRFHash: csrfHash, SessionExpiresAt: expires, RequestID: requestID(request), AuditID: newUUID(),
	})
	if err != nil {
		handler.setupError(writer, request, err)
		return
	}
	if handler.config.OnSetupComplete != nil {
		handler.config.OnSetupComplete()
	}
	handler.setCookie(writer, secret, expires)
	handler.json(writer, http.StatusCreated, sessionDTO(principal, csrfToken))
}

func (handler *ManagementHandler) storeSetupMarker(ctx context.Context, authority control.SetupAuthority, key string, body []byte, checksum string) error {
	storeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stored := make(chan error, 1)
	go func() { stored <- handler.config.StoreMarker(storeContext, key, body, checksum) }()
	ticker := time.NewTicker(setupHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case err := <-stored:
			return err
		case <-ticker.C:
			if err := handler.config.Auth.HeartbeatSetup(ctx, authority, setupLease); err != nil {
				cancel()
				return errors.Join(err, <-stored)
			}
		case <-ctx.Done():
			cancel()
			return errors.Join(ctx.Err(), <-stored)
		}
	}
}

func (handler *ManagementHandler) postSession(writer http.ResponseWriter, request *http.Request) {
	if !handler.originAndJSON(writer, request) {
		return
	}
	object, ok := handler.decodeObject(writer, request, authBodyLimit, "email", "password")
	if !ok {
		return
	}
	var body generated.LoginRequest
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	email, err := control.NormalizeEmail(body.Email)
	if err != nil {
		email = strings.ToLower(strings.TrimSpace(body.Email))
	}
	ip := directClientIP(request.RemoteAddr)
	buckets := [][32]byte{handler.loginBucket("account", ip+"\x00"+email), handler.loginBucket("ip", ip)}
	if err := handler.config.Auth.ConsumeLoginLimits(request.Context(), buckets, []int{5, 20}); err != nil {
		if errors.Is(err, control.ErrLoginLimited) {
			writer.Header().Set("Retry-After", "60")
			handler.error(writer, request, http.StatusTooManyRequests, "admission_limited", true)
		} else {
			handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		}
		return
	}
	credential, found, err := handler.config.Auth.LoadLoginCredential(request.Context(), email)
	if err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	valid := false
	if found {
		valid, err = handler.config.Passwords.Verify(body.Password, credential.PasswordPHC)
	} else {
		err = handler.config.Passwords.VerifyUnknown(body.Password)
	}
	if err != nil {
		handler.passwordError(writer, request, err)
		return
	}
	if !found || !credential.Active || !valid {
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
		return
	}
	secret, tokenHash, csrfToken, csrfHash, err := newSessionSecrets()
	if err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	expires := handler.config.Now().Add(sessionLifetime)
	principal, err := handler.config.Auth.CreateSession(request.Context(), credential, tokenHash, csrfHash, expires)
	if err != nil {
		handler.authError(writer, request, err)
		return
	}
	handler.setCookie(writer, secret, expires)
	handler.json(writer, http.StatusOK, sessionDTO(principal, csrfToken))
}

func (handler *ManagementHandler) getSession(writer http.ResponseWriter, request *http.Request) {
	principal, _, csrfToken, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	handler.json(writer, http.StatusOK, sessionDTO(principal, csrfToken))
}

func (handler *ManagementHandler) deleteSession(writer http.ResponseWriter, request *http.Request) {
	_, tokenHash, _, ok := handler.authenticate(writer, request, true)
	if !ok {
		return
	}
	if err := handler.config.Auth.RevokeSession(request.Context(), tokenHash); err != nil {
		handler.authError(writer, request, err)
		return
	}
	handler.clearCookie(writer)
	noStore(writer)
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *ManagementHandler) changePassword(writer http.ResponseWriter, request *http.Request) {
	principal, _, _, ok := handler.authenticate(writer, request, true)
	if !ok {
		return
	}
	if strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0])) != "application/json" {
		handler.error(writer, request, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(writer, request, authBodyLimit, "current_password", "new_password")
	if !ok {
		return
	}
	var body generated.CredentialChangeRequest
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	credential, err := handler.config.Auth.LoadUserCredential(request.Context(), principal.UserID)
	if err != nil {
		handler.authError(writer, request, err)
		return
	}
	valid, err := handler.config.Passwords.Verify(body.CurrentPassword, credential.PasswordPHC)
	if err != nil {
		handler.passwordError(writer, request, err)
		return
	}
	if !valid {
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
		return
	}
	newPHC, err := handler.config.Passwords.Hash(body.NewPassword)
	if err != nil {
		handler.passwordError(writer, request, err)
		return
	}
	if err := handler.config.Auth.ChangePassword(request.Context(), principal.UserID, credential.CredentialRevision, newPHC); err != nil {
		handler.authError(writer, request, err)
		return
	}
	handler.clearCookie(writer)
	noStore(writer)
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *ManagementHandler) listProjectKeys(writer http.ResponseWriter, request *http.Request) {
	principal, _, _, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	tenantID, projectID, ok := handler.adminProjectRequest(writer, request, principal, request.URL.Query().Get("tenant_id"))
	if !ok {
		return
	}
	keys, err := handler.config.Auth.ListProjectKeys(request.Context(), tenantID, projectID)
	if err != nil {
		handler.authError(writer, request, err)
		return
	}
	items := make([]generated.Key, 0, len(keys))
	for _, key := range keys {
		items = append(items, generated.Key{KeyId: uuid.MustParse(key.KeyID), Label: key.Label, KeyPrefix: key.KeyPrefix, State: generated.KeyState(key.State), Revision: strconv.FormatInt(key.Revision, 10), CreatedAt: key.CreatedAt})
	}
	handler.json(writer, http.StatusOK, generated.KeyList{Items: items})
}

func (handler *ManagementHandler) createProjectKey(writer http.ResponseWriter, request *http.Request) {
	principal, _, _, ok := handler.authenticate(writer, request, true)
	if !ok {
		return
	}
	if strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0])) != "application/json" {
		handler.error(writer, request, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(writer, request, managementBodyLimit, "tenant_id", "label")
	if !ok {
		return
	}
	var body generated.KeyCreate
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, projectID, ok := handler.adminProjectRequest(writer, request, principal, string(body.TenantId))
	if !ok {
		return
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	publicKey := hex.EncodeToString(secretBytes)
	keyHash := sha256.Sum256([]byte(publicKey))
	metadata, err := handler.config.Auth.CreateProjectKey(request.Context(), control.CreateProjectKeyCommand{
		TenantID: tenantID, ProjectID: projectID, KeyID: newUUID(), Label: body.Label, KeyPrefix: publicKey[:8], KeyHash: keyHash,
		ActorUserID: principal.UserID, RequestID: requestID(request), AuditID: newUUID(),
	})
	if err != nil {
		handler.authError(writer, request, err)
		return
	}
	dsnURL, _ := url.Parse(handler.config.PublicOrigin)
	dsnURL.User = url.User(publicKey)
	dsnURL.Path = "/" + strconv.FormatInt(projectID, 10)
	handler.json(writer, http.StatusCreated, generated.CreatedKey{
		KeyId: uuid.MustParse(metadata.KeyID), Label: metadata.Label, KeyPrefix: metadata.KeyPrefix, State: generated.CreatedKeyState(metadata.State),
		Revision: strconv.FormatInt(metadata.Revision, 10), CreatedAt: metadata.CreatedAt, PublicKey: publicKey, Dsn: dsnURL.String(),
	})
}

func (handler *ManagementHandler) revokeProjectKey(writer http.ResponseWriter, request *http.Request) {
	principal, _, _, ok := handler.authenticate(writer, request, true)
	if !ok {
		return
	}
	tenantID, projectID, ok := handler.adminProjectRequest(writer, request, principal, request.URL.Query().Get("tenant_id"))
	if !ok {
		return
	}
	keyID := strings.ToLower(request.PathValue("key_id"))
	if parsed, err := uuid.Parse(keyID); err != nil || parsed.String() != keyID {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	revision, err := strconv.ParseInt(request.URL.Query().Get("revision"), 10, 64)
	if err != nil || revision <= 0 {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	if err := handler.config.Auth.RevokeProjectKey(request.Context(), control.RevokeProjectKeyCommand{
		TenantID: tenantID, ProjectID: projectID, KeyID: keyID, ExpectedRevision: revision,
		ActorUserID: principal.UserID, RequestID: requestID(request), AuditID: newUUID(),
	}); err != nil {
		handler.authError(writer, request, err)
		return
	}
	noStore(writer)
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *ManagementHandler) adminProjectRequest(writer http.ResponseWriter, request *http.Request, principal control.SessionPrincipal, tenantValue string) (int64, int64, bool) {
	tenantID, err := strconv.ParseInt(tenantValue, 10, 64)
	projectID, projectErr := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil || projectErr != nil || tenantID <= 0 || projectID <= 0 {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return 0, 0, false
	}
	for _, tenant := range principal.Tenants {
		if tenant.TenantID == tenantID && tenant.Role == "admin" {
			return tenantID, projectID, true
		}
	}
	handler.error(writer, request, http.StatusForbidden, "forbidden", false)
	return 0, 0, false
}

func (handler *ManagementHandler) authenticate(writer http.ResponseWriter, request *http.Request, mutation bool) (control.SessionPrincipal, [32]byte, string, bool) {
	cookie, err := request.Cookie(handler.config.CookieName)
	if err != nil {
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
		return control.SessionPrincipal{}, [32]byte{}, "", false
	}
	secret, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil || len(secret) != 32 {
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
		return control.SessionPrincipal{}, [32]byte{}, "", false
	}
	tokenHash := sha256.Sum256(secret)
	principal, err := handler.config.Auth.AuthenticateSession(request.Context(), tokenHash)
	if err != nil {
		handler.authError(writer, request, err)
		return control.SessionPrincipal{}, [32]byte{}, "", false
	}
	csrfToken, csrfHash := csrfForSecret(secret)
	if subtle.ConstantTimeCompare(csrfHash[:], principal.CSRFHash[:]) != 1 {
		handler.error(writer, request, http.StatusUnauthorized, "unauthenticated", false)
		return control.SessionPrincipal{}, [32]byte{}, "", false
	}
	if mutation {
		if request.Header.Get("Origin") != handler.config.PublicOrigin || subtle.ConstantTimeCompare([]byte(request.Header.Get("X-CSRF-Token")), []byte(csrfToken)) != 1 {
			handler.error(writer, request, http.StatusForbidden, "csrf_failed", false)
			return control.SessionPrincipal{}, [32]byte{}, "", false
		}
	}
	return principal, tokenHash, csrfToken, true
}
