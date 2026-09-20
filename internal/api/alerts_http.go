package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/alerts"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

func (handler *ManagementHandler) registerAlertRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/destinations", handler.listDestinations)
	mux.HandleFunc("POST /v1/destinations", handler.createDestination)
	mux.HandleFunc("PATCH /v1/destinations/{id}", handler.updateDestination)
	mux.HandleFunc("GET /v1/alerts", handler.listAlerts)
	mux.HandleFunc("POST /v1/alerts", handler.createAlert)
	mux.HandleFunc("PATCH /v1/alerts/{id}", handler.updateAlert)
}

func (handler *ManagementHandler) listDestinations(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, ok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	if !ok {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	items, admin, err := handler.config.Alerts.ListDestinations(r.Context(), tenantID, principal.UserID)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.DestinationList{Items: make([]generated.Destination, 0, len(items))}
	for _, value := range items {
		dto := generated.Destination{DestinationId: uuid.MustParse(value.DestinationID), Name: value.Name, Revision: strconv.FormatInt(value.Revision, 10), Enabled: value.Enabled, HasSecret: value.HasSecret}
		if admin {
			dto.Url = &value.URL
		}
		result.Items = append(result.Items, dto)
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) createDestination(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	if !jsonContentType(r) {
		handler.error(w, r, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(w, r, managementBodyLimit, "tenant_id", "name", "url", "secret")
	if !ok {
		return
	}
	var body generated.DestinationCreate
	if !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, valid := parsePositiveID(string(body.TenantId))
	if !valid || strings.TrimSpace(body.Name) == "" || len(body.Name) > 128 || alerts.ValidateDestinationURL(body.Url) != nil || body.Secret != nil && len(*body.Secret) > 4096 { // pragma: allowlist secret
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	var encrypted []byte
	var keyID string
	if body.Secret != nil && *body.Secret != "" { // pragma: allowlist secret
		var err error
		encrypted, err = handler.config.AlertCipher.Encrypt(*body.Secret)
		if err != nil {
			handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
			return
		}
		keyID = handler.config.AlertCipher.KeyID()
	}
	value, err := handler.config.Alerts.CreateDestination(r.Context(), control.CreateDestinationCommand{TenantID: tenantID, ActorUserID: principal.UserID, DestinationID: newUUID(), Name: body.Name, URL: body.Url, SecretCiphertext: encrypted, EncryptionKeyID: keyID, RequestID: requestID(r), AuditID: newUUID()})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusCreated, destinationDTO(value, true))
}

func (handler *ManagementHandler) updateDestination(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	if !jsonContentType(r) {
		handler.error(w, r, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(w, r, managementBodyLimit, "tenant_id", "revision", "name", "url", "secret", "enabled")
	if !ok {
		return
	}
	var body generated.DestinationPatch
	if !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	if len(object) == 2 {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, valid := parsePositiveID(string(body.TenantId))
	revision, revOK := parsePositiveID(string(body.Revision))
	destinationID, uuidOK := canonicalUUID(r.PathValue("id"))
	if !valid || !revOK || !uuidOK || body.Name != nil && (strings.TrimSpace(*body.Name) == "" || len(*body.Name) > 128) || body.Url != nil && alerts.ValidateDestinationURL(*body.Url) != nil {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	command := control.UpdateDestinationCommand{TenantID: tenantID, ActorUserID: principal.UserID, ExpectedRevision: revision, DestinationID: destinationID, Name: body.Name, URL: body.Url, Enabled: body.Enabled, RequestID: requestID(r), AuditID: newUUID()}
	if raw, exists := object["secret"]; exists {
		command.SecretSet = true // pragma: allowlist secret
		if raw != nil {
			secret, ok := raw.(string)
			if !ok {
				handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
				return
			}
			encrypted, err := handler.config.AlertCipher.Encrypt(secret)
			if err != nil {
				handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
				return
			}
			command.SecretCiphertext = encrypted // pragma: allowlist secret
			if len(encrypted) > 0 {
				command.EncryptionKeyID = handler.config.AlertCipher.KeyID()
			}
		}
	}
	value, err := handler.config.Alerts.UpdateDestination(r.Context(), command)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, destinationDTO(value, true))
}

func (handler *ManagementHandler) listAlerts(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	projectID, pok := parsePositiveID(r.URL.Query().Get("project_id"))
	if !tok || !pok {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	limit, ok := parseAlertListLimit(r.URL.Query().Get("limit"))
	if !ok {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	after, err := handler.decodeAlertCursor(r.URL.Query().Get("cursor"), tenantID, projectID, principal.UserID)
	if err != nil {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	items, err := handler.config.Alerts.ListAlertPage(r.Context(), tenantID, projectID, principal.UserID, limit, after)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.RuleList{Items: make([]generated.Rule, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		result.NextCursor, err = handler.encodeAlertCursor(tenantID, projectID, principal.UserID, items[len(items)-1].AlertID)
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, value := range items {
		dto, err := ruleDTO(value)
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
		result.Items = append(result.Items, dto)
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) createAlert(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	if !jsonContentType(r) {
		handler.error(w, r, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(w, r, managementBodyLimit, "tenant_id", "project_id", "name", "kind", "destination_id", "cooldown_seconds", "rule")
	if !ok {
		return
	}
	var body generated.RuleCreate
	if !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	projectID, pok := parsePositiveID(string(body.ProjectId))
	raw, _ := json.Marshal(body.Rule)
	validated, err := alerts.ValidateRule(alerts.Kind(body.Kind), raw)
	if !tok || !pok || strings.TrimSpace(body.Name) == "" || len(body.Name) > 128 || body.DestinationId == uuid.Nil || body.CooldownSeconds < 0 || body.CooldownSeconds > 86400 || err != nil {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	value, err := handler.config.Alerts.CreateAlert(r.Context(), control.CreateAlertCommand{TenantID: tenantID, ProjectID: projectID, ActorUserID: principal.UserID, AlertID: newUUID(), Name: body.Name, DestinationID: body.DestinationId.String(), Kind: string(validated.Kind), RuleBytes: validated.Bytes, RuleSHA256: validated.SHA256, CooldownSeconds: body.CooldownSeconds, RequestID: requestID(r), AuditID: newUUID()})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	dto, dtoErr := ruleDTO(value)
	if dtoErr != nil {
		handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	handler.json(w, http.StatusCreated, dto)
}

func (handler *ManagementHandler) updateAlert(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	if !jsonContentType(r) {
		handler.error(w, r, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(w, r, managementBodyLimit, "tenant_id", "project_id", "revision", "name", "enabled", "destination_id", "cooldown_seconds", "rule")
	if !ok {
		return
	}
	var body generated.RulePatch
	if !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	if len(object) == 3 {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	projectID, pok := parsePositiveID(string(body.ProjectId))
	revision, rok := parsePositiveID(string(body.Revision))
	alertID, uok := canonicalUUID(r.PathValue("id"))
	if !tok || !pok || !rok || !uok || body.Name != nil && (strings.TrimSpace(*body.Name) == "" || len(*body.Name) > 128) || body.DestinationId != nil && *body.DestinationId == uuid.Nil || body.CooldownSeconds != nil && (*body.CooldownSeconds < 0 || *body.CooldownSeconds > 86400) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	current, err := handler.config.Alerts.GetAlert(r.Context(), tenantID, projectID, principal.UserID, alertID)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	kind := current.Kind
	command := control.UpdateAlertCommand{TenantID: tenantID, ProjectID: projectID, ActorUserID: principal.UserID, ExpectedRevision: revision, AlertID: alertID, Name: body.Name, Enabled: body.Enabled, CooldownSeconds: body.CooldownSeconds, RequestID: requestID(r), AuditID: newUUID()}
	if body.DestinationId != nil {
		value := body.DestinationId.String()
		command.DestinationID = &value
	}
	if body.Rule != nil {
		raw, _ := json.Marshal(body.Rule)
		validated, validationErr := alerts.ValidateRule(alerts.Kind(kind), raw)
		if validationErr != nil {
			handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
			return
		}
		command.RuleBytes, command.RuleSHA256 = validated.Bytes, validated.SHA256
	}
	value, err := handler.config.Alerts.UpdateAlert(r.Context(), command)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	dto, dtoErr := ruleDTO(value)
	if dtoErr != nil {
		handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
		return
	}
	handler.json(w, http.StatusOK, dto)
}

func destinationDTO(value control.AlertDestination, admin bool) generated.Destination {
	dto := generated.Destination{DestinationId: uuid.MustParse(value.DestinationID), Name: value.Name, Revision: strconv.FormatInt(value.Revision, 10), Enabled: value.Enabled, HasSecret: value.HasSecret}
	if admin {
		dto.Url = &value.URL
	}
	return dto
}
func ruleDTO(value control.AlertRule) (generated.Rule, error) {
	var rule generated.AlertRule
	if err := json.Unmarshal(value.RuleBytes, &rule); err != nil {
		return generated.Rule{}, err
	}
	dto := generated.Rule{AlertId: uuid.MustParse(value.AlertID), ProjectId: strconv.FormatInt(value.ProjectID, 10), Name: value.Name, Revision: strconv.FormatInt(value.Revision, 10), Enabled: value.Enabled, Kind: generated.RuleKind(value.Kind), Rule: rule, DestinationId: uuid.MustParse(value.DestinationID), CooldownSeconds: value.CooldownSeconds}
	if value.LastCompletedEndUS != nil {
		v := strconv.FormatInt(*value.LastCompletedEndUS, 10)
		dto.LastCompletedEndUs = &v
	}
	if value.LastFiredEndUS != nil {
		v := strconv.FormatInt(*value.LastFiredEndUS, 10)
		dto.LastFiredEndUs = &v
	}
	return dto, nil
}
func parsePositiveID(value string) (int64, bool) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil && parsed > 0
}
func canonicalUUID(value string) (string, bool) {
	value = strings.ToLower(value)
	parsed, err := uuid.Parse(value)
	return value, err == nil && parsed.String() == value
}

type alertListCursor struct {
	Version      int    `json:"v"`
	Purpose      int    `json:"p"`
	TenantID     int64  `json:"t"`
	ProjectID    int64  `json:"j"`
	UserID       int64  `json:"u"`
	AfterAlertID string `json:"a"`
}

func parseAlertListLimit(raw string) (int, bool) {
	if raw == "" {
		return 100, true
	}
	value, err := strconv.Atoi(raw)
	return value, err == nil && value >= 1 && value <= 1000
}

func (handler *ManagementHandler) encodeAlertCursor(tenantID, projectID, userID int64, after string) (string, error) {
	claims := alertListCursor{Version: 1, Purpose: 1, TenantID: tenantID, ProjectID: projectID, UserID: userID, AfterAlertID: after}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	_, _ = mac.Write([]byte("eventglass-alert-list-cursor-v1\x00"))
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (handler *ManagementHandler) decodeAlertCursor(token string, tenantID, projectID, userID int64) (string, error) {
	if token == "" {
		return "", nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return "", errors.New("invalid alert cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) > 1024 {
		return "", errors.New("invalid alert cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("invalid alert cursor")
	}
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	_, _ = mac.Write([]byte("eventglass-alert-list-cursor-v1\x00"))
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return "", errors.New("invalid alert cursor")
	}
	var claims alertListCursor
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Version != 1 || claims.Purpose != 1 || claims.TenantID != tenantID || claims.ProjectID != projectID || claims.UserID != userID {
		return "", errors.New("invalid alert cursor")
	}
	if normalized, ok := canonicalUUID(claims.AfterAlertID); !ok || normalized != claims.AfterAlertID {
		return "", errors.New("invalid alert cursor")
	}
	return claims.AfterAlertID, nil
}

func jsonContentType(r *http.Request) bool {
	return strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])) == "application/json"
}
