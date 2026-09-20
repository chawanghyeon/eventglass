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
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

type deliveryListCursor struct {
	Version     int    `json:"v"`
	Purpose     int    `json:"p"`
	TenantID    int64  `json:"t"`
	ProjectID   int64  `json:"j"`
	UserID      int64  `json:"u"`
	AlertID     string `json:"a"`
	State       string `json:"s"`
	CreatedAtUS int64  `json:"c"`
	DeliveryID  string `json:"d"`
}

func (handler *ManagementHandler) listDeliveries(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	projectID, pok := parsePositiveID(r.URL.Query().Get("project_id"))
	limit, lok := parseAlertListLimit(r.URL.Query().Get("limit"))
	alertID := r.URL.Query().Get("alert_id")
	state := r.URL.Query().Get("state")
	if alertID != "" {
		var valid bool
		alertID, valid = canonicalUUID(alertID)
		if !valid {
			handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
			return
		}
	}
	if !tok || !pok || !lok || state != "" && !generated.DeliveryState(state).Valid() {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	beforeTime, beforeID, err := handler.decodeDeliveryCursor(r.URL.Query().Get("cursor"), tenantID, projectID, principal.UserID, alertID, state)
	if err != nil {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	items, err := handler.config.Alerts.ListDeliveryPage(r.Context(), control.DeliveryPageCommand{TenantID: tenantID, ProjectID: projectID, ActorUserID: principal.UserID, AlertID: alertID, State: state, Limit: limit, BeforeCreatedAt: beforeTime, BeforeDeliveryID: beforeID})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.DeliveryList{Items: make([]generated.Delivery, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		result.NextCursor, err = handler.encodeDeliveryCursor(tenantID, projectID, principal.UserID, alertID, state, last.CreatedAt, last.Authority.DeliveryID)
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, item := range items {
		result.Items = append(result.Items, deliveryDTO(item))
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) retryDelivery(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	if !jsonContentType(r) {
		handler.error(w, r, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return
	}
	object, ok := handler.decodeObject(w, r, managementBodyLimit, "tenant_id", "project_id", "revision")
	if !ok {
		return
	}
	var body generated.ScopedRevision
	if !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	projectID, pok := parsePositiveID(string(body.ProjectId))
	revision, rok := parsePositiveID(string(body.Revision))
	deliveryID, dok := canonicalUUID(r.PathValue("id"))
	if !tok || !pok || !rok || !dok {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	value, err := handler.config.Alerts.RetryDelivery(r.Context(), control.RetryDeliveryCommand{TenantID: tenantID, ProjectID: projectID, ActorUserID: principal.UserID, ExpectedRevision: revision, DeliveryID: deliveryID})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, deliveryDTO(value))
}

func deliveryDTO(value control.Delivery) generated.Delivery {
	result := generated.Delivery{DeliveryId: uuid.MustParse(value.Authority.DeliveryID), AlertId: uuid.MustParse(value.AlertID), State: generated.DeliveryState(value.State), Revision: strconv.FormatInt(value.Revision, 10), Attempt: value.Attempt, CreatedAt: value.CreatedAt}
	result.LastHttpStatus, result.ErrorCode = value.LastStatus, value.ErrorCode
	if value.State == "queued" {
		retry := value.RetryAt
		result.NextRetryAt = &retry
	}
	return result
}

func (handler *ManagementHandler) encodeDeliveryCursor(tenantID, projectID, userID int64, alertID, state string, createdAt time.Time, deliveryID string) (string, error) {
	claims := deliveryListCursor{Version: 1, Purpose: 2, TenantID: tenantID, ProjectID: projectID, UserID: userID, AlertID: alertID, State: state, CreatedAtUS: createdAt.UnixMicro(), DeliveryID: deliveryID}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	_, _ = mac.Write([]byte("eventglass-delivery-list-cursor-v1\x00"))
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (handler *ManagementHandler) decodeDeliveryCursor(token string, tenantID, projectID, userID int64, alertID, state string) (*time.Time, string, error) {
	if token == "" {
		return nil, "", nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, "", errors.New("invalid delivery cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) > 1024 {
		return nil, "", errors.New("invalid delivery cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, "", errors.New("invalid delivery cursor")
	}
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	_, _ = mac.Write([]byte("eventglass-delivery-list-cursor-v1\x00"))
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, "", errors.New("invalid delivery cursor")
	}
	var claims deliveryListCursor
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Version != 1 || claims.Purpose != 2 || claims.TenantID != tenantID || claims.ProjectID != projectID || claims.UserID != userID || claims.AlertID != alertID || claims.State != state || claims.CreatedAtUS <= 0 {
		return nil, "", errors.New("invalid delivery cursor")
	}
	if normalized, ok := canonicalUUID(claims.DeliveryID); !ok || normalized != claims.DeliveryID {
		return nil, "", errors.New("invalid delivery cursor")
	}
	created := time.UnixMicro(claims.CreatedAtUS).UTC()
	return &created, claims.DeliveryID, nil
}
