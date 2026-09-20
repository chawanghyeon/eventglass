package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
)

func (handler *ManagementHandler) registerOperatorRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/projects", handler.listProjects)
	mux.HandleFunc("POST /v1/projects", handler.createProject)
	mux.HandleFunc("PATCH /v1/projects/{id}", handler.updateProject)
	mux.HandleFunc("GET /v1/issues", handler.listIssues)
	mux.HandleFunc("GET /v1/issues/{id}", handler.getIssue)
	mux.HandleFunc("PATCH /v1/issues/{id}", handler.updateIssue)
	mux.HandleFunc("GET /v1/issues/{id}/occurrences", handler.listIssueOccurrences)
	mux.HandleFunc("GET /v1/system/sdk-outcomes", handler.listSDKOutcomes)
}

type operatorCursor struct {
	Version    int     `json:"v"`
	Purpose    string  `json:"p"`
	TenantID   int64   `json:"t"`
	UserID     int64   `json:"u"`
	ProjectIDs []int64 `json:"j,omitempty"`
	Status     string  `json:"s,omitempty"`
	ProjectID  int64   `json:"i,omitempty"`
	IssueID    string  `json:"x,omitempty"`
	ReceivedUS int64   `json:"r,omitempty"`
	EventUS    int64   `json:"e,omitempty"`
	NS         int     `json:"n,omitempty"`
	RecordID   string  `json:"d,omitempty"`
	StartUS    int64   `json:"a,omitempty"`
	EndUS      int64   `json:"b,omitempty"`
	Category   string  `json:"c,omitempty"`
	Reason     string  `json:"q,omitempty"`
}

func (handler *ManagementHandler) encodeOperatorCursor(claims operatorCursor) (string, error) {
	claims.Version = 1
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	_, _ = mac.Write([]byte("eventglass-operator-cursor-v1\x00"))
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (handler *ManagementHandler) decodeOperatorCursor(token string) (operatorCursor, error) {
	if token == "" {
		return operatorCursor{}, nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return operatorCursor{}, errors.New("invalid operator cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) > 8192 {
		return operatorCursor{}, errors.New("invalid operator cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return operatorCursor{}, errors.New("invalid operator cursor")
	}
	mac := hmac.New(sha256.New, handler.config.LoginBucketKey[:])
	_, _ = mac.Write([]byte("eventglass-operator-cursor-v1\x00"))
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return operatorCursor{}, errors.New("invalid operator cursor")
	}
	var claims operatorCursor
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Version != 1 {
		return operatorCursor{}, errors.New("invalid operator cursor")
	}
	return claims, nil
}

func (handler *ManagementHandler) listProjects(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	limit, lok := parseAlertListLimit(r.URL.Query().Get("limit"))
	claims, err := handler.decodeOperatorCursor(r.URL.Query().Get("cursor"))
	if !tok || !lok || err != nil || claims.Version != 0 && (claims.Purpose != "projects" || claims.TenantID != tenantID || claims.UserID != principal.UserID || claims.ProjectID <= 0) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	items, err := handler.config.Auth.ListProjectPage(r.Context(), control.ProjectPageCommand{TenantID: tenantID, ActorUserID: principal.UserID, AfterProjectID: claims.ProjectID, Limit: limit})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.ProjectList{Items: make([]generated.Project, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		result.NextCursor, err = handler.encodeOperatorCursor(operatorCursor{Purpose: "projects", TenantID: tenantID, UserID: principal.UserID, ProjectID: items[len(items)-1].ProjectID})
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, item := range items {
		result.Items = append(result.Items, projectDTO(item))
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) createProject(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	object, ok := handler.decodeJSON(w, r, "tenant_id", "name", "default_service", "allowed_origins")
	if !ok {
		return
	}
	var body generated.ProjectCreate
	if !hasNonNullFields(object, "tenant_id", "name", "default_service", "allowed_origins") || !decodeKnownObject(object, &body) || body.AllowedOrigins == nil || !validOrigins(body.AllowedOrigins) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, valid := parsePositiveID(string(body.TenantId))
	if !valid {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	value, err := handler.config.Auth.CreateProject(r.Context(), control.CreateProjectCommand{TenantID: tenantID, ActorUserID: principal.UserID, Name: body.Name, DefaultService: body.DefaultService, AllowedOrigins: body.AllowedOrigins, RequestID: requestID(r), AuditID: newUUID()})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusCreated, projectDTO(value))
}

func (handler *ManagementHandler) updateProject(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	object, ok := handler.decodeJSON(w, r, "tenant_id", "revision", "name", "state", "default_service", "allowed_origins", "scrub_rules")
	if !ok {
		return
	}
	var body generated.ProjectPatch
	if !hasNonNullFields(object, "tenant_id", "revision") || len(object) <= 2 || hasNullField(object, "name", "state", "default_service", "allowed_origins", "scrub_rules") || !decodeKnownObject(object, &body) || body.AllowedOrigins != nil && !validOrigins(*body.AllowedOrigins) || body.ScrubRules != nil && !validScrubRules(*body.ScrubRules) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	projectID, pok := parsePositiveID(r.PathValue("id"))
	revision, rok := parsePositiveID(string(body.Revision))
	if !tok || !pok || !rok {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	var state *string
	if body.State != nil {
		value := string(*body.State)
		state = &value
	}
	var scrub []byte
	if body.ScrubRules != nil {
		scrub, _ = json.Marshal(body.ScrubRules)
	}
	value, err := handler.config.Auth.UpdateProject(r.Context(), control.UpdateProjectCommand{TenantID: tenantID, ProjectID: projectID, ActorUserID: principal.UserID, ExpectedRevision: revision, Name: body.Name, State: state, DefaultService: body.DefaultService, AllowedOrigins: body.AllowedOrigins, ScrubRules: scrub, RequestID: requestID(r), AuditID: newUUID()})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, projectDTO(value))
}

func (handler *ManagementHandler) listIssues(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	projects, pok := parseProjectIDs(r.URL.Query()["project_ids"])
	limit, lok := parseAlertListLimit(r.URL.Query().Get("limit"))
	status := r.URL.Query().Get("status")
	claims, err := handler.decodeOperatorCursor(r.URL.Query().Get("cursor"))
	if !tok || !pok || !lok || status != "" && !generated.IssueStatus(status).Valid() || err != nil || claims.Version != 0 && (claims.Purpose != "issues" || claims.TenantID != tenantID || claims.UserID != principal.UserID || claims.Status != status || !slices.Equal(claims.ProjectIDs, projects) || claims.ReceivedUS == 0 || !validSHA256(claims.IssueID)) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	var before *int64
	if claims.Version != 0 {
		before = &claims.ReceivedUS
	}
	items, err := handler.config.Auth.ListIssuePage(r.Context(), control.IssuePageCommand{TenantID: tenantID, ActorUserID: principal.UserID, ProjectIDs: projects, Status: status, Limit: limit, BeforeReceivedUS: before, BeforeIssueID: claims.IssueID})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.IssueList{Items: make([]generated.Issue, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		result.NextCursor, err = handler.encodeOperatorCursor(operatorCursor{Purpose: "issues", TenantID: tenantID, UserID: principal.UserID, ProjectIDs: projects, Status: status, ReceivedUS: last.LastReceivedUS, IssueID: last.IssueID})
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, item := range items {
		result.Items = append(result.Items, issueDTO(item))
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) getIssue(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	projectID, pok := parsePositiveID(r.URL.Query().Get("project_id"))
	issueID := strings.ToLower(r.PathValue("id"))
	if !tok || !pok || !validSHA256(issueID) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	value, err := handler.config.Auth.GetIssue(r.Context(), tenantID, projectID, principal.UserID, issueID)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, issueDTO(value))
}

func (handler *ManagementHandler) updateIssue(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	object, ok := handler.decodeJSON(w, r, "tenant_id", "project_id", "revision", "status", "operation_id")
	if !ok {
		return
	}
	var body generated.IssuePatch
	if !hasNonNullFields(object, "tenant_id", "project_id", "revision", "status") || hasNullField(object, "operation_id") || !decodeKnownObject(object, &body) || !body.Status.Valid() {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	projectID, pok := parsePositiveID(string(body.ProjectId))
	revision, rok := parsePositiveID(string(body.Revision))
	issueID := strings.ToLower(r.PathValue("id"))
	if !tok || !pok || !rok || !validSHA256(issueID) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	action := control.IssueReopen
	if body.Status == generated.Resolved {
		action = control.IssueResolve
	} else if body.Status == generated.Ignored {
		action = control.IssueIgnore
	}
	operationID := ""
	if body.OperationId != nil {
		operationID = body.OperationId.String()
	}
	_, err := handler.config.Auth.ChangeIssueStatus(r.Context(), control.IssueStatusCommand{TenantID: tenantID, ProjectID: projectID, IssueID: issueID, ExpectedRevision: revision, Action: action, ActorUserID: &principal.UserID, RequestID: requestID(r), AuditID: newUUID(), OperationID: operationID})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	value, err := handler.config.Auth.GetIssue(r.Context(), tenantID, projectID, principal.UserID, issueID)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, issueDTO(value))
}

func (handler *ManagementHandler) decodeJSON(w http.ResponseWriter, r *http.Request, allowed ...string) (map[string]any, bool) {
	if !jsonContentType(r) {
		handler.error(w, r, http.StatusUnsupportedMediaType, "unsupported_encoding", false)
		return nil, false
	}
	return handler.decodeObject(w, r, managementBodyLimit, allowed...)
}

func projectDTO(value control.Project) generated.Project {
	result := generated.Project{TenantId: strconv.FormatInt(value.TenantID, 10), ProjectId: strconv.FormatInt(value.ProjectID, 10), Name: value.Name, State: generated.ProjectState(value.State), DefaultService: value.DefaultService, AllowedOrigins: value.AllowedOrigins, Revision: strconv.FormatInt(value.Revision, 10), AuthRevision: strconv.FormatInt(value.AuthRevision, 10), ScrubRevision: strconv.FormatInt(value.ScrubRevision, 10)}
	if len(value.ScrubRules) > 0 {
		var rules generated.ScrubRules
		if json.Unmarshal(value.ScrubRules, &rules) == nil && validScrubRules(rules) {
			result.ScrubRules = &rules
		} else {
			result.ScrubRules = &generated.ScrubRules{Version: 1, RedactKeys: []string{}, RedactPaths: []string{}, BodyPatterns: []string{}}
		}
	}
	return result
}

func issueDTO(value control.ManagedIssue) generated.Issue {
	return generated.Issue{IssueId: value.IssueID, ProjectId: strconv.FormatInt(value.ProjectID, 10), Status: generated.IssueStatus(value.Status), Revision: strconv.FormatInt(value.Revision, 10), Title: value.Title, GroupingVersion: value.GroupingVersion, LifetimeOccurrenceCount: strconv.FormatInt(value.OccurrenceCount, 10), First: issuePointDTO(value.First), Last: issuePointDTO(value.Last), LastReceivedUs: strconv.FormatInt(value.LastReceivedUS, 10), DetailRetentionFloorUs: strconv.FormatInt(value.DetailRetentionFloorUS, 10)}
}

func issuePointDTO(value control.ManagedIssuePoint) generated.IssuePoint {
	release := ""
	if value.Release != nil {
		release = *value.Release
	}
	return generated.IssuePoint{EventUs: strconv.FormatInt(value.EventUS, 10), Ns: value.NS, RecordId: value.RecordID, Release: release}
}

func validOrigins(origins []string) bool {
	if len(origins) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, raw := range origins {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" || seen[raw] {
			return false
		}
		seen[raw] = true
	}
	return true
}

func validScrubRules(rules generated.ScrubRules) bool {
	if rules.Version != 1 || len(rules.RedactKeys) > 32 || len(rules.RedactPaths) > 32 || len(rules.BodyPatterns) > 32 {
		return false
	}
	for _, value := range rules.RedactKeys {
		if value == "" || len(value) > 128 {
			return false
		}
	}
	for _, value := range rules.RedactPaths {
		if !strings.HasPrefix(value, "/") || len(value) > 1024 {
			return false
		}
	}
	for _, value := range rules.BodyPatterns {
		if value == "" || len(value) > 1024 {
			return false
		}
	}
	return true
}

func parseProjectIDs(values []string) ([]int64, bool) {
	result := make([]int64, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			parsed, ok := parsePositiveID(part)
			if !ok {
				return nil, false
			}
			result = append(result, parsed)
		}
	}
	if len(result) == 0 || len(result) > 1000 {
		return nil, false
	}
	slices.Sort(result)
	if compact := slices.Compact(result); len(compact) != len(result) {
		return nil, false
	}
	return result, true
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func hasNonNullFields(object map[string]any, names ...string) bool {
	for _, name := range names {
		if value, exists := object[name]; !exists || value == nil {
			return false
		}
	}
	return true
}

func hasNullField(object map[string]any, names ...string) bool {
	for _, name := range names {
		if value, exists := object[name]; exists && value == nil {
			return true
		}
	}
	return false
}
