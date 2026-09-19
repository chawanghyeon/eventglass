package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/google/uuid"
)

const queryBodyLimit = 64 << 10

var (
	ErrPublicQueryNotFound = errors.New("public query not found")
	ErrPublicQueryGone     = errors.New("public query expired")
	ErrPublicQueryInvalid  = errors.New("public query input is invalid")
	ErrPublicQueryLimit    = errors.New("public query result exceeds limits")
)

type SearchSubmission struct {
	Result any
	Job    *generated.QueryJob
}

type AggregateSubmission struct {
	Result any
	Job    *generated.QueryJob
}

type PublicQueryService interface {
	Search(context.Context, control.SessionPrincipal, [32]byte, query.PublicSearchRequest) (SearchSubmission, error)
	Aggregate(context.Context, control.SessionPrincipal, [32]byte, query.PublicAggregateRequest) (AggregateSubmission, error)
	Record(context.Context, control.SessionPrincipal, [32]byte, int64, int64, string, string) (any, error)
	Job(context.Context, control.SessionPrincipal, [32]byte, int64, string) (generated.QueryJob, error)
	Cancel(context.Context, control.SessionPrincipal, [32]byte, int64, string) error
	RenewSnapshot(context.Context, control.SessionPrincipal, [32]byte, int64, string, string) (string, error)
	ReleaseSnapshot(context.Context, control.SessionPrincipal, [32]byte, int64, string) error
	Live(context.Context, control.SessionPrincipal, [32]byte, query.PublicLiveRequest, string, func(query.LiveEvent) error) error
}

func (handler *ManagementHandler) registerQueryRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/search", handler.postSearch)
	mux.HandleFunc("POST /v1/aggregate", handler.postAggregate)
	mux.HandleFunc("GET /v1/records/{id}", handler.getRecord)
	mux.HandleFunc("GET /v1/query-jobs/{id}", handler.getQueryJob)
	mux.HandleFunc("DELETE /v1/query-jobs/{id}", handler.deleteQueryJob)
	mux.HandleFunc("POST /v1/snapshots/{id}/heartbeat", handler.heartbeatSnapshot)
	mux.HandleFunc("DELETE /v1/snapshots/{id}", handler.deleteSnapshot)
	mux.HandleFunc("GET /v1/live", handler.getLive)
}

func (handler *ManagementHandler) getLive(writer http.ResponseWriter, request *http.Request) {
	principal, tokenHash, _, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	spec, resume, err := parseLiveRequest(request)
	if err != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	handler.streamLive(writer, request, principal, tokenHash, spec, resume)
}

func (handler *ManagementHandler) streamLive(writer http.ResponseWriter, request *http.Request, principal control.SessionPrincipal, tokenHash [32]byte, spec query.PublicLiveRequest, resume string) {
	select {
	case handler.liveSlots <- struct{}{}:
		defer func() { <-handler.liveSlots }()
	default:
		writer.Header().Set("Retry-After", "1")
		handler.error(writer, request, http.StatusTooManyRequests, "admission_limited", true)
		return
	}
	started := false
	emit := func(event query.LiveEvent) error {
		encoded, err := encodeSSE(event)
		if err != nil {
			return err
		}
		if !started {
			noStore(writer)
			writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			writer.Header().Set("X-Accel-Buffering", "no")
			writer.WriteHeader(http.StatusOK)
			started = true
		}
		controller := http.NewResponseController(writer)
		_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := writer.Write(encoded); err != nil {
			return err
		}
		if err := controller.Flush(); err != nil {
			return err
		}
		// The deadline bounds this write, not the idle interval before heartbeat.
		_ = controller.SetWriteDeadline(time.Time{})
		return nil
	}
	err := handler.config.Queries.Live(request.Context(), principal, tokenHash, spec, resume, emit)
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	if !started {
		handler.publicQueryError(writer, request, err)
	}
}

func parseLiveRequest(request *http.Request) (query.PublicLiveRequest, string, error) {
	values := request.URL.Query()
	tenantID, err := parseCanonicalInt64(values.Get("tenant_id"))
	if err != nil || tenantID <= 0 {
		return query.PublicLiveRequest{}, "", errors.New("invalid live tenant")
	}
	projects, err := parseLiveInt64Set(values["project_ids"])
	if err != nil || len(projects) < 1 || len(projects) > 100 {
		return query.PublicLiveRequest{}, "", errors.New("invalid live projects")
	}
	kinds, err := parseLiveKinds(values["kinds"])
	if err != nil {
		return query.PublicLiveRequest{}, "", err
	}
	expression := values.Get("expression")
	if len(expression) > 8192 {
		return query.PublicLiveRequest{}, "", errors.New("live expression is too large")
	}
	root := &query.Node{Op: "constant", Constant: true}
	if expression != "" {
		root, err = query.ParseCEL(expression)
		if err != nil {
			return query.PublicLiveRequest{}, "", err
		}
	}
	canonical, err := query.CanonicalFilter(root)
	if err != nil {
		return query.PublicLiveRequest{}, "", err
	}
	result := query.PublicLiveRequest{TenantID: tenantID, ProjectIDs: projects, Kinds: kinds, Filter: root, Canonical: canonical}
	if value := values.Get("catchup_start_us"); value != "" {
		micros, parseErr := parseCanonicalInt64(value)
		if parseErr != nil {
			return result, "", parseErr
		}
		start := time.UnixMicro(micros)
		result.CatchupStart = &start
	}
	resume := values.Get("resume_token")
	lastEventID := request.Header.Get("Last-Event-ID")
	if len(resume) > query.MaxTokenPayload*2 || len(lastEventID) > query.MaxTokenPayload*2 || resume != "" && lastEventID != "" && resume != lastEventID {
		return result, "", errors.New("live resume checkpoints disagree")
	}
	if resume == "" {
		resume = lastEventID
	}
	return result, resume, nil
}

func parseLiveInt64Set(raw []string) ([]int64, error) {
	parts := splitQuerySet(raw)
	result := make([]int64, len(parts))
	for index, part := range parts {
		value, err := parseCanonicalInt64(part)
		if err != nil || value <= 0 {
			return nil, errors.New("invalid live project")
		}
		result[index] = value
	}
	slices.Sort(result)
	if len(result) != len(slices.Compact(result)) {
		return nil, errors.New("duplicate live project")
	}
	return result, nil
}

func parseLiveKinds(raw []string) ([]model.Kind, error) {
	parts := splitQuerySet(raw)
	if len(parts) == 0 {
		return []model.Kind{model.KindError, model.KindLog, model.KindTransaction}, nil
	}
	result := make([]model.Kind, len(parts))
	for index, part := range parts {
		result[index] = model.Kind(part)
		if result[index] != model.KindError && result[index] != model.KindLog && result[index] != model.KindTransaction {
			return nil, errors.New("invalid live kind")
		}
	}
	slices.Sort(result)
	if len(result) != len(slices.Compact(result)) {
		return nil, errors.New("duplicate live kind")
	}
	return result, nil
}

func splitQuerySet(raw []string) []string {
	var result []string
	for _, value := range raw {
		for _, part := range strings.Split(value, ",") {
			if part = strings.TrimSpace(part); part != "" {
				result = append(result, part)
			}
		}
	}
	return result
}

func encodeSSE(event query.LiveEvent) ([]byte, error) {
	if event.Type != "rows" && event.Type != "checkpoint" && event.Type != "heartbeat" && event.Type != "resync_required" && event.Type != "error" {
		return nil, errors.New("invalid live event type")
	}
	if strings.ContainsAny(event.ID, "\r\n") {
		return nil, errors.New("invalid live event id")
	}
	data, err := json.Marshal(event.Data)
	if err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	if event.ID != "" {
		fmt.Fprintf(&encoded, "id: %s\n", event.ID)
	}
	fmt.Fprintf(&encoded, "event: %s\ndata: %s\n\n", event.Type, data)
	if encoded.Len() > query.LiveMaximumPending {
		return nil, errLivePendingLimit
	}
	return encoded.Bytes(), nil
}

func (handler *ManagementHandler) postSearch(writer http.ResponseWriter, request *http.Request) {
	if !handler.originAndJSON(writer, request) {
		return
	}
	principal, tokenHash, _, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	object, ok := handler.decodeObject(writer, request, queryBodyLimit,
		"tenant_id", "project_ids", "start_us", "end_us", "time_basis", "kinds", "expression", "filter",
		"read_token", "cursor", "limit", "sort", "mode", "projection")
	if !ok {
		return
	}
	var body generated.SearchRequest
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	spec, err := parseSearchRequest(body)
	if err != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	submission, err := handler.config.Queries.Search(request.Context(), principal, tokenHash, spec)
	if err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	if submission.Result != nil && submission.Job == nil {
		handler.json(writer, http.StatusOK, submission.Result)
		return
	}
	if submission.Job == nil || submission.Result != nil {
		handler.publicQueryError(writer, request, errors.New("invalid search submission"))
		return
	}
	writer.Header().Set("Location", "/v1/query-jobs/"+submission.Job.QueryId.String()+"?tenant_id="+strconv.FormatInt(spec.Dataset.Spec.TenantID, 10))
	handler.json(writer, http.StatusAccepted, *submission.Job)
}

func (handler *ManagementHandler) postAggregate(writer http.ResponseWriter, request *http.Request) {
	if !handler.originAndJSON(writer, request) {
		return
	}
	principal, tokenHash, _, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	object, ok := handler.decodeObject(writer, request, queryBodyLimit,
		"tenant_id", "project_ids", "start_us", "end_us", "time_basis", "kinds", "expression", "filter",
		"metrics", "group_by", "histogram", "top", "order", "read_token", "mode")
	if !ok {
		return
	}
	var body generated.AggregateRequest
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	spec, err := parseAggregateRequest(body)
	if err != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	submission, err := handler.config.Queries.Aggregate(request.Context(), principal, tokenHash, spec)
	if err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	if submission.Result != nil && submission.Job == nil {
		handler.json(writer, http.StatusOK, submission.Result)
		return
	}
	if submission.Job == nil || submission.Result != nil {
		handler.publicQueryError(writer, request, errors.New("invalid aggregate submission"))
		return
	}
	writer.Header().Set("Location", "/v1/query-jobs/"+submission.Job.QueryId.String()+"?tenant_id="+strconv.FormatInt(spec.Dataset.Spec.TenantID, 10))
	handler.json(writer, http.StatusAccepted, *submission.Job)
}

func (handler *ManagementHandler) getRecord(writer http.ResponseWriter, request *http.Request) {
	principal, tokenHash, _, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	tenantID, tenantErr := parseCanonicalInt64(request.URL.Query().Get("tenant_id"))
	projectID, projectErr := parseCanonicalInt64(request.URL.Query().Get("project_id"))
	recordID := request.PathValue("id")
	if tenantErr != nil || projectErr != nil || tenantID <= 0 || projectID <= 0 || !validLowerSHA(recordID) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	result, err := handler.config.Queries.Record(request.Context(), principal, tokenHash, tenantID, projectID, recordID, request.URL.Query().Get("read_token"))
	if err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	handler.json(writer, http.StatusOK, result)
}

func (handler *ManagementHandler) getQueryJob(writer http.ResponseWriter, request *http.Request) {
	principal, tokenHash, _, ok := handler.authenticate(writer, request, false)
	if !ok {
		return
	}
	tenantID, queryID, ok := queryPathScope(writer, request, handler)
	if !ok {
		return
	}
	result, err := handler.config.Queries.Job(request.Context(), principal, tokenHash, tenantID, queryID)
	if err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	handler.json(writer, http.StatusOK, result)
}

func (handler *ManagementHandler) deleteQueryJob(writer http.ResponseWriter, request *http.Request) {
	principal, tokenHash, _, ok := handler.authenticate(writer, request, true)
	if !ok {
		return
	}
	tenantID, queryID, ok := queryPathScope(writer, request, handler)
	if !ok {
		return
	}
	if err := handler.config.Queries.Cancel(request.Context(), principal, tokenHash, tenantID, queryID); err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	noStore(writer)
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *ManagementHandler) heartbeatSnapshot(writer http.ResponseWriter, request *http.Request) {
	principal, tokenHash, _, ok := handler.authenticate(writer, request, true)
	if !ok || !handler.originAndJSON(writer, request) {
		return
	}
	object, ok := handler.decodeObject(writer, request, authBodyLimit, "tenant_id", "read_token")
	if !ok {
		return
	}
	var body generated.SnapshotHeartbeat
	if !decodeKnownObject(object, &body) {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, err := parseCanonicalInt64(body.TenantId)
	if err != nil || uuid.Validate(request.PathValue("id")) != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	token, err := handler.config.Queries.RenewSnapshot(request.Context(), principal, tokenHash, tenantID, request.PathValue("id"), body.ReadToken)
	if err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	handler.json(writer, http.StatusOK, generated.ReadTokenResponse{ReadToken: token})
}

func (handler *ManagementHandler) deleteSnapshot(writer http.ResponseWriter, request *http.Request) {
	principal, tokenHash, _, ok := handler.authenticate(writer, request, true)
	if !ok {
		return
	}
	tenantID, err := parseCanonicalInt64(request.URL.Query().Get("tenant_id"))
	if err != nil || uuid.Validate(request.PathValue("id")) != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return
	}
	if err := handler.config.Queries.ReleaseSnapshot(request.Context(), principal, tokenHash, tenantID, request.PathValue("id")); err != nil {
		handler.publicQueryError(writer, request, err)
		return
	}
	noStore(writer)
	writer.WriteHeader(http.StatusNoContent)
}

func queryPathScope(writer http.ResponseWriter, request *http.Request, handler *ManagementHandler) (int64, string, bool) {
	tenantID, err := parseCanonicalInt64(request.URL.Query().Get("tenant_id"))
	queryID := request.PathValue("id")
	if err != nil || tenantID <= 0 || uuid.Validate(queryID) != nil {
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
		return 0, "", false
	}
	return tenantID, queryID, true
}

func (handler *ManagementHandler) publicQueryError(writer http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, ErrPublicQueryNotFound):
		handler.error(writer, request, http.StatusNotFound, "not_found", false)
	case errors.Is(err, ErrPublicQueryGone), errors.Is(err, control.ErrSnapshotExpired), errors.Is(err, query.ErrTokenExpired):
		handler.error(writer, request, http.StatusGone, "expired", false)
	case errors.Is(err, control.ErrForbidden), errors.Is(err, control.ErrUnauthenticated), errors.Is(err, query.ErrTokenForbidden):
		handler.error(writer, request, http.StatusForbidden, "forbidden", false)
	case errors.Is(err, control.ErrStorageGeneration), errors.Is(err, query.ErrTokenGenerationChanged):
		handler.error(writer, request, http.StatusConflict, "storage_generation_changed", false)
	case errors.Is(err, control.ErrQueryLimitExceeded), errors.Is(err, query.ErrQueryLimit), errors.Is(err, ErrPublicQueryLimit):
		handler.error(writer, request, http.StatusUnprocessableEntity, "query_limit_exceeded", false)
	case errors.Is(err, context.DeadlineExceeded):
		handler.error(writer, request, http.StatusGatewayTimeout, "query_timeout", true)
	case errors.Is(err, ErrPublicQueryInvalid), errors.Is(err, query.ErrTokenMalformed), errors.Is(err, query.ErrTokenMismatch):
		handler.error(writer, request, http.StatusBadRequest, "invalid_input", false)
	default:
		handler.error(writer, request, http.StatusServiceUnavailable, "dependency_unavailable", true)
	}
}

func validLowerSHA(value string) bool {
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
