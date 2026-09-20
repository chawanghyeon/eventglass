package api

import (
	"net/http"
	"strconv"
	"strings"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
)

func (handler *ManagementHandler) listIssueOccurrences(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	projectID, pok := parsePositiveID(r.URL.Query().Get("project_id"))
	limit, lok := parseAlertListLimit(r.URL.Query().Get("limit"))
	issueID := strings.ToLower(r.PathValue("id"))
	claims, err := handler.decodeOperatorCursor(r.URL.Query().Get("cursor"))
	if !tok || !pok || !lok || !validSHA256(issueID) || err != nil || claims.Version != 0 && (claims.Purpose != "occurrences" || claims.TenantID != tenantID || claims.UserID != principal.UserID || claims.ProjectID != projectID || claims.IssueID != issueID || claims.NS < 0 || claims.NS > 999 || !validSHA256(claims.RecordID)) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	var beforeEvent *int64
	var beforeNS *int
	if claims.Version != 0 {
		beforeEvent, beforeNS = &claims.EventUS, &claims.NS
	}
	items, err := handler.config.Auth.ListOccurrencePage(r.Context(), control.OccurrencePageCommand{TenantID: tenantID, ProjectID: projectID, ActorUserID: principal.UserID, IssueID: issueID, Limit: limit, BeforeEventUS: beforeEvent, BeforeNS: beforeNS, BeforeRecordID: claims.RecordID})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.OccurrenceList{Items: make([]generated.Occurrence, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		result.NextCursor, err = handler.encodeOperatorCursor(operatorCursor{Purpose: "occurrences", TenantID: tenantID, UserID: principal.UserID, ProjectID: projectID, IssueID: issueID, EventUS: last.EventUS, NS: last.NS, RecordID: last.RecordID})
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, item := range items {
		release := ""
		if item.Release != nil {
			release = *item.Release
		}
		result.Items = append(result.Items, generated.Occurrence{RecordId: item.RecordID, ProjectId: strconv.FormatInt(item.ProjectID, 10), EventUs: strconv.FormatInt(item.EventUS, 10), Ns: item.NS, ReceivedUs: strconv.FormatInt(item.ReceivedUS, 10), Release: release, DetailAvailable: item.DetailAvailable})
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) listSDKOutcomes(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	startUS, sok := parseSignedInt64(r.URL.Query().Get("start_us"))
	endUS, eok := parseSignedInt64(r.URL.Query().Get("end_us"))
	limit, lok := parseAlertListLimit(r.URL.Query().Get("limit"))
	claims, err := handler.decodeOperatorCursor(r.URL.Query().Get("cursor"))
	if !tok || !sok || !eok || startUS >= endUS || !lok || err != nil || claims.Version != 0 && (claims.Purpose != "sdk-outcomes" || claims.TenantID != tenantID || claims.UserID != principal.UserID || claims.StartUS != startUS || claims.EndUS != endUS || !validSHA256(claims.Category) || !validSHA256(claims.Reason)) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	items, err := handler.config.Auth.ListSDKOutcomePage(r.Context(), control.SDKOutcomePageCommand{TenantID: tenantID, ActorUserID: principal.UserID, StartUS: startUS, EndUS: endUS, Limit: limit, AfterCategorySHA256: claims.Category, AfterReasonSHA256: claims.Reason})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.SDKOutcomeList{Items: make([]generated.SDKOutcome, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		result.NextCursor, err = handler.encodeOperatorCursor(operatorCursor{Purpose: "sdk-outcomes", TenantID: tenantID, UserID: principal.UserID, StartUS: startUS, EndUS: endUS, Category: last.CategorySHA256, Reason: last.ReasonSHA256})
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, item := range items {
		result.Items = append(result.Items, generated.SDKOutcome{SdkName: item.SDKName, Category: item.Category, Reason: item.Reason, Count: strconv.FormatInt(item.Count, 10), Approximate: item.Approximate})
	}
	handler.json(w, http.StatusOK, result)
}

func parseSignedInt64(value string) (int64, bool) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}
