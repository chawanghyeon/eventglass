package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
)

func (handler *ManagementHandler) listUsers(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, tok := parsePositiveID(r.URL.Query().Get("tenant_id"))
	limit, lok := parseAlertListLimit(r.URL.Query().Get("limit"))
	claims, err := handler.decodeOperatorCursor(r.URL.Query().Get("cursor"))
	if !tok || !lok || err != nil || claims.Version != 0 && (claims.Purpose != "users" || claims.TenantID != tenantID || claims.UserID != principal.UserID || claims.AfterUserID <= 0) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	items, err := handler.config.Auth.ListUserPage(r.Context(), control.UserPageCommand{TenantID: tenantID, ActorUserID: principal.UserID, AfterUserID: claims.AfterUserID, Limit: limit})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	result := generated.UserList{Items: make([]generated.User, 0, min(len(items), limit))}
	if len(items) > limit {
		items = items[:limit]
		result.NextCursor, err = handler.encodeOperatorCursor(operatorCursor{Purpose: "users", TenantID: tenantID, UserID: principal.UserID, AfterUserID: items[len(items)-1].UserID})
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	for _, item := range items {
		result.Items = append(result.Items, managedUserDTO(item))
	}
	handler.json(w, http.StatusOK, result)
}

func (handler *ManagementHandler) createUser(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	object, ok := handler.decodeJSON(w, r, "tenant_id", "email", "initial_password", "role", "project_grants")
	if !ok {
		return
	}
	var body generated.UserCreate
	if !hasNonNullFields(object, "tenant_id", "email", "initial_password", "role", "project_grants") || !decodeKnownObject(object, &body) || !body.Role.Valid() || body.ProjectGrants == nil {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, valid := parsePositiveID(string(body.TenantId))
	grants, validGrants := parseManagedGrants(body.ProjectGrants)
	if !valid || !validGrants {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	phc, err := handler.config.Passwords.Hash(body.InitialPassword)
	if err != nil {
		handler.passwordError(w, r, err)
		return
	}
	value, err := handler.config.Auth.CreateUser(r.Context(), control.CreateUserCommand{TenantID: tenantID, ActorUserID: principal.UserID, Email: body.Email, PasswordPHC: phc, Role: string(body.Role), ProjectGrants: grants, RequestID: requestID(r), AuditID: newUUID()})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusCreated, managedUserDTO(value))
}

func (handler *ManagementHandler) updateUser(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	object, ok := handler.decodeJSON(w, r, "tenant_id", "revision", "state", "role", "project_grants", "new_password")
	if !ok {
		return
	}
	var body generated.UserPatch
	if !hasNonNullFields(object, "tenant_id", "revision") || len(object) <= 2 || hasNullField(object, "state", "project_grants", "new_password") || !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	targetID, uok := parsePositiveID(r.PathValue("id"))
	revision, rok := parsePositiveID(string(body.Revision))
	if !tok || !uok || !rok || body.State != nil && !body.State.Valid() {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	command := control.UpdateUserCommand{TenantID: tenantID, ActorUserID: principal.UserID, TargetUserID: targetID, ExpectedRevision: revision, RequestID: requestID(r), AuditID: newUUID()}
	if body.State != nil {
		value := string(*body.State)
		command.State = &value
	}
	if _, present := object["role"]; present {
		command.RoleSet = true
		if !hasNullField(object, "role") {
			if body.Role == nil || !body.Role.Valid() {
				handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
				return
			}
			value := string(*body.Role)
			command.Role = &value
		}
	}
	if body.ProjectGrants != nil {
		command.GrantsSet = true
		var valid bool
		command.ProjectGrants, valid = parseManagedGrants(*body.ProjectGrants)
		if !valid {
			handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
			return
		}
	}
	if body.NewPassword != nil { // pragma: allowlist secret -- user-supplied credential is hashed before persistence
		phc, err := handler.config.Passwords.Hash(*body.NewPassword)
		if err != nil {
			handler.passwordError(w, r, err)
			return
		}
		command.PasswordPHC = &phc
	}
	value, err := handler.config.Auth.UpdateUser(r.Context(), command)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, managedUserDTO(value))
}

func parseManagedGrants(items []generated.ProjectGrant) ([]control.ProjectGrant, bool) {
	result := make([]control.ProjectGrant, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		id, ok := parsePositiveID(string(item.ProjectId))
		if !ok || !item.Role.Valid() || seen[string(item.ProjectId)] {
			return nil, false
		}
		seen[string(item.ProjectId)] = true
		result = append(result, control.ProjectGrant{ProjectID: id, Role: string(item.Role)})
	}
	return result, true
}

func managedUserDTO(value control.ManagedUser) generated.User {
	result := generated.User{UserId: strconv.FormatInt(value.UserID, 10), Email: value.Email, State: generated.UserState(value.State), Revision: strconv.FormatInt(value.Revision, 10), ProjectGrants: make([]generated.ProjectGrant, 0, len(value.ProjectGrants))}
	if value.Role != nil {
		role := generated.UserRole(*value.Role)
		result.Role = &role
	}
	for _, grant := range value.ProjectGrants {
		result.ProjectGrants = append(result.ProjectGrants, generated.ProjectGrant{ProjectId: strconv.FormatInt(grant.ProjectID, 10), Role: generated.ProjectGrantRole(grant.Role)})
	}
	return result
}

func (handler *ManagementHandler) getSystem(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, false)
	if !ok {
		return
	}
	tenantID, valid := parsePositiveID(r.URL.Query().Get("tenant_id"))
	if !valid {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	status, err := handler.config.Auth.ReadSystemStatus(r.Context(), tenantID, principal.UserID)
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	endUS := status.NowUS + 1
	outcomes, err := handler.config.Auth.ListSDKOutcomePage(r.Context(), control.SDKOutcomePageCommand{TenantID: tenantID, ActorUserID: principal.UserID, StartUS: status.Counters.Since.UnixMicro(), EndUS: endUS, Limit: 100})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	var nextCursor string
	if len(outcomes) > 100 {
		outcomes = outcomes[:100]
		last := outcomes[len(outcomes)-1]
		nextCursor, err = handler.encodeOperatorCursor(operatorCursor{Purpose: "sdk-outcomes", TenantID: tenantID, UserID: principal.UserID, StartUS: status.Counters.Since.UnixMicro(), EndUS: endUS, Category: last.CategorySHA256, Reason: last.ReasonSHA256})
		if err != nil {
			handler.error(w, r, http.StatusServiceUnavailable, "dependency_unavailable", true)
			return
		}
	}
	handler.json(w, http.StatusOK, handler.systemDTO(r.Context(), status, outcomes, nextCursor))
}

func (handler *ManagementHandler) updateRetention(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := handler.authenticate(w, r, true)
	if !ok {
		return
	}
	object, ok := handler.decodeJSON(w, r, "tenant_id", "revision", "retention_days")
	if !ok {
		return
	}
	var body generated.RetentionPatch
	if !hasNonNullFields(object, "tenant_id", "revision", "retention_days") || !decodeKnownObject(object, &body) {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	tenantID, tok := parsePositiveID(string(body.TenantId))
	revision, rok := parsePositiveID(string(body.Revision))
	if !tok || !rok || body.RetentionDays < 1 || body.RetentionDays > 3650 {
		handler.error(w, r, http.StatusBadRequest, "invalid_input", false)
		return
	}
	status, err := handler.config.Auth.ChangeRetentionPolicyAuthorized(r.Context(), control.ChangeRetentionCommand{TenantID: tenantID, ActorUserID: principal.UserID, ExpectedRevision: revision, Days: body.RetentionDays, RequestID: requestID(r), AuditID: newUUID()})
	if err != nil {
		handler.authError(w, r, err)
		return
	}
	handler.json(w, http.StatusOK, generated.Retention{Days: status.RetentionDays, Revision: strconv.FormatInt(status.RetentionRevision, 10), FloorUs: strconv.FormatInt(status.RetentionFloorUS, 10)})
}

func (handler *ManagementHandler) systemDTO(ctx context.Context, status control.SystemStatus, outcomes []control.SDKOutcome, nextCursor string) generated.System {
	result := generated.System{Generation: strconv.FormatInt(status.Generation, 10), RecoveryState: generated.SystemRecoveryState(status.RecoveryState), AlertsPaused: status.AlertsPaused, Retention: generated.Retention{Days: status.RetentionDays, Revision: strconv.FormatInt(status.RetentionRevision, 10), FloorUs: strconv.FormatInt(status.RetentionFloorUS, 10)}, Lanes: make([]generated.LaneStatus, 0, len(status.Lanes)), Resources: []generated.Resource{}, Dependencies: []generated.DependencyStatus{{Name: "postgresql", Status: generated.Healthy}}, Backup: generated.BackupStatus{State: status.Backup.State}}
	for _, lane := range status.Lanes {
		dto := generated.LaneStatus{LaneId: lane.LaneID, AcceptedSeq: strconv.FormatInt(lane.AcceptedSeq, 10), PublishedSeq: strconv.FormatInt(lane.PublishedSeq, 10), ErrorCode: lane.ErrorCode}
		if lane.OldestPendingReceivedUS != nil {
			value := strconv.FormatInt(*lane.OldestPendingReceivedUS, 10)
			dto.OldestPendingReceivedUs = &value
		}
		result.Lanes = append(result.Lanes, dto)
	}
	if status.Backup.LastSuccessAt != nil {
		result.Backup.LastSuccessAt = status.Backup.LastSuccessAt
	}
	if status.Backup.LastRestoreAt != nil {
		result.Backup.LastRestoreAt = status.Backup.LastRestoreAt
	}
	if status.Backup.WALAgeSeconds != nil {
		value := strconv.FormatInt(*status.Backup.WALAgeSeconds, 10)
		result.Backup.WalAgeSeconds = &value
	}
	if handler.config.Resources != nil {
		for _, item := range handler.config.Resources() {
			result.Resources = append(result.Resources, generated.Resource{Name: item.Name, Unit: generated.ResourceUnit(item.Unit), Used: strconv.FormatInt(item.Used, 10), Max: strconv.FormatInt(item.Max, 10)})
		}
	}
	storageState := generated.Unavailable
	if handler.config.StorageHealth != nil {
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := handler.config.StorageHealth(checkCtx)
		cancel()
		if err == nil {
			storageState = generated.Healthy
		} else {
			storageState = generated.Degraded
		}
	}
	result.Dependencies = append(result.Dependencies, generated.DependencyStatus{Name: "object_storage", Status: storageState})
	since := status.Counters.Since
	rejected := int64(0)
	if handler.config.RejectedRequests != nil {
		rejected = handler.config.RejectedRequests()
	}
	result.Ingest = generated.IngestCounters{Since: since, Scope: generated.IngestCountersScope("tenant"), AcceptedRequests: strconv.FormatInt(status.Counters.AcceptedRequests, 10), AcceptedRecords: strconv.FormatInt(status.Counters.AcceptedRecords, 10), DuplicateRecords: strconv.FormatInt(status.Counters.DuplicateRecords, 10), ConflictRecords: strconv.FormatInt(status.Counters.ConflictRecords, 10), PublishedRecords: strconv.FormatInt(status.Counters.PublishedRecords, 10), RejectedRequests: strconv.FormatInt(rejected, 10), RejectedSince: handler.config.StartedAt, RejectedScope: generated.IngestCountersRejectedScopeProcess}
	result.Sdk = generated.SDKCounters{Since: since, Scope: generated.SDKCountersScope("tenant"), ReportedDrops: strconv.FormatInt(status.Counters.ReportedDrops, 10), ReportedDropsApproximate: true, UnsupportedItems: strconv.FormatInt(status.Counters.UnsupportedItems, 10), ByReason: make([]generated.SDKOutcome, 0, len(outcomes))}
	for _, item := range outcomes {
		result.Sdk.ByReason = append(result.Sdk.ByReason, generated.SDKOutcome{SdkName: item.SDKName, Category: item.Category, Reason: item.Reason, Count: strconv.FormatInt(item.Count, 10), Approximate: item.Approximate})
	}
	if nextCursor != "" {
		result.Sdk.NextCursor = &nextCursor
	}
	return result
}
