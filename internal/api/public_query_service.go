package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/google/uuid"
)

type QueryAdapter struct {
	Control           *control.QueryOperations
	Store             PublicQueryStore
	Tokens            *query.TokenCodec
	Exporter          QueryExportRunner
	Sync              QuerySyncExecutor
	ScratchDir        string
	InstallationID    string
	StorageGeneration int64
	Working           *resource.Budget
}

type PublicQueryStore interface {
	query.CatalogObjectReader
	DownloadToFile(context.Context, string, string, int64, string, int64) error
}

func (service *QueryAdapter) Search(ctx context.Context, principal control.SessionPrincipal, tokenHash [32]byte, request query.PublicSearchRequest) (_ SearchSubmission, resultErr error) {
	if err := service.validate(); err != nil {
		return SearchSubmission{}, err
	}
	baseHash := searchCursorHash(request)
	var cursor *query.CursorTuple
	var cursorSnapshot string
	if request.Cursor != "" {
		claims, err := service.Tokens.VerifyCursor(request.Cursor, query.TokenExpectation{
			Generation: service.StorageGeneration, PrincipalHash: query.PrincipalHash(principal.UserID, tokenHash),
			DatasetHash: request.Dataset.SHA256, OperationHash: baseHash, Sort: request.Sort, Limit: request.Limit,
		})
		if err != nil {
			return SearchSubmission{}, err
		}
		cursor, cursorSnapshot = &claims.Last, claims.SnapshotID
	}
	snapshot, _, err := service.resolveSnapshot(ctx, principal, tokenHash, request.Dataset, request.ReadToken, cursorSnapshot)
	if err != nil {
		return SearchSubmission{}, err
	}
	plan, err := query.BuildPlan(request.Dataset.Spec, snapshotScope(snapshot), request.Dataset.Filter)
	defer func() {
		if resultErr != nil && request.ReadToken == "" && cursorSnapshot == "" {
			service.releaseSnapshot(ctx, tokenHash, snapshot)
		}
	}()
	if err != nil {
		return SearchSubmission{}, errors.Join(ErrPublicQueryInvalid, err)
	}
	operation, err := query.BuildRowOperation(query.RowOperationSpec{Plan: plan, Sort: request.Sort, Limit: request.Limit, Cursor: cursor})
	if err != nil {
		return SearchSubmission{}, errors.Join(ErrPublicQueryInvalid, err)
	}
	operation.Result.CursorHash = baseHash
	job, synchronous, err := service.createAndPlan(ctx, tokenHash, snapshot, "search", operation, request.Mode)
	if err != nil {
		return SearchSubmission{}, err
	}
	if synchronous {
		result, err := service.executeSynchronous(ctx, tokenHash, snapshot.TenantID, job.QueryId.String())
		if err != nil {
			return SearchSubmission{}, err
		}
		return SearchSubmission{Result: result}, nil
	}
	return SearchSubmission{Job: &job}, nil
}

func (service *QueryAdapter) Aggregate(ctx context.Context, principal control.SessionPrincipal, tokenHash [32]byte, request query.PublicAggregateRequest) (_ AggregateSubmission, resultErr error) {
	if err := service.validate(); err != nil {
		return AggregateSubmission{}, err
	}
	snapshot, _, err := service.resolveSnapshot(ctx, principal, tokenHash, request.Dataset, request.ReadToken, "")
	if err != nil {
		return AggregateSubmission{}, err
	}
	defer func() {
		if resultErr != nil && request.ReadToken == "" {
			service.releaseSnapshot(ctx, tokenHash, snapshot)
		}
	}()
	plan, err := query.BuildPlan(request.Dataset.Spec, snapshotScope(snapshot), request.Dataset.Filter)
	if err != nil {
		return AggregateSubmission{}, errors.Join(ErrPublicQueryInvalid, err)
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{
		Plan: plan, GroupBy: request.GroupBy, Metrics: request.Metrics, Histogram: request.Histogram,
		Top: request.Top, Order: request.Order,
	})
	if err != nil {
		return AggregateSubmission{}, errors.Join(ErrPublicQueryInvalid, err)
	}
	job, synchronous, err := service.createAndPlan(ctx, tokenHash, snapshot, "aggregate", operation, request.Mode)
	if err != nil {
		return AggregateSubmission{}, err
	}
	if synchronous {
		result, err := service.executeSynchronous(ctx, tokenHash, snapshot.TenantID, job.QueryId.String())
		if err != nil {
			return AggregateSubmission{}, err
		}
		return AggregateSubmission{Result: result}, nil
	}
	return AggregateSubmission{Job: &job}, nil
}

func (service *QueryAdapter) Job(ctx context.Context, _ control.SessionPrincipal, tokenHash [32]byte, tenantID int64, queryID string) (generated.QueryJob, error) {
	status, err := service.Control.GetQueryStatus(ctx, tokenHash, tenantID, queryID)
	if err != nil {
		return generated.QueryJob{}, publicStatusError(err)
	}
	result := queryJobDTO(status)
	if status.State == "succeeded" {
		final, err := service.finalizeQueryResult(ctx, tokenHash, status)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return generated.QueryJob{}, err
			}
			code := queryResultFailureCode(err)
			if failErr := service.Control.FailSucceededQueryResult(ctx, tokenHash, tenantID, queryID, code); failErr != nil {
				return generated.QueryJob{}, errors.Join(err, failErr)
			}
			status.State, status.ErrorCode = "failed", code
			return queryJobDTO(status), nil
		}
		result.Result = final
	}
	return result, nil
}

func (service *QueryAdapter) Cancel(ctx context.Context, _ control.SessionPrincipal, tokenHash [32]byte, tenantID int64, queryID string) error {
	return publicStatusError(service.Control.CancelQuery(ctx, tokenHash, tenantID, queryID))
}

func (service *QueryAdapter) RenewSnapshot(ctx context.Context, principal control.SessionPrincipal, tokenHash [32]byte, tenantID int64, snapshotID, readToken string) (string, error) {
	claims, err := service.Tokens.VerifyRead(readToken, query.TokenExpectation{Generation: service.StorageGeneration, PrincipalHash: query.PrincipalHash(principal.UserID, tokenHash)})
	if err != nil || claims.SnapshotID != snapshotID {
		return "", errors.Join(query.ErrTokenMismatch, err)
	}
	snapshot, err := service.Control.RenewSnapshot(ctx, tokenHash, tenantID, snapshotID, claims.DatasetHash)
	if err != nil {
		return "", err
	}
	return service.Tokens.SignRead(snapshot)
}

func (service *QueryAdapter) ReleaseSnapshot(ctx context.Context, _ control.SessionPrincipal, tokenHash [32]byte, tenantID int64, snapshotID string) error {
	return service.Control.ReleaseSnapshot(ctx, tokenHash, tenantID, snapshotID)
}

func (service *QueryAdapter) Record(ctx context.Context, principal control.SessionPrincipal, tokenHash [32]byte, tenantID, projectID int64, recordID, readToken string) (_ any, resultErr error) {
	if err := service.validate(); err != nil {
		return nil, err
	}
	var snapshot model.QuerySnapshot
	var dataset model.DatasetSpec
	var filter *query.Node
	if readToken != "" {
		claims, err := service.Tokens.VerifyRead(readToken, query.TokenExpectation{
			Generation: service.StorageGeneration, PrincipalHash: query.PrincipalHash(principal.UserID, tokenHash),
		})
		if err != nil {
			return nil, err
		}
		snapshot, err = service.Control.LoadSnapshot(ctx, tokenHash, tenantID, claims.SnapshotID, claims.DatasetHash)
		if err != nil {
			return nil, err
		}
		dataset, err = query.DecodeDatasetIdentity(snapshot.DatasetBytes)
		if err != nil {
			return nil, err
		}
		if dataset.TenantID != tenantID || !containsProject(dataset.ProjectIDs, projectID) {
			return nil, ErrPublicQueryNotFound
		}
		filter, err = query.DecodeCanonicalFilter(dataset.Filter)
		if err != nil {
			return nil, err
		}
	} else {
		filter = &query.Node{Op: "constant", Constant: true}
		canonical, err := query.CanonicalFilter(filter)
		if err != nil {
			return nil, err
		}
		dataset = model.DatasetSpec{
			TenantID: tenantID, ProjectIDs: []int64{projectID}, TimeBasis: model.QueryTimeReceived,
			StartUS: math.MinInt64, EndUS: math.MaxInt64, Filter: canonical,
		}
		hash, encoded, err := query.DatasetHash(dataset)
		if err != nil {
			return nil, err
		}
		snapshot, err = service.Control.CreateSnapshot(ctx, control.CreateSnapshotCommand{
			SnapshotID: uuid.NewString(), SessionTokenHash: tokenHash, TenantID: tenantID, ProjectIDs: []int64{projectID},
			DatasetSHA256: hash, DatasetBytes: encoded,
		})
		if err != nil {
			return nil, err
		}
	}
	plan, err := query.BuildPlan(dataset, snapshotScope(snapshot), filter)
	defer func() {
		if resultErr != nil && readToken == "" {
			service.releaseSnapshot(ctx, tokenHash, snapshot)
		}
	}()
	if err != nil {
		return nil, errors.Join(ErrPublicQueryInvalid, err)
	}
	operation, err := query.BuildDetailOperation(plan, recordID)
	if err != nil {
		return nil, errors.Join(ErrPublicQueryInvalid, err)
	}
	job, synchronous, err := service.createAndPlan(ctx, tokenHash, snapshot, "detail", operation, query.ModeSync)
	if err != nil {
		return nil, err
	}
	if !synchronous {
		return nil, errors.New("detail query was not admitted synchronously")
	}
	result, err := service.executeSynchronous(ctx, tokenHash, tenantID, job.QueryId.String())
	if errors.Is(err, ErrPublicQueryNotFound) && readToken == "" {
		expired, lookupErr := service.Control.RecordKnownExpired(ctx, tokenHash, tenantID, projectID, recordID)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if expired {
			return nil, ErrPublicQueryGone
		}
	}
	return result, err
}

func containsProject(projects []int64, projectID int64) bool {
	for _, candidate := range projects {
		if candidate == projectID {
			return true
		}
	}
	return false
}

func (service *QueryAdapter) releaseSnapshot(ctx context.Context, token [32]byte, snapshot model.QuerySnapshot) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = service.Control.ReleaseSnapshot(cleanup, token, snapshot.TenantID, snapshot.SnapshotID)
}

func (service *QueryAdapter) resolveSnapshot(ctx context.Context, principal control.SessionPrincipal, tokenHash [32]byte, dataset query.PublicDataset, readToken, cursorSnapshot string) (model.QuerySnapshot, string, error) {
	principalHash := query.PrincipalHash(principal.UserID, tokenHash)
	var snapshot model.QuerySnapshot
	if readToken == "" && cursorSnapshot == "" {
		var err error
		snapshot, err = service.Control.CreateSnapshot(ctx, control.CreateSnapshotCommand{
			SnapshotID: uuid.NewString(), SessionTokenHash: tokenHash, TenantID: dataset.Spec.TenantID,
			ProjectIDs: dataset.Spec.ProjectIDs, DatasetSHA256: dataset.SHA256, DatasetBytes: dataset.EncodedBytes,
		})
		if err != nil {
			return snapshot, "", err
		}
	} else {
		snapshotID := cursorSnapshot
		if readToken != "" {
			claims, err := service.Tokens.VerifyRead(readToken, query.TokenExpectation{Generation: service.StorageGeneration, PrincipalHash: principalHash, DatasetHash: dataset.SHA256})
			if err != nil {
				return snapshot, "", err
			}
			if snapshotID != "" && snapshotID != claims.SnapshotID {
				return snapshot, "", query.ErrTokenMismatch
			}
			snapshotID = claims.SnapshotID
		}
		var err error
		snapshot, err = service.Control.LoadSnapshot(ctx, tokenHash, dataset.Spec.TenantID, snapshotID, dataset.SHA256)
		if err != nil {
			return snapshot, "", err
		}
	}
	signed, err := service.Tokens.SignRead(snapshot)
	return snapshot, signed, err
}

func (service *QueryAdapter) createAndPlan(ctx context.Context, tokenHash [32]byte, snapshot model.QuerySnapshot, kind string, operation engine.QueryOperation, mode query.RequestMode, minimum ...[model.LaneCount]int64) (generated.QueryJob, bool, error) {
	submission := query.Submission{Control: service.Control, Objects: service.Store, Working: service.Working, Generation: service.StorageGeneration}
	if len(minimum) > 0 {
		submission.MinimumBatchSeq = minimum[0]
	}
	job, synchronous, err := submission.Submit(ctx, tokenHash, snapshot, kind, operation, mode)
	if err != nil {
		return generated.QueryJob{}, false, err
	}
	snapshotID := uuid.MustParse(snapshot.SnapshotID)
	return generated.QueryJob{SnapshotId: &snapshotID, QueryId: uuid.MustParse(job.Authority.QueryID), State: generated.QueryJobState(job.State), ExpiresAt: job.ExpiresAt, PollAfterMs: 500}, synchronous, nil
}

func (service *QueryAdapter) executor() QuerySyncExecutor {
	if service.Sync != nil {
		return service.Sync
	}
	return query.Awaiter{Control: service.Control}
}

func (service *QueryAdapter) executeSynchronous(ctx context.Context, tokenHash [32]byte, tenantID int64, queryID string) (any, error) {
	syncContext, cancel := context.WithTimeout(ctx, control.QuerySyncTimeout)
	defer cancel()
	if err := service.executor().Execute(syncContext, tokenHash, tenantID, queryID); err != nil {
		_ = service.Control.CancelQuery(context.WithoutCancel(ctx), tokenHash, tenantID, queryID)
		if errors.Is(err, engine.ErrQueryExecutionLimit) || errors.Is(err, query.ErrQueryLimit) {
			return nil, errors.Join(ErrPublicQueryLimit, err)
		}
		return nil, err
	}
	status, err := service.Control.GetQueryStatus(ctx, tokenHash, tenantID, queryID)
	if err != nil {
		return nil, publicStatusError(err)
	}
	if status.State != "succeeded" {
		return nil, control.ErrQueryTerminal
	}
	result, err := service.finalizeQueryResult(ctx, tokenHash, status)
	if err != nil && !errors.Is(err, ErrPublicQueryNotFound) {
		_ = service.Control.FailSucceededQueryResult(context.WithoutCancel(ctx), tokenHash, tenantID, queryID, queryResultFailureCode(err))
	}
	if errors.Is(err, engine.ErrQueryExecutionLimit) {
		return nil, errors.Join(ErrPublicQueryLimit, err)
	}
	return result, err
}

func (service *QueryAdapter) validate() error {
	if service == nil || service.Control == nil || service.Store == nil || service.Tokens == nil || service.Exporter == nil || service.Working == nil || service.ScratchDir == "" || service.InstallationID == "" || service.StorageGeneration <= 0 {
		return errors.New("public query service is incomplete")
	}
	return nil
}

func snapshotScope(snapshot model.QuerySnapshot) model.SnapshotScope {
	result := model.SnapshotScope{RetentionFloorUS: snapshot.RetentionFloorUS}
	for _, lane := range snapshot.Lanes {
		result.LaneCuts[lane.LaneID] = lane.CutSeq
	}
	return result
}

func searchCursorHash(request query.PublicSearchRequest) string {
	encoded, _ := json.Marshal(struct {
		Dataset string `json:"dataset"`
		Limit   int    `json:"limit"`
		Sort    string `json:"sort"`
	}{request.Dataset.SHA256, request.Limit, request.Sort})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func queryJobDTO(status control.QueryStatus) generated.QueryJob {
	result := generated.QueryJob{QueryId: uuid.MustParse(status.QueryID), State: generated.QueryJobState(status.State), ExpiresAt: status.ExpiresAt, PollAfterMs: 500}
	snapshotID := uuid.MustParse(status.SnapshotID)
	result.SnapshotId = &snapshotID
	if status.ErrorCode != "" {
		result.Error = &generated.Error{Code: status.ErrorCode, Message: "query failed", Retryable: false, RequestId: uuid.New()}
	}
	return result
}

func publicStatusError(err error) error {
	switch {
	case errors.Is(err, control.ErrQueryNotFound):
		return ErrPublicQueryNotFound
	case errors.Is(err, control.ErrQueryGone):
		return ErrPublicQueryGone
	default:
		return err
	}
}

func queryResultFailureCode(err error) string {
	if errors.Is(err, engine.ErrQueryExecutionLimit) || errors.Is(err, query.ErrQueryLimit) {
		return "result_limit_exceeded"
	}
	return "query_result_invalid"
}
