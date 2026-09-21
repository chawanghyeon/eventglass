package api

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/google/uuid"
)

const (
	liveLookback      = 15 * time.Minute
	livePollInterval  = time.Second
	liveHeartbeat     = 15 * time.Second
	liveTokenLifetime = 15 * time.Minute
	liveTokenRefresh  = 10 * time.Minute
)

type liveRowsData struct {
	Rows []generated.ListRow `json:"rows"`
}

type liveCheckpointData struct {
	ResumeToken string `json:"resume_token"`
}

type liveControlData struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

type livePage struct {
	Rows      []generated.ListRow
	Positions []query.LiveRowPosition
	Complete  bool
}

var errLivePendingLimit = errors.New("live event exceeds pending byte limit")

func (service *QueryAdapter) Live(ctx context.Context, principal control.SessionPrincipal, tokenHash [32]byte, request query.PublicLiveRequest, resume string, emit func(query.LiveEvent) error) error {
	if err := service.validate(); err != nil {
		return err
	}
	if emit == nil {
		return errors.New("live query execution is unavailable")
	}
	if err := validateLiveRequest(request, time.Now()); err != nil {
		return errors.Join(ErrPublicQueryInvalid, err)
	}
	scopeHash, err := query.LiveScopeHash(query.LiveScope{TenantID: request.TenantID, Projects: request.ProjectIDs, Kinds: request.Kinds, Filter: request.Canonical})
	if err != nil {
		return errors.Join(ErrPublicQueryInvalid, err)
	}
	principalHash := query.PrincipalHash(principal.UserID, tokenHash)

	positions, initialized, streamStarted, err := service.livePreflight(ctx, tokenHash, &request, resume, scopeHash, principalHash, emit)
	if err != nil || !initialized {
		return err
	}
	lastEmission := time.Now()
	lastCheckpoint := time.Time{}
	if streamStarted {
		lastEmission = time.Now()
		lastCheckpoint = lastEmission
	}
	var budget query.LiveBudget
	ticker := time.NewTimer(livePollInterval)
	defer ticker.Stop()
	var drainCuts *[model.LaneCount]int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			before := positions
			page, cuts, snapshotID, pollErr := service.pollLive(ctx, tokenHash, request, positions, now, drainCuts)
			if pollErr != nil {
				if errors.Is(pollErr, context.Canceled) || errors.Is(pollErr, context.DeadlineExceeded) {
					return pollErr
				}
				if !streamStarted {
					return pollErr
				}
				if errors.Is(pollErr, control.ErrForbidden) || errors.Is(pollErr, control.ErrUnauthenticated) {
					return emit(query.LiveEvent{Type: "error", Data: liveControlData{Code: "forbidden", Retryable: false}})
				}
				if errors.Is(pollErr, control.ErrStorageGeneration) || errors.Is(pollErr, query.ErrTokenGenerationChanged) || errors.Is(pollErr, query.ErrTokenExpired) {
					return emit(query.LiveEvent{Type: "resync_required", Data: liveControlData{Code: "checkpoint_expired", Retryable: false}})
				}
				return emit(query.LiveEvent{Type: "error", Data: liveControlData{Code: "dependency_unavailable", Retryable: true}})
			}
			if code := budget.Observe(now, len(page.Rows), page.Complete); code != "" {
				_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
				return emit(query.LiveEvent{Type: "resync_required", Data: liveControlData{Code: code, Retryable: false}})
			}
			positions, err = query.AdvanceLivePositions(positions, cuts, page.Positions, page.Complete)
			if err != nil {
				_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
				return err
			}
			changed := positions != before
			if len(page.Rows) > 0 {
				token, signErr := service.signLiveCheckpoint(principalHash, scopeHash, positions, now, request)
				if signErr != nil {
					_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
					return signErr
				}
				if err := emitBoundedLive(emit, query.LiveEvent{Type: "rows", ID: token, Data: liveRowsData{Rows: page.Rows}}); err != nil {
					_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
					if errors.Is(err, errLivePendingLimit) {
						return emit(query.LiveEvent{Type: "resync_required", Data: liveControlData{Code: "slow_client", Retryable: false}})
					}
					return err
				}
				lastEmission = now
				streamStarted = true
				lastCheckpoint = now
			}
			if page.Complete && (changed || !streamStarted) {
				token, signErr := service.signLiveCheckpoint(principalHash, scopeHash, positions, now, request)
				if signErr != nil {
					_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
					return signErr
				}
				if err := emitBoundedLive(emit, query.LiveEvent{Type: "checkpoint", ID: token, Data: liveCheckpointData{ResumeToken: token}}); err != nil {
					_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
					return err
				}
				lastEmission = now
				streamStarted = true
				lastCheckpoint = now
			}
			_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshotID)
			if page.Complete {
				drainCuts = nil
				ticker.Reset(livePollInterval)
			} else {
				if drainCuts == nil {
					drainCuts = &cuts
				}
				ticker.Reset(time.Nanosecond)
			}
			if streamStarted && now.Sub(lastCheckpoint) >= liveTokenRefresh {
				token, signErr := service.signLiveCheckpoint(principalHash, scopeHash, positions, now, request)
				if signErr != nil {
					return signErr
				}
				if err := emitBoundedLive(emit, query.LiveEvent{Type: "checkpoint", ID: token, Data: liveCheckpointData{ResumeToken: token}}); err != nil {
					return err
				}
				lastEmission, lastCheckpoint = now, now
			}
			if now.Sub(lastEmission) >= liveHeartbeat {
				if err := emitBoundedLive(emit, query.LiveEvent{Type: "heartbeat", Data: struct{}{}}); err != nil {
					return err
				}
				lastEmission = now
				streamStarted = true
			}
		}
	}
}

func validateLiveRequest(request query.PublicLiveRequest, now time.Time) error {
	if request.TenantID <= 0 || len(request.ProjectIDs) < 1 || len(request.ProjectIDs) > 100 || len(request.Kinds) < 1 || len(request.Canonical) == 0 || request.Filter == nil {
		return errors.New("invalid live scope")
	}
	if request.CatchupStart != nil && (request.CatchupStart.After(now) || request.CatchupStart.Before(now.Add(-liveLookback))) {
		return errors.New("live catchup is outside the allowed window")
	}
	return nil
}

func (service *QueryAdapter) livePreflight(ctx context.Context, tokenHash [32]byte, request *query.PublicLiveRequest, resume, scopeHash, principalHash string, emit func(query.LiveEvent) error) ([model.LaneCount]query.LivePosition, bool, bool, error) {
	now := time.Now()
	snapshot, err := service.createLiveSnapshot(ctx, tokenHash, *request, now)
	if err != nil {
		return [model.LaneCount]query.LivePosition{}, false, false, err
	}
	defer service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshot.SnapshotID) //nolint:errcheck
	cuts := liveSnapshotCuts(snapshot)
	if resume != "" {
		claims, verifyErr := service.Tokens.VerifyLive(resume, query.TokenExpectation{Generation: snapshot.StorageGeneration, PrincipalHash: principalHash}, scopeHash)
		if errors.Is(verifyErr, query.ErrTokenExpired) || errors.Is(verifyErr, query.ErrTokenGenerationChanged) {
			err = emitBoundedLive(emit, query.LiveEvent{Type: "resync_required", Data: liveControlData{Code: "checkpoint_expired", Retryable: false}})
			return claims.Positions, false, true, err
		}
		if verifyErr != nil {
			return claims.Positions, false, false, verifyErr
		}
		if claims.StartUS == nil {
			return claims.Positions, false, true, emit(query.LiveEvent{Type: "resync_required", Data: liveControlData{Code: "checkpoint_expired", Retryable: false}})
		}
		start := time.UnixMicro(*claims.StartUS)
		request.CatchupStart = &start
		return claims.Positions, true, false, nil
	}
	positions := query.InitialLivePositions(cuts, request.CatchupStart != nil)
	if request.CatchupStart != nil {
		return positions, true, false, nil
	}
	token, err := service.signLiveCheckpoint(principalHash, scopeHash, positions, now, *request)
	if err == nil {
		err = emitBoundedLive(emit, query.LiveEvent{Type: "checkpoint", ID: token, Data: liveCheckpointData{ResumeToken: token}})
	}
	return positions, true, true, err
}

func (service *QueryAdapter) pollLive(ctx context.Context, tokenHash [32]byte, request query.PublicLiveRequest, positions [model.LaneCount]query.LivePosition, now time.Time, target *[model.LaneCount]int64) (page livePage, cuts [model.LaneCount]int64, snapshotID string, resultErr error) {
	snapshot, err := service.createLiveSnapshot(ctx, tokenHash, request, now)
	if err != nil {
		return page, cuts, "", err
	}
	snapshotID = snapshot.SnapshotID
	defer func() {
		if resultErr != nil {
			_ = service.Control.ReleaseSnapshot(context.WithoutCancel(ctx), tokenHash, request.TenantID, snapshot.SnapshotID)
		}
	}()
	cuts = liveSnapshotCuts(snapshot)
	if target != nil {
		cuts = *target
	}
	if positions == query.InitialLivePositions(cuts, false) {
		return livePage{Complete: true}, cuts, snapshotID, nil
	}
	scope := snapshotScope(snapshot)
	scope.LaneCuts = cuts
	plan, err := query.BuildPlan(liveDataset(request, now).Spec, scope, request.Filter)
	if err != nil {
		return page, cuts, snapshotID, errors.Join(ErrPublicQueryInvalid, err)
	}
	operation, err := query.BuildLiveOperation(plan, positions)
	if err != nil {
		return page, cuts, snapshotID, err
	}
	job, synchronous, err := service.createAndPlan(ctx, tokenHash, snapshot, "search", operation, query.ModeSync, query.LiveMinimumSequences(positions))
	if err != nil {
		return page, cuts, snapshotID, err
	}
	if !synchronous {
		return page, cuts, snapshotID, errors.New("live query was not admitted synchronously")
	}
	status, err := service.executeLiveQuery(ctx, tokenHash, request.TenantID, job.QueryId.String())
	if err != nil {
		return page, cuts, snapshotID, err
	}
	page, resultErr = service.finalizeLiveResult(ctx, status)
	if resultErr == nil {
		_, resultErr = service.Control.GetQueryStatus(ctx, tokenHash, request.TenantID, status.QueryID)
	}
	return page, cuts, snapshotID, resultErr
}

func (service *QueryAdapter) createLiveSnapshot(ctx context.Context, tokenHash [32]byte, request query.PublicLiveRequest, now time.Time) (model.QuerySnapshot, error) {
	dataset := liveDataset(request, now)
	return service.Control.CreateSnapshot(ctx, control.CreateSnapshotCommand{
		SnapshotID: uuid.NewString(), SessionTokenHash: tokenHash, TenantID: request.TenantID, ProjectIDs: request.ProjectIDs,
		DatasetSHA256: dataset.SHA256, DatasetBytes: dataset.EncodedBytes,
	})
}

func liveDataset(request query.PublicLiveRequest, now time.Time) query.PublicDataset {
	start := int64(math.MinInt64)
	if request.CatchupStart != nil {
		start = request.CatchupStart.UnixMicro()
	}
	spec := model.DatasetSpec{TenantID: request.TenantID, ProjectIDs: append([]int64(nil), request.ProjectIDs...), Kinds: append([]model.Kind(nil), request.Kinds...), TimeBasis: model.QueryTimeReceived, StartUS: start, EndUS: math.MaxInt64, Filter: append([]byte(nil), request.Canonical...)}
	digest, encoded, _ := query.DatasetHash(spec)
	return query.PublicDataset{Spec: spec, Filter: request.Filter, SHA256: digest, EncodedBytes: encoded}
}

func liveSnapshotCuts(snapshot model.QuerySnapshot) [model.LaneCount]int64 {
	var cuts [model.LaneCount]int64
	for _, lane := range snapshot.Lanes {
		cuts[lane.LaneID] = lane.CutSeq
	}
	return cuts
}

func (service *QueryAdapter) executeLiveQuery(ctx context.Context, tokenHash [32]byte, tenantID int64, queryID string) (control.QueryStatus, error) {
	syncContext, cancel := context.WithTimeout(ctx, control.QuerySyncTimeout)
	defer cancel()
	if err := service.executor().Execute(syncContext, tokenHash, tenantID, queryID); err != nil {
		_ = service.Control.CancelQuery(context.WithoutCancel(ctx), tokenHash, tenantID, queryID)
		return control.QueryStatus{}, err
	}
	status, err := service.Control.GetQueryStatus(ctx, tokenHash, tenantID, queryID)
	if err != nil {
		return status, publicStatusError(err)
	}
	if status.State != "succeeded" {
		return status, control.ErrQueryTerminal
	}
	return status, nil
}

func (service *QueryAdapter) finalizeLiveResult(ctx context.Context, status control.QueryStatus) (livePage, error) {
	if status.Result == nil {
		return livePage{}, errors.New("live result artifact is unavailable")
	}
	var operation engine.QueryOperation
	if err := strictResultJSON(status.Operation, &operation); err != nil || operation.Version != engine.QueryExecutionProtocolVersion || operation.Kind != "live" || operation.Result.Kind != "live" || operation.Result.Limit != query.LivePageRows {
		return livePage{}, errors.Join(engine.ErrQueryExecutionInvalid, err)
	}
	if err := ensureResultDirectory(service.ScratchDir); err != nil {
		return livePage{}, err
	}
	directory, err := os.MkdirTemp(service.ScratchDir, "live-")
	if err != nil {
		return livePage{}, err
	}
	defer os.RemoveAll(directory)
	parquetPath, jsonPath := filepath.Join(directory, "result.parquet"), filepath.Join(directory, "result.jsonl")
	if err := service.Store.DownloadToFile(ctx, status.Result.ObjectKey, parquetPath, status.Result.Bytes, status.Result.SHA256, engine.MaxQueryOutputBytes); err != nil {
		return livePage{}, err
	}
	if _, err := service.Exporter.Export(ctx, engine.QueryExportRequest{Version: engine.QueryExecutionProtocolVersion, InputPath: parquetPath, OutputPath: jsonPath}); err != nil {
		return livePage{}, err
	}
	lines, closeLines, err := openJSONLines(jsonPath)
	if err != nil {
		return livePage{}, err
	}
	defer closeLines()
	page := livePage{Rows: make([]generated.ListRow, 0, query.LivePageRows), Positions: make([]query.LiveRowPosition, 0, query.LivePageRows), Complete: true}
	seen := 0
	for lines.Scan() {
		seen++
		if seen > query.LivePageRows+1 {
			return livePage{}, query.ErrQueryLimit
		}
		object, err := decodeJSONLine(lines.Bytes())
		if err != nil {
			return livePage{}, err
		}
		row, tuple, err := decodeListRow(object, "received_desc")
		if err != nil || tuple.LaneID == nil || tuple.BatchSeq == nil || tuple.RecordOrdinal == nil {
			return livePage{}, errors.Join(engine.ErrQueryExecutionInvalid, err)
		}
		if len(page.Rows) == query.LivePageRows {
			page.Complete = false
			continue
		}
		page.Rows = append(page.Rows, row)
		page.Positions = append(page.Positions, query.LiveRowPosition{LaneID: *tuple.LaneID, BatchSeq: *tuple.BatchSeq, RecordOrdinal: *tuple.RecordOrdinal, RecordID: tuple.RecordID})
	}
	if err := lines.Err(); err != nil {
		return livePage{}, err
	}
	return page, nil
}

func (service *QueryAdapter) signLiveCheckpoint(principalHash, scopeHash string, positions [model.LaneCount]query.LivePosition, now time.Time, request query.PublicLiveRequest) (string, error) {
	return service.Tokens.SignLiveFrom(service.StorageGeneration, principalHash, scopeHash, positions, now.Add(liveTokenLifetime).UnixMicro(), liveDataset(request, now).Spec.StartUS)
}

func emitBoundedLive(emit func(query.LiveEvent) error, event query.LiveEvent) error {
	encoded, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}
	// Include the ID and framing, not just JSON, in the pending-byte budget.
	if len(encoded)+len(event.ID)+len(event.Type)+32 > query.LiveMaximumPending {
		return errLivePendingLimit
	}
	return emit(event)
}
