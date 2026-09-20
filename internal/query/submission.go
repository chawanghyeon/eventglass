package query

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
)

// Submission owns the transport-independent catalog/plan/job transition. HTTP,
// Live and future evaluators share this path rather than assembling SQL/tasks.
type Submission struct {
	Control         *control.QueryOperations
	Objects         CatalogObjectReader
	Generation      int64
	MinimumBatchSeq [model.LaneCount]int64
}

func (s Submission) Submit(ctx context.Context, token [32]byte, snapshot model.QuerySnapshot, kind string, operation engine.QueryOperation, mode RequestMode) (control.QueryJob, bool, error) {
	if s.Control == nil || s.Objects == nil || s.Generation <= 0 {
		return control.QueryJob{}, false, errors.New("query submission dependencies are required")
	}
	encoded, err := CanonicalOperation(operation)
	if err != nil {
		return control.QueryJob{}, false, err
	}
	digest := sha256.Sum256(encoded)
	hash := hex.EncodeToString(digest[:])
	dataset, err := DecodeDatasetIdentity(snapshot.DatasetBytes)
	if err != nil {
		return control.QueryJob{}, false, err
	}
	files, err := LoadVerifiedCatalog(ctx, s.Control, s.Objects, control.CatalogCommand{
		SessionTokenHash: token, TenantID: snapshot.TenantID, SnapshotID: snapshot.SnapshotID,
		DatasetSHA256: snapshot.DatasetSHA256, DatasetBytes: snapshot.DatasetBytes,
		TimeBasis: dataset.TimeBasis, StartUS: dataset.StartUS, EndUS: dataset.EndUS, Kinds: dataset.Kinds,
		MinimumBatchSeq: s.MinimumBatchSeq,
	})
	if err != nil {
		return control.QueryJob{}, false, err
	}
	var size int64
	for _, file := range files {
		if file.Bytes > 0 && size <= math.MaxInt64-file.Bytes {
			size += file.Bytes
		} else {
			size = math.MaxInt64
		}
	}
	sync := mode == ModeSync || mode == ModeAuto && len(files) <= MaxFilesPerScan && size <= TargetScanBytes
	timeout := control.QueryMaximumTimeout
	if sync {
		timeout = control.QuerySyncTimeout
	}
	job, err := s.Control.CreateQuery(ctx, control.CreateQueryCommand{
		QueryID: uuid.NewString(), SessionTokenHash: token, TenantID: snapshot.TenantID, SnapshotID: snapshot.SnapshotID,
		OperationKind: kind, OperationHash: hash, OperationBytes: encoded, Owner: uuid.NewString(), Timeout: timeout,
	})
	if err != nil {
		return job, false, err
	}
	plan, err := BuildExecutionPlan(PlanScope{QueryID: job.Authority.QueryID, TenantID: snapshot.TenantID,
		SnapshotID: snapshot.SnapshotID, Generation: s.Generation, OperationHash: hash, Operation: encoded,
		DeadlineUS: job.Deadline.UnixMicro()}, files)
	if err == nil {
		err = s.Control.SealQueryPlan(ctx, control.SealQueryPlanCommand{Authority: job.Authority, PlanSHA256: plan.SHA256,
			Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes})
	}
	if err != nil {
		code := "query_planning_failed"
		if errors.Is(err, ErrQueryLimit) || errors.Is(err, ErrCatalogLimit) {
			code = "query_limit_exceeded"
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = s.Control.FailQueryPlanning(cleanup, job.Authority, code)
		return job, false, err
	}
	job.State = "queued"
	return job, sync, nil
}

type alertCatalogPager struct {
	control *control.QueryOperations
	alertID string
}

func (p alertCatalogPager) CatalogPage(ctx context.Context, command control.CatalogCommand) ([]model.CatalogFile, error) {
	return p.control.CatalogPageForAlert(ctx, control.AlertCatalogCommand{AlertID: p.alertID, CatalogCommand: command})
}

// SubmitAlert uses the same catalog verification, partitioning, durable job,
// and plan sealing path as interactive queries, but authorizes solely through
// the live alert snapshot instead of borrowing a browser session.
func (s Submission) SubmitAlert(ctx context.Context, alertID string, snapshot model.QuerySnapshot, operation engine.QueryOperation) (control.QueryJob, error) {
	if s.Control == nil || s.Objects == nil || s.Generation <= 0 || uuid.Validate(alertID) != nil {
		return control.QueryJob{}, errors.New("alert query submission dependencies are required")
	}
	encoded, err := CanonicalOperation(operation)
	if err != nil {
		return control.QueryJob{}, err
	}
	digest := sha256.Sum256(encoded)
	hash := hex.EncodeToString(digest[:])
	dataset, err := DecodeDatasetIdentity(snapshot.DatasetBytes)
	if err != nil {
		return control.QueryJob{}, err
	}
	files, err := LoadVerifiedCatalog(ctx, alertCatalogPager{control: s.Control, alertID: alertID}, s.Objects, control.CatalogCommand{TenantID: snapshot.TenantID, SnapshotID: snapshot.SnapshotID, DatasetSHA256: snapshot.DatasetSHA256, DatasetBytes: snapshot.DatasetBytes, TimeBasis: dataset.TimeBasis, StartUS: dataset.StartUS, EndUS: dataset.EndUS, Kinds: dataset.Kinds, MinimumBatchSeq: s.MinimumBatchSeq})
	if err != nil {
		return control.QueryJob{}, err
	}
	job, err := s.Control.CreateAlertQuery(ctx, control.CreateAlertQueryCommand{QueryID: uuid.NewString(), TenantAlertID: alertID, TenantID: snapshot.TenantID, SnapshotID: snapshot.SnapshotID, OperationHash: hash, OperationBytes: encoded, Owner: uuid.NewString(), Timeout: control.QueryMaximumTimeout})
	if err != nil {
		return job, err
	}
	plan, err := BuildExecutionPlan(PlanScope{QueryID: job.Authority.QueryID, TenantID: snapshot.TenantID, SnapshotID: snapshot.SnapshotID, Generation: s.Generation, OperationHash: hash, Operation: encoded, DeadlineUS: job.Deadline.UnixMicro()}, files)
	if err == nil {
		err = s.Control.SealQueryPlan(ctx, control.SealQueryPlanCommand{Authority: job.Authority, PlanSHA256: plan.SHA256, Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes})
	}
	if err != nil {
		code := "query_planning_failed"
		if errors.Is(err, ErrQueryLimit) || errors.Is(err, ErrCatalogLimit) {
			code = "query_limit_exceeded"
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = s.Control.FailQueryPlanning(cleanup, job.Authority, code)
		return job, err
	}
	job.State = "queued"
	return job, nil
}

func CanonicalOperation(operation engine.QueryOperation) ([]byte, error) {
	encoded, err := json.Marshal(operation)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// Awaiter never executes native tasks. An API-only process can await work done
// by any worker through the authoritative job state. Context owns its lifetime.
type Awaiter struct{ Control *control.QueryOperations }

func (a Awaiter) Execute(ctx context.Context, token [32]byte, tenant int64, id string) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := a.Control.GetQueryStatus(ctx, token, tenant, id)
		if err != nil {
			return err
		}
		switch status.State {
		case "succeeded":
			return nil
		case "failed", "canceled":
			return control.ErrQueryTerminal
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
