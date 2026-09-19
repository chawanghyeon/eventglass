package app

import (
	"context"
	"errors"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

type queryPlanningControl interface {
	LoadQueryPlanContext(context.Context, control.QueryCoordinatorAuthority) (control.QueryPlanContext, error)
	CatalogPageForQuery(context.Context, control.QueryCoordinatorAuthority, control.CatalogCommand) ([]model.CatalogFile, error)
	SealQueryPlan(context.Context, control.SealQueryPlanCommand) error
	FailQueryPlanning(context.Context, control.QueryCoordinatorAuthority, string) error
}

type DurableQueryCoordinator struct {
	Control queryPlanningControl
	Objects query.CatalogObjectReader
}

func (coordinator DurableQueryCoordinator) Execute(ctx context.Context, authority control.QueryCoordinatorAuthority) error {
	if coordinator.Control == nil || coordinator.Objects == nil {
		return errors.New("query coordinator dependencies are required")
	}
	planContext, err := coordinator.Control.LoadQueryPlanContext(ctx, authority)
	if err != nil {
		return err
	}
	dataset, err := query.DecodeDatasetIdentity(planContext.Snapshot.DatasetBytes)
	if err != nil {
		return coordinator.fail(ctx, authority, err)
	}
	pager := durableCatalogPager{control: coordinator.Control, authority: authority}
	files, err := query.LoadVerifiedCatalog(ctx, pager, coordinator.Objects, control.CatalogCommand{
		TenantID: authority.TenantID, SnapshotID: planContext.Snapshot.SnapshotID,
		DatasetSHA256: planContext.Snapshot.DatasetSHA256, DatasetBytes: planContext.Snapshot.DatasetBytes,
		TimeBasis: dataset.TimeBasis, StartUS: dataset.StartUS, EndUS: dataset.EndUS, Kinds: dataset.Kinds,
	})
	if err != nil {
		return coordinator.fail(ctx, authority, err)
	}
	execution, err := query.BuildExecutionPlan(query.PlanScope{
		QueryID: authority.QueryID, TenantID: authority.TenantID, SnapshotID: planContext.Snapshot.SnapshotID,
		Generation: authority.StorageGeneration, OperationHash: planContext.OperationHash,
		Operation: planContext.Operation, DeadlineUS: planContext.Deadline.UnixMicro(),
	}, files)
	if err != nil {
		return coordinator.fail(ctx, authority, err)
	}
	return coordinator.Control.SealQueryPlan(ctx, control.SealQueryPlanCommand{
		Authority: authority, PlanSHA256: execution.SHA256, Tasks: execution.Tasks,
		ScanCount: execution.ScanCount, ManifestBytes: execution.ManifestBytes,
	})
}

func (coordinator DurableQueryCoordinator) fail(ctx context.Context, authority control.QueryCoordinatorAuthority, cause error) error {
	code := "query_planning_failed"
	if errors.Is(cause, query.ErrQueryLimit) || errors.Is(cause, query.ErrCatalogLimit) {
		code = "query_limit_exceeded"
	}
	return errors.Join(cause, coordinator.Control.FailQueryPlanning(ctx, authority, code))
}

type durableCatalogPager struct {
	control   queryPlanningControl
	authority control.QueryCoordinatorAuthority
}

func (pager durableCatalogPager) CatalogPage(ctx context.Context, command control.CatalogCommand) ([]model.CatalogFile, error) {
	return pager.control.CatalogPageForQuery(ctx, pager.authority, command)
}
