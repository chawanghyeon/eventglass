package query

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type planningBudgetControl struct {
	plan                   control.QueryPlanContext
	loads, seals, failures int
	working                *resource.Budget
}

func (c *planningBudgetControl) LoadQueryPlanContext(context.Context, control.QueryCoordinatorAuthority) (control.QueryPlanContext, error) {
	c.loads++
	return c.plan, nil
}
func (c *planningBudgetControl) CatalogPageForQuery(context.Context, control.QueryCoordinatorAuthority, control.CatalogCommand) ([]model.CatalogFile, error) {
	return []model.CatalogFile{{FileID: "00000000-0000-4000-8000-000000000001", ObjectKey: "analytics", Bytes: 1, SHA256: "sha"}}, nil
}
func (c *planningBudgetControl) SealQueryPlan(context.Context, control.SealQueryPlanCommand) error {
	c.seals++
	if c.working.Used() != PlanningWorkingBytes {
		return errors.New("permit released before sealing")
	}
	return nil
}
func (c *planningBudgetControl) FailQueryPlanning(context.Context, control.QueryCoordinatorAuthority, string) error {
	c.failures++
	if c.working.Used() != PlanningWorkingBytes {
		return errors.New("permit released before planning cleanup")
	}
	return nil
}

func TestPlanningBudgetOwnedThroughCatalogJoinAndSeal(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "seal", true: "cancel_and_join"}[canceled], func(t *testing.T) {
			filter, err := CanonicalFilter(&Node{Op: "constant", Constant: true})
			if err != nil {
				t.Fatal(err)
			}
			digest, dataset, err := DatasetHash(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{1}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 100, Filter: filter})
			if err != nil {
				t.Fatal(err)
			}
			scope := testPlanScope()
			working := resource.NewBudget(PlanningWorkingBytes)
			controlFixture := &planningBudgetControl{working: working, plan: control.QueryPlanContext{
				Snapshot:      model.QuerySnapshot{TenantID: 1, SnapshotID: scope.SnapshotID, DatasetBytes: dataset, DatasetSHA256: digest},
				OperationHash: scope.OperationHash, Operation: scope.Operation, Deadline: time.UnixMicro(scope.DeadlineUS),
			}}
			started, finish := make(chan struct{}), make(chan struct{})
			cancellationObserved := make(chan struct{})
			releaseReader := sync.OnceFunc(func() { close(finish) })
			defer releaseReader()
			reader := catalogReaderFunc(func(ctx context.Context, _ string) (storage.ObjectInfo, error) {
				close(started)
				if canceled {
					<-ctx.Done()
					close(cancellationObserved)
				}
				<-finish
				if canceled {
					return storage.ObjectInfo{}, ctx.Err()
				}
				return storage.ObjectInfo{Size: 1, SHA256: "sha"}, nil
			})
			coordinator := Coordinator{Control: controlFixture, Objects: reader, Working: working}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				result <- coordinator.Execute(ctx, control.QueryCoordinatorAuthority{QueryID: scope.QueryID, TenantID: 1, StorageGeneration: 1})
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("catalog read did not start")
			}
			submission := Submission{Control: &control.QueryOperations{}, Objects: reader, Working: working, Generation: 1}
			if _, _, err := submission.Submit(ctx, [32]byte{}, model.QuerySnapshot{}, "search", engine.QueryOperation{}, ModeSync); !errors.Is(err, resource.ErrLimited) {
				t.Fatalf("concurrent planning bypassed shared budget: %v", err)
			}
			if canceled {
				cancel()
				<-cancellationObserved
			}
			select {
			case err := <-result:
				t.Fatalf("planning returned before catalog reader joined: %v", err)
			default:
			}
			if working.Used() != PlanningWorkingBytes {
				t.Fatal("live reader lost its permit")
			}
			releaseReader()
			err = <-result
			if (err != nil) != canceled || working.Used() != 0 {
				t.Fatalf("planning result=%v reservation=%d", err, working.Used())
			}
			if canceled && controlFixture.failures != 1 || !canceled && controlFixture.seals != 1 {
				t.Fatalf("seals=%d failures=%d", controlFixture.seals, controlFixture.failures)
			}
		})
	}
}

func TestSubmissionReleasesPlanningPermitOnInvalidDataset(t *testing.T) {
	working := resource.NewBudget(PlanningWorkingBytes)
	submission := Submission{Control: &control.QueryOperations{}, Objects: catalogHead{}, Working: working, Generation: 1}
	if _, _, err := submission.Submit(context.Background(), [32]byte{}, model.QuerySnapshot{}, "search", engine.QueryOperation{}, ModeSync); err == nil {
		t.Fatal("invalid dataset accepted")
	}
	if working.Used() != 0 {
		t.Fatal("planning error leaked its reservation")
	}
}
