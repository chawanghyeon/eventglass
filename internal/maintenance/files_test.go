package maintenance

import (
	"errors"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestMaintenanceDiskCannotUseIndependentSpillAllowance(t *testing.T) {
	budget := resource.NewBudget(engine.DefaultNativeSpillBytes)
	if _, err := reserveDisk(budget, control.CompactionWork{}); !errors.Is(err, resource.ErrLimited) {
		t.Fatalf("overcommitted shared disk: %v", err)
	}
	if budget.Used() != 0 {
		t.Fatal("failed reservation leaked")
	}
	budget = resource.NewBudget(4 << 30)
	permit, err := reserveDisk(budget, control.CompactionWork{})
	if err != nil {
		t.Fatal(err)
	}
	if budget.Used() != engine.DefaultNativeSpillBytes+2*engine.MaxBundleFileBytes {
		t.Fatal("missing output/spill budget")
	}
	permit.Release()
	if budget.Used() != 0 {
		t.Fatal("reservation leaked")
	}
}
