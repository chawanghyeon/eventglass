package app

import (
	"errors"

	"github.com/chawanghyeon/eventglass/internal/resource"
)

const (
	apiIngressBytes      = int64(64 << 20)
	apiWorkingBytes      = int64(256 << 20)
	combinedIngressBytes = int64(32 << 20)
	combinedWorkingBytes = int64(192 << 20)
	sharedDiskBytes      = int64((4 << 30) - (512 << 20))
)

type Resources struct {
	Ingress       *resource.Budget
	Working       *resource.Budget
	Disk          *resource.Budget
	PoolMaxConns  int32
	GoMemoryLimit int64
}

func ResourcesForRoles(roles map[Role]bool) (Resources, error) {
	if len(roles) == 0 {
		return Resources{}, errors.New("roles are required")
	}
	ingress, working := apiIngressBytes, apiWorkingBytes
	pool, memory := int32(8), int64(352<<20)
	if len(roles) > 1 {
		ingress, working = combinedIngressBytes, combinedWorkingBytes
		pool, memory = 12, 224<<20
	} else if roles[RoleWorker] {
		ingress, working = 1, 64<<20
		pool, memory = 2, 96<<20
	} else if roles[RoleScheduler] {
		ingress, working = 1, 64<<20
		pool, memory = 2, 128<<20
	}
	return Resources{
		Ingress: resource.NewBudget(ingress), Working: resource.NewBudget(working), Disk: resource.NewBudget(sharedDiskBytes),
		PoolMaxConns: pool, GoMemoryLimit: memory,
	}, nil
}
