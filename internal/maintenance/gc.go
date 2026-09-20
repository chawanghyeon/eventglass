package maintenance

import (
	"context"
	"errors"

	"github.com/chawanghyeon/eventglass/internal/control"
)

type GCControl interface {
	ClaimGCObjects(context.Context, int) ([]control.GCObject, error)
	ConfirmGCObjects(context.Context, []control.GCObject) error
}

type GCStore interface {
	Delete(context.Context, []string) error
}

type GCWorkflow struct {
	Control GCControl
	Store   GCStore
}

func (workflow GCWorkflow) RunOnce(ctx context.Context) (bool, error) {
	if workflow.Control == nil || workflow.Store == nil {
		return false, errors.New("invalid GC workflow")
	}
	objects, err := workflow.Control.ClaimGCObjects(ctx, 100)
	if err != nil || len(objects) == 0 {
		return false, err
	}
	keys := make([]string, len(objects))
	for index, object := range objects {
		keys[index] = object.ObjectKey
	}
	if err := workflow.Store.Delete(ctx, keys); err != nil {
		return true, err
	}
	return true, workflow.Control.ConfirmGCObjects(ctx, objects)
}
