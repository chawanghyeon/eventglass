package app

import (
	"context"
	"reflect"
	"testing"
)

func TestNativeWorkerOperationsBoundQueryBurst(t *testing.T) {
	runtime := &Runtime{}
	var progressed []string
	conversionCalls := 0
	conversion := func(context.Context) (bool, error) {
		conversionCalls++
		progressed = append(progressed, "conversion")
		return true, nil
	}
	query := func(context.Context) (bool, error) {
		progressed = append(progressed, "query")
		return true, nil
	}
	work := runtime.nativeWorkerOperations()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for index := range work {
		switch work[index].name {
		case "conversion":
			work[index].run = func(ctx context.Context) (bool, error) {
				completed, err := conversion(ctx)
				if conversionCalls == 2 {
					cancel()
				}
				return completed, err
			}
		case "query":
			work[index].run = query
		default:
			work[index].run = func(context.Context) (bool, error) { return false, nil }
		}
	}
	if err := runtime.runWorkerGroup(ctx, work); err != nil {
		t.Fatal(err)
	}
	want := []string{"conversion", "query", "query", "query", "query", "query", "query", "query", "query", "conversion"}
	if !reflect.DeepEqual(progressed, want) {
		t.Fatalf("progressed=%v want=%v", progressed, want)
	}
}
