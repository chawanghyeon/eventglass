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

func TestNativeWorkerDoesNotRepeatEmptyQueryClaimWithinSweep(t *testing.T) {
	runtime := &Runtime{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conversions, queries := 0, 0
	work := runtime.nativeWorkerOperations()
	for index := range work {
		switch work[index].name {
		case "conversion":
			work[index].run = func(context.Context) (bool, error) {
				conversions++
				if conversions == 2 {
					cancel()
				}
				return true, nil
			}
		case "query":
			work[index].run = func(context.Context) (bool, error) {
				queries++
				return false, nil
			}
		default:
			work[index].run = func(context.Context) (bool, error) { return false, nil }
		}
	}
	if err := runtime.runWorkerGroup(ctx, work); err != nil {
		t.Fatal(err)
	}
	if queries != 1 {
		t.Fatalf("empty query claims between conversions=%d, want 1", queries)
	}
}

func TestWorkerSweepStopsClaimsAfterCancellation(t *testing.T) {
	runtime := &Runtime{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work := []workerOperation{
		{"first", func(context.Context) (bool, error) { cancel(); return false, nil }},
		{"second", func(context.Context) (bool, error) { t.Fatal("claimed after cancellation"); return false, nil }},
	}
	if err := runtime.runWorkerGroup(ctx, work); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRechecksEmptyClaimAfterOtherProgress(t *testing.T) {
	runtime := &Runtime{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, calls := false, 0
	claim := func(context.Context) (bool, error) {
		calls++
		if ready {
			cancel()
			return true, nil
		}
		return false, nil
	}
	work := []workerOperation{
		{"query", claim}, {"query", claim},
		{"plan", func(context.Context) (bool, error) { ready = true; return true, nil }},
	}
	if err := runtime.runWorkerGroup(ctx, work); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("query calls=%d, want empty claim then ready claim", calls)
	}
}

func TestNativeWorkerDoesNotAdmitMaintenanceWithoutMeasuredSpareTime(t *testing.T) {
	runtime := &Runtime{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conversions, maintenance := 0, 0
	work := runtime.nativeWorkerOperations()
	for index := range work {
		switch work[index].name {
		case "conversion":
			work[index].run = func(context.Context) (bool, error) {
				conversions++
				if conversions == 3 {
					cancel()
				}
				return true, nil
			}
		case "retention", "compaction":
			work[index].run = func(context.Context) (bool, error) {
				maintenance++
				return true, nil
			}
		default:
			work[index].run = func(context.Context) (bool, error) { return false, nil }
		}
	}
	if err := runtime.runWorkerGroup(ctx, work); err != nil {
		t.Fatal(err)
	}
	if maintenance != 0 {
		t.Fatalf("maintenance attempts=%d without any observed idle time", maintenance)
	}
}
