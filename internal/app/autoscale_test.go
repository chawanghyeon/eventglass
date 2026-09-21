package app

import (
	"testing"
	"time"
)

func TestScalerUsesTwoSamplesFreezesDependenciesAndBoundsScaleIn(t *testing.T) {
	scaler, err := NewScaler(ScaleConfig{MinWorkers: 1, MaxWorkers: 20, TargetDrain: 5 * time.Second, OldestTarget: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	large := ScaleSample{Now: now, QueuedBytes: 40 << 20, CurrentWorkers: 1}
	if got := scaler.Observe(large); got.DesiredWorkers != 1 || got.Frozen {
		t.Fatalf("first breach=%#v", got)
	}
	if got := scaler.Observe(large); got.DesiredWorkers != 8 {
		t.Fatalf("second breach=%#v", got)
	}
	large.CurrentWorkers, large.DependencyErrorRatio = 8, .21
	if got := scaler.Observe(large); got.DesiredWorkers != 8 || !got.Frozen {
		t.Fatalf("dependency freeze=%#v", got)
	}
	quiet := ScaleSample{Now: now, CurrentWorkers: 8}
	if got := scaler.Observe(quiet); got.DesiredWorkers != 8 {
		t.Fatalf("early scale in=%#v", got)
	}
	quiet.Now = now.Add(5 * time.Minute)
	if got := scaler.Observe(quiet); got.DesiredWorkers != 6 {
		t.Fatalf("25%% scale in=%#v", got)
	}
}

func TestScalerWarmMinimumEWMAOldestAndConnectionBudget(t *testing.T) {
	scaler, _ := NewScaler(ScaleConfig{MinWorkers: 1, MaxWorkers: 4, TargetDrain: 500 * time.Millisecond, OldestTarget: 500 * time.Millisecond})
	now := time.Unix(1000, 0)
	if got := scaler.Observe(ScaleSample{Now: now, CurrentWorkers: 1}); got.DesiredWorkers != 1 {
		t.Fatalf("warm minimum=%#v", got)
	}
	sample := ScaleSample{Now: now, CurrentWorkers: 1, OldestAge: time.Second, CompletedBytes: 4 << 20, Interval: time.Second}
	if got := scaler.Observe(sample); got.DesiredWorkers != 1 || got.ServiceRate <= 1<<20 {
		t.Fatalf("oldest first sample=%#v", got)
	}
	if got := scaler.Observe(sample); got.DesiredWorkers != 2 {
		t.Fatalf("oldest second sample=%#v", got)
	}
	if err := ValidateReplicaConnectionBudget(1, 20, 1); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplicaConnectionBudget(2, 20, 1); err == nil {
		t.Fatal("PG64 connection overflow accepted")
	}
}
