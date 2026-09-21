package app

import (
	"testing"
	"time"
)

func TestMaintenanceBudgetDoesNotCreditOversleptIdleWait(t *testing.T) {
	start := time.Unix(1_000, 0)
	budget := maintenanceTimeBudget{origin: start}
	end := budget.recordIdleWait(start, start.Add(30*time.Second))
	if !end.Equal(start.Add(workerIdle)) {
		t.Fatalf("credited descheduling as idle: %s", end.Sub(start))
	}
	if got := budget.allowance(start.Add(30 * time.Second)); got != workerIdle/4 {
		t.Fatalf("overslept wait earned %s, want %s", got, workerIdle/4)
	}
}

func TestMaintenanceBudgetStartsEmptyAndChargesActualJoinedTime(t *testing.T) {
	start := time.Unix(1_000, 0)
	budget := maintenanceTimeBudget{origin: start}
	if got := budget.allowance(start.Add(time.Hour)); got != 0 {
		t.Fatalf("unobserved process age earned credit: %s", got)
	}
	budget.record(start, start.Add(40*time.Second), false)
	if got := budget.allowance(start.Add(40 * time.Second)); got != 10*time.Second {
		t.Fatalf("40s idle allowance=%s, want 10s", got)
	}
	// Charge both successful and failed work, including time after its deadline
	// until it actually joins. Completion status cannot refund time already used.
	budget.record(start.Add(40*time.Second), start.Add(43*time.Second), true)
	if got := budget.allowance(start.Add(43 * time.Second)); got != 7*time.Second {
		t.Fatalf("joined attempt did not consume actual time: %s", got)
	}
	budget.record(start.Add(43*time.Second), start.Add(52*time.Second), true)
	if got := budget.allowance(start.Add(52 * time.Second)); got != 0 {
		t.Fatalf("cancellation overrun/debt admitted more work: %s", got)
	}
}

func TestMaintenanceBudgetExpiresCreditBeforeGrantingIt(t *testing.T) {
	start := time.Unix(1_000, 0)
	budget := maintenanceTimeBudget{origin: start}
	budget.record(start, start.Add(4*time.Second), false)
	if got := budget.allowance(start.Add(59 * time.Second)); got != time.Second {
		t.Fatalf("credit valid through task end was lost: %s", got)
	}
	// At 60s, any positive grant must discard the oldest idle bucket. A full
	// second would spend credit already expired at its own completion time.
	if got := budget.allowance(start.Add(60 * time.Second)); got != 750*time.Millisecond {
		t.Fatalf("grant spent soon-expiring credit: %s", got)
	}
	if got := budget.allowance(start.Add(64 * time.Second)); got != 0 {
		t.Fatalf("expired credit remained usable: %s", got)
	}
	if got := budget.allowance(start.Add(-time.Second)); got != 0 {
		t.Fatalf("backward clock earned credit: %s", got)
	}
}

func TestMaintenanceBudgetConservativelyHandlesPartialBucketsAndLongJoins(t *testing.T) {
	start := time.Unix(1_000, 0)
	budget := maintenanceTimeBudget{origin: start}
	budget.record(start.Add(500*time.Millisecond), start.Add(10*time.Second), false)
	idle, _ := budget.totals(start.Add(60*time.Second+time.Nanosecond), start.Add(60*time.Second+time.Nanosecond))
	if idle != 9*time.Second {
		t.Fatalf("partial oldest idle bucket credited: %s", idle)
	}
	budget.record(start.Add(10*time.Second), start.Add(130*time.Second), true)
	if got := budget.allowance(start.Add(130 * time.Second)); got != 0 {
		t.Fatalf("long blocked join forgot its debit: %s", got)
	}
	_, work := budget.totals(start.Add(130*time.Second), start.Add(130*time.Second))
	if work != maintenanceWindow {
		t.Fatalf("last minute of long join=%s, want %s", work, maintenanceWindow)
	}
	budget.record(start.Add(130*time.Second), start.Add(230*time.Second), false)
	if got := budget.allowance(start.Add(230 * time.Second)); got != 12*time.Second {
		t.Fatalf("bounded ring did not recover after real idle: %s", got)
	}
}
