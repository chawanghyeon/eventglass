package query

import (
	"testing"
	"time"
)

func TestLiveBudgetBurstAndSlowBacklog(t *testing.T) {
	now := time.Now()
	var burst LiveBudget
	for range 100 {
		if code := burst.Observe(now, 100, false); code != "" {
			t.Fatal(code)
		}
	}
	if code := burst.Observe(now, 1, false); code != "rate_limit" {
		t.Fatal(code)
	}
	var slow LiveBudget
	if code := slow.Observe(now, 100, false); code != "" {
		t.Fatal(code)
	}
	if code := slow.Observe(now.Add(15*time.Minute), 1, false); code != "backlog_expired" {
		t.Fatal(code)
	}
	var complete LiveBudget
	complete.Observe(now, 100, false)
	complete.Observe(now.Add(time.Minute), 1, true)
	if code := complete.Observe(now.Add(time.Hour), 0, true); code != "" {
		t.Fatal(code)
	}
}
