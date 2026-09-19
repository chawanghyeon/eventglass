package query

import "time"

// LiveBudget measures delivered volume and incomplete-drain age independently.
// Call before emitting any page; a failure must not advance a checkpoint.
type LiveBudget struct {
	window  time.Time
	rows    int
	pending time.Time
}

func (b *LiveBudget) Observe(now time.Time, rows int, complete bool) string {
	if !b.pending.IsZero() && now.Sub(b.pending) >= 15*time.Minute {
		return "backlog_expired"
	}
	if b.window.IsZero() || now.Sub(b.window) >= 10*time.Second {
		b.window, b.rows = now, 0
	}
	if rows < 0 || b.rows+rows > LiveMaximumRows {
		return "rate_limit"
	}
	b.rows += rows
	if complete {
		b.pending = time.Time{}
	} else if b.pending.IsZero() {
		b.pending = now
	}
	return ""
}
