package app

import "time"

const (
	maintenanceWindow = time.Minute
	// Leave time for canceled native work to exit and join. Unused allowance
	// is not charged; a slow join is charged in full, never hidden by timeout.
	maintenanceJoinAllowance = nativeChildStopGrace
	maintenanceMinWorkSlice  = 200 * time.Millisecond
)

// maintenanceTimeBudget is owned by the worker dispatch goroutine. Only an
// actually idle foreground sweep earns credit, never a process's startup age,
// a failed database claim, or time spent waiting for another native child.
// Maintenance may consume at most one quarter of observed idle time: M <= I/4
// is equivalent to M <= 20% of spare lane time (I+M).
//
// Keep 61 fixed one-second buckets. Discard partial oldest idle buckets, but
// charge partial oldest maintenance buckets in full. Aging credit or a slow
// cancellation join can leave debt; that debt denies subsequent admission.
type maintenanceTimeBudget struct {
	origin  time.Time
	buckets [61]maintenanceTimeBucket
}

type maintenanceTimeBucket struct {
	second     int64
	idle, work time.Duration
}

func (budget *maintenanceTimeBudget) recordIdleWait(start, end time.Time) time.Time {
	// An overslept timer is not proof that foreground work stayed absent while
	// the process was descheduled/throttled. Credit at most the requested wait.
	if plannedEnd := start.Add(workerIdle); end.After(plannedEnd) {
		end = plannedEnd
	}
	budget.record(start, end, false)
	return end
}

func (budget *maintenanceTimeBudget) record(start, end time.Time, maintenance bool) {
	if !end.After(start) || end.Before(budget.origin) {
		return
	}
	start = maxTime(start, maxTime(budget.origin, end.Add(-maintenanceWindow)))
	for start.Before(end) {
		second := int64(start.Sub(budget.origin) / time.Second)
		boundary := budget.origin.Add(time.Duration(second+1) * time.Second)
		stop := end
		if boundary.Before(stop) {
			stop = boundary
		}
		bucket := &budget.buckets[second%int64(len(budget.buckets))]
		if bucket.second != second {
			*bucket = maintenanceTimeBucket{second: second}
		}
		if maintenance {
			bucket.work += stop.Sub(start)
		} else {
			bucket.idle += stop.Sub(start)
		}
		start = stop
	}
}

func (budget *maintenanceTimeBudget) totals(now, idleHorizon time.Time) (idle, work time.Duration) {
	for _, bucket := range budget.buckets {
		start := budget.origin.Add(time.Duration(bucket.second) * time.Second)
		if start.After(now) {
			continue
		}
		if !start.Before(idleHorizon.Add(-maintenanceWindow)) {
			idle += bucket.idle
		}
		if start.Add(time.Second).After(now.Add(-maintenanceWindow)) {
			work += bucket.work
		}
	}
	return idle, work
}

func (budget *maintenanceTimeBudget) allowance(now time.Time) time.Duration {
	if now.Before(budget.origin) {
		return 0
	}
	idle, work := budget.totals(now, now)
	upper := min(idle/4-work, maintenanceWindow/5)
	if upper <= 0 {
		return 0
	}
	// Do not grant credit that expires while the admitted task is running.
	// Retain all current work debits in this forecast, even if they would age
	// out, so feasibility decreases monotonically with the requested slice.
	low, high := int64(0), int64(upper/time.Microsecond)
	for low < high {
		middle := low + (high-low+1)/2
		duration := time.Duration(middle) * time.Microsecond
		futureIdle, _ := budget.totals(now, now.Add(duration))
		if futureIdle/4-work >= duration {
			low = middle
		} else {
			high = middle - 1
		}
	}
	return time.Duration(low) * time.Microsecond
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}
