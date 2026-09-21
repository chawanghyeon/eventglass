//go:build comparison

package comparison

import (
	"math"
	"testing"
	"time"
)

func TestBacklogSlopePerMinute(t *testing.T) {
	if slope := backlogSlopePerMinute([]int64{0, 2, 4, 6}, 5*time.Second); math.Abs(slope-24) > .0001 {
		t.Fatalf("growing slope=%f", slope)
	}
	if slope := backlogSlopePerMinute([]int64{6, 4, 2, 0}, 5*time.Second); math.Abs(slope+24) > .0001 {
		t.Fatalf("draining slope=%f", slope)
	}
	if slope := backlogSlopePerMinute([]int64{2}, 5*time.Second); slope != 0 {
		t.Fatalf("single slope=%f", slope)
	}
}

func TestAddS3MetricsAccumulatesRestartedWorker(t *testing.T) {
	var report comparisonReport
	first := []byte("eventglass_s3_requests_total{operation=\"put\"} 2\neventglass_s3_transfer_bytes_total{direction=\"put\"} 30\n")
	second := []byte("eventglass_s3_requests_total{operation=\"put\"} 3\neventglass_s3_transfer_bytes_total{direction=\"put\"} 40\n")
	addS3Metrics(first, &report)
	addS3Metrics(second, &report)
	if report.S3PutRequests != 5 || report.S3PutBytes != 70 {
		t.Fatalf("restart counters requests=%d bytes=%d", report.S3PutRequests, report.S3PutBytes)
	}
}

func TestWorkloadTargetsRequireLatencySamples(t *testing.T) {
	report := comparisonReport{ACKP95MS: 0, VisibilityP95MS: 0, Targets: make(map[string]bool)}
	setWorkloadTargets(&report, 0)
	if report.Targets["ack_p95_le_500ms"] || report.Targets["visibility_p95_le_5s"] {
		t.Fatal("missing latency samples must not satisfy latency targets")
	}
	report.ACKSamples, report.VisibilitySamples = 1, 1
	setWorkloadTargets(&report, 0)
	if !report.Targets["ack_p95_le_500ms"] || !report.Targets["visibility_p95_le_5s"] {
		t.Fatal("measured zero latency satisfies the numeric targets")
	}
}
