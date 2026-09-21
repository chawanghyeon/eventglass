package app

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

func (runtime *Runtime) serveAutoscaleMetrics(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != "/metrics" {
		http.NotFound(writer, request)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), time.Second)
	defer cancel()
	backlog, err := runtime.database.ReadAutoscaleBacklog(ctx)
	if err != nil {
		http.Error(writer, "autoscale metrics unavailable", http.StatusServiceUnavailable)
		return
	}
	dependencyAvailable := 1
	info, err := runtime.store.Head(ctx, runtime.markerKey)
	if err != nil || info.Size != runtime.markerBytes || info.SHA256 != runtime.markerSHA {
		dependencyAvailable = 0
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintln(writer, "# HELP eventglass_autoscale_queued_bytes Durable queued input bytes by fixed worker pool.")
	_, _ = fmt.Fprintln(writer, "# TYPE eventglass_autoscale_queued_bytes gauge")
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_queued_bytes{pool=\"ingest\"} %d\n", backlog.Ingest.QueuedBytes)
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_queued_bytes{pool=\"query\"} %d\n", backlog.Query.QueuedBytes)
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_queued_bytes{pool=\"maintenance\"} %d\n", backlog.Maintenance.QueuedBytes)
	_, _ = fmt.Fprintln(writer, "# HELP eventglass_autoscale_oldest_age_seconds Oldest durable work age by fixed worker pool.")
	_, _ = fmt.Fprintln(writer, "# TYPE eventglass_autoscale_oldest_age_seconds gauge")
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_oldest_age_seconds{pool=\"ingest\"} %.3f\n", backlog.Ingest.OldestAge.Seconds())
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_oldest_age_seconds{pool=\"query\"} %.3f\n", backlog.Query.OldestAge.Seconds())
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_oldest_age_seconds{pool=\"maintenance\"} %.3f\n", backlog.Maintenance.OldestAge.Seconds())
	_, _ = fmt.Fprintln(writer, "# HELP eventglass_autoscale_dependency_available One only while PostgreSQL and the installation S3 marker respond within one second.")
	_, _ = fmt.Fprintln(writer, "# TYPE eventglass_autoscale_dependency_available gauge")
	_, _ = fmt.Fprintf(writer, "eventglass_autoscale_dependency_available %d\n", dependencyAvailable)
	operations := runtime.store.OperationCounts()
	_, _ = fmt.Fprintln(writer, "# HELP eventglass_s3_requests_total S3 requests issued by this process, with fixed operation labels.")
	_, _ = fmt.Fprintln(writer, "# TYPE eventglass_s3_requests_total counter")
	_, _ = fmt.Fprintf(writer, "eventglass_s3_requests_total{operation=\"put\"} %d\n", operations.PutRequests)
	_, _ = fmt.Fprintf(writer, "eventglass_s3_requests_total{operation=\"head\"} %d\n", operations.HeadRequests)
	_, _ = fmt.Fprintf(writer, "eventglass_s3_requests_total{operation=\"get\"} %d\n", operations.FullGetRequests)
	_, _ = fmt.Fprintf(writer, "eventglass_s3_requests_total{operation=\"range_get\"} %d\n", operations.RangeRequests)
	_, _ = fmt.Fprintln(writer, "# HELP eventglass_s3_transfer_bytes_total S3 application bytes transferred by this process, with fixed direction labels.")
	_, _ = fmt.Fprintln(writer, "# TYPE eventglass_s3_transfer_bytes_total counter")
	_, _ = fmt.Fprintf(writer, "eventglass_s3_transfer_bytes_total{direction=\"put\"} %d\n", operations.PutBytes)
	_, _ = fmt.Fprintf(writer, "eventglass_s3_transfer_bytes_total{direction=\"get\"} %d\n", operations.FullGetBytes)
	_, _ = fmt.Fprintf(writer, "eventglass_s3_transfer_bytes_total{direction=\"range_get\"} %d\n", operations.RangeBytes)
	runtime.stats.writeMetrics(writer)
}
