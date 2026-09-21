package control

import (
	"context"
	"time"
)

type AutoscalePool struct {
	QueuedBytes int64
	OldestAge   time.Duration
}

type AutoscaleBacklog struct {
	Ingest, Query, Maintenance AutoscalePool
}

func (database *RuntimeDatabase) ReadAutoscaleBacklog(ctx context.Context) (AutoscaleBacklog, error) {
	var result AutoscaleBacklog
	var ingestAge, queryAge, maintenanceAge float64
	err := database.pool.QueryRow(ctx, `SELECT
		COALESCE((SELECT sum(oi.expected_bytes) FROM jobs j JOIN ingest_batches b ON b.tenant_id=j.tenant_id AND b.lane_id=j.lane_id AND b.batch_seq=j.batch_seq JOIN object_intents oi ON oi.intent_id=b.journal_intent_id WHERE j.state IN ('queued','running')),0),
		COALESCE((SELECT extract(epoch FROM clock_timestamp()-min(j.created_at)) FROM jobs j WHERE j.state IN ('queued','running')),0),
		COALESCE((SELECT sum(GREATEST(q.plan_input_bytes,octet_length(q.operation_bytes))) FROM query_jobs q WHERE q.state IN ('planning','queued','running')),0),
		COALESCE((SELECT extract(epoch FROM clock_timestamp()-min(q.created_at)) FROM query_jobs q WHERE q.state IN ('planning','queued','running')),0),
		COALESCE((SELECT sum(f.bytes) FROM maintenance_tasks m JOIN maintenance_inputs mi ON mi.task_id=m.task_id JOIN files f ON f.tenant_id=mi.tenant_id AND f.bundle_id=mi.bundle_id WHERE m.state IN ('queued','running','prepared')),0),
		COALESCE((SELECT extract(epoch FROM clock_timestamp()-min(m.created_at)) FROM maintenance_tasks m WHERE m.state IN ('queued','running','prepared')),0)`).Scan(
		&result.Ingest.QueuedBytes, &ingestAge, &result.Query.QueuedBytes, &queryAge, &result.Maintenance.QueuedBytes, &maintenanceAge)
	result.Ingest.OldestAge = time.Duration(ingestAge * float64(time.Second))
	result.Query.OldestAge = time.Duration(queryAge * float64(time.Second))
	result.Maintenance.OldestAge = time.Duration(maintenanceAge * float64(time.Second))
	return result, err
}
