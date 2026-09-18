package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIngestSchemaBatchScopeAndGlobalProjectIdentity(t *testing.T) {
	env := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := control.ApplyMigrations(ctx, env["EVENTGLASS_DATABASE_URL"]); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, env["EVENTGLASS_DATABASE_URL"])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	exec := func(sql string) {
		t.Helper()
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	reject := func(sql, code string) {
		t.Helper()
		exec("SAVEPOINT negative")
		_, err := tx.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Fatalf("expected SQLSTATE %s, got %v", code, err)
		}
		exec("ROLLBACK TO SAVEPOINT negative")
		exec("RELEASE SAVEPOINT negative")
	}
	exec("INSERT INTO tenants(tenant_id) VALUES(100),(200)")
	exec("INSERT INTO projects(tenant_id, project_id, scrub_revision) VALUES(100,10,1),(100,11,1),(200,20,1)")
	reject("INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES(200,10,1)", "23505")
	reject("INSERT INTO project_keys(tenant_id,project_id,key_hash) VALUES(200,10,decode(repeat('00',32),'hex'))", "23503")
	exec("INSERT INTO lanes(tenant_id,lane_id) VALUES(100,0),(200,0)")
	exec(`INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,expires_at,expected_bytes,expected_sha256)
	VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000099',100,1,'v1/test/journal','journal','pending','test',clock_timestamp()+interval '1 hour',1,repeat('a',64))`)
	batchSQL := `INSERT INTO ingest_batches(tenant_id,lane_id,batch_seq,batch_id,journal_intent_id,request_count,received_time_us,record_count,accepted_count,duplicate_count,conflict_count,journal_sha256)
	VALUES(100,0,1,'00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000001',3,1,2,2,0,0,repeat('a',64))`
	exec(batchSQL)
	exec(`INSERT INTO receipts(acceptance_id,tenant_id,project_id,lane_id,batch_seq,content_sha256,request_index,selection_json,selection_sha256,ordinal_first,ordinal_last,accepted_count,duplicate_count,conflict_count,unsupported_count,received_time_us) VALUES
	('00000000-0000-4000-8000-000000000010',100,10,0,1,repeat('a',64),0,'[0]',repeat('a',64),0,0,1,0,0,0,1),
	('00000000-0000-4000-8000-000000000011',100,11,0,1,repeat('a',64),1,'[]',repeat('a',64),1,0,0,0,0,1,1),
	('00000000-0000-4000-8000-000000000012',100,11,0,1,repeat('a',64),2,'[1]',repeat('a',64),1,1,1,0,0,0,1)`)
	reject("UPDATE receipts SET project_id=20 WHERE project_id=10", "23503")
	reject("UPDATE ingest_batches SET tenant_id=200", "23503")
	reject("UPDATE receipts SET accepted_count=2 WHERE request_index=0", "23514")
	reject("UPDATE receipts SET request_index=0 WHERE request_index=1", "23505")
	reject("UPDATE object_intents SET object_key='../escape'", "23514")
	exec("SET CONSTRAINTS ALL IMMEDIATE")
	reject(`INSERT INTO event_dedupe(tenant_id,project_id,kind,source_event_id,record_id,receipt_acceptance_id,payload_sha256,expires_at)
	VALUES(100,11,'error',repeat('a',32),repeat('b',64),'00000000-0000-4000-8000-000000000010',repeat('c',64),clock_timestamp()+interval '37 days')`, "23503")
	var requests, projects int
	if err := tx.QueryRow(ctx, "SELECT count(*), count(DISTINCT project_id) FROM receipts WHERE tenant_id=100").Scan(&requests, &projects); err != nil {
		t.Fatal(err)
	}
	if requests != 3 || projects != 2 {
		t.Fatalf("multi-project batch receipts=%d projects=%d", requests, projects)
	}
}
