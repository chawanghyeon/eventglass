package integration

import (
	"context"
	"encoding/json"
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
	reject("INSERT INTO project_keys(tenant_id,project_id,key_hash,key_id,key_prefix) VALUES(200,10,decode(repeat('00',32),'hex'),'00000000-0000-4000-8000-000000000200','00000000')", "23503")
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
	reject("UPDATE ingest_batches SET tenant_id=200 WHERE tenant_id=100", "23503")
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

func TestMigration0003UpgradesExistingRowsAndScopedProducerFK(t *testing.T) {
	env := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := control.ApplyMigrations(ctx, env["EVENTGLASS_DATABASE_URL"]); err != nil {
		t.Fatal(err)
	}
	manifest, err := control.MigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest) != 14 {
		t.Fatalf("migration count=%d", len(manifest))
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
	// The suite may already have exercised migration 0010. Remove its produced
	// fixture objects inside this rollback-only transaction before restoring the
	// pre-0010 producer constraint shape.
	exec(`DELETE FROM file_blocks WHERE file_id IN (SELECT f.file_id FROM files f JOIN object_intents oi ON oi.intent_id=f.intent_id WHERE oi.maintenance_task_id IS NOT NULL)`)
	exec(`DELETE FROM files WHERE intent_id IN (SELECT intent_id FROM object_intents WHERE maintenance_task_id IS NOT NULL)`)
	exec(`DELETE FROM object_intents WHERE maintenance_task_id IS NOT NULL`)
	exec(`UPDATE bundles SET reserved_by=NULL WHERE reserved_by IS NOT NULL`)
	exec(`DELETE FROM maintenance_tasks`)
	exec(manifest[10].DownSQL)
	exec(`INSERT INTO tenants(tenant_id) VALUES(399) ON CONFLICT DO NOTHING`)
	exec(`INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,expires_at,expected_bytes,expected_sha256)
		VALUES('00000000-0000-4000-8000-000000000399','00000000-0000-4000-8000-000000000099',399,1,'v1/upgrade/gc-deleting','temporary','deleting','upgrade',clock_timestamp()-interval '9 days',1,repeat('c',64))`)
	exec(manifest[10].UpSQL)
	var gcBackfilled bool
	if err := tx.QueryRow(ctx, `SELECT gc_marked_at IS NOT NULL AND gc_confirmed_at IS NULL FROM object_intents WHERE intent_id='00000000-0000-4000-8000-000000000399'`).Scan(&gcBackfilled); err != nil || !gcBackfilled {
		t.Fatalf("GC upgrade backfill=%v err=%v", gcBackfilled, err)
	}
	exec(`DELETE FROM object_intents WHERE intent_id='00000000-0000-4000-8000-000000000399'`)
	exec(manifest[10].DownSQL)
	exec(`DELETE FROM ingest_batches WHERE recovery_state='retired'`)
	exec(manifest[9].DownSQL)
	exec(manifest[8].DownSQL)
	exec(manifest[7].DownSQL)
	exec(manifest[6].DownSQL)
	exec(manifest[3].DownSQL)
	exec(manifest[2].DownSQL)
	exec("INSERT INTO tenants(tenant_id) VALUES(301),(302)")
	exec("INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES(301,3011,1),(302,3021,1)")
	exec("INSERT INTO project_keys(tenant_id,project_id,key_hash) VALUES(301,3011,decode(repeat('ab',32),'hex'))")
	exec("INSERT INTO lanes(tenant_id,lane_id) VALUES(301,0),(302,0)")
	exec(`INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,expires_at,expected_bytes,expected_sha256) VALUES
		('00000000-0000-4000-8000-000000000301','00000000-0000-4000-8000-000000000099',301,1,'v1/upgrade/301','journal','pending','test',clock_timestamp()+interval '1 hour',1,repeat('a',64)),
		('00000000-0000-4000-8000-000000000302','00000000-0000-4000-8000-000000000099',302,1,'v1/upgrade/302','journal','pending','test',clock_timestamp()+interval '1 hour',1,repeat('b',64))`)
	exec(`INSERT INTO ingest_batches(tenant_id,lane_id,batch_seq,batch_id,journal_intent_id,request_count,received_time_us,record_count,accepted_count,duplicate_count,conflict_count,journal_sha256) VALUES
		(301,0,1,'00000000-0000-4000-8000-000000003011','00000000-0000-4000-8000-000000000301',1,1,1,1,0,0,repeat('a',64)),
		(302,0,1,'00000000-0000-4000-8000-000000003021','00000000-0000-4000-8000-000000000302',1,1,0,0,0,0,repeat('b',64))`)
	exec(`INSERT INTO receipts(acceptance_id,tenant_id,project_id,lane_id,batch_seq,content_sha256,request_index,selection_json,selection_sha256,ordinal_first,ordinal_last,accepted_count,duplicate_count,conflict_count,unsupported_count,received_time_us)
		VALUES('00000000-0000-4000-8000-000000003012',301,3011,0,1,repeat('a',64),0,'{"version":1,"accepted":[[0,0]],"duplicate":[],"conflict":[]}',repeat('b',64),0,0,1,0,0,0,1)`)
	exec(`INSERT INTO sdk_outcomes(acceptance_id,item_ordinal,category,reason,quantity,approximate)
		VALUES('00000000-0000-4000-8000-000000003012',0,'error\category','quoted "reason"',7,true)`)
	exec(manifest[2].UpSQL)

	var tenantName, projectName, keyPrefix string
	var retentionDays, policyVersion int
	if err := tx.QueryRow(ctx, `SELECT t.name,p.name,k.key_prefix,p.retention_days,r.policy_revision
		FROM tenants t JOIN projects p USING(tenant_id) JOIN project_keys k USING(tenant_id,project_id)
		JOIN receipts r USING(tenant_id,project_id) WHERE t.tenant_id=301`).Scan(&tenantName, &projectName, &keyPrefix, &retentionDays, &policyVersion); err != nil {
		t.Fatal(err)
	}
	if tenantName != "Tenant" || projectName != "Project" || keyPrefix != "abababab" || retentionDays != 30 || policyVersion != 1 {
		t.Fatalf("unexpected upgraded defaults: %q %q %q %d %d", tenantName, projectName, keyPrefix, retentionDays, policyVersion)
	}
	var category, reason string
	var categoryBytes, reasonBytes int
	if err := tx.QueryRow(ctx, `SELECT category_json::jsonb #>> '{}', reason_json::jsonb #>> '{}', octet_length(category_sha256), octet_length(reason_sha256) FROM sdk_outcomes`).Scan(&category, &reason, &categoryBytes, &reasonBytes); err != nil {
		t.Fatal(err)
	}
	if category != `error\category` || reason != `quoted "reason"` || categoryBytes != 32 || reasonBytes != 32 {
		t.Fatalf("outcome changed during upgrade: %q %q %d %d", category, reason, categoryBytes, reasonBytes)
	}
	exec(`INSERT INTO sdk_outcomes(acceptance_id,item_ordinal,category_json,reason_json,category_sha256,reason_sha256,quantity,approximate)
		SELECT '00000000-0000-4000-8000-000000003012',1,'"error"',to_json(repeat('x',4000))::text,
		sha256(convert_to('"error"','UTF8')),sha256(convert_to(to_json(repeat('x',4000))::text,'UTF8')),1,false`)
	nulJSON, _ := json.Marshal("\x00")
	literalBackslashJSON, _ := json.Marshal(`\u0000`)
	for _, encoded := range [][]byte{nulJSON, literalBackslashJSON} {
		if _, err := tx.Exec(ctx, `INSERT INTO sdk_outcomes(acceptance_id,item_ordinal,category_json,reason_json,category_sha256,reason_sha256,quantity,approximate)
			VALUES('00000000-0000-4000-8000-000000003012',2,$1,'"test"',sha256(convert_to($1,'UTF8')),sha256(convert_to('"test"','UTF8')),1,false)`, string(encoded)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := tx.Query(ctx, `SELECT category_json FROM sdk_outcomes WHERE item_ordinal=2 ORDER BY category_json`)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		seen[encoded] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if len(seen) != 2 || !seen[string(nulJSON)] || !seen[string(literalBackslashJSON)] {
		t.Fatalf("NUL and literal backslash-u were not distinct: %#v", seen)
	}
	exec(`INSERT INTO jobs(job_id,kind,tenant_id,lane_id,batch_seq,state,storage_generation) VALUES
		('00000000-0000-4000-8000-000000003013','convert',301,0,1,'prepared',1),
		('00000000-0000-4000-8000-000000003023','convert',302,0,1,'prepared',1)`)
	exec(`UPDATE object_intents SET conversion_job_id='00000000-0000-4000-8000-000000003013',producer_generation=1,producer_fence=1 WHERE tenant_id=301`)
	reject(`UPDATE object_intents SET conversion_job_id='00000000-0000-4000-8000-000000003023' WHERE tenant_id=301`, "23503")
}

func TestMigration0006BackfillsExistingSnapshotAuthority(t *testing.T) {
	env := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := control.ApplyMigrations(ctx, env["EVENTGLASS_DATABASE_URL"]); err != nil {
		t.Fatal(err)
	}
	manifest, err := control.MigrationManifest()
	if err != nil {
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
	if _, err := tx.Exec(ctx, manifest[5].DownSQL); err != nil {
		t.Fatal(err)
	}
	const snapshotID = "00000000-0000-4000-8000-000000006006"
	for _, statement := range []string{
		`INSERT INTO installations(singleton,installation_id,storage_generation,schema_version,storage_identity) VALUES(true,'00000000-0000-4000-8000-000000006006',1,1,'migration-fixture') ON CONFLICT(singleton) DO NOTHING`,
		`INSERT INTO tenants(tenant_id,auth_revision) VALUES(6006,9)`,
		`INSERT INTO projects(tenant_id,project_id,scrub_revision,auth_revision) VALUES(6006,60061,1,11)`,
		`INSERT INTO users(user_id,email_normalized,password_phc,auth_revision)
		VALUES(6006,'migration-0006@example.invalid','$argon2id$v=19$m=65536,t=3,p=2$ZXZlbnRnbGFzcy1kdW1teQ$3fS7jbTBnU4p8kMPSY1XANFKxf3f6OUt0OXbKsCB9BA',7)`,
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO query_snapshots(snapshot_id,tenant_id,user_id,principal_kind,principal_ref,auth_revision,
			storage_generation,dataset_hash,dataset_bytes,retention_floor_us,expires_at,max_until)
		VALUES($1,6006,6006,'user','migration-principal',7,1,repeat('a',64),decode('7b7d','hex'),0,
			clock_timestamp()+interval '15 minutes',clock_timestamp()+interval '1 hour')`, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO snapshot_projects(snapshot_id,tenant_id,project_id) VALUES($1,6006,60061)`, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, manifest[5].UpSQL); err != nil {
		t.Fatal(err)
	}
	var tenantRevision, projectRevision int64
	var retentionDays int
	var retentionRevision int64
	if err := tx.QueryRow(ctx, `SELECT s.tenant_auth_revision,sp.project_auth_revision,i.retention_days,i.retention_revision
		FROM query_snapshots s JOIN snapshot_projects sp USING(snapshot_id,tenant_id) CROSS JOIN installations i
		WHERE s.snapshot_id=$1 AND i.singleton`, snapshotID).Scan(&tenantRevision, &projectRevision, &retentionDays, &retentionRevision); err != nil {
		t.Fatal(err)
	}
	if tenantRevision != 9 || projectRevision != 11 || retentionDays != 30 || retentionRevision != 1 {
		t.Fatalf("backfill tenant=%d project=%d retention=%d/%d", tenantRevision, projectRevision, retentionDays, retentionRevision)
	}
}
