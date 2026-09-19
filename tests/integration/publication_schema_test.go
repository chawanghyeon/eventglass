package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPublicationSchemaScopesOccurrenceUniquenessAndNullableRelease(t *testing.T) {
	fixture := setupAcceptFixture(t, 801)
	other := setupAcceptFixture(t, 802)
	lane := 4
	acceptanceID := fixture.uuidForLane(lane)
	request := fixture.request(acceptanceID, "publication-schema", "", "")
	batch := fixture.batch(t, lane, "publication-schema", []control.VerifiedRequest{request})
	results, err := control.Accept(context.Background(), fixture.pool, batch)
	if err != nil || len(results) != 1 {
		t.Fatalf("Accept=%#v err=%v", results, err)
	}
	otherAcceptance := other.uuidForLane(lane)
	otherBatch := other.batch(t, lane, "publication-other", []control.VerifiedRequest{other.request(otherAcceptance, "publication-other", "", "")})
	if _, err := control.Accept(context.Background(), other.pool, otherBatch); err != nil {
		t.Fatal(err)
	}

	tx, err := fixture.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	exec := func(statement string, arguments ...any) {
		t.Helper()
		if _, err := tx.Exec(context.Background(), statement, arguments...); err != nil {
			t.Fatal(err)
		}
	}
	reject := func(statement, code string, arguments ...any) {
		t.Helper()
		exec("SAVEPOINT publication_negative")
		_, err := tx.Exec(context.Background(), statement, arguments...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Fatalf("expected SQLSTATE %s, got %v", code, err)
		}
		exec("ROLLBACK TO SAVEPOINT publication_negative")
		exec("RELEASE SAVEPOINT publication_negative")
	}

	outputID := fixture.uuidForLane(0)
	exec(`INSERT INTO job_outputs(output_id,tenant_id,job_id,prepare_fence,manifest_version,header_json,manifest_sha256,occurrence_sha256,selected_record_count,selected_error_count,state)
		VALUES($1,$2,$3,1,1,$4,repeat('a',64),repeat('b',64),1,1,'prepared')`, outputID, fixture.tenantID, batch.JobID, []byte(`{"version":1}`))
	exec(`UPDATE jobs SET state='prepared',prepared_output_id=$2 WHERE job_id=$1`, batch.JobID, outputID)
	exec(`INSERT INTO job_output_parts(output_id,part_index,metadata_json,metadata_sha256) VALUES($1,0,$2,repeat('c',64))`, outputID, []byte(`{"version":1}`))
	recordID := request.Candidates[0].RecordID
	exec(`INSERT INTO job_output_occurrences(output_id,record_id,tenant_id,project_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us,release_json,issue_id,grouping_version,fingerprint_sha256,title_json)
		VALUES($1,$2,$3,$4,$5,$6,$7,0,10,0,20,NULL,repeat('d',64),1,repeat('d',64),'"title"')`,
		outputID, recordID, fixture.tenantID, fixture.projectID, acceptanceID, lane, results[0].BatchSeq)
	reject(`INSERT INTO job_output_occurrences(output_id,record_id,tenant_id,project_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us,issue_id,grouping_version,fingerprint_sha256,title_json)
		VALUES($1,$2,$3,$4,$5,$6,$7,0,10,0,20,repeat('e',64),1,repeat('e',64),'"title"')`, "23503",
		outputID, strings.Repeat("f", 64), fixture.tenantID, other.projectID, acceptanceID, lane, results[0].BatchSeq)

	bundleID := fixture.uuidForLane(0)
	exec(`INSERT INTO bundles(bundle_id,tenant_id,lane_id,schema_version,grouping_version,event_day,kind,input_seq_min,input_seq_max,row_count,identity_sha256,valid_from_generation)
		VALUES($1,$2,$3,1,1,'2026-01-01','error',$4,$4,1,repeat('1',64),1)`, bundleID, fixture.tenantID, lane, results[0].BatchSeq)
	exec(`INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, fixture.tenantID, bundleID, fixture.projectID)
	reject(`INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, "23503", fixture.tenantID, bundleID, other.projectID)
	reject(`INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
		VALUES($1,$2,$3,$4,'analytics',1,repeat('2',64),1,10,10,20,20,$5,$5)`, "23503",
		fixture.uuidForLane(0), fixture.tenantID, bundleID, otherBatch.Journal.IntentID, results[0].BatchSeq)

	issueID := strings.Repeat("d", 64)
	exec(`INSERT INTO issues(tenant_id,project_id,issue_id,grouping_version,fingerprint_sha256,status,revision,occurrence_count,
		first_event_time_us,first_event_time_ns,first_record_id,first_release_json,last_event_time_us,last_event_time_ns,last_record_id,last_release_json,last_received_time_us,title_json)
		VALUES($1,$2,$3,1,$3,'unresolved',1,1,10,0,$4,NULL,10,0,$4,NULL,20,'"title"')`, fixture.tenantID, fixture.projectID, issueID, recordID)
	exec(`INSERT INTO issue_occurrences(record_id,tenant_id,project_id,issue_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us,release_json)
		VALUES($1,$2,$3,$4,$5,$6,$7,0,10,0,20,NULL)`, recordID, fixture.tenantID, fixture.projectID, issueID, acceptanceID, lane, results[0].BatchSeq)
	reject(`INSERT INTO issue_occurrences(record_id,tenant_id,project_id,issue_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us)
		VALUES($1,$2,$3,$4,$5,$6,$7,0,10,0,20)`, "23505", recordID, fixture.tenantID, fixture.projectID, issueID, acceptanceID, lane, results[0].BatchSeq)
	var firstNull, lastNull, occurrenceNull bool
	if err := tx.QueryRow(context.Background(), `SELECT i.first_release_json IS NULL,i.last_release_json IS NULL,o.release_json IS NULL
		FROM issues i JOIN issue_occurrences o USING(tenant_id,project_id,issue_id) WHERE i.issue_id=$1`, issueID).Scan(&firstNull, &lastNull, &occurrenceNull); err != nil {
		t.Fatal(err)
	}
	if !firstNull || !lastNull || !occurrenceNull {
		t.Fatalf("nullable releases first=%v last=%v occurrence=%v", firstNull, lastNull, occurrenceNull)
	}
}
