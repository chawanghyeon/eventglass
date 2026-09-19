package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type preparedFixtureOutput struct {
	command control.PrepareCommand
	rootSHA string
}

func TestPreparePublicationOrderTakeoverRevocationAndIdempotence(t *testing.T) {
	fixture := setupAcceptFixture(t, 901)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}
	lane := 7
	firstAcceptance := fixture.uuidForLane(lane)
	secondAcceptance := fixture.uuidForLane(lane)
	firstBatch := fixture.batch(t, lane, "publish-first", []control.VerifiedRequest{fixture.request(firstAcceptance, "publish-first", "", "")})
	firstReceipts, err := control.Accept(ctx, fixture.pool, firstBatch)
	if err != nil {
		t.Fatal(err)
	}
	secondBatch := fixture.batch(t, lane, "publish-second", []control.VerifiedRequest{fixture.request(secondAcceptance, "publish-second", "", "")})
	secondReceipts, err := control.Accept(ctx, fixture.pool, secondBatch)
	if err != nil {
		t.Fatal(err)
	}
	firstJob, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "converter-first", time.Minute)
	if err != nil || firstJob == nil || firstJob.BatchSeq != firstReceipts[0].BatchSeq {
		t.Fatalf("first conversion claim=%#v err=%v", firstJob, err)
	}
	secondJob, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "converter-second", time.Minute)
	if err != nil || secondJob == nil || secondJob.BatchSeq != secondReceipts[0].BatchSeq {
		t.Fatalf("second conversion claim=%#v err=%v", secondJob, err)
	}
	sharedIssueID := fixtureSHA("publication-shared-issue")
	secondOutput := prepareFixtureOutput(t, fixture, secondBatch, secondReceipts[0], secondJob, sharedIssueID)
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET storage_generation=2 WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if err := control.Prepare(ctx, fixture.pool, secondOutput.command); !errors.Is(err, control.ErrJobFenceStale) {
		t.Fatalf("stale storage generation Prepare=%v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET storage_generation=1 WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if err := control.Prepare(ctx, fixture.pool, secondOutput.command); err != nil {
		t.Fatal(err)
	}
	if err := control.Prepare(ctx, fixture.pool, secondOutput.command); err != nil {
		t.Fatalf("lost Prepare reply was not idempotent: %v", err)
	}
	blocked, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, lane, "publisher", time.Minute)
	if err != nil || blocked != nil {
		t.Fatalf("successor bypassed missing predecessor: job=%#v err=%v", blocked, err)
	}
	firstOutput := prepareFixtureOutput(t, fixture, firstBatch, firstReceipts[0], firstJob, sharedIssueID)
	if err := control.Prepare(ctx, fixture.pool, firstOutput.command); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE projects SET state='disabled',auth_revision=auth_revision+1 WHERE tenant_id=$1 AND project_id=$2`, fixture.tenantID, fixture.projectID); err != nil {
		t.Fatal(err)
	}
	publication, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, lane, "publisher-old", time.Minute)
	if err != nil || publication == nil || publication.BatchSeq != firstReceipts[0].BatchSeq {
		t.Fatalf("publication claim=%#v err=%v", publication, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, publication.Authority.JobID); err != nil {
		t.Fatal(err)
	}
	takeover, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, lane, "publisher-new", time.Minute)
	if err != nil || takeover == nil || takeover.Authority.Fence <= publication.Authority.Fence {
		t.Fatalf("publication takeover=%#v err=%v", takeover, err)
	}
	staleCommand := publishCommand(*publication, fixture.tenantID, lane)
	if _, err := control.Publish(ctx, fixture.pool, staleCommand); !errors.Is(err, control.ErrJobFenceStale) {
		t.Fatalf("stale publisher result=%v", err)
	}
	result, err := control.Publish(ctx, fixture.pool, publishCommand(*takeover, fixture.tenantID, lane))
	if err != nil || result.CatalogGeneration != 1 || result.AlreadyPublished {
		t.Fatalf("first Publish=%#v err=%v", result, err)
	}
	again, err := control.Publish(ctx, fixture.pool, publishCommand(*takeover, fixture.tenantID, lane))
	if err != nil || !again.AlreadyPublished || again.CatalogGeneration != 1 {
		t.Fatalf("lost Publish reply=%#v err=%v", again, err)
	}
	next, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, lane, "publisher-next", time.Minute)
	if err != nil || next == nil || next.BatchSeq != secondReceipts[0].BatchSeq {
		t.Fatalf("next publication=%#v err=%v", next, err)
	}
	result, err = control.Publish(ctx, fixture.pool, publishCommand(*next, fixture.tenantID, lane))
	if err != nil || result.CatalogGeneration != 2 {
		t.Fatalf("second Publish=%#v err=%v", result, err)
	}
	var publishedSeq, generation, issueCount, occurrenceCount, bundleCount int64
	if err := fixture.pool.QueryRow(ctx, `SELECT l.published_seq,l.catalog_generation,i.occurrence_count,
		(SELECT count(*) FROM issue_occurrences WHERE issue_id=$3),(SELECT count(*) FROM bundles WHERE tenant_id=$1)
		FROM lanes l JOIN issues i ON i.tenant_id=l.tenant_id AND i.project_id=$4 AND i.issue_id=$3
		WHERE l.tenant_id=$1 AND l.lane_id=$2`, fixture.tenantID, lane, sharedIssueID, fixture.projectID).Scan(&publishedSeq, &generation, &issueCount, &occurrenceCount, &bundleCount); err != nil {
		t.Fatal(err)
	}
	if publishedSeq != secondReceipts[0].BatchSeq || generation != 2 || issueCount != 2 || occurrenceCount != 2 || bundleCount != 2 {
		t.Fatalf("published=%d generation=%d issue=%d occurrences=%d bundles=%d", publishedSeq, generation, issueCount, occurrenceCount, bundleCount)
	}
}

func TestConcurrentMultiLanePublishCountsIssueOccurrencesOnce(t *testing.T) {
	fixture := setupAcceptFixture(t, 902)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}
	type accepted struct {
		batch   control.VerifiedBatch
		receipt control.ReceiptResult
	}
	acceptedByLane := make(map[int]accepted)
	for _, lane := range []int{2, 11} {
		acceptanceID := fixture.uuidForLane(lane)
		batch := fixture.batch(t, lane, "multi-lane-"+string(rune('a'+lane)), []control.VerifiedRequest{fixture.request(acceptanceID, "multi-lane-"+string(rune('a'+lane)), "", "")})
		receipts, err := control.Accept(ctx, fixture.pool, batch)
		if err != nil {
			t.Fatal(err)
		}
		acceptedByLane[lane] = accepted{batch: batch, receipt: receipts[0]}
	}
	jobs := make(map[int]*control.ConversionJob)
	for range acceptedByLane {
		job, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "multi-converter-"+fixture.uuidForLane(0), time.Minute)
		if err != nil || job == nil {
			t.Fatalf("conversion claim=%#v err=%v", job, err)
		}
		jobs[job.LaneID] = job
	}
	issueID := fixtureSHA("multi-lane-shared-issue")
	for lane, accepted := range acceptedByLane {
		output := prepareFixtureOutput(t, fixture, accepted.batch, accepted.receipt, jobs[lane], issueID)
		if err := control.Prepare(ctx, fixture.pool, output.command); err != nil {
			t.Fatal(err)
		}
	}
	commands := make([]control.PublishCommand, 0, 2)
	for lane := range acceptedByLane {
		job, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, lane, "multi-publisher-"+fixture.uuidForLane(0), time.Minute)
		if err != nil || job == nil {
			t.Fatalf("publication claim lane=%d job=%#v err=%v", lane, job, err)
		}
		commands = append(commands, publishCommand(*job, fixture.tenantID, lane))
	}
	start := make(chan struct{})
	errorsCh := make(chan error, len(commands))
	for _, command := range commands {
		go func(command control.PublishCommand) {
			<-start
			_, err := control.Publish(context.Background(), fixture.pool, command)
			errorsCh <- err
		}(command)
	}
	close(start)
	for range commands {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	for _, command := range commands {
		result, err := control.Publish(ctx, fixture.pool, command)
		if err != nil || !result.AlreadyPublished {
			t.Fatalf("duplicate Publish=%#v err=%v", result, err)
		}
	}
	var issueCount, occurrences, createdTransitions int64
	if err := fixture.pool.QueryRow(ctx, `SELECT occurrence_count,
		(SELECT count(*) FROM issue_occurrences WHERE issue_id=$1),
		(SELECT count(*) FROM issue_transitions WHERE issue_id=$1 AND type='created')
		FROM issues WHERE tenant_id=$2 AND project_id=$3 AND issue_id=$1`, issueID, fixture.tenantID, fixture.projectID).Scan(&issueCount, &occurrences, &createdTransitions); err != nil {
		t.Fatal(err)
	}
	if issueCount != 2 || occurrences != 2 || createdTransitions != 1 {
		t.Fatalf("issue=%d occurrences=%d created transitions=%d", issueCount, occurrences, createdTransitions)
	}
}

func TestEmptyPreparedOutputPublishesCutWithoutCatalogFiles(t *testing.T) {
	fixture := setupAcceptFixture(t, 903)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}
	lane := 5
	request := fixture.request(fixture.uuidForLane(lane), "empty-publication", "", "")
	request.Candidates = nil
	batch := fixture.batch(t, lane, "empty-publication", []control.VerifiedRequest{request})
	receipts, err := control.Accept(ctx, fixture.pool, batch)
	if err != nil || receipts[0].AcceptedCount != 0 {
		t.Fatalf("empty Accept=%#v err=%v", receipts, err)
	}
	job, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "empty-converter", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("empty conversion claim=%#v err=%v", job, err)
	}
	root := t.TempDir()
	occurrencePath := filepath.Join(root, "occurrences.jsonl")
	occurrenceBytes := writeJSONLines(t, occurrencePath, struct {
		Version int `json:"version"`
	}{1})
	occurrenceSHA := fileSHA(t, occurrencePath)
	emptyIdentity := sha256.Sum256(nil)
	header := model.OutputManifestHeader{
		Version: 1, OutputID: fixture.uuidForLane(0), JobID: job.Authority.JobID, TenantID: fixture.tenantID, LaneID: lane, BatchSeq: receipts[0].BatchSeq,
		JournalSHA256: batch.Journal.SHA256, ReceiptSetSHA256: receiptSetDigest(receipts[0]), SelectedIdentitySHA256: hex.EncodeToString(emptyIdentity[:]),
		OccurrenceSummarySHA256: occurrenceSHA, GroupingVersion: 1,
	}
	manifestRoot := model.OutputManifestRoot{Version: 1, Header: header, Parts: []model.OutputManifestPartRef{}}
	command := control.PrepareCommand{
		Authority: job.Authority, TenantID: fixture.tenantID, LaneID: lane, BatchSeq: receipts[0].BatchSeq, Root: manifestRoot,
		Parts: []control.PreparedPartInput{}, OccurrencePath: occurrencePath, OccurrenceBytes: occurrenceBytes, OccurrenceSHA: occurrenceSHA,
	}
	if err := control.Prepare(ctx, fixture.pool, command); err != nil {
		t.Fatal(err)
	}
	publication, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, lane, "empty-publisher", time.Minute)
	if err != nil || publication == nil {
		t.Fatalf("empty publication claim=%#v err=%v", publication, err)
	}
	result, err := control.Publish(ctx, fixture.pool, publishCommand(*publication, fixture.tenantID, lane))
	if err != nil || result.CatalogGeneration != 1 {
		t.Fatalf("empty Publish=%#v err=%v", result, err)
	}
	var publishedSeq, generation, bundles, files int64
	if err := fixture.pool.QueryRow(ctx, `SELECT published_seq,catalog_generation,
		(SELECT count(*) FROM bundles WHERE tenant_id=$1),(SELECT count(*) FROM files WHERE tenant_id=$1)
		FROM lanes WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, lane).Scan(&publishedSeq, &generation, &bundles, &files); err != nil {
		t.Fatal(err)
	}
	if publishedSeq != receipts[0].BatchSeq || generation != 1 || bundles != 0 || files != 0 {
		t.Fatalf("empty cut=%d generation=%d bundles=%d files=%d", publishedSeq, generation, bundles, files)
	}
}

func publishCommand(job control.PublicationJob, tenantID int64, laneID int) control.PublishCommand {
	return control.PublishCommand{Authority: job.Authority, TenantID: tenantID, LaneID: laneID, BatchSeq: job.BatchSeq, OutputID: job.OutputID, ManifestSHA: job.ManifestSHA}
}

func prepareFixtureOutput(t *testing.T, fixture *acceptFixture, batch control.VerifiedBatch, receipt control.ReceiptResult, job *control.ConversionJob, issueID string) preparedFixtureOutput {
	t.Helper()
	root := t.TempDir()
	outputID := fixture.uuidForLane(0)
	bundleID := fixture.uuidForLane(0)
	makeFile := func(role string) model.FileManifest {
		intentID := fixture.uuidForLane(0)
		fileSHA := fixtureSHA(role + ":" + outputID)
		intent := control.IntentAuthority{IntentID: intentID, Owner: job.Authority.Owner, Fence: job.Authority.Fence, Bytes: 1, SHA256: fileSHA}
		registration := control.OutputIntentRegistration{
			Authority: job.Authority, TenantID: fixture.tenantID, Role: role,
			ObjectKey: "v1/integration/bundles/" + outputID + "/" + role + ".parquet", Intent: intent,
		}
		if err := control.RegisterOutputIntent(context.Background(), fixture.pool, registration); err != nil {
			t.Fatal(err)
		}
		if err := control.MarkOutputIntentUploaded(context.Background(), fixture.pool, job.Authority, fixture.tenantID, intent); err != nil {
			t.Fatal(err)
		}
		return model.FileManifest{
			FileID: fixture.uuidForLane(0), IntentID: intentID, Role: role, Bytes: 1, SHA256: fileSHA, RowCount: 1,
			MinEventTimeUS: 10, MaxEventTimeUS: 10, MinReceivedTimeUS: receipt.ReceivedTimeUS, MaxReceivedTimeUS: receipt.ReceivedTimeUS,
			MinBatchSeq: receipt.BatchSeq, MaxBatchSeq: receipt.BatchSeq, Blocks: []model.FileBlockManifest{{Index: 0, SHA256: fileSHA}},
		}
	}
	bundle := model.BundleManifest{
		BundleID: bundleID, EventDay: "2026-01-01", Kind: model.KindError, InputSeqMin: receipt.BatchSeq, InputSeqMax: receipt.BatchSeq,
		RowCount: 1, IdentitySHA256: identityDigest(batch.Requests[0].Candidates[0].RecordID), ProjectIDs: []int64{fixture.projectID},
		Analytics: makeFile("analytics"), Payload: makeFile("payload"),
	}
	occurrencePath := filepath.Join(root, "occurrences.jsonl")
	occurrence := model.IssueOccurrenceSummary{
		RecordID: batch.Requests[0].Candidates[0].RecordID, ProjectID: fixture.projectID, AcceptanceID: receipt.AcceptanceID,
		LaneID: receipt.LaneID, BatchSeq: receipt.BatchSeq, Ordinal: receipt.OrdinalFirst, EventTimeUS: 10, ReceivedTimeUS: receipt.ReceivedTimeUS,
		IssueID: issueID, GroupingVersion: 1, FingerprintSHA256: issueID, TitleJSON: `"publication issue"`,
	}
	occurrenceBytes := writeJSONLines(t, occurrencePath, struct {
		Version int `json:"version"`
	}{1}, occurrence)
	occurrenceSHA := fileSHA(t, occurrencePath)
	header := model.OutputManifestHeader{
		Version: 1, OutputID: outputID, JobID: job.Authority.JobID, TenantID: fixture.tenantID, LaneID: receipt.LaneID, BatchSeq: receipt.BatchSeq,
		JournalSHA256: batch.Journal.SHA256, ReceiptSetSHA256: receiptSetDigest(receipt), SelectedIdentitySHA256: bundle.IdentitySHA256,
		OccurrenceSummarySHA256: occurrenceSHA, GroupingVersion: 1, SelectedRecordCount: 1, SelectedErrorCount: 1, BundleCount: 1,
	}
	manifestDirectory := filepath.Join(root, "manifest")
	pager, err := storage.NewOutputManifestPager(manifestDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err := pager.Append(bundle); err != nil {
		t.Fatal(err)
	}
	manifest, err := pager.Finish(header)
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]control.PreparedPartInput, len(manifest.Parts))
	for index, part := range manifest.Parts {
		parts[index] = control.PreparedPartInput{Index: part.Index, Path: part.Path, Bytes: part.Bytes, SHA256: part.SHA256}
	}
	return preparedFixtureOutput{
		command: control.PrepareCommand{Authority: job.Authority, TenantID: fixture.tenantID, LaneID: receipt.LaneID, BatchSeq: receipt.BatchSeq, Root: manifest.Root, Parts: parts, OccurrencePath: occurrencePath, OccurrenceBytes: occurrenceBytes, OccurrenceSHA: occurrenceSHA},
		rootSHA: manifest.RootSHA256,
	}
}

func writeJSONLines(t *testing.T, path string, values ...any) int64 {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func identityDigest(recordID string) string {
	digest := sha256.Sum256([]byte(recordID + "\n"))
	return hex.EncodeToString(digest[:])
}

func receiptSetDigest(receipt control.ReceiptResult) string {
	encoded, _ := json.Marshal(struct {
		AcceptanceID string `json:"acceptance_id"`
		SelectionSHA string `json:"selection_sha256"`
	}{receipt.AcceptanceID, receipt.SelectionSHA256})
	digest := sha256.Sum256(append(encoded, '\n'))
	return hex.EncodeToString(digest[:])
}
