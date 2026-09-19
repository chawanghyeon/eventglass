package control

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PreparedPartInput struct {
	Index  int
	Path   string
	Bytes  int64
	SHA256 string
}

type PrepareCommand struct {
	Authority       JobAuthority
	TenantID        int64
	LaneID          int
	BatchSeq        int64
	Root            model.OutputManifestRoot
	Parts           []PreparedPartInput
	OccurrencePath  string
	OccurrenceBytes int64
	OccurrenceSHA   string
}

type preparedIntent struct {
	role string
	file model.FileManifest
}

// Prepare atomically persists immutable output metadata and releases the
// conversion lease. It performs no object-store I/O.
func Prepare(ctx context.Context, pool *pgxpool.Pool, command PrepareCommand) error {
	rootJSON, rootSHA, err := validatePrepareCommand(command)
	if err != nil {
		return err
	}
	if err := verifyPreparedInputs(command); err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return err
	}
	if err := lockRuntimeGeneration(ctx, tx, command.Authority.InstallationID, command.Authority.StorageGeneration); err != nil {
		return err
	}
	if err := lockPrepareJob(ctx, tx, command, rootSHA); err != nil {
		if errors.Is(err, errPrepareAlreadyCommitted) {
			return tx.Commit(ctx)
		}
		return err
	}
	if err := validatePrepareBatch(ctx, tx, command); err != nil {
		return err
	}
	header := command.Root.Header
	if _, err := tx.Exec(ctx, `INSERT INTO job_outputs(output_id,tenant_id,job_id,prepare_fence,manifest_version,header_json,manifest_sha256,occurrence_sha256,selected_record_count,selected_error_count,state)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'prepared')`,
		header.OutputID, command.TenantID, command.Authority.JobID, command.Authority.Fence, command.Root.Version, rootJSON, rootSHA,
		command.OccurrenceSHA, header.SelectedRecordCount, header.SelectedErrorCount); err != nil {
		return err
	}
	intents, bundleCount, rowCount, errorCount, err := insertPreparedParts(ctx, tx, command)
	if err != nil {
		return err
	}
	if bundleCount != header.BundleCount || rowCount != int64(header.SelectedRecordCount) || errorCount != int64(header.SelectedErrorCount) {
		return errors.New("prepared manifest totals differ from header")
	}
	occurrenceCount, err := insertPreparedOccurrences(ctx, tx, command)
	if err != nil {
		return err
	}
	if occurrenceCount != header.SelectedErrorCount {
		return errors.New("prepared Issue summaries differ from header")
	}
	if err := referencePreparedIntents(ctx, tx, command, intents); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE jobs SET state='prepared',prepared_output_id=$2,owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
		WHERE job_id=$1 AND storage_generation=$3 AND state='running' AND owner=$4 AND fence=$5 AND lease_until>clock_timestamp()`,
		command.Authority.JobID, header.OutputID, command.Authority.StorageGeneration, command.Authority.Owner, command.Authority.Fence)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrJobFenceStale
	}
	return tx.Commit(ctx)
}

var errPrepareAlreadyCommitted = errors.New("prepare already committed")

func validatePrepareCommand(command PrepareCommand) ([]byte, string, error) {
	if command.TenantID <= 0 || command.LaneID < 0 || command.LaneID >= model.LaneCount || command.BatchSeq <= 0 || command.Authority.InstallationID == "" || command.Authority.StorageGeneration <= 0 || command.Authority.JobID == "" || command.Authority.Owner == "" || command.Authority.Fence <= 0 || !filepath.IsAbs(command.OccurrencePath) || command.OccurrenceBytes <= 0 || command.OccurrenceBytes > model.MaxManifestBytes || !validSHA(command.OccurrenceSHA) {
		return nil, "", errors.New("invalid Prepare command")
	}
	header := command.Root.Header
	if header.OutputID == "" || header.JobID != command.Authority.JobID || header.TenantID != command.TenantID || header.LaneID != command.LaneID || header.BatchSeq != command.BatchSeq || header.OccurrenceSummarySHA256 != command.OccurrenceSHA || len(command.Parts) != len(command.Root.Parts) {
		return nil, "", errors.New("Prepare manifest scope mismatch")
	}
	rootJSON, err := command.Root.CanonicalJSON()
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(rootJSON)
	return rootJSON, hex.EncodeToString(digest[:]), nil
}

func verifyPreparedInputs(command PrepareCommand) error {
	total := int64(0)
	for index, part := range command.Parts {
		if part.Index != index || !filepath.IsAbs(part.Path) || part.Bytes <= 0 || part.Bytes > model.MaxManifestPartBytes || !validSHA(part.SHA256) || part.SHA256 != command.Root.Parts[index].SHA256 {
			return errors.New("invalid prepared manifest part evidence")
		}
		if err := verifyLocalFile(part.Path, part.Bytes, part.SHA256); err != nil {
			return err
		}
		total += part.Bytes
		if total > model.MaxManifestBytes {
			return errors.New("prepared manifest exceeds metadata limit")
		}
	}
	return verifyLocalFile(command.OccurrencePath, command.OccurrenceBytes, command.OccurrenceSHA)
}

func verifyLocalFile(path string, expectedBytes int64, expectedSHA string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, expectedBytes+1))
	if err != nil || written != expectedBytes || hex.EncodeToString(hash.Sum(nil)) != expectedSHA {
		return errors.Join(errors.New("prepared local file evidence mismatch"), err)
	}
	return nil
}

func lockPrepareJob(ctx context.Context, tx pgx.Tx, command PrepareCommand, rootSHA string) error {
	var tenantID int64
	var laneID int
	var batchSeq, generation, fence int64
	var state, owner string
	var outputID *string
	err := tx.QueryRow(ctx, `SELECT tenant_id,lane_id,batch_seq,storage_generation,state,COALESCE(owner,''),fence,prepared_output_id::text
		FROM jobs WHERE job_id=$1 FOR UPDATE`, command.Authority.JobID).Scan(&tenantID, &laneID, &batchSeq, &generation, &state, &owner, &fence, &outputID)
	if err != nil {
		return err
	}
	if tenantID != command.TenantID || laneID != command.LaneID || batchSeq != command.BatchSeq || generation != command.Authority.StorageGeneration {
		return ErrJobFenceStale
	}
	if state == "prepared" && outputID != nil && *outputID == command.Root.Header.OutputID {
		var existingSHA string
		if err := tx.QueryRow(ctx, `SELECT manifest_sha256 FROM job_outputs WHERE output_id=$1 AND state='prepared'`, *outputID).Scan(&existingSHA); err != nil {
			return err
		}
		if existingSHA == rootSHA {
			return errPrepareAlreadyCommitted
		}
		return errors.New("conflicting prepared output")
	}
	if state != "running" || owner != command.Authority.Owner || fence != command.Authority.Fence {
		return ErrJobFenceStale
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT lease_until>clock_timestamp() FROM jobs WHERE job_id=$1`, command.Authority.JobID).Scan(&live); err != nil || !live {
		return errors.Join(ErrJobFenceStale, err)
	}
	return nil
}

func validatePrepareBatch(ctx context.Context, tx pgx.Tx, command PrepareCommand) error {
	header := command.Root.Header
	var state, journalSHA string
	var accepted int
	if err := tx.QueryRow(ctx, `SELECT state,journal_sha256,accepted_count FROM ingest_batches WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 FOR SHARE`, command.TenantID, command.LaneID, command.BatchSeq).Scan(&state, &journalSHA, &accepted); err != nil {
		return err
	}
	if state != "accepted" || journalSHA != header.JournalSHA256 || accepted != header.SelectedRecordCount {
		return errors.New("Prepare input batch differs from manifest")
	}
	rows, err := tx.Query(ctx, `SELECT acceptance_id::text,selection_sha256,grouping_version FROM receipts WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 ORDER BY acceptance_id`, command.TenantID, command.LaneID, command.BatchSeq)
	if err != nil {
		return err
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var acceptanceID, selectionSHA string
		var groupingVersion int
		if err := rows.Scan(&acceptanceID, &selectionSHA, &groupingVersion); err != nil {
			return err
		}
		if groupingVersion != header.GroupingVersion {
			return errors.New("receipt grouping version differs from manifest")
		}
		encoded, _ := json.Marshal(struct {
			AcceptanceID string `json:"acceptance_id"`
			SelectionSHA string `json:"selection_sha256"`
		}{acceptanceID, selectionSHA})
		hash.Write(encoded)
		hash.Write([]byte{'\n'})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != header.ReceiptSetSHA256 {
		return errors.New("receipt set digest differs from manifest")
	}
	return nil
}

func insertPreparedParts(ctx context.Context, tx pgx.Tx, command PrepareCommand) (map[string]preparedIntent, int, int64, int64, error) {
	intents := make(map[string]preparedIntent)
	var rows, errorRows int64
	bundles := 0
	for index, input := range command.Parts {
		encoded, err := os.ReadFile(input.Path)
		if err != nil || int64(len(encoded)) != input.Bytes {
			return nil, 0, 0, 0, errors.Join(errors.New("read prepared manifest part"), err)
		}
		digest := sha256.Sum256(encoded)
		if hex.EncodeToString(digest[:]) != input.SHA256 {
			return nil, 0, 0, 0, errors.New("prepared manifest part changed")
		}
		var part model.OutputManifestPart
		if err := strictControlJSON(encoded, &part); err != nil || part.Version != model.OutputManifestVersion || part.Index != index || len(part.Bundles) == 0 {
			return nil, 0, 0, 0, errors.Join(errors.New("invalid prepared manifest part"), err)
		}
		canonical, err := json.Marshal(part)
		if err != nil || !bytes.Equal(canonical, encoded) {
			return nil, 0, 0, 0, errors.Join(errors.New("prepared manifest part is noncanonical"), err)
		}
		for _, bundle := range part.Bundles {
			if err := validatePreparedBundle(bundle, command); err != nil {
				return nil, 0, 0, 0, err
			}
			for role, file := range map[string]model.FileManifest{"analytics": bundle.Analytics, "payload": bundle.Payload} {
				if _, duplicate := intents[file.IntentID]; duplicate {
					return nil, 0, 0, 0, errors.New("prepared manifest reuses an output intent")
				}
				intents[file.IntentID] = preparedIntent{role: role, file: file}
			}
			bundles++
			rows += bundle.RowCount
			if bundle.Kind == model.KindError {
				errorRows += bundle.RowCount
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO job_output_parts(output_id,part_index,metadata_json,metadata_sha256) VALUES($1,$2,$3,$4)`, command.Root.Header.OutputID, index, encoded, input.SHA256); err != nil {
			return nil, 0, 0, 0, err
		}
	}
	return intents, bundles, rows, errorRows, nil
}

func validatePreparedBundle(bundle model.BundleManifest, command PrepareCommand) error {
	parsedDay, dayErr := time.Parse(time.DateOnly, bundle.EventDay)
	if bundle.BundleID == "" || dayErr != nil || parsedDay.Format(time.DateOnly) != bundle.EventDay || bundle.InputSeqMin != command.BatchSeq || bundle.InputSeqMax != command.BatchSeq || bundle.RowCount <= 0 || !validSHA(bundle.IdentitySHA256) || len(bundle.ProjectIDs) == 0 || bundle.Analytics.Role != "analytics" || bundle.Payload.Role != "payload" || bundle.Analytics.RowCount != bundle.RowCount || bundle.Payload.RowCount != bundle.RowCount {
		return errors.New("invalid prepared bundle")
	}
	if bundle.Kind != model.KindError && bundle.Kind != model.KindLog && bundle.Kind != model.KindTransaction {
		return errors.New("invalid prepared bundle kind")
	}
	previous := int64(0)
	for _, projectID := range bundle.ProjectIDs {
		if projectID <= previous {
			return errors.New("prepared project IDs are not ordered")
		}
		previous = projectID
	}
	for _, file := range []model.FileManifest{bundle.Analytics, bundle.Payload} {
		if file.FileID == "" || file.IntentID == "" || file.Bytes <= 0 || !validSHA(file.SHA256) || file.MinEventTimeUS > file.MaxEventTimeUS || file.MinReceivedTimeUS > file.MaxReceivedTimeUS || file.MinBatchSeq != command.BatchSeq || file.MaxBatchSeq != command.BatchSeq || len(file.Blocks) != int((file.Bytes+model.FileBlockBytes-1)/model.FileBlockBytes) {
			return errors.New("invalid prepared file")
		}
		for index, block := range file.Blocks {
			if block.Index != index || !validSHA(block.SHA256) {
				return errors.New("invalid prepared file blocks")
			}
		}
	}
	return nil
}

func insertPreparedOccurrences(ctx context.Context, tx pgx.Tx, command PrepareCommand) (int, error) {
	file, err := os.Open(command.OccurrencePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(io.LimitReader(file, command.OccurrenceBytes+1), hash))
	scanner.Buffer(make([]byte, 64<<10), (1<<20)+(16<<10))
	if !scanner.Scan() {
		return 0, errors.New("Issue occurrence header missing")
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := strictControlJSON(scanner.Bytes(), &header); err != nil || header.Version != 1 {
		return 0, errors.Join(errors.New("invalid Issue occurrence header"), err)
	}
	count := 0
	previous := ""
	batch := make([][]any, 0, 512)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"job_output_occurrences"}, []string{"output_id", "record_id", "tenant_id", "project_id", "acceptance_id", "lane_id", "batch_seq", "ordinal", "event_time_us", "event_time_ns", "received_time_us", "release_json", "issue_id", "grouping_version", "fingerprint_sha256", "title_json"}, pgx.CopyFromRows(batch))
		batch = batch[:0]
		return err
	}
	for scanner.Scan() {
		var occurrence model.IssueOccurrenceSummary
		if err := strictControlJSON(scanner.Bytes(), &occurrence); err != nil {
			return 0, err
		}
		if occurrence.RecordID <= previous || !validSHA(occurrence.RecordID) || occurrence.LaneID != command.LaneID || occurrence.BatchSeq != command.BatchSeq || occurrence.ProjectID <= 0 || occurrence.Ordinal < 0 || occurrence.EventNS > 999 || !validSHA(occurrence.IssueID) || occurrence.IssueID != occurrence.FingerprintSHA256 || occurrence.GroupingVersion != command.Root.Header.GroupingVersion || !validJSONString(occurrence.TitleJSON) || !validOptionalJSONString(occurrence.ReleaseJSON) {
			return 0, errors.New("invalid prepared Issue occurrence")
		}
		batch = append(batch, []any{command.Root.Header.OutputID, occurrence.RecordID, command.TenantID, occurrence.ProjectID, occurrence.AcceptanceID, occurrence.LaneID, occurrence.BatchSeq, occurrence.Ordinal, occurrence.EventTimeUS, occurrence.EventNS, occurrence.ReceivedTimeUS, occurrence.ReleaseJSON, occurrence.IssueID, occurrence.GroupingVersion, occurrence.FingerprintSHA256, occurrence.TitleJSON})
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return 0, err
			}
		}
		previous = occurrence.RecordID
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil || position != command.OccurrenceBytes || hex.EncodeToString(hash.Sum(nil)) != command.OccurrenceSHA {
		return 0, errors.Join(errors.New("Issue occurrence summary changed during Prepare"), err)
	}
	return count, flush()
}

func referencePreparedIntents(ctx context.Context, tx pgx.Tx, command PrepareCommand, expected map[string]preparedIntent) error {
	ids := make([]string, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		want := expected[id]
		var kind, state, owner, expectedSHA, uploadedSHA, jobID string
		var generation, fence, expectedBytes, uploadedBytes, producerGeneration, producerFence int64
		err := tx.QueryRow(ctx, `SELECT kind,state,owner,fence,storage_generation,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256,conversion_job_id::text,producer_generation,producer_fence
			FROM object_intents WHERE tenant_id=$1 AND intent_id=$2 FOR UPDATE`, command.TenantID, id).Scan(&kind, &state, &owner, &fence, &generation, &expectedBytes, &expectedSHA, &uploadedBytes, &uploadedSHA, &jobID, &producerGeneration, &producerFence)
		if err != nil {
			return err
		}
		if kind != want.role || state != "uploaded" || owner != command.Authority.Owner || fence != command.Authority.Fence || generation != command.Authority.StorageGeneration || expectedBytes != want.file.Bytes || uploadedBytes != want.file.Bytes || expectedSHA != want.file.SHA256 || uploadedSHA != want.file.SHA256 || jobID != command.Authority.JobID || producerGeneration != generation || producerFence != command.Authority.Fence {
			return ErrIntentStale
		}
	}
	if len(ids) == 0 {
		return nil
	}
	result, err := tx.Exec(ctx, `UPDATE object_intents SET state='referenced',protect_until=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND intent_id=ANY($2::uuid[]) AND state='uploaded'`, command.TenantID, ids)
	if err != nil {
		return err
	}
	if result.RowsAffected() != int64(len(ids)) {
		return ErrIntentStale
	}
	return nil
}

func strictControlJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func validJSONString(value string) bool {
	var decoded string
	return json.Unmarshal([]byte(value), &decoded) == nil && len([]rune(decoded)) <= 512
}

func validOptionalJSONString(value *string) bool {
	return value == nil || validJSONString(*value)
}
