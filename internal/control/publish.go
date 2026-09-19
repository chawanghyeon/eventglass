package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PublishCommand struct {
	Authority   JobAuthority
	TenantID    int64
	LaneID      int
	BatchSeq    int64
	OutputID    string
	ManifestSHA string
}

type PublishResult struct {
	CatalogGeneration int64
	AlreadyPublished  bool
}

func Publish(ctx context.Context, pool *pgxpool.Pool, command PublishCommand) (PublishResult, error) {
	if err := validatePublishCommand(command, pool); err != nil {
		return PublishResult{}, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PublishResult{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return PublishResult{}, err
	}
	if err := lockRuntimeGeneration(ctx, tx, command.Authority.InstallationID, command.Authority.StorageGeneration); err != nil {
		return PublishResult{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id=$1 FOR SHARE`, command.TenantID); err != nil {
		return PublishResult{}, err
	}
	var publishedSeq, generation int64
	if err := tx.QueryRow(ctx, `SELECT published_seq,catalog_generation FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, command.TenantID, command.LaneID).Scan(&publishedSeq, &generation); err != nil {
		return PublishResult{}, err
	}
	already, err := lockPublishJob(ctx, tx, command)
	if err != nil {
		return PublishResult{}, err
	}
	if already {
		if err := tx.Commit(ctx); err != nil {
			return PublishResult{}, err
		}
		return PublishResult{CatalogGeneration: generation, AlreadyPublished: true}, nil
	}
	if command.BatchSeq != publishedSeq+1 || generation == math.MaxInt64 {
		return PublishResult{}, errors.New("publication is not the next lane batch")
	}
	root, err := lockPreparedOutput(ctx, tx, command)
	if err != nil {
		return PublishResult{}, err
	}
	if err := validatePublishBatch(ctx, tx, command, root); err != nil {
		return PublishResult{}, err
	}
	intents, totals, err := inspectDurableParts(ctx, tx, command, root)
	if err != nil {
		return PublishResult{}, err
	}
	if totals.bundles != root.Header.BundleCount || totals.rows != int64(root.Header.SelectedRecordCount) || totals.errorRows != int64(root.Header.SelectedErrorCount) {
		return PublishResult{}, errors.New("durable manifest totals differ from root")
	}
	if err := lockPublishIntents(ctx, tx, command, intents); err != nil {
		return PublishResult{}, err
	}
	newGeneration := generation + 1
	if err := insertPublishedCatalog(ctx, tx, command, root, newGeneration); err != nil {
		return PublishResult{}, err
	}
	if err := publishIssueOccurrences(ctx, tx, command); err != nil {
		return PublishResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE job_outputs SET state='published' WHERE output_id=$1 AND state='prepared'`, command.OutputID); err != nil {
		return PublishResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE ingest_batches SET state='published' WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 AND state='accepted'`, command.TenantID, command.LaneID, command.BatchSeq); err != nil {
		return PublishResult{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
		WHERE job_id=$1 AND state='running' AND owner=$2 AND fence=$3`, command.Authority.JobID, command.Authority.Owner, command.Authority.Fence)
	if err != nil || result.RowsAffected() != 1 {
		return PublishResult{}, errors.Join(ErrJobFenceStale, err)
	}
	result, err = tx.Exec(ctx, `UPDATE lanes SET published_seq=$3,catalog_generation=$4 WHERE tenant_id=$1 AND lane_id=$2 AND published_seq=$5 AND catalog_generation=$6`, command.TenantID, command.LaneID, command.BatchSeq, newGeneration, publishedSeq, generation)
	if err != nil || result.RowsAffected() != 1 {
		return PublishResult{}, errors.Join(errors.New("lane publication compare-and-swap failed"), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PublishResult{}, err
	}
	return PublishResult{CatalogGeneration: newGeneration}, nil
}

func validatePublishCommand(command PublishCommand, pool *pgxpool.Pool) error {
	if pool == nil || command.TenantID <= 0 || command.LaneID < 0 || command.LaneID >= model.LaneCount || command.BatchSeq <= 0 || command.OutputID == "" || !validSHA(command.ManifestSHA) || command.Authority.InstallationID == "" || command.Authority.StorageGeneration <= 0 || command.Authority.JobID == "" || command.Authority.Owner == "" || command.Authority.Fence <= 0 {
		return errors.New("invalid Publish command")
	}
	return nil
}

func lockPublishJob(ctx context.Context, tx pgx.Tx, command PublishCommand) (bool, error) {
	var tenantID int64
	var laneID int
	var batchSeq, generation, fence int64
	var state, owner string
	var outputID *string
	var leaseLive bool
	err := tx.QueryRow(ctx, `SELECT tenant_id,lane_id,batch_seq,storage_generation,state,COALESCE(owner,''),fence,prepared_output_id::text,COALESCE(lease_until>clock_timestamp(),false)
		FROM jobs WHERE job_id=$1 FOR UPDATE`, command.Authority.JobID).Scan(&tenantID, &laneID, &batchSeq, &generation, &state, &owner, &fence, &outputID, &leaseLive)
	if err != nil {
		return false, err
	}
	if tenantID != command.TenantID || laneID != command.LaneID || batchSeq != command.BatchSeq || generation != command.Authority.StorageGeneration || outputID == nil || *outputID != command.OutputID {
		return false, ErrJobFenceStale
	}
	if state == "completed" {
		var manifestSHA, outputState string
		if err := tx.QueryRow(ctx, `SELECT manifest_sha256,state FROM job_outputs WHERE output_id=$1`, command.OutputID).Scan(&manifestSHA, &outputState); err != nil {
			return false, err
		}
		if manifestSHA == command.ManifestSHA && outputState == "published" {
			return true, nil
		}
		return false, errors.New("completed job conflicts with Publish command")
	}
	if state != "running" || owner != command.Authority.Owner || fence != command.Authority.Fence || !leaseLive {
		return false, ErrJobFenceStale
	}
	return false, nil
}

func lockPreparedOutput(ctx context.Context, tx pgx.Tx, command PublishCommand) (model.OutputManifestRoot, error) {
	var encoded []byte
	var sha, state string
	if err := tx.QueryRow(ctx, `SELECT header_json,manifest_sha256,state FROM job_outputs WHERE tenant_id=$1 AND output_id=$2 AND job_id=$3 FOR UPDATE`, command.TenantID, command.OutputID, command.Authority.JobID).Scan(&encoded, &sha, &state); err != nil {
		return model.OutputManifestRoot{}, err
	}
	if sha != command.ManifestSHA || state != "prepared" {
		return model.OutputManifestRoot{}, errors.New("prepared output identity mismatch")
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != sha {
		return model.OutputManifestRoot{}, errors.New("stored manifest root checksum mismatch")
	}
	var root model.OutputManifestRoot
	if err := strictControlJSON(encoded, &root); err != nil {
		return root, err
	}
	canonical, err := root.CanonicalJSON()
	if err != nil || !bytesEqual(canonical, encoded) || root.Header.OutputID != command.OutputID || root.Header.JobID != command.Authority.JobID || root.Header.TenantID != command.TenantID || root.Header.LaneID != command.LaneID || root.Header.BatchSeq != command.BatchSeq {
		return root, errors.Join(errors.New("stored manifest root is noncanonical or out of scope"), err)
	}
	return root, nil
}

func validatePublishBatch(ctx context.Context, tx pgx.Tx, command PublishCommand, root model.OutputManifestRoot) error {
	var state, journalSHA string
	var accepted int
	if err := tx.QueryRow(ctx, `SELECT state,journal_sha256,accepted_count FROM ingest_batches WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 FOR UPDATE`, command.TenantID, command.LaneID, command.BatchSeq).Scan(&state, &journalSHA, &accepted); err != nil {
		return err
	}
	if state != "accepted" || journalSHA != root.Header.JournalSHA256 || accepted != root.Header.SelectedRecordCount {
		return errors.New("accepted input differs from prepared output")
	}
	return nil
}

type publicationTotals struct {
	bundles   int
	rows      int64
	errorRows int64
}

func inspectDurableParts(ctx context.Context, tx pgx.Tx, command PublishCommand, root model.OutputManifestRoot) (map[string]preparedIntent, publicationTotals, error) {
	intents := make(map[string]preparedIntent)
	var totals publicationTotals
	err := walkDurableParts(ctx, tx, command.OutputID, root, func(bundle model.BundleManifest) error {
		if err := validatePreparedBundle(bundle, PrepareCommand{TenantID: command.TenantID, LaneID: command.LaneID, BatchSeq: command.BatchSeq}); err != nil {
			return err
		}
		for role, file := range map[string]model.FileManifest{"analytics": bundle.Analytics, "payload": bundle.Payload} {
			if _, exists := intents[file.IntentID]; exists {
				return errors.New("durable manifest reuses an output intent")
			}
			intents[file.IntentID] = preparedIntent{role: role, file: file}
		}
		totals.bundles++
		totals.rows += bundle.RowCount
		if bundle.Kind == model.KindError {
			totals.errorRows += bundle.RowCount
		}
		return nil
	})
	return intents, totals, err
}

func walkDurableParts(ctx context.Context, tx pgx.Tx, outputID string, root model.OutputManifestRoot, consume func(model.BundleManifest) error) error {
	for index := range root.Parts {
		var partIndex int
		var encoded []byte
		var sha string
		if err := tx.QueryRow(ctx, `SELECT part_index,metadata_json,metadata_sha256 FROM job_output_parts WHERE output_id=$1 AND part_index=$2`, outputID, index).Scan(&partIndex, &encoded, &sha); err != nil {
			return err
		}
		if partIndex != index || root.Parts[index].Index != index || root.Parts[index].SHA256 != sha {
			return errors.New("durable manifest part sequence mismatch")
		}
		digest := sha256.Sum256(encoded)
		if hex.EncodeToString(digest[:]) != sha {
			return errors.New("durable manifest part checksum mismatch")
		}
		var part model.OutputManifestPart
		if err := strictControlJSON(encoded, &part); err != nil || part.Version != model.OutputManifestVersion || part.Index != index || len(part.Bundles) == 0 {
			return errors.Join(errors.New("invalid durable manifest part"), err)
		}
		canonical, err := json.Marshal(part)
		if err != nil || !bytesEqual(canonical, encoded) {
			return errors.Join(errors.New("durable manifest part is noncanonical"), err)
		}
		for _, bundle := range part.Bundles {
			if err := consume(bundle); err != nil {
				return err
			}
		}
	}
	var storedParts int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM job_output_parts WHERE output_id=$1`, outputID).Scan(&storedParts); err != nil || storedParts != len(root.Parts) {
		return errors.Join(errors.New("durable manifest parts are incomplete"), err)
	}
	return nil
}

func lockPublishIntents(ctx context.Context, tx pgx.Tx, command PublishCommand, expected map[string]preparedIntent) error {
	ids := make([]string, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		want := expected[id]
		var kind, state, owner, sha, uploadedSHA, jobID string
		var generation, fence, bytes, uploadedBytes, producerGeneration, producerFence int64
		if err := tx.QueryRow(ctx, `SELECT kind,state,owner,fence,storage_generation,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256,conversion_job_id::text,producer_generation,producer_fence
			FROM object_intents WHERE tenant_id=$1 AND intent_id=$2 FOR UPDATE`, command.TenantID, id).Scan(&kind, &state, &owner, &fence, &generation, &bytes, &sha, &uploadedBytes, &uploadedSHA, &jobID, &producerGeneration, &producerFence); err != nil {
			return err
		}
		if kind != want.role || state != "referenced" || owner != command.Authority.Owner || fence != command.Authority.Fence || generation != command.Authority.StorageGeneration || bytes != want.file.Bytes || uploadedBytes != bytes || sha != want.file.SHA256 || uploadedSHA != sha || jobID != command.Authority.JobID || producerGeneration != generation || producerFence != fence {
			return ErrIntentStale
		}
	}
	return nil
}

func insertPublishedCatalog(ctx context.Context, tx pgx.Tx, command PublishCommand, root model.OutputManifestRoot, generation int64) error {
	return walkDurableParts(ctx, tx, command.OutputID, root, func(bundle model.BundleManifest) error {
		_, err := tx.Exec(ctx, `INSERT INTO bundles(bundle_id,tenant_id,lane_id,schema_version,grouping_version,event_day,kind,input_seq_min,input_seq_max,row_count,identity_sha256,valid_from_generation)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, bundle.BundleID, command.TenantID, command.LaneID, model.SchemaVersion, root.Header.GroupingVersion, bundle.EventDay, bundle.Kind, bundle.InputSeqMin, bundle.InputSeqMax, bundle.RowCount, bundle.IdentitySHA256, generation)
		if err != nil {
			return err
		}
		for _, file := range []model.FileManifest{bundle.Analytics, bundle.Payload} {
			if _, err := tx.Exec(ctx, `INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, file.FileID, command.TenantID, bundle.BundleID, file.IntentID, file.Role, file.Bytes, file.SHA256, file.RowCount, file.MinEventTimeUS, file.MaxEventTimeUS, file.MinReceivedTimeUS, file.MaxReceivedTimeUS, file.MinBatchSeq, file.MaxBatchSeq); err != nil {
				return err
			}
			for _, block := range file.Blocks {
				if _, err := tx.Exec(ctx, `INSERT INTO file_blocks(file_id,block_index,sha256) VALUES($1,$2,$3)`, file.FileID, block.Index, block.SHA256); err != nil {
					return err
				}
			}
		}
		for _, projectID := range bundle.ProjectIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, command.TenantID, bundle.BundleID, projectID); err != nil {
				return err
			}
		}
		return nil
	})
}

func bytesEqual(left, right []byte) bool {
	return len(left) == len(right) && string(left) == string(right)
}

func deterministicTransitionID(issueID string, revision int64) string {
	digest := sha256.Sum256([]byte(issueID + ":" + strconv.FormatInt(revision, 10)))
	value := digest[:16]
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(value)
	return hexValue[:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:]
}
