package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxAcceptAttempts = 3

var errRetryAccept = errors.New("retry accept transaction")

type projectAuthorityKey struct {
	projectID int64
	keyHash   [32]byte
}

type sourceKey struct {
	projectID int64
	kind      model.Kind
	sourceID  string
}

type sourceOccurrence struct {
	request int
	local   int
	global  int
	value   Candidate
}

func Accept(ctx context.Context, pool *pgxpool.Pool, batch VerifiedBatch) ([]ReceiptResult, error) {
	return accept(ctx, pool, batch, nil)
}

func accept(ctx context.Context, pool *pgxpool.Pool, batch VerifiedBatch, onTransaction func()) ([]ReceiptResult, error) {
	if pool == nil {
		return nil, ErrInvalidVerifiedBatch
	}
	if err := validateVerifiedBatch(batch); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < maxAcceptAttempts; attempt++ {
		if onTransaction != nil {
			onTransaction()
		}
		result, err := acceptOnce(ctx, pool, batch)
		if err == nil {
			return result, nil
		}
		if !retryableAcceptError(err) || attempt+1 == maxAcceptAttempts {
			return nil, err
		}
	}
	return nil, errRetryAccept
}

func acceptOnce(ctx context.Context, pool *pgxpool.Pool, batch VerifiedBatch) ([]ReceiptResult, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return nil, err
	}

	var installationID string
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation FROM installations WHERE singleton FOR SHARE`).Scan(&installationID, &generation); err != nil {
		return nil, err
	}
	if installationID != batch.InstallationID || generation != batch.StorageGeneration {
		return nil, ErrAuthorizationStale
	}
	var tenantState string
	var tenantRevision int64
	if err := tx.QueryRow(ctx, `SELECT state,auth_revision FROM tenants WHERE tenant_id=$1 FOR SHARE`, batch.TenantID).Scan(&tenantState, &tenantRevision); err != nil {
		return nil, err
	}
	if tenantState != "active" {
		return nil, ErrTenantDisabled
	}

	existing, missing, err := lookupAndClassify(ctx, tx, batch)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return confirmAfterCommitError(ctx, pool, batch, err)
		}
		return existing, nil
	}
	if len(existing) != 0 {
		return nil, &ExistingSubsetError{Existing: existing, Missing: missing}
	}

	retention, err := lockProjectAuthorities(ctx, tx, batch, tenantRevision)
	if err != nil {
		return nil, err
	}
	var acceptedSeq, lastReceived int64
	if err := tx.QueryRow(ctx, `SELECT accepted_seq,last_received_time_us FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, batch.TenantID, batch.LaneID).Scan(&acceptedSeq, &lastReceived); err != nil {
		return nil, err
	}
	if acceptedSeq == math.MaxInt64 {
		return nil, errors.New("lane accepted sequence overflow")
	}
	existing, missing, err = lookupAndClassify(ctx, tx, batch)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return confirmAfterCommitError(ctx, pool, batch, err)
		}
		return existing, nil
	}
	if len(existing) != 0 {
		return nil, &ExistingSubsetError{Existing: existing, Missing: missing}
	}

	classes, err := selectCandidates(ctx, tx, batch, retention)
	if err != nil {
		return nil, err
	}
	if err := lockJournalIntent(ctx, tx, batch); err != nil {
		return nil, err
	}
	var databaseTime int64
	if err := tx.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&databaseTime); err != nil {
		return nil, err
	}
	receivedTime := max(databaseTime, lastReceived)
	batchSeq := acceptedSeq + 1
	results, acceptedCount, duplicateCount, conflictCount, err := buildReceiptResults(batch, classes, batchSeq, receivedTime)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ingest_batches(
		tenant_id,lane_id,batch_seq,batch_id,journal_intent_id,request_count,received_time_us,record_count,accepted_count,duplicate_count,conflict_count,journal_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		batch.TenantID, batch.LaneID, batchSeq, batch.BatchID, batch.Journal.IntentID, len(batch.Requests), receivedTime,
		acceptedCount+duplicateCount+conflictCount, acceptedCount, duplicateCount, conflictCount, batch.Journal.SHA256); err != nil {
		return nil, err
	}
	if err := insertReceiptsAndDiagnostics(ctx, tx, batch, results); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO jobs(job_id,kind,tenant_id,lane_id,batch_seq,state,storage_generation)
		VALUES($1,'convert',$2,$3,$4,'queued',$5)`, batch.JobID, batch.TenantID, batch.LaneID, batchSeq, batch.StorageGeneration); err != nil {
		return nil, err
	}
	command, err := tx.Exec(ctx, `UPDATE object_intents SET state='referenced',updated_at=clock_timestamp()
		WHERE intent_id=$1 AND tenant_id=$2 AND state='uploaded' AND owner=$3 AND fence=$4`,
		batch.Journal.IntentID, batch.TenantID, batch.Journal.Owner, batch.Journal.Fence)
	if err != nil {
		return nil, err
	}
	if command.RowsAffected() != 1 {
		return nil, ErrIntentStale
	}
	command, err = tx.Exec(ctx, `UPDATE lanes SET accepted_seq=$3,last_received_time_us=$4
		WHERE tenant_id=$1 AND lane_id=$2 AND accepted_seq=$5`, batch.TenantID, batch.LaneID, batchSeq, receivedTime, acceptedSeq)
	if err != nil {
		return nil, err
	}
	if command.RowsAffected() != 1 {
		return nil, errRetryAccept
	}
	if err := tx.Commit(ctx); err != nil {
		return confirmAfterCommitError(ctx, pool, batch, err)
	}
	return results, nil
}

func lookupAndClassify(ctx context.Context, queryer receiptQueryer, batch VerifiedBatch) ([]ReceiptResult, []string, error) {
	found, err := lookupReceipts(ctx, queryer, batch.Requests)
	if err != nil {
		return nil, nil, err
	}
	existing, missing, err := classifyExisting(batch, found)
	if err != nil {
		return nil, nil, err
	}
	if len(missing) == 0 {
		existing = orderedResults(batch.Requests, found)
	}
	return existing, missing, nil
}

func lockProjectAuthorities(ctx context.Context, tx pgx.Tx, batch VerifiedBatch, tenantRevision int64) (map[int64]int, error) {
	authorities := make(map[projectAuthorityKey]AuthorizationSnapshot)
	requestIDs := make(map[projectAuthorityKey][]string)
	for _, request := range batch.Requests {
		if request.Authorization.TenantRevision != tenantRevision {
			return nil, &AuthorizationRejectError{Rejected: []AuthorizationRejection{{AcceptanceID: request.Index.AcceptanceID, Cause: ErrAuthorizationStale}}}
		}
		key := projectAuthorityKey{request.Index.ProjectID, request.Authorization.KeyHash}
		if previous, exists := authorities[key]; exists && previous != request.Authorization {
			return nil, &AuthorizationRejectError{Rejected: []AuthorizationRejection{{AcceptanceID: request.Index.AcceptanceID, Cause: ErrAuthorizationStale}}}
		}
		authorities[key] = request.Authorization
		requestIDs[key] = append(requestIDs[key], request.Index.AcceptanceID)
	}
	keys := make([]projectAuthorityKey, 0, len(authorities))
	for key := range authorities {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].projectID != keys[j].projectID {
			return keys[i].projectID < keys[j].projectID
		}
		return string(keys[i].keyHash[:]) < string(keys[j].keyHash[:])
	})
	retention := make(map[int64]int, len(keys))
	var rejected []AuthorizationRejection
	for _, key := range keys {
		expected := authorities[key]
		var projectState, keyState string
		var projectRevision, keyRevision, configRevision int64
		var scrubRevision, retentionDays int
		err := tx.QueryRow(ctx, `SELECT p.state,p.auth_revision,p.scrub_revision,p.config_revision,p.retention_days,k.state,k.revision
			FROM projects p JOIN project_keys k ON k.tenant_id=p.tenant_id AND k.project_id=p.project_id
			WHERE p.tenant_id=$1 AND p.project_id=$2 AND k.key_hash=$3 FOR SHARE OF p,k`,
			batch.TenantID, key.projectID, key.keyHash[:]).Scan(&projectState, &projectRevision, &scrubRevision, &configRevision, &retentionDays, &keyState, &keyRevision)
		if errors.Is(err, pgx.ErrNoRows) {
			for _, acceptanceID := range requestIDs[key] {
				rejected = append(rejected, AuthorizationRejection{AcceptanceID: acceptanceID, Cause: ErrKeyRevoked})
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if projectState != "active" {
			for _, acceptanceID := range requestIDs[key] {
				rejected = append(rejected, AuthorizationRejection{AcceptanceID: acceptanceID, Cause: ErrProjectDisabled})
			}
			continue
		}
		if keyState != "active" {
			for _, acceptanceID := range requestIDs[key] {
				rejected = append(rejected, AuthorizationRejection{AcceptanceID: acceptanceID, Cause: ErrKeyRevoked})
			}
			continue
		}
		if projectRevision != expected.ProjectRevision || keyRevision != expected.KeyRevision || scrubRevision != expected.ScrubRevision || configRevision != expected.ConfigRevision {
			for _, acceptanceID := range requestIDs[key] {
				rejected = append(rejected, AuthorizationRejection{AcceptanceID: acceptanceID, Cause: ErrAuthorizationStale})
			}
			continue
		}
		retention[key.projectID] = retentionDays
	}
	if len(rejected) != 0 {
		return nil, &AuthorizationRejectError{Rejected: rejected}
	}
	return retention, nil
}

func selectCandidates(ctx context.Context, tx pgx.Tx, batch VerifiedBatch, retention map[int64]int) ([][]model.ReceiptClass, error) {
	classes := make([][]model.ReceiptClass, len(batch.Requests))
	groups := make(map[sourceKey][]sourceOccurrence)
	for requestIndex, request := range batch.Requests {
		classes[requestIndex] = make([]model.ReceiptClass, len(request.Candidates))
		for local, candidate := range request.Candidates {
			classes[requestIndex][local] = model.ReceiptAccepted
			if candidate.SourceEventID == "" {
				continue
			}
			key := sourceKey{request.Index.ProjectID, candidate.Kind, candidate.SourceEventID}
			groups[key] = append(groups[key], sourceOccurrence{requestIndex, local, request.Index.OrdinalFirst + local, candidate})
		}
	}
	keys := make([]sourceKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].projectID != keys[j].projectID {
			return keys[i].projectID < keys[j].projectID
		}
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].sourceID < keys[j].sourceID
	})
	for _, key := range keys {
		occurrences := groups[key]
		sort.Slice(occurrences, func(i, j int) bool { return occurrences[i].global < occurrences[j].global })
		winner := occurrences[0]
		request := batch.Requests[winner.request]
		command, err := tx.Exec(ctx, `INSERT INTO event_dedupe(tenant_id,project_id,kind,source_event_id,record_id,receipt_acceptance_id,payload_sha256,expires_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,clock_timestamp()+make_interval(days=>$8)) ON CONFLICT DO NOTHING`,
			batch.TenantID, key.projectID, key.kind, key.sourceID, winner.value.RecordID, request.Index.AcceptanceID, winner.value.DedupeSHA256, retention[key.projectID])
		if err != nil {
			return nil, err
		}
		comparisonHash := winner.value.DedupeSHA256
		winnerAccepted := command.RowsAffected() == 1
		if !winnerAccepted {
			var existingHash string
			var expired bool
			err := tx.QueryRow(ctx, `SELECT payload_sha256,expires_at<=clock_timestamp() FROM event_dedupe
				WHERE tenant_id=$1 AND project_id=$2 AND kind=$3 AND source_event_id=$4 FOR UPDATE`,
				batch.TenantID, key.projectID, key.kind, key.sourceID).Scan(&existingHash, &expired)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errRetryAccept
			}
			if err != nil {
				return nil, err
			}
			if expired {
				command, err := tx.Exec(ctx, `UPDATE event_dedupe SET record_id=$5,receipt_acceptance_id=$6,payload_sha256=$7,
					expires_at=clock_timestamp()+make_interval(days=>$8),created_at=clock_timestamp()
					WHERE tenant_id=$1 AND project_id=$2 AND kind=$3 AND source_event_id=$4`,
					batch.TenantID, key.projectID, key.kind, key.sourceID, winner.value.RecordID, request.Index.AcceptanceID, winner.value.DedupeSHA256, retention[key.projectID])
				if err != nil {
					return nil, err
				}
				if command.RowsAffected() != 1 {
					return nil, errRetryAccept
				}
				winnerAccepted = true
			} else {
				comparisonHash = existingHash
			}
		}
		start := 0
		if winnerAccepted {
			start = 1
		}
		for index := start; index < len(occurrences); index++ {
			occurrence := occurrences[index]
			if occurrence.value.DedupeSHA256 == comparisonHash {
				classes[occurrence.request][occurrence.local] = model.ReceiptDuplicate
			} else {
				classes[occurrence.request][occurrence.local] = model.ReceiptConflict
			}
		}
	}
	return classes, nil
}

func lockJournalIntent(ctx context.Context, tx pgx.Tx, batch VerifiedBatch) error {
	var installationID, state, owner, expectedSHA string
	var generation, fence, expectedBytes int64
	var uploadedBytes *int64
	var uploadedSHA *string
	var live bool
	err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,state,owner,fence,expected_bytes,expected_sha256,
		uploaded_bytes,uploaded_sha256,expires_at>clock_timestamp()
		FROM object_intents WHERE tenant_id=$1 AND intent_id=$2 FOR UPDATE`, batch.TenantID, batch.Journal.IntentID).
		Scan(&installationID, &generation, &state, &owner, &fence, &expectedBytes, &expectedSHA, &uploadedBytes, &uploadedSHA, &live)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrIntentStale
	}
	if err != nil {
		return err
	}
	if installationID != batch.InstallationID || generation != batch.StorageGeneration || state != "uploaded" || owner != batch.Journal.Owner || fence != batch.Journal.Fence ||
		expectedBytes != batch.Journal.Bytes || expectedSHA != batch.Journal.SHA256 || uploadedBytes == nil || uploadedSHA == nil || *uploadedBytes != batch.Journal.Bytes || *uploadedSHA != batch.Journal.SHA256 || !live {
		return ErrIntentStale
	}
	return nil
}

func buildReceiptResults(batch VerifiedBatch, classes [][]model.ReceiptClass, batchSeq, receivedTime int64) ([]ReceiptResult, int, int, int, error) {
	results := make([]ReceiptResult, len(batch.Requests))
	acceptedTotal, duplicateTotal, conflictTotal := 0, 0, 0
	for index, request := range batch.Requests {
		selection, err := model.NewReceiptSelection(classes[index])
		if err != nil {
			return nil, 0, 0, 0, err
		}
		digest, err := selection.SHA256(len(classes[index]))
		if err != nil {
			return nil, 0, 0, 0, err
		}
		accepted, duplicate, conflict := 0, 0, 0
		for _, class := range classes[index] {
			switch class {
			case model.ReceiptAccepted:
				accepted++
			case model.ReceiptDuplicate:
				duplicate++
			case model.ReceiptConflict:
				conflict++
			}
		}
		acceptedTotal += accepted
		duplicateTotal += duplicate
		conflictTotal += conflict
		results[index] = ReceiptResult{
			AcceptanceID: request.Index.AcceptanceID, TenantID: batch.TenantID, ProjectID: request.Index.ProjectID, LaneID: batch.LaneID, BatchSeq: batchSeq,
			ContentSHA256: request.Index.ContentSHA256, Selection: selection, SelectionSHA256: digest,
			OrdinalFirst: request.Index.OrdinalFirst, OrdinalLast: request.Index.OrdinalLast, AcceptedCount: accepted, DuplicateCount: duplicate, ConflictCount: conflict,
			UnsupportedCount: len(request.Index.UnsupportedItems), ReceivedTimeUS: receivedTime,
		}
	}
	return results, acceptedTotal, duplicateTotal, conflictTotal, nil
}

func insertReceiptsAndDiagnostics(ctx context.Context, tx pgx.Tx, batch VerifiedBatch, results []ReceiptResult) error {
	for requestIndex, request := range batch.Requests {
		result := results[requestIndex]
		selectionJSON, err := result.Selection.CanonicalJSON(len(request.Candidates))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO receipts(acceptance_id,tenant_id,project_id,lane_id,batch_seq,content_sha256,request_index,
			selection_json,selection_sha256,ordinal_first,ordinal_last,accepted_count,duplicate_count,conflict_count,unsupported_count,received_time_us,
			policy_revision,normalizer_version,grouping_version,dedupe_hash_version)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,1,$19)`,
			request.Index.AcceptanceID, batch.TenantID, request.Index.ProjectID, batch.LaneID, result.BatchSeq, request.Index.ContentSHA256, requestIndex,
			string(selectionJSON), result.SelectionSHA256, request.Index.OrdinalFirst, request.Index.OrdinalLast, result.AcceptedCount, result.DuplicateCount,
			result.ConflictCount, len(request.Index.UnsupportedItems), result.ReceivedTimeUS, request.Authorization.ScrubRevision, model.NormalizerVersion, 1)
		if err != nil {
			return err
		}
		outcomes, err := checkedOutcomeTotals(request.Index.Outcomes)
		if err != nil {
			return err
		}
		for _, outcome := range outcomes {
			categoryJSON, _ := json.Marshal(outcome.Category)
			reasonJSON, _ := json.Marshal(outcome.Reason)
			categorySHA := sha256.Sum256(categoryJSON)
			reasonSHA := sha256.Sum256(reasonJSON)
			command, err := tx.Exec(ctx, `INSERT INTO sdk_outcomes(acceptance_id,item_ordinal,category_json,reason_json,category_sha256,reason_sha256,quantity,approximate)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(acceptance_id,item_ordinal,category_sha256,reason_sha256) DO NOTHING`,
				request.Index.AcceptanceID, outcome.ItemOrdinal, string(categoryJSON), string(reasonJSON), categorySHA[:], reasonSHA[:], outcome.Quantity, outcome.Approximate)
			if err != nil {
				return err
			}
			if command.RowsAffected() == 0 {
				var storedCategory, storedReason string
				if err := tx.QueryRow(ctx, `SELECT category_json,reason_json FROM sdk_outcomes
					WHERE acceptance_id=$1 AND item_ordinal=$2 AND category_sha256=$3 AND reason_sha256=$4`,
					request.Index.AcceptanceID, outcome.ItemOrdinal, categorySHA[:], reasonSHA[:]).Scan(&storedCategory, &storedReason); err != nil {
					return err
				}
				if storedCategory != string(categoryJSON) || storedReason != string(reasonJSON) {
					return errors.New("SDK outcome hash collision")
				}
				return errors.New("duplicate aggregated SDK outcome")
			}
		}
		for _, item := range request.Index.UnsupportedItems {
			typeJSON, _ := json.Marshal(item.Type)
			if _, err := tx.Exec(ctx, `INSERT INTO receipt_unsupported(acceptance_id,item_ordinal,item_type_json,byte_count) VALUES($1,$2,$3,$4)`,
				request.Index.AcceptanceID, item.ItemOrdinal, string(typeJSON), item.Bytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func confirmAfterCommitError(ctx context.Context, pool *pgxpool.Pool, batch VerifiedBatch, commitErr error) ([]ReceiptResult, error) {
	found, err := lookupReceipts(ctx, pool, batch.Requests)
	if err != nil {
		return nil, errors.Join(commitErr, err)
	}
	_, missing, err := classifyExisting(batch, found)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		return orderedResults(batch.Requests, found), nil
	}
	return nil, commitErr
}

func retryableAcceptError(err error) bool {
	if errors.Is(err, errRetryAccept) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}
