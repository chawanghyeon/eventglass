package control

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidVerifiedBatch  = errors.New("invalid verified ingest batch")
	ErrIdempotencyConflict   = errors.New("acceptance ID content conflict")
	ErrAlreadyAcceptedSubset = errors.New("only part of the batch was already accepted")
	ErrAuthorizationStale    = errors.New("ingest authorization snapshot is stale")
	ErrTenantDisabled        = errors.New("ingest tenant is disabled")
	ErrProjectDisabled       = errors.New("ingest project is disabled")
	ErrKeyRevoked            = errors.New("ingest key is revoked")
	ErrIntentStale           = errors.New("journal intent is not live and uploaded")
)

type AuthorizationSnapshot struct {
	TenantRevision  int64
	ProjectRevision int64
	KeyRevision     int64
	ScrubRevision   int
	ConfigRevision  int64
	KeyHash         [32]byte
}

type Candidate struct {
	Position      int
	RecordID      string
	Kind          model.Kind
	SourceEventID string
	DedupeSHA256  string
}

type VerifiedRequest struct {
	Index         model.JournalRequestIndex
	Candidates    []Candidate
	Authorization AuthorizationSnapshot
}

type IntentAuthority struct {
	IntentID string
	Owner    string
	Fence    int64
	Bytes    int64
	SHA256   string
}

type VerifiedBatch struct {
	InstallationID    string
	StorageGeneration int64
	TenantID          int64
	LaneID            int
	BatchID           string
	JobID             string
	Journal           IntentAuthority
	Requests          []VerifiedRequest
}

type ReceiptResult struct {
	AcceptanceID     string
	TenantID         int64
	ProjectID        int64
	LaneID           int
	BatchSeq         int64
	ContentSHA256    string
	Selection        model.ReceiptSelection
	SelectionSHA256  string
	OrdinalFirst     int
	OrdinalLast      int
	AcceptedCount    int
	DuplicateCount   int
	ConflictCount    int
	UnsupportedCount int
	ReceivedTimeUS   int64
}

type ExistingSubsetError struct {
	Existing []ReceiptResult
	Missing  []string
}

func (err *ExistingSubsetError) Error() string { return ErrAlreadyAcceptedSubset.Error() }
func (err *ExistingSubsetError) Unwrap() error { return ErrAlreadyAcceptedSubset }

type receiptQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func validateVerifiedBatch(batch VerifiedBatch) error {
	if batch.TenantID <= 0 || batch.StorageGeneration <= 0 || batch.LaneID < 0 || batch.LaneID >= model.LaneCount || len(batch.Requests) == 0 || len(batch.Requests) > 1000 {
		return ErrInvalidVerifiedBatch
	}
	if _, err := model.LaneForAcceptance(batch.InstallationID); err != nil {
		return fmt.Errorf("%w: installation ID", ErrInvalidVerifiedBatch)
	}
	if _, err := model.LaneForAcceptance(batch.BatchID); err != nil {
		return fmt.Errorf("%w: batch ID", ErrInvalidVerifiedBatch)
	}
	if _, err := model.LaneForAcceptance(batch.JobID); err != nil {
		return fmt.Errorf("%w: job ID", ErrInvalidVerifiedBatch)
	}
	if _, err := model.LaneForAcceptance(batch.Journal.IntentID); err != nil || batch.Journal.Owner == "" || batch.Journal.Fence <= 0 || batch.Journal.Bytes <= 0 || !validSHA(batch.Journal.SHA256) {
		return fmt.Errorf("%w: journal authority", ErrInvalidVerifiedBatch)
	}
	seen := make(map[string]bool, len(batch.Requests))
	position := 0
	for _, request := range batch.Requests {
		lane, err := model.LaneForAcceptance(request.Index.AcceptanceID)
		if err != nil || lane != batch.LaneID || seen[request.Index.AcceptanceID] || request.Index.ProjectID <= 0 || !validSHA(request.Index.ContentSHA256) {
			return fmt.Errorf("%w: request identity", ErrInvalidVerifiedBatch)
		}
		seen[request.Index.AcceptanceID] = true
		if request.Index.OrdinalFirst != position || request.Index.OrdinalLast != position+len(request.Candidates)-1 || request.Index.RecordCount != len(request.Candidates) || len(request.Candidates) > 10000-position || len(request.Index.Outcomes) > 10000 || len(request.Index.UnsupportedItems) > 1000 {
			return fmt.Errorf("%w: request range", ErrInvalidVerifiedBatch)
		}
		for local, candidate := range request.Candidates {
			if candidate.Position != local || !validSHA(candidate.RecordID) || (candidate.Kind != model.KindError && candidate.Kind != model.KindLog && candidate.Kind != model.KindTransaction) {
				return fmt.Errorf("%w: candidate identity", ErrInvalidVerifiedBatch)
			}
			hasDedupe := candidate.SourceEventID != "" || candidate.DedupeSHA256 != ""
			if hasDedupe {
				if (candidate.Kind != model.KindError && candidate.Kind != model.KindTransaction) || len(candidate.SourceEventID) != 32 || candidate.SourceEventID != strings.ToLower(candidate.SourceEventID) || !validSHA(candidate.DedupeSHA256) {
					return fmt.Errorf("%w: candidate dedupe", ErrInvalidVerifiedBatch)
				}
				if _, err := hex.DecodeString(candidate.SourceEventID); err != nil {
					return fmt.Errorf("%w: candidate source ID", ErrInvalidVerifiedBatch)
				}
			}
		}
		if _, err := checkedOutcomeTotals(request.Index.Outcomes); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidVerifiedBatch, err)
		}
		for _, outcome := range request.Index.Outcomes {
			if outcome.ItemOrdinal < 0 || outcome.Category == "" || outcome.Reason == "" || outcome.Quantity < 0 {
				return fmt.Errorf("%w: SDK outcome", ErrInvalidVerifiedBatch)
			}
		}
		for _, item := range request.Index.UnsupportedItems {
			if item.ItemOrdinal < 0 || item.Type == "" || len(item.Type) > 128 || item.Bytes < 0 {
				return fmt.Errorf("%w: unsupported item", ErrInvalidVerifiedBatch)
			}
		}
		if request.Authorization.TenantRevision <= 0 || request.Authorization.ProjectRevision <= 0 || request.Authorization.KeyRevision <= 0 || request.Authorization.ScrubRevision <= 0 || request.Authorization.ConfigRevision <= 0 {
			return fmt.Errorf("%w: authorization revision", ErrInvalidVerifiedBatch)
		}
		position += len(request.Candidates)
	}
	return nil
}

func lookupReceipts(ctx context.Context, queryer receiptQueryer, requests []VerifiedRequest) (map[string]ReceiptResult, error) {
	if len(requests) == 0 {
		return map[string]ReceiptResult{}, nil
	}
	placeholders := make([]string, len(requests))
	arguments := make([]any, len(requests))
	for index, request := range requests {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
		arguments[index] = request.Index.AcceptanceID
	}
	rows, err := queryer.Query(ctx, `SELECT acceptance_id::text,tenant_id,project_id,lane_id,batch_seq,content_sha256,
		selection_json::text,selection_sha256,ordinal_first,ordinal_last,accepted_count,duplicate_count,conflict_count,unsupported_count,received_time_us
		FROM receipts WHERE acceptance_id IN (`+strings.Join(placeholders, ",")+`)`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]ReceiptResult, len(requests))
	for rows.Next() {
		var receipt ReceiptResult
		var selectionJSON string
		if err := rows.Scan(&receipt.AcceptanceID, &receipt.TenantID, &receipt.ProjectID, &receipt.LaneID, &receipt.BatchSeq, &receipt.ContentSHA256,
			&selectionJSON, &receipt.SelectionSHA256, &receipt.OrdinalFirst, &receipt.OrdinalLast, &receipt.AcceptedCount, &receipt.DuplicateCount,
			&receipt.ConflictCount, &receipt.UnsupportedCount, &receipt.ReceivedTimeUS); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(selectionJSON), &receipt.Selection); err != nil {
			return nil, err
		}
		recordCount := receipt.OrdinalLast - receipt.OrdinalFirst + 1
		if encoded, err := receipt.Selection.CanonicalJSON(recordCount); err != nil {
			return nil, err
		} else if digest, _ := receipt.Selection.SHA256(recordCount); digest != receipt.SelectionSHA256 || len(encoded) == 0 {
			return nil, errors.New("stored receipt selection checksum mismatch")
		}
		result[receipt.AcceptanceID] = receipt
	}
	return result, rows.Err()
}

func classifyExisting(batch VerifiedBatch, found map[string]ReceiptResult) ([]ReceiptResult, []string, error) {
	existing := make([]ReceiptResult, 0, len(found))
	missing := make([]string, 0, len(batch.Requests)-len(found))
	for _, request := range batch.Requests {
		receipt, ok := found[request.Index.AcceptanceID]
		if !ok {
			missing = append(missing, request.Index.AcceptanceID)
			continue
		}
		if receipt.TenantID != batch.TenantID || receipt.ProjectID != request.Index.ProjectID || receipt.ContentSHA256 != request.Index.ContentSHA256 {
			return nil, nil, ErrIdempotencyConflict
		}
		existing = append(existing, receipt)
	}
	return existing, missing, nil
}

func orderedResults(requests []VerifiedRequest, found map[string]ReceiptResult) []ReceiptResult {
	result := make([]ReceiptResult, 0, len(requests))
	for _, request := range requests {
		result = append(result, found[request.Index.AcceptanceID])
	}
	return result
}

func validSHA(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func checkedOutcomeTotals(outcomes []model.Outcome) ([]model.Outcome, error) {
	type key struct {
		item             int
		category, reason string
	}
	totals := make(map[key]model.Outcome, len(outcomes))
	for _, outcome := range outcomes {
		id := key{outcome.ItemOrdinal, outcome.Category, outcome.Reason}
		current, exists := totals[id]
		if !exists {
			totals[id] = outcome
			continue
		}
		if outcome.Quantity > math.MaxInt64-current.Quantity {
			return nil, errors.New("SDK outcome quantity overflow")
		}
		current.Quantity += outcome.Quantity
		current.Approximate = current.Approximate || outcome.Approximate
		totals[id] = current
	}
	result := make([]model.Outcome, 0, len(totals))
	for _, outcome := range totals {
		result = append(result, outcome)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ItemOrdinal != result[j].ItemOrdinal {
			return result[i].ItemOrdinal < result[j].ItemOrdinal
		}
		if result[i].Category != result[j].Category {
			return result[i].Category < result[j].Category
		}
		return result[i].Reason < result[j].Reason
	})
	return result, nil
}
