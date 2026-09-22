package query

import (
	"bytes"
	"slices"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

// pruneFirstPageRows proves a lower bound on the (limit+1)th row's primary
// descending sort key. Only fully matching files contribute their row counts.
// Every file whose maximum can tie or beat that bound remains, including files
// that overlap the dataset only partly. Secondary tie keys are not in catalog
// metadata, so equality MUST NOT be pruned. All catalog HEAD checks have already
// completed; this only reduces native scan work, never integrity verification.
func pruneFirstPageRows(scope PlanScope, files []model.CatalogFile) []model.CatalogFile {
	snapshot := scope.Snapshot
	if snapshot == nil || snapshot.TenantID != scope.TenantID || snapshot.SnapshotID != scope.SnapshotID || snapshot.StorageGeneration != scope.Generation {
		return files
	}
	var operation engine.QueryOperation
	if decodeExact(scope.Operation, &operation) != nil || operation.Kind != "rows" {
		return files
	}
	dataset, err := DecodeDatasetIdentity(snapshot.DatasetBytes)
	if err != nil || dataset.TenantID != scope.TenantID {
		return files
	}
	filter, err := DecodeCanonicalFilter(dataset.Filter)
	if err != nil || filter.Op != "constant" || !filter.Constant {
		return files
	}
	snapshotScope := model.SnapshotScope{RetentionFloorUS: snapshot.RetentionFloorUS}
	for lane, cut := range snapshot.Lanes {
		if cut.LaneID != lane {
			return files
		}
		snapshotScope.LaneCuts[lane] = cut.CutSeq
	}
	compiled, err := BuildPlan(dataset, snapshotScope, filter)
	if err != nil {
		return files
	}
	expected, err := BuildRowOperation(RowOperationSpec{Plan: compiled, Sort: operation.Result.Sort, Limit: operation.Result.Limit})
	if err != nil {
		return files
	}
	// Reconstruct the exact existing first-page operation instead of guessing
	// whether SQL contains a cursor/additional predicate. CursorHash binds the
	// response token, not a scan predicate, and is copied without interpreting it.
	expected.Result.CursorHash = operation.Result.CursorHash
	encoded, err := CanonicalOperation(expected)
	if err != nil || !bytes.Equal(encoded, scope.Operation) {
		return files
	}
	type lowerBound struct{ minimum, rows int64 }
	bounds := make([]lowerBound, 0, len(files))
	for _, file := range files {
		if !file.AllProjectsSelected || file.RowCount <= 0 || file.LaneID < 0 || file.LaneID >= model.LaneCount ||
			file.MinBatchSeq <= 0 || file.MinBatchSeq > file.MaxBatchSeq || file.MaxBatchSeq > snapshotScope.LaneCuts[file.LaneID] ||
			file.MinReceivedTimeUS < snapshot.RetentionFloorUS || file.MinReceivedTimeUS > file.MaxReceivedTimeUS || file.MinEventTimeUS > file.MaxEventTimeUS ||
			len(dataset.Kinds) > 0 && !slices.Contains(dataset.Kinds, file.Kind) {
			continue
		}
		minimum, maximum := file.MinEventTimeUS, file.MaxEventTimeUS
		if dataset.TimeBasis == model.QueryTimeReceived {
			minimum, maximum = file.MinReceivedTimeUS, file.MaxReceivedTimeUS
		}
		if minimum < dataset.StartUS || maximum >= dataset.EndUS {
			continue
		}
		minimum = file.MinEventTimeUS
		if operation.Result.Sort == "received_desc" {
			minimum = file.MinReceivedTimeUS
		}
		bounds = append(bounds, lowerBound{minimum: minimum, rows: file.RowCount})
	}
	slices.SortFunc(bounds, func(left, right lowerBound) int {
		if left.minimum > right.minimum {
			return -1
		}
		if left.minimum < right.minimum {
			return 1
		}
		return 0
	})
	remaining := int64(operation.Result.Limit) + 1
	for _, bound := range bounds {
		// Saturating subtraction avoids trusting a sum of catalog row counts
		// that could overflow. The limit itself is validated by BuildRowOperation.
		if bound.rows < remaining {
			remaining -= bound.rows
			continue
		}
		selected := make([]model.CatalogFile, 0, len(files))
		for _, file := range files {
			minimum, maximum := file.MinEventTimeUS, file.MaxEventTimeUS
			if operation.Result.Sort == "received_desc" {
				minimum, maximum = file.MinReceivedTimeUS, file.MaxReceivedTimeUS
			}
			if minimum > maximum || maximum >= bound.minimum {
				selected = append(selected, file)
			}
		}
		return selected
	}
	return files
}
