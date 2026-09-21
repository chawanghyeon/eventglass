package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// Read each role in one native scan, retaining an exact identity/count for each
// input, not just the union. ORDER BY is bounded by the task's native memory and
// spill settings; Go holds one streaming hash and at most128 input counters.
func verifyCompactionInputs(ctx context.Context, db *sql.DB, request CompactionRequest, analyticsPaths, payloadPaths []string) error {
	counts := make([]int64, len(request.Inputs))
	if err := verifyCompactionInputRole(ctx, db, request, analyticsPaths, counts, true); err != nil {
		return err
	}
	return verifyCompactionInputRole(ctx, db, request, payloadPaths, counts, false)
}

func verifyCompactionInputRole(ctx context.Context, db *sql.DB, request CompactionRequest, paths []string, counts []int64, analytics bool) error {
	positions := make(map[string]int, len(paths))
	for index, path := range paths {
		positions[path] = index
	}
	scope := "true"
	var arguments []any
	if analytics {
		day, _ := time.Parse(time.DateOnly, request.EventDay)
		scope = `COALESCE(tenant_id=? AND lane_id=? AND kind=? AND schema_version=? AND grouping_version=? AND event_time_us>=? AND event_time_us<? AND project_id>0 AND batch_seq>0 AND received_time_us IS NOT NULL,false)`
		arguments = []any{request.TenantID, request.LaneID, request.Kind, request.SchemaVersion, request.GroupingVersion, day.UnixMicro(), day.Add(24 * time.Hour).UnixMicro()}
	}
	// Explicit filename=true requests scan provenance. The pinned-engine tests
	// ensure a physical filename column cannot relabel/swizzle paired inputs.
	rows, err := db.QueryContext(ctx, `SELECT filename,record_id,`+scope+` FROM read_parquet(`+parquetPathList(paths)+`,filename=true,hive_partitioning=false) ORDER BY filename,record_id`, arguments...)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := make([]bool, len(paths))
	identity := sha256.New()
	current, previous := -1, ""
	var count int64
	finish := func() error {
		if current < 0 {
			return nil
		}
		if count == 0 || hex.EncodeToString(identity.Sum(nil)) != request.Inputs[current].IdentitySHA256 {
			return errors.New("compaction input pair identity mismatch")
		}
		if analytics {
			counts[current] = count
		} else if counts[current] != count {
			return errors.New("compaction input pair count mismatch")
		}
		return nil
	}
	for rows.Next() {
		var path, recordID string
		var scoped bool
		if err := rows.Scan(&path, &recordID, &scoped); err != nil {
			return err
		}
		index, ok := positions[path]
		if !ok || !scoped {
			return errors.New("compaction input scope mismatch")
		}
		if index != current {
			if err := finish(); err != nil {
				return err
			}
			if seen[index] {
				return errors.New("compaction input rows are not grouped")
			}
			seen[index], current, previous, count = true, index, "", 0
			identity.Reset()
		}
		if err := appendRecordIdentity(identity, recordID, previous); err != nil {
			return err
		}
		previous = recordID
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := finish(); err != nil {
		return err
	}
	for _, present := range seen {
		if !present {
			return errors.New("compaction input is empty or missing")
		}
	}
	return nil
}
