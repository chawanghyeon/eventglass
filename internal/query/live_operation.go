package query

import (
	"errors"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func BuildLiveOperation(plan CompiledPlan, positions [model.LaneCount]LivePosition) (engine.QueryOperation, error) {
	if plan.Where.Text == "" {
		return engine.QueryOperation{}, errors.New("invalid live query plan")
	}
	positionPredicates := make([]string, model.LaneCount)
	arguments := append([]any(nil), plan.Where.Args...)
	for lane, position := range positions {
		if position.BatchSeq < 0 || position.Ordinal < -1 {
			return engine.QueryOperation{}, errors.New("invalid live query position")
		}
		positionPredicates[lane] = `(r.lane_id=? AND (r.batch_seq>? OR (r.batch_seq=? AND r.record_ordinal>?)))`
		arguments = append(arguments, lane, position.BatchSeq, position.BatchSeq, position.Ordinal)
	}
	arguments = append(arguments, LivePageRows+1)
	scanArguments, err := encodeQueryArguments(arguments)
	if err != nil {
		return engine.QueryOperation{}, err
	}
	reduceArguments, _ := encodeQueryArguments([]any{LivePageRows + 1})
	order := `r.lane_id ASC,r.batch_seq ASC,r.record_ordinal ASC,r.record_id ASC`
	return engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "live", MaxRows: LivePageRows + 1,
		Result:    engine.QueryResultPlan{Kind: "live", Limit: LivePageRows, Sort: "live_asc"},
		ScanSQL:   `SELECT ` + listProjection + ` FROM input_rows r WHERE ` + plan.Where.Text + ` AND (` + strings.Join(positionPredicates, ` OR `) + `) ORDER BY ` + order + ` LIMIT ?`,
		ReduceSQL: `SELECT ` + listProjection + ` FROM input_rows r ORDER BY ` + order + ` LIMIT ?`,
		EmptySQL: `SELECT CAST('' AS VARCHAR) record_id,CAST(0 AS BIGINT) project_id,CAST('error' AS VARCHAR) kind,
			CAST(0 AS BIGINT) event_time_us,CAST(0 AS USMALLINT) event_time_ns_remainder,CAST(0 AS BIGINT) received_time_us,
			CAST(0 AS INTEGER) lane_id,CAST(0 AS BIGINT) batch_seq,CAST(0 AS INTEGER) record_ordinal,CAST('' AS VARCHAR) level,
			CAST(NULL AS SMALLINT) severity_number,CAST('' AS VARCHAR) message,CAST(NULL AS VARCHAR) service,
			CAST(NULL AS VARCHAR) environment,CAST(NULL AS VARCHAR) release,CAST(NULL AS VARCHAR) trace_id,
			CAST(NULL AS VARCHAR) issue_id WHERE false ORDER BY lane_id,batch_seq,record_ordinal,record_id`,
		ScanArguments: scanArguments, ReduceArguments: reduceArguments,
	}, nil
}
