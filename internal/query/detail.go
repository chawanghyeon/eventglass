package query

import (
	"errors"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

const detailProjection = `p.record_id,p.raw_json,p.envelope_sdk_json,p.normalization_warnings_json,p.canonical_metadata_json,
	p.received_time_us,p.lane_id,p.batch_seq,p.record_ordinal,p.issue_id,p.grouping_version`

func BuildDetailOperation(plan CompiledPlan, recordID string) (engine.QueryOperation, error) {
	if plan.Where.Text == "" || !validRecordID(recordID) {
		return engine.QueryOperation{}, errors.New("invalid detail operation")
	}
	arguments := append([]any(nil), plan.Where.Args...)
	arguments = append(arguments, recordID)
	scanArguments, err := encodeQueryArguments(arguments)
	if err != nil {
		return engine.QueryOperation{}, err
	}
	return engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "detail", MaxRows: 2,
		Result: engine.QueryResultPlan{Kind: "detail", Limit: 1, RecordID: recordID},
		ScanSQL: `SELECT p.record_id,p.raw_json,p.envelope_sdk_json,p.normalization_warnings_json,p.canonical_metadata_json,
			r.received_time_us,r.lane_id,r.batch_seq,r.record_ordinal,r.issue_id,r.grouping_version
			FROM input_rows r JOIN input_payload p USING(record_id) WHERE ` + plan.Where.Text + ` AND r.record_id=? LIMIT 2`,
		ReduceSQL: `SELECT ` + detailProjection + ` FROM input_rows p LIMIT 2`,
		EmptySQL: `SELECT CAST('' AS VARCHAR) record_id,CAST('' AS VARCHAR) raw_json,CAST(NULL AS VARCHAR) envelope_sdk_json,
			CAST('[]' AS VARCHAR) normalization_warnings_json,CAST('{}' AS VARCHAR) canonical_metadata_json,
			CAST(0 AS BIGINT) received_time_us,CAST(0 AS INTEGER) lane_id,CAST(0 AS BIGINT) batch_seq,
			CAST(0 AS INTEGER) record_ordinal,CAST(NULL AS VARCHAR) issue_id,CAST(NULL AS INTEGER) grouping_version WHERE false`,
		ScanArguments: scanArguments,
	}, nil
}

func validRecordID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
