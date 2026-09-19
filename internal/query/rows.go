package query

import (
	"errors"
	"strconv"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

const listProjection = `r.record_id,r.project_id,r.kind,r.event_time_us,r.event_time_ns_remainder,r.received_time_us,
	r.lane_id,r.batch_seq,r.record_ordinal,r.level,r.severity_number,r.message,r.service,r.environment,r.release,r.trace_id,r.issue_id`

type RowOperationSpec struct {
	Plan   CompiledPlan
	Sort   string
	Limit  int
	Cursor *CursorTuple
}

func BuildRowOperation(spec RowOperationSpec) (engine.QueryOperation, error) {
	if spec.Limit < 1 || spec.Limit > 1000 || spec.Plan.Where.Text == "" {
		return engine.QueryOperation{}, errors.New("invalid row operation")
	}
	order, emptyOrder := "", ""
	switch spec.Sort {
	case "event_desc":
		order = `r.event_time_us DESC,r.event_time_ns_remainder DESC,r.record_id DESC`
		emptyOrder = `event_time_us,event_time_ns_remainder,record_id`
	case "received_desc":
		order = `r.received_time_us DESC,r.lane_id DESC,r.batch_seq DESC,r.record_ordinal DESC,r.record_id DESC`
		emptyOrder = `received_time_us,lane_id,batch_seq,record_ordinal,record_id`
	default:
		return engine.QueryOperation{}, errors.New("unsupported row sort")
	}
	where := spec.Plan.Where.Text
	arguments := append([]any(nil), spec.Plan.Where.Args...)
	if spec.Cursor != nil {
		cursor, err := CompileCursorPredicate(spec.Sort, *spec.Cursor)
		if err != nil {
			return engine.QueryOperation{}, err
		}
		where += " AND (" + cursor.Text + ")"
		arguments = append(arguments, cursor.Args...)
	}
	fetch := spec.Limit + 1
	arguments = append(arguments, fetch)
	scanArguments, err := encodeQueryArguments(arguments)
	if err != nil {
		return engine.QueryOperation{}, err
	}
	reduceArguments, _ := encodeQueryArguments([]any{fetch})
	return engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "rows", MaxRows: int64(fetch),
		ScanSQL:   `SELECT ` + listProjection + ` FROM input_rows r WHERE ` + where + ` ORDER BY ` + order + ` LIMIT ?`,
		ReduceSQL: `SELECT ` + listProjection + ` FROM input_rows r ORDER BY ` + order + ` LIMIT ?`,
		EmptySQL: `SELECT CAST('' AS VARCHAR) record_id,CAST(0 AS BIGINT) project_id,CAST('error' AS VARCHAR) kind,
			CAST(0 AS BIGINT) event_time_us,CAST(0 AS USMALLINT) event_time_ns_remainder,CAST(0 AS BIGINT) received_time_us,
			CAST(0 AS INTEGER) lane_id,CAST(0 AS BIGINT) batch_seq,CAST(0 AS INTEGER) record_ordinal,CAST('' AS VARCHAR) level,
			CAST(NULL AS SMALLINT) severity_number,CAST('' AS VARCHAR) message,CAST(NULL AS VARCHAR) service,
			CAST(NULL AS VARCHAR) environment,CAST(NULL AS VARCHAR) release,CAST(NULL AS VARCHAR) trace_id,
			CAST(NULL AS VARCHAR) issue_id WHERE false ORDER BY ` + emptyOrder,
		ScanArguments: scanArguments, ReduceArguments: reduceArguments,
	}, nil
}

func encodeQueryArguments(values []any) ([]engine.QueryArgument, error) {
	result := make([]engine.QueryArgument, len(values))
	for index, value := range values {
		switch typed := value.(type) {
		case nil:
			result[index] = engine.QueryArgument{Type: "null"}
		case string:
			result[index] = engine.QueryArgument{Type: "string", Value: typed}
		case model.Kind:
			result[index] = engine.QueryArgument{Type: "string", Value: string(typed)}
		case int:
			result[index] = engine.QueryArgument{Type: "int64", Value: strconv.Itoa(typed)}
		case int64:
			result[index] = engine.QueryArgument{Type: "int64", Value: strconv.FormatInt(typed, 10)}
		case float64:
			result[index] = engine.QueryArgument{Type: "double", Value: strconv.FormatFloat(typed, 'g', -1, 64)}
		case bool:
			result[index] = engine.QueryArgument{Type: "bool", Value: strconv.FormatBool(typed)}
		default:
			return nil, errors.New("unsupported bound query argument")
		}
	}
	return result, nil
}
