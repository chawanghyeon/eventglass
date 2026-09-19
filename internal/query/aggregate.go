package query

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

type GroupDimension struct {
	Op        string
	Name      string
	Namespace string
	Path      string
	Type      ScalarType
}

type NumericField struct {
	Op        string
	Name      string
	Namespace string
	Path      string
	Type      ScalarType
}

type AggregateMetric struct {
	Name  string
	Op    string
	Field *NumericField
}

type AggregateHistogram struct {
	IntervalUS   int64
	EmptyBuckets bool
}

type AggregateOperationSpec struct {
	Plan      CompiledPlan
	GroupBy   []GroupDimension
	Metrics   []AggregateMetric
	Histogram *AggregateHistogram
}

var metricNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

func BuildAggregateOperation(spec AggregateOperationSpec) (engine.QueryOperation, error) {
	if spec.Plan.Where.Text == "" || len(spec.GroupBy) > 2 || len(spec.Metrics) < 1 || len(spec.Metrics) > 8 {
		return engine.QueryOperation{}, errors.New("invalid aggregate operation")
	}
	groupScan, groupReduce, groupEmpty, groupArgs, err := compileAggregateGroups(spec)
	if err != nil {
		return engine.QueryOperation{}, err
	}
	metricScan, metricReduce, metricEmpty, metricArgs, err := compileAggregateMetrics(spec.Metrics)
	if err != nil {
		return engine.QueryOperation{}, err
	}
	scanSelect := append(append([]string{}, groupScan...), metricScan...)
	reduceSelect := append(append([]string{}, groupReduce...), metricReduce...)
	emptySelect := append(append([]string{}, groupEmpty...), metricEmpty...)
	if len(scanSelect) == 0 {
		return engine.QueryOperation{}, errors.New("aggregate has no output state")
	}
	scan := `SELECT ` + strings.Join(scanSelect, ",") + ` FROM input_rows r WHERE ` + spec.Plan.Where.Text
	if len(groupScan) > 0 {
		scan += ` GROUP BY ALL`
	}
	fillEmptyBuckets := spec.Histogram != nil && spec.Histogram.EmptyBuckets && len(spec.GroupBy) == 0
	if fillEmptyBuckets {
		bucket := histogramBucketSQL(spec.Plan, spec.Histogram.IntervalUS)
		scan += ` UNION ALL SELECT ` + strings.Join(emptySelect, ",") + aggregateEmptySource(spec) +
			` WHERE NOT EXISTS (SELECT 1 FROM input_rows r WHERE ` + spec.Plan.Where.Text + ` AND ` + bucket + `=bucket_start_us)`
	}
	reduce := `SELECT ` + strings.Join(reduceSelect, ",") + ` FROM input_rows r`
	if len(groupReduce) > 0 {
		reduce += ` GROUP BY ALL`
	}
	empty := `SELECT ` + strings.Join(emptySelect, ",")
	empty += aggregateEmptySource(spec)
	arguments := append(groupArgs, metricArgs...)
	arguments = append(arguments, spec.Plan.Where.Args...)
	if fillEmptyBuckets {
		arguments = append(arguments, spec.Plan.Where.Args...)
	}
	scanArguments, err := encodeQueryArguments(arguments)
	if err != nil {
		return engine.QueryOperation{}, err
	}
	return engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "aggregate", MaxRows: MaxAggregateGroups,
		ScanSQL: scan, ReduceSQL: reduce, EmptySQL: empty, ScanArguments: scanArguments,
	}, nil
}

func compileAggregateGroups(spec AggregateOperationSpec) (scan, reduce, empty []string, args []any, err error) {
	if spec.Histogram != nil {
		if _, bucketErr := HistogramBucketStarts(spec.Plan.Plan.Dataset.StartUS, spec.Plan.Plan.Dataset.EndUS, spec.Histogram.IntervalUS); bucketErr != nil {
			return nil, nil, nil, nil, bucketErr
		}
		interval := spec.Histogram.IntervalUS
		bucket := histogramBucketSQL(spec.Plan, interval)
		scan = append(scan, bucket+` AS bucket_start_us`)
		reduce = append(reduce, `r.bucket_start_us`)
		empty = append(empty, `CAST(bucket_start_us AS BIGINT) AS bucket_start_us`)
	}
	for index, dimension := range spec.GroupBy {
		columns, columnArgs, compileErr := compileGroupDimension(dimension, index)
		if compileErr != nil {
			return nil, nil, nil, nil, compileErr
		}
		scan = append(scan, columns...)
		args = append(args, columnArgs...)
		prefix := fmt.Sprintf("g%d_", index)
		reduce = append(reduce,
			"r."+prefix+"type", "r."+prefix+"string", "r."+prefix+"integer", "r."+prefix+"double", "r."+prefix+"boolean")
		empty = append(empty,
			`CAST(NULL AS VARCHAR) AS `+prefix+`type`, `CAST(NULL AS VARCHAR) AS `+prefix+`string`,
			`CAST(NULL AS DECIMAL(38,0)) AS `+prefix+`integer`, `CAST(NULL AS DOUBLE) AS `+prefix+`double`,
			`CAST(NULL AS BOOLEAN) AS `+prefix+`boolean`)
	}
	return scan, reduce, empty, args, nil
}

func histogramBucketSQL(plan CompiledPlan, interval int64) string {
	timeColumn := "r.event_time_us"
	if plan.Plan.Dataset.TimeBasis == model.QueryTimeReceived {
		timeColumn = "r.received_time_us"
	}
	return fmt.Sprintf(`CASE WHEN %[1]s>=0 THEN %[1]s-(%[1]s%%%[2]d) ELSE %[1]s-(((%[1]s%%%[2]d)+%[2]d)%%%[2]d) END`, timeColumn, interval)
}

func compileGroupDimension(dimension GroupDimension, index int) ([]string, []any, error) {
	prefix := fmt.Sprintf("g%d_", index)
	missing := `'missing'`
	var typeExpr, stringExpr, integerExpr, doubleExpr, booleanExpr string
	var args []any
	switch dimension.Op {
	case "field":
		column, ok := fixedColumns[dimension.Name]
		typeName, typeOK := fieldTypes[dimension.Name]
		if !ok || !typeOK {
			return nil, nil, errors.New("unsupported aggregate group field")
		}
		value := "r." + column
		typeExpr = fmt.Sprintf(`CASE WHEN %[1]s IS NULL THEN %s ELSE '%s' END`, value, missing, typeName)
		stringExpr, integerExpr, doubleExpr, booleanExpr = typedGroupValues(value, typeName)
	case "attr":
		if dimension.Type != StringType && dimension.Type != IntegerType && dimension.Type != DoubleType && dimension.Type != BooleanType {
			return nil, nil, errors.New("typed aggregate group must be scalar")
		}
		column := map[ScalarType]string{StringType: "string_value", IntegerType: "integer_value", DoubleType: "double_value", BooleanType: "boolean_value"}[dimension.Type]
		value := `(SELECT a.` + column + ` FROM UNNEST(r.attrs) AS u(a) WHERE a.namespace=? AND a.path=? AND a.value_type=?)`
		args = append(args, dimension.Namespace, dimension.Path, string(dimension.Type))
		typeExpr = fmt.Sprintf(`CASE WHEN %s IS NULL THEN %s ELSE '%s' END`, value, missing, dimension.Type)
		// The value expression appears in its typed column; double grouping
		// repeats it once more to normalize IEEE negative zero.
		stringExpr, integerExpr, doubleExpr, booleanExpr = typedGroupValues(value, dimension.Type)
		args = append(args, dimension.Namespace, dimension.Path, string(dimension.Type))
		if dimension.Type == DoubleType {
			args = append(args, dimension.Namespace, dimension.Path, string(dimension.Type))
		}
	case "group_attr":
		base := ` FROM UNNEST(r.attrs) AS u(a) WHERE a.namespace=? AND a.path=? LIMIT 1)`
		typeExpr = `(SELECT a.value_type` + base
		stringExpr = `(SELECT a.string_value` + base
		integerExpr = `(SELECT a.integer_value` + base
		doubleValue := `(SELECT a.double_value` + base
		doubleExpr = `CASE WHEN ` + doubleValue + `=0 THEN 0.0 ELSE ` + doubleValue + ` END`
		booleanExpr = `(SELECT a.boolean_value` + base
		for range 6 {
			args = append(args, dimension.Namespace, dimension.Path)
		}
		typeExpr = `COALESCE(` + typeExpr + `,'missing')`
	default:
		return nil, nil, errors.New("unsupported aggregate group operation")
	}
	return []string{
		typeExpr + ` AS ` + prefix + `type`, stringExpr + ` AS ` + prefix + `string`, integerExpr + ` AS ` + prefix + `integer`,
		doubleExpr + ` AS ` + prefix + `double`, booleanExpr + ` AS ` + prefix + `boolean`,
	}, args, nil
}

func typedGroupValues(value string, scalarType ScalarType) (string, string, string, string) {
	null := `CAST(NULL AS `
	stringExpr, integerExpr := null+`VARCHAR)`, null+`DECIMAL(38,0))`
	doubleExpr, booleanExpr := null+`DOUBLE)`, null+`BOOLEAN)`
	switch scalarType {
	case StringType:
		stringExpr = value
	case IntegerType:
		integerExpr = value
	case DoubleType:
		doubleExpr = `CASE WHEN ` + value + `=0 THEN 0.0 ELSE ` + value + ` END`
	case BooleanType:
		booleanExpr = value
	}
	return stringExpr, integerExpr, doubleExpr, booleanExpr
}

func compileAggregateMetrics(metrics []AggregateMetric) (scan, reduce, empty []string, args []any, err error) {
	seen := map[string]bool{}
	for index, metric := range metrics {
		if !metricNamePattern.MatchString(metric.Name) || seen[metric.Name] {
			return nil, nil, nil, nil, errors.New("invalid or duplicate aggregate metric name")
		}
		seen[metric.Name] = true
		prefix := fmt.Sprintf("m%d_", index)
		if metric.Op == "count" {
			if metric.Field != nil {
				return nil, nil, nil, nil, errors.New("count metric cannot name a field")
			}
			scan = append(scan, `count(*)::BIGINT AS `+prefix+`valid`, `0::BIGINT AS `+prefix+`excluded`)
			reduce = append(reduce, `sum(r.`+prefix+`valid)::BIGINT AS `+prefix+`valid`, `sum(r.`+prefix+`excluded)::BIGINT AS `+prefix+`excluded`)
			empty = append(empty, `0::BIGINT AS `+prefix+`valid`, `0::BIGINT AS `+prefix+`excluded`)
			continue
		}
		if metric.Op != "sum" && metric.Op != "min" && metric.Op != "max" && metric.Op != "avg" || metric.Field == nil {
			return nil, nil, nil, nil, errors.New("unsupported aggregate metric")
		}
		value, valueArgs, fieldType, fieldErr := compileNumericField(*metric.Field)
		if fieldErr != nil {
			return nil, nil, nil, nil, fieldErr
		}
		args = append(args, valueArgs...)
		scan = append(scan, `count(`+value+`)::BIGINT AS `+prefix+`valid`, `(count(*)-count(`+value+`))::BIGINT AS `+prefix+`excluded`)
		args = append(args, valueArgs...)
		reduce = append(reduce, `sum(r.`+prefix+`valid)::BIGINT AS `+prefix+`valid`, `sum(r.`+prefix+`excluded)::BIGINT AS `+prefix+`excluded`)
		empty = append(empty, `0::BIGINT AS `+prefix+`valid`, `0::BIGINT AS `+prefix+`excluded`)
		if fieldType == IntegerType {
			for limb := 0; limb < IntegerLimbCount; limb++ {
				expression, limbErr := IntegerLimbSQL(value, limb)
				if limbErr != nil {
					return nil, nil, nil, nil, limbErr
				}
				scan = append(scan, fmt.Sprintf(`coalesce(sum(%s),0::DECIMAL(38,0)) AS %sl%d`, expression, prefix, limb))
				// IntegerLimbSQL repeats the value expression for null, sign,
				// and magnitude extraction. Preserve placeholder order exactly.
				for range 3 {
					args = append(args, valueArgs...)
				}
				reduce = append(reduce, fmt.Sprintf(`sum(r.%sl%d) AS %sl%d`, prefix, limb, prefix, limb))
				empty = append(empty, fmt.Sprintf(`0::DECIMAL(38,0) AS %sl%d`, prefix, limb))
			}
			scan = append(scan, `min(`+value+`) AS `+prefix+`min`, `max(`+value+`) AS `+prefix+`max`)
			args = append(args, valueArgs...)
			args = append(args, valueArgs...)
			reduce = append(reduce, `min(r.`+prefix+`min) AS `+prefix+`min`, `max(r.`+prefix+`max) AS `+prefix+`max`)
			empty = append(empty, `CAST(NULL AS DECIMAL(38,0)) AS `+prefix+`min`, `CAST(NULL AS DECIMAL(38,0)) AS `+prefix+`max`)
		} else {
			scan = append(scan, `fsum(`+value+`) AS `+prefix+`sum`, `min(`+value+`) AS `+prefix+`min`, `max(`+value+`) AS `+prefix+`max`)
			args = append(args, valueArgs...)
			args = append(args, valueArgs...)
			args = append(args, valueArgs...)
			reduce = append(reduce, `fsum(r.`+prefix+`sum) AS `+prefix+`sum`, `min(r.`+prefix+`min) AS `+prefix+`min`, `max(r.`+prefix+`max) AS `+prefix+`max`)
			empty = append(empty, `CAST(NULL AS DOUBLE) AS `+prefix+`sum`, `CAST(NULL AS DOUBLE) AS `+prefix+`min`, `CAST(NULL AS DOUBLE) AS `+prefix+`max`)
		}
	}
	return scan, reduce, empty, args, nil
}

func compileNumericField(field NumericField) (string, []any, ScalarType, error) {
	if field.Type != IntegerType && field.Type != DoubleType {
		return "", nil, "", errors.New("numeric metric field must be integer or double")
	}
	switch field.Op {
	case "field":
		column, ok := fixedColumns[field.Name]
		if !ok || fieldTypes[field.Name] != field.Type {
			return "", nil, "", errors.New("fixed metric field type mismatch")
		}
		return "r." + column, nil, field.Type, nil
	case "attr":
		column := map[ScalarType]string{IntegerType: "integer_value", DoubleType: "double_value"}[field.Type]
		return `(SELECT a.` + column + ` FROM UNNEST(r.attrs) AS u(a) WHERE a.namespace=? AND a.path=? AND a.value_type=?)`,
			[]any{field.Namespace, field.Path, string(field.Type)}, field.Type, nil
	default:
		return "", nil, "", errors.New("unsupported numeric metric field")
	}
}

func aggregateEmptySource(spec AggregateOperationSpec) string {
	if len(spec.GroupBy) != 0 || spec.Histogram == nil || !spec.Histogram.EmptyBuckets {
		if len(spec.GroupBy) == 0 && spec.Histogram == nil {
			return ""
		}
		return ` WHERE false`
	}
	starts, err := HistogramBucketStarts(spec.Plan.Plan.Dataset.StartUS, spec.Plan.Plan.Dataset.EndUS, spec.Histogram.IntervalUS)
	if err != nil || len(starts) == 0 {
		return ` WHERE false`
	}
	values := make([]string, len(starts))
	for index, value := range starts {
		values[index] = fmt.Sprintf("%d", value)
	}
	return ` FROM unnest([` + strings.Join(values, ",") + `]) AS buckets(bucket_start_us)`
}
