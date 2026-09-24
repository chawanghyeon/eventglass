//go:build duckdb_use_static_lib

package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

func universalSQLLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func universalJSONPath(path, suffix string) string {
	return universalSQLLiteral("$." + strconv.Quote(path) + suffix)
}

func universalSQLPredicate(p universalPredicate) string {
	path := universalJSONPath(p.path, "")
	kind := universalJSONPath(p.path, ".Kind")
	value := universalJSONPath(p.path, ".S")
	integer := universalJSONPath(p.path, ".I")
	array := universalJSONPath(p.path, ".A")
	var sql string
	switch p.op {
	case "term":
		word := universalSQLLiteral(p.text)
		sql = "list_contains(string_split(search0,' ')," + word + ") OR list_contains(string_split(search1,' ')," + word + ")"
	case "eq":
		sql = "json_extract_string(fields_json," + kind + ")='string' AND json_extract_string(fields_json," + value + ")=" + universalSQLLiteral(p.text)
	case "neq":
		sql = "json_extract_string(fields_json," + kind + ")='string' AND json_extract_string(fields_json," + value + ")<>" + universalSQLLiteral(p.text)
	case "gte":
		sql = "json_extract_string(fields_json," + kind + ")='int' AND try_cast(json_extract_string(fields_json," + integer + ") AS BIGINT)>=" + strconv.FormatInt(p.n, 10)
	case "exists":
		sql = "json_exists(fields_json," + path + ")"
	case "null":
		sql = "json_extract_string(fields_json," + kind + ")='null'"
	case "array":
		needle, _ := json.Marshal(p.text)
		sql = "json_extract_string(fields_json," + kind + ")='array' AND json_contains(json_extract(fields_json," + array + ")," + universalSQLLiteral(string(needle)) + ")"
	case "contains":
		needle := universalSQLLiteral(p.text)
		sql = "contains(search0," + needle + ") OR contains(search1," + needle + ")"
	case "regex":
		pattern := universalSQLLiteral(p.text)
		sql = "regexp_matches(search0," + pattern + ") OR regexp_matches(search1," + pattern + ")"
	case "and", "or":
		if len(p.children) == 0 {
			if p.op == "and" {
				return "TRUE"
			}
			return "FALSE"
		}
		operator := " AND "
		if p.op == "or" {
			operator = " OR "
		}
		children := make([]string, 0, len(p.children))
		for _, child := range p.children {
			children = append(children, universalSQLPredicate(child))
		}
		return "(" + strings.Join(children, operator) + ")"
	case "not":
		return "NOT (" + universalSQLPredicate(p.children[0]) + ")"
	default:
		panic("unsupported predicate in pinned DuckDB comparison")
	}
	// The product treats a missing or type-mismatched leaf as false before NOT.
	return "COALESCE((" + sql + "),FALSE)"
}

func writeUniversalCSV(path string, docs []universalDoc) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	for _, d := range docs {
		encoded, err := json.Marshal(d.Fields)
		if err != nil {
			f.Close()
			return err
		}
		source, err := json.Marshal(d)
		if err != nil {
			f.Close()
			return err
		}
		if err := w.Write([]string{strconv.Itoa(d.ID), strconv.Itoa(d.Tenant), strconv.FormatInt(d.When, 10), strconv.FormatBool(d.Live), strconv.FormatInt(d.Duration, 10), d.Search[0], d.Search[1], string(encoded), string(source)}); err != nil {
			f.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func universalDuckDBAnswer(t *testing.T, ctx context.Context, db *sql.DB, analytics, payload string, p universalPredicate, groupPath string) (universalAnswer, []universalDoc) {
	t.Helper()
	where := "tenant=1 AND when_ts>=20 AND when_ts<80 AND live AND " + universalSQLPredicate(p)
	path := universalJSONPath(groupPath, "")
	group := "CASE WHEN json_exists(fields_json," + path + ") THEN " +
		"coalesce(json_extract_string(fields_json," + universalJSONPath(groupPath, ".Kind") + "),'') || ':' || " +
		"coalesce(json_extract_string(fields_json," + universalJSONPath(groupPath, ".S") + "),'') || ':' || " +
		"coalesce(json_extract_string(fields_json," + universalJSONPath(groupPath, ".I") + "),'0') ELSE 'missing' END"
	a := universalAnswer{group: make(map[string]int64)}
	rows, err := db.QueryContext(ctx, "SELECT "+group+",count(*),sum(duration) FROM read_parquet(?) WHERE "+where+" GROUP BY 1", analytics)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var key string
		var count, sum int64
		if err := rows.Scan(&key, &count, &sum); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		a.group[key] = sum
		a.count += int(count)
		a.sum += sum
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	rows, err = db.QueryContext(ctx, "SELECT p.source_json FROM (SELECT id,duration FROM read_parquet(?) WHERE "+where+" ORDER BY duration DESC,id LIMIT 5) winners JOIN read_parquet(?) p USING(id) ORDER BY winners.duration DESC,winners.id", analytics, payload)
	if err != nil {
		t.Fatal(err)
	}
	var details []universalDoc
	for rows.Next() {
		var d universalDoc
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(encoded), &d); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		a.top = append(a.top, d.ID)
		details = append(details, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	return a, details
}

func TestUniversalPinnedDuckDBParquetParity(t *testing.T) {
	if os.Getenv("EVENTGLASS_UNIFIED_DUCKDB") != "1" {
		t.Skip("opt-in pinned DuckDB comparison")
	}
	ctx := context.Background()
	rows := 10000
	if value := os.Getenv("EVENTGLASS_UNIFIED_DUCKDB_ROWS"); value != "" {
		if value != "100000" {
			t.Fatal("EVENTGLASS_UNIFIED_DUCKDB_ROWS must be 100000 when set")
		}
		rows = 100000
	}
	docs := universalFixture(rows)
	root := t.TempDir()
	csvPath, analyticsPath, payloadPath, singlePath, segmentPath := filepath.Join(root, "docs.csv"), filepath.Join(root, "analytics.parquet"), filepath.Join(root, "payload.parquet"), filepath.Join(root, "single.parquet"), filepath.Join(root, "universal.bin")
	singleOnly := os.Getenv("EVENTGLASS_UNIFIED_SINGLE_ONLY") == "1"
	single := singleOnly || os.Getenv("EVENTGLASS_UNIFIED_SINGLE_PARQUET") == "1"
	if err := writeUniversalCSV(csvPath, docs); err != nil {
		t.Fatal(err)
	}
	t.Log("phase=csv_ready")
	docs = nil
	debug.FreeOSMemory()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil || version != "v2.0.0-dev84020" {
		t.Fatalf("unpinned DuckDB version %q: %v", version, err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE docs(id BIGINT,tenant BIGINT,when_ts BIGINT,live BOOLEAN,duration BIGINT,search0 VARCHAR,search1 VARCHAR,fields_json VARCHAR,source_json VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "COPY docs FROM "+universalSQLLiteral(csvPath)+" (HEADER FALSE)"); err != nil {
		t.Fatal(err)
	}
	t.Log("phase=duckdb_loaded")
	if !singleOnly {
		if _, err := db.ExecContext(ctx, "COPY (SELECT id,tenant,when_ts,live,duration,search0,search1,fields_json FROM docs) TO "+universalSQLLiteral(analyticsPath)+" (FORMAT PARQUET,COMPRESSION ZSTD)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "COPY (SELECT id,source_json FROM docs) TO "+universalSQLLiteral(payloadPath)+" (FORMAT PARQUET,COMPRESSION ZSTD)"); err != nil {
			t.Fatal(err)
		}
	}
	if single {
		if _, err := db.ExecContext(ctx, "COPY docs TO "+universalSQLLiteral(singlePath)+" (FORMAT PARQUET,COMPRESSION ZSTD)"); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("phase=parquet_ready")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(csvPath); err != nil {
		t.Fatal(err)
	}
	debug.FreeOSMemory()
	docs = universalFixture(rows)
	var segmentBytes int64
	if singleOnly {
		analyticsPath, payloadPath = singlePath, singlePath
	} else {
		_, segmentBytes, err = writeUniversalObject(segmentPath, docs)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("phase=segment_ready")
	}
	debug.FreeOSMemory()
	cases := append(universalCases(), universalPredicate{op: "eq", path: "attrs/order_id", text: "order1237"}, universalPredicate{op: "term", text: "trace1237"}, universalPredicate{op: "eq", path: "attrs/custom/237", text: "v1237"})
	for _, p := range cases {
		t.Logf("phase=query op=%s", p.op)
		for _, groupPath := range []string{"tags/region", "attrs/note", "attrs/order_id", "attrs/custom/237"} {
			want, wantDetails := oracleUniversalRange(docs, p, groupPath)
			queryDB, err := engine.Open(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			parquetAnswer, parquetDetails := universalDuckDBAnswer(t, ctx, queryDB, analyticsPath, payloadPath, p, groupPath)
			if err := queryDB.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(parquetAnswer, want) || !reflect.DeepEqual(parquetDetails, wantDetails) {
				t.Fatalf("op=%s group=%s parquet=%+v oracle=%+v", p.op, groupPath, parquetAnswer, want)
			}
			debug.FreeOSMemory()
			if single && !singleOnly {
				singleDB, err := engine.Open(ctx, "")
				if err != nil {
					t.Fatal(err)
				}
				singleAnswer, singleDetails := universalDuckDBAnswer(t, ctx, singleDB, singlePath, singlePath, p, groupPath)
				if err := singleDB.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(singleAnswer, want) || !reflect.DeepEqual(singleDetails, wantDetails) {
					t.Fatalf("op=%s group=%s single=%+v oracle=%+v", p.op, groupPath, singleAnswer, want)
				}
				debug.FreeOSMemory()
			}
			if !singleOnly {
				segmentAnswer, segmentDetails, err := runUniversalRange(ctx, localSource{dir: root}, segmentBytes, p, groupPath)
				if err != nil || !reflect.DeepEqual(segmentAnswer, want) || !reflect.DeepEqual(segmentDetails, wantDetails) {
					t.Fatalf("op=%s group=%s segment=%+v oracle=%+v err=%v", p.op, groupPath, segmentAnswer, want, err)
				}
			}
			debug.FreeOSMemory()
		}
	}
	if !singleOnly && rows == 10000 && os.Getenv("EVENTGLASS_UNIFIED_TIMING") == "1" {
		queryDB, err := engine.Open(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		defer queryDB.Close()
		for _, tc := range []struct {
			name string
			p    universalPredicate
		}{{"rare", universalPredicate{op: "term", text: "fatal"}}, {"broad", universalPredicate{op: "term", text: "request"}}, {"regex", universalPredicate{op: "regex", text: "timeout|한글"}}} {
			var parquetTimes, singleTimes, segmentTimes []int64
			for i := range 30 {
				runParquet := func() {
					start := time.Now()
					universalDuckDBAnswer(t, ctx, queryDB, analyticsPath, payloadPath, tc.p, "tags/region")
					parquetTimes = append(parquetTimes, time.Since(start).Microseconds())
				}
				runSegment := func() {
					start := time.Now()
					if _, _, err := runUniversalRange(ctx, localSource{dir: root}, segmentBytes, tc.p, "tags/region"); err != nil {
						t.Fatal(err)
					}
					segmentTimes = append(segmentTimes, time.Since(start).Microseconds())
				}
				runSingle := func() {
					start := time.Now()
					universalDuckDBAnswer(t, ctx, queryDB, singlePath, singlePath, tc.p, "tags/region")
					singleTimes = append(singleTimes, time.Since(start).Microseconds())
				}
				if single {
					switch i % 3 {
					case 0:
						runParquet()
						runSingle()
						runSegment()
					case 1:
						runSingle()
						runSegment()
						runParquet()
					default:
						runSegment()
						runParquet()
						runSingle()
					}
				} else if i%2 == 0 {
					runParquet()
					runSegment()
				} else {
					runSegment()
					runParquet()
				}
			}
			slices.Sort(parquetTimes)
			slices.Sort(segmentTimes)
			t.Logf("local query=%s reps=30 parquet_p50_us=%d parquet_p95_us=%d parquet_p99_us=%d segment_p50_us=%d segment_p95_us=%d segment_p99_us=%d", tc.name, parquetTimes[15], parquetTimes[28], parquetTimes[29], segmentTimes[15], segmentTimes[28], segmentTimes[29])
			if single {
				slices.Sort(singleTimes)
				t.Logf("local query=%s reps=30 single_p50_us=%d single_p95_us=%d single_p99_us=%d", tc.name, singleTimes[15], singleTimes[28], singleTimes[29])
			}
		}
	}
	if !singleOnly {
		analyticsInfo, err := os.Stat(analyticsPath)
		if err != nil {
			t.Fatal(err)
		}
		payloadInfo, err := os.Stat(payloadPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("pinned_duckdb=%s docs=%d cases=%d groups=4 analytics_bytes=%d payload_bytes=%d parquet_total_bytes=%d segment_bytes=%d", version, len(docs), len(cases), analyticsInfo.Size(), payloadInfo.Size(), analyticsInfo.Size()+payloadInfo.Size(), segmentBytes)
	}
	if single {
		info, err := os.Stat(singlePath)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("pinned_duckdb=%s docs=%d cases=%d groups=4 single_parquet_bytes=%d", version, len(docs), len(cases), info.Size())
	}
}
