package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestCompactionAnalyticsSizeUsesAllTypedVariableColumns(t *testing.T) {
	request := compactionInspectionFixture(t)
	db, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`DESCRIBE SELECT * FROM read_parquet(?)`, request.Inputs[0].AnalyticsPath)
	if err != nil {
		t.Fatal(err)
	}
	var populated, empty []string
	expression := compactionAnalyticsRowBytesSQL()
	if strings.Contains(expression, "to_json") || strings.Contains(expression, "encode(") {
		t.Fatal("sizing must not serialize or copy the complete native row")
	}
	for rows.Next() {
		var name, kind string
		var rest [4]sql.NullString
		if err := rows.Scan(&name, &kind, &rest[0], &rest[1], &rest[2], &rest[3]); err != nil {
			t.Fatal(err)
		}
		value := "NULL"
		switch {
		case kind == "VARCHAR":
			if !strings.Contains(expression, "bit_length("+name+")") {
				t.Fatalf("new variable column is not sized: %s", name)
			}
			value = "''"
			if name == "message" {
				value = "'한글'"
			}
		case name == "attrs":
			for _, field := range regexp.MustCompile(`\b([a-z_]+) VARCHAR\b`).FindAllStringSubmatch(kind, -1) {
				if !strings.Contains(expression, "bit_length(attribute."+field[1]+")") {
					t.Fatalf("new variable attribute field is not sized: %s", field[1])
				}
			}
			value = `[{namespace:'ns',path:'p',value_type:'string',string_value:'한글',integer_value:NULL,double_value:NULL,boolean_value:NULL,json_value:NULL,unit:''},NULL]`
		case name == "search_values":
			value = `['한',NULL,'']`
		case name == "warnings":
			value = `[chr(10)]`
		case kind == "BIGINT" || kind == "INTEGER" || kind == "SMALLINT" || kind == "USMALLINT" || kind == "BOOLEAN":
		default:
			t.Fatalf("review sizing for new native type: %s %s", name, kind)
		}
		populated = append(populated, "("+value+")::"+kind+" AS "+name)
		empty = append(empty, "NULL::"+kind+" AS "+name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, test := range []struct {
		name    string
		columns []string
		want    int64
	}{{"unicode_and_null_elements", populated, 4096 + 6 + 2*256 + 15 + 3*32 + 3 + 32 + 1}, {"all_null", empty, 4096}} {
		t.Run(test.name, func(t *testing.T) {
			var got int64
			if err := db.QueryRow("SELECT " + expression + " FROM (SELECT " + strings.Join(test.columns, ",") + ")").Scan(&got); err != nil || got != test.want {
				t.Fatalf("typed byte charge=%d want=%d err=%v", got, test.want, err)
			}
		})
	}
}

func TestCompactionPartManifestBoundsAndNumericOrder(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"chunk=10", "chunk=2", "chunk=0"} {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "data_0.parquet"), []byte("test"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{filepath.Join(root, "chunk=0/data_0.parquet"), filepath.Join(root, "chunk=2/data_0.parquet"), filepath.Join(root, "chunk=10/data_0.parquet")}
	got, err := compactionPartPaths(root, 3, 12)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("parts=%v err=%v", got, err)
	}
	for _, limit := range []struct{ count, bytes int64 }{{2, 12}, {4, 12}, {3, 11}, {-1, 12}, {1025, 12}, {3, -1}} {
		if _, err := compactionPartPaths(root, limit.count, limit.bytes); err == nil {
			t.Fatalf("accepted bound %+v", limit)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "chunk=0/unexpected"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := compactionPartPaths(root, 3, 100); err == nil {
		t.Fatal("accepted uncounted file")
	}
}

func TestCompactionPartManifestRejectsSymlinksAndInvalidNames(t *testing.T) {
	for _, name := range []string{"chunk=-1", "chunk=bad", "other=0", "chunk=0"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(dir, "data_0.parquet")); err != nil {
				t.Fatal(err)
			}
			if _, err := compactionPartPaths(root, 1, 100); err == nil {
				t.Fatal("invalid partition admitted")
			}
		})
	}
	if paths, err := compactionPartPaths(t.TempDir(), 0, 0); err != nil || len(paths) != 0 {
		t.Fatalf("empty=%v %v", paths, err)
	}
}
