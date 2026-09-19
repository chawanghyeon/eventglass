package architecture

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestImplementedManagementRoutesMatchInventory(t *testing.T) {
	root := filepath.Join("..", "..")
	var declared, actual []string
	data, err := os.ReadFile(filepath.Join(root, "api", "implemented-routes.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &declared); err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(filepath.Join(root, "internal", "api"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "HandleFunc" {
				return true
			}
			literal, ok := call.Args[0].(*ast.BasicLit)
			if !ok {
				return true
			}
			value, _ := strconv.Unquote(literal.Value)
			if strings.Contains(value, " /v1/") {
				actual = append(actual, value)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(actual)
	sort.Strings(declared)
	if !reflect.DeepEqual(actual, declared) {
		t.Fatalf("registered=%v declared=%v", actual, declared)
	}
}

func TestFrontendOwnershipBoundaries(t *testing.T) {
	root := filepath.Join("..", "..", "web", "src")
	imports := regexp.MustCompile(`(?:from\s+|import\s*)["']([^"']+)["']`)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || (!strings.HasSuffix(path, ".ts") && !strings.HasSuffix(path, ".tsx")) || strings.Contains(path, ".test.") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		owner := strings.Split(filepath.ToSlash(rel), "/")[0]
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range imports.FindAllSubmatch(data, -1) {
			name := string(match[1])
			if !strings.HasPrefix(name, ".") {
				continue
			}
			target, _ := filepath.Rel(root, filepath.Clean(filepath.Join(filepath.Dir(path), name)))
			dependency := strings.Split(filepath.ToSlash(target), "/")[0]
			if owner == "generated" || (owner == "api" || owner == "shared") && (dependency == "features" || dependency == "app") {
				t.Errorf("%s: forbidden %s -> %s import", rel, owner, dependency)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
