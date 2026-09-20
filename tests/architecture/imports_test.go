package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestProductionDependencyBoundaries(t *testing.T) {
	allowed := map[string]string{
		"app":         "*",
		"api":         "ingest sdk query alerts control model resource engine",
		"ingest":      "sdk model resource control storage engine issues",
		"sdk":         "",
		"model":       "",
		"control":     "model issues",
		"issues":      "model",
		"query":       "model control engine storage resource",
		"engine":      "model storage",
		"storage":     "model resource",
		"maintenance": "control storage engine model resource",
		"alerts":      "query control model resource",
		"resource":    "",
		"testkit":     "*",
	}
	const prefix = "github.com/chawanghyeon/eventglass/internal/"
	root := filepath.Join("..", "..", "internal")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		owner := strings.Split(filepath.ToSlash(relative), "/")[0]
		dependencies, exists := allowed[owner]
		if !exists {
			t.Errorf("%s: undeclared architecture owner %s", path, owner)
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if strings.HasPrefix(imported, prefix) {
				target := strings.Split(strings.TrimPrefix(imported, prefix), "/")[0]
				if target == "testkit" && owner != "testkit" {
					t.Errorf("%s: production depends on testkit", path)
				} else if dependencies != "*" && !strings.Contains(" "+dependencies+" ", " "+target+" ") {
					t.Errorf("%s: forbidden %s -> %s dependency", path, owner, target)
				}
			}
			if imported == "github.com/chawanghyeon/eventglass/api/generated" && owner != "api" {
				t.Errorf("%s: generated HTTP DTOs outside api", path)
			}
			if (strings.Contains(imported, "duckdb") && owner != "engine") ||
				(strings.Contains(imported, "jackc/pgx") && owner != "control") ||
				(strings.Contains(imported, "aws-sdk") && owner != "storage") {
				t.Errorf("%s: driver outside its owner: %s", path, imported)
			}
			if owner == "model" || owner == "issues" || owner == "sdk" {
				if strings.HasPrefix(imported, "net/") || imported == "database/sql" || imported == "os" || imported == "os/exec" {
					t.Errorf("%s: side-effect dependency in pure package: %s", path, imported)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
