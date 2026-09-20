package app

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWebAssetsDoNotHideMissingAPIsOrEscapeRoot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html>app</html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets", "app-hash.js"), []byte("export {}"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "assets", "escape")); err != nil {
		t.Fatal(err)
	}
	handler, err := newWebHandler(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path      string
		status    int
		immutable bool
	}{
		{"/logs/abc", 200, false}, {"/assets/app-hash.js", 200, true}, {"/v1/missing", 404, false}, {"/api/missing", 404, false}, {"/assets/missing.js", 404, false}, {"/assets/escape", 404, false}, {"/assets/../index.html", 404, false},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", test.path, nil))
		if response.Code != test.status || strings.Contains(response.Header().Get("Cache-Control"), "immutable") != test.immutable {
			t.Fatalf("%s: status=%d headers=%v", test.path, response.Code, response.Header())
		}
	}
}
