package app

import (
	"errors"
	"net/http"
	"os"
	"path"
	"strings"
)

// Web assets are public; all data APIs keep their own authorization. Only
// declared SPA routes fall back to index, never an unknown API or asset path.
func newWebHandler(directory string) (http.Handler, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	index, err := root.Stat("index.html")
	_ = root.Close()
	if err != nil || !index.Mode().IsRegular() {
		return nil, errors.New("web build index is missing")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name != "" && path.Clean(name) != name {
			http.NotFound(w, r)
			return
		}
		asset := strings.HasPrefix(name, "assets/")
		if !asset {
			section := strings.Split(name, "/")[0]
			switch section {
			case "", "login", "setup", "logs", "explore", "issues", "projects", "alerts", "system":
				name = "index.html"
			default:
				http.NotFound(w, r)
				return
			}
		}
		root, err := os.OpenRoot(directory)
		if err != nil {
			http.Error(w, "web assets unavailable", http.StatusServiceUnavailable)
			return
		}
		defer root.Close()
		file, err := root.Open(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Cache-Control", "no-store")
		if asset {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		http.ServeContent(w, r, path.Base(name), info.ModTime(), file)
	}), nil
}
