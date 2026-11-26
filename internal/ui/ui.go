// Package ui embeds and serves the ChronosSched Flight-Recorder Web Console.
package ui

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var indexHTML []byte

// Handler returns an http.Handler that serves the embedded Flight-Recorder
// Web Console on /, /ui, and /ui/ with no-store caching headers.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		switch r.URL.Path {
		case "/", "/ui", "/ui/":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				_, _ = w.Write(indexHTML)
			}
		default:
			http.NotFound(w, r)
		}
	})
}
