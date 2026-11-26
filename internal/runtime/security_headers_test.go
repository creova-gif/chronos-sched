package runtime

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type flusherRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flusherRecorder) Flush() {
	f.flushed = true
	f.ResponseRecorder.Flush()
}

func TestSecurityHeadersMiddleware(t *testing.T) {
	t.Run("baseline headers on /v1/runs without CSP", func(t *testing.T) {
		handler := SecurityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("X-Content-Type-Options = %q, want %q", got, "nosniff")
		}
		if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
			t.Fatalf("X-Frame-Options = %q, want %q", got, "DENY")
		}
		if got := rec.Header().Get("Referrer-Policy"); got != "no-referrer" {
			t.Fatalf("Referrer-Policy = %q, want %q", got, "no-referrer")
		}
		if got := rec.Header().Get("Content-Security-Policy"); got != "" {
			t.Fatalf("Content-Security-Policy = %q, want empty on /v1/runs", got)
		}
	})

	t.Run("CSP header on /ui and /ui/", func(t *testing.T) {
		wantCSP := "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:"
		handler := SecurityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		for _, path := range []string{"/ui", "/ui/"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Fatalf("%s: X-Content-Type-Options = %q, want %q", path, got, "nosniff")
			}
			if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
				t.Fatalf("%s: X-Frame-Options = %q, want %q", path, got, "DENY")
			}
			if got := rec.Header().Get("Referrer-Policy"); got != "no-referrer" {
				t.Fatalf("%s: Referrer-Policy = %q, want %q", path, got, "no-referrer")
			}
			if got := rec.Header().Get("Content-Security-Policy"); got != wantCSP {
				t.Fatalf("%s: Content-Security-Policy = %q, want %q", path, got, wantCSP)
			}
		}
	})

	t.Run("preserves http.Flusher for SSE", func(t *testing.T) {
		var sawFlusher bool
		handler := SecurityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fl, ok := w.(http.Flusher)
			sawFlusher = ok
			if ok {
				fl.Flush()
			}
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/v1/runs/123/events", nil)
		rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
		handler.ServeHTTP(rec, req)

		if !sawFlusher {
			t.Fatal("expected http.ResponseWriter passed to next handler to implement http.Flusher")
		}
		if !rec.flushed {
			t.Fatal("expected Flush() to be called on underlying ResponseWriter")
		}
	})
}
