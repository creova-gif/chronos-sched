package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestTenantMiddlewareAndContext(t *testing.T) {
	t.Run("context fallback when unset", func(t *testing.T) {
		if got := TenantFromContext(context.Background()); got != DefaultTenant {
			t.Fatalf("TenantFromContext(Background) = %q, want %q", got, DefaultTenant)
		}
		if got := TenantFromContext(context.TODO()); got != DefaultTenant {
			t.Fatalf("TenantFromContext(TODO) = %q, want %q", got, DefaultTenant)
		}
	})

	t.Run("default fallback when header absent or whitespace", func(t *testing.T) {
		var gotTenant string
		h := TenantMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTenant = TenantFromContext(r.Context())
			writeJSON(w, http.StatusOK, map[string]string{"tenant": gotTenant})
		}))

		for _, hdr := range []string{"", "   \t "} {
			gotTenant = ""
			req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
			if hdr != "" {
				req.Header.Set(HeaderTenant, hdr)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			if gotTenant != DefaultTenant {
				t.Fatalf("context tenant = %q, want %q", gotTenant, DefaultTenant)
			}
			if echo := rec.Header().Get(HeaderTenant); echo != DefaultTenant {
				t.Fatalf("response header %s = %q, want %q", HeaderTenant, echo, DefaultTenant)
			}
		}
	})

	t.Run("valid custom tenant", func(t *testing.T) {
		var gotTenant string
		h := TenantMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTenant = TenantFromContext(r.Context())
			writeJSON(w, http.StatusOK, map[string]string{"tenant": gotTenant})
		}))

		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		req.Header.Set(HeaderTenant, "  acme-prod.1  ")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if gotTenant != "acme-prod.1" {
			t.Fatalf("context tenant = %q, want %q", gotTenant, "acme-prod.1")
		}
		if echo := rec.Header().Get(HeaderTenant); echo != "acme-prod.1" {
			t.Fatalf("response header %s = %q, want %q", HeaderTenant, echo, "acme-prod.1")
		}
	})

	t.Run("invalid tenant returns 400 JSON", func(t *testing.T) {
		called := false
		h := TenantMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))

		for _, bad := range []string{"bad/tenant!", strings.Repeat("a", 65)} {
			called = false
			req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
			req.Header.Set(HeaderTenant, bad)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if called {
				t.Fatalf("next handler should not be called for invalid tenant %q", bad)
			}
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 for %q", rec.Code, bad)
			}
			if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
				t.Fatalf("Content-Type = %q, want application/json", ct)
			}
			var errResp ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("decode ErrorResponse: %v", err)
			}
			if errResp.Error == "" {
				t.Fatalf("expected non-empty ErrorResponse.Error for %q", bad)
			}
		}
	})

	t.Run("composition with ObservabilityMiddleware under race", func(t *testing.T) {
		var logBuf bytes.Buffer
		var logMu sync.Mutex
		logger := slog.New(slog.NewJSONHandler(&syncWriter{mu: &logMu, buf: &logBuf}, nil))

		h := ObservabilityMiddleware(logger, TenantMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]string{"tenant": TenantFromContext(r.Context())})
		})))

		tenants := []string{"", "acme-prod.1", "tenant_2", "bad/tenant!"}
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ten := tenants[idx%len(tenants)]
				req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
				if ten != "" {
					req.Header.Set(HeaderTenant, ten)
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)

				if ten == "bad/tenant!" {
					if rec.Code != http.StatusBadRequest {
						t.Errorf("status = %d, want 400", rec.Code)
					}
					return
				}
				if rec.Code != http.StatusOK {
					t.Errorf("status = %d, want 200", rec.Code)
				}
				want := ten
				if want == "" {
					want = DefaultTenant
				}
				if got := rec.Header().Get(HeaderTenant); got != want {
					t.Errorf("header %s = %q, want %q", HeaderTenant, got, want)
				}
				if got := rec.Header().Get(headerRequestID); got == "" {
					t.Errorf("expected non-empty %s header", headerRequestID)
				}
			}(i)
		}
		wg.Wait()
	})
}

type syncWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
