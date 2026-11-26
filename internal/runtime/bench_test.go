package runtime

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func BenchmarkRateLimitMiddlewareAllow(b *testing.B) {
	handler := RateLimitMiddleware(
		RateLimitConfig{
			RequestsPerSecond: 1e9,
			Burst:             1e9,
		},
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	req = req.WithContext(WithTenant(req.Context(), "bench-tenant"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("expected status 200, got %d", rec.Code)
		}
	}
}

func BenchmarkCoordinatorReadyzAndSecurityHeaders(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		b.Fatalf("sqlite.Open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })

	coord := NewCoordinator(st, CoordinatorConfig{})
	handler := coord.Handler()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("expected status 200, got %d", rec.Code)
		}
	}
}
