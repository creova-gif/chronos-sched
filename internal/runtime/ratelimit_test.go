package runtime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestRateLimitMiddleware_BurstIsolationAndExemptions(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	handler := TenantMiddleware(RateLimitMiddleware(RateLimitConfig{
		RequestsPerSecond: 0.001, // negligible refill during test
		Burst:             2,
	}, okHandler))

	doReq := func(method, path, tenant string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if tenant != "" {
			req.Header.Set(HeaderTenant, tenant)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Tenant alpha: 2 requests allowed within burst, 3rd rejected with 429.
	rec1 := doReq(http.MethodPost, "/v1/runs", "alpha")
	if rec1.Code != http.StatusOK {
		t.Fatalf("alpha req 1: expected 200, got %d", rec1.Code)
	}
	if got := rec1.Header().Get("X-RateLimit-Limit"); got != "2" {
		t.Fatalf("alpha req 1: expected X-RateLimit-Limit=2, got %q", got)
	}
	if got := rec1.Header().Get("X-RateLimit-Remaining"); got != "1" {
		t.Fatalf("alpha req 1: expected X-RateLimit-Remaining=1, got %q", got)
	}

	rec2 := doReq(http.MethodPost, "/v1/runs", "alpha")
	if rec2.Code != http.StatusOK {
		t.Fatalf("alpha req 2: expected 200, got %d", rec2.Code)
	}
	if got := rec2.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("alpha req 2: expected X-RateLimit-Remaining=0, got %q", got)
	}

	rec3 := doReq(http.MethodPost, "/v1/runs", "alpha")
	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("alpha req 3: expected 429, got %d", rec3.Code)
	}
	if got := rec3.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("alpha req 3: expected Retry-After=1, got %q", got)
	}
	if got := rec3.Header().Get("X-RateLimit-Limit"); got != "2" {
		t.Fatalf("alpha req 3: expected X-RateLimit-Limit=2, got %q", got)
	}
	if got := rec3.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("alpha req 3: expected X-RateLimit-Remaining=0, got %q", got)
	}
	var errResp ErrorResponse
	if err := json.NewDecoder(rec3.Body).Decode(&errResp); err != nil {
		t.Fatalf("alpha req 3: decode ErrorResponse: %v", err)
	}
	if errResp.Error != "rate limit exceeded" {
		t.Fatalf("alpha req 3: expected error %q, got %q", "rate limit exceeded", errResp.Error)
	}

	// Tenant beta has an independent burst.
	beta1 := doReq(http.MethodPost, "/v1/runs", "beta")
	if beta1.Code != http.StatusOK {
		t.Fatalf("beta req 1: expected 200, got %d", beta1.Code)
	}
	if got := beta1.Header().Get("X-RateLimit-Remaining"); got != "1" {
		t.Fatalf("beta req 1: expected X-RateLimit-Remaining=1, got %q", got)
	}

	// Exempt endpoints bypass rate limiting even when tenant alpha is exhausted.
	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/ui", "/ui/"} {
		exemptRec := doReq(http.MethodGet, path, "alpha")
		if exemptRec.Code != http.StatusOK {
			t.Fatalf("GET %s for exhausted alpha: expected 200, got %d", path, exemptRec.Code)
		}
	}

	// Concurrent requests under -race.
	var wg sync.WaitGroup
	tenants := []string{"alpha", "beta", "gamma", "delta"}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tenant := tenants[idx%len(tenants)]
			rec := doReq(http.MethodGet, "/v1/runs", tenant)
			if rec.Code != http.StatusOK && rec.Code != http.StatusTooManyRequests {
				t.Errorf("concurrent req %d (%s): unexpected status %d", idx, tenant, rec.Code)
			}
			_ = doReq(http.MethodGet, "/healthz", tenant)
		}(i)
	}
	wg.Wait()
}

func TestRateLimitMiddleware_DisabledAndDefaultBurst(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	// Disabled when RequestsPerSecond <= 0: returns next unchanged.
	disabled := RateLimitMiddleware(RateLimitConfig{RequestsPerSecond: 0, Burst: 1}, okHandler)
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		disabled.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/runs", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("disabled req %d: expected 200, got %d", i, rec.Code)
		}
		if got := rec.Header().Get("X-RateLimit-Limit"); got != "" {
			t.Fatalf("disabled req %d: expected no X-RateLimit-Limit header, got %q", i, got)
		}
	}

	// Default burst when Burst <= 0: max(1, int(RequestsPerSecond)).
	defaultBurst := RateLimitMiddleware(RateLimitConfig{RequestsPerSecond: 0.5, Burst: 0}, okHandler)
	rec1 := httptest.NewRecorder()
	defaultBurst.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/v1/runs", nil))
	if rec1.Code != http.StatusOK {
		t.Fatalf("default burst req 1: expected 200, got %d", rec1.Code)
	}
	if got := rec1.Header().Get("X-RateLimit-Limit"); got != "1" {
		t.Fatalf("default burst req 1: expected X-RateLimit-Limit=1, got %q", got)
	}

	rec2 := httptest.NewRecorder()
	defaultBurst.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/v1/runs", nil))
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("default burst req 2: expected 429, got %d", rec2.Code)
	}
}

func TestRateLimitMiddleware_BoundedBuckets(t *testing.T) {
	// 1. Fresh buckets exceed maxRateLimitBuckets: oldest bucket is evicted.
	rl := newRateLimiter(10, 5)
	base := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	total := maxRateLimitBuckets + 256
	for i := 0; i < total; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		allowed, _ := rl.allow(tenant, base.Add(time.Duration(i)*time.Millisecond))
		if !allowed {
			t.Fatalf("expected first request for %s to be allowed", tenant)
		}
		if len(rl.buckets) > maxRateLimitBuckets {
			t.Fatalf("buckets exceeded cap at i=%d: got %d, want <= %d", i, len(rl.buckets), maxRateLimitBuckets)
		}
	}
	if len(rl.buckets) != maxRateLimitBuckets {
		t.Fatalf("expected %d buckets after eviction, got %d", maxRateLimitBuckets, len(rl.buckets))
	}
	if _, exists := rl.buckets["tenant-0"]; exists {
		t.Fatalf("expected oldest bucket tenant-0 to be evicted")
	}
	newest := fmt.Sprintf("tenant-%d", total-1)
	if _, exists := rl.buckets[newest]; !exists {
		t.Fatalf("expected newest bucket %s to remain", newest)
	}

	// 2. Stale buckets (> staleBucketTTL) are evicted before falling back to oldest.
	rlStale := newRateLimiter(10, 5)
	for i := 0; i < maxRateLimitBuckets; i++ {
		tenant := fmt.Sprintf("stale-%d", i)
		rlStale.allow(tenant, base)
	}
	// Refresh one bucket so only it is non-stale after staleBucketTTL passes.
	freshTime := base.Add(staleBucketTTL + time.Second)
	rlStale.allow("stale-0", base.Add(5*time.Minute))
	rlStale.allow("brand-new", freshTime)
	if len(rlStale.buckets) != 2 {
		t.Fatalf("expected 2 buckets after stale eviction (1 fresh + 1 new), got %d", len(rlStale.buckets))
	}
	if _, exists := rlStale.buckets["stale-0"]; !exists {
		t.Fatalf("expected non-stale bucket stale-0 to remain")
	}

	// 3. Concurrent distinct tenants under -race stay strictly bounded.
	rlRace := newRateLimiter(10, 5)
	var wg sync.WaitGroup
	workers := 16
	perWorker := (maxRateLimitBuckets / workers) + 64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				tenant := fmt.Sprintf("w%d-t%d", workerID, j)
				rlRace.allow(tenant, time.Now())
			}
		}(w)
	}
	wg.Wait()

	rlRace.mu.Lock()
	gotBuckets := len(rlRace.buckets)
	rlRace.mu.Unlock()
	if gotBuckets > maxRateLimitBuckets {
		t.Fatalf("concurrent buckets exceeded cap: got %d, want <= %d", gotBuckets, maxRateLimitBuckets)
	}
}
