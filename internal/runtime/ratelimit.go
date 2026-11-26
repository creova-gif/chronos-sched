package runtime

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// RateLimitConfig configures per-tenant token-bucket HTTP rate limiting.
type RateLimitConfig struct {
	RequestsPerSecond float64
	Burst             int
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

type rateLimiter struct {
	mu      sync.Mutex
	rps     float64
	burst   int
	buckets map[string]*tokenBucket
}

func newRateLimiter(rps float64, burst int) *rateLimiter {
	return &rateLimiter{
		rps:     rps,
		burst:   burst,
		buckets: make(map[string]*tokenBucket),
	}
}

func (l *rateLimiter) allow(tenant string, now time.Time) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[tenant]
	if !ok {
		b = &tokenBucket{
			tokens: float64(l.burst),
			last:   now,
		}
		l.buckets[tenant] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * l.rps
			if maxTokens := float64(l.burst); b.tokens > maxTokens {
				b.tokens = maxTokens
			}
			b.last = now
		}
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true, int(b.tokens)
	}
	return false, 0
}

func isRateLimitExempt(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	switch r.URL.Path {
	case "/healthz", "/readyz", "/metrics", "/ui", "/ui/":
		return true
	default:
		return false
	}
}

// RateLimitMiddleware enforces per-tenant token-bucket rate limiting on HTTP
// requests. When cfg.RequestsPerSecond <= 0, it returns next unchanged.
// GET requests to /healthz, /readyz, /metrics, /ui, and /ui/ are exempt.
func RateLimitMiddleware(cfg RateLimitConfig, next http.Handler) http.Handler {
	if cfg.RequestsPerSecond <= 0 {
		return next
	}

	burst := cfg.Burst
	if burst <= 0 {
		burst = max(1, int(cfg.RequestsPerSecond))
	}

	limiter := newRateLimiter(cfg.RequestsPerSecond, burst)
	limitHeader := strconv.Itoa(burst)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isRateLimitExempt(r) {
			next.ServeHTTP(w, r)
			return
		}

		tenant := TenantFromContext(r.Context())
		allowed, remaining := limiter.allow(tenant, time.Now())

		w.Header().Set("X-RateLimit-Limit", limitHeader)
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))

		if !allowed {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, ErrorResponse{Error: "rate limit exceeded"})
			return
		}

		next.ServeHTTP(w, r)
	})
}
