package runtime

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
)

const bearerPrefix = "Bearer "

// AuthMiddleware enforces optional constant-time Bearer token authentication
// on Coordinator HTTP routes. When token is empty, it returns next unchanged.
// GET /healthz is always exempt for load-balancer and health probes.
func AuthMiddleware(token string, next http.Handler) http.Handler {
	return AuthMiddlewareWithAudit(token, nil, next)
}

// AuthMiddlewareWithAudit enforces optional constant-time Bearer token
// authentication on Coordinator HTTP routes and emits a security audit log
// entry via logger when a request is denied. When token is empty, it returns
// next unchanged. GET /healthz, GET /ui, and GET /ui/ are always exempt.
func AuthMiddlewareWithAudit(token string, logger *slog.Logger, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	expected := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && (r.URL.Path == "/healthz" || r.URL.Path == "/ui" || r.URL.Path == "/ui/") {
			next.ServeHTTP(w, r)
			return
		}
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), bearerPrefix)
		if !ok || subtle.ConstantTimeCompare([]byte(got), expected) != 1 {
			logSecurityAudit(r.Context(), logger, "auth_bearer", "denied", TenantFromContext(r.Context()), "", "", r.Method+" "+r.URL.Path)
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, ErrorResponse{Error: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
