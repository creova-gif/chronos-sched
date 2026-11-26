package runtime

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

const (
	// HeaderTenant is the HTTP header carrying the multi-tenant namespace ID.
	HeaderTenant = "X-Chronos-Tenant"
	// DefaultTenant is the fallback namespace used when no tenant header is provided.
	DefaultTenant = "default"
	maxTenantLen  = 64
)

type tenantContextKey struct{}

// ValidateTenantID trims whitespace from tenant, falls back to DefaultTenant when
// empty, and validates that the ID is at most 64 characters using only [a-zA-Z0-9._-].
func ValidateTenantID(tenant string) (string, error) {
	trimmed := strings.TrimSpace(tenant)
	if trimmed == "" {
		return DefaultTenant, nil
	}
	if len(trimmed) > maxTenantLen {
		return "", fmt.Errorf("invalid tenant ID: length %d exceeds maximum %d", len(trimmed), maxTenantLen)
	}
	for i := 0; i < len(trimmed); i++ {
		if !isValidTenantChar(trimmed[i]) {
			return "", fmt.Errorf("invalid tenant ID %q: contains disallowed character %q", trimmed, trimmed[i])
		}
	}
	return trimmed, nil
}

func isValidTenantChar(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '.' || b == '_' || b == '-'
}

// WithTenant returns a child context carrying the given tenant namespace.
func WithTenant(ctx context.Context, tenant string) context.Context {
	if tenant == "" {
		tenant = DefaultTenant
	}
	return context.WithValue(ctx, tenantContextKey{}, tenant)
}

// TenantFromContext extracts the tenant namespace from ctx, returning
// DefaultTenant if unset.
func TenantFromContext(ctx context.Context) string {
	if ctx == nil {
		return DefaultTenant
	}
	if tenant, ok := ctx.Value(tenantContextKey{}).(string); ok && tenant != "" {
		return tenant
	}
	return DefaultTenant
}

// TenantMiddleware extracts and validates HeaderTenant, echoes the resolved
// tenant on the response header, and attaches it to the request context.
func TenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, err := ValidateTenantID(r.Header.Get(HeaderTenant))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}
		w.Header().Set(HeaderTenant, tenant)
		next.ServeHTTP(w, r.WithContext(WithTenant(r.Context(), tenant)))
	})
}
