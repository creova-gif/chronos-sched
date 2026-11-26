package runtime

import (
	"fmt"
	"net/http"

	"github.com/creova-gif/chronos-sched/internal/store"
)

// authorizeRunTenant verifies that the tenant in r.Context() owns runID when
// the backing store implements store.TenantStore. On a tenant mismatch it writes
// a 404 JSON error response (to avoid leaking run existence across tenants) and
// returns false.
func (c *Coordinator) authorizeRunTenant(w http.ResponseWriter, r *http.Request, runID string) bool {
	if ts, ok := c.store.(store.TenantStore); ok {
		owner, err := ts.GetRunTenant(r.Context(), runID)
		tenantID := TenantFromContext(r.Context())
		if err == nil && owner != tenantID {
			c.metrics.IncTenantDenied()
			c.auditLog(r.Context(), "tenant_access", "denied", tenantID, runID, "", fmt.Sprintf("owner=%s", owner))
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: store.ErrRunNotFound.Error()})
			return false
		}
	}
	return true
}
