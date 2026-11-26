package store

import (
	"context"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

// DefaultTenant is the fallback tenant identifier used when a run has no
// explicit tenant assignment or an empty tenant ID is supplied.
const DefaultTenant = "default"

// TenantStore is an optional JournalStore extension for row-level tenant
// isolation across workflow runs.
type TenantStore interface {
	SetRunTenant(ctx context.Context, runID, tenantID string) error
	GetRunTenant(ctx context.Context, runID string) (string, error)
	ListRunIDsByTenant(ctx context.Context, tenantID string, statuses ...engine.RunStatus) ([]string, error)
}
