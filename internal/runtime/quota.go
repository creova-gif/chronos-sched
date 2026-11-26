package runtime

import (
	"context"
	"errors"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

// ErrTenantQuotaExceeded is returned when a tenant has reached its configured
// active run limit.
var ErrTenantQuotaExceeded = errors.New("tenant active run quota exceeded")

func (c *Coordinator) checkTenantRunQuota(ctx context.Context) error {
	if c.cfg.MaxActiveRunsPerTenant <= 0 {
		return nil
	}
	if ts, ok := c.store.(store.TenantStore); ok {
		ids, err := ts.ListRunIDsByTenant(ctx, TenantFromContext(ctx), engine.RunStatusRunning, engine.RunStatusPaused)
		if err != nil {
			return err
		}
		if len(ids) >= c.cfg.MaxActiveRunsPerTenant {
			return ErrTenantQuotaExceeded
		}
	}
	return nil
}
