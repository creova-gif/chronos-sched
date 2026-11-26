package sqlite

import (
	"context"

	"github.com/creova-gif/chronos-sched/internal/store"
)

var _ store.HealthChecker = (*Store)(nil)

// Ping verifies SQLite reader connectivity with an O(1) query.
func (s *Store) Ping(ctx context.Context) error {
	var one int
	return s.reader.QueryRowContext(ctx, "SELECT 1").Scan(&one)
}
