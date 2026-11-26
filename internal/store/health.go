package store

import "context"

// HealthChecker is an optional interface that a JournalStore may implement
// to provide an O(1) readiness check without scanning run records.
type HealthChecker interface {
	Ping(ctx context.Context) error
}
