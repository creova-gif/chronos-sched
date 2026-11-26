package store

import "context"

// IdempotencyRecord maps an idempotency key within a tenant to the workflow run
// and workflow hash that first claimed it.
type IdempotencyRecord struct {
	TenantID     string
	Key          string
	RunID        string
	WorkflowHash string
	CreatedAtNs  int64
}

// IdempotencyStore is an optional JournalStore extension for deduplicating
// workflow submissions by (tenant_id, idempotency_key).
type IdempotencyStore interface {
	GetIdempotencyKey(ctx context.Context, tenantID, key string) (IdempotencyRecord, bool, error)
	PutIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error
	PruneIdempotencyKeys(ctx context.Context, cutoffNs int64) (int64, error)
}
