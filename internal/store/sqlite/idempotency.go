package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/creova-gif/chronos-sched/internal/store"
)

const idempotencySchemaSQL = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
	tenant_id       TEXT NOT NULL,
	idempotency_key TEXT NOT NULL,
	run_id          TEXT NOT NULL,
	workflow_hash   TEXT NOT NULL,
	created_at_ns   INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, idempotency_key)
);
`

var _ store.IdempotencyStore = (*Store)(nil)

func (s *Store) ensureIdempotencySchema(ctx context.Context) error {
	if _, err := s.writer.ExecContext(ctx, idempotencySchemaSQL); err != nil {
		return fmt.Errorf("init idempotency schema: %w", err)
	}
	return nil
}

// GetIdempotencyKey looks up an idempotency record for (tenantID, key).
// An empty tenantID defaults to store.DefaultTenant.
// If the key is not found or the idempotency_keys table does not yet exist,
// it returns a zero record, false, and nil.
func (s *Store) GetIdempotencyKey(ctx context.Context, tenantID, key string) (store.IdempotencyRecord, bool, error) {
	if tenantID == "" {
		tenantID = store.DefaultTenant
	}

	var rec store.IdempotencyRecord
	err := s.reader.QueryRowContext(ctx, `
		SELECT tenant_id, idempotency_key, run_id, workflow_hash, created_at_ns
		FROM idempotency_keys
		WHERE tenant_id = ? AND idempotency_key = ?`,
		tenantID, key,
	).Scan(&rec.TenantID, &rec.Key, &rec.RunID, &rec.WorkflowHash, &rec.CreatedAtNs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table") {
			return store.IdempotencyRecord{}, false, nil
		}
		return store.IdempotencyRecord{}, false, fmt.Errorf("get idempotency key (%q, %q): %w", tenantID, key, err)
	}
	return rec, true, nil
}

// PutIdempotencyKey records an idempotency key mapping if not already present.
// An empty TenantID defaults to store.DefaultTenant.
func (s *Store) PutIdempotencyKey(ctx context.Context, rec store.IdempotencyRecord) error {
	if s.readOnly {
		return store.ErrStoreReadOnly
	}
	if err := s.ensureIdempotencySchema(ctx); err != nil {
		return err
	}
	if rec.TenantID == "" {
		rec.TenantID = store.DefaultTenant
	}

	_, err := s.writer.ExecContext(ctx, `
		INSERT INTO idempotency_keys (tenant_id, idempotency_key, run_id, workflow_hash, created_at_ns)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(tenant_id, idempotency_key) DO NOTHING`,
		rec.TenantID, rec.Key, rec.RunID, rec.WorkflowHash, rec.CreatedAtNs,
	)
	if err != nil {
		return fmt.Errorf("put idempotency key (%q, %q): %w", rec.TenantID, rec.Key, err)
	}
	return nil
}

// PruneIdempotencyKeys deletes idempotency keys with created_at_ns < cutoffNs
// and returns the number of deleted rows.
// If the idempotency_keys table does not yet exist, it returns 0, nil.
func (s *Store) PruneIdempotencyKeys(ctx context.Context, cutoffNs int64) (int64, error) {
	if s.readOnly {
		return 0, store.ErrStoreReadOnly
	}

	res, err := s.writer.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE created_at_ns < ?`, cutoffNs)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, fmt.Errorf("prune idempotency keys: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune idempotency keys rows affected: %w", err)
	}
	return n, nil
}
