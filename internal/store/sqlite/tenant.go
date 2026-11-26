package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

const tenantSchemaSQL = `
CREATE TABLE IF NOT EXISTS run_tenants (
	run_id    TEXT PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default'
);
CREATE INDEX IF NOT EXISTS idx_run_tenants_tenant ON run_tenants(tenant_id);
`

var _ store.TenantStore = (*Store)(nil)

func (s *Store) ensureTenantSchema(ctx context.Context) error {
	if _, err := s.writer.ExecContext(ctx, tenantSchemaSQL); err != nil {
		return fmt.Errorf("init tenant schema: %w", err)
	}
	return nil
}

// SetRunTenant assigns or updates the tenant ID for a workflow run.
// An empty tenantID defaults to store.DefaultTenant.
func (s *Store) SetRunTenant(ctx context.Context, runID, tenantID string) error {
	if s.readOnly {
		return store.ErrStoreReadOnly
	}
	if err := s.ensureTenantSchema(ctx); err != nil {
		return err
	}
	if tenantID == "" {
		tenantID = store.DefaultTenant
	}

	_, err := s.writer.ExecContext(ctx, `
		INSERT INTO run_tenants (run_id, tenant_id)
		VALUES (?, ?)
		ON CONFLICT(run_id) DO UPDATE SET tenant_id = excluded.tenant_id`,
		runID, tenantID,
	)
	if err != nil {
		return fmt.Errorf("set tenant for run %q: %w", runID, err)
	}
	return nil
}

// GetRunTenant returns the tenant ID assigned to runID, defaulting to
// store.DefaultTenant when the run has no explicit assignment or when the
// run_tenants table does not yet exist.
func (s *Store) GetRunTenant(ctx context.Context, runID string) (string, error) {
	var tenantID string
	err := s.reader.QueryRowContext(ctx, `SELECT tenant_id FROM run_tenants WHERE run_id = ?`, runID).Scan(&tenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table") {
			return store.DefaultTenant, nil
		}
		return "", fmt.Errorf("get tenant for run %q: %w", runID, err)
	}
	if tenantID == "" {
		return store.DefaultTenant, nil
	}
	return tenantID, nil
}

// ListRunIDsByTenant returns run IDs belonging to tenantID, optionally filtered
// by run statuses and ordered newest-first by creation timestamp.
// If run_tenants does not yet exist (e.g. on a read-only database before any
// tenant write), it falls back to querying runs when tenantID is
// store.DefaultTenant or returns an empty slice otherwise.
func (s *Store) ListRunIDsByTenant(ctx context.Context, tenantID string, statuses ...engine.RunStatus) ([]string, error) {
	if tenantID == "" {
		tenantID = store.DefaultTenant
	}

	query := `
		SELECT r.run_id
		FROM runs r
		LEFT JOIN run_tenants rt ON r.run_id = rt.run_id
		WHERE COALESCE(rt.tenant_id, 'default') = ?`
	args := []any{tenantID}
	if clause, statusArgs := buildStatusFilter("r.status", statuses); clause != "" {
		query += ` AND ` + clause
		args = append(args, statusArgs...)
	}
	query += ` ORDER BY r.created_at_ns DESC`

	rows, err := s.reader.QueryContext(ctx, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return s.listRunIDsFallback(ctx, tenantID, statuses)
		}
		return nil, fmt.Errorf("list run ids for tenant %q: %w", tenantID, err)
	}
	return scanRunIDs(rows)
}

func (s *Store) listRunIDsFallback(ctx context.Context, tenantID string, statuses []engine.RunStatus) ([]string, error) {
	if tenantID != store.DefaultTenant {
		return []string{}, nil
	}

	query := `SELECT run_id FROM runs`
	var args []any
	if clause, statusArgs := buildStatusFilter("status", statuses); clause != "" {
		query += ` WHERE ` + clause
		args = append(args, statusArgs...)
	}
	query += ` ORDER BY created_at_ns DESC`

	rows, err := s.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list run ids fallback for tenant %q: %w", tenantID, err)
	}
	return scanRunIDs(rows)
}

func buildStatusFilter(column string, statuses []engine.RunStatus) (string, []any) {
	if len(statuses) == 0 {
		return "", nil
	}
	placeholders := make([]string, len(statuses))
	args := make([]any, len(statuses))
	for i, st := range statuses {
		placeholders[i] = "?"
		args[i] = string(st)
	}
	return column + ` IN (` + strings.Join(placeholders, ",") + `)`, args
}

func scanRunIDs(rows *sql.Rows) ([]string, error) {
	defer func() { _ = rows.Close() }()

	runIDs := []string{}
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, fmt.Errorf("scan run id: %w", err)
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run ids: %w", err)
	}
	return runIDs, nil
}
