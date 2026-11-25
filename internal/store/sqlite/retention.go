package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

const pruneBatchSize = 500

var _ store.RetentionStore = (*Store)(nil)

type pruneRunRow struct {
	runID       string
	parentRunID string
	status      engine.RunStatus
	createdAtNs int64
}

// PruneTerminalRuns deletes terminal runs created before opts.CutoffNs while
// preserving any parent run referenced by a non-deleted descendant run.
func (s *Store) PruneTerminalRuns(ctx context.Context, opts store.PruneOptions) (store.PruneReport, error) {
	if s.readOnly {
		return store.PruneReport{}, store.ErrStoreReadOnly
	}

	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return store.PruneReport{}, fmt.Errorf("begin prune tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	runs, err := loadRunsForPruneTx(ctx, tx)
	if err != nil {
		return store.PruneReport{}, err
	}

	candidates, skipped := computePruneSet(runs, opts.CutoffNs)
	report := store.PruneReport{
		CandidateRunIDs:       candidates,
		SkippedLineageParents: skipped,
	}

	if opts.DryRun {
		return report, nil
	}

	if len(candidates) > 0 {
		deletedEvents, deletedRuns, err := deleteRunsAndEventsTx(ctx, tx, candidates)
		if err != nil {
			return store.PruneReport{}, err
		}
		report.DeletedEvents = deletedEvents
		report.DeletedRuns = deletedRuns
	}

	if err := tx.Commit(); err != nil {
		return store.PruneReport{}, fmt.Errorf("commit prune tx: %w", err)
	}

	if opts.Vacuum {
		if _, err := s.writer.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			return report, fmt.Errorf("wal checkpoint truncate: %w", err)
		}
		if _, err := s.writer.ExecContext(ctx, `VACUUM`); err != nil {
			return report, fmt.Errorf("vacuum sqlite db: %w", err)
		}
		report.Vacuumed = true
	}

	return report, nil
}

func loadRunsForPruneTx(ctx context.Context, tx *sql.Tx) ([]pruneRunRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT run_id, parent_run_id, status, created_at_ns FROM runs`)
	if err != nil {
		return nil, fmt.Errorf("query runs for prune: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var runs []pruneRunRow
	for rows.Next() {
		var r pruneRunRow
		var status string
		if err := rows.Scan(&r.runID, &r.parentRunID, &status, &r.createdAtNs); err != nil {
			return nil, fmt.Errorf("scan run for prune: %w", err)
		}
		r.status = engine.RunStatus(status)
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs for prune: %w", err)
	}
	return runs, nil
}

func isTerminalStatus(status engine.RunStatus) bool {
	return status == engine.RunStatusCompleted ||
		status == engine.RunStatusFailed ||
		status == engine.RunStatusCancelled
}

func computePruneSet(runs []pruneRunRow, cutoffNs int64) (candidates []string, skipped []string) {
	toDelete := make(map[string]bool, len(runs))
	for _, r := range runs {
		if isTerminalStatus(r.status) && r.createdAtNs < cutoffNs {
			toDelete[r.runID] = true
		}
	}

	// Fix-point propagation: if a child run is NOT being deleted in this pass,
	// its parent cannot be deleted either, protecting the entire ancestor chain.
	protected := make(map[string]bool)
	for {
		changed := false
		for _, r := range runs {
			if r.parentRunID == "" {
				continue
			}
			if !toDelete[r.runID] && toDelete[r.parentRunID] {
				toDelete[r.parentRunID] = false
				protected[r.parentRunID] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	for id, del := range toDelete {
		if del {
			candidates = append(candidates, id)
		}
	}
	for id := range protected {
		skipped = append(skipped, id)
	}
	sort.Strings(candidates)
	sort.Strings(skipped)
	return candidates, skipped
}

func deleteRunsAndEventsTx(ctx context.Context, tx *sql.Tx, runIDs []string) (int64, int, error) {
	var totalEvents int64
	var totalRuns int

	for start := 0; start < len(runIDs); start += pruneBatchSize {
		end := start + pruneBatchSize
		if end > len(runIDs) {
			end = len(runIDs)
		}
		batch := runIDs[start:end]

		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for i, id := range batch {
			placeholders[i] = "?"
			args[i] = id
		}
		inClause := strings.Join(placeholders, ",")

		resEv, err := tx.ExecContext(ctx, `DELETE FROM events WHERE run_id IN (`+inClause+`)`, args...)
		if err != nil {
			return 0, 0, fmt.Errorf("delete events for pruned runs: %w", err)
		}
		evCount, err := resEv.RowsAffected()
		if err != nil {
			return 0, 0, fmt.Errorf("events rows affected: %w", err)
		}
		totalEvents += evCount

		resRun, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE run_id IN (`+inClause+`)`, args...)
		if err != nil {
			return 0, 0, fmt.Errorf("delete pruned runs: %w", err)
		}
		runCount, err := resRun.RowsAffected()
		if err != nil {
			return 0, 0, fmt.Errorf("runs rows affected: %w", err)
		}
		totalRuns += int(runCount)
	}

	return totalEvents, totalRuns, nil
}
