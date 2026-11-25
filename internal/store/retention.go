package store

import "context"

// PruneOptions configures terminal run pruning and optional SQLite compaction.
type PruneOptions struct {
	CutoffNs int64 `json:"cutoff_ns"`
	DryRun   bool  `json:"dry_run"`
	Vacuum   bool  `json:"vacuum"`
}

// PruneReport summarizes the result of a PruneTerminalRuns invocation.
type PruneReport struct {
	CandidateRunIDs       []string `json:"candidate_run_ids"`
	DeletedRuns           int      `json:"deleted_runs"`
	DeletedEvents         int64    `json:"deleted_events"`
	SkippedLineageParents []string `json:"skipped_lineage_parents"`
	Vacuumed              bool     `json:"vacuumed"`
}

// RetentionStore is an optional JournalStore extension for lineage-safe
// pruning of terminal runs and storage compaction.
type RetentionStore interface {
	PruneTerminalRuns(ctx context.Context, opts PruneOptions) (PruneReport, error)
}
