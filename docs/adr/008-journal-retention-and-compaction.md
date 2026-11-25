# ADR-008: Journal Retention and Compaction
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
ChronosSched persists every workflow run and its append-only event stream in a SQLite WAL database (`runs` and `events`). Over time, completed, failed, and cancelled runs accumulate unboundedly. However, ChronosSched supports time-travel forks (`parent_run_id`, `fork_from_seq`), where `LoadRunEvents` recursively walks parent prefixes to reconstruct a forked child's event stream. Naively deleting old terminal runs by `created_at_ns` would break lineage resolution for newer or still-running descendant forks.

## Decision
1. **Optional `RetentionStore` Extension (`internal/store/retention.go`):**
   - Introduce `RetentionStore` with `PruneTerminalRuns(ctx, PruneOptions) (PruneReport, error)` rather than expanding `JournalStore`, preserving backwards compatibility for alternate store implementations.
   - `PruneOptions` supports `CutoffNs` (delete terminal runs with `created_at_ns < CutoffNs`), `DryRun` (compute `CandidateRunIDs` and `SkippedLineageParents` without mutating SQLite), and `Vacuum` (run `PRAGMA wal_checkpoint(TRUNCATE)` and `VACUUM` after commit).
2. **Lineage-Safe Fix-Point Pruning (`internal/store/sqlite/retention.go`):**
   - Execute run selection and deletion inside a single `IMMEDIATE` transaction on `s.writer` (rejecting read-only stores with `store.ErrStoreReadOnly`).
   - Initially mark terminal runs (`COMPLETED`, `FAILED`, `CANCELLED`) with `created_at_ns < CutoffNs` for deletion. Active (`PENDING`, `RUNNING`, `PAUSED`) runs are never eligible.
   - Run a bottom-up fix-point pass: any run *not* marked for deletion protects its `parent_run_id`, which transitively protects the entire ancestor chain. Protected parents are reported in `SkippedLineageParents`. Conversely, a multi-hop chain where every run is old and terminal is pruned together in one pass.
   - Delete `events` before `runs` in bounded batches to respect `FOREIGN KEY (run_id) REFERENCES runs(run_id)` and SQLite variable limits.
   - When `Vacuum` is true (and `DryRun` is false), execute `PRAGMA wal_checkpoint(TRUNCATE)` followed by `VACUUM` outside the transaction on `s.writer`.

## Alternatives Considered
- **Foreign key `ON DELETE RESTRICT` on `parent_run_id`:** Would require schema migration on existing databases and would still require application-level fix-point logic to prune multi-hop terminal chains cleanly in a single pass.
- **In-place event flattening on fork:** Materializing full parent prefixes into every forked child would eliminate parent dependencies at the cost of write amplification and storage bloat on large journals.

## Consequences & Failure Modes
- **Long-lived forks pin ancestors:** A single active or recent fork keeps its entire ancestor chain alive until that leaf (and any siblings) become eligible. `SkippedLineageParents` makes this visible to operators.
- **`VACUUM` pauses writers briefly:** Because `VACUUM` rewrites the database file outside the prune transaction on `s.writer`, it is opt-in via `PruneOptions.Vacuum`.
