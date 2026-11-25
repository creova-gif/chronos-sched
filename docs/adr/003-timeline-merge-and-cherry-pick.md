# ADR-003: Timeline Merge and Cherry-Pick Planning
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Chronos already supports deterministic timeline forking (`CmdForkRun`) and timeline comparison (`DiffRuns`), enabling operators to branch an execution from a saved checkpoint, explore alternative parameters or fixes, and inspect diverging events. As workflows grow into multi-branch DAGs, operators need a safe way to bring completed task outputs from a forked branch back onto a base timeline—either as a whole-branch merge or as an explicit cherry-pick of specific tasks—without silently breaking causal consistency for downstream tasks.

## Decision
Introduce a pure, deterministic merge planner in `internal/engine/merge.go` (`PlanBranchMerge` and `FormatMergePlan`) that operates solely on the workflow `DAG` and the base/branch event streams:

1. **Common-Ancestor Anchoring:** Use `DiffRuns` and `Fold` to locate `CommonPrefixSeq` and reconstruct both `RunState` snapshots without side effects.
2. **Task Selection:**
   - Full merge (`taskIDs` empty): select all tasks in `dag.TopoOrder` that completed on `branch` after `CommonPrefixSeq` and whose completion status or output differs from `base`.
   - Cherry-pick (`taskIDs` non-empty): validate every requested task exists in `dag` and has `TaskStatusCompleted` on `branch`, returning a clear error otherwise, and emit selections in `dag.TopoOrder`.
3. **Conflict Detection & Resolution Strategies:**
   - A conflict occurs when a selected task also completed after `CommonPrefixSeq` on `base` with a different JSON output (`!jsonEqual`).
   - `MergeFailOnConflict` (default): records all conflicts in `MergePlan.Conflicts`, omits conflicting tasks from `AdoptedTasks`, and returns both the populated `*MergePlan` and sentinel `ErrMergeConflict` so callers can inspect conflicts programmatically.
   - `MergePreferBranch`: records the conflict metadata for auditability while adopting the branch's task output into `AdoptedTasks`.
4. **Causal Downstream Invalidation:** Traverse `dag.Dependents` transitively from all `AdoptedTasks`, exclude any task already in `AdoptedTasks`, and emit `InvalidatedDownstream` ordered by `dag.TopoOrder`. Every downstream task whose upstream inputs changed must re-execute to preserve causal consistency.
5. **Human-Readable Preview:** Provide `FormatMergePlan` to render a compact Git-style summary (`+` adopted, `!` conflicts, `~` invalidated downstream).

## Alternatives Considered
- **Event-Log Splicing (Rebasing raw events):** Replaying raw branch events directly onto the base journal breaks monotonic sequence numbers, fencing tokens, and interleaved worker leases. Planning at the task-output and DAG-dependency level keeps replay invariants intact.
- **Silent Last-Write-Wins by Timestamp:** Comparing wall-clock timestamps across forked runs is non-deterministic and risks silently dropping base work. Explicit conflict detection anchored at `CommonPrefixSeq` with opt-in `MergePreferBranch` makes divergence visible and deterministic.
- **Direct Downstream Invalidation Only:** Invalidating only immediate dependents would leave grandchildren with stale cached outputs derived from superseded upstream values. Transitive invalidation via `dag.Dependents` guarantees full causal consistency.

## Consequences & Failure Modes
- **Positive:** Merge planning is 100% pure, deterministic, and testable without storage or worker side effects; callers receive actionable conflict diagnostics even on failure.
- **Limitation:** Output equality relies on canonical JSON comparison (`jsonEqual`); non-deterministic map payloads or external side effects outside recorded task outputs cannot be detected by the planner.
- **Failure Modes:** Requesting an unknown or non-completed branch task fails fast before planning; `MergeFailOnConflict` prevents accidental overwrite of diverged base work unless the caller explicitly chooses `MergePreferBranch`.
