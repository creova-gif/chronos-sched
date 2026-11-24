# ADR-002: Programmable Scheduling Constraints in Deterministic Engine
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Milestone 1 established a pure, clock-injected event-sourced scheduling engine (`Step` / `Apply` / `Fold`) with monotonic fencing tokens, lease expiration, and timeline forking. To support realistic multi-resource workloads and SLA-bound pipelines in Milestone 2, the scheduler needs programmable per-task scheduling constraints—specifically resource requirements, exponential retry backoff, and task execution deadlines—while preserving 100% deterministic replay, backward compatibility with existing callers, and stronger replay verification invariants.

## Decision
1. **Declarative Task Constraints (`TaskSpec` & `dag.go`):**
   - Extend `TaskSpec` with optional `Resources map[string]int` (`resources`), `DeadlineMs int64` (`deadline_ms`), and `RetryBackoffMs int64` (`retry_backoff_ms`).
   - Validate in `BuildDAG` that `DeadlineMs >= 0`, `RetryBackoffMs >= 0`, and every resource key is non-empty with value `>= 0`.
2. **Resource-Aware Leasing (`CmdLeaseNextTask`):**
   - Add optional `AvailableResources map[string]int` to `CmdLeaseNextTask`.
   - When `AvailableResources` is `nil` or empty, resource checks are bypassed to maintain 100% backward compatibility with existing workers and tests.
   - When non-empty, `pickNextReadyTask` only selects a ready task when `AvailableResources[k] >= need` for every `k, need` in `spec.Resources`.
3. **Exponential Retry Backoff (`TaskState.RetryAfterNs`):**
   - When `CmdFailTask` or `CmdExpireLeases` schedules a retry (`WillRetry == true`) for a task with `RetryBackoffMs > 0`, compute `RetryAfterNs = nowNs + (RetryBackoffMs * (1 << (attempt - 1))) * 1e6` (shift clamped to `[0, 30]`) and record it in `TaskFailedPayload` / `LeaseExpiredPayload`.
   - `Apply` persists `RetryAfterNs` on `TaskState` without requiring `DAG` access, preserving deterministic replay and `Apply`'s existing signature.
   - `pickNextReadyTask` skips pending tasks where `ts.RetryAfterNs > nowNs`.
4. **Task Deadlines (`TaskState.StartedAtNs` & `CmdExpireLeases`):**
   - Record `StartedAtNs` on `TaskState` at first `TASK_LEASED` (or `TASK_STARTED` if unset), and clear it only on `TIMELINE_FORKED` for non-completed tasks.
   - In `CmdExpireLeases`, expire any `LEASED`/`RUNNING` task whose `spec.DeadlineMs > 0` and `nowNs - ts.StartedAtNs >= spec.DeadlineMs * 1e6` with reason `"task deadline exceeded"` and an accompanying `TASK_OUTCOME_AMBIGUOUS` event.
5. **Replay Safety Invariants (`VerifyReplay`):**
   - Enforce during replay that every `TASK_LEASED` event only leases a task whose `DependsOn` dependencies are already `COMPLETED`, and that `fencing_token` is strictly increasing across all `TASK_LEASED` events.

## Alternatives Considered
- **Passing `*DAG` into `Apply` to recompute backoff during replay:** Rejected because it breaks `Apply`'s existing signature across `internal/runtime` and `internal/store`, and storing `RetryAfterNs` directly in the event payload makes event journals self-describing even if backoff policy changes later.
- **Separate `EventTaskDeadlineExceeded` event type:** Rejected to avoid introducing new event variants that downstream consumers (`internal/tui`, `internal/store`) must handle; reusing `LEASE_EXPIRED` with reason `"task deadline exceeded"` plus `TASK_OUTCOME_AMBIGUOUS` preserves full compatibility.
- **Global cluster resource accounting inside `RunState`:** Rejected for M2 because resource availability is worker-local at lease time; passing `AvailableResources` on `CmdLeaseNextTask` keeps the engine stateless with respect to worker topology.

## Consequences & Failure Modes
- **Positive:** Fully deterministic under injected clocks; zero breaking changes to `internal/runtime`, `internal/store`, `cmd/chronos`, or `internal/tui`; corrupted or out-of-order journals are caught immediately by `VerifyReplay`.
- **Failure Modes:**
  - If a task's `Resources` exceed every worker's `AvailableResources`, the task will remain `PENDING` indefinitely until a capable worker polls (no automatic deadlock detection in `Step`).
  - Because `StartedAtNs` is anchored at first lease, a task that exceeds its `DeadlineMs` with remaining retries will continue to exceed its deadline on subsequent reaper ticks unless `DeadlineMs` is intended as a hard overall task SLA.
