# ADR-001: Deterministic Event Journal, SQLite WAL Dual-Pool, and Fencing Model
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context

ChronosSched Milestone 1 delivers a single-binary Go distributed workflow scheduler with live multi-process worker crash recovery (<5s detection), deterministic side-effect-free replay, and Git-style checkpoint forking (`chronos fork` and `chronos diff`), with zero mandatory external infrastructure (no Postgres, Redis, or Kafka).

We need an architecture that:
1. Guarantees 100% deterministic state reconstruction from recorded events and enables fast, zero-flakiness unit/fault-injection testing.
2. Prevents `SQLITE_BUSY` write races and deferred-transaction lock-upgrade deadlocks under concurrent worker heartbeats and completions.
3. Enforces honest at-least-once task execution semantics where zombie workers cannot commit stale completions after a lease expires.
4. Supports copy-on-write timeline forking from any completed checkpoint without mutating parent runs or rewriting storage in Milestone 2.

## Decision

1. **Five Deep `internal/` Packages + Thin `pkg/chronos` Facade:**
   - `internal/engine`: Pure, clock-injected deterministic state machine (`ParseDAG`, `Step`, `Fold`, `DiffRuns`) and canonical `EventType` enum with zero I/O.
   - `internal/store`: Pluggable `JournalStore` interface + pure-Go `modernc.org/sqlite` WAL adapter.
   - `internal/runtime`: Single-writer Coordinator (HTTP/JSON RPC + bounded SSE fanout + lease reaper) and standalone Worker process (built-in `sim`, `shell`, `http` handlers).
   - `internal/tui`: Bubble Tea live workflow inspector (`[k]` SIGKILL enabled only in `chronos demo`, disabled in `workflow inspect --live`).
   - `cmd/chronos`: Thin CLI entrypoint + `//go:embed` default 100-task `demo.yaml`.
   - `pkg/chronos`: Minimal 1-file public Go handler registration SDK (`HandlerFunc`, `RegisterHandler`, `RunWorker`).

2. **SQLite WAL Dual-Pool Concurrency & Durability:**
   - Dedicated writer pool: `SetMaxOpenConns(1)`, `SetMaxIdleConns(1)`, `_journal_mode=WAL`, `_txlock=immediate`, `_foreign_keys=1`, `_busy_timeout=2000` (strictly shorter than the `5s` RPC timeout), `_synchronous=NORMAL` (guarantees zero acknowledged state loss across worker/coordinator `SIGKILL` crashes while keeping commits fast for the provisional `<5ms` p95 local scheduling target).
   - Separate read-only reader pool (`SetMaxOpenConns(4)`) for concurrent `history`, `replay`, `diff`, and inspect reads.
   - Optimistic append guard via `PRIMARY KEY (run_id, seq)` plus indexes on `(run_id, event_type, seq)` and `runs(status)`.

3. **Reference-by-Lineage Copy-on-Write Forking & Automatic Checkpoints:**
   - Every `TASK_COMPLETED` atomically appends a numbered `CHECKPOINT_SAVED` (`cp=1, 2, 3...`, with optional YAML `checkpoint_name`) in the same WAL transaction.
   - `chronos fork run_001 --checkpoint <N|name> --override task.key=value` resolves the checkpoint to `fork_from_seq`, records `parent_run_id` and `fork_from_seq` in `runs`, stores only post-fork events (`seq > fork_from_seq`) for `run_002`, clears inherited active leases on non-completed tasks (re-enqueuing them as `PENDING`), and rejects `--override` targeting any task already `COMPLETED <= fork_from_seq`.
   - `JournalStore.LoadRunEvents(run_id)` recursively resolves ancestor prefixes across single- or multi-hop forks to return one unified ordered event stream to `engine.Fold`.

4. **Monotonic Fencing Tokens & `<5s` Lease Budget:**
   - Every task lease increments a monotonic `fencing_token`.
   - Worker heartbeat interval: `1s`; Lease TTL: `3s`; Coordinator lease reaper tick: `250ms` (worst-case crash detection ~`3.3s` including WAL commit, safely under `<5s` with 2-missed-heartbeat tolerance).
   - Lease expiry appends `LEASE_EXPIRED` + `TASK_OUTCOME_AMBIGUOUS` and reschedules per retry policy; late completions with stale tokens are rejected (`STALE_FENCING_TOKEN`, HTTP 409).
   - Coordinator restart reloads non-terminal runs via `engine.Fold` and applies a bounded `3s` startup grace window for surviving workers to reconnect and renew active leases.

5. **Transport Protocol:**
   - Go stdlib `net/http` with typed JSON RPC endpoints for coordinator-worker and mutating CLI calls (`workflow run`, `fork`) plus SSE (`text/event-stream`) with bounded per-subscriber channels for live TUI updates. Offline `history`, `replay`, and `diff` read the SQLite WAL file directly in read-only mode.

## Alternatives Considered

- **8 Public `pkg/` Packages:** Rejected because it leaks internal structs as public API before schemas stabilize and introduces shallow wrapper boilerplate.
- **ConnectRPC + Protobuf:** Deferred to keep M1 zero-codegen so `go install` and `go test ./...` work with pure Go stdlib.
- **Content-Addressed Merkle-DAG Custom Store:** Rejected for M1 because building a crash-safe custom object store duplicates storage-engine work and distracts from scheduler/lease correctness.
- **`_synchronous=FULL` on SQLite WAL:** Deferred because per-commit `F_FULLFSYNC` on macOS ARM64 APFS adds 3-10ms latency; M1's stated durability boundary is OS process termination (`SIGKILL`), which `WAL + NORMAL` guarantees.

## Consequences & Failure Modes

- **Positive:** Millisecond deterministic simulation tests in `internal/engine` with zero I/O; zero `SQLITE_BUSY` races; clean `JournalStore` seam for Milestone 2 consensus or custom storage engines.
- **Trade-offs / Failure Modes:**
  - `_synchronous=NORMAL` does not guarantee survival against sudden OS power loss before WAL checkpointing (acceptable for M1 process-crash boundary; documented honestly).
  - Single-coordinator writer is a single point of failure during coordinator downtime (mitigated by SQLite WAL recovery + `3s` reconnect grace window; multi-coordinator HA deferred to Milestone 2).
  - Slow SSE subscribers are dropped/forced to resync via bounded channels so a paused terminal never stalls task scheduling.
