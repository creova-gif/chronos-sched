# ChronosSched

**Deterministic distributed workflow scheduler and interactive execution flight recorder in a single Go binary.**

ChronosSched combines multi-worker DAG execution with an append-only event journal, live crash recovery, side-effect-free deterministic replay, and Git-style timeline branching (`fork` and `diff`) — with zero mandatory external infrastructure (no Postgres, Redis, or Kafka).

## Quick Start (Milestone 1 Target)

```bash
# Run the self-contained 5-worker live TUI demo (embedded 100-task DAG)
chronos demo --workers 5

# Inspect recorded execution history and replay deterministically (offline SQLite read)
chronos history run_001
chronos replay run_001

# Fork an isolated timeline from an earlier checkpoint, diff, merge-plan, and inspect lineage
chronos fork run_001 --checkpoint 3 --override task.key=value
chronos diff run_001 run_002
chronos merge run_001 run_002 --prefer-branch
chronos branches

# Active-standby HA coordinator + resource-constrained workers
chronos server start --ha-id coord-1 --ha-ttl 5s
chronos worker start --concurrency 4 --resources cpu=4,mem_mb=1024
```

## Measured Local Performance (Apple M5, darwin/arm64)

- **Pure Engine (`BenchmarkSchedule100TaskDAG`):** 100-task DAG scheduled and folded in `1.14ms`–`2.02ms` (`~11–20 µs/task`).
- **Real SQLite WAL (`BenchmarkSQLiteWALDispatchAndComplete`, 200x):** `0.118 ms` p50, **`0.313 ms` p95**, `0.455 ms` p99 per lease + WAL commit + complete + checkpoint WAL cycle.
- **Worker `SIGKILL` Recovery (`TestE2EWorkerSIGKILLRecoveryUnder5Seconds`):** Real OS child worker killed via `SIGKILL`, detected and recovered under `<5s` (`1s` heartbeat, `3s` lease TTL, `250ms` reaper tick).

## Execution Guarantees

- **At-least-once task execution**, guarded by idempotency keys and monotonic `fencing_token` values.
- **Durable state transitions** committed to SQLite WAL before acknowledging progress.
- **Deterministic replay** reconstructed from recorded events without re-executing external side effects.
- **Explicit ambiguity handling** (`TASK_OUTCOME_AMBIGUOUS`) when a worker dies mid-lease before reporting completion.
- **No false exactly-once claims** for external side effects without destination cooperation.

## Documentation

- [Milestone 1 Design & Engineering Review](docs/designs/chronos-sched-m1.md)
- [ADR-001: Event Journal, SQLite WAL & Fencing Model](docs/adr/001-event-journal-sqlite-fencing.md)
- [ADR-002: Programmable Scheduling Constraints](docs/adr/002-programmable-scheduling-constraints.md)
- [ADR-003: Timeline Merge & Cherry-Pick](docs/adr/003-timeline-merge-and-cherry-pick.md)
- [ADR-004: Coordinator HA Leader Lease](docs/adr/004-coordinator-ha-leader-lease.md)
- [Threat Model](docs/THREAT_MODEL.md)
- [Engineering Standards](AGENTS.md)
