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

# Fork an isolated timeline from an earlier checkpoint and diff the two runs
chronos fork run_001 --checkpoint 3 --override task.key=value
chronos diff run_001 run_002
```

## Execution Guarantees

- **At-least-once task execution**, guarded by idempotency keys and monotonic `fencing_token` values.
- **Durable state transitions** committed to SQLite WAL before acknowledging progress.
- **Deterministic replay** reconstructed from recorded events without re-executing external side effects.
- **Explicit ambiguity handling** (`TASK_OUTCOME_AMBIGUOUS`) when a worker dies mid-lease before reporting completion.
- **No false exactly-once claims** for external side effects without destination cooperation.

## Documentation

- [Milestone 1 Design & Engineering Review](docs/designs/chronos-sched-m1.md)
- [ADR-001: Event Journal, SQLite WAL & Fencing Model](docs/adr/001-event-journal-sqlite-fencing.md)
- [Threat Model](docs/THREAT_MODEL.md)
- [Engineering Standards](AGENTS.md)
