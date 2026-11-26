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

# Fork an isolated timeline from an earlier checkpoint, diff, merge, and inspect lineage
chronos fork run_001 --checkpoint 3 --override task.key=value
chronos diff run_001 run_002
chronos merge run_001 run_002 --prefer-branch --apply
chronos branches

# Export portable SHA-256 flight-recorder bundles and Perfetto / Chrome Trace timelines
chronos export run_001 -o crash.bundle.json
chronos import crash.bundle.json --run-id run_repro
chronos trace run_001 -o trace.json

# Recurring deterministic cron schedules, live lifecycle controls, and journal GC
chronos schedule add nightly --cron "0 2 * * *" --workflow pipeline.yaml
chronos workflow pause run_001 && chronos workflow resume run_001
chronos gc --older-than 168h --dry-run
chronos doctor
chronos inspect-at run_001 --checkpoint 3
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
- [ADR-005: Prometheus Metrics & Observability](docs/adr/005-prometheus-metrics-and-observability.md)
- [ADR-006: Deterministic Cron Schedules](docs/adr/006-deterministic-cron-schedules.md)
- [ADR-007: Flight-Recorder Bundle & Perfetto Trace](docs/adr/007-flight-recorder-bundle-and-perfetto-trace.md)
- [ADR-008: Journal Retention & Compaction](docs/adr/008-journal-retention-and-compaction.md)
- [ADR-009: Webhook Notifications & Retries](docs/adr/009-webhook-notifications-and-retries.md)
- [ADR-010: Coordinator Bearer Auth](docs/adr/010-coordinator-bearer-auth.md)
- [ADR-011: Dead-Letter Queue Requeue, Middleware & Readiness Probe](docs/adr/011-dlq-requeue-and-observability.md)
- [Dead-Letter Queue (`chronos dlq list`), JSON Request Logging (`slog`), W3C `traceparent` & TUI Time-Travel Scrubber (`[s]` in TUI)](CHANGELOG.md)
- [OpenAPI 3.1 Specification](docs/api/openapi.yaml)
- [CHANGELOG](CHANGELOG.md)
- [Threat Model](docs/THREAT_MODEL.md)
- [Engineering Standards](AGENTS.md)
