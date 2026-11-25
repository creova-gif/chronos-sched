# Changelog

All notable changes to ChronosSched will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- High-concurrency chaos test (`TestChaosConcurrentWorkersKillAndLiveFork`) exercising a 30-task layered DAG across 4 concurrent workers with abrupt 50% worker fleet loss, concurrent live timeline forking from checkpoint 3 during recovery, and full offline `VerifyReplay` and `DiffRuns` validation.

## [0.1.0] - 2026-11-25

### Added
- **Deterministic Core Engine (`internal/engine`, ADR-001):**
  - YAML DAG parser, cycle detection, topological ordering, and pure event-sourced `Step`/`Fold` state machine.
  - At-least-once task execution guarded by monotonic `fencing_token` values and explicit `TASK_OUTCOME_AMBIGUOUS` recording on mid-lease worker loss.
  - Side-effect-free offline `VerifyReplay`, Git-style timeline `Fork` from numeric or named checkpoints with parameter overrides, and `DiffRuns` timeline comparison.
- **Programmable Scheduling Constraints (ADR-002):**
  - Task `priority`, per-task `resources` requirements, worker resource capacity scheduling (`--resources`), and `concurrency_group` limits.
- **Timeline Merge Planning (ADR-003):**
  - Deterministic branch merge planning (`chronos merge` with `--prefer-branch`) and lineage tree inspection (`chronos branches`).
- **SQLite WAL Dual-Pool Store & HA Leader Election (`internal/store/sqlite`, ADR-004):**
  - Single-writer SQLite WAL journal pool with concurrent read pool for offline inspection and replay.
  - Active-standby Coordinator HA leader lease election (`--ha-id`, `--ha-ttl`).
- **Distributed Runtime & Public Go SDK (`internal/runtime`, `pkg/chronos`):**
  - Coordinator HTTP/JSON API with bounded non-blocking SSE event streaming, lease expiration reaper, and crash-restart `Recover()` from SQLite WAL.
  - Standalone polling Worker with heartbeats, automatic reconnect backoff, built-in `sim`/`shell`/`http` handlers, and custom handler registration via `pkg/chronos`.
- **CLI, Interactive TUI & Packaging (`cmd/chronos`, `internal/tui`):**
  - Single-binary CLI (`demo`, `server`, `worker`, `run`, `history`, `replay`, `fork`, `diff`, `merge`, `branches`) and live Bubble Tea TUI with SSE streaming and interactive checkpoint picker.
  - GoReleaser configuration (`.goreleaser.yaml`) and security threat model (`docs/THREAT_MODEL.md`).
- **Verification & Measured Benchmarks (Apple M5, darwin/arm64):**
  - Real OS child-process `SIGKILL` recovery verified under `<5s` (`TestE2EWorkerSIGKILLRecoveryUnder5Seconds`) and Coordinator crash-restart recovery (`TestE2ECoordinatorCrashRestartRecovery`).
  - Pure engine 100-task DAG scheduled and folded in `1.14ms`–`2.02ms` (`BenchmarkSchedule100TaskDAG`).
  - SQLite WAL lease + commit + complete + checkpoint cycle measured at `0.118ms` p50, `0.313ms` p95, `0.455ms` p99 (`BenchmarkSQLiteWALDispatchAndComplete`, 200x).
