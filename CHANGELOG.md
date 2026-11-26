# Changelog

All notable changes to ChronosSched will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Timeline Merge Execution (ADR-003):** Execute deterministic branch merges (`chronos merge --apply`, `POST /v1/runs/{id}/merge`) in addition to dry-run merge planning.
- **Coordinator HA Leader Election (ADR-004):** Active-standby Coordinator leader lease election (`--ha-id`, `--ha-ttl`).
- **Prometheus Metrics & Instrumentation (ADR-005):** `/metrics` Prometheus exposition endpoint and runtime instrumentation.
- **Deterministic Cron Schedules (ADR-006):** Cron schedule management via `chronos schedule add|list|delete`.
- **Workflow Lifecycle Control:** Pause, resume, and cancel controls across Coordinator API, CLI, and TUI.
- **Flight-Recorder Bundles & Trace Export (ADR-007):** Portable run export/import bundles and Perfetto/Chrome Trace output (`chronos export|import|trace`).
- **Lineage-Safe Journal Retention & Compaction (ADR-008):** `chronos gc` retention pruning and compaction preserving active fork lineage.
- **Webhook Notifications (ADR-009):** Outbound webhook delivery with HMAC-SHA256 payload signing (`--webhook-url`, `--webhook-secret`) and retry backoff.
- **Coordinator & Worker Bearer Token Auth (ADR-010):** Optional constant-time Bearer token authentication (`--auth-token`, `CHRONOS_AUTH_TOKEN`).
- **Secrets Redaction:** `--redact` flag in `chronos history` and `chronos export` to scrub sensitive keys from event payloads.
- **Diagnostics & Versioning:** `chronos doctor` environment/journal health checks and `chronos version` build metadata command.
- **Public Go Client SDK (`pkg/chronos`):** SSE `StreamRunEvents`, Bearer token authentication, and Dead-Letter Queue methods.
- **Interactive TUI Enhancements:** Run/task filters, workflow lifecycle controls, and Time-Travel Scrubber (`[s]` toggle, `left`/`[` & `right`/`]`).
- **Dead-Letter Queue (ADR-011):** `dead_letters` SQLite table, `chronos dlq list|requeue`, `GET /v1/dlq`, and `POST /v1/dlq/{id}/requeue`.
- **HTTP Observability & Readiness (ADR-011):** Structured JSON `slog` request logging, W3C `Traceparent` / `X-Request-Id` propagation, panic recovery (`ObservabilityMiddleware`), and `/readyz` readiness probe.
- **OpenAPI 3.1 Specification:** Updated `docs/api/openapi.yaml` covering merge, lifecycle, DLQ, and readiness endpoints.
- **High-Concurrency Chaos Test:** `TestChaosConcurrentWorkersKillAndLiveFork` exercising a 30-task layered DAG across 4 concurrent workers with abrupt 50% worker fleet loss, concurrent live timeline forking from checkpoint 3 during recovery, and full offline `VerifyReplay` and `DiffRuns` validation.

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
