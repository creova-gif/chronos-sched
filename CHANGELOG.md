# Changelog

All notable changes to ChronosSched will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **SDK Run Polling (`pkg/chronos`):** Added `(*Client).WaitForRun(ctx, runID, pollInterval)` polling helper.
- **Webhook SSRF & Redirect Protection (`internal/runtime`):** Added `WebhookConfig.BlockPrivateIPs`, `WebhookConfig.AllowRedirects`, and `--webhook-block-private-ips` CLI flag.
- **Middleware Ordering (`internal/runtime`):** Enforced auth-before-rate-limit middleware ordering in `Coordinator.Handler()`.
- **SDK Run Listing (`pkg/chronos`):** Added `Client.ListRuns` for listing and filtering coordinator runs.
- **Bearer Auth Audit Logging:** Added `AuthMiddlewareWithAudit` emitting structured `auth_bearer` security audit logs.
- **TUI Scrubber Exit Hint:** Updated `RenderScrubBanner` to display `[s/esc] Exit` alongside step controls.
- **Run Listing Pagination & Filtering:** Added `?status` (comma-separated `RUNNING|PAUSED|COMPLETED|FAILED|CANCELLED`) and `?limit` (default `100`, `1..500`) query parameters to `GET /v1/runs` with `400` validation errors.
- **Doctor Diagnostics & Posture Checks:** Added `Ping` connectivity and `auth_token` security posture checks to `chronos doctor`.
- **CLI Runs Command:** Added `chronos runs [--status RUNNING|COMPLETED|FAILED] [--coordinator ...]` to list and filter coordinator runs.
- **O(1) Readiness Probe:** Added `HealthChecker.Ping` for lightweight, constant-time `/readyz` readiness checks.
- **Uniform Request Body Guardrails:** Standardized `decodeJSONBody` with a 2 MiB maximum body size across Coordinator HTTP handlers.
- **Worker Panic Recovery:** Added `invokeHandlerSafe` in the worker execution loop to recover panics and convert them into structured task failures.
- **Per-Tenant Active Run Quota:** Added `MaxActiveRunsPerTenant` enforcement on workflow submission and forks.
- **HTTP Security Headers:** Added `SecurityHeadersMiddleware` for baseline security headers and Web Console CSP.
- **Web Console Auto-Refresh:** Added an `Auto-refresh (3s)` toggle to the embedded Flight-Recorder Web Console.
- **Per-Tenant Token-Bucket Rate Limiting (ADR-014):** `RateLimitMiddleware` returning `429 Too Many Requests` with `Retry-After`, configured via `runtime.RateLimitConfig` and `chronos server start` flags (`--rate-limit-rps`, `--rate-limit-burst`).
- **Row-Level Tenant Run Isolation (ADR-014):** `TenantStore` persistence and `Coordinator` enforcement isolating run access and listings by `X-Chronos-Tenant`.
- **HTTP Payload Guardrails (ADR-014):** `http.MaxBytesReader` request body size limits on Coordinator HTTP endpoints.
- **Workflow Submission Idempotency (ADR-015):** Per-tenant `Idempotency-Key` deduplication on `POST /v1/workflows/run` backed by `IdempotencyStore` (`200` replay, `409` workflow conflict), `Client.SubmitWorkflowIdempotent`, `chronos workflow run --idempotency-key`, and `IdempotencyStore.PruneIdempotencyKeys`.
- **Worker Poll Jitter:** Added jitter to worker polling intervals to reduce coordinator thundering-herd contention.
- **Architecture Decision Records:** Added `ADR-014` (`docs/adr/014-rate-limiting-and-tenant-enforcement.md`) and `ADR-015` (`docs/adr/015-idempotency-and-enterprise-hardening.md`).
- **Structured JSON Server Logging:** Added structured JSON `slog` server logging (`--log-level`) wired into `chronos server start`.
- **Idempotency Key Garbage Collection:** Added `chronos gc` pruning of expired `idempotency_keys` (`pruned_idempotency_keys`).
- **WCAG 2.1 AA Web Console Accessibility:** Added `skip-link`, `<main>` landmark, `:focus-visible` outlines, and `aria-live="polite"` status announcements to the embedded Web Console.
- **Runtime Performance Benchmarks:** Added runtime middleware and `/readyz` benchmarks (`internal/runtime/bench_test.go`).

## [0.2.0] - 2026-11-26

### Added
- **Content-Addressed Task Artifacts (ADR-012):** SQLite `task_artifacts` store (`internal/store/artifacts.go`, `internal/store/sqlite/artifacts.go`), Coordinator HTTP endpoints (`POST /v1/runs/{id}/tasks/{task_id}/artifacts`, `GET /v1/runs/{id}/artifacts`, `GET /v1/runs/{id}/tasks/{task_id}/artifacts/{name}`), `pkg/chronos` SDK helpers, and `chronos artifacts list|get|put` CLI commands.
- **Embedded Flight-Recorder Web Console (ADR-012):** Zero-build `embed.FS` web UI served at `GET /ui/` (`internal/ui/`) for inspecting runs, workers, DLQ entries, and `/readyz` health.
- **Human-in-the-Loop Approval Gates (ADR-013):** Deterministic `WAITING_APPROVAL` task state with `TASK_APPROVAL_REQUESTED`, `TASK_APPROVED`, and `TASK_REJECTED` journal events (`internal/engine/approval.go`), Coordinator endpoints (`POST /v1/runs/{id}/tasks/{task_id}/request-approval`, `POST /v1/runs/{id}/tasks/{task_id}/approve`), `pkg/chronos` SDK methods, and `chronos approve` CLI command.
- **Multi-Tenant `X-Chronos-Tenant` Middleware (ADR-013):** `TenantMiddleware` (`internal/runtime/tenant.go`) validating `X-Chronos-Tenant` (defaulting to `"default"`), echoing the resolved tenant header, and propagating tenant context across requests.
- **Container & CI Tooling:** Added `Dockerfile`, `docker-compose.yml`, and `.github/workflows/ci.yml` for reproducible builds and automated CI verification.
- **Architecture Decision Records:** Added `ADR-012` (`docs/adr/012-task-artifacts-and-embedded-ui.md`) and `ADR-013` (`docs/adr/013-approval-gates-and-tenant-isolation.md`).
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
