# Design: ChronosSched Milestone 4 — Full Product Engineering & Scope Expansion

Status: Proposed
Repo: `github.com/creova-gif/chronos-sched`
Owner: Justin Mafie

## Overview

Milestones 1 through 3 established ChronosSched's deterministic event-sourced DAG engine, SQLite WAL `JournalStore` with monotonic fencing tokens, multi-coordinator HA leader leases, deterministic cron schedules, flight-recorder bundle export, journal retention/compaction, webhook notifications, bearer auth, Dead-Letter Queue (DLQ) with requeueing, structured `slog` + W3C `Traceparent` observability, and `/readyz` readiness probing.

Milestone 4 (M4) expands ChronosSched from a core scheduler + CLI/TUI runtime into a complete, production-ready product surface while preserving its foundational guarantees: **single-binary, zero external infrastructure, deterministic replay, and honest at-least-once semantics**.

M4 comprises five cohesive workstreams:
- **M4.1 Task Artifact & Output Store**
- **M4.2 Embedded Flight-Recorder Web Console**
- **M4.3 Human-in-the-Loop Approval Gates**
- **M4.4 Multi-Tenant Namespace Isolation (`tenant_id`)**
- **M4.5 Production Packaging & CI**

---

## M4.1 Task Artifact & Output Store

### Problem
Embedding large task stdout/stderr logs, generated payloads, or intermediate blobs directly in `engine.Event` records bloats the SQLite event journal, slows `LoadRunEvents` deterministic replay, and inflates timeline fork/diff memory footprints.

### Design
- **Storage Interface (`internal/store/artifacts.go`)**:
  - Define `ArtifactMetadata`, `ArtifactBlob`, and an optional `ArtifactStore` extension interface (following the `DLQStore` pattern in `internal/store/dlq.go`) so `JournalStore` implementations opt in cleanly without breaking existing contracts.
  - Core operations: `PutArtifact(ctx, meta, content) (ArtifactMetadata, error)`, `GetArtifact(ctx, runID, taskID, artifactID) (ArtifactMetadata, []byte, error)`, `ListTaskArtifacts(ctx, runID, taskID) ([]ArtifactMetadata, error)`, and `DeleteRunArtifacts(ctx, runID) error`.
- **Content-Addressed SQLite Adapter (`internal/store/sqlite/artifacts.go`)**:
  - Compute SHA-256 digest of artifact bytes on write.
  - Deduplicate immutable blob payloads in `artifact_blobs(sha256 TEXT PRIMARY KEY, size_bytes INTEGER NOT NULL, content_type TEXT NOT NULL, data BLOB NOT NULL, created_at_ns INTEGER NOT NULL)`.
  - Associate task outputs in `task_artifacts(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, run_id TEXT NOT NULL, task_id TEXT NOT NULL, name TEXT NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, content_type TEXT NOT NULL, created_at_ns INTEGER NOT NULL)` indexed by `(run_id, task_id, created_at_ns)`.
  - Enforce a configurable per-artifact size cap (defaulting to a safe bounded limit, e.g., 16 MiB) and graceful empty-slice return on `OpenReadOnly` databases lacking the table.
  - Journal events only store lightweight artifact reference IDs/digests, keeping replay deterministic and fast.
- **HTTP, CLI & SDK Surface**:
  - `GET /v1/runs/{id}/tasks/{task_id}/artifacts` — list artifact metadata for a task.
  - `POST /v1/runs/{id}/tasks/{task_id}/artifacts` — upload/attach an artifact (used by workers or operators).
  - `GET /v1/runs/{id}/tasks/{task_id}/artifacts/{artifact_id}` — download raw artifact bytes with `Content-Type` and `ETag` (`"<sha256>"`).
  - CLI (`chronos artifacts list` / `chronos artifacts get`) and Go SDK client methods (`ListTaskArtifacts`, `GetArtifact`, `PutArtifact`).

---

## M4.2 Embedded Flight-Recorder Web Console

### Problem
Operators running ChronosSched in containers, headless servers, or CI/staging environments often lack terminal access for the Bubble Tea TUI (`chronos workflow inspect --live`). They need an immediate, zero-setup browser console without Node.js build pipelines or external reverse proxies.

### Design
- **Zero-Build Embedded Assets (`internal/ui/`)**:
  - Use Go's `//go:embed` (`embed.FS`) to bundle static HTML, CSS, and vanilla ES module JavaScript directly into the `chronos` binary.
  - Serve the console at `GET /ui/` (with `GET /ui` redirecting to `/ui/`) via the Coordinator HTTP mux in `internal/runtime/`.
  - Keep `/ui/` read-only by default for inspection, with mutating actions (such as DLQ requeue or approval decisions) calling authenticated `/v1/...` endpoints and passing the operator's bearer token when auth is enabled.
- **Console Views**:
  1. **Live Runs & DAG Timeline**: Lists runs filterable by `status` and `tenant_id`, displays task state transitions, fencing tokens, and streams live updates via existing SSE (`/v1/runs/{id}/events`).
  2. **Worker Fleet & Drain Status**: Shows registered workers, active leases, heartbeat freshness, and draining state.
  3. **Dead-Letter Queue (DLQ)**: Lists unrequeued/all dead-letter entries from `GET /v1/dlq` with one-click requeue (`POST /v1/dlq/{id}/requeue`).
  4. **Health & Metrics Summary**: Surfaces `/readyz` store readiness, active run/worker counts, and quick links to `/metrics` (Prometheus) and `/healthz`.

---

## M4.3 Human-in-the-Loop Approval Gates

### Problem
Production pipelines (deployments, schema migrations, sensitive data exports, or high-cost batch jobs) frequently require an explicit human sign-off before a downstream task executes, and that decision must be auditable and replay-safe.

### Design
- **Workflow Spec Extension**:
  - Tasks in workflow YAML may declare an `approval` gate (`required: true`, optional `prompt`, optional `timeout`).
- **Deterministic Journal Events (`internal/engine/types.go`, `internal/engine/engine.go`)**:
  - `TASK_APPROVAL_REQUESTED` (`EventTaskApprovalRequested`): Emitted when a task's upstream dependencies are satisfied and it enters `WAITING_APPROVAL` instead of immediately becoming runnable/leased.
  - `TASK_APPROVED` (`EventTaskApproved`): Records `actor`, `comment`, and `approved_at_ns`, transitioning the task to runnable so the scheduler can grant a worker lease.
  - `TASK_REJECTED` (`EventTaskRejected`): Records `actor`, `reason`, and `rejected_at_ns`, transitioning the task to a terminal failed/rejected state (and honoring downstream failure/DLQ semantics).
  - Because all three transitions are persisted as ordered journal events, deterministic replay (`chronos replay --dry-run`) and timeline forks reconstruct approval decisions without re-prompting.
- **Operator HTTP & CLI Surface**:
  - `POST /v1/runs/{id}/tasks/{task_id}/approve` (body: `{"decision": "approve" | "reject", "actor": "...", "comment": "..."}`).
  - CLI command `chronos task approve <run_id> <task_id> [--reject] [--comment "..."]` and Web Console action buttons.

---

## M4.4 Multi-Tenant Namespace Isolation (`tenant_id`)

### Problem
Shared ChronosSched coordinators serving multiple teams, environments, or customers currently share a flat global namespace for runs, schedules, artifacts, and DLQ entries.

### Design
- **Backward-Compatible `tenant_id` Partitioning**:
  - Add `TenantID string` (default `"default"`) to `RunRecord`, `DeadLetterEntry`, `ScheduleRecord`, and `ArtifactMetadata`.
  - Schema migration adds `tenant_id TEXT NOT NULL DEFAULT 'default'` with composite indexes on `(tenant_id, ...)` so existing SQLite databases upgrade seamlessly with zero manual migration.
- **Request & CLI Propagation**:
  - HTTP requests read optional `X-Chronos-Tenant` header (falling back to `"default"` when omitted or empty).
  - CLI commands accept `--tenant <id>` (defaulting to `"default"`).
  - Store queries (`ListRuns`, `GetRun`, `ListDeadLetters`, `ListSchedules`, `ListTaskArtifacts`, `GetArtifact`) scope results to the resolved `tenant_id`, returning `ErrRunNotFound` / `404` on cross-tenant access attempts to prevent tenant enumeration.

---

## M4.5 Production Packaging & CI

### Problem
Ad-hoc local `go build` workflows do not provide reproducible container images, local multi-component orchestration, or automated race/lint gates on pull requests.

### Design
- **Multi-Stage `Dockerfile`**:
  - Builder stage: `golang:1.23-alpine` compiling a static `CGO_ENABLED=0` `chronos` binary (`modernc.org/sqlite` requires no CGO).
  - Runtime stage: `gcr.io/distroless/static-debian12:nonroot` (or minimal `alpine` non-root) exposing coordinator HTTP/UI port and mounting `/var/lib/chronos` for SQLite WAL persistence, with `/healthz` and `/readyz` healthcheck support.
- **`docker-compose.yml`**:
  - Defines a `coordinator` service (persisting WAL data in a named volume) and scalable `worker` service replicas connecting to the coordinator.
- **CI Workflow (`.github/workflows/ci.yml`)**:
  - Runs on push and pull_request to `main`.
  - Enforces `gofmt` formatting check (`test -z "$(gofmt -l .)"`), `go vet ./...`, and `go test -race -count=1 ./...` in alignment with `AGENTS.md`.
