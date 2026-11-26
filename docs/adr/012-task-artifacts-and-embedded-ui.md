# ADR-012: Content-Addressed Task Artifacts and Embedded Flight-Recorder Web UI
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
As ChronosSched moves into Milestone 4 (`docs/designs/chronos-sched-m4.md`), two operational gaps limit production adoption:
1. **Journal Bloat from Large Task Outputs:** Storing task logs, payloads, and binary outputs inside `engine.Event` payloads inflates the SQLite WAL event journal, degrades `LoadRunEvents` replay latency, and duplicates large blobs across forked timelines.
2. **Headless Observability & Operator Access:** The Bubble Tea TUI (`chronos workflow inspect --live`) requires an interactive terminal. Operators running ChronosSched in containers, Kubernetes, or remote VMs need a browser-accessible UI to inspect runs, worker drain state, `/readyz` health, and DLQ entries without deploying a separate Node.js frontend service.

## Decision

### 1. Content-Addressed SQLite Task Artifact Store (`internal/store/artifacts.go`, `internal/store/sqlite/artifacts.go`)
- **Optional `ArtifactStore` interface:** Define `ArtifactStore` in `internal/store/artifacts.go` alongside `JournalStore` (matching the `DLQStore` extension pattern in `internal/store/dlq.go`) so existing store implementations remain compatible.
- **SHA-256 Deduplication in SQLite:**
  - Store immutable raw bytes once in `artifact_blobs` keyed by `sha256`.
  - Store per-task references in `task_artifacts` (`id`, `tenant_id`, `run_id`, `task_id`, `name`, `sha256`, `size_bytes`, `content_type`, `created_at_ns`), decoupling payload storage from the `events` table.
  - Event journal entries reference only lightweight artifact IDs/digests, preserving deterministic replay and fast timeline forks.
- **HTTP & Client Surface:** Expose `/v1/runs/{id}/tasks/{task_id}/artifacts` (list/upload) and `/v1/runs/{id}/tasks/{task_id}/artifacts/{artifact_id}` (download with `ETag: "<sha256>"`) plus CLI/SDK helpers.

### 2. Zero-Build Embedded Web Console (`internal/ui/`)
- **Single-Binary `embed.FS`:** Bundle static HTML, CSS, and vanilla ES module JavaScript in `internal/ui/` using Go's `//go:embed` directive, mounted at `GET /ui/` on the Coordinator's HTTP server.
- **Zero Build Toolchain:** Avoid Node.js, npm, webpack, or Vite in the repository and CI pipeline. Keeping the UI as plain web standards ensures `go build ./...` and `go test -race ./...` remain self-contained.
- **API-Driven & Auth-Aware:** The UI consumes existing Coordinator JSON/SSE endpoints (`/v1/runs`, `/v1/runs/{id}/events`, `/v1/workers`, `/v1/dlq`, `/v1/dlq/{id}/requeue`, `/readyz`, `/metrics`) and attaches `Authorization: Bearer <token>` and `X-Chronos-Tenant` headers from user-provided session inputs when configured.

## Alternatives Considered
- **External Object Storage (S3/GCS) as the only artifact backend:** Rejected for the default single-binary path because it violates ChronosSched's zero-external-infrastructure constraint (`AGENTS.md` / M1 design). Keeping `ArtifactStore` as an interface allows an S3/GCS adapter to be added later without changing engine or HTTP contracts.
- **Storing artifacts on local filesystem paths outside SQLite:** Rejected because filesystem files fall out of sync with SQLite WAL backups, `OpenReadOnly` inspection, and `tenant_id` / run deletion cleanup.
- **React/TypeScript SPA with Vite build step:** Rejected to avoid introducing Node.js build dependencies, lockfiles, and supply-chain surface into a pure-Go single-binary project.

## Consequences & Failure Modes
- **Positive:**
  - Preserves single-binary distribution (`CGO_ENABLED=0`) and zero external infra requirements.
  - Keeps event journal replay fast and deterministic regardless of task output size.
  - Deduplicates identical task outputs across retries and timeline forks via SHA-256 content addressing.
- **Failure modes & Mitigations:**
  - *SQLite database growth from large artifacts:* Mitigated by enforcing a configurable max artifact size per upload and providing `DeleteRunArtifacts` / retention cleanup.
  - *Browser UI drift from `/v1` API:* Mitigated by serving the UI from the same binary and version as the API handlers and covering `/ui/` routes with Go HTTP integration tests.
  - *Unauthenticated UI access to sensitive run data:* Mitigated by keeping mutating actions behind the Coordinator's `AuthMiddleware` (`/v1/...`) and avoiding embedding any static secrets in `internal/ui/`.
