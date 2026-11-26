# ADR-011: Dead-Letter Queue, Requeueing, and HTTP Observability
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
As ChronosSched workflows run in production-like environments, operators need three capabilities beyond core scheduling and Prometheus counters (ADR-005):
1. **Dead-Letter Queue (DLQ) & Requeueing:** When a task exhausts all retries (or fails terminally on lease expiration), operators need a durable record of the failure reason, attempt count, and workflow context, plus a way to inspect and requeue failed work.
2. **Structured Request Logging & Trace Correlation:** HTTP requests to the Coordinator need structured JSON logs (`slog`) with W3C `Traceparent` and `X-Request-Id` propagation, latency/byte accounting, and panic recovery that does not break SSE streaming (`http.Flusher`).
3. **Readiness Probing (`/readyz`):** Unlike `/healthz` (which only confirms the HTTP listener is up), orchestrators and load balancers need a readiness probe that verifies `JournalStore` connectivity before routing traffic.

## Decision

### 1. Dead-Letter Queue (`DLQStore` & `dead_letters`)
- **Storage interface (`internal/store/dlq.go`):** Define `DeadLetterEntry` and the optional `DLQStore` extension (`RecordDeadLetter`, `ListDeadLetters`, `MarkDeadLetterRequeued`) so `JournalStore` implementations can opt in without breaking existing store contracts.
- **SQLite adapter (`internal/store/sqlite/dlq.go`):** Persist dead-lettered task failures in a `dead_letters` table indexed by `(requeued, failed_at_ns DESC, id DESC)`. `ListDeadLetters` gracefully returns an empty slice if the table does not yet exist on a read-only database.
- **Coordinator & CLI/HTTP surface:**
  - The Coordinator records dead letters via `DLQStore` when tasks exhaust retries or expire terminally.
  - `chronos dlq list [--all] [--db ...]` inspects active or all (including requeued) entries directly from the SQLite WAL journal.
  - HTTP endpoints `GET /v1/dlq` and `POST /v1/dlq/{id}/requeue` expose DLQ inspection and requeueing over the Coordinator API.

### 2. Structured `slog` Middleware & W3C Trace Propagation (`ObservabilityMiddleware`)
- Implemented in `internal/runtime/middleware.go` and wrapping the Coordinator's `AuthMiddleware` + `http.ServeMux`:
  - **Trace & Request IDs:** Parses incoming W3C `Traceparent` headers (`00-<trace_id>-<parent_id>-<flags>`), generating a fresh 16-byte `trace_id` when absent or malformed and always minting a new 8-byte `span_id` on the response `Traceparent` header. Propagates or generates `X-Request-Id`.
  - **Structured JSON logging:** Logs `method`, `path`, `status`, `bytes`, `duration_ms`, `trace_id`, and `request_id` via `log/slog` (`Debug` level for `/healthz` to avoid probe noise; `Info` level for all other routes).
  - **Panic recovery & SSE compatibility:** Recovers handler panics, logs the stack trace at `Error` level, and returns `500 Internal Server Error` JSON if headers have not yet been sent. `statusResponseWriter` implements `http.Flusher` and `Unwrap()` so SSE streams (`/v1/runs/{id}/events`) continue flushing without buffering.

### 3. Readiness Probe (`/readyz`)
- Implemented in `internal/runtime/readyz.go`:
  - `handleReadyz` probes store readiness via `c.store.ListRuns(r.Context(), engine.RunStatusRunning)`.
  - If the store returns an error, it responds with `503 Service Unavailable` and `ReadyzResponse{Status: "not_ready", Error: err.Error()}`.
  - Otherwise it briefly locks `c.mu` to snapshot `len(c.runs)` and `len(c.workers)` and responds with `200 OK` and `ReadyzResponse{Status: "ready", ActiveRuns: ..., Workers: ...}`.

## Alternatives Considered
- **Separate DLQ database file:** Rejected to avoid cross-database consistency issues and extra operational flags; co-locating `dead_letters` in the existing SQLite WAL journal keeps backups and `OpenReadOnly` inspection atomic.
- **OpenTelemetry Go SDK for HTTP tracing:** Rejected in favor of zero-dependency W3C `Traceparent` parsing and `log/slog` JSON output, consistent with ADR-005's minimal-dependency policy.
- **Reusing `/healthz` for DB checks:** Rejected because liveness (`/healthz`) must remain fast, unauthenticated, and independent of transient store lock contention, whereas readiness (`/readyz`) specifically gates traffic on store availability.

## Consequences & Failure Modes
- **Positive:** Zero new external dependencies; operators gain durable failure visibility, correlated JSON logs, and accurate readiness signaling.
- **Failure modes:**
  - *Store unavailability during `/readyz`:* Returns `503` immediately with the underlying store error string, signaling orchestrators to drain traffic without crashing the process.
  - *Malformed `Traceparent` headers:* Treated as absent and replaced with a newly generated trace ID rather than rejecting client requests.
  - *DLQ growth:* Unrequeued entries accumulate in `dead_letters` over time and should be periodically pruned or requeued by operators.
