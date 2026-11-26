# ADR-015: Idempotency Keys, Security Headers, and Worker Connection Pooling
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
As ChronosSched matures toward enterprise multi-tenant deployments, three reliability and security gaps needed to be addressed across the Coordinator API and Worker runtime:
1. **Duplicate Workflow Submissions on Client Retry:** Network timeouts or gateway retries on `POST /v1/workflows/run` could create duplicate workflow runs if clients had no way to supply a deterministic idempotency key.
2. **Baseline HTTP Security Headers:** Coordinator responses—especially the embedded Flight-Recorder Web Console served at `/ui/`—lacked standard browser security headers (`X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, and `Content-Security-Policy`).
3. **Worker HTTP Connection Churn:** Go's default `http.Transport` limits `MaxIdleConnsPerHost` to `2`. Under high worker concurrency (`cfg.Concurrency`), concurrent heartbeats, task polls, and task completions against the Coordinator exhausted idle connections and caused unnecessary TCP/TLS reconnects.

## Decision

### 1. Per-Tenant `Idempotency-Key` Deduplication (`IdempotencyStore`)
- **Store Interface (`internal/store/idempotency.go`):**
  - Defined `store.IdempotencyStore` with `GetIdempotencyKey(ctx, tenantID, key)` and `PutIdempotencyKey(ctx, rec)` operating on `IdempotencyRecord` (`TenantID`, `Key`, `RunID`, `WorkflowHash`, `CreatedAtNs`).
- **SQLite Persistence (`internal/store/sqlite/idempotency.go`):**
  - Added the `idempotency_keys` table with `PRIMARY KEY (tenant_id, idempotency_key)` and `ON CONFLICT(tenant_id, idempotency_key) DO NOTHING`, defaulting empty `tenantID` values to `store.DefaultTenant`.
- **Coordinator HTTP Contract (`POST /v1/workflows/run`):**
  - `handleSubmitWorkflow` inspects the optional `Idempotency-Key` request header:
    - If `(tenant_id, idempotency_key)` exists with a matching `WorkflowHash`, the Coordinator returns `200 OK` with the existing `SubmitWorkflowResponse` (idempotent replay) without creating a new run or consuming active-run quota.
    - If `(tenant_id, idempotency_key)` exists with a different `WorkflowHash`, the Coordinator rejects the request with `409 Conflict`.
    - Otherwise, the run is created (`201 Created`) and the idempotency mapping is persisted.

### 2. Baseline HTTP Security Headers (`SecurityHeadersMiddleware`)
- **`SecurityHeadersMiddleware` (`internal/runtime/security_headers.go`):**
  - Wraps Coordinator HTTP handlers to attach `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: no-referrer` on all responses.
  - On `/ui` and `/ui/` routes, attaches `Content-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:`.

### 3. Worker `http.Transport` Connection Pooling
- **Tuned Worker Client (`internal/runtime/worker.go`):**
  - `NewWorker` configures its `http.Client` with a custom `http.Transport`:
    - `MaxIdleConns: max(64, cfg.Concurrency*2)`
    - `MaxIdleConnsPerHost: max(32, cfg.Concurrency*2)`
    - `IdleConnTimeout: 90 * time.Second`
  - This keeps persistent connections pooled across concurrent worker goroutines communicating with the Coordinator.

## Alternatives Considered
- **In-memory idempotency cache only:** Rejected because Coordinator restarts or HA leader failovers would lose idempotency mappings and allow duplicate workflow runs on retry.
- **Global (non-tenant-scoped) idempotency keys:** Rejected because one tenant's key could collide with or leak information to another tenant.
- **Relying on Go's `http.DefaultTransport`:** Rejected because `MaxIdleConnsPerHost: 2` throttles connection reuse for workers running with higher concurrency.

## Consequences & Failure Modes
- **Positive:**
  - Safe, deterministic client retries on `POST /v1/workflows/run` with strict tenant scoping and payload mismatch detection (`409 Conflict`).
  - Hardened browser security posture for the embedded Web Console and API responses.
  - Reduced connection churn and lower tail latency between Workers and the Coordinator.
- **Failure modes & Mitigations:**
  - *Idempotency key reused with modified workflow YAML:* Mitigated by comparing `WorkflowHash` and returning `409 Conflict` rather than silently replaying a stale run.
  - *Unbounded growth of `idempotency_keys` over long-running deployments:* Future retention/compaction policies can prune entries older than the journal retention window without affecting active deduplication.
