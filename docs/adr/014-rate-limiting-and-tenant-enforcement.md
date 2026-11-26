# ADR-014: Per-Tenant Rate Limiting, Row-Level Tenant Enforcement, and Payload Guardrails
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
ADR-013 introduced `TenantMiddleware` (`X-Chronos-Tenant` header parsing and context propagation) and `AuthMiddleware`, but two enterprise multi-tenancy and hardening gaps remained:
1. **Noisy-Neighbor API Protection:** Without per-tenant request throttling, a single tenant submitting high-frequency workflow or polling requests could saturate Coordinator HTTP handlers and SQLite write locks for all other tenants.
2. **Row-Level Tenant Isolation & Payload Bounds:** Carrying a tenant ID in `r.Context()` alone did not prevent a tenant from reading, mutating, or listing runs owned by another tenant, nor did HTTP handlers bound incoming request body sizes against oversized payloads.

## Decision

### 1. Per-Tenant Token-Bucket Rate Limiting (`RateLimitMiddleware`)
- **Configuration (`runtime.RateLimitConfig` & CLI Flags):**
  - `CoordinatorConfig.RateLimit` configures per-tenant token-bucket throttling via `RequestsPerSecond` (`--rate-limit-rps`, `0` = disabled) and `Burst` (`--rate-limit-burst`).
- **Enforcement & Response Contract:**
  - `RateLimitMiddleware` runs after `TenantMiddleware` so every request is attributed to its resolved tenant (`TenantFromContext`).
  - When a tenant's token bucket is exhausted, the middleware short-circuits with HTTP `429 Too Many Requests` and sets the `Retry-After` response header so clients and SDKs can back off deterministically.
  - When `RequestsPerSecond <= 0`, rate limiting is disabled and requests pass through with zero overhead.

### 2. Row-Level Tenant Run Isolation (`TenantStore` & `Coordinator`)
- **Persistent Tenant Ownership (`TenantStore`):**
  - Run records persist their owning tenant ID via `TenantStore`, associating every newly submitted run with the caller's resolved tenant (`"default"` when omitted).
- **Coordinator Access Enforcement:**
  - `Coordinator` enforces tenant ownership on run-scoped operations (inspect, history, lifecycle control, approvals, artifacts, merge, and run listings) by comparing the request's tenant context against the run's owning tenant.
  - Cross-tenant access attempts are rejected rather than leaking run metadata or state across tenant boundaries.

### 3. Request Payload Guardrails (`http.MaxBytesReader`)
- **Bounded Request Bodies:**
  - Coordinator JSON and workflow-submission HTTP handlers wrap `r.Body` with `http.MaxBytesReader` to enforce explicit maximum payload sizes, preventing memory exhaustion from oversized or malformed request bodies.

## Alternatives Considered
- **Global (non-tenant) rate limiter:** Rejected because a single noisy tenant would exhaust the shared quota and cause `429` errors for well-behaved tenants.
- **Database-per-tenant SQLite files:** Rejected for now because managing dynamic connection pools, WAL checkpoints, and cross-tenant leader election across N SQLite files adds significant operational complexity compared to row-level `TenantStore` isolation in a single journal.
- **Relying solely on reverse-proxy body limits:** Rejected because ChronosSched is frequently deployed standalone (`chronos server start`) or in tests without an external ingress proxy.

## Consequences & Failure Modes
- **Positive:**
  - Fair per-tenant API isolation with standard `429` + `Retry-After` backpressure semantics.
  - Strong row-level isolation preventing accidental or unauthorized cross-tenant run access.
  - Bounded HTTP request memory usage via `MaxBytesReader`.
- **Failure modes & Mitigations:**
  - *Legitimate burst traffic hitting `429`:* Mitigated by exposing `--rate-limit-burst` separately from `--rate-limit-rps` and returning `Retry-After` for client backoff.
  - *Legacy single-tenant runs without explicit tenant headers:* Mitigated by consistently defaulting omitted `X-Chronos-Tenant` headers to `"default"` across `TenantMiddleware`, `TenantStore`, and `Coordinator`.
  - *Oversized workflow YAML or artifact metadata payloads:* Mitigated by `MaxBytesReader` failing fast before full body buffering.
