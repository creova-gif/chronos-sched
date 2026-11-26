# ADR-013: Human-in-the-Loop Approval Gates and Multi-Tenant Isolation
- **Status:** Proposed
- **Owner:** Justin Mafie

## Context
As ChronosSched advances through Milestone 4 (`docs/designs/chronos-sched-m4.md`), production workflows require two foundational control-plane capabilities:
1. **Human-in-the-Loop Approval Gates:** Sensitive workflow steps (such as production deployments, schema migrations, or external mutations) need an explicit pause-and-approve mechanism before a worker leases and executes them, with every decision recorded deterministically in the event journal.
2. **Multi-Tenant Request Isolation:** Shared Coordinator deployments serving multiple teams or environments need a lightweight, backward-compatible namespace boundary carried across HTTP requests, logs, and storage contexts without breaking existing single-tenant clients.

## Decision

### 1. Event-Sourced Approval Gates (`internal/engine/approval.go`, `internal/runtime/approval_http.go`)
- **Task Status & Journal Events:**
  - Introduce `TaskStatusWaitingApproval` (`"WAITING_APPROVAL"`) alongside three deterministic journal event types: `TASK_APPROVAL_REQUESTED`, `TASK_APPROVED`, and `TASK_REJECTED`.
  - Each approval event records an `ApprovalPayload` (`task_id`, `actor`, `comment`, `approved`).
- **State Transitions & Deterministic Replay:**
  - `RequestTaskApproval` transitions a `PENDING` task into `WAITING_APPROVAL` and emits `TASK_APPROVAL_REQUESTED`.
  - `ResolveTaskApproval` transitions a `WAITING_APPROVAL` task either back to `PENDING` (`TASK_APPROVED`, allowing normal scheduling) when `approved=true`, or to `FAILED` (`TASK_REJECTED`, setting `LastError = comment`) when `approved=false`.
  - `ApplyApprovalEvent` replays all three event types deterministically onto `RunState` during `Fold` and `VerifyReplay`.
- **HTTP, SDK & CLI Surface:**
  - Expose `POST /v1/runs/{id}/tasks/{task_id}/request-approval` and `POST /v1/runs/{id}/tasks/{task_id}/approve` on the Coordinator, with corresponding helpers in `pkg/chronos` (`pkg/chronos/approvals.go`) and `chronos approve` in the CLI.

### 2. Multi-Tenant HTTP Middleware (`internal/runtime/tenant.go`)
- **`X-Chronos-Tenant` Header & `"default"` Fallback:**
  - `TenantMiddleware` extracts `X-Chronos-Tenant` (`HeaderTenant`), trims whitespace, and falls back to `"default"` (`DefaultTenant`) when the header is omitted or empty.
  - Non-empty tenant IDs are validated by `ValidateTenantID`: maximum length 64 characters, restricted to `[a-zA-Z0-9._-]`. Invalid values fail fast with HTTP `400 Bad Request`.
  - The middleware echoes the resolved `X-Chronos-Tenant` header on every HTTP response and stores it in `r.Context()` via `WithTenant` / `TenantFromContext`.
- **Middleware Ordering:**
  - `TenantMiddleware` runs inside `ObservabilityMiddleware` and before `AuthMiddleware` (`ObservabilityMiddleware -> TenantMiddleware -> AuthMiddleware -> mux`), ensuring tenant validation and context propagation apply uniformly across Coordinator endpoints.

## Alternatives Considered
- **External approval state table outside the event journal:** Rejected because out-of-band approval flags would diverge from `VerifyReplay`, `DiffRuns`, and timeline `Fork` semantics.
- **Polling-based approval task handler instead of a first-class `WAITING_APPROVAL` state:** Rejected because holding a worker lease while waiting for human input wastes worker capacity and triggers lease expiration ambiguity (`TASK_OUTCOME_AMBIGUOUS`).
- **Mandatory `X-Chronos-Tenant` header with no fallback:** Rejected because it would break existing single-tenant CLI, SDK, and test callers; defaulting to `"default"` preserves full backward compatibility.

## Consequences & Failure Modes
- **Positive:**
  - Human approval decisions are immutable, auditable, and deterministically replayable in `chronos history` and `chronos replay`.
  - Tasks in `WAITING_APPROVAL` consume zero worker slots until explicitly approved.
  - Invalid or malicious `X-Chronos-Tenant` headers (e.g., path traversal or whitespace injection) are rejected at the HTTP boundary before reaching handlers.
- **Failure modes & Mitigations:**
  - *Stalled workflows awaiting human approval:* Mitigated by surfacing `WAITING_APPROVAL` status clearly in `/v1/runs/{id}`, the TUI, and the embedded `/ui/` console so operators can approve or reject promptly.
  - *Approval request races with worker dispatch:* Mitigated by `RequestTaskApproval` enforcing `TaskStatusPending` under the Coordinator's state lock and rejecting invalid transitions with `ErrInvalidTransition`.
  - *Cross-tenant header spoofing in unauthenticated deployments:* Mitigated by pairing `TenantMiddleware` with `AuthMiddleware` (`--auth-token`) in shared environments.
