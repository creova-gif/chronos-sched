# ADR-010: Optional Coordinator Bearer Token Authentication
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Chronos-Sched runs both in zero-config local development mode and in shared/remote deployments where the Coordinator HTTP API (`/v1/*` and `/metrics`) must be protected against unauthorized workflow submissions, run mutations, and worker impersonation. Load balancers and CLI health checks still need unauthenticated access to `GET /healthz`.

## Decision
We add optional static Bearer token authentication to `internal/runtime`:
- **Configuration:** `CoordinatorConfig.AuthToken` and `WorkerConfig.AuthToken` default to `""` (open local mode).
- **Middleware (`AuthMiddleware` in `internal/runtime/auth.go`):**
  - When `AuthToken == ""`, `AuthMiddleware` returns the underlying `http.Handler` directly with zero per-request overhead.
  - When `AuthToken != ""`, `GET /healthz` is exempted for health probes, while all other routes require `Authorization: Bearer <token>`.
  - Token verification uses `crypto/subtle.ConstantTimeCompare` to prevent timing side-channel leaks.
  - Missing or invalid credentials return `401 Unauthorized` with `WWW-Authenticate: Bearer` and JSON `ErrorResponse{Error: "unauthorized"}`.
- **Worker client (`Worker.postJSON`):** When `WorkerConfig.AuthToken` is non-empty, the worker attaches `Authorization: Bearer <token>` to all Coordinator HTTP requests (`register`, `heartbeat`, `poll`, `started`, `complete`, `fail`).

## Alternatives Considered
- **mTLS everywhere:** Stronger mutual identity, but significantly higher operational friction for local/single-binary deployments; can be layered in front of the Coordinator via a reverse proxy or TLS listener later.
- **Per-worker JWT / RBAC:** Overkill for current single-tenant M3 scope; static shared secret covers coordinator-to-worker and client-to-coordinator access control cleanly.

## Consequences & Failure Modes
- **Shared secret rotation:** Changing `AuthToken` requires restarting/updating both Coordinator and Workers together (no multi-key grace window in M3).
- **Transport security:** Bearer tokens rely on TLS in non-local deployments to prevent eavesdropping on the wire.
- **Health probe exposure:** `GET /healthz` intentionally remains unauthenticated and must never expose sensitive run state.
