# ADR-009: Webhook Notifications and Retries
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Chronos-Sched needs to notify external systems (CI pipelines, alerting, downstream orchestrators) when workflow runs reach terminal states (`COMPLETED`, `FAILED`, `CANCELLED`). Because the coordinator scheduling loop is single-threaded and latency-sensitive, slow or unreachable webhook endpoints must never stall task leasing or state transitions, and receivers must be able to authenticate payloads.

## Decision
We implement an asynchronous `WebhookNotifier` in `internal/runtime/webhooks.go`:
- **Non-blocking bounded queue:** `Notify` pushes `WebhookPayload` into a buffered channel (`QueueSize`, default `128`) using a `select`/`default` drop path, guaranteeing O(1) non-blocking enqueue from the coordinator loop.
- **Background worker & retries:** `Start(ctx)` drains the queue and delivers HTTP `POST` JSON requests with `Timeout` (default `5s`), retrying transient failures (network errors, HTTP `>= 500`, and `429`) with exponential backoff (`BaseBackoff`, default `100ms`) up to `MaxAttempts` (default `3`).
- **HMAC-SHA256 signing:** When `Secret` is configured, `SignPayload` sets `X-Chronos-Signature-256: sha256=<hex>` over the exact marshaled JSON body.
- **Strict URL validation:** `NewWebhookNotifier` rejects non-`http`/`https` schemes and empty hosts.

## Alternatives Considered
- **Synchronous HTTP delivery in `Step` / coordinator:** Rejected because network timeouts or slow endpoints would block scheduling and lease heartbeats.
- **Unbounded queue or persistent outbox table:** Rejected for M3 to keep memory bounded and avoid extra SQLite write amplification; can be layered on later if guaranteed-once delivery is required.

## Consequences & Failure Modes
- **Load shedding:** Under sustained webhook outages exceeding `QueueSize`, excess terminal notifications are dropped (counted via `Dropped()`) rather than exhausting memory or stalling the scheduler.
- **At-most-once across restarts:** In-memory queued notifications are lost if the coordinator process terminates or `ctx` is cancelled before draining.
- **Non-retryable 4xx:** Client errors other than `429` are treated as permanent and not retried.
