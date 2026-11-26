# Threat Model
- **Assets:** API credentials, tenant data (`org_id`), execution logs, downstream tools.
- **Trust Boundaries:** Client -> Gateway -> Sandbox -> Secret Manager / Storage.
- **Mitigations:** Strict schema validation, `org_id` tenant scoping, GCP Secret Manager + OIDC, `gitleaks` in CI.

## Implemented Runtime & CLI Security Mitigations
- **Coordinator & Worker Bearer Token Auth (`AuthMiddleware`, ADR-010):** Constant-time (`crypto/subtle.ConstantTimeCompare`) `Authorization: Bearer <token>` validation across Coordinator HTTP routes via `--auth-token` / `CHRONOS_AUTH_TOKEN` (`GET /healthz` exempt for health probes).
- **Webhook HMAC-SHA256 Signing (ADR-009):** `SignPayload` signs outbound webhook JSON bodies with `X-Chronos-Signature-256: sha256=<hex>` when `--webhook-secret` is configured, allowing receivers to verify authenticity and payload integrity.
- **Secrets Redaction (`--redact`):** `chronos history --redact` and `chronos export --redact` scrub sensitive keys (secrets, tokens, passwords) from event payloads before CLI output or bundle export.
- **Panic-to-500 JSON Sanitization (`ObservabilityMiddleware`, ADR-011):** Recovers HTTP handler panics, logs stack traces server-side via structured `slog`, and returns a generic `{"error":"internal server error"}` HTTP 500 JSON response without leaking stack traces to HTTP clients.
- **Read-Only SQLite Isolation (`sqlite.OpenReadOnly`):** Offline CLI inspection, diagnostics, and DLQ listing (`history`, `replay`, `diff`, `export`, `doctor`, `dlq list`) open the SQLite WAL journal strictly in read-only mode to prevent accidental state mutation.
