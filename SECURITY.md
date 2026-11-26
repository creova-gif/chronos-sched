# Security Policy

## Supported Versions

| Version | Supported |
| :--- | :--- |
| `0.2.x` | :white_check_mark: Supported |
| `< 0.2.0` | :x: Unsupported |

Only the `0.2.x` release line receives security updates and vulnerability patches.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security vulnerability in ChronosSched, please report it privately:

1. **GitHub Security Advisories (Preferred):** Open a private vulnerability report via the repository's **Security > Advisories > New draft security advisory** flow on GitHub (`https://github.com/creova-gif/chronos-sched/security/advisories`).
2. **Maintainer Contact:** If you cannot use GitHub Security Advisories, contact the repository maintainer (`@creova-gif`) privately via GitHub.

Please include:
- A clear description of the vulnerability and affected component(s).
- Steps to reproduce, proof-of-concept, or affected version/commit.
- Potential impact and any suggested remediation if known.

### Response & Triage SLA

- **Initial Triage:** Within **48 hours** of report submission.
- **Status Updates:** Regular updates during investigation and remediation.
- **Disclosure:** Coordinated disclosure once a fix is released on the supported `0.2.x` branch.

## Security Controls Overview

ChronosSched implements defense-in-depth controls across the Coordinator HTTP API, worker communication, and CLI tooling (see also [Threat Model](docs/THREAT_MODEL.md)):

- **Bearer Authentication (`AuthMiddleware`):** Constant-time (`crypto/subtle.ConstantTimeCompare`) `Authorization: Bearer <token>` validation across Coordinator HTTP routes when `--auth-token` or `CHRONOS_AUTH_TOKEN` is configured (`/healthz` exempt for health probes).
- **HMAC-SHA256 Webhooks (`SignPayload`):** Outbound terminal run webhook payloads are signed with `X-Chronos-Signature-256: sha256=<hex>` using `--webhook-secret` so receivers can verify payload authenticity and integrity.
- **Secrets Redaction (`--redact`):** `chronos history --redact` and `chronos export --redact` scrub sensitive keys (tokens, secrets, passwords) from event payloads before CLI output or flight-recorder bundle export.
- **Tenant Isolation (`TenantMiddleware` + `authorizeRunTenant`):** `TenantMiddleware` extracts and validates the `X-Chronos-Tenant` header (or `CHRONOS_TENANT` in the CLI/SDK) into the request context, and `authorizeRunTenant` enforces per-run tenant ownership across run, approval, artifact, and DLQ endpoints.
- **Per-Tenant Rate Limiting (`RateLimitMiddleware`):** Enforces per-tenant token-bucket rate limiting on Coordinator HTTP routes to protect against noisy-neighbor exhaustion.
- **Security Headers (`SecurityHeadersMiddleware`):** Attaches baseline HTTP security headers to all Coordinator HTTP responses.
- **Bounded Request Bodies (`MaxBytesReader`):** Request JSON decoding enforces `http.MaxBytesReader` limits (`decodeJSONBody`) to prevent oversized payload memory exhaustion.

## Out of Scope

The following are generally considered out of scope for vulnerability reports:

- Vulnerabilities affecting unsupported versions (`< 0.2.0`).
- Attacks requiring local root/administrator access or direct filesystem write access to the host's SQLite WAL database (`.chronos/chronos.db`).
- Denial-of-service or unauthorized access reports against local development instances intentionally started without `--auth-token` or with rate limiting disabled.
- Vulnerabilities in third-party downstream webhook receivers or external user-configured task commands.
- Social engineering, phishing, or automated scanner noise without a concrete, reproducible security impact.
