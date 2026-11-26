# Contributing to ChronosSched

Thank you for your interest in contributing to ChronosSched! Please review these guidelines to keep contributions consistent with our engineering standards.

## Prerequisites

- **Go 1.26+** (see `go.mod` for the exact toolchain version)
- **Git**
- **Docker** (optional, for containerized builds or local test environments)

## Local Setup & Testing

1. Clone the repository and enter the project directory:
   ```sh
   git clone https://github.com/creova-gif/chronos-sched.git
   cd chronos-sched
   ```

2. Download module dependencies:
   ```sh
   go mod download
   ```

3. Format, vet, and run the full test suite with the race detector before submitting changes:
   ```sh
   gofmt -l .
   go vet ./...
   go test -race -count=1 ./...
   ```

## Engineering Standards

- **Explicit Error Handling:** Always check returned errors explicitly. Never discard errors silently.
- **Deterministic Unit Tests:** Every pull request must include deterministic unit tests covering new behavior or bug fixes.
- **Benchmarks:** Never claim benchmark numbers without actually running them and including reproducible methodology.
- **Formatting & Static Analysis:** All Go code must be formatted with `gofmt` and pass `go vet ./...` and `go test -race -count=1 ./...` cleanly.

## Architecture Decision Records (ADRs)

Record every non-trivial design choice in `docs/adr/` using [`docs/adr/000-template.md`](docs/adr/000-template.md):

1. Copy `docs/adr/000-template.md` to `docs/adr/<NNN>-<short-title>.md` (incrementing the number sequentially).
2. Fill in **Status**, **Owner**, **Context**, **Decision**, **Alternatives Considered**, and **Consequences & Failure Modes**.
3. Reference the ADR in your pull request description.

## Branch & Pull Request Workflow

1. Create a focused feature or bugfix branch from `main` (e.g., `feat/cron-parser` or `fix/sqlite-lock`).
2. Keep commits clean, focused, and logically scoped.
3. Ensure `gofmt`, `go vet ./...`, and `go test -race -count=1 ./...` pass locally.
4. Open a pull request using the repository's PR template and complete the verification checklist.

## Security Guidelines

- **Never commit secrets or sensitive data:** Do not commit `.env` files, API keys, tokens, credentials, or PII/PHI fixtures.
- **Report vulnerabilities responsibly:** If you discover a security issue, please report it privately to the maintainers rather than opening a public issue.
