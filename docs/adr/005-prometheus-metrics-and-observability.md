# ADR-005: Prometheus Metrics and Observability
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
`chronos-sched` needs low-overhead, scrapeable operational telemetry for runs, task leasing/completion/failure, lease expirations, ambiguous outcomes, stale fencing tokens, timeline forks/merges, and scheduling latency. Adding the official Prometheus client library (`prometheus/client_golang`) pulls in a large transitive dependency tree (protobuf, model, procfs, and additional `golang.org/x` packages) that increases binary size, supply-chain surface, and build times for a compact scheduler whose metric set is small and well-defined.

## Decision
Implement `internal/metrics` using **zero external dependencies** (Go standard library only: `sync`, `sync/atomic`, `net/http`, `fmt`, `strings`, `time`):
- Expose a `Recorder` struct whose zero value is ready for use.
- Track all nine scheduler counters with lock-free `atomic.Uint64` increments:
  - `chronos_runs_submitted_total`
  - `chronos_tasks_leased_total`
  - `chronos_tasks_completed_total`
  - `chronos_tasks_failed_total`
  - `chronos_leases_expired_total`
  - `chronos_ambiguous_outcomes_total`
  - `chronos_stale_fencing_tokens_total`
  - `chronos_timelines_forked_total`
  - `chronos_timelines_merged_total`
- Track scheduling decision latency in `chronos_schedule_latency_ms` using a fixed-bucket histogram (`0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, +Inf` ms) guarded by a `sync.RWMutex` so that `ObserveScheduleLatency` and `RenderPrometheus` maintain consistent, monotonically non-decreasing cumulative bucket snapshots (`_bucket`, `_sum`, `_count`).
- Emit Prometheus text exposition format `0.0.4` via `RenderPrometheus() string` and serve it over HTTP via `Handler() http.Handler` with `Content-Type: text/plain; version=0.0.4; charset=utf-8` (supporting `GET` and `HEAD`).

## Alternatives Considered
1. **`prometheus/client_golang`**: Provides native histograms, Go runtime/process collectors, and registry abstractions, but introduces dozens of transitive dependencies and reflection/registry overhead that are unnecessary for this bounded metric set.
2. **OpenTelemetry Metrics SDK**: Adds even heavier dependencies and exporter complexity when Prometheus pull scraping is the primary target.
3. **Structured JSON metrics endpoint**: Requires custom adapters or scrapers instead of working out-of-the-box with Prometheus, VictoriaMetrics, and Grafana Agent.

## Consequences & Failure Modes
- **Positive:** Zero new entries in `go.mod`/`go.sum`, deterministic exposition format, lock-free hot-path counter increments, and race-verified consistent histogram snapshots.
- **Trade-offs:** Go runtime/process metrics (`go_*`, `process_*`) are not emitted automatically, and dynamic label sets are not supported; adding high-cardinality labels later would require explicit validation or migrating to a full client library.
- **Failure modes:**
  - *Negative clock deltas:* Clamped to `0` ms before bucket assignment to prevent corrupting `_sum` or violating bucket semantics.
  - *Concurrent scrape vs. observation:* Reading histogram buckets without a snapshot lock could expose non-monotonic cumulative counts or `+Inf != _count`; mitigated by guarding the fixed bucket array, sum, and count under `sync.RWMutex`.
  - *Nil `Recorder`:* Methods guard against a nil receiver so callers that disable metrics do not panic.
