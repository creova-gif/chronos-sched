# ADR-006: Deterministic Recurring Cron Schedules
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Chronos needs first-class recurring workflow schedules (standard 5-field cron expressions and fixed-interval `@every` shorthands) that survive coordinator restarts without double-firing scheduled slots or introducing third-party cron dependencies.

## Decision
1. **Zero-Dependency UTC Cron Engine (`internal/schedule/cron.go`):** Implement a pure-stdlib parser and next-occurrence calculator (`ParseSpec`, `Spec.Next`) supporting 5-field cron (`*`, `*/N`, single values, comma lists, `a-b` ranges, `a-b/N` stepped ranges, and Sunday as `0` or `7`) plus `@hourly`, `@daily`, `@midnight`, and `@every <duration>` (minimum `1s`). All evaluations operate strictly in UTC to avoid DST ambiguity.
2. **SQLite WAL Store with Atomic CAS (`internal/schedule/store.go`):** Persist schedules in a `schedules` table using the ADR-001 SQLite WAL configuration (`journal_mode(WAL)`, `synchronous(NORMAL)`, `busy_timeout(2000)`, `_txlock=immediate`, `MaxOpenConns(1)`). Slot claims use `Advance(ctx, id, expectedNextRunNs, newLastRunNs, newNextRunNs)`, an atomic compare-and-swap `UPDATE ... WHERE id = ? AND enabled = 1 AND next_run_at_ns = ?` so concurrent ticks or coordinator restarts can never claim the same slot twice.
3. **Deterministic Idempotent Run IDs & Misfire Policies (`internal/schedule/runner.go`):** Each claimed slot derives a deterministic run ID (`sched_<id>_<unix_sec>`) before invoking `submitFn`. Two misfire policies are supported:
   - `skip` (`MisfireSkip`): fires the due slot once and advances `next_run_at_ns` to `spec.Next(now)`, skipping intermediate missed intervals.
   - `catch_up` (`MisfireCatchUp`): advances one interval at a time (`spec.Next(slotTime)`), firing each missed slot in chronological order (bounded per tick by `maxCatchUpSlotsPerTick = 1000`).

## Alternatives Considered
- **Third-party cron library (`robfig/cron`):** Adds an external dependency and embeds its own in-memory timer goroutines, making injectable-clock testing and durable SQLite cursor advancement harder to control.
- **In-memory schedule map without CAS:** Vulnerable to double-firing across restarts or overlapping ticks.

## Consequences & Failure Modes
- **At-most-once slot claim vs. submission failure:** Because `Advance` commits before `submitFn`, a crash or transient error inside `submitFn` after `Advance` skips that slot rather than double-firing it; operators can inspect `last_run_at_ns` or re-trigger manually using the deterministic run ID.
- **UTC-only schedules:** Timezone-local cron expressions (e.g., `America/New_York`) are intentionally out of scope for now to guarantee deterministic replay and cross-host consistency.
- **Bounded catch-up:** `MisfireCatchUp` caps iterations at 1,000 slots per `Tick` to prevent a single tick from blocking indefinitely after prolonged downtime; remaining slots drain on subsequent ticks.
