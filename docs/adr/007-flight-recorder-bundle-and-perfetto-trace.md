# ADR-007: Portable Flight-Recorder Bundles and Chrome Trace / Perfetto Export
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
ChronosSched records every scheduling decision, lease, fencing token, checkpoint, fork, and merge as an immutable event journal in SQLite. Operators debugging production incidents or sharing reproducible workflow failures need two portable, zero-dependency artifacts:
1. A **self-contained flight-recorder bundle** that can be exported from one coordinator database, verified offline for tamper resistance and replay invariants, and imported into another database without requiring the original parent run lineage.
2. A **visual execution timeline** that renders per-worker task spans, retries, lease expirations, stale fencing rejections, checkpoints, and timeline forks/merges in industry-standard trace viewers (`chrome://tracing` and `https://ui.perfetto.dev`).

## Decision
Add package `internal/export` using only the Go standard library plus `internal/engine` and `internal/store`:
1. **Flight-Recorder Bundle (`bundle.go`)**:
   - Define `FlightBundle` (`format_version: "chronos.flight-bundle/v1"`, `exported_at_ns`, `run`, `events`, `checksum_sha256`).
   - `ExportBundle` reads `GetRun` and the full lineage-resolved `LoadRunEvents` (`1..N`) and computes a deterministic SHA-256 digest over the canonical compact JSON of `(Run, Events)`.
   - `VerifyBundle` enforces format version, constant-time SHA-256 verification, embedded `WorkflowYAML` DAG parsing (and `WorkflowHash` agreement), and full `engine.VerifyReplay` validation.
   - `ImportBundle` verifies the bundle, clears `ParentRunID` and `ForkFromSeq` (since the bundle already carries the flattened `1..N` stream), rewrites all `Event.RunID` values to the target run ID (honoring `overrideRunID` when provided) so foreign-key invariants hold in a fresh database, and persists atomically via `JournalStore.CreateRun`.
2. **Chrome Trace / Perfetto Timeline (`trace.go`)**:
   - `ToChromeTrace` emits Chrome Trace Event Format JSON (`{"traceEvents": [...]}`) under `pid: 1`.
   - Each distinct `worker_id` is sorted lexicographically and assigned a deterministic `tid` (`1..W`), with `tid: 0` reserved for `coordinator`; `M` (`thread_name`) metadata events label every lane.
   - `TASK_LEASED` / `TASK_STARTED` events are paired with `TASK_COMPLETED`, `TASK_FAILED`, or `LEASE_EXPIRED` into `X` (complete) spans with relative microsecond `ts` and non-negative `dur`, carrying `attempt`, `fencing_token`, and `status` in `args`. Any unclosed in-flight spans are closed at the stream's maximum timestamp with `status: "IN_FLIGHT"`.
   - `CHECKPOINT_SAVED`, `TASK_OUTCOME_AMBIGUOUS`, `STALE_FENCING_TOKEN`, `TIMELINE_FORKED`, and `TIMELINE_MERGED` emit `i` (instant) marker events (`s: "t"` when worker-scoped, `s: "g"` when global).

## Alternatives Considered
1. **Copying raw SQLite database files:** Exposes unrelated runs, requires copying WAL/SHM sidecar files safely, and cannot isolate a single forked run without its ancestor rows.
2. **Protobuf Perfetto native `.pftrace` format:** Adds protobuf codegen and dependency overhead; Chrome Trace JSON is natively supported by both Perfetto UI and `chrome://tracing` and is trivially inspectable with `jq`.
3. **Preserving `ParentRunID`/`ForkFromSeq` on import:** Fails foreign-key checks when importing a forked run into an empty database that lacks the parent run. Flattening the resolved `1..N` stream makes every bundle standalone while preserving the `TIMELINE_FORKED` event metadata inside the journal.

## Consequences & Failure Modes
- **Positive:** Zero new third-party dependencies, cryptographic tamper detection on bundles, full replay-invariant enforcement before import, and immediate visual debugging in Perfetto.
- **Trade-offs:** Flattening forked runs on import means the imported run no longer shares prefix rows with its parent in the destination store (slightly higher storage for imported forks, acceptable for diagnostic bundles).
- **Failure modes:**
  - *Clock skew in event timestamps:* `ToChromeTrace` anchors `ts` to the minimum timestamp across all events and clamps negative deltas to `0` µs so Perfetto never receives negative durations.
  - *Pretty-printed JSON round-trips:* `json.Marshal` compacts `json.RawMessage` payloads inside `computeChecksum`, ensuring checksum verification succeeds regardless of whether a bundle file was formatted with `Marshal` or `MarshalIndent`.
  - *Tampered or corrupted bundles:* Rejected by `VerifyBundle` and `ImportBundle` before `CreateRun` is ever invoked.
