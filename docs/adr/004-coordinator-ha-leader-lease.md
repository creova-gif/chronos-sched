# ADR-004: Active-Standby Coordinator HA Leader Lease
- **Status:** Accepted
- **Owner:** Justin Mafie

## Context
Chronos-Sched runs a durable workflow coordinator that appends state transitions to the SQLite WAL journal (ADR-001). Running a single coordinator process creates a single point of failure during host restarts, crashes, or rolling deployments. Running two coordinators simultaneously without coordination would cause split-brain dispatching, duplicate task execution, and sequence conflicts in the event journal.

We need an active-standby High Availability (HA) leader election mechanism that:
1. Guarantees mutual exclusion (at most one active coordinator at any time).
2. Issues a strictly monotonic fencing `Epoch` per leadership term so stale leaders cannot corrupt state after a pause or partition.
3. Requires zero external infrastructure beyond the existing SQLite WAL database already used by Chronos-Sched.
4. Supports fast failover on graceful shutdown and bounded TTL failover on crash.

## Decision
We implement an active-standby leader election package in `internal/ha`:

1. **Interface (`LeaderElector`) & Lease (`LeaderLease`):**
   - `LeaderLease` carries `LockName`, `HolderID`, `Epoch int64`, and `ExpiresAtNs int64`.
   - `LeaderElector` exposes `TryAcquire`, `Renew`, `Release`, and `CurrentLeader`, with sentinel errors `ErrLeaseLost` and `ErrNoLeader`.

2. **SQLite WAL Backend (`SQLiteElector`):**
   - Follows ADR-001 conventions (`modernc.org/sqlite`, `_pragma=journal_mode(WAL)`, `_pragma=busy_timeout(2000)`, `_pragma=synchronous(NORMAL)`, `_txlock=immediate`, `SetMaxOpenConns(1)`).
   - Stores active leases in `coordinator_leader (lock_name TEXT PRIMARY KEY, holder_id TEXT NOT NULL, epoch INTEGER NOT NULL, expires_at_ns INTEGER NOT NULL)`.
   - Tracks the highest issued epoch in `coordinator_leader_epochs (lock_name TEXT PRIMARY KEY, last_epoch INTEGER NOT NULL)` so that `Release` can delete the active row from `coordinator_leader` while still guaranteeing strictly monotonic epochs across graceful handovers.
   - `TryAcquire` runs in an immediate transaction: inserts a new row if missing, or atomically takes over with `epoch = epoch + 1` if `expires_at_ns <= now` or already held by `candidateID`.
   - `Renew` updates `expires_at_ns` only when `lock_name`, `holder_id`, and `epoch` match AND `expires_at_ns > now`; 0 affected rows returns `ErrLeaseLost`.
   - `Release` deletes the row only when `lock_name`, `holder_id`, and `epoch` match so a stale leader can never evict a newer leader.

3. **Coordinator Loop (`RunLeaderLoop`):**
   - Standby instances poll `TryAcquire` at `renewInterval`.
   - Upon acquisition, a child `leaderCtx` is passed to `onElected(leaderCtx, lease)` in a goroutine while the loop renews at `renewInterval`.
   - Any `Renew` failure immediately cancels `leaderCtx`, waits for `onElected` to drain, invokes `onLost()`, and returns to standby.
   - On parent `ctx.Done()`, `leaderCtx` is cancelled, `onElected` is drained, and `Release` is invoked with a 1-second timeout for immediate standby takeover.

## Alternatives Considered
- **Embedded Raft / etcd / Consul:** Provides multi-node consensus across separate disks, but introduces heavy operational complexity, cluster quorum management, and external dependencies that violate Chronos-Sched's single-binary zero-dependency design for Milestone 2. The `LeaderElector` interface cleanly allows a future etcd/Postgres backend if multi-host HA is needed later.
- **OS File Locks (`flock`):** Fails to provide monotonic fencing epochs, behaves inconsistently across networked filesystems, and does not carry lease holder metadata or TTL expiry visibility.
- **Soft-delete / `expires_at_ns = 0` on `Release`:** Avoids a second table, but leaves stale holder rows in `coordinator_leader`. Keeping `coordinator_leader` strictly for active leases while persisting `last_epoch` in `coordinator_leader_epochs` keeps both table semantics and monotonic epoch guarantees clean.

## Consequences & Failure Modes
- **Single-Host / Shared-Storage Scope:** SQLite WAL requires coordinators to share a local POSIX filesystem with reliable locking (not NFS). Multi-node HA across hosts will require a networked `LeaderElector` implementation (e.g., PostgreSQL or etcd).
- **Clock Skew & GC Pauses:** Because lease expiry relies on wall-clock timestamps (`UnixNano`), a long process pause exceeding `ttl` will allow a standby to take over. `Renew` will fail with `ErrLeaseLost` upon resume, cancelling `leaderCtx`. Downstream journal writes should propagate the lease `Epoch` as a fencing token to reject late writes during the window between pause and cancellation.
- **SQLite Lock Contention:** Using `_txlock=immediate` with `busy_timeout(2000)` serializes competing `TryAcquire`/`Renew` transactions cleanly without `SQLITE_BUSY` errors under normal contention.
