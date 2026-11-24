package sqlite_test

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func BenchmarkSQLiteWALDispatchAndComplete(b *testing.B) {
	ctx := context.Background()
	dbPath := filepath.Join(b.TempDir(), "bench-wal.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		b.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	wfYAML := `
name: bench-wal-single-task
tasks:
  - id: task_1
    handler: sim
    checkpoint_name: cp_task_1
    params:
      duration_ms: 0
`
	dag, err := engine.ParseDAG([]byte(wfYAML))
	if err != nil {
		b.Fatalf("ParseDAG: %v", err)
	}

	baseTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	// Pre-create b.N runs so each timed iteration measures pure end-to-end
	// single-task lease + WAL commit + complete + checkpoint WAL commit latency.
	states := make([]*engine.RunState, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("run_bench_%06d", i)
		ts := baseTime.Add(time.Duration(i) * time.Millisecond)
		state := engine.NewRunState(dag, runID)
		startEvs, _, err := engine.Step(dag, state, engine.CmdStartRun{RunID: runID}, ts)
		if err != nil {
			b.Fatalf("CmdStartRun: %v", err)
		}
		for _, ev := range startEvs {
			if err := engine.Apply(state, ev); err != nil {
				b.Fatalf("Apply start: %v", err)
			}
		}
		rec := store.RunRecord{
			RunID:        runID,
			WorkflowName: dag.Spec.Name,
			WorkflowHash: dag.WorkflowHash,
			WorkflowYAML: wfYAML,
			Status:       state.Status,
			CreatedAtNs:  ts.UnixNano(),
		}
		if err := st.CreateRun(ctx, rec, startEvs); err != nil {
			b.Fatalf("CreateRun: %v", err)
		}
		states[i] = state
	}

	latencies := make([]time.Duration, 0, b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		state := states[i]
		ts := baseTime.Add(time.Duration(i+1) * time.Second)
		iterStart := time.Now()

		// 1. Lease task + SQLite WAL commit.
		prevSeq := state.LastSeq
		leaseEvs, _, err := engine.Step(dag, state, engine.CmdLeaseNextTask{
			WorkerID: "bench-worker-1",
			LeaseTTL: engine.DefaultLeaseTTL,
		}, ts)
		if err != nil || len(leaseEvs) == 0 {
			b.Fatalf("CmdLeaseNextTask err=%v len=%d", err, len(leaseEvs))
		}
		if err := st.Append(ctx, state.RunID, prevSeq, leaseEvs, engine.RunStatusRunning); err != nil {
			b.Fatalf("Append lease: %v", err)
		}
		for _, ev := range leaseEvs {
			if err := engine.Apply(state, ev); err != nil {
				b.Fatalf("Apply lease: %v", err)
			}
		}

		// 2. Complete task + checkpoint emission + SQLite WAL commit.
		prevSeq = state.LastSeq
		doneEvs, _, err := engine.Step(dag, state, engine.CmdCompleteTask{
			TaskID:       leaseEvs[0].TaskID,
			WorkerID:     "bench-worker-1",
			FencingToken: leaseEvs[0].FencingToken,
			Output:       map[string]any{"ok": true, "iter": i},
		}, ts.Add(100*time.Microsecond))
		if err != nil {
			b.Fatalf("CmdCompleteTask: %v", err)
		}
		hasCheckpoint := false
		for _, ev := range doneEvs {
			if ev.Type == engine.EventCheckpointSaved {
				hasCheckpoint = true
			}
		}
		if !hasCheckpoint {
			b.Fatalf("expected CHECKPOINT_SAVED event in completion batch, got %+v", doneEvs)
		}
		if err := st.Append(ctx, state.RunID, prevSeq, doneEvs, engine.RunStatusCompleted); err != nil {
			b.Fatalf("Append complete+checkpoint: %v", err)
		}
		for _, ev := range doneEvs {
			if err := engine.Apply(state, ev); err != nil {
				b.Fatalf("Apply complete: %v", err)
			}
		}

		latencies = append(latencies, time.Since(iterStart))
	}

	b.StopTimer()

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		b.ReportMetric(percentileMs(latencies, 0.50), "p50_ms")
		b.ReportMetric(percentileMs(latencies, 0.95), "p95_ms")
		b.ReportMetric(percentileMs(latencies, 0.99), "p99_ms")
	}
}

func percentileMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / float64(time.Millisecond)
}
