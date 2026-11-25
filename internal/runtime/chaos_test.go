package runtime_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func buildLayeredChaosDAGYAML(layers, width, durationMs, maxRetries int) string {
	var b strings.Builder
	b.WriteString("name: chaos-30-task-dag\n")
	b.WriteString("tasks:\n")
	for l := 0; l < layers; l++ {
		for i := 0; i < width; i++ {
			id := fmt.Sprintf("l%d_t%d", l, i)
			fmt.Fprintf(&b, "  - id: %s\n", id)
			b.WriteString("    handler: sim\n")
			fmt.Fprintf(&b, "    max_retries: %d\n", maxRetries)
			if l > 0 {
				depA := fmt.Sprintf("l%d_t%d", l-1, i)
				depB := fmt.Sprintf("l%d_t%d", l-1, (i+1)%width)
				fmt.Fprintf(&b, "    depends_on: [%s, %s]\n", depA, depB)
			}
			fmt.Fprintf(&b, "    params:\n      duration_ms: %d\n", durationMs)
		}
	}
	return b.String()
}

func TestChaosConcurrentWorkersKillAndLiveFork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "chaos.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   600 * time.Millisecond,
		ReaperTick: 100 * time.Millisecond,
	})
	reaperCtx, reaperCancel := context.WithCancel(ctx)
	var reaperWG sync.WaitGroup
	reaperWG.Add(1)
	go func() {
		defer reaperWG.Done()
		coord.StartReaper(reaperCtx)
	}()
	defer func() {
		reaperCancel()
		reaperWG.Wait()
	}()

	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	// Start 4 concurrent workers (w-1..w-4).
	const numWorkers = 4
	workerCancels := make([]context.CancelFunc, numWorkers)
	var workerWG sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wID := fmt.Sprintf("w-%d", i+1)
		wCtx, wCancel := context.WithCancel(ctx)
		workerCancels[i] = wCancel
		w := runtime.NewWorker(runtime.WorkerConfig{
			WorkerID:          wID,
			CoordinatorURL:    srv.URL,
			Concurrency:       2,
			HeartbeatInterval: 150 * time.Millisecond,
			PollInterval:      15 * time.Millisecond,
		})
		workerWG.Add(1)
		go func(worker *runtime.Worker, runCtx context.Context) {
			defer workerWG.Done()
			_ = worker.Run(runCtx)
		}(w, wCtx)
	}
	defer func() {
		for _, c := range workerCancels {
			c()
		}
		workerWG.Wait()
	}()

	// 6 layers x 5 tasks = 30 tasks, max_retries: 2, duration_ms: 40.
	wfYAML := buildLayeredChaosDAGYAML(6, 5, 40, 2)
	dag, err := engine.ParseDAG([]byte(wfYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}
	if len(dag.TopoOrder) != 30 {
		t.Fatalf("expected 30 tasks in chaos DAG, got %d", len(dag.TopoOrder))
	}

	const (
		baseRunID = "run_chaos_base"
		forkRunID = "run_chaos_fork"
	)

	if _, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        baseRunID,
		WorkflowYAML: wfYAML,
	}); err != nil {
		t.Fatalf("SubmitWorkflow(%s): %v", baseRunID, err)
	}

	// Wait until at least 5 checkpoints are saved mid-flight.
	for {
		snap, err := coord.Snapshot(ctx, baseRunID)
		if err != nil {
			t.Fatalf("Snapshot(%s): %v", baseRunID, err)
		}
		if snap.State.Status == engine.RunStatusFailed {
			t.Fatalf("%s failed before chaos injection: %s", baseRunID, snap.State.FailureReason)
		}
		if len(snap.State.Checkpoints) >= 5 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for >=5 checkpoints on %s (got %d)", baseRunID, len(snap.State.Checkpoints))
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Chaos step 1: Abruptly cancel w-1 and w-2 (50% worker fleet loss).
	workerCancels[0]()
	workerCancels[1]()

	// Chaos step 2: Concurrently fork run_chaos_fork from checkpoint 3 while
	// run_chaos_base is still recovering from lost worker leases.
	forkResp, err := coord.ForkRun(ctx, baseRunID, runtime.ForkRunRequest{
		NewRunID:   forkRunID,
		Checkpoint: "3",
	})
	if err != nil {
		t.Fatalf("ForkRun(%s from cp 3): %v", forkRunID, err)
	}
	if forkResp.RunID != forkRunID || forkResp.CheckpointNum != 3 {
		t.Fatalf("unexpected ForkRunResponse: %+v", forkResp)
	}

	// Wait for BOTH run_chaos_base and run_chaos_fork to complete.
	waitCompleted := func(runID string) *engine.RunState {
		t.Helper()
		for {
			snap, err := coord.Snapshot(ctx, runID)
			if err != nil {
				t.Fatalf("Snapshot(%s): %v", runID, err)
			}
			if snap.State.Status == engine.RunStatusFailed {
				t.Fatalf("run %s unexpectedly failed: %s", runID, snap.State.FailureReason)
			}
			if snap.State.Status == engine.RunStatusCompleted {
				return snap.State
			}
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for %s to complete; status=%s checkpoints=%d",
					runID, snap.State.Status, len(snap.State.Checkpoints))
			}
			time.Sleep(25 * time.Millisecond)
		}
	}

	baseState := waitCompleted(baseRunID)
	forkState := waitCompleted(forkRunID)

	for _, pair := range []struct {
		id    string
		state *engine.RunState
	}{
		{baseRunID, baseState},
		{forkRunID, forkState},
	} {
		if len(pair.state.Tasks) != 30 {
			t.Fatalf("%s: expected 30 tasks in final state, got %d", pair.id, len(pair.state.Tasks))
		}
		for _, taskID := range dag.TopoOrder {
			ts := pair.state.Tasks[taskID]
			if ts == nil || ts.Status != engine.TaskStatusCompleted {
				t.Fatalf("%s: task %s not completed: %+v", pair.id, taskID, ts)
			}
		}
	}

	// Verify 100% deterministic offline replay on both runs.
	evsBase, err := st.LoadRunEvents(ctx, baseRunID)
	if err != nil {
		t.Fatalf("LoadRunEvents(%s): %v", baseRunID, err)
	}
	evsFork, err := st.LoadRunEvents(ctx, forkRunID)
	if err != nil {
		t.Fatalf("LoadRunEvents(%s): %v", forkRunID, err)
	}

	repBase, err := engine.VerifyReplay(dag, evsBase)
	if err != nil {
		t.Fatalf("VerifyReplay(%s): %v", baseRunID, err)
	}
	if repBase.FinalStatus != engine.RunStatusCompleted || repBase.CompletedTasks != 30 || repBase.TotalTasks != 30 {
		t.Fatalf("unexpected replay report for %s: %+v", baseRunID, repBase)
	}

	repFork, err := engine.VerifyReplay(dag, evsFork)
	if err != nil {
		t.Fatalf("VerifyReplay(%s): %v", forkRunID, err)
	}
	if repFork.FinalStatus != engine.RunStatusCompleted || repFork.CompletedTasks != 30 || repFork.TotalTasks != 30 {
		t.Fatalf("unexpected replay report for %s: %+v", forkRunID, repFork)
	}

	// Verify DiffRuns succeeds cleanly across the base and live-forked timelines.
	diff, err := engine.DiffRuns(dag, evsBase, evsFork)
	if err != nil {
		t.Fatalf("DiffRuns(%s, %s): %v", baseRunID, forkRunID, err)
	}
	if diff.CommonPrefixSeq == 0 {
		t.Fatalf("expected non-zero CommonPrefixSeq between %s and %s, got %+v", baseRunID, forkRunID, diff)
	}
}
