package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
	"github.com/creova-gif/chronos-sched/pkg/chronos"
)

// TestHelperWorkerProcess is invoked as a real child OS process by E2E SIGKILL tests.
func TestHelperWorkerProcess(t *testing.T) {
	if os.Getenv("CHRONOS_TEST_HELPER_WORKER") != "1" {
		return
	}
	coordURL := os.Getenv("CHRONOS_TEST_COORD_URL")
	workerID := os.Getenv("CHRONOS_TEST_WORKER_ID")
	w := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          workerID,
		CoordinatorURL:    coordURL,
		Concurrency:       2,
		HeartbeatInterval: 500 * time.Millisecond,
		PollInterval:      25 * time.Millisecond,
	})
	_ = w.Run(context.Background())
	os.Exit(0)
}

func TestE2EWorkerSIGKILLRecoveryUnder5Seconds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "sigkill.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,   // 3s
		ReaperTick: engine.DefaultReaperTick, // 250ms
	})
	go coord.StartReaper(ctx)

	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	// Spawn victim worker as a REAL child OS process so SIGKILL is genuine.
	victimCmd := exec.Command(os.Args[0], "-test.run=^TestHelperWorkerProcess$")
	victimCmd.Env = append(os.Environ(),
		"CHRONOS_TEST_HELPER_WORKER=1",
		"CHRONOS_TEST_COORD_URL="+srv.URL,
		"CHRONOS_TEST_WORKER_ID=worker-victim",
	)
	if err := victimCmd.Start(); err != nil {
		t.Fatalf("start victim child worker: %v", err)
	}
	defer func() {
		if victimCmd.Process != nil {
			_ = victimCmd.Process.Kill()
			_, _ = victimCmd.Process.Wait()
		}
	}()

	wfYAML := `
name: sigkill-recovery
tasks:
  - id: slow_step
    handler: sim
    max_retries: 2
    checkpoint_name: after_slow
    params:
      duration_ms: 2500
  - id: finish_step
    handler: sim
    depends_on: [slow_step]
    params:
      duration_ms: 20
`
	sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_sigkill",
		WorkflowYAML: wfYAML,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	// Wait until worker-victim starts executing slow_step.
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := coord.Snapshot(ctx, sub.RunID)
		if err == nil && snap.State.Tasks["slow_step"].Status == engine.TaskStatusRunning && snap.State.Tasks["slow_step"].WorkerID == "worker-victim" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for worker-victim to start slow_step")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Kill victim OS process abruptly with SIGKILL and start healthy survivor worker.
	killTime := time.Now()
	if err := victimCmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL victim: %v", err)
	}
	_, _ = victimCmd.Process.Wait()

	survivor := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          "worker-survivor",
		CoordinatorURL:    srv.URL,
		Concurrency:       2,
		HeartbeatInterval: engine.DefaultHeartbeatInterval,
		PollInterval:      25 * time.Millisecond,
	})
	survivorCtx, survivorCancel := context.WithCancel(ctx)
	defer survivorCancel()
	go func() { _ = survivor.Run(survivorCtx) }()

	// Wait for lease expiry detection (<5s) and full workflow completion.
	var detectedAt time.Time
	for {
		snap, err := coord.Snapshot(ctx, sub.RunID)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if detectedAt.IsZero() && snap.State.Tasks["slow_step"].AmbiguousCount > 0 {
			detectedAt = time.Now()
		}
		if snap.State.Status == engine.RunStatusFailed {
			t.Fatalf("recovered run unexpectedly failed: %s", snap.State.FailureReason)
		}
		if snap.State.Status == engine.RunStatusCompleted {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for run completion after SIGKILL; state=%+v", snap.State)
		}
		time.Sleep(50 * time.Millisecond)
	}

	detectLatency := detectedAt.Sub(killTime)
	if detectedAt.IsZero() || detectLatency >= 5*time.Second {
		t.Fatalf("expected SIGKILL detection in <5s, got %v (detectedAt=%v)", detectLatency, detectedAt)
	}

	// Verify deterministic offline replay.
	evs, err := st.LoadRunEvents(ctx, sub.RunID)
	if err != nil {
		t.Fatalf("LoadRunEvents: %v", err)
	}
	dag, _ := engine.ParseDAG([]byte(wfYAML))
	rep, err := engine.VerifyReplay(dag, evs)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if rep.FinalStatus != engine.RunStatusCompleted || rep.AmbiguousEvents != 1 || rep.CompletedTasks != 2 {
		t.Fatalf("unexpected replay report after SIGKILL: %+v", rep)
	}
}

func TestE2ECoordinatorCrashRestartRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "coord-restart.db")
	st1, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open st1: %v", err)
	}

	coord1 := runtime.NewCoordinator(st1, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	srv1 := httptest.NewServer(coord1.Handler())

	wfYAML := `
name: coord-restart
tasks:
  - id: step1
    handler: sim
    params: { duration_ms: 20 }
  - id: step2
    handler: sim
    depends_on: [step1]
    max_retries: 1
    params: { duration_ms: 40 }
`
	if _, err := coord1.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_restart",
		WorkflowYAML: wfYAML,
	}); err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	// Run worker until step1 completes, then abruptly stop Coordinator 1.
	w1Ctx, w1Cancel := context.WithCancel(ctx)
	w1 := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:       "w-1",
		CoordinatorURL: srv1.URL,
		Concurrency:    1,
		PollInterval:   20 * time.Millisecond,
	})
	go func() { _ = w1.Run(w1Ctx) }()

	for {
		snap, err := coord1.Snapshot(ctx, "run_restart")
		if err == nil && snap.State.Tasks["step1"].Status == engine.TaskStatusCompleted {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	w1Cancel()
	srv1.Close()
	_ = st1.Close()

	// Boot Coordinator 2 against the SAME SQLite WAL journal and call Recover().
	st2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open st2: %v", err)
	}
	defer func() { _ = st2.Close() }()

	coord2 := runtime.NewCoordinator(st2, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	if err := coord2.Recover(ctx); err != nil {
		t.Fatalf("coord2.Recover: %v", err)
	}
	go coord2.StartReaper(ctx)

	srv2 := httptest.NewServer(coord2.Handler())
	defer srv2.Close()

	w2Ctx, w2Cancel := context.WithCancel(ctx)
	defer w2Cancel()
	w2 := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:       "w-2",
		CoordinatorURL: srv2.URL,
		Concurrency:    2,
		PollInterval:   20 * time.Millisecond,
	})
	go func() { _ = w2.Run(w2Ctx) }()

	for {
		snap, err := coord2.Snapshot(ctx, "run_restart")
		if err != nil {
			t.Fatalf("coord2.Snapshot: %v", err)
		}
		if snap.State.Status == engine.RunStatusFailed {
			t.Fatalf("recovered run unexpectedly failed: %s", snap.State.FailureReason)
		}
		if snap.State.Status == engine.RunStatusCompleted {
			if snap.State.Tasks["step1"].Attempt != 1 {
				t.Fatalf("step1 should NOT have re-run after coordinator restart; attempts=%d", snap.State.Tasks["step1"].Attempt)
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for recovered run completion")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestE2ESDKCustomHandlerForkDiffAndStaleHTTP409(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "fork-diff.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{})
	go coord.StartReaper(ctx)
	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	// Use public pkg/chronos SDK to register a custom Go handler.
	sdkWorker := chronos.NewWorker(chronos.WorkerConfig{
		WorkerID:       "sdk-w1",
		CoordinatorURL: srv.URL,
		Concurrency:    2,
	})
	sdkWorker.RegisterHandler("double", func(_ context.Context, input map[string]any) (map[string]any, error) {
		val := 1
		if n, ok := input["n"].(float64); ok {
			val = int(n)
		} else if n, ok := input["n"].(int); ok {
			val = n
		}
		return map[string]any{"doubled": val * 2}, nil
	})
	wCtx, wCancel := context.WithCancel(ctx)
	defer wCancel()
	go func() { _ = sdkWorker.Run(wCtx) }()

	wfYAML := `
name: sdk-fork-demo
tasks:
  - id: seed
    handler: sim
    checkpoint_name: seeded
    params: { duration_ms: 10 }
  - id: compute
    handler: double
    depends_on: [seed]
    params: { n: 5 }
`
	if _, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_orig",
		WorkflowYAML: wfYAML,
	}); err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	waitDone := func(runID string) *engine.RunState {
		for {
			snap, err := coord.Snapshot(ctx, runID)
			if err == nil && snap.State.Status == engine.RunStatusCompleted {
				return snap.State
			}
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for %s", runID)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	origState := waitDone("run_orig")
	if fmt.Sprint(origState.Tasks["compute"].Output["doubled"]) != "10" {
		t.Fatalf("expected doubled=10 on run_orig, got %+v", origState.Tasks["compute"].Output)
	}

	// Fork at checkpoint "seeded" with override compute.n = 21.
	forkResp, err := coord.ForkRun(ctx, "run_orig", runtime.ForkRunRequest{
		NewRunID:   "run_forked",
		Checkpoint: "seeded",
		Overrides:  map[string]map[string]any{"compute": {"n": 21}},
	})
	if err != nil {
		t.Fatalf("ForkRun: %v", err)
	}
	forkedState := waitDone(forkResp.RunID)
	if fmt.Sprint(forkedState.Tasks["compute"].Output["doubled"]) != "42" {
		t.Fatalf("expected doubled=42 on run_forked, got %+v", forkedState.Tasks["compute"].Output)
	}

	// Diff run_orig vs run_forked.
	evsOrig, _ := st.LoadRunEvents(ctx, "run_orig")
	evsFork, _ := st.LoadRunEvents(ctx, "run_forked")
	dag, _ := engine.ParseDAG([]byte(wfYAML))
	diff, err := engine.DiffRuns(dag, evsOrig, evsFork)
	if err != nil {
		t.Fatalf("DiffRuns: %v", err)
	}
	if len(diff.TaskDeltas) != 1 || diff.TaskDeltas[0].TaskID != "compute" || !diff.TaskDeltas[0].OutputChanged {
		t.Fatalf("expected single compute output delta, got %+v", diff.TaskDeltas)
	}

	// Verify HTTP 409 on stale fencing token against an active run.
	if _, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_stale_check",
		WorkflowYAML: "name: s\ntasks:\n  - id: t1\n    handler: sim\n    params: { duration_ms: 500 }\n",
	}); err != nil {
		t.Fatalf("submit stale check: %v", err)
	}
	body, _ := json.Marshal(runtime.TaskCompleteRequest{
		RunID:        "run_stale_check",
		TaskID:       "t1",
		WorkerID:     "ghost-worker",
		FencingToken: 999,
	})
	resp, err := http.Post(srv.URL+"/v1/tasks/complete", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST stale complete: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected HTTP 409 Conflict for stale fencing token, got %d", resp.StatusCode)
	}
}
