package runtime_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
	"github.com/creova-gif/chronos-sched/pkg/chronos"
)

func openTestCoordinator(t *testing.T) (*runtime.Coordinator, *httptest.Server) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "resources.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	srv := httptest.NewServer(coord.Handler())
	t.Cleanup(srv.Close)
	return coord, srv
}

func waitForRunCompleted(t *testing.T, ctx context.Context, coord *runtime.Coordinator, runID string, timeout time.Duration) runtime.RunSnapshotResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		snap, err := coord.Snapshot(ctx, runID)
		if err != nil {
			t.Fatalf("Snapshot(%q): %v", runID, err)
		}
		if snap.State.Status == engine.RunStatusCompleted {
			return snap
		}
		if snap.State.Status == engine.RunStatusFailed {
			t.Fatalf("run %q failed unexpectedly: %s", runID, snap.State.FailureReason)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for run %q to complete (status=%s)", runID, snap.State.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestResourceCapacityFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	coord, srv := openTestCoordinator(t)

	wfYAML := `
name: resource-filtering
tasks:
  - id: heavy_task
    handler: sim
    resources:
      cpu: 4
    params:
      duration_ms: 20
`
	sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_res_filter",
		WorkflowYAML: wfYAML,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	smallCtx, smallCancel := context.WithCancel(ctx)
	defer smallCancel()
	smallWorker := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          "worker-small",
		CoordinatorURL:    srv.URL,
		Concurrency:       2,
		Resources:         map[string]int{"cpu": 2},
		HeartbeatInterval: 200 * time.Millisecond,
		PollInterval:      15 * time.Millisecond,
	})
	smallDone := make(chan error, 1)
	go func() { smallDone <- smallWorker.Run(smallCtx) }()

	// Wait until worker-small is registered and has had several poll cycles.
	regDeadline := time.Now().Add(3 * time.Second)
	for {
		snap, err := coord.Snapshot(ctx, sub.RunID)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if len(snap.Workers) == 1 && snap.Workers[0].WorkerID == "worker-small" {
			if got := snap.Workers[0].Resources["cpu"]; got != 2 {
				t.Fatalf("worker-small reported cpu=%d, want 2", got)
			}
			break
		}
		if time.Now().After(regDeadline) {
			t.Fatalf("timed out waiting for worker-small registration")
		}
		time.Sleep(15 * time.Millisecond)
	}

	time.Sleep(150 * time.Millisecond)
	snap, err := coord.Snapshot(ctx, sub.RunID)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if st := snap.State.Tasks["heavy_task"].Status; st != engine.TaskStatusPending {
		t.Fatalf("heavy_task status = %s while only cpu:2 worker is online; want PENDING", st)
	}

	// Start a second worker via the public chronos SDK with cpu: 8.
	bigCtx, bigCancel := context.WithCancel(ctx)
	defer bigCancel()
	bigWorker := chronos.NewWorker(chronos.WorkerConfig{
		WorkerID:          "worker-big",
		CoordinatorURL:    srv.URL,
		Concurrency:       2,
		Resources:         map[string]int{"cpu": 8},
		HeartbeatInterval: 200 * time.Millisecond,
	})
	bigDone := make(chan error, 1)
	go func() { bigDone <- bigWorker.Run(bigCtx) }()

	finalSnap := waitForRunCompleted(t, ctx, coord, sub.RunID, 5*time.Second)
	if got := finalSnap.State.Tasks["heavy_task"].WorkerID; got != "worker-big" {
		t.Fatalf("heavy_task completed by %q, want worker-big", got)
	}

	smallCancel()
	bigCancel()
	<-smallDone
	<-bigDone
}

func TestResourceInFlightAccountingAndRelease(t *testing.T) {
	t.Run("two_cpu2_tasks_run_concurrently_on_cpu4_worker", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		coord, srv := openTestCoordinator(t)

		wfYAML := `
name: concurrent-cpu2
tasks:
  - id: task_a
    handler: barrier
    resources:
      cpu: 2
  - id: task_b
    handler: barrier
    resources:
      cpu: 2
`
		sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
			RunID:        "run_res_concurrent",
			WorkflowYAML: wfYAML,
		})
		if err != nil {
			t.Fatalf("SubmitWorkflow: %v", err)
		}

		var entered atomic.Int32
		bothEntered := make(chan struct{})

		wCtx, wCancel := context.WithCancel(ctx)
		defer wCancel()
		worker := runtime.NewWorker(runtime.WorkerConfig{
			WorkerID:          "worker-cpu4-concurrent",
			CoordinatorURL:    srv.URL,
			Concurrency:       2,
			Resources:         map[string]int{"cpu": 4},
			HeartbeatInterval: 200 * time.Millisecond,
			PollInterval:      15 * time.Millisecond,
		})
		worker.RegisterHandler("barrier", func(hCtx context.Context, _ map[string]any) (map[string]any, error) {
			if entered.Add(1) == 2 {
				close(bothEntered)
			}
			select {
			case <-bothEntered:
				return map[string]any{"ok": true}, nil
			case <-hCtx.Done():
				return nil, hCtx.Err()
			}
		})
		wDone := make(chan error, 1)
		go func() { wDone <- worker.Run(wCtx) }()

		select {
		case <-bothEntered:
		case <-time.After(5 * time.Second):
			t.Fatalf("expected both cpu:2 tasks to run concurrently on cpu:4 worker; entered=%d", entered.Load())
		}

		waitForRunCompleted(t, ctx, coord, sub.RunID, 5*time.Second)
		wCancel()
		<-wDone
	})

	t.Run("cpu3_tasks_serialized_and_capacity_released_on_completion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		coord, srv := openTestCoordinator(t)

		wfYAML := `
name: serialized-cpu3
tasks:
  - id: task_1
    handler: gated
    resources:
      cpu: 3
  - id: task_2
    handler: gated
    resources:
      cpu: 3
`
		sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
			RunID:        "run_res_serial",
			WorkflowYAML: wfYAML,
		})
		if err != nil {
			t.Fatalf("SubmitWorkflow: %v", err)
		}

		var inFlight atomic.Int32
		var maxInFlight atomic.Int32
		var startedCount atomic.Int32
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})

		wCtx, wCancel := context.WithCancel(ctx)
		defer wCancel()
		worker := runtime.NewWorker(runtime.WorkerConfig{
			WorkerID:          "worker-cpu4-serial",
			CoordinatorURL:    srv.URL,
			Concurrency:       2,
			Resources:         map[string]int{"cpu": 4},
			HeartbeatInterval: 200 * time.Millisecond,
			PollInterval:      15 * time.Millisecond,
		})
		worker.RegisterHandler("gated", func(hCtx context.Context, _ map[string]any) (map[string]any, error) {
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			if startedCount.Add(1) == 1 {
				close(firstStarted)
				select {
				case <-releaseFirst:
				case <-hCtx.Done():
					return nil, hCtx.Err()
				}
			}
			return map[string]any{"ok": true}, nil
		})
		wDone := make(chan error, 1)
		go func() { wDone <- worker.Run(wCtx) }()

		select {
		case <-firstStarted:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for first cpu:3 task to start")
		}

		// Hold first task across several poll intervals; second poll goroutine must NOT lease the other cpu:3 task.
		time.Sleep(150 * time.Millisecond)
		if got := inFlight.Load(); got != 1 {
			t.Fatalf("inFlight = %d while first cpu:3 task is running on cpu:4 worker; want 1", got)
		}
		if got := startedCount.Load(); got != 1 {
			t.Fatalf("startedCount = %d while first cpu:3 task is running; want 1", got)
		}

		// Release first task; its completion must free cpu:3 so the second task can be leased and complete.
		close(releaseFirst)

		finalSnap := waitForRunCompleted(t, ctx, coord, sub.RunID, 5*time.Second)
		if got := maxInFlight.Load(); got != 1 {
			t.Fatalf("maxInFlight = %d, want 1", got)
		}
		if finalSnap.State.Tasks["task_1"].Status != engine.TaskStatusCompleted ||
			finalSnap.State.Tasks["task_2"].Status != engine.TaskStatusCompleted {
			t.Fatalf("expected both tasks COMPLETED, got task_1=%s task_2=%s",
				finalSnap.State.Tasks["task_1"].Status, finalSnap.State.Tasks["task_2"].Status)
		}

		wCancel()
		<-wDone
	})
}
