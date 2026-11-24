package runtime_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestSSEHistoricalCatchupAndLiveEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "sse-live.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	wfYAML := `
name: sse-catchup-live
tasks:
  - id: step1
    handler: fast
    checkpoint_name: after_step1
  - id: step2
    handler: fast
    depends_on: [step1]
`
	sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_sse_live",
		WorkflowYAML: wfYAML,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	// Open SSE stream BEFORE starting any worker so RUN_STARTED arrives as
	// historical catch-up and subsequent task events arrive live.
	sseCtx, sseCancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, srv.URL+"/v1/runs/"+sub.RunID+"/events", nil)
	if err != nil {
		sseCancel()
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		sseCancel()
		t.Fatalf("GET SSE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		sseCancel()
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		_ = resp.Body.Close()
		sseCancel()
		t.Fatalf("expected Content-Type text/event-stream, got %q", ct)
	}

	msgCh := make(chan runtime.SSEMessage, 64)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var msg runtime.SSEMessage
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &msg); err != nil {
				errCh <- fmt.Errorf("unmarshal SSE event: %w", err)
				return
			}
			msgCh <- msg
		}
		if err := scanner.Err(); err != nil && sseCtx.Err() == nil {
			errCh <- err
		}
		close(msgCh)
	}()

	// 1. Confirm historical catch-up event arrives first.
	select {
	case err := <-errCh:
		sseCancel()
		_ = resp.Body.Close()
		t.Fatalf("SSE reader error before catch-up: %v", err)
	case first, ok := <-msgCh:
		if !ok {
			sseCancel()
			_ = resp.Body.Close()
			t.Fatalf("SSE stream closed before historical catch-up event")
		}
		if first.Event.Seq != 1 || first.Event.Type != engine.EventRunStarted {
			sseCancel()
			_ = resp.Body.Close()
			t.Fatalf("expected historical RUN_STARTED at seq 1, got seq=%d type=%s", first.Event.Seq, first.Event.Type)
		}
	case <-ctx.Done():
		sseCancel()
		_ = resp.Body.Close()
		t.Fatalf("timed out waiting for historical RUN_STARTED event")
	}

	// 2. Start a worker to drive live task lease, start, completion, checkpoint, and run completion.
	wCtx, wCancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	w := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:       "sse-worker-1",
		CoordinatorURL: srv.URL,
		Concurrency:    2,
		PollInterval:   15 * time.Millisecond,
	})
	w.RegisterHandler("fast", func(_ context.Context, _ map[string]any) (map[string]any, error) {
		return map[string]any{"done": true}, nil
	})
	go func() {
		defer close(workerDone)
		_ = w.Run(wCtx)
	}()

	// 3. Collect live events until RUN_COMPLETED and verify strict ordering.
	seenTypes := map[engine.EventType]int{
		engine.EventRunStarted: 1,
	}
	lastSeq := int64(1)
	completed := false

	for !completed {
		select {
		case err := <-errCh:
			t.Fatalf("SSE reader error: %v", err)
		case msg, ok := <-msgCh:
			if !ok {
				t.Fatalf("SSE stream closed prematurely before RUN_COMPLETED (lastSeq=%d)", lastSeq)
			}
			if msg.Event.Seq <= lastSeq {
				t.Fatalf("events out of order or duplicated: got seq=%d after lastSeq=%d (type=%s)", msg.Event.Seq, lastSeq, msg.Event.Type)
			}
			lastSeq = msg.Event.Seq
			seenTypes[msg.Event.Type]++
			if msg.Event.Type == engine.EventRunCompleted {
				if msg.RunStatus != engine.RunStatusCompleted {
					t.Fatalf("expected RunStatus=%s on RUN_COMPLETED, got %s", engine.RunStatusCompleted, msg.RunStatus)
				}
				completed = true
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for live RUN_COMPLETED event; seenTypes=%v", seenTypes)
		}
	}

	// Cleanly tear down SSE stream and worker before httptest.Server closes.
	wCancel()
	<-workerDone
	sseCancel()
	_ = resp.Body.Close()
	for range msgCh {
	}

	for _, required := range []engine.EventType{
		engine.EventRunStarted,
		engine.EventTaskLeased,
		engine.EventTaskStarted,
		engine.EventTaskCompleted,
		engine.EventCheckpointSaved,
		engine.EventRunCompleted,
	} {
		if seenTypes[required] == 0 {
			t.Fatalf("expected at least one %s event in SSE stream, got counts=%v", required, seenTypes)
		}
	}
	if seenTypes[engine.EventTaskCompleted] != 2 {
		t.Fatalf("expected 2 TASK_COMPLETED events, got %d", seenTypes[engine.EventTaskCompleted])
	}
}

func TestSSESlowConsumerDoesNotBlockCoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "sse-slow.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	// Build a 90-task workflow so execution produces >270 live events, exceeding
	// the coordinator's 256-slot per-subscriber buffer channel.
	var wfBuilder strings.Builder
	wfBuilder.WriteString("name: sse-slow-consumer\ntasks:\n")
	for i := 0; i < 90; i++ {
		fmt.Fprintf(&wfBuilder, "  - id: task_%03d\n    handler: noop\n", i)
	}

	sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_sse_slow",
		WorkflowYAML: wfBuilder.String(),
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	// Connect an SSE consumer, read the initial historical event so we know the
	// subscription is registered, and then STOP READING completely.
	stalledCtx, stalledCancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(stalledCtx, http.MethodGet, srv.URL+"/v1/runs/"+sub.RunID+"/events", nil)
	if err != nil {
		stalledCancel()
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		stalledCancel()
		t.Fatalf("GET SSE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		stalledCancel()
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}

	// Read just the first data line (RUN_STARTED) and then intentionally stall.
	br := bufio.NewReader(resp.Body)
	firstLine, err := br.ReadString('\n')
	if err != nil {
		_ = resp.Body.Close()
		stalledCancel()
		t.Fatalf("read initial SSE line: %v", err)
	}
	if !strings.HasPrefix(firstLine, "data: ") {
		_ = resp.Body.Close()
		stalledCancel()
		t.Fatalf("expected initial SSE data line, got %q", firstLine)
	}

	// Execute the entire workflow on another goroutine while the SSE consumer is stalled.
	wCtx, wCancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	w := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:       "fast-worker-1",
		CoordinatorURL: srv.URL,
		Concurrency:    4,
		PollInterval:   5 * time.Millisecond,
	})
	w.RegisterHandler("noop", func(_ context.Context, _ map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})
	go func() {
		defer close(workerDone)
		_ = w.Run(wCtx)
	}()

	doneCh := make(chan error, 1)
	go func() {
		for {
			snap, err := coord.Snapshot(ctx, sub.RunID)
			if err != nil {
				doneCh <- fmt.Errorf("Snapshot: %w", err)
				return
			}
			if snap.State.Status == engine.RunStatusFailed {
				doneCh <- fmt.Errorf("workflow failed: %s", snap.State.FailureReason)
				return
			}
			if snap.State.Status == engine.RunStatusCompleted {
				doneCh <- nil
				return
			}
			if ctx.Err() != nil {
				doneCh <- fmt.Errorf("timed out waiting for coordinator completion with stalled SSE consumer")
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	select {
	case err := <-doneCh:
		if err != nil {
			wCancel()
			<-workerDone
			stalledCancel()
			_ = resp.Body.Close()
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		wCancel()
		<-workerDone
		stalledCancel()
		_ = resp.Body.Close()
		t.Fatalf("coordinator stalled while SSE consumer was not reading")
	}

	// Clean shutdown of worker and stalled SSE connection before httptest.Server closes.
	wCancel()
	<-workerDone
	stalledCancel()
	_ = resp.Body.Close()
}
