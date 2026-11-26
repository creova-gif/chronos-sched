package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerHTTPTransportAndGracefulWait(t *testing.T) {
	w := NewWorker(WorkerConfig{Concurrency: 32})
	if w.client.Timeout != 5*time.Second {
		t.Fatalf("expected client Timeout 5s, got %v", w.client.Timeout)
	}
	tr, ok := w.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", w.client.Transport)
	}
	if tr.MaxIdleConnsPerHost < 64 {
		t.Fatalf("expected MaxIdleConnsPerHost >= 64, got %d", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns < 64 {
		t.Fatalf("expected MaxIdleConns >= 64, got %d", tr.MaxIdleConns)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Fatalf("expected IdleConnTimeout 90s, got %v", tr.IdleConnTimeout)
	}

	var leased atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/workers/register":
			_ = json.NewEncoder(rw).Encode(WorkerRegisterResponse{OK: true})
		case "/v1/workers/heartbeat":
			_ = json.NewEncoder(rw).Encode(HeartbeatResponse{})
		case "/v1/workers/poll":
			if leased.CompareAndSwap(false, true) {
				_ = json.NewEncoder(rw).Encode(LeasePollResponse{
					Lease: &LeasedTask{
						RunID:        "run-1",
						TaskID:       "task-1",
						Handler:      "slow",
						FencingToken: 1,
					},
				})
				return
			}
			_ = json.NewEncoder(rw).Encode(LeasePollResponse{})
		default:
			_, _ = rw.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	worker := NewWorker(WorkerConfig{
		WorkerID:          "w-graceful",
		CoordinatorURL:    srv.URL,
		Concurrency:       1,
		HeartbeatInterval: 20 * time.Millisecond,
		PollInterval:      10 * time.Millisecond,
	})

	startedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var handlerDone atomic.Bool

	worker.RegisterHandler("slow", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		close(startedCh)
		<-releaseCh
		handlerDone.Store(true)
		return map[string]any{"done": true}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- worker.Run(ctx)
	}()

	select {
	case <-startedCh:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("timed out waiting for in-flight handler to start")
	}

	cancel()

	select {
	case err := <-runErrCh:
		t.Fatalf("Worker.Run returned before in-flight handler finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if handlerDone.Load() {
		t.Fatal("expected in-flight handler to still be blocked before release")
	}

	close(releaseCh)

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Worker.Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Worker.Run to exit after handler finished")
	}

	if !handlerDone.Load() {
		t.Fatal("expected in-flight handler to finish before Worker.Run returned")
	}
}
