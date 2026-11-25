package chronos_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/pkg/chronos"
)

func TestStreamRunEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, _, srv := setupTestClientAndServer(t)

	wfYAML := `
name: sdk-stream-test
tasks:
  - id: step1
    handler: sim
    checkpoint_name: after_step1
`
	sub, err := client.SubmitWorkflow(ctx, "run_stream", wfYAML)
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	var (
		mu     sync.Mutex
		events []chronos.StreamEvent
	)
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- client.StreamRunEvents(streamCtx, sub.RunID, func(ev chronos.StreamEvent) error {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
			if ev.Type == "RUN_COMPLETED" {
				streamCancel()
			}
			return nil
		})
	}()

	// Complete the single task so the coordinator emits TASK_LEASED, TASK_COMPLETED,
	// CHECKPOINT_SAVED, and RUN_COMPLETED.
	pollAndCompleteTask(t, srv.URL, "worker-stream-1", "step1", map[string]any{"ok": true})

	select {
	case err := <-streamDone:
		if err != nil {
			t.Fatalf("StreamRunEvents returned unexpected error: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for StreamRunEvents to exit: %v", ctx.Err())
	}

	mu.Lock()
	defer mu.Unlock()

	wantOrder := []string{
		"RUN_STARTED",
		"TASK_LEASED",
		"TASK_COMPLETED",
		"CHECKPOINT_SAVED",
		"RUN_COMPLETED",
	}
	if len(events) != len(wantOrder) {
		gotTypes := make([]string, len(events))
		for i, ev := range events {
			gotTypes[i] = ev.Type
		}
		t.Fatalf("expected %d events %v, got %d: %v", len(wantOrder), wantOrder, len(events), gotTypes)
	}
	for i, want := range wantOrder {
		if events[i].Type != want {
			t.Errorf("event[%d].Type = %q, want %q", i, events[i].Type, want)
		}
		if events[i].Seq != int64(i+1) {
			t.Errorf("event[%d].Seq = %d, want %d", i, events[i].Seq, i+1)
		}
	}
	last := events[len(events)-1]
	if last.RunStatus != chronos.RunStatusCompleted {
		t.Errorf("final event RunStatus = %q, want %q", last.RunStatus, chronos.RunStatusCompleted)
	}
}

func TestClientWithBearerToken(t *testing.T) {
	const token = "test-secret-token"
	var (
		mu       sync.Mutex
		authSeen []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authSeen = append(authSeen, r.Method+" "+r.URL.Path+":"+r.Header.Get("Authorization"))
		mu.Unlock()

		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"event\":{\"seq\":1,\"ts_ns\":100,\"event_type\":\"RUN_COMPLETED\"},\"run_status\":\"COMPLETED\"}\n\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := chronos.NewClient(srv.URL).WithBearerToken(token)
	if err := client.Health(ctx); err != nil {
		t.Fatalf("Health with bearer token: %v", err)
	}

	var got []chronos.StreamEvent
	if err := client.StreamRunEvents(ctx, "run-1", func(ev chronos.StreamEvent) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("StreamRunEvents with bearer token: %v", err)
	}
	if len(got) != 1 || got[0].Type != "RUN_COMPLETED" || got[0].RunStatus != chronos.RunStatusCompleted {
		t.Fatalf("unexpected stream events: %+v", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(authSeen) != 2 {
		t.Fatalf("expected 2 authenticated requests, got %d: %v", len(authSeen), authSeen)
	}
	for _, entry := range authSeen {
		if !strings.HasSuffix(entry, ":Bearer "+token) {
			t.Errorf("expected Bearer token header in %q", entry)
		}
	}
}
