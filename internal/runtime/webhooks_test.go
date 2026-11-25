package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

func TestWebhookDeliveryAndHMACSignature(t *testing.T) {
	const secret = "super-secret-signing-key"

	var (
		mu          sync.Mutex
		gotBody     []byte
		gotSig      string
		gotCT       string
		deliveredCh = make(chan struct{}, 1)
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		mu.Lock()
		gotBody = body
		gotSig = r.Header.Get(webhookSignatureHeader)
		gotCT = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case deliveredCh <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	notifier, err := NewWebhookNotifier(WebhookConfig{
		URL:         srv.URL,
		Secret:      secret,
		Timeout:     2 * time.Second,
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		QueueSize:   8,
	})
	if err != nil {
		t.Fatalf("NewWebhookNotifier: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		notifier.Start(ctx)
	}()

	payload := WebhookPayload{
		Event:        "RUN_COMPLETED",
		RunID:        "run-123",
		WorkflowName: "etl-daily",
		Status:       engine.RunStatusCompleted,
		TimestampNs:  1700000000000000000,
	}
	notifier.Notify(payload)

	select {
	case <-deliveredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}

	mu.Lock()
	bodyCopy := append([]byte(nil), gotBody...)
	sigCopy := gotSig
	ctCopy := gotCT
	mu.Unlock()

	if ctCopy != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ctCopy)
	}

	expectedSig := SignPayload(secret, bodyCopy)
	if expectedSig == "" || sigCopy != expectedSig {
		t.Fatalf("signature mismatch: got %q, want %q", sigCopy, expectedSig)
	}

	var decoded WebhookPayload
	if err := json.Unmarshal(bodyCopy, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded != payload {
		t.Fatalf("decoded payload mismatch: got %+v, want %+v", decoded, payload)
	}

	cancel()
	<-done
}

func TestWebhookRetry503Then200(t *testing.T) {
	var attempts atomic.Int32
	deliveredCh := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		select {
		case deliveredCh <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	notifier, err := NewWebhookNotifier(WebhookConfig{
		URL:         srv.URL,
		MaxAttempts: 3,
		BaseBackoff: 5 * time.Millisecond,
		Timeout:     2 * time.Second,
		QueueSize:   4,
	})
	if err != nil {
		t.Fatalf("NewWebhookNotifier: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		notifier.Start(ctx)
	}()

	notifier.Notify(WebhookPayload{
		Event:         "RUN_FAILED",
		RunID:         "run-456",
		WorkflowName:  "ml-train",
		Status:        engine.RunStatusFailed,
		TimestampNs:   1700000001000000000,
		FailureReason: "task-2 exited 1",
	})

	select {
	case <-deliveredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retried webhook delivery")
	}

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}

	cancel()
	<-done
}

func TestWebhookNotifyNonBlockingWhenQueueFull(t *testing.T) {
	notifier, err := NewWebhookNotifier(WebhookConfig{
		URL:       "http://127.0.0.1:65535/hook",
		QueueSize: 2,
	})
	if err != nil {
		t.Fatalf("NewWebhookNotifier: %v", err)
	}

	// Do not call Start so the queue remains full.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			notifier.Notify(WebhookPayload{
				Event: "RUN_COMPLETED",
				RunID: "run-full",
			})
		}
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Notify blocked when queue was full")
	}

	if got := notifier.Dropped(); got != 8 {
		t.Fatalf("expected 8 dropped notifications, got %d", got)
	}
}

func TestWebhookNewNotifierInvalidURL(t *testing.T) {
	cases := []string{
		"",
		"ftp://example.com/hook",
		"javascript:alert(1)",
		"http://",
		"/relative/path",
		"://missing-scheme",
	}

	for _, raw := range cases {
		if _, err := NewWebhookNotifier(WebhookConfig{URL: raw}); err == nil {
			t.Errorf("expected error for invalid URL %q, got nil", raw)
		}
	}
}
