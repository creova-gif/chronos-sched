package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net"
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

func TestWebhookBlocksRedirectsByDefault(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var redirectHits atomic.Int32
	redirectHitCh := make(chan struct{}, 1)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectHits.Add(1)
		http.Redirect(w, r, target.URL, http.StatusFound)
		select {
		case redirectHitCh <- struct{}{}:
		default:
		}
	}))
	defer redirector.Close()

	notifier, err := NewWebhookNotifier(WebhookConfig{
		URL:         redirector.URL,
		Timeout:     2 * time.Second,
		MaxAttempts: 2,
		BaseBackoff: 5 * time.Millisecond,
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
		Event: "RUN_COMPLETED",
		RunID: "run-redirect",
	})

	select {
	case <-redirectHitCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for redirector request")
	}

	// Give any potential redirect or retry a brief window to settle.
	time.Sleep(50 * time.Millisecond)

	if got := targetHits.Load(); got != 0 {
		t.Fatalf("expected redirect target to never be hit, got %d hits", got)
	}
	if got := redirectHits.Load(); got != 1 {
		t.Fatalf("expected 1 hit on redirector, got %d", got)
	}

	cancel()
	<-done
}

func TestWebhookBlockPrivateIPsRejectsLiteralAndLocalhost(t *testing.T) {
	cases := []string{
		"http://127.0.0.1:8080/hook",
		"http://169.254.169.254/latest/meta-data/",
		"http://10.0.0.1/hook",
		"http://[::1]:8080/hook",
		"http://localhost:8080/hook",
	}

	for _, raw := range cases {
		if _, err := NewWebhookNotifier(WebhookConfig{
			URL:             raw,
			BlockPrivateIPs: true,
		}); err == nil {
			t.Errorf("expected error for blocked webhook URL %q, got nil", raw)
		}
	}
}

func TestIsBlockedWebhookIP(t *testing.T) {
	cases := []struct {
		ip      string
		blocked bool
	}{
		// Public IPs
		{ip: "93.184.216.34", blocked: false},
		{ip: "8.8.8.8", blocked: false},
		{ip: "2606:4700:4700::1111", blocked: false},

		// Loopback
		{ip: "127.0.0.1", blocked: true},
		{ip: "::1", blocked: true},

		// Link-local / cloud metadata
		{ip: "169.254.169.254", blocked: true},
		{ip: "fe80::1", blocked: true},

		// RFC1918 private & ULA
		{ip: "10.0.0.1", blocked: true},
		{ip: "172.16.0.1", blocked: true},
		{ip: "192.168.1.1", blocked: true},
		{ip: "fc00::1", blocked: true},
		{ip: "fd12:3456:789a::1", blocked: true},

		// Unspecified & multicast
		{ip: "0.0.0.0", blocked: true},
		{ip: "::", blocked: true},
		{ip: "224.0.0.1", blocked: true},

		// IPv4-mapped IPv6
		{ip: "::ffff:127.0.0.1", blocked: true},
		{ip: "::ffff:169.254.169.254", blocked: true},
		{ip: "::ffff:93.184.216.34", blocked: false},
	}

	for _, tc := range cases {
		parsed := net.ParseIP(tc.ip)
		if parsed == nil {
			t.Fatalf("failed to parse IP %q", tc.ip)
		}
		if got := isBlockedWebhookIP(parsed); got != tc.blocked {
			t.Errorf("isBlockedWebhookIP(%q) = %v, want %v", tc.ip, got, tc.blocked)
		}
	}
}
