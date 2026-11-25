package runtime

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

const (
	defaultWebhookTimeout     = 5 * time.Second
	defaultWebhookMaxAttempts = 3
	defaultWebhookBaseBackoff = 100 * time.Millisecond
	defaultWebhookQueueSize   = 128

	webhookSignatureHeader = "X-Chronos-Signature-256"
)

// WebhookPayload is the JSON body delivered when a run reaches a terminal state.
type WebhookPayload struct {
	Event         string           `json:"event"`
	RunID         string           `json:"run_id"`
	WorkflowName  string           `json:"workflow_name"`
	Status        engine.RunStatus `json:"status"`
	TimestampNs   int64            `json:"timestamp_ns"`
	FailureReason string           `json:"failure_reason,omitempty"`
}

// WebhookConfig configures outbound terminal-run webhook delivery.
type WebhookConfig struct {
	URL         string
	Secret      string
	Timeout     time.Duration
	MaxAttempts int
	BaseBackoff time.Duration
	QueueSize   int
}

// WebhookNotifier asynchronously delivers signed webhook notifications from a bounded queue.
type WebhookNotifier struct {
	cfg     WebhookConfig
	client  *http.Client
	queue   chan WebhookPayload
	dropped atomic.Int64
}

// NewWebhookNotifier validates cfg, applies defaults, and constructs a WebhookNotifier.
func NewWebhookNotifier(cfg WebhookConfig) (*WebhookNotifier, error) {
	parsed, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid webhook URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid webhook URL %q: must use http or https scheme with a non-empty host", cfg.URL)
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultWebhookTimeout
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultWebhookMaxAttempts
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = defaultWebhookBaseBackoff
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultWebhookQueueSize
	}

	return &WebhookNotifier{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		queue: make(chan WebhookPayload, cfg.QueueSize),
	}, nil
}

// Start drains queued webhook notifications until ctx is cancelled.
func (n *WebhookNotifier) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-n.queue:
			n.deliverWithRetry(ctx, payload)
		}
	}
}

// Notify enqueues a webhook payload without blocking; if the queue is full, the payload is dropped.
func (n *WebhookNotifier) Notify(payload WebhookPayload) {
	select {
	case n.queue <- payload:
	default:
		n.dropped.Add(1)
	}
}

// Dropped returns the total number of notifications dropped due to a full queue.
func (n *WebhookNotifier) Dropped() int64 {
	return n.dropped.Load()
}

// SignPayload computes "sha256=" + hex(HMAC-SHA256(secret, body)) when secret is non-empty.
func SignPayload(secret string, body []byte) string {
	if secret == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func (n *WebhookNotifier) deliverWithRetry(ctx context.Context, payload WebhookPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	sig := SignPayload(n.cfg.Secret, body)

	backoff := n.cfg.BaseBackoff
	for attempt := 1; attempt <= n.cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}

		retryable := n.sendOnce(ctx, body, sig)
		if !retryable || attempt == n.cfg.MaxAttempts {
			return
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
	}
}

func (n *WebhookNotifier) sendOnce(ctx context.Context, body []byte, sig string) (retryable bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if sig != "" {
		req.Header.Set(webhookSignatureHeader, sig)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return true
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode >= http.StatusInternalServerError || resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return false
}
