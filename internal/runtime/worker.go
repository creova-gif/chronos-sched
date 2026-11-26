package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

// HandlerFunc executes a single task payload and returns its structured output.
type HandlerFunc func(ctx context.Context, input map[string]any) (map[string]any, error)

type WorkerConfig struct {
	WorkerID          string
	CoordinatorURL    string
	AuthToken         string
	Concurrency       int
	Resources         map[string]int
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
}

type activeTaskExec struct {
	lease  ActiveLease
	cancel context.CancelFunc
}

// Worker runs standalone task execution loops, 1s heartbeats, and reconnect
// backoff against a Coordinator HTTP/JSON endpoint.
type Worker struct {
	cfg      WorkerConfig
	client   *http.Client
	handlers map[string]HandlerFunc

	mu     sync.Mutex
	active map[string]activeTaskExec // key: runID/taskID
}

func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.WorkerID == "" {
		cfg.WorkerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = engine.DefaultHeartbeatInterval
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	cfg.CoordinatorURL = strings.TrimRight(cfg.CoordinatorURL, "/")
	cfg.Resources = cloneResources(cfg.Resources)

	w := &Worker{
		cfg: cfg,
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        max(64, cfg.Concurrency*2),
				MaxIdleConnsPerHost: max(32, cfg.Concurrency*2),
				IdleConnTimeout:     90 * time.Second,
			},
		},
		handlers: make(map[string]HandlerFunc),
		active:   make(map[string]activeTaskExec),
	}
	w.RegisterHandler("sim", builtinSimHandler)
	w.RegisterHandler("shell", builtinShellHandler)
	w.RegisterHandler("http", builtinHTTPHandler)
	return w
}

func (w *Worker) RegisterHandler(name string, fn HandlerFunc) {
	w.handlers[name] = fn
}

// Run blocks until ctx is cancelled, continuously maintaining registration,
// 1s heartbeats, and concurrency-bounded task execution loops.
func (w *Worker) Run(ctx context.Context) error {
	// Best-effort initial registration with bounded retry backoff.
	backoff := 50 * time.Millisecond
	for {
		if err := w.register(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 500*time.Millisecond {
				backoff *= 2
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(ctx)
	}()

	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.pollLoop(ctx)
		}()
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (w *Worker) register(ctx context.Context) error {
	req := WorkerRegisterRequest{
		WorkerID:     w.cfg.WorkerID,
		PID:          os.Getpid(),
		Concurrency:  w.cfg.Concurrency,
		Resources:    w.cfg.Resources,
		ActiveLeases: w.snapshotActiveLeases(),
	}
	var resp WorkerRegisterResponse
	return w.postJSON(ctx, "/v1/workers/register", req, &resp)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req := HeartbeatRequest{
				WorkerID:     w.cfg.WorkerID,
				PID:          os.Getpid(),
				Concurrency:  w.cfg.Concurrency,
				Resources:    w.cfg.Resources,
				ActiveLeases: w.snapshotActiveLeases(),
			}
			var resp HeartbeatResponse
			if err := w.postJSON(ctx, "/v1/workers/heartbeat", req, &resp); err != nil {
				continue
			}
			for _, rev := range resp.Revoked {
				w.cancelActive(rev)
			}
		}
	}
}

func (w *Worker) pollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var resp LeasePollResponse
		if err := w.postJSON(ctx, "/v1/workers/poll", LeasePollRequest{
			WorkerID:  w.cfg.WorkerID,
			Resources: w.cfg.Resources,
		}, &resp); err != nil || resp.Lease == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.pollDelay()):
			}
			continue
		}
		w.executeLease(ctx, resp.Lease)
	}
}

func (w *Worker) pollDelay() time.Duration {
	base := w.cfg.PollInterval
	if base <= 0 {
		return 0
	}
	jitterMax := int64(base / 4)
	if jitterMax <= 0 {
		return base
	}
	return base + time.Duration(rand.Int64N(jitterMax+1))
}

func (w *Worker) executeLease(parentCtx context.Context, lease *LeasedTask) {
	taskCtx, cancel := context.WithCancel(parentCtx)
	if lease.TimeoutMs > 0 {
		taskCtx, cancel = context.WithTimeout(parentCtx, time.Duration(lease.TimeoutMs)*time.Millisecond)
	}
	defer cancel()

	key := lease.RunID + "/" + lease.TaskID
	al := ActiveLease{RunID: lease.RunID, TaskID: lease.TaskID, FencingToken: lease.FencingToken}
	w.mu.Lock()
	w.active[key] = activeTaskExec{lease: al, cancel: cancel}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.active, key)
		w.mu.Unlock()
	}()

	_ = w.postJSON(parentCtx, "/v1/tasks/started", TaskStartedRequest{
		RunID:        lease.RunID,
		TaskID:       lease.TaskID,
		WorkerID:     w.cfg.WorkerID,
		FencingToken: lease.FencingToken,
	}, nil)

	handler, ok := w.handlers[lease.Handler]
	if !ok {
		_ = w.postJSON(parentCtx, "/v1/tasks/fail", TaskFailRequest{
			RunID:        lease.RunID,
			TaskID:       lease.TaskID,
			WorkerID:     w.cfg.WorkerID,
			FencingToken: lease.FencingToken,
			Error:        fmt.Sprintf("unknown handler %q", lease.Handler),
		}, nil)
		return
	}

	output, err := invokeHandlerSafe(taskCtx, handler, lease.Input)
	if err != nil {
		if parentCtx.Err() != nil {
			return
		}
		_ = w.postJSON(parentCtx, "/v1/tasks/fail", TaskFailRequest{
			RunID:        lease.RunID,
			TaskID:       lease.TaskID,
			WorkerID:     w.cfg.WorkerID,
			FencingToken: lease.FencingToken,
			Error:        err.Error(),
		}, nil)
		return
	}

	// Retry completion briefly in case coordinator is mid-restart.
	for attempt := 0; attempt < 15; attempt++ {
		err := w.postJSON(parentCtx, "/v1/tasks/complete", TaskCompleteRequest{
			RunID:        lease.RunID,
			TaskID:       lease.TaskID,
			WorkerID:     w.cfg.WorkerID,
			FencingToken: lease.FencingToken,
			Output:       output,
		}, nil)
		if err == nil || parentCtx.Err() != nil {
			return
		}
		select {
		case <-parentCtx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (w *Worker) snapshotActiveLeases() []ActiveLease {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]ActiveLease, 0, len(w.active))
	for _, a := range w.active {
		out = append(out, a.lease)
	}
	return out
}

func (w *Worker) cancelActive(rev ActiveLease) {
	key := rev.RunID + "/" + rev.TaskID
	w.mu.Lock()
	if cur, ok := w.active[key]; ok && cur.lease.FencingToken == rev.FencingToken {
		cur.cancel()
	}
	w.mu.Unlock()
}

func (w *Worker) postJSON(ctx context.Context, path string, reqBody any, respBody any) error {
	raw, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.CoordinatorURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.cfg.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.AuthToken)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if respBody != nil {
		return json.NewDecoder(resp.Body).Decode(respBody)
	}
	return nil
}

func builtinSimHandler(ctx context.Context, input map[string]any) (map[string]any, error) {
	durMs := int64(40)
	if v, ok := input["duration_ms"]; ok {
		switch n := v.(type) {
		case int:
			durMs = int64(n)
		case int64:
			durMs = n
		case float64:
			durMs = int64(n)
		}
	}
	if durMs > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(durMs) * time.Millisecond):
		}
	}
	out := map[string]any{"status": "ok"}
	for k, v := range input {
		if k != "duration_ms" {
			out[k] = v
		}
	}
	return out, nil
}

func builtinShellHandler(ctx context.Context, input map[string]any) (map[string]any, error) {
	cmdStr, _ := input["cmd"].(string)
	if strings.TrimSpace(cmdStr) == "" {
		return nil, fmt.Errorf("shell handler requires non-empty 'cmd' param")
	}
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	b, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("shell command failed: %w: %s", err, strings.TrimSpace(string(b)))
	}
	return map[string]any{"stdout": strings.TrimSpace(string(b))}, nil
}

func builtinHTTPHandler(ctx context.Context, input map[string]any) (map[string]any, error) {
	targetURL, _ := input["url"].(string)
	if strings.TrimSpace(targetURL) == "" {
		return nil, fmt.Errorf("http handler requires non-empty 'url' param")
	}
	method, _ := input["method"].(string)
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return map[string]any{"status_code": resp.StatusCode, "body": string(body)}, nil
}

func invokeHandlerSafe(ctx context.Context, fn HandlerFunc, input map[string]any) (output map[string]any, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			output = nil
			err = fmt.Errorf("handler panic: %v", rec)
		}
	}()
	return fn(ctx, input)
}
