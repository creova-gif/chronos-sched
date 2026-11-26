package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

const testAuthWorkflowYAML = `name: auth-test-wf
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 10
`

func newAuthTestCoordinator(t *testing.T, token string) (*runtime.Coordinator, *httptest.Server) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "auth.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
		AuthToken:  token,
	})
	srv := httptest.NewServer(coord.Handler())
	t.Cleanup(srv.Close)
	return coord, srv
}

func doAuthRequest(t *testing.T, method, url, authHeader string, body []byte) *http.Response {
	t.Helper()
	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		t.Fatalf("NewRequest(%s %s): %v", method, url, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do(%s %s): %v", method, url, err)
	}
	return resp
}

func assertUnauthorized(t *testing.T, resp *http.Response) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("expected WWW-Authenticate=Bearer, got %q", got)
	}
	var errResp runtime.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Error != "unauthorized" {
		t.Fatalf("expected ErrorResponse.Error=%q, got %q", "unauthorized", errResp.Error)
	}
}

func TestAuthOpenMode(t *testing.T) {
	_, srv := newAuthTestCoordinator(t, "")

	for _, tc := range []struct {
		method string
		path   string
		body   []byte
		want   int
	}{
		{http.MethodGet, "/healthz", nil, http.StatusOK},
		{http.MethodGet, "/v1/runs", nil, http.StatusOK},
		{http.MethodGet, "/metrics", nil, http.StatusOK},
		{http.MethodPost, "/v1/workflows/run", mustJSON(t, runtime.SubmitWorkflowRequest{WorkflowYAML: testAuthWorkflowYAML}), http.StatusCreated},
	} {
		resp := doAuthRequest(t, tc.method, srv.URL+tc.path, "", tc.body)
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("%s %s: expected %d, got %d", tc.method, tc.path, tc.want, resp.StatusCode)
		}
	}
}

func TestAuthProtectedMode(t *testing.T) {
	const token = "top-secret"
	_, srv := newAuthTestCoordinator(t, token)

	// /healthz remains unauthenticated for probes.
	healthResp := doAuthRequest(t, http.MethodGet, srv.URL+"/healthz", "", nil)
	_ = healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz without auth: expected 200, got %d", healthResp.StatusCode)
	}

	submitBody := mustJSON(t, runtime.SubmitWorkflowRequest{WorkflowYAML: testAuthWorkflowYAML})
	routes := []struct {
		method string
		path   string
		body   []byte
		want   int
	}{
		{http.MethodGet, "/v1/runs", nil, http.StatusOK},
		{http.MethodGet, "/metrics", nil, http.StatusOK},
		{http.MethodPost, "/v1/workflows/run", submitBody, http.StatusCreated},
	}

	for _, r := range routes {
		// Missing Authorization header.
		assertUnauthorized(t, doAuthRequest(t, r.method, srv.URL+r.path, "", r.body))
		// Wrong Bearer token.
		assertUnauthorized(t, doAuthRequest(t, r.method, srv.URL+r.path, "Bearer wrong-token", r.body))
		// Wrong scheme.
		assertUnauthorized(t, doAuthRequest(t, r.method, srv.URL+r.path, "Basic "+token, r.body))

		// Valid Bearer token.
		okResp := doAuthRequest(t, r.method, srv.URL+r.path, "Bearer "+token, r.body)
		_ = okResp.Body.Close()
		if okResp.StatusCode != r.want {
			t.Fatalf("%s %s with valid token: expected %d, got %d", r.method, r.path, r.want, okResp.StatusCode)
		}
	}
}

func TestAuthWorkerEndToEnd(t *testing.T) {
	const token = "top-secret"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coord, srv := newAuthTestCoordinator(t, token)

	submitBody := mustJSON(t, runtime.SubmitWorkflowRequest{
		RunID:        "run-auth-e2e",
		WorkflowYAML: testAuthWorkflowYAML,
	})
	resp := doAuthRequest(t, http.MethodPost, srv.URL+"/v1/workflows/run", "Bearer "+token, submitBody)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit workflow: expected 201, got %d", resp.StatusCode)
	}

	w := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          "auth-worker-1",
		CoordinatorURL:    srv.URL,
		AuthToken:         token,
		Concurrency:       1,
		HeartbeatInterval: 100 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
	})

	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- w.Run(workerCtx)
	}()

	snap := waitForRunCompleted(t, ctx, coord, "run-auth-e2e", 5*time.Second)
	if snap.State.Tasks["step1"].Status != engine.TaskStatusCompleted {
		t.Fatalf("expected step1 COMPLETED, got %s", snap.State.Tasks["step1"].Status)
	}

	stopWorker()
	select {
	case <-workerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to stop")
	}
}

func TestAuthMiddlewareWithAuditLogsDenied(t *testing.T) {
	const secretToken = "super-secret-expected-token"

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := runtime.AuthMiddlewareWithAudit(secretToken, logger, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
	}
	if nextCalled {
		t.Fatal("expected next handler not to be called on unauthorized request")
	}

	raw := buf.String()
	if strings.Contains(raw, secretToken) {
		t.Fatalf("audit log leaked expected token: %s", raw)
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &entry); err != nil {
		t.Fatalf("json.Unmarshal audit log: %v (raw=%q)", err, raw)
	}
	if audit, ok := entry["audit"].(bool); !ok || !audit {
		t.Fatalf("expected audit=true, got %#v", entry["audit"])
	}
	if entry["action"] != "auth_bearer" {
		t.Fatalf("expected action=%q, got %#v", "auth_bearer", entry["action"])
	}
	if entry["outcome"] != "denied" {
		t.Fatalf("expected outcome=%q, got %#v", "denied", entry["outcome"])
	}
}

func TestAuthMiddlewareWithAuditAndMetricsIncrementsCounter(t *testing.T) {
	const token = "secret-token"
	var deniedCount int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := runtime.AuthMiddlewareWithAuditAndMetrics(token, nil, func() {
		deniedCount++
	}, next)

	deniedReq := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	deniedReq.Header.Set("Authorization", "Bearer wrong-token")
	deniedRec := httptest.NewRecorder()
	handler.ServeHTTP(deniedRec, deniedReq)
	if deniedRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", deniedRec.Code)
	}
	if deniedCount != 1 {
		t.Fatalf("expected onDenied count = 1 after 401, got %d", deniedCount)
	}

	okReq := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	okReq.Header.Set("Authorization", "Bearer "+token)
	okRec := httptest.NewRecorder()
	handler.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", okRec.Code)
	}
	if deniedCount != 1 {
		t.Fatalf("expected onDenied count to remain 1 after 200, got %d", deniedCount)
	}
}

func TestAuthBeforeRateLimitProtectsTenantBucket(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := runtime.TenantMiddleware(
		runtime.AuthMiddlewareWithAudit(
			"secret-token",
			nil,
			runtime.RateLimitMiddleware(
				runtime.RateLimitConfig{RequestsPerSecond: 0.001, Burst: 1},
				okHandler,
			),
		),
	)

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		req.Header.Set("X-Chronos-Tenant", "victim")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated request %d: expected 401 Unauthorized, got %d", i+1, rec.Code)
		}
	}

	authReq := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	authReq.Header.Set("Authorization", "Bearer secret-token")
	authReq.Header.Set("X-Chronos-Tenant", "victim")
	authRec := httptest.NewRecorder()
	handler.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("authenticated request: expected 200 OK, got %d", authRec.Code)
	}
	if got := authRec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("expected X-RateLimit-Remaining=0, got %q", got)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}
