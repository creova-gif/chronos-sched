package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type auditLogRecord struct {
	Level    string `json:"level"`
	Msg      string `json:"msg"`
	Audit    bool   `json:"audit"`
	Action   string `json:"action"`
	Outcome  string `json:"outcome"`
	TenantID string `json:"tenant_id"`
	RunID    string `json:"run_id"`
	Actor    string `json:"actor"`
	Detail   string `json:"detail"`
}

func parseAuditRecords(t *testing.T, raw string) []auditLogRecord {
	t.Helper()
	var records []auditLogRecord
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec auditLogRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.Audit {
			records = append(records, rec)
		}
	}
	return records
}

func TestSecurityAuditLogs(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "security-audit.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	var buf syncBuffer
	coord := NewCoordinator(st, CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
		Logger:     slog.New(slog.NewJSONHandler(&buf, nil)),
	})
	handler := coord.Handler()

	const auditWorkflowYAML = `name: security-audit-wf
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 10
`

	// 1. Submit a workflow as tenant "alpha", then attempt cross-tenant access as "beta".
	submitBody, err := json.Marshal(SubmitWorkflowRequest{
		RunID:        "run-audit-1",
		WorkflowYAML: auditWorkflowYAML,
	})
	if err != nil {
		t.Fatalf("marshal submit request: %v", err)
	}
	submitReq := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", bytes.NewReader(submitBody))
	submitReq.Header.Set("Content-Type", "application/json")
	submitReq.Header.Set(HeaderTenant, "alpha")
	submitRec := httptest.NewRecorder()
	handler.ServeHTTP(submitRec, submitReq)
	if submitRec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d, want 201 (%s)", submitRec.Code, submitRec.Body.String())
	}

	crossReq := httptest.NewRequest(http.MethodGet, "/v1/runs/run-audit-1", nil)
	crossReq.Header.Set(HeaderTenant, "beta")
	crossRec := httptest.NewRecorder()
	handler.ServeHTTP(crossRec, crossReq)
	if crossRec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant GET status = %d, want 404 (%s)", crossRec.Code, crossRec.Body.String())
	}

	// 2. Request approval and approve as tenant "alpha" with actor "alice".
	reqAppReq := httptest.NewRequest(http.MethodPost, "/v1/runs/run-audit-1/tasks/step1/request-approval", nil)
	reqAppReq.Header.Set(HeaderTenant, "alpha")
	reqAppRec := httptest.NewRecorder()
	handler.ServeHTTP(reqAppRec, reqAppReq)
	if reqAppRec.Code != http.StatusOK {
		t.Fatalf("request-approval status = %d, want 200 (%s)", reqAppRec.Code, reqAppRec.Body.String())
	}

	approveBody, err := json.Marshal(ApprovalRequest{
		Actor:    "alice",
		Comment:  "looks good",
		Approved: true,
	})
	if err != nil {
		t.Fatalf("marshal approve request: %v", err)
	}
	approveReq := httptest.NewRequest(http.MethodPost, "/v1/runs/run-audit-1/tasks/step1/approve", bytes.NewReader(approveBody))
	approveReq.Header.Set("Content-Type", "application/json")
	approveReq.Header.Set(HeaderTenant, "alpha")
	approveRec := httptest.NewRecorder()
	handler.ServeHTTP(approveRec, approveReq)
	if approveRec.Code != http.StatusOK {
		t.Fatalf("approve status = %d, want 200 (%s)", approveRec.Code, approveRec.Body.String())
	}

	// 3. Record a dead-letter entry for run-audit-1 and requeue it as tenant "alpha".
	if err := st.RecordDeadLetter(ctx, store.DeadLetterEntry{
		RunID:        "run-audit-1",
		TaskID:       "step1",
		WorkflowName: "security-audit-wf",
		Attempt:      3,
		Reason:       "max retries exhausted",
		FailedAtNs:   time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	dlqListReq := httptest.NewRequest(http.MethodGet, "/v1/dlq", nil)
	dlqListReq.Header.Set(HeaderTenant, "alpha")
	dlqListRec := httptest.NewRecorder()
	handler.ServeHTTP(dlqListRec, dlqListReq)
	if dlqListRec.Code != http.StatusOK {
		t.Fatalf("GET /v1/dlq status = %d, want 200 (%s)", dlqListRec.Code, dlqListRec.Body.String())
	}
	var dlqList ListDeadLettersResponse
	if err := json.Unmarshal(dlqListRec.Body.Bytes(), &dlqList); err != nil {
		t.Fatalf("unmarshal dlq list: %v", err)
	}
	if len(dlqList.Entries) != 1 {
		t.Fatalf("len(dlq entries) = %d, want 1", len(dlqList.Entries))
	}

	requeueReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/dlq/%d/requeue", dlqList.Entries[0].ID), nil)
	requeueReq.Header.Set(HeaderTenant, "alpha")
	requeueRec := httptest.NewRecorder()
	handler.ServeHTTP(requeueRec, requeueReq)
	if requeueRec.Code != http.StatusCreated {
		t.Fatalf("requeue status = %d, want 201 (%s)", requeueRec.Code, requeueRec.Body.String())
	}
	var requeueResp RequeueDeadLetterResponse
	if err := json.Unmarshal(requeueRec.Body.Bytes(), &requeueResp); err != nil {
		t.Fatalf("unmarshal requeue response: %v", err)
	}

	records := parseAuditRecords(t, buf.String())
	if len(records) != 3 {
		t.Fatalf("audit record count = %d, want 3; raw logs:\n%s", len(records), buf.String())
	}

	var sawDenied, sawApproved, sawRequeued bool
	for _, rec := range records {
		if !rec.Audit {
			t.Errorf("expected audit=true on record: %+v", rec)
		}
		switch rec.Action {
		case "tenant_access":
			sawDenied = true
			if rec.Level != "WARN" || rec.Outcome != "denied" || rec.TenantID != "beta" || rec.RunID != "run-audit-1" {
				t.Errorf("unexpected tenant_access audit record: %+v", rec)
			}
		case "task_approval":
			sawApproved = true
			if rec.Level != "INFO" || rec.Outcome != "approved" || rec.TenantID != "alpha" || rec.RunID != "run-audit-1" || rec.Actor != "alice" {
				t.Errorf("unexpected task_approval audit record: %+v", rec)
			}
		case "dlq_requeue":
			sawRequeued = true
			if rec.Level != "INFO" || rec.Outcome != "requeued" || rec.TenantID != "alpha" || rec.RunID != requeueResp.RequeuedRunID {
				t.Errorf("unexpected dlq_requeue audit record: %+v", rec)
			}
		default:
			t.Errorf("unexpected audit action %q: %+v", rec.Action, rec)
		}
	}
	if !sawDenied || !sawApproved || !sawRequeued {
		t.Fatalf("missing expected audit records: denied=%v approved=%v requeued=%v", sawDenied, sawApproved, sawRequeued)
	}
}
