package chronos_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/creova-gif/chronos-sched/pkg/chronos"
)

func TestClientApprovalsRequestAndResolve(t *testing.T) {
	const wantToken = "approval-secret"

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runs/{id}/tasks/{task_id}/request-approval", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		var body struct {
			Actor   string `json:"actor"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.Actor != "scheduler" || body.Comment != "deploy gate" {
			t.Fatalf("unexpected request-approval body: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(chronos.ApprovalResponse{
			RunID:      r.PathValue("id"),
			TaskID:     r.PathValue("task_id"),
			TaskStatus: "WAITING_APPROVAL",
			EventSeq:   12,
		})
	})

	mux.HandleFunc("POST /v1/runs/{id}/tasks/{task_id}/approve", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		var body struct {
			Actor    string `json:"actor"`
			Comment  string `json:"comment"`
			Approved *bool  `json:"approved"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.Approved == nil {
			t.Fatal("expected explicit approved boolean in JSON body")
		}
		if r.PathValue("task_id") == "missing" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"task not found"}`))
			return
		}

		status := "FAILED"
		seq := int64(14)
		if *body.Approved {
			status = "PENDING"
			seq = 13
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(chronos.ApprovalResponse{
			RunID:      r.PathValue("id"),
			TaskID:     r.PathValue("task_id"),
			TaskStatus: status,
			EventSeq:   seq,
		})
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := chronos.NewClient(srv.URL).WithBearerToken(wantToken)
	ctx := context.Background()

	reqResp, err := client.RequestTaskApproval(ctx, "run-1", "deploy", "scheduler", "deploy gate")
	if err != nil {
		t.Fatalf("RequestTaskApproval: %v", err)
	}
	if reqResp.RunID != "run-1" || reqResp.TaskID != "deploy" || reqResp.TaskStatus != "WAITING_APPROVAL" || reqResp.EventSeq != 12 {
		t.Fatalf("unexpected RequestTaskApproval response: %+v", reqResp)
	}

	approveResp, err := client.ResolveTaskApproval(ctx, "run-1", "deploy", "alice", "looks good", true)
	if err != nil {
		t.Fatalf("ResolveTaskApproval(true): %v", err)
	}
	if approveResp.TaskStatus != "PENDING" || approveResp.EventSeq != 13 {
		t.Fatalf("unexpected approve response: %+v", approveResp)
	}

	rejectResp, err := client.ResolveTaskApproval(ctx, "run-1", "deploy", "bob", "unsafe", false)
	if err != nil {
		t.Fatalf("ResolveTaskApproval(false): %v", err)
	}
	if rejectResp.TaskStatus != "FAILED" || rejectResp.EventSeq != 14 {
		t.Fatalf("unexpected reject response: %+v", rejectResp)
	}

	_, err = client.ResolveTaskApproval(ctx, "run-1", "missing", "bob", "nope", true)
	var apiErr *chronos.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 APIError, got %v", err)
	}
}
