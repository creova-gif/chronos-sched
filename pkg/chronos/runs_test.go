package chronos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientListRuns(t *testing.T) {
	wantRuns := []RunRecord{
		{
			RunID:        "run-1",
			ParentRunID:  "run-0",
			ForkFromSeq:  4,
			WorkflowName: "etl",
			WorkflowHash: "sha256:abc",
			WorkflowYAML: "name: etl\n",
			Status:       RunStatusCompleted,
			CreatedAtNs:  1700000000000000000,
		},
	}

	t.Run("success with filters and headers", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Fatalf("method = %s, want GET", r.Method)
			}
			if r.URL.Path != "/v1/runs" {
				t.Fatalf("path = %s, want /v1/runs", r.URL.Path)
			}
			if got := r.URL.Query().Get("status"); got != "COMPLETED" {
				t.Fatalf("status query = %q, want COMPLETED", got)
			}
			if got := r.URL.Query().Get("limit"); got != "25" {
				t.Fatalf("limit query = %q, want 25", got)
			}
			if got := r.URL.RawQuery; got != "limit=25&status=COMPLETED" && got != "status=COMPLETED&limit=25" {
				t.Fatalf("raw query = %q, unexpected", got)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
				t.Fatalf("Authorization = %q, want Bearer secret-token", got)
			}
			if got := r.Header.Get("X-Chronos-Tenant"); got != "tenant-a" {
				t.Fatalf("X-Chronos-Tenant = %q, want tenant-a", got)
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(wantRuns); err != nil {
				t.Fatalf("encode response: %v", err)
			}
		}))
		defer srv.Close()

		client := NewClient(srv.URL).WithBearerToken("secret-token").WithTenant("tenant-a")
		got, err := client.ListRuns(context.Background(), ListRunsOptions{
			Status: string(RunStatusCompleted),
			Limit:  25,
		})
		if err != nil {
			t.Fatalf("ListRuns error: %v", err)
		}
		if !reflect.DeepEqual(got, wantRuns) {
			t.Fatalf("ListRuns = %#v, want %#v", got, wantRuns)
		}
	})

	t.Run("400 api error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			if err := json.NewEncoder(w).Encode(errorResponse{Error: "invalid status filter"}); err != nil {
				t.Fatalf("encode error response: %v", err)
			}
		}))
		defer srv.Close()

		client := NewClient(srv.URL)
		_, err := client.ListRuns(context.Background(), ListRunsOptions{Status: "BOGUS"})
		if err == nil {
			t.Fatal("ListRuns expected error, got nil")
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("error = %T (%v), want *APIError", err, err)
		}
		if apiErr.StatusCode != http.StatusBadRequest {
			t.Fatalf("APIError.StatusCode = %d, want 400", apiErr.StatusCode)
		}
		if apiErr.Message != "invalid status filter" {
			t.Fatalf("APIError.Message = %q, want %q", apiErr.Message, "invalid status filter")
		}
	})
}

func TestClientWaitForRun(t *testing.T) {
	t.Run("transitions_to_completed", func(t *testing.T) {
		var polls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/v1/runs/run-1" {
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
			}

			n := polls.Add(1)
			status := RunStatusPending
			switch n {
			case 1:
				status = RunStatusPending
			case 2:
				status = RunStatusRunning
			default:
				status = RunStatusCompleted
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(RunSnapshot{
				State: &RunState{
					RunID:  "run-1",
					Status: status,
				},
			}); err != nil {
				t.Fatalf("encode snapshot: %v", err)
			}
		}))
		defer srv.Close()

		client := NewClient(srv.URL)
		snap, err := client.WaitForRun(context.Background(), "run-1", 5*time.Millisecond)
		if err != nil {
			t.Fatalf("WaitForRun error: %v", err)
		}
		if snap.State == nil || snap.State.Status != RunStatusCompleted {
			t.Fatalf("status = %v, want %s", snap.State, RunStatusCompleted)
		}
		if got := polls.Load(); got != 3 {
			t.Fatalf("polls = %d, want 3", got)
		}
	})

	t.Run("context_cancelled", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(RunSnapshot{
				State: &RunState{
					RunID:  "run-2",
					Status: RunStatusRunning,
				},
			}); err != nil {
				t.Fatalf("encode snapshot: %v", err)
			}
		}))
		defer srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
		defer cancel()

		client := NewClient(srv.URL)
		_, err := client.WaitForRun(ctx, "run-2", 10*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitForRun error = %v, want context.DeadlineExceeded", err)
		}
	})
}
