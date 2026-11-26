package runtime

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestDecodeJSONBody_SizeLimit(t *testing.T) {
	t.Run("within limit decodes cleanly", func(t *testing.T) {
		payload := `{"worker_id":"w1"}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(payload))
		rec := httptest.NewRecorder()

		var dst WorkerRegisterRequest
		if err := decodeJSONBody(rec, req, &dst, int64(len(payload))); err != nil {
			t.Fatalf("decodeJSONBody unexpected error: %v", err)
		}
		if dst.WorkerID != "w1" {
			t.Fatalf("WorkerID = %q, want %q", dst.WorkerID, "w1")
		}
	})

	t.Run("exceeding maxBytes returns MaxBytesError", func(t *testing.T) {
		payload := `{"worker_id":"worker-too-long"}`
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(payload))
		rec := httptest.NewRecorder()

		var dst WorkerRegisterRequest
		err := decodeJSONBody(rec, req, &dst, 10)
		if err == nil {
			t.Fatal("expected error for oversized payload, got nil")
		}
		var maxErr *http.MaxBytesError
		if !errors.As(err, &maxErr) {
			t.Fatalf("expected *http.MaxBytesError, got %T: %v", err, err)
		}
		if maxErr.Limit != 10 {
			t.Fatalf("MaxBytesError.Limit = %d, want 10", maxErr.Limit)
		}
	})

	t.Run("handleSubmitWorkflow returns 413 on oversized body", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "submit.db")
		st, err := sqlite.Open(dbPath)
		if err != nil {
			t.Fatalf("sqlite.Open: %v", err)
		}
		defer func() { _ = st.Close() }()

		coord := NewCoordinator(st, CoordinatorConfig{})
		oversized := make([]byte, defaultMaxJSONBodyBytes+64)
		copy(oversized, `{"yaml":"`)
		for i := len(`{"yaml":"`); i < len(oversized)-2; i++ {
			oversized[i] = 'a'
		}
		copy(oversized[len(oversized)-2:], `"}`)

		req := httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(oversized))
		rec := httptest.NewRecorder()
		coord.handleSubmitWorkflow(rec, req)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
		}
	})
}
