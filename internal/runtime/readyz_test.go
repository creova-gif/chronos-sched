package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestReadyz_HealthySQLiteStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "readyz.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := NewCoordinator(st, CoordinatorConfig{})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	coord.handleReadyz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body=%s)", rec.Code, rec.Body.String())
	}

	var resp ReadyzResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode ReadyzResponse: %v", err)
	}
	if resp.Status != "ready" {
		t.Fatalf("expected status %q, got %q", "ready", resp.Status)
	}
	if resp.ActiveRuns != 0 || resp.Workers != 0 {
		t.Fatalf("expected active_runs=0 workers=0, got active_runs=%d workers=%d", resp.ActiveRuns, resp.Workers)
	}
	if resp.Error != "" {
		t.Fatalf("expected empty error, got %q", resp.Error)
	}
}

func TestReadyz_ClosedSQLiteStoreReturns503(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "readyz-closed.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("st.Close: %v", err)
	}

	coord := NewCoordinator(st, CoordinatorConfig{})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	coord.handleReadyz(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d (body=%s)", rec.Code, rec.Body.String())
	}

	var resp ReadyzResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode ReadyzResponse: %v", err)
	}
	if resp.Status != "not_ready" {
		t.Fatalf("expected status %q, got %q", "not_ready", resp.Status)
	}
	if resp.Error == "" {
		t.Fatalf("expected non-empty error when store is closed")
	}
}

type failingListRunsStore struct {
	store.JournalStore
	err error
}

func (s failingListRunsStore) ListRuns(_ context.Context, _ ...engine.RunStatus) ([]store.RunRecord, error) {
	return nil, s.err
}

func TestReadyz_StoreErrorReturns503(t *testing.T) {
	coord := NewCoordinator(failingListRunsStore{err: errors.New("db unavailable")}, CoordinatorConfig{})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	coord.handleReadyz(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d (body=%s)", rec.Code, rec.Body.String())
	}

	var resp ReadyzResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode ReadyzResponse: %v", err)
	}
	if resp.Status != "not_ready" {
		t.Fatalf("expected status %q, got %q", "not_ready", resp.Status)
	}
	if resp.Error != "db unavailable" {
		t.Fatalf("expected error %q, got %q", "db unavailable", resp.Error)
	}
}

type failingPingStore struct {
	store.JournalStore
	err error
}

func (s failingPingStore) Ping(_ context.Context) error {
	return s.err
}

func TestReadyz_HealthCheckerErrorReturns503(t *testing.T) {
	coord := NewCoordinator(failingPingStore{err: errors.New("ping failed")}, CoordinatorConfig{})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	coord.handleReadyz(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d (body=%s)", rec.Code, rec.Body.String())
	}

	var resp ReadyzResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode ReadyzResponse: %v", err)
	}
	if resp.Status != "not_ready" {
		t.Fatalf("expected status %q, got %q", "not_ready", resp.Status)
	}
	if resp.Error != "ping failed" {
		t.Fatalf("expected error %q, got %q", "ping failed", resp.Error)
	}
}
