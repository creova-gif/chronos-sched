package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestFormatVersion(t *testing.T) {
	origVersion, origCommit, origDate := version, commit, date
	t.Cleanup(func() {
		version, commit, date = origVersion, origCommit, origDate
	})

	version = "v1.2.3"
	commit = "abc1234"
	date = "2026-11-25T00:00:00Z"

	got := FormatVersion()
	for _, want := range []string{"v1.2.3", "abc1234", "2026-11-25T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Fatalf("FormatVersion() = %q, expected substring %q", got, want)
		}
	}
}

func TestDoctorChecksUninitialized(t *testing.T) {
	ctx := context.Background()
	missingDB := filepath.Join(t.TempDir(), "missing.db")

	checks := RunDoctorChecks(ctx, missingDB, "")
	report, hasFailure := FormatDoctorReport(checks)
	if hasFailure {
		t.Fatalf("expected no failure for uninitialized DB, got report:\n%s", report)
	}

	byName := indexChecks(checks)
	if byName["go_runtime"].Status != "PASS" {
		t.Fatalf("expected go_runtime PASS, got %+v", byName["go_runtime"])
	}
	if byName["sqlite_journal"].Status != "WARN" {
		t.Fatalf("expected sqlite_journal WARN, got %+v", byName["sqlite_journal"])
	}
	if byName["sqlite_integrity"].Status != "WARN" {
		t.Fatalf("expected sqlite_integrity WARN, got %+v", byName["sqlite_integrity"])
	}
}

func TestDoctorChecksInitializedAndCoordinator(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos.db")

	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	err = st.CreateRun(ctx, store.RunRecord{
		RunID:        "run-1",
		WorkflowName: "wf",
		WorkflowHash: "hash",
		WorkflowYAML: "name: wf",
		Status:       engine.RunStatusCompleted,
		CreatedAtNs:  time.Now().UnixNano(),
	}, nil)
	if err != nil {
		_ = st.Close()
		t.Fatalf("CreateRun: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("st.Close: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	checks := RunDoctorChecks(ctx, dbPath, srv.URL)
	report, hasFailure := FormatDoctorReport(checks)
	if hasFailure {
		t.Fatalf("expected no failure, got report:\n%s", report)
	}

	byName := indexChecks(checks)
	for _, name := range []string{"go_runtime", "sqlite_journal", "sqlite_integrity", "coordinator_health"} {
		c, ok := byName[name]
		if !ok {
			t.Fatalf("missing check %q in %+v", name, checks)
		}
		if c.Status != "PASS" {
			t.Fatalf("expected %s PASS, got %+v", name, c)
		}
	}
	if !strings.Contains(byName["sqlite_journal"].Detail, "1 runs") {
		t.Fatalf("expected run count in sqlite_journal detail, got %q", byName["sqlite_journal"].Detail)
	}
	if !strings.Contains(byName["sqlite_integrity"].Detail, "ok") {
		t.Fatalf("expected ok in sqlite_integrity detail, got %q", byName["sqlite_integrity"].Detail)
	}
}

func TestFormatDoctorReportFailure(t *testing.T) {
	checks := []DoctorCheck{
		{Name: "go_runtime", Status: "PASS", Detail: "go1.23"},
		{Name: "sqlite_integrity", Status: "FAIL", Detail: "corrupt"},
	}
	report, hasFailure := FormatDoctorReport(checks)
	if !hasFailure {
		t.Fatalf("expected hasFailure=true, got false")
	}
	if !strings.Contains(report, "[FAIL] sqlite_integrity: corrupt") {
		t.Fatalf("unexpected report:\n%s", report)
	}
}

func indexChecks(checks []DoctorCheck) map[string]DoctorCheck {
	out := make(map[string]DoctorCheck, len(checks))
	for _, c := range checks {
		out[c.Name] = c
	}
	return out
}
