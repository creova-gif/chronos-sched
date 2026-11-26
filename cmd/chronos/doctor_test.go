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
	t.Setenv("CHRONOS_AUTH_TOKEN", "")
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
	if byName["auth_token"].Status != "WARN" {
		t.Fatalf("expected auth_token WARN, got %+v", byName["auth_token"])
	}
}

func TestDoctorChecksInitializedAndCoordinator(t *testing.T) {
	t.Setenv("CHRONOS_AUTH_TOKEN", "test-token")
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
	for _, name := range []string{"go_runtime", "sqlite_journal", "sqlite_integrity", "auth_token", "coordinator_health"} {
		c, ok := byName[name]
		if !ok {
			t.Fatalf("missing check %q in %+v", name, checks)
		}
		if c.Status != "PASS" {
			t.Fatalf("expected %s PASS, got %+v", name, c)
		}
	}
	if !strings.Contains(byName["sqlite_journal"].Detail, "runs=1") {
		t.Fatalf("expected runs=1 in sqlite_journal detail, got %q", byName["sqlite_journal"].Detail)
	}
	if !strings.Contains(byName["sqlite_journal"].Detail, "ping=ok") {
		t.Fatalf("expected ping=ok in sqlite_journal detail, got %q", byName["sqlite_journal"].Detail)
	}
	if !strings.Contains(byName["sqlite_integrity"].Detail, "ok") {
		t.Fatalf("expected ok in sqlite_integrity detail, got %q", byName["sqlite_integrity"].Detail)
	}
}

func TestDoctorChecksAuthPosture(t *testing.T) {
	ctx := context.Background()
	missingDB := filepath.Join(t.TempDir(), "missing.db")

	t.Setenv("CHRONOS_AUTH_TOKEN", "")
	checks := RunDoctorChecks(ctx, missingDB, "")
	report, hasFailure := FormatDoctorReport(checks)
	if hasFailure {
		t.Fatalf("expected WARN not to fail doctor report:\n%s", report)
	}
	authCheck := indexChecks(checks)["auth_token"]
	if authCheck.Status != "WARN" {
		t.Fatalf("expected auth_token WARN when unset, got %+v", authCheck)
	}
	if !strings.Contains(authCheck.Detail, "CHRONOS_AUTH_TOKEN is unset") {
		t.Fatalf("unexpected auth_token WARN detail: %q", authCheck.Detail)
	}

	t.Setenv("CHRONOS_AUTH_TOKEN", "super-secret-value")
	checks = RunDoctorChecks(ctx, missingDB, "")
	authCheck = indexChecks(checks)["auth_token"]
	if authCheck.Status != "PASS" {
		t.Fatalf("expected auth_token PASS when set, got %+v", authCheck)
	}
	if strings.Contains(authCheck.Detail, "super-secret-value") {
		t.Fatalf("auth_token detail must not leak token value: %q", authCheck.Detail)
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
