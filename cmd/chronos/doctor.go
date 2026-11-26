package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// DoctorCheck represents the outcome of a single diagnostic check.
type DoctorCheck struct {
	Name   string
	Status string // "PASS", "WARN", or "FAIL"
	Detail string
}

// FormatVersion returns the formatted build-time version string.
func FormatVersion() string {
	return fmt.Sprintf("chronos %s (commit %s, built %s)", version, commit, date)
}

// RunDoctorChecks executes local runtime, SQLite journal, integrity, auth
// posture, and optional coordinator health diagnostics.
func RunDoctorChecks(ctx context.Context, dbPath, coordURL string) []DoctorCheck {
	checks := []DoctorCheck{
		{
			Name:   "go_runtime",
			Status: "PASS",
			Detail: fmt.Sprintf("%s (%s/%s)", runtime.Version(), runtime.GOOS, runtime.GOARCH),
		},
		checkSQLiteJournal(ctx, dbPath),
		checkSQLiteIntegrity(ctx, dbPath),
		checkAuthPosture(),
	}

	if strings.TrimSpace(coordURL) != "" {
		checks = append(checks, checkCoordinatorHealth(ctx, coordURL))
	}

	return checks
}

func checkSQLiteJournal(ctx context.Context, dbPath string) DoctorCheck {
	if dbPath == "" {
		return DoctorCheck{
			Name:   "sqlite_journal",
			Status: "WARN",
			Detail: "no database path configured (run 'chronos init')",
		}
	}
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return DoctorCheck{
				Name:   "sqlite_journal",
				Status: "WARN",
				Detail: fmt.Sprintf("not yet initialized at %s (run 'chronos init')", dbPath),
			}
		}
		return DoctorCheck{
			Name:   "sqlite_journal",
			Status: "FAIL",
			Detail: fmt.Sprintf("stat %s: %v", dbPath, err),
		}
	}

	st, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		return DoctorCheck{
			Name:   "sqlite_journal",
			Status: "FAIL",
			Detail: fmt.Sprintf("open read-only: %v", err),
		}
	}
	defer func() { _ = st.Close() }()

	if err := st.Ping(ctx); err != nil {
		return DoctorCheck{
			Name:   "sqlite_journal",
			Status: "FAIL",
			Detail: fmt.Sprintf("ping: %v", err),
		}
	}

	runs, err := st.ListRuns(ctx)
	if err != nil {
		return DoctorCheck{
			Name:   "sqlite_journal",
			Status: "FAIL",
			Detail: fmt.Sprintf("list runs: %v", err),
		}
	}
	return DoctorCheck{
		Name:   "sqlite_journal",
		Status: "PASS",
		Detail: fmt.Sprintf("%s (runs=%d, ping=ok)", dbPath, len(runs)),
	}
}

func checkAuthPosture() DoctorCheck {
	if strings.TrimSpace(os.Getenv("CHRONOS_AUTH_TOKEN")) == "" {
		return DoctorCheck{
			Name:   "auth_token",
			Status: "WARN",
			Detail: "CHRONOS_AUTH_TOKEN is unset (Coordinator API unauthenticated unless --auth-token is passed)",
		}
	}
	return DoctorCheck{
		Name:   "auth_token",
		Status: "PASS",
		Detail: "CHRONOS_AUTH_TOKEN is set",
	}
}

func checkSQLiteIntegrity(ctx context.Context, dbPath string) DoctorCheck {
	if dbPath == "" {
		return DoctorCheck{
			Name:   "sqlite_integrity",
			Status: "WARN",
			Detail: "skipped (no database path configured)",
		}
	}
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return DoctorCheck{
				Name:   "sqlite_integrity",
				Status: "WARN",
				Detail: fmt.Sprintf("skipped (%s not yet initialized)", dbPath),
			}
		}
		return DoctorCheck{
			Name:   "sqlite_integrity",
			Status: "FAIL",
			Detail: fmt.Sprintf("stat %s: %v", dbPath, err),
		}
	}

	q := url.Values{}
	q.Add("_pragma", "busy_timeout(2000)")
	q.Set("mode", "ro")
	dsn := fmt.Sprintf("file:%s?%s", filepath.ToSlash(dbPath), q.Encode())

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return DoctorCheck{
			Name:   "sqlite_integrity",
			Status: "FAIL",
			Detail: fmt.Sprintf("open read-only: %v", err),
		}
	}
	defer func() { _ = db.Close() }()

	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return DoctorCheck{
			Name:   "sqlite_integrity",
			Status: "FAIL",
			Detail: fmt.Sprintf("PRAGMA quick_check: %v", err),
		}
	}
	if result != "ok" {
		return DoctorCheck{
			Name:   "sqlite_integrity",
			Status: "FAIL",
			Detail: fmt.Sprintf("PRAGMA quick_check returned %q", result),
		}
	}
	return DoctorCheck{
		Name:   "sqlite_integrity",
		Status: "PASS",
		Detail: "PRAGMA quick_check: ok",
	}
}

func checkCoordinatorHealth(ctx context.Context, coordURL string) DoctorCheck {
	endpoint := strings.TrimRight(coordURL, "/") + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return DoctorCheck{
			Name:   "coordinator_health",
			Status: "WARN",
			Detail: fmt.Sprintf("invalid coordinator URL %q: %v", coordURL, err),
		}
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return DoctorCheck{
			Name:   "coordinator_health",
			Status: "WARN",
			Detail: fmt.Sprintf("%s unreachable: %v", endpoint, err),
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return DoctorCheck{
			Name:   "coordinator_health",
			Status: "WARN",
			Detail: fmt.Sprintf("%s returned HTTP %d", endpoint, resp.StatusCode),
		}
	}

	return DoctorCheck{
		Name:   "coordinator_health",
		Status: "PASS",
		Detail: fmt.Sprintf("%s returned 200 OK", endpoint),
	}
}

// FormatDoctorReport formats diagnostic checks into a human-readable report
// and indicates whether any check ended with status "FAIL".
func FormatDoctorReport(checks []DoctorCheck) (string, bool) {
	var sb strings.Builder
	hasFailure := false
	for _, c := range checks {
		if c.Status == "FAIL" {
			hasFailure = true
		}
		fmt.Fprintf(&sb, "[%s] %s: %s\n", c.Status, c.Name, c.Detail)
	}
	return sb.String(), hasFailure
}
