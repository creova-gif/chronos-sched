package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLIBinaryFullWorkflowForkDiffAndBranches(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "chronos")
	build := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, string(out))
	}

	runCLI := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(binPath, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("chronos %v failed: %v\n%s", args, err, string(out))
		}
		return string(out)
	}

	// 1. chronos init
	initOut := runCLI("init")
	if !strings.Contains(initOut, "Initialized ChronosSched workspace") {
		t.Fatalf("unexpected init output: %s", initOut)
	}

	// Pick free local port for coordinator.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	coordURL := "http://" + addr
	dbPath := filepath.Join(dir, ".chronos", "chronos.db")

	// 2. Start coordinator + worker processes.
	serverCmd := exec.Command(binPath, "server", "start", "--addr", addr, "--db", dbPath)
	serverCmd.Dir = dir
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() {
		_ = serverCmd.Process.Kill()
		_, _ = serverCmd.Process.Wait()
	}()

	workerCmd := exec.Command(binPath, "worker", "start", "--coordinator", coordURL, "--concurrency", "2", "--worker-id", "w-cli-1")
	workerCmd.Dir = dir
	if err := workerCmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	defer func() {
		_ = workerCmd.Process.Kill()
		_, _ = workerCmd.Process.Wait()
	}()

	// Wait briefly for coordinator to listen.
	time.Sleep(250 * time.Millisecond)

	// 3. Submit small 2-task workflow via chronos workflow run.
	wfPath := filepath.Join(dir, "pipeline.yaml")
	wfYAML := `
name: cli-e2e
tasks:
  - id: step1
    handler: sim
    checkpoint_name: cp_one
    params: { duration_ms: 10, env: prod }
  - id: step2
    handler: sim
    depends_on: [step1]
    params: { duration_ms: 10, mode: normal }
`
	if err := os.WriteFile(wfPath, []byte(wfYAML), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	subOut := runCLI("workflow", "run", wfPath, "--coordinator", coordURL, "--run-id", "run_cli_1")
	if !strings.Contains(subOut, "run_cli_1") {
		t.Fatalf("unexpected submit output: %s", subOut)
	}

	// Wait for run to reach top-level COMPLETED status.
	waitDone := func(runID string) {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			out := runCLI("workflow", "inspect", runID, "--coordinator", coordURL)
			var parsed struct {
				State struct {
					Status string `json:"status"`
				} `json:"state"`
			}
			if err := json.Unmarshal([]byte(out), &parsed); err == nil && parsed.State.Status == "COMPLETED" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s to complete", runID)
	}
	waitDone("run_cli_1")

	// 4. Fork run_cli_2 from checkpoint cp_one with override on step2.
	forkOut := runCLI("fork", "run_cli_1", "--checkpoint", "cp_one", "--override", "step2.mode=shadow", "--new-run-id", "run_cli_2", "--coordinator", coordURL)
	if !strings.Contains(forkOut, "Forked run_cli_2 from run_cli_1") {
		t.Fatalf("unexpected fork output: %s", forkOut)
	}
	waitDone("run_cli_2")

	// 4b. Materialize live merge via chronos merge --apply and verify /metrics endpoint.
	mergeApplyOut := runCLI("merge", "run_cli_1", "run_cli_2", "--prefer-branch", "--apply", "--new-run-id", "run_cli_merged", "--coordinator", coordURL)
	if !strings.Contains(mergeApplyOut, "run_cli_merged") {
		t.Fatalf("unexpected merge --apply output:\n%s", mergeApplyOut)
	}
	waitDone("run_cli_merged")

	mResp, err := http.Get(coordURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	mBody, _ := io.ReadAll(mResp.Body)
	_ = mResp.Body.Close()
	if mResp.StatusCode != http.StatusOK || !strings.Contains(string(mBody), "chronos_timelines_merged_total 1") {
		t.Fatalf("unexpected /metrics response (status=%d):\n%s", mResp.StatusCode, string(mBody))
	}

	// 4c. Probe live coordinator readiness via doctor and drain worker w-cli-1.
	liveDocOut := runCLI("doctor", "--db", dbPath, "--coordinator", coordURL)
	if !strings.Contains(liveDocOut, "[PASS] coordinator_readyz") {
		t.Fatalf("expected [PASS] coordinator_readyz in doctor output:\n%s", liveDocOut)
	}
	drainOut := runCLI("worker", "drain", "w-cli-1", "--coordinator", coordURL)
	if !strings.Contains(drainOut, "Worker w-cli-1 draining=true") {
		t.Fatalf("unexpected worker drain output:\n%s", drainOut)
	}
	listOut := runCLI("worker", "list", "--coordinator", coordURL)
	if !strings.Contains(listOut, "w-cli-1") || !strings.Contains(listOut, "draining=true") {
		t.Fatalf("unexpected worker list output:\n%s", listOut)
	}

	// 5. Stop server & worker, then run offline SQLite CLI commands: history, replay, diff, branches.
	_ = workerCmd.Process.Kill()
	_, _ = workerCmd.Process.Wait()
	_ = serverCmd.Process.Kill()
	_, _ = serverCmd.Process.Wait()

	histOut := runCLI("history", "run_cli_2", "--db", dbPath)
	if !strings.Contains(histOut, "TIMELINE_FORKED") {
		t.Fatalf("expected TIMELINE_FORKED in history:\n%s", histOut)
	}

	replayOut := runCLI("replay", "run_cli_2", "--dry-run", "--db", dbPath)
	if !strings.Contains(replayOut, "Replay OK") || !strings.Contains(replayOut, "tasks=2/2") {
		t.Fatalf("unexpected replay output:\n%s", replayOut)
	}

	diffOut := runCLI("diff", "run_cli_1", "run_cli_2", "--db", dbPath)
	if !strings.Contains(diffOut, "step2") || !strings.Contains(diffOut, "output_changed=true") {
		t.Fatalf("expected step2 output_changed=true in diff:\n%s", diffOut)
	}

	branchOut := runCLI("branches", "--db", dbPath)
	if !strings.Contains(branchOut, "run_cli_1") || !strings.Contains(branchOut, "run_cli_2") {
		t.Fatalf("expected both runs in branches tree:\n%s", branchOut)
	}

	mergeOut := runCLI("merge", "run_cli_1", "run_cli_2", "--prefer-branch", "--db", dbPath)
	if !strings.Contains(mergeOut, "step2") {
		t.Fatalf("expected step2 in merge plan:\n%s", mergeOut)
	}

	// 6. Export flight-recorder bundle, import into a fresh DB, replay, and export Perfetto trace.
	bundlePath := filepath.Join(dir, "run_cli_2.bundle.json")
	exportOut := runCLI("export", "run_cli_2", "-o", bundlePath, "--db", dbPath)
	if !strings.Contains(exportOut, "Exported flight-recorder bundle") {
		t.Fatalf("unexpected export output: %s", exportOut)
	}

	db2Path := filepath.Join(dir, "imported.db")
	importOut := runCLI("import", bundlePath, "--run-id", "run_imported", "--db", db2Path)
	if !strings.Contains(importOut, "Imported verified bundle as run_imported") {
		t.Fatalf("unexpected import output: %s", importOut)
	}
	replayImported := runCLI("replay", "run_imported", "--db", db2Path)
	if !strings.Contains(replayImported, "Replay OK") || !strings.Contains(replayImported, "tasks=2/2") {
		t.Fatalf("unexpected imported replay output:\n%s", replayImported)
	}

	tracePath := filepath.Join(dir, "trace.json")
	traceOut := runCLI("trace", "run_cli_2", "-o", tracePath, "--db", dbPath)
	if !strings.Contains(traceOut, "Chrome Trace / Perfetto") {
		t.Fatalf("unexpected trace output: %s", traceOut)
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil || !strings.Contains(string(traceBytes), `"traceEvents"`) {
		t.Fatalf("expected valid traceEvents JSON, err=%v", err)
	}

	// 7. Recurring schedule CLI: add -> list -> delete.
	schedAdd := runCLI("schedule", "add", "nightly", "--cron", "0 2 * * *", "--workflow", wfPath, "--db", dbPath)
	if !strings.Contains(schedAdd, `Scheduled "nightly"`) {
		t.Fatalf("unexpected schedule add output: %s", schedAdd)
	}
	schedList := runCLI("schedule", "list", "--db", dbPath)
	if !strings.Contains(schedList, "nightly") {
		t.Fatalf("expected nightly in schedule list:\n%s", schedList)
	}
	schedDel := runCLI("schedule", "delete", "nightly", "--db", dbPath)
	if !strings.Contains(schedDel, `Deleted schedule "nightly"`) {
		t.Fatalf("unexpected schedule delete output: %s", schedDel)
	}

	// 8. Lineage-safe GC dry-run.
	gcOut := runCLI("gc", "--older-than", "0s", "--dry-run", "--db", dbPath)
	if !strings.Contains(gcOut, "GC dry_run=true") || !strings.Contains(gcOut, "deleted_runs=0") {
		t.Fatalf("unexpected gc dry-run output: %s", gcOut)
	}

	// 9. chronos version & chronos doctor.
	verOut := runCLI("version")
	if !strings.Contains(verOut, "chronos") {
		t.Fatalf("unexpected version output: %s", verOut)
	}
	docOut := runCLI("doctor", "--db", dbPath, "--coordinator", "")
	if !strings.Contains(docOut, "[PASS] sqlite_integrity") {
		t.Fatalf("unexpected doctor output:\n%s", docOut)
	}

	// 10. Filtered history & time-travel inspect-at.
	filtHist := runCLI("history", "run_cli_1", "--task", "step1", "--type", "TASK_COMPLETED", "--db", dbPath)
	if !strings.Contains(filtHist, "step1") || strings.Contains(filtHist, "step2") {
		t.Fatalf("unexpected filtered history:\n%s", filtHist)
	}
	inspectAt := runCLI("inspect-at", "run_cli_1", "--checkpoint", "cp_one", "--db", dbPath)
	if !strings.Contains(inspectAt, `"status": "RUNNING"`) {
		t.Fatalf("expected intermediate RUNNING state at cp_one:\n%s", inspectAt)
	}

	// 11. Dead-letter queue list on read-only store.
	dlqOut := runCLI("dlq", "list", "--db", dbPath)
	if !strings.Contains(dlqOut, "No dead-letter entries.") {
		t.Fatalf("unexpected dlq list output:\n%s", dlqOut)
	}
}
