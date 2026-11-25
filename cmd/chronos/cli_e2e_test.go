package main

import (
	"encoding/json"
	"net"
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
}
