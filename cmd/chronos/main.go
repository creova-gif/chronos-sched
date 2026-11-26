package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/export"
	"github.com/creova-gif/chronos-sched/internal/ha"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/schedule"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
	"github.com/creova-gif/chronos-sched/internal/tui"
)

//go:embed demo.yaml
var embeddedDemoYAML string

const (
	defaultCoordAddr = "127.0.0.1:7420"
	defaultCoordURL  = "http://127.0.0.1:7420"
	defaultDBPath    = ".chronos/chronos.db"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "chronos: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}
	switch args[0] {
	case "init":
		return cmdInit(args[1:])
	case "demo":
		return cmdDemo(args[1:])
	case "server":
		if len(args) < 2 || args[1] != "start" {
			return fmt.Errorf("usage: chronos server start [--addr %s] [--db %s]", defaultCoordAddr, defaultDBPath)
		}
		return cmdServerStart(args[2:])
	case "worker":
		if len(args) < 2 || args[1] != "start" {
			return fmt.Errorf("usage: chronos worker start [--coordinator %s] [--concurrency 8] [--worker-id id]", defaultCoordURL)
		}
		return cmdWorkerStart(args[2:])
	case "workflow":
		if len(args) < 2 {
			return fmt.Errorf("usage: chronos workflow <run|inspect> ...")
		}
		switch args[1] {
		case "run":
			return cmdWorkflowRun(args[2:])
		case "inspect":
			return cmdWorkflowInspect(args[2:])
		case "pause":
			return cmdWorkflowLifecycle("pause", args[2:])
		case "resume":
			return cmdWorkflowLifecycle("resume", args[2:])
		case "cancel":
			return cmdWorkflowLifecycle("cancel", args[2:])
		default:
			return fmt.Errorf("unknown workflow subcommand %q", args[1])
		}
	case "history":
		return cmdHistory(args[1:])
	case "inspect-at":
		return cmdInspectAt(args[1:])
	case "replay":
		return cmdReplay(args[1:])
	case "fork":
		return cmdFork(args[1:])
	case "diff":
		return cmdDiff(args[1:])
	case "branches":
		return cmdBranches(args[1:])
	case "merge":
		return cmdMerge(args[1:])
	case "export":
		return cmdExport(args[1:])
	case "import":
		return cmdImport(args[1:])
	case "trace":
		return cmdTrace(args[1:])
	case "schedule":
		return cmdSchedule(args[1:])
	case "gc":
		return cmdGC(args[1:])
	case "dlq":
		return cmdDLQ(args[1:])
	case "doctor":
		return cmdDoctor(args[1:])
	case "version":
		fmt.Println(FormatVersion())
		return nil
	case "-h", "--help", "help":
		printUsage()
		return nil
	default:
		return fmt.Errorf("unknown command %q (run 'chronos --help')", args[0])
	}
}

func printUsage() {
	fmt.Print(`ChronosSched - Deterministic Distributed Workflow Scheduler & Flight Recorder

Usage:
  chronos init [--dir .chronos]
  chronos demo [--workers 5] [--workflow path.yaml] [--headless]
  chronos server start [--addr 127.0.0.1:7420] [--db .chronos/chronos.db] [--ha-id node-1] [--ha-ttl 5s] [--auth-token tok] [--webhook-url url] [--webhook-secret sec]
  chronos worker start [--coordinator http://127.0.0.1:7420] [--auth-token tok] [--concurrency 8] [--worker-id id] [--resources cpu=4,mem_mb=1024]
  chronos workflow run <file.yaml> [--coordinator http://127.0.0.1:7420] [--run-id id]
  chronos workflow inspect <run_id> [--live] [--coordinator http://127.0.0.1:7420]
  chronos workflow <pause|resume|cancel> <run_id> [--reason text] [--coordinator http://127.0.0.1:7420]
  chronos history <run_id> [--task id] [--worker id] [--type EVENT] [--min-seq N] [--max-seq N] [--redact] [--db .chronos/chronos.db]
  chronos inspect-at <run_id> (--checkpoint <N|name> | --seq <N>) [--db .chronos/chronos.db]
  chronos replay <run_id> [--dry-run] [--db .chronos/chronos.db]
  chronos fork <run_id> --checkpoint <N|name> [--override task.key=value]... [--coordinator http://127.0.0.1:7420]
  chronos diff <run_a> <run_b> [--db .chronos/chronos.db]
  chronos branches [--db .chronos/chronos.db]
  chronos merge <base_run> <branch_run> [--task id]... [--prefer-branch] [--apply] [--new-run-id id] [--coordinator http://127.0.0.1:7420] [--db .chronos/chronos.db]
  chronos export <run_id> [-o bundle.json] [--redact] [--db .chronos/chronos.db]
  chronos import <bundle.json> [--run-id id] [--db .chronos/chronos.db]
  chronos trace <run_id> [-o trace.json] [--db .chronos/chronos.db]
  chronos schedule <add|list|delete> ... [--db .chronos/chronos.db]
  chronos gc [--older-than 168h] [--dry-run] [--vacuum] [--db .chronos/chronos.db]
  chronos dlq <list|requeue> ... [--db .chronos/chronos.db] [--coordinator http://127.0.0.1:7420]
  chronos doctor [--db .chronos/chronos.db] [--coordinator http://127.0.0.1:7420]
  chronos version
`)
}

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	dir := fs.String("dir", ".chronos", "directory for ChronosSched state")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(*dir, "chronos.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		return err
	}
	_ = st.Close()

	cfgPath := filepath.Join(*dir, "config.yaml")
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		cfg := fmt.Sprintf("coordinator_addr: %s\ndb_path: %s\nheartbeat_interval: 1s\nlease_ttl: 3s\nreaper_tick: 250ms\n", defaultCoordAddr, dbPath)
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
			return err
		}
	}
	demoPath := "demo.yaml"
	if _, err := os.Stat(demoPath); os.IsNotExist(err) {
		if err := os.WriteFile(demoPath, []byte(embeddedDemoYAML), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("Initialized ChronosSched workspace in %s (db: %s, starter workflow: %s)\n", *dir, dbPath, demoPath)
	return nil
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	workers := fs.Int("workers", 5, "number of real OS worker processes to spawn")
	workflowPath := fs.String("workflow", "", "optional YAML workflow override (defaults to embedded 100-task demo)")
	dbPath := fs.String("db", "", "optional SQLite DB path (defaults to temp dir or .chronos/demo.db)")
	headless := fs.Bool("headless", false, "run without interactive TUI and exit on completion")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workers < 1 {
		return fmt.Errorf("--workers must be >= 1")
	}

	wfYAML := embeddedDemoYAML
	if *workflowPath != "" {
		b, err := os.ReadFile(*workflowPath)
		if err != nil {
			return fmt.Errorf("read workflow %q: %w", *workflowPath, err)
		}
		wfYAML = string(b)
	}

	resolvedDB := *dbPath
	if resolvedDB == "" {
		resolvedDB = filepath.Join(".chronos", fmt.Sprintf("demo-%d.db", time.Now().Unix()))
	}
	st, err := sqlite.Open(resolvedDB)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	go coord.StartReaper(ctx)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen demo coordinator: %w", err)
	}
	coordURL := "http://" + ln.Addr().String()
	srv := &http.Server{Handler: coord.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	var childMu sync.Mutex
	var children []*exec.Cmd
	for i := 1; i <= *workers; i++ {
		c := exec.CommandContext(ctx, exe, "worker", "start",
			"--coordinator", coordURL,
			"--concurrency", "2",
			"--worker-id", fmt.Sprintf("w-%d", i),
		)
		if err := c.Start(); err != nil {
			return fmt.Errorf("spawn worker w-%d: %w", i, err)
		}
		children = append(children, c)
	}
	defer func() {
		childMu.Lock()
		for _, c := range children {
			if c.Process != nil {
				_ = c.Process.Kill()
				_, _ = c.Process.Wait()
			}
		}
		childMu.Unlock()
	}()

	sub, err := coord.SubmitWorkflow(ctx, runtime.SubmitWorkflowRequest{
		RunID:        "run_001",
		WorkflowYAML: wfYAML,
	})
	if err != nil {
		return err
	}

	if *headless {
		for {
			snap, err := coord.Snapshot(ctx, sub.RunID)
			if err != nil {
				return err
			}
			if snap.State.Status == engine.RunStatusCompleted || snap.State.Status == engine.RunStatusFailed {
				fmt.Printf("Demo run %s finished: status=%s tasks=%d checkpoints=%d db=%s\n",
					sub.RunID, snap.State.Status, len(snap.State.TopoOrder), len(snap.State.Checkpoints), resolvedDB)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	model := tui.NewModel(tui.Config{
		RunID:     sub.RunID,
		AllowKill: true,
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			snap, err := coord.Snapshot(ctx, runID)
			if err != nil {
				return runtime.RunSnapshotResponse{}, nil, err
			}
			evs, err := st.LoadRunEvents(ctx, runID)
			return snap, evs, err
		},
		OnKillWorker: func() string {
			childMu.Lock()
			defer childMu.Unlock()
			for i, c := range children {
				if c != nil && c.Process != nil {
					pid := c.Process.Pid
					_ = c.Process.Kill()
					_, _ = c.Process.Wait()
					children[i] = nil
					return fmt.Sprintf("Sent SIGKILL to worker w-%d (pid %d); watching <5s lease recovery...", i+1, pid)
				}
			}
			return "No live demo worker subprocesses remaining to kill."
		},
		OnFork: func(runID string, cpNum int) (string, error) {
			resp, err := coord.ForkRun(ctx, runID, runtime.ForkRunRequest{
				Checkpoint: strconv.Itoa(cpNum),
			})
			if err != nil {
				return "", err
			}
			return resp.RunID, nil
		},
		OnForkWithOverride: func(runID string, cpNum int, override string) (string, error) {
			var overrides map[string]map[string]any
			if strings.TrimSpace(override) != "" {
				var err error
				overrides, err = parseOverrides([]string{strings.TrimSpace(override)})
				if err != nil {
					return "", err
				}
			}
			resp, err := coord.ForkRun(ctx, runID, runtime.ForkRunRequest{
				Checkpoint: strconv.Itoa(cpNum),
				Overrides:  overrides,
			})
			if err != nil {
				return "", err
			}
			return resp.RunID, nil
		},
		OnReplay: func(runID string) (string, error) {
			return replayRunFromStore(ctx, st, runID)
		},
		OnScrubState: func(runID string, seq int64) (*engine.RunState, error) {
			rec, err := st.GetRun(ctx, runID)
			if err != nil {
				return nil, err
			}
			dag, err := engine.ParseDAG([]byte(rec.WorkflowYAML))
			if err != nil {
				return nil, err
			}
			evs, err := st.LoadRunEvents(ctx, runID)
			if err != nil {
				return nil, err
			}
			return engine.StateAtSeq(dag, evs, seq)
		},
		OnPauseResume: func(runID string, pause bool) (string, error) {
			action := "resume"
			if pause {
				action = "pause"
			}
			var resp runtime.RunLifecycleResponse
			if err := rpcPost(coordURL+"/v1/runs/"+runID+"/"+action, runtime.RunLifecycleRequest{Reason: "tui"}, &resp); err != nil {
				return "", err
			}
			return fmt.Sprintf("Run %s -> %s", resp.RunID, resp.Status), nil
		},
		OnCancel: func(runID string) (string, error) {
			var resp runtime.RunLifecycleResponse
			if err := rpcPost(coordURL+"/v1/runs/"+runID+"/cancel", runtime.RunLifecycleRequest{Reason: "tui"}, &resp); err != nil {
				return "", err
			}
			return fmt.Sprintf("Run %s -> %s", resp.RunID, resp.Status), nil
		},
		OnTraceExport: func(runID string) (string, error) {
			evs, err := st.LoadRunEvents(ctx, runID)
			if err != nil {
				return "", err
			}
			raw, err := export.ToChromeTrace(evs)
			if err != nil {
				return "", err
			}
			outFile := filepath.Join(".chronos", runID+".trace.json")
			if err := os.WriteFile(outFile, raw, 0o644); err != nil {
				return "", err
			}
			return fmt.Sprintf("Exported Perfetto trace -> %s", outFile), nil
		},
		OnDiff: func(runA, runB string) (string, error) {
			return diffRunsFromStore(ctx, st, runA, runB)
		},
	})

	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return err
	}
	fmt.Printf("Demo journal persisted at %s (try: chronos history %s --db %s)\n", resolvedDB, sub.RunID, resolvedDB)
	return nil
}

func cmdServerStart(args []string) error {
	fs := flag.NewFlagSet("server start", flag.ContinueOnError)
	addr := fs.String("addr", defaultCoordAddr, "HTTP listen address")
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	haID := fs.String("ha-id", "", "optional HA candidate ID to enable active-standby leader election")
	haTTL := fs.Duration("ha-ttl", 5*time.Second, "leader lease TTL when --ha-id is set")
	authToken := fs.String("auth-token", os.Getenv("CHRONOS_AUTH_TOKEN"), "optional Bearer token for Coordinator API auth")
	webhookURL := fs.String("webhook-url", "", "optional webhook URL for terminal run notifications")
	webhookSecret := fs.String("webhook-secret", "", "optional HMAC-SHA256 secret for webhook signing")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var notifier *runtime.WebhookNotifier
	if strings.TrimSpace(*webhookURL) != "" {
		var err error
		notifier, err = runtime.NewWebhookNotifier(runtime.WebhookConfig{
			URL:    *webhookURL,
			Secret: *webhookSecret,
		})
		if err != nil {
			return fmt.Errorf("invalid webhook config: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if notifier != nil {
		go notifier.Start(ctx)
	}

	serveOnce := func(runCtx context.Context, epoch int64) error {
		st, err := sqlite.Open(*dbPath)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()

		coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
			LeaseTTL:   engine.DefaultLeaseTTL,
			ReaperTick: engine.DefaultReaperTick,
			AuthToken:  *authToken,
			Webhook:    notifier,
		})
		if err := coord.Recover(runCtx); err != nil {
			return fmt.Errorf("recover coordinator state: %w", err)
		}
		go coord.StartReaper(runCtx)

		if schedStore, err := schedule.OpenSQLiteStore(*dbPath); err == nil {
			defer func() { _ = schedStore.Close() }()
			runner := schedule.NewRunner(schedStore, func(sCtx context.Context, runID, wfYAML string) error {
				_, err := coord.SubmitWorkflow(sCtx, runtime.SubmitWorkflowRequest{
					RunID:        runID,
					WorkflowYAML: wfYAML,
				})
				return err
			}, time.Now)
			go runner.Run(runCtx, time.Second)
		}

		srv := &http.Server{Addr: *addr, Handler: coord.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			<-runCtx.Done()
			shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutCtx)
		}()

		if epoch > 0 {
			fmt.Printf("ChronosSched HA leader (%s, epoch=%d) listening on http://%s (db: %s)\n", *haID, epoch, *addr, *dbPath)
		} else {
			fmt.Printf("ChronosSched coordinator listening on http://%s (db: %s)\n", *addr, *dbPath)
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}

	if strings.TrimSpace(*haID) == "" {
		return serveOnce(ctx, 0)
	}

	elector, err := ha.OpenSQLiteElector(*dbPath, ha.DefaultLockName)
	if err != nil {
		return fmt.Errorf("open HA elector: %w", err)
	}
	defer func() { _ = elector.Close() }()

	renewInterval := *haTTL / 3
	if renewInterval < 250*time.Millisecond {
		renewInterval = 250 * time.Millisecond
	}
	fmt.Printf("ChronosSched coordinator %q entering HA standby (ttl=%s, db=%s)\n", *haID, *haTTL, *dbPath)
	ha.RunLeaderLoop(ctx, elector, *haID, *haTTL, renewInterval,
		func(leaderCtx context.Context, lease ha.LeaderLease) {
			if err := serveOnce(leaderCtx, lease.Epoch); err != nil && leaderCtx.Err() == nil {
				fmt.Fprintf(os.Stderr, "chronos leader server error: %v\n", err)
			}
		},
		func() {
			fmt.Fprintf(os.Stderr, "chronos coordinator %q lost leadership; returning to standby\n", *haID)
		},
	)
	return nil
}

func cmdWorkerStart(args []string) error {
	fs := flag.NewFlagSet("worker start", flag.ContinueOnError)
	coordURL := fs.String("coordinator", defaultCoordURL, "coordinator HTTP URL")
	authToken := fs.String("auth-token", os.Getenv("CHRONOS_AUTH_TOKEN"), "optional Bearer token for Coordinator API auth")
	concurrency := fs.Int("concurrency", 8, "concurrent task execution slots")
	workerID := fs.String("worker-id", "", "worker ID (defaults to worker-<pid>)")
	resourcesRaw := fs.String("resources", "", "optional worker resource capacity (e.g. cpu=4,mem_mb=1024)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resources, err := parseResources(*resourcesRaw)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          *workerID,
		CoordinatorURL:    *coordURL,
		AuthToken:         *authToken,
		Concurrency:       *concurrency,
		HeartbeatInterval: engine.DefaultHeartbeatInterval,
		Resources:         resources,
	})
	return w.Run(ctx)
}

func parseResources(raw string) (map[string]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := make(map[string]int)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq <= 0 || eq == len(part)-1 {
			return nil, fmt.Errorf("invalid resource %q (expected key=int)", part)
		}
		k := strings.TrimSpace(part[:eq])
		v, err := strconv.Atoi(strings.TrimSpace(part[eq+1:]))
		if err != nil || v < 0 {
			return nil, fmt.Errorf("invalid resource quantity for %q: must be non-negative integer", k)
		}
		out[k] = v
	}
	return out, nil
}

func parseFlagsAnyOrder(fs *flag.FlagSet, args []string) error {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") && len(a) > 1 {
			flags = append(flags, a)
			if !strings.Contains(a, "=") {
				name := strings.TrimLeft(a, "-")
				if f := fs.Lookup(name); f != nil {
					if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); !ok || !bf.IsBoolFlag() {
						if i+1 < len(args) {
							i++
							flags = append(flags, args[i])
						}
					}
				}
			}
		} else {
			positional = append(positional, a)
		}
	}
	return fs.Parse(append(flags, positional...))
}

func cmdWorkflowRun(args []string) error {
	fs := flag.NewFlagSet("workflow run", flag.ContinueOnError)
	coordURL := fs.String("coordinator", defaultCoordURL, "coordinator HTTP URL")
	runID := fs.String("run-id", "", "optional explicit run ID")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos workflow run <file.yaml>")
	}
	rawYAML, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	var resp runtime.SubmitWorkflowResponse
	if err := rpcPost(*coordURL+"/v1/workflows/run", runtime.SubmitWorkflowRequest{
		RunID:        *runID,
		WorkflowYAML: string(rawYAML),
	}, &resp); err != nil {
		return err
	}
	fmt.Printf("Submitted workflow %q as %s (%d tasks)\n", resp.WorkflowName, resp.RunID, resp.TaskCount)
	return nil
}

func cmdWorkflowInspect(args []string) error {
	fs := flag.NewFlagSet("workflow inspect", flag.ContinueOnError)
	coordURL := fs.String("coordinator", defaultCoordURL, "coordinator HTTP URL")
	live := fs.Bool("live", false, "open read-only live Bubble Tea TUI")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos workflow inspect <run_id> [--live]")
	}
	runID := fs.Arg(0)

	fetchSnap := func(id string) (runtime.RunSnapshotResponse, []engine.Event, error) {
		var snap runtime.RunSnapshotResponse
		if err := rpcGet(*coordURL+"/v1/runs/"+id, &snap); err != nil {
			return runtime.RunSnapshotResponse{}, nil, err
		}
		return snap, nil, nil
	}

	if !*live {
		snap, _, err := fetchSnap(runID)
		if err != nil {
			return err
		}
		b, _ := json.MarshalIndent(snap, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var evMu sync.Mutex
	var liveEvents []engine.Event
	sseCh := make(chan runtime.SSEMessage, 256)
	go func() {
		_ = tui.StreamEvents(ctx, *coordURL+"/v1/runs/"+runID+"/events", sseCh)
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-sseCh:
				evMu.Lock()
				liveEvents = append(liveEvents, msg.Event)
				evMu.Unlock()
			}
		}
	}()

	model := tui.NewModel(tui.Config{
		RunID:     runID,
		AllowKill: false, // Read-only live inspector per M1 design
		Fetch: func(id string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			snap, _, err := fetchSnap(id)
			if err != nil {
				return runtime.RunSnapshotResponse{}, nil, err
			}
			evMu.Lock()
			evsCopy := append([]engine.Event(nil), liveEvents...)
			evMu.Unlock()
			return snap, evsCopy, nil
		},
		OnForkWithOverride: func(parentID string, cpNum int, override string) (string, error) {
			var overrides map[string]map[string]any
			if strings.TrimSpace(override) != "" {
				var err error
				overrides, err = parseOverrides([]string{strings.TrimSpace(override)})
				if err != nil {
					return "", err
				}
			}
			var resp runtime.ForkRunResponse
			if err := rpcPost(*coordURL+"/v1/runs/"+parentID+"/fork", runtime.ForkRunRequest{
				Checkpoint: strconv.Itoa(cpNum),
				Overrides:  overrides,
			}, &resp); err != nil {
				return "", err
			}
			return resp.RunID, nil
		},
		OnPauseResume: func(id string, pause bool) (string, error) {
			action := "resume"
			if pause {
				action = "pause"
			}
			var resp runtime.RunLifecycleResponse
			if err := rpcPost(*coordURL+"/v1/runs/"+id+"/"+action, runtime.RunLifecycleRequest{Reason: "tui"}, &resp); err != nil {
				return "", err
			}
			return fmt.Sprintf("Run %s -> %s", resp.RunID, resp.Status), nil
		},
		OnCancel: func(id string) (string, error) {
			var resp runtime.RunLifecycleResponse
			if err := rpcPost(*coordURL+"/v1/runs/"+id+"/cancel", runtime.RunLifecycleRequest{Reason: "tui"}, &resp); err != nil {
				return "", err
			}
			return fmt.Sprintf("Run %s -> %s", resp.RunID, resp.Status), nil
		},
	})
	_, err := tea.NewProgram(model, tea.WithAltScreen()).Run()
	return err
}

func cmdHistory(args []string) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	taskID := fs.String("task", "", "filter by task ID")
	workerID := fs.String("worker", "", "filter by worker ID")
	evType := fs.String("type", "", "filter by event type (e.g. TASK_COMPLETED)")
	minSeq := fs.Int64("min-seq", 0, "minimum event seq")
	maxSeq := fs.Int64("max-seq", 0, "maximum event seq")
	redact := fs.Bool("redact", false, "redact sensitive keys (secrets, tokens, passwords) in output")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos history <run_id> [--task id] [--worker id] [--type EVENT] [--redact] [--db %s]", defaultDBPath)
	}
	runID := fs.Arg(0)

	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	evs, err := st.LoadRunEvents(context.Background(), runID)
	if err != nil {
		return err
	}
	evs = engine.FilterEvents(evs, engine.EventFilter{
		TaskID:    *taskID,
		WorkerID:  *workerID,
		EventType: engine.EventType(*evType),
		MinSeq:    *minSeq,
		MaxSeq:    *maxSeq,
	})
	if *redact {
		evs = engine.RedactEvents(evs)
	}
	for _, ev := range evs {
		fmt.Printf("%04d  %-22s  task=%-14s  att=%d  tok=%d  worker=%-10s  %s\n",
			ev.Seq, ev.Type, ev.TaskID, ev.Attempt, ev.FencingToken, ev.WorkerID, string(ev.Payload))
	}
	return nil
}

func cmdInspectAt(args []string) error {
	fs := flag.NewFlagSet("inspect-at", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	checkpoint := fs.String("checkpoint", "", "checkpoint number (N) or YAML checkpoint_name")
	seq := fs.Int64("seq", 0, "exact event sequence number")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 || (*checkpoint == "" && *seq <= 0) {
		return fmt.Errorf("usage: chronos inspect-at <run_id> (--checkpoint <N|name> | --seq <N>) [--db %s]", defaultDBPath)
	}
	runID := fs.Arg(0)

	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	rec, err := st.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	dag, err := engine.ParseDAG([]byte(rec.WorkflowYAML))
	if err != nil {
		return err
	}
	evs, err := st.LoadRunEvents(ctx, runID)
	if err != nil {
		return err
	}

	var state *engine.RunState
	if *checkpoint != "" {
		state, _, err = engine.StateAtCheckpoint(dag, evs, *checkpoint)
	} else {
		state, err = engine.StateAtSeq(dag, evs, *seq)
	}
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	_ = fs.Bool("dry-run", true, "side-effect-free deterministic replay (always enabled in M1; kept as alias)")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos replay <run_id> [--dry-run] [--db %s]", defaultDBPath)
	}
	runID := fs.Arg(0)

	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	out, err := replayRunFromStore(context.Background(), st, runID)
	if err != nil {
		return err
	}
	fmt.Println(out)
	return nil
}

func replayRunFromStore(ctx context.Context, st *sqlite.Store, runID string) (string, error) {
	rec, err := st.GetRun(ctx, runID)
	if err != nil {
		return "", err
	}
	dag, err := engine.ParseDAG([]byte(rec.WorkflowYAML))
	if err != nil {
		return "", err
	}
	evs, err := st.LoadRunEvents(ctx, runID)
	if err != nil {
		return "", err
	}
	rep, err := engine.VerifyReplay(dag, evs)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Replay OK (side-effect-free): run=%s status=%s events=%d tasks=%d/%d checkpoints=%d ambiguous=%d",
		rep.RunID, rep.FinalStatus, rep.EventsReplayed, rep.CompletedTasks, rep.TotalTasks, rep.Checkpoints, rep.AmbiguousEvents), nil
}

func cmdBranches(args []string) error {
	fs := flag.NewFlagSet("branches", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	runs, err := st.ListRuns(context.Background())
	if err != nil {
		return err
	}
	fmt.Println(store.FormatBranchTree(runs))
	return nil
}

func cmdMerge(args []string) error {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path (preview mode)")
	coordURL := fs.String("coordinator", defaultCoordURL, "coordinator HTTP URL (required for --apply)")
	apply := fs.Bool("apply", false, "materialize the merged timeline via coordinator RPC")
	newRunID := fs.String("new-run-id", "", "optional run ID for materialized merge (--apply)")
	preferBranch := fs.Bool("prefer-branch", false, "resolve conflicting outputs by preferring the branch timeline")
	var tasks multiFlag
	fs.Var(&tasks, "task", "specific task ID to cherry-pick from branch (repeatable; default all diverging completed tasks)")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: chronos merge <base_run> <branch_run> [--task id]... [--prefer-branch] [--apply] [--db %s]", defaultDBPath)
	}
	baseID, branchID := fs.Arg(0), fs.Arg(1)

	if *apply {
		var resp runtime.MergeRunResponse
		if err := rpcPost(*coordURL+"/v1/runs/"+baseID+"/merge", runtime.MergeRunRequest{
			BranchRunID:  branchID,
			NewRunID:     *newRunID,
			Tasks:        tasks,
			PreferBranch: *preferBranch,
		}, &resp); err != nil {
			return err
		}
		if resp.PlanText != "" {
			fmt.Print(resp.PlanText)
		}
		fmt.Printf("Merged %s + %s -> %s (status=%s, adopted=%d, invalidated=%d)\n",
			resp.BaseRunID, resp.BranchRunID, resp.RunID, resp.Status, resp.AdoptedCount, len(resp.InvalidatedDownstream))
		return nil
	}

	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	baseRec, err := st.GetRun(ctx, baseID)
	if err != nil {
		return err
	}
	dag, err := engine.ParseDAG([]byte(baseRec.WorkflowYAML))
	if err != nil {
		return err
	}
	baseEvs, err := st.LoadRunEvents(ctx, baseID)
	if err != nil {
		return err
	}
	branchEvs, err := st.LoadRunEvents(ctx, branchID)
	if err != nil {
		return err
	}

	strategy := engine.MergeFailOnConflict
	if *preferBranch {
		strategy = engine.MergePreferBranch
	}
	plan, err := engine.PlanBranchMerge(dag, baseEvs, branchEvs, tasks, strategy)
	if plan != nil {
		fmt.Print(engine.FormatMergePlan(plan))
	}
	if err != nil {
		if errors.Is(err, engine.ErrMergeConflict) {
			return fmt.Errorf("%w (re-run with --prefer-branch to adopt branch outputs)", err)
		}
		return err
	}
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdFork(args []string) error {
	fs := flag.NewFlagSet("fork", flag.ContinueOnError)
	coordURL := fs.String("coordinator", defaultCoordURL, "coordinator HTTP URL")
	checkpoint := fs.String("checkpoint", "", "checkpoint number (N) or YAML checkpoint_name")
	newRunID := fs.String("new-run-id", "", "optional new run ID")
	var overridesRaw multiFlag
	fs.Var(&overridesRaw, "override", "task payload override in task.key=value format (repeatable)")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 || *checkpoint == "" {
		return fmt.Errorf("usage: chronos fork <run_id> --checkpoint <N|name> [--override task.key=value]")
	}
	parentRunID := fs.Arg(0)
	overrides, err := parseOverrides(overridesRaw)
	if err != nil {
		return err
	}
	var resp runtime.ForkRunResponse
	if err := rpcPost(*coordURL+"/v1/runs/"+parentRunID+"/fork", runtime.ForkRunRequest{
		NewRunID:   *newRunID,
		Checkpoint: *checkpoint,
		Overrides:  overrides,
	}, &resp); err != nil {
		return err
	}
	fmt.Printf("Forked %s from %s at checkpoint %d (seq %d)\n", resp.RunID, resp.ParentRunID, resp.CheckpointNum, resp.ForkFromSeq)
	return nil
}

func cmdDiff(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: chronos diff <run_a> <run_b> [--db %s]", defaultDBPath)
	}
	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	out, err := diffRunsFromStore(context.Background(), st, fs.Arg(0), fs.Arg(1))
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}

func cmdWorkflowLifecycle(action string, args []string) error {
	fs := flag.NewFlagSet("workflow "+action, flag.ContinueOnError)
	coordURL := fs.String("coordinator", defaultCoordURL, "coordinator HTTP URL")
	reason := fs.String("reason", "", "optional reason for lifecycle transition")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos workflow %s <run_id> [--reason text]", action)
	}
	runID := fs.Arg(0)
	var resp runtime.RunLifecycleResponse
	if err := rpcPost(*coordURL+"/v1/runs/"+runID+"/"+action, runtime.RunLifecycleRequest{Reason: *reason}, &resp); err != nil {
		return err
	}
	fmt.Printf("Run %s -> %s\n", resp.RunID, resp.Status)
	return nil
}

func cmdExport(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	outPath := fs.String("o", "", "output bundle file path (default stdout)")
	redact := fs.Bool("redact", false, "redact sensitive keys in exported event payloads")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos export <run_id> [-o bundle.json] [--redact] [--db %s]", defaultDBPath)
	}
	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	bundle, err := export.ExportBundle(context.Background(), st, fs.Arg(0))
	if err != nil {
		return err
	}
	if *redact {
		bundle.Events = engine.RedactEvents(bundle.Events)
	}
	raw, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, raw, 0o644); err != nil {
			return err
		}
		fmt.Printf("Exported flight-recorder bundle for %s (%d events, sha256=%s) -> %s\n",
			bundle.Run.RunID, len(bundle.Events), bundle.ChecksumSHA256[:12], *outPath)
		return nil
	}
	fmt.Println(string(raw))
	return nil
}

func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	overrideRunID := fs.String("run-id", "", "optional run ID override on import")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos import <bundle.json> [--run-id id] [--db %s]", defaultDBPath)
	}
	raw, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	var bundle export.FlightBundle
	if err := json.Unmarshal(raw, &bundle); err != nil {
		return fmt.Errorf("parse bundle JSON: %w", err)
	}
	st, err := sqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	runID, err := export.ImportBundle(context.Background(), st, &bundle, *overrideRunID)
	if err != nil {
		return err
	}
	fmt.Printf("Imported verified bundle as %s (%d events) into %s\n", runID, len(bundle.Events), *dbPath)
	return nil
}

func cmdTrace(args []string) error {
	fs := flag.NewFlagSet("trace", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	outPath := fs.String("o", "", "output Chrome Trace / Perfetto JSON path (default stdout)")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos trace <run_id> [-o trace.json] [--db %s]", defaultDBPath)
	}
	st, err := sqlite.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	evs, err := st.LoadRunEvents(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	raw, err := export.ToChromeTrace(evs)
	if err != nil {
		return err
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, raw, 0o644); err != nil {
			return err
		}
		fmt.Printf("Wrote Chrome Trace / Perfetto timeline for %s -> %s\n", fs.Arg(0), *outPath)
		return nil
	}
	fmt.Println(string(raw))
	return nil
}

func cmdSchedule(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: chronos schedule <add|list|delete> ...")
	}
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("schedule add", flag.ContinueOnError)
		dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
		cronExpr := fs.String("cron", "", "5-field cron expression or @every/@hourly/@daily")
		wfPath := fs.String("workflow", "", "YAML workflow file path")
		misfire := fs.String("misfire", string(schedule.MisfireSkip), "misfire policy: SKIP or CATCH_UP")
		if err := parseFlagsAnyOrder(fs, args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 || *cronExpr == "" || *wfPath == "" {
			return fmt.Errorf("usage: chronos schedule add <id> --cron '<expr>' --workflow <file.yaml> [--misfire SKIP|CATCH_UP]")
		}
		spec, err := schedule.ParseSpec(*cronExpr)
		if err != nil {
			return err
		}
		rawYAML, err := os.ReadFile(*wfPath)
		if err != nil {
			return err
		}
		if _, err := engine.ParseDAG(rawYAML); err != nil {
			return err
		}
		st, err := schedule.OpenSQLiteStore(*dbPath)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()

		now := time.Now().UTC()
		nextRun := spec.Next(now)
		rec := schedule.ScheduleRecord{
			ID:            fs.Arg(0),
			CronExpr:      *cronExpr,
			WorkflowYAML:  string(rawYAML),
			MisfirePolicy: schedule.MisfirePolicy(strings.ToLower(*misfire)),
			NextRunAtNs:   nextRun.UnixNano(),
			Enabled:       true,
			CreatedAtNs:   now.UnixNano(),
		}
		if err := st.Upsert(context.Background(), rec); err != nil {
			return err
		}
		fmt.Printf("Scheduled %q (%s) next_run=%s\n", rec.ID, rec.CronExpr, nextRun.Format(time.RFC3339))
		return nil

	case "list":
		fs := flag.NewFlagSet("schedule list", flag.ContinueOnError)
		dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
		if err := parseFlagsAnyOrder(fs, args[1:]); err != nil {
			return err
		}
		st, err := schedule.OpenSQLiteStore(*dbPath)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()
		recs, err := st.List(context.Background())
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			fmt.Println("No schedules configured.")
			return nil
		}
		for _, r := range recs {
			fmt.Printf("%-18s  %-16s  misfire=%-8s  enabled=%v  next=%s\n",
				r.ID, r.CronExpr, r.MisfirePolicy, r.Enabled, time.Unix(0, r.NextRunAtNs).UTC().Format(time.RFC3339))
		}
		return nil

	case "delete":
		fs := flag.NewFlagSet("schedule delete", flag.ContinueOnError)
		dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
		if err := parseFlagsAnyOrder(fs, args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return fmt.Errorf("usage: chronos schedule delete <id>")
		}
		st, err := schedule.OpenSQLiteStore(*dbPath)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()
		if err := st.Delete(context.Background(), fs.Arg(0)); err != nil {
			return err
		}
		fmt.Printf("Deleted schedule %q\n", fs.Arg(0))
		return nil

	default:
		return fmt.Errorf("unknown schedule subcommand %q", args[0])
	}
}

func cmdGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	olderThan := fs.Duration("older-than", 7*24*time.Hour, "prune terminal runs older than this duration")
	dryRun := fs.Bool("dry-run", false, "report prunable runs without deleting")
	vacuum := fs.Bool("vacuum", false, "run WAL checkpoint and VACUUM after pruning")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}

	st, err := sqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	cutoffNs := time.Now().Add(-*olderThan).UnixNano()
	rep, err := st.PruneTerminalRuns(context.Background(), store.PruneOptions{
		CutoffNs: cutoffNs,
		DryRun:   *dryRun,
		Vacuum:   *vacuum,
	})
	if err != nil {
		return err
	}
	fmt.Printf("GC dry_run=%v candidates=%d deleted_runs=%d deleted_events=%d skipped_lineage_parents=%d vacuumed=%v\n",
		*dryRun, len(rep.CandidateRunIDs), rep.DeletedRuns, rep.DeletedEvents, len(rep.SkippedLineageParents), rep.Vacuumed)
	return nil
}

func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	coordURL := fs.String("coordinator", defaultCoordURL, "optional coordinator URL to probe /healthz")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	checks := RunDoctorChecks(context.Background(), *dbPath, *coordURL)
	report, hasFail := FormatDoctorReport(checks)
	fmt.Print(report)
	if hasFail {
		return fmt.Errorf("doctor reported one or more FAIL checks")
	}
	return nil
}

func diffRunsFromStore(ctx context.Context, st *sqlite.Store, runA, runB string) (string, error) {
	recA, err := st.GetRun(ctx, runA)
	if err != nil {
		return "", err
	}
	dag, err := engine.ParseDAG([]byte(recA.WorkflowYAML))
	if err != nil {
		return "", err
	}
	evsA, err := st.LoadRunEvents(ctx, runA)
	if err != nil {
		return "", err
	}
	evsB, err := st.LoadRunEvents(ctx, runB)
	if err != nil {
		return "", err
	}
	diff, err := engine.DiffRuns(dag, evsA, evsB)
	if err != nil {
		return "", err
	}
	return engine.FormatDiff(diff), nil
}

func parseOverrides(raw []string) (map[string]map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]map[string]any)
	for _, item := range raw {
		eq := strings.IndexByte(item, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("invalid override %q (expected task.key=value)", item)
		}
		lhs, val := item[:eq], item[eq+1:]
		dot := strings.IndexByte(lhs, '.')
		if dot <= 0 || dot == len(lhs)-1 {
			return nil, fmt.Errorf("invalid override key %q (expected task.key=value)", lhs)
		}
		taskID, key := lhs[:dot], lhs[dot+1:]
		if out[taskID] == nil {
			out[taskID] = make(map[string]any)
		}
		out[taskID][key] = val
	}
	return out, nil
}

func rpcPost(url string, reqBody any, respBody any) error {
	raw, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok := os.Getenv("CHRONOS_AUTH_TOKEN"); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if respBody != nil {
		return json.NewDecoder(resp.Body).Decode(respBody)
	}
	return nil
}

func cmdDLQ(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: chronos dlq <list|requeue> ... [--db %s] [--coordinator http://127.0.0.1:7420]", defaultDBPath)
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
		includeAll := fs.Bool("all", false, "include requeued dead-letter entries")
		dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
		if err := parseFlagsAnyOrder(fs, args[1:]); err != nil {
			return err
		}
		st, err := sqlite.OpenReadOnly(*dbPath)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()

		entries, err := st.ListDeadLetters(context.Background(), *includeAll)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			fmt.Println("No dead-letter entries.")
			return nil
		}
		for _, e := range entries {
			status := "active"
			if e.Requeued {
				status = "requeued:" + e.RequeuedRunID
			}
			fmt.Printf("#%d run=%s task=%s workflow=%s attempt=%d status=%s reason=%q\n",
				e.ID, e.RunID, e.TaskID, e.WorkflowName, e.Attempt, status, e.Reason)
		}
		return nil
	case "requeue":
		fs := flag.NewFlagSet("dlq requeue", flag.ContinueOnError)
		coordURL := fs.String("coordinator", "http://127.0.0.1:7420", "coordinator base URL")
		if err := parseFlagsAnyOrder(fs, args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return fmt.Errorf("usage: chronos dlq requeue <id> [--coordinator http://127.0.0.1:7420]")
		}
		id, err := strconv.ParseInt(fs.Arg(0), 10, 64)
		if err != nil || id <= 0 {
			return fmt.Errorf("invalid dead-letter id %q", fs.Arg(0))
		}
		var resp runtime.RequeueDeadLetterResponse
		if err := rpcPost(fmt.Sprintf("%s/v1/dlq/%d/requeue", strings.TrimRight(*coordURL, "/"), id), struct{}{}, &resp); err != nil {
			return err
		}
		fmt.Printf("Requeued dead-letter #%d as run %s\n", resp.DeadLetterID, resp.RequeuedRunID)
		return nil
	default:
		return fmt.Errorf("unknown dlq subcommand %q (expected list or requeue)", args[0])
	}
}

func rpcGet(url string, respBody any) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if tok := os.Getenv("CHRONOS_AUTH_TOKEN"); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(respBody)
}
