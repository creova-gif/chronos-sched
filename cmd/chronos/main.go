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
	"github.com/creova-gif/chronos-sched/internal/ha"
	"github.com/creova-gif/chronos-sched/internal/runtime"
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
		default:
			return fmt.Errorf("unknown workflow subcommand %q", args[1])
		}
	case "history":
		return cmdHistory(args[1:])
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
  chronos server start [--addr 127.0.0.1:7420] [--db .chronos/chronos.db] [--ha-id node-1] [--ha-ttl 5s]
  chronos worker start [--coordinator http://127.0.0.1:7420] [--concurrency 8] [--worker-id id] [--resources cpu=4,mem_mb=1024]
  chronos workflow run <file.yaml> [--coordinator http://127.0.0.1:7420] [--run-id id]
  chronos workflow inspect <run_id> [--live] [--coordinator http://127.0.0.1:7420]
  chronos history <run_id> [--db .chronos/chronos.db]
  chronos replay <run_id> [--dry-run] [--db .chronos/chronos.db]
  chronos fork <run_id> --checkpoint <N|name> [--override task.key=value]... [--coordinator http://127.0.0.1:7420]
  chronos diff <run_a> <run_b> [--db .chronos/chronos.db]
  chronos branches [--db .chronos/chronos.db]
  chronos merge <base_run> <branch_run> [--task id]... [--prefer-branch] [--db .chronos/chronos.db]
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
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveOnce := func(runCtx context.Context, epoch int64) error {
		st, err := sqlite.Open(*dbPath)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()

		coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
			LeaseTTL:   engine.DefaultLeaseTTL,
			ReaperTick: engine.DefaultReaperTick,
		})
		if err := coord.Recover(runCtx); err != nil {
			return fmt.Errorf("recover coordinator state: %w", err)
		}
		go coord.StartReaper(runCtx)

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
	})
	_, err := tea.NewProgram(model, tea.WithAltScreen()).Run()
	return err
}

func cmdHistory(args []string) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: chronos history <run_id> [--db %s]", defaultDBPath)
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
	for _, ev := range evs {
		fmt.Printf("%04d  %-22s  task=%-14s  att=%d  tok=%d  worker=%-10s  %s\n",
			ev.Seq, ev.Type, ev.TaskID, ev.Attempt, ev.FencingToken, ev.WorkerID, string(ev.Payload))
	}
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
	dbPath := fs.String("db", defaultDBPath, "SQLite WAL journal path")
	preferBranch := fs.Bool("prefer-branch", false, "resolve conflicting outputs by preferring the branch timeline")
	var tasks multiFlag
	fs.Var(&tasks, "task", "specific task ID to cherry-pick from branch (repeatable; default all diverging completed tasks)")
	if err := parseFlagsAnyOrder(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: chronos merge <base_run> <branch_run> [--task id]... [--prefer-branch] [--db %s]", defaultDBPath)
	}
	baseID, branchID := fs.Arg(0), fs.Arg(1)

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
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(raw))
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

func rpcGet(url string, respBody any) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
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
