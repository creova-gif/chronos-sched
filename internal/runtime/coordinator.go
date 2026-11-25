package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

type CoordinatorConfig struct {
	LeaseTTL   time.Duration
	ReaperTick time.Duration
}

type activeRun struct {
	dag          *engine.DAG
	workflowYAML string
	state        *engine.RunState
}

// Coordinator is the single-writer scheduler process owning in-memory active run
// state, lease expiration, fencing verification, SQLite WAL persistence, and
// bounded SSE live streaming.
type Coordinator struct {
	store store.JournalStore
	cfg   CoordinatorConfig
	nowFn func() time.Time

	mu      sync.Mutex
	runs    map[string]*activeRun
	workers map[string]*WorkerInfo
	runSeq  atomic.Int64

	subMu sync.Mutex
	subs  map[string]map[chan SSEMessage]struct{}
}

func NewCoordinator(st store.JournalStore, cfg CoordinatorConfig) *Coordinator {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = engine.DefaultLeaseTTL
	}
	if cfg.ReaperTick <= 0 {
		cfg.ReaperTick = engine.DefaultReaperTick
	}
	return &Coordinator{
		store:   st,
		cfg:     cfg,
		nowFn:   time.Now,
		runs:    make(map[string]*activeRun),
		workers: make(map[string]*WorkerInfo),
		subs:    make(map[string]map[chan SSEMessage]struct{}),
	}
}

// Recover reloads all non-terminal runs from JournalStore via engine.Fold and
// grants a bounded startup grace window (cfg.LeaseTTL) to inherited active leases
// so surviving workers can reconnect and renew without false-positive expiry.
func (c *Coordinator) Recover(ctx context.Context) error {
	records, err := c.store.ListRuns(ctx, engine.RunStatusRunning)
	if err != nil {
		return fmt.Errorf("list running runs for recovery: %w", err)
	}
	now := c.nowFn()
	graceExpiry := now.Add(c.cfg.LeaseTTL).UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rec := range records {
		dag, err := engine.ParseDAG([]byte(rec.WorkflowYAML))
		if err != nil {
			return fmt.Errorf("parse recovered workflow for run %q: %w", rec.RunID, err)
		}
		events, err := c.store.LoadRunEvents(ctx, rec.RunID)
		if err != nil {
			return fmt.Errorf("load recovered events for run %q: %w", rec.RunID, err)
		}
		state, err := engine.Fold(dag, events)
		if err != nil {
			return fmt.Errorf("fold recovered run %q: %w", rec.RunID, err)
		}
		for _, ts := range state.Tasks {
			if ts.Status == engine.TaskStatusLeased || ts.Status == engine.TaskStatusRunning {
				ts.LeaseExpiresAtNs = graceExpiry
			}
		}
		c.runs[rec.RunID] = &activeRun{
			dag:          dag,
			workflowYAML: rec.WorkflowYAML,
			state:        state,
		}
	}
	return nil
}

// StartReaper runs the 250ms lease expiration loop until ctx is cancelled.
func (c *Coordinator) StartReaper(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.ReaperTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.ReapExpiredLeases(ctx)
		}
	}
}

func (c *Coordinator) ReapExpiredLeases(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.nowFn()
	// Mark stale workers offline in snapshot view.
	for _, w := range c.workers {
		if now.UnixNano()-w.LastHeartbeat > c.cfg.LeaseTTL.Nanoseconds() {
			w.Alive = false
		}
	}

	var firstErr error
	for _, ar := range c.runs {
		if ar.state.Status != engine.RunStatusRunning {
			continue
		}
		prevSeq := ar.state.LastSeq
		evs, _, err := engine.Step(ar.dag, ar.state, engine.CmdExpireLeases{}, now)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(evs) == 0 {
			continue
		}
		if err := c.commitEventsLocked(ctx, ar, prevSeq, evs); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Coordinator) SubmitWorkflow(ctx context.Context, req SubmitWorkflowRequest) (SubmitWorkflowResponse, error) {
	dag, err := engine.ParseDAG([]byte(req.WorkflowYAML))
	if err != nil {
		return SubmitWorkflowResponse{}, err
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		runID = fmt.Sprintf("run_%03d_%d", c.runSeq.Add(1), c.nowFn().UnixNano()%1_000_000)
	}

	now := c.nowFn()
	state := engine.NewRunState(dag, runID)
	evs, _, err := engine.Step(dag, state, engine.CmdStartRun{RunID: runID}, now)
	if err != nil {
		return SubmitWorkflowResponse{}, err
	}
	for _, ev := range evs {
		if err := engine.Apply(state, ev); err != nil {
			return SubmitWorkflowResponse{}, err
		}
	}

	rec := store.RunRecord{
		RunID:        runID,
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
		WorkflowYAML: req.WorkflowYAML,
		Status:       state.Status,
		CreatedAtNs:  now.UnixNano(),
	}
	if err := c.store.CreateRun(ctx, rec, evs); err != nil {
		return SubmitWorkflowResponse{}, err
	}

	c.mu.Lock()
	c.runs[runID] = &activeRun{dag: dag, workflowYAML: req.WorkflowYAML, state: state}
	c.mu.Unlock()

	c.broadcast(runID, evs, state.Status)
	return SubmitWorkflowResponse{
		RunID:        runID,
		WorkflowName: dag.Spec.Name,
		TaskCount:    len(dag.TopoOrder),
	}, nil
}

func (c *Coordinator) ForkRun(ctx context.Context, parentRunID string, req ForkRunRequest) (ForkRunResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ar, err := c.loadRunLocked(ctx, parentRunID)
	if err != nil {
		return ForkRunResponse{}, err
	}
	newRunID := strings.TrimSpace(req.NewRunID)
	if newRunID == "" {
		newRunID = fmt.Sprintf("run_%03d_%d", c.runSeq.Add(1), c.nowFn().UnixNano()%1_000_000)
	}
	cp, err := engine.ResolveCheckpoint(ar.state, req.Checkpoint)
	if err != nil {
		return ForkRunResponse{}, err
	}

	now := c.nowFn()
	forkEvs, _, err := engine.Step(ar.dag, ar.state, engine.CmdForkRun{
		NewRunID:           newRunID,
		CheckpointSelector: req.Checkpoint,
		Overrides:          req.Overrides,
	}, now)
	if err != nil {
		return ForkRunResponse{}, err
	}

	rec := store.RunRecord{
		RunID:        newRunID,
		ParentRunID:  parentRunID,
		ForkFromSeq:  cp.Seq,
		WorkflowName: ar.dag.Spec.Name,
		WorkflowHash: ar.dag.WorkflowHash,
		WorkflowYAML: ar.workflowYAML,
		Status:       engine.RunStatusRunning,
		CreatedAtNs:  now.UnixNano(),
	}
	if err := c.store.CreateRun(ctx, rec, forkEvs); err != nil {
		return ForkRunResponse{}, err
	}

	allEvs, err := c.store.LoadRunEvents(ctx, newRunID)
	if err != nil {
		return ForkRunResponse{}, fmt.Errorf("reload forked run events: %w", err)
	}
	forkState, err := engine.Fold(ar.dag, allEvs)
	if err != nil {
		return ForkRunResponse{}, fmt.Errorf("fold forked run: %w", err)
	}
	c.runs[newRunID] = &activeRun{
		dag:          ar.dag,
		workflowYAML: ar.workflowYAML,
		state:        forkState,
	}
	c.broadcast(newRunID, forkEvs, forkState.Status)

	return ForkRunResponse{
		RunID:         newRunID,
		ParentRunID:   parentRunID,
		ForkFromSeq:   cp.Seq,
		CheckpointNum: cp.Number,
	}, nil
}

func (c *Coordinator) Snapshot(ctx context.Context, runID string) (RunSnapshotResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ar, err := c.loadRunLocked(ctx, runID)
	if err != nil {
		return RunSnapshotResponse{}, err
	}
	now := c.nowFn().UnixNano()
	workers := make([]WorkerInfo, 0, len(c.workers))
	for _, w := range c.workers {
		cp := *w
		cp.Resources = cloneResources(w.Resources)
		cp.Alive = now-w.LastHeartbeat <= c.cfg.LeaseTTL.Nanoseconds()
		workers = append(workers, cp)
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].WorkerID < workers[j].WorkerID })

	// Deep copy state via JSON so callers cannot race with mutations.
	raw, err := json.Marshal(ar.state)
	if err != nil {
		return RunSnapshotResponse{}, err
	}
	var stateCopy engine.RunState
	if err := json.Unmarshal(raw, &stateCopy); err != nil {
		return RunSnapshotResponse{}, err
	}
	return RunSnapshotResponse{State: &stateCopy, Workers: workers}, nil
}

func (c *Coordinator) loadRunLocked(ctx context.Context, runID string) (*activeRun, error) {
	if ar, ok := c.runs[runID]; ok {
		return ar, nil
	}
	rec, err := c.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	dag, err := engine.ParseDAG([]byte(rec.WorkflowYAML))
	if err != nil {
		return nil, err
	}
	events, err := c.store.LoadRunEvents(ctx, runID)
	if err != nil {
		return nil, err
	}
	state, err := engine.Fold(dag, events)
	if err != nil {
		return nil, err
	}
	ar := &activeRun{dag: dag, workflowYAML: rec.WorkflowYAML, state: state}
	if state.Status == engine.RunStatusRunning {
		c.runs[runID] = ar
	}
	return ar, nil
}

func (c *Coordinator) commitEventsLocked(ctx context.Context, ar *activeRun, prevSeq int64, evs []engine.Event) error {
	// Preview new status without mutating ar.state until SQLite WAL commit succeeds.
	nextStatus := ar.state.Status
	for _, ev := range evs {
		switch ev.Type {
		case engine.EventRunCompleted:
			nextStatus = engine.RunStatusCompleted
		case engine.EventRunFailed:
			nextStatus = engine.RunStatusFailed
		}
	}
	if err := c.store.Append(ctx, ar.state.RunID, prevSeq, evs, nextStatus); err != nil {
		return err
	}
	for _, ev := range evs {
		if err := engine.Apply(ar.state, ev); err != nil {
			return err
		}
	}
	c.broadcast(ar.state.RunID, evs, ar.state.Status)
	return nil
}

func (c *Coordinator) subscribe(runID string) (chan SSEMessage, func()) {
	ch := make(chan SSEMessage, 256)
	c.subMu.Lock()
	if c.subs[runID] == nil {
		c.subs[runID] = make(map[chan SSEMessage]struct{})
	}
	c.subs[runID][ch] = struct{}{}
	c.subMu.Unlock()
	return ch, func() {
		c.subMu.Lock()
		delete(c.subs[runID], ch)
		if len(c.subs[runID]) == 0 {
			delete(c.subs, runID)
		}
		c.subMu.Unlock()
	}
}

func (c *Coordinator) broadcast(runID string, evs []engine.Event, status engine.RunStatus) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	for ch := range c.subs[runID] {
		for _, ev := range evs {
			select {
			case ch <- SSEMessage{Event: ev, RunStatus: status}:
			default:
				// Bounded non-blocking send: slow TUI subscriber never stalls coordinator.
			}
		}
	}
}

// Handler returns the Coordinator's net/http handler.
func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/workflows/run", c.handleSubmitWorkflow)
	mux.HandleFunc("GET /v1/runs", c.handleListRuns)
	mux.HandleFunc("GET /v1/runs/{id}", c.handleGetRun)
	mux.HandleFunc("GET /v1/runs/{id}/events", c.handleRunEventsSSE)
	mux.HandleFunc("POST /v1/runs/{id}/fork", c.handleForkRun)
	mux.HandleFunc("POST /v1/workers/register", c.handleWorkerRegister)
	mux.HandleFunc("POST /v1/workers/poll", c.handleWorkerPoll)
	mux.HandleFunc("POST /v1/workers/heartbeat", c.handleWorkerHeartbeat)
	mux.HandleFunc("POST /v1/tasks/started", c.handleTaskStarted)
	mux.HandleFunc("POST /v1/tasks/complete", c.handleTaskComplete)
	mux.HandleFunc("POST /v1/tasks/fail", c.handleTaskFail)
	return mux
}

func (c *Coordinator) handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
	var req SubmitWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	resp, err := c.SubmitWorkflow(r.Context(), req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (c *Coordinator) handleListRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := c.store.ListRuns(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (c *Coordinator) handleGetRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	snap, err := c.Snapshot(r.Context(), runID)
	if err != nil {
		if errors.Is(err, store.ErrRunNotFound) {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (c *Coordinator) handleForkRun(w http.ResponseWriter, r *http.Request) {
	parentID := r.PathValue("id")
	var req ForkRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	resp, err := c.ForkRun(r.Context(), parentID, req)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, store.ErrRunNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (c *Coordinator) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	var req WorkerRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	if req.WorkerID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "worker_id is required"})
		return
	}
	c.mu.Lock()
	now := c.nowFn()
	res := cloneResources(req.Resources)
	if len(res) == 0 {
		if prev, ok := c.workers[req.WorkerID]; ok {
			res = cloneResources(prev.Resources)
		}
	}
	c.workers[req.WorkerID] = &WorkerInfo{
		WorkerID:      req.WorkerID,
		PID:           req.PID,
		Concurrency:   req.Concurrency,
		Resources:     res,
		LastHeartbeat: now.UnixNano(),
		Alive:         true,
	}
	c.renewLeasesLocked(req.WorkerID, req.ActiveLeases, now)
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, WorkerRegisterResponse{OK: true})
}

func (c *Coordinator) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	if req.WorkerID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "worker_id is required"})
		return
	}
	c.mu.Lock()
	now := c.nowFn()
	res := cloneResources(req.Resources)
	if len(res) == 0 {
		if prev, ok := c.workers[req.WorkerID]; ok {
			res = cloneResources(prev.Resources)
		}
	}
	c.workers[req.WorkerID] = &WorkerInfo{
		WorkerID:      req.WorkerID,
		PID:           req.PID,
		Concurrency:   req.Concurrency,
		Resources:     res,
		LastHeartbeat: now.UnixNano(),
		Alive:         true,
	}
	renewed, revoked := c.renewLeasesLocked(req.WorkerID, req.ActiveLeases, now)
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, HeartbeatResponse{Renewed: renewed, Revoked: revoked})
}

func (c *Coordinator) renewLeasesLocked(workerID string, leases []ActiveLease, now time.Time) (int, []ActiveLease) {
	newExpiry := now.Add(c.cfg.LeaseTTL).UnixNano()
	renewed := 0
	var revoked []ActiveLease
	for _, al := range leases {
		ar, ok := c.runs[al.RunID]
		if !ok || ar.state.Status != engine.RunStatusRunning {
			revoked = append(revoked, al)
			continue
		}
		ts, ok := ar.state.Tasks[al.TaskID]
		if !ok || (ts.Status != engine.TaskStatusLeased && ts.Status != engine.TaskStatusRunning) || ts.WorkerID != workerID || ts.FencingToken != al.FencingToken {
			revoked = append(revoked, al)
			continue
		}
		ts.LeaseExpiresAtNs = newExpiry
		renewed++
	}
	return renewed, revoked
}

func (c *Coordinator) handleWorkerPoll(w http.ResponseWriter, r *http.Request) {
	var req LeasePollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	if req.WorkerID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "worker_id is required"})
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.nowFn()
	var declaredResources map[string]int
	if wi, ok := c.workers[req.WorkerID]; ok {
		wi.LastHeartbeat = now.UnixNano()
		wi.Alive = true
		if len(req.Resources) > 0 {
			wi.Resources = cloneResources(req.Resources)
		}
		declaredResources = wi.Resources
	} else if len(req.Resources) > 0 {
		declaredResources = req.Resources
	}
	availableResources := c.availableWorkerResourcesLocked(req.WorkerID, declaredResources)

	// Deterministic run iteration order.
	runIDs := make([]string, 0, len(c.runs))
	for id, ar := range c.runs {
		if ar.state.Status == engine.RunStatusRunning {
			runIDs = append(runIDs, id)
		}
	}
	sort.Strings(runIDs)

	for _, id := range runIDs {
		ar := c.runs[id]
		prevSeq := ar.state.LastSeq
		evs, _, err := engine.Step(ar.dag, ar.state, engine.CmdLeaseNextTask{
			WorkerID:           req.WorkerID,
			LeaseTTL:           c.cfg.LeaseTTL,
			AvailableResources: availableResources,
		}, now)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		if len(evs) == 0 {
			continue
		}
		if err := c.commitEventsLocked(r.Context(), ar, prevSeq, evs); err != nil {
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		ev := evs[0]
		spec := ar.dag.TasksByID[ev.TaskID]
		writeJSON(w, http.StatusOK, LeasePollResponse{
			Lease: &LeasedTask{
				RunID:        id,
				TaskID:       ev.TaskID,
				Handler:      spec.Handler,
				Attempt:      ev.Attempt,
				FencingToken: ev.FencingToken,
				Input:        engine.ResolveTaskInput(ar.dag, ar.state, ev.TaskID),
				TimeoutMs:    spec.TimeoutMs,
			},
		})
		return
	}
	writeJSON(w, http.StatusOK, LeasePollResponse{})
}

func (c *Coordinator) handleTaskStarted(w http.ResponseWriter, r *http.Request) {
	var req TaskStartedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	c.applyTaskCommand(w, r, req.RunID, engine.CmdMarkTaskStarted{
		TaskID:       req.TaskID,
		WorkerID:     req.WorkerID,
		FencingToken: req.FencingToken,
	})
}

func (c *Coordinator) handleTaskComplete(w http.ResponseWriter, r *http.Request) {
	var req TaskCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	c.applyTaskCommand(w, r, req.RunID, engine.CmdCompleteTask{
		TaskID:       req.TaskID,
		WorkerID:     req.WorkerID,
		FencingToken: req.FencingToken,
		Output:       req.Output,
	})
}

func (c *Coordinator) handleTaskFail(w http.ResponseWriter, r *http.Request) {
	var req TaskFailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	c.applyTaskCommand(w, r, req.RunID, engine.CmdFailTask{
		TaskID:       req.TaskID,
		WorkerID:     req.WorkerID,
		FencingToken: req.FencingToken,
		ErrMsg:       req.Error,
	})
}

func (c *Coordinator) applyTaskCommand(w http.ResponseWriter, r *http.Request, runID string, cmd engine.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ar, err := c.loadRunLocked(r.Context(), runID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrRunNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, ErrorResponse{Error: err.Error()})
		return
	}

	prevSeq := ar.state.LastSeq
	evs, _, stepErr := engine.Step(ar.dag, ar.state, cmd, c.nowFn())
	if len(evs) > 0 {
		if err := c.commitEventsLocked(r.Context(), ar, prevSeq, evs); err != nil {
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
	}
	if stepErr != nil {
		if errors.Is(stepErr, engine.ErrStaleFencingToken) {
			writeJSON(w, http.StatusConflict, ErrorResponse{Error: stepErr.Error()})
			return
		}
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: stepErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (c *Coordinator) handleRunEventsSSE(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "streaming unsupported"})
		return
	}

	// Subscribe BEFORE loading historical events so no events are missed.
	ch, unsub := c.subscribe(runID)
	defer unsub()

	history, err := c.store.LoadRunEvents(r.Context(), runID)
	if err != nil {
		if errors.Is(err, store.ErrRunNotFound) {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	var lastSeq int64
	for _, ev := range history {
		b, _ := json.Marshal(SSEMessage{Event: ev})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
		lastSeq = ev.Seq
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			if msg.Event.Seq <= lastSeq {
				continue
			}
			lastSeq = msg.Event.Seq
			b, _ := json.Marshal(msg)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func (c *Coordinator) availableWorkerResourcesLocked(workerID string, declared map[string]int) map[string]int {
	if len(declared) == 0 {
		return nil
	}
	avail := cloneResources(declared)
	for _, ar := range c.runs {
		if ar.state.Status != engine.RunStatusRunning {
			continue
		}
		for taskID, ts := range ar.state.Tasks {
			if ts.WorkerID != workerID {
				continue
			}
			if ts.Status != engine.TaskStatusLeased && ts.Status != engine.TaskStatusRunning {
				continue
			}
			spec, ok := ar.dag.TasksByID[taskID]
			if !ok {
				continue
			}
			for k, reqAmt := range spec.Resources {
				avail[k] -= reqAmt
			}
		}
	}
	return avail
}

func cloneResources(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
