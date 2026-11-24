package tui

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
)

type SnapshotFetcher func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error)

type Config struct {
	RunID              string
	AllowKill          bool
	Fetch              SnapshotFetcher
	OnKillWorker       func() string
	OnFork             func(runID string, checkpointNum int) (string, error)
	OnForkWithOverride func(runID string, checkpointNum int, override string) (string, error)
	OnDiff             func(runA, runB string) (string, error)
	OnReplay           func(runID string) (string, error)
}

type tickMsg time.Time

type snapshotMsg struct {
	snap   runtime.RunSnapshotResponse
	events []engine.Event
	err    error
}

// Model implements the Bubble Tea live workflow flight-recorder inspector.
type Model struct {
	cfg               Config
	activeRun         string
	forkedRun         string
	branches          []string
	branchIdx         int
	snap              runtime.RunSnapshotResponse
	events            []engine.Event
	statusLine        string
	diffView          string
	forkModalOpen     bool
	forkSelectedIdx   int
	forkOverrideInput string
	width             int
	height            int
	quitting          bool
}

func NewModel(cfg Config) Model {
	var branches []string
	if cfg.RunID != "" {
		branches = []string{cfg.RunID}
	}
	return Model{
		cfg:        cfg,
		activeRun:  cfg.RunID,
		branches:   branches,
		statusLine: "Connected to ChronosSched coordinator.",
		width:      100,
		height:     32,
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.fetchCmd(), tickCmd())
}

func tickCmd() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m Model) fetchCmd() tea.Cmd {
	runID := m.activeRun
	fetch := m.cfg.Fetch
	return func() tea.Msg {
		if fetch == nil {
			return snapshotMsg{}
		}
		snap, evs, err := fetch(runID)
		return snapshotMsg{snap: snap, events: evs, err: err}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tickMsg:
		return m, tea.Batch(m.fetchCmd(), tickCmd())

	case snapshotMsg:
		if msg.err != nil {
			m.statusLine = fmt.Sprintf("Refresh error: %v", msg.err)
		} else if msg.snap.State != nil {
			m.snap = msg.snap
			m.events = msg.events
			if m.forkModalOpen && len(m.snap.State.Checkpoints) > 0 && m.forkSelectedIdx >= len(m.snap.State.Checkpoints) {
				m.forkSelectedIdx = len(m.snap.State.Checkpoints) - 1
			}
		}
		return m, nil

	case tea.KeyMsg:
		if m.forkModalOpen {
			return m.updateForkModal(msg)
		}

		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit

		case "k":
			if !m.cfg.AllowKill {
				m.statusLine = "[k] disabled in read-only live inspect mode (available in chronos demo)"
				return m, nil
			}
			if m.cfg.OnKillWorker != nil {
				m.statusLine = m.cfg.OnKillWorker()
			}
			return m, m.fetchCmd()

		case "f":
			if (m.cfg.OnFork == nil && m.cfg.OnForkWithOverride == nil) || m.snap.State == nil {
				return m, nil
			}
			if len(m.snap.State.Checkpoints) == 0 {
				m.statusLine = "No checkpoints saved yet; wait for at least 1 task to complete."
				return m, nil
			}
			m.forkModalOpen = true
			m.forkSelectedIdx = 0
			m.forkOverrideInput = ""
			m.diffView = ""
			return m, nil

		case "b":
			if len(m.branches) <= 1 {
				m.statusLine = "No forked branches yet; press [f] to fork a timeline first."
				return m, nil
			}
			m.branchIdx = (m.branchIdx + 1) % len(m.branches)
			m.activeRun = m.branches[m.branchIdx]
			m.statusLine = fmt.Sprintf("Switched active branch to %s (%d/%d)", m.activeRun, m.branchIdx+1, len(m.branches))
			return m, m.fetchCmd()

		case "r":
			if m.cfg.OnReplay == nil {
				m.statusLine = "Replay check unavailable."
				return m, nil
			}
			out, err := m.cfg.OnReplay(m.activeRun)
			if err != nil {
				m.statusLine = fmt.Sprintf("Replay failed: %v", err)
				return m, nil
			}
			m.statusLine = out
			return m, nil

		case "d":
			if m.diffView != "" {
				m.diffView = ""
				return m, nil
			}
			if m.cfg.OnDiff == nil || m.forkedRun == "" {
				m.statusLine = "Press [f] to fork a timeline first before running [d] diff."
				return m, nil
			}
			runA, runB := m.activeRun, m.forkedRun
			if runA == runB && m.cfg.RunID != "" && m.cfg.RunID != runB {
				runA = m.cfg.RunID
			}
			out, err := m.cfg.OnDiff(runA, runB)
			if err != nil {
				m.statusLine = fmt.Sprintf("Diff failed: %v", err)
				return m, nil
			}
			m.diffView = out
			return m, nil
		}
	}
	return m, nil
}

func (m Model) updateForkModal(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "ctrl+c" {
		m.quitting = true
		return m, tea.Quit
	}
	if msg.Type == tea.KeyEsc || msg.String() == "esc" {
		m.forkModalOpen = false
		m.forkOverrideInput = ""
		m.statusLine = "Fork cancelled."
		return m, nil
	}

	var cps []engine.CheckpointInfo
	if m.snap.State != nil {
		cps = m.snap.State.Checkpoints
	}

	switch msg.String() {
	case "up":
		if m.forkSelectedIdx > 0 {
			m.forkSelectedIdx--
		}
		return m, nil
	case "down":
		if m.forkSelectedIdx < len(cps)-1 {
			m.forkSelectedIdx++
		}
		return m, nil
	case "k":
		if m.forkOverrideInput == "" {
			if m.forkSelectedIdx > 0 {
				m.forkSelectedIdx--
			}
			return m, nil
		}
		m.forkOverrideInput += "k"
		return m, nil
	case "j":
		if m.forkOverrideInput == "" {
			if m.forkSelectedIdx < len(cps)-1 {
				m.forkSelectedIdx++
			}
			return m, nil
		}
		m.forkOverrideInput += "j"
		return m, nil
	case "backspace", "ctrl+h":
		if len(m.forkOverrideInput) > 0 {
			_, size := utf8.DecodeLastRuneInString(m.forkOverrideInput)
			m.forkOverrideInput = m.forkOverrideInput[:len(m.forkOverrideInput)-size]
		}
		return m, nil
	case "enter":
		m.forkModalOpen = false
		override := strings.TrimSpace(m.forkOverrideInput)
		m.forkOverrideInput = ""
		if len(cps) == 0 {
			m.statusLine = "No checkpoints saved yet; wait for at least 1 task to complete."
			return m, nil
		}
		idx := m.forkSelectedIdx
		if idx < 0 {
			idx = 0
		}
		if idx >= len(cps) {
			idx = len(cps) - 1
		}
		cp := cps[idx]

		var (
			newRunID string
			err      error
		)
		if m.cfg.OnForkWithOverride != nil {
			newRunID, err = m.cfg.OnForkWithOverride(m.activeRun, cp.Number, override)
		} else if m.cfg.OnFork != nil {
			newRunID, err = m.cfg.OnFork(m.activeRun, cp.Number)
		}
		if err != nil {
			m.statusLine = fmt.Sprintf("Fork failed: %v", err)
			return m, nil
		}
		m.forkedRun = newRunID
		m.addBranch(newRunID)
		if override != "" {
			m.statusLine = fmt.Sprintf("Forked %s from %s at checkpoint %d (seq %d, override=%s)", newRunID, m.activeRun, cp.Number, cp.Seq, override)
		} else {
			m.statusLine = fmt.Sprintf("Forked %s from %s at checkpoint %d (seq %d)", newRunID, m.activeRun, cp.Number, cp.Seq)
		}
		return m, m.fetchCmd()
	}

	if msg.Type == tea.KeyRunes {
		m.forkOverrideInput += string(msg.Runes)
	} else if msg.Type == tea.KeySpace {
		m.forkOverrideInput += " "
	}
	return m, nil
}

func (m *Model) addBranch(runID string) {
	if runID == "" {
		return
	}
	for _, existing := range m.branches {
		if existing == runID {
			return
		}
	}
	m.branches = append(m.branches, runID)
}

func (m Model) View() string {
	if m.quitting {
		return ""
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("86"))
	borderStyle := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
	mutedStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("244"))

	var b strings.Builder
	status := engine.RunStatusPending
	totalTasks, doneTasks, runningTasks, cpCount, ambCount := 0, 0, 0, 0, 0
	if m.snap.State != nil {
		status = m.snap.State.Status
		totalTasks = len(m.snap.State.TopoOrder)
		cpCount = len(m.snap.State.Checkpoints)
		for _, id := range m.snap.State.TopoOrder {
			ts := m.snap.State.Tasks[id]
			ambCount += ts.AmbiguousCount
			switch ts.Status {
			case engine.TaskStatusCompleted:
				doneTasks++
			case engine.TaskStatusLeased, engine.TaskStatusRunning:
				runningTasks++
			}
		}
	}
	aliveWorkers := 0
	for _, w := range m.snap.Workers {
		if w.Alive {
			aliveWorkers++
		}
	}
	branches := len(m.branches)
	if branches == 0 {
		branches = 1
	}

	header := fmt.Sprintf("CHRONOS - Workflow Inspector  [%s | %s | branches: %d]", m.activeRun, status, branches)
	b.WriteString(titleStyle.Render(header) + "\n")
	b.WriteString(fmt.Sprintf("Workers: %d/%d alive   Tasks: %d/%d done (%d active)   Checkpoints: %d   Ambiguous: %d\n",
		aliveWorkers, len(m.snap.Workers), doneTasks, totalTasks, runningTasks, cpCount, ambCount))
	if strip := renderWorkersStrip(m.snap.Workers); strip != "" {
		b.WriteString(strip + "\n")
	}

	if m.forkModalOpen {
		b.WriteString(borderStyle.Render(renderForkModal(m.snap.State, m.forkSelectedIdx, m.forkOverrideInput)))
		b.WriteString("\n")
	} else if m.diffView != "" {
		b.WriteString(borderStyle.Render("TIMELINE DIFF (press [d] to close)\n" + m.diffView))
		b.WriteString("\n")
	} else {
		left := renderDAGPane(m.snap.State)
		right := renderJournalPane(m.events)
		b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top,
			borderStyle.Width(44).Render(left),
			borderStyle.Width(52).Render(right),
		))
		b.WriteString("\n")
	}

	b.WriteString(mutedStyle.Render("Status: "+m.statusLine) + "\n")
	if m.cfg.AllowKill {
		b.WriteString(mutedStyle.Render("[k] Kill worker (SIGKILL)  [f] Fork checkpoint  [b] Switch branch  [r] Replay check  [d] Diff fork  [q] Quit") + "\n")
	} else {
		b.WriteString(mutedStyle.Render("[f] Fork checkpoint  [b] Switch branch  [r] Replay check  [d] Diff fork  [q] Quit  (read-only: [k] disabled)") + "\n")
	}
	return b.String()
}

func renderWorkersStrip(workers []runtime.WorkerInfo) string {
	if len(workers) == 0 {
		return ""
	}
	aliveStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("42"))
	deadStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196"))

	parts := make([]string, 0, len(workers))
	for _, w := range workers {
		badge := deadStyle.Render("DEAD")
		if w.Alive {
			badge = aliveStyle.Render("ALIVE")
		}
		parts = append(parts, fmt.Sprintf("%s(pid=%d)[%s]", w.WorkerID, w.PID, badge))
	}
	return "Workers Strip: " + strings.Join(parts, "  ")
}

func renderForkModal(st *engine.RunState, selectedIdx int, overrideInput string) string {
	var b strings.Builder
	b.WriteString("FORK CHECKPOINT PICKER\n")
	if st == nil || len(st.Checkpoints) == 0 {
		b.WriteString("  (no checkpoints available)\n")
	} else {
		for i, cp := range st.Checkpoints {
			cursor := "  "
			if i == selectedIdx {
				cursor = "> "
			}
			namePart := ""
			if cp.Name != "" {
				namePart = fmt.Sprintf(" name=%s", cp.Name)
			}
			fmt.Fprintf(&b, "%scp=%d%s task=%s seq=%d\n", cursor, cp.Number, namePart, cp.TaskID, cp.Seq)
		}
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "Override (task.key=value): %s_\n", overrideInput)
	b.WriteString("Controls: up/down (or j/k) select  |  type override (backspace edits)  |  enter fork  |  esc cancel")
	return b.String()
}

func renderDAGPane(st *engine.RunState) string {
	var b strings.Builder
	b.WriteString("DAG Execution\n")
	if st == nil {
		b.WriteString("  (waiting for snapshot...)")
		return b.String()
	}
	// Show active tasks first, then a slice of completed/pending tasks (up to 14 rows).
	shown := 0
	const maxRows = 14
	addRow := func(ts *engine.TaskState) {
		if shown >= maxRows {
			return
		}
		tag := "[q]  "
		switch ts.Status {
		case engine.TaskStatusCompleted:
			tag = "[ok] "
		case engine.TaskStatusRunning, engine.TaskStatusLeased:
			tag = "[run]"
		case engine.TaskStatusFailed:
			tag = "[err]"
		}
		worker := ts.WorkerID
		if worker == "" {
			worker = "wait"
		}
		fmt.Fprintf(&b, "%s %-16s (%s)\n", tag, ts.ID, worker)
		shown++
	}
	for _, id := range st.TopoOrder {
		if ts := st.Tasks[id]; ts.Status == engine.TaskStatusRunning || ts.Status == engine.TaskStatusLeased {
			addRow(ts)
		}
	}
	for _, id := range st.TopoOrder {
		if ts := st.Tasks[id]; ts.Status == engine.TaskStatusFailed {
			addRow(ts)
		}
	}
	for _, id := range st.TopoOrder {
		if ts := st.Tasks[id]; ts.Status == engine.TaskStatusPending {
			addRow(ts)
		}
	}
	for i := len(st.TopoOrder) - 1; i >= 0 && shown < maxRows; i-- {
		if ts := st.Tasks[st.TopoOrder[i]]; ts.Status == engine.TaskStatusCompleted {
			addRow(ts)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func renderJournalPane(events []engine.Event) string {
	var b strings.Builder
	b.WriteString("Event Journal\n")
	if len(events) == 0 {
		b.WriteString("  (no events yet)")
		return b.String()
	}
	start := 0
	if len(events) > 14 {
		start = len(events) - 14
	}
	for _, ev := range events[start:] {
		line := fmt.Sprintf("%03d %-21s", ev.Seq, ev.Type)
		if ev.TaskID != "" {
			line += " " + ev.TaskID
		}
		if ev.FencingToken > 0 {
			line += fmt.Sprintf(" tok=%d", ev.FencingToken)
		}
		b.WriteString(line + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}
