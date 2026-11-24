package tui_test

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/tui"
)

func TestTUIKillDisabledInReadOnlyInspectAndEnabledInDemo(t *testing.T) {
	t.Parallel()

	killed := false
	roModel := tui.NewModel(tui.Config{
		RunID:     "run_001",
		AllowKill: false,
		OnKillWorker: func() string {
			killed = true
			return "killed"
		},
	})
	next, _ := roModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	if killed {
		t.Fatalf("expected [k] to be disabled in read-only inspect mode")
	}
	view := next.(tui.Model).View()
	if !strings.Contains(view, "read-only") {
		t.Fatalf("expected read-only indicator in view:\n%s", view)
	}

	demoModel := tui.NewModel(tui.Config{
		RunID:     "run_001",
		AllowKill: true,
		OnKillWorker: func() string {
			killed = true
			return "Killed w-1"
		},
		Fetch: func(_ string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			return runtime.RunSnapshotResponse{}, nil, nil
		},
	})
	nextDemo, _ := demoModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	if !killed {
		t.Fatalf("expected [k] to invoke OnKillWorker in demo mode")
	}
	demoView := nextDemo.(tui.Model).View()
	if !strings.Contains(demoView, "Killed w-1") {
		t.Fatalf("expected kill status in demo view:\n%s", demoView)
	}
}
