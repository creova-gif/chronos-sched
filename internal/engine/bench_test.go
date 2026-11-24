package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

func BenchmarkSchedule100TaskDAG(b *testing.B) {
	tasks := make([]engine.TaskSpec, 100)
	for i := 0; i < 100; i++ {
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("task_%03d", (i-1)/2)}
		}
		tasks[i] = engine.TaskSpec{
			ID:         fmt.Sprintf("task_%03d", i),
			Handler:    "sim",
			DependsOn:  deps,
			MaxRetries: 1,
		}
	}
	dag, err := engine.BuildDAG(engine.WorkflowSpec{Name: "bench-100", Tasks: tasks}, []byte("bench-100"))
	if err != nil {
		b.Fatalf("BuildDAG: %v", err)
	}
	baseTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := engine.NewRunState(dag, "run_bench")
		evs, _, err := engine.Step(dag, state, engine.CmdStartRun{RunID: "run_bench"}, baseTime)
		if err != nil {
			b.Fatal(err)
		}
		_ = engine.Apply(state, evs[0])

		step := 1
		for state.Status == engine.RunStatusRunning {
			ts := baseTime.Add(time.Duration(step) * time.Millisecond)
			leaseEvs, _, err := engine.Step(dag, state, engine.CmdLeaseNextTask{WorkerID: "w-1"}, ts)
			if err != nil || len(leaseEvs) == 0 {
				b.Fatalf("lease err=%v len=%d", err, len(leaseEvs))
			}
			_ = engine.Apply(state, leaseEvs[0])

			doneEvs, _, err := engine.Step(dag, state, engine.CmdCompleteTask{
				TaskID:       leaseEvs[0].TaskID,
				WorkerID:     "w-1",
				FencingToken: leaseEvs[0].FencingToken,
				Output:       map[string]any{"ok": true},
			}, ts.Add(100*time.Microsecond))
			if err != nil {
				b.Fatal(err)
			}
			for _, ev := range doneEvs {
				_ = engine.Apply(state, ev)
			}
			step++
		}
	}
}
