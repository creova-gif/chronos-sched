package main

import (
	"flag"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

func TestParseFlagsAnyOrder(t *testing.T) {
	t.Parallel()
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	db := fs.String("db", "default.db", "")
	dry := fs.Bool("dry-run", false, "")
	if err := parseFlagsAnyOrder(fs, []string{"run_001", "--dry-run", "--db", "/tmp/custom.db"}); err != nil {
		t.Fatalf("parseFlagsAnyOrder: %v", err)
	}
	if fs.Arg(0) != "run_001" || *db != "/tmp/custom.db" || !*dry {
		t.Fatalf("unexpected parse result: arg0=%q db=%q dry=%v", fs.Arg(0), *db, *dry)
	}
}

func TestEmbeddedDemoYAMLIsValid100TaskDAG(t *testing.T) {
	t.Parallel()
	dag, err := engine.ParseDAG([]byte(embeddedDemoYAML))
	if err != nil {
		t.Fatalf("ParseDAG(embeddedDemoYAML): %v", err)
	}
	if len(dag.TopoOrder) != 100 {
		t.Fatalf("expected 100 tasks in embedded demo.yaml, got %d", len(dag.TopoOrder))
	}
}

func TestParseOverrides(t *testing.T) {
	t.Parallel()
	got, err := parseOverrides([]string{"transform.mode=fast", "transform.retries=3"})
	if err != nil {
		t.Fatalf("parseOverrides valid: %v", err)
	}
	if got["transform"]["mode"] != "fast" || got["transform"]["retries"] != "3" {
		t.Fatalf("unexpected overrides: %+v", got)
	}
	if _, err := parseOverrides([]string{"badformat"}); err == nil {
		t.Fatalf("expected error on missing '='")
	}
	if _, err := parseOverrides([]string{"nodot=val"}); err == nil {
		t.Fatalf("expected error on missing '.'")
	}
}

func TestParseResources(t *testing.T) {
	t.Parallel()
	got, err := parseResources("cpu=4, mem_mb=1024")
	if err != nil {
		t.Fatalf("parseResources valid: %v", err)
	}
	if got["cpu"] != 4 || got["mem_mb"] != 1024 {
		t.Fatalf("unexpected resources: %+v", got)
	}
	if gotNil, err := parseResources(""); err != nil || gotNil != nil {
		t.Fatalf("expected nil for empty resources, got %v err=%v", gotNil, err)
	}
	if _, err := parseResources("cpu=-1"); err == nil {
		t.Fatalf("expected error for negative resource")
	}
	if _, err := parseResources("badformat"); err == nil {
		t.Fatalf("expected error for malformed resource")
	}
}
