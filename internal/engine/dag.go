package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// ParseDAG parses a YAML workflow definition, validates all structural and
// dependency invariants, and computes a deterministic topological order.
func ParseDAG(yamlBytes []byte) (*DAG, error) {
	if len(yamlBytes) == 0 {
		return nil, fmt.Errorf("workflow YAML is empty")
	}

	var spec WorkflowSpec
	if err := yaml.Unmarshal(yamlBytes, &spec); err != nil {
		return nil, fmt.Errorf("parse workflow YAML: %w", err)
	}
	return BuildDAG(spec, yamlBytes)
}

// BuildDAG validates a WorkflowSpec and builds an indexed, topologically sorted DAG.
func BuildDAG(spec WorkflowSpec, rawSource []byte) (*DAG, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}
	if len(spec.Tasks) == 0 {
		return nil, fmt.Errorf("workflow %q must define at least one task", spec.Name)
	}

	tasksByID := make(map[string]TaskSpec, len(spec.Tasks))
	dependents := make(map[string][]string, len(spec.Tasks))
	inDegree := make(map[string]int, len(spec.Tasks))
	seenCheckpoints := make(map[string]string)

	for _, t := range spec.Tasks {
		if t.ID == "" {
			return nil, fmt.Errorf("task id cannot be empty")
		}
		if t.Handler == "" {
			return nil, fmt.Errorf("task %q must define a handler", t.ID)
		}
		if t.MaxRetries < 0 {
			return nil, fmt.Errorf("task %q has negative max_retries %d", t.ID, t.MaxRetries)
		}
		if t.TimeoutMs < 0 {
			return nil, fmt.Errorf("task %q has negative timeout_ms %d", t.ID, t.TimeoutMs)
		}
		if _, exists := tasksByID[t.ID]; exists {
			return nil, fmt.Errorf("duplicate task id %q", t.ID)
		}
		if t.CheckpointName != "" {
			if prev, exists := seenCheckpoints[t.CheckpointName]; exists {
				return nil, fmt.Errorf("duplicate checkpoint_name %q on tasks %q and %q", t.CheckpointName, prev, t.ID)
			}
			seenCheckpoints[t.CheckpointName] = t.ID
		}
		tasksByID[t.ID] = t
		inDegree[t.ID] = 0
	}

	for _, t := range spec.Tasks {
		seenDep := make(map[string]struct{}, len(t.DependsOn))
		for _, dep := range t.DependsOn {
			if dep == t.ID {
				return nil, fmt.Errorf("task %q cannot depend on itself", t.ID)
			}
			if _, ok := tasksByID[dep]; !ok {
				return nil, fmt.Errorf("task %q depends on unknown task %q", t.ID, dep)
			}
			if _, dup := seenDep[dep]; dup {
				continue
			}
			seenDep[dep] = struct{}{}
			dependents[dep] = append(dependents[dep], t.ID)
			inDegree[t.ID]++
		}
	}

	// Deterministic Kahn's algorithm: highest priority first, tie-break by task ID asc.
	var ready []string
	for id, deg := range inDegree {
		if deg == 0 {
			ready = append(ready, id)
		}
	}
	sortReady := func(ids []string) {
		sort.Slice(ids, func(i, j int) bool {
			ti, tj := tasksByID[ids[i]], tasksByID[ids[j]]
			if ti.Priority != tj.Priority {
				return ti.Priority > tj.Priority
			}
			return ti.ID < tj.ID
		})
	}
	sortReady(ready)

	topo := make([]string, 0, len(spec.Tasks))
	for len(ready) > 0 {
		curr := ready[0]
		ready = ready[1:]
		topo = append(topo, curr)

		added := false
		for _, next := range dependents[curr] {
			inDegree[next]--
			if inDegree[next] == 0 {
				ready = append(ready, next)
				added = true
			}
		}
		if added {
			sortReady(ready)
		}
	}

	if len(topo) != len(spec.Tasks) {
		return nil, fmt.Errorf("workflow %q contains a dependency cycle", spec.Name)
	}

	sum := sha256.Sum256(rawSource)
	return &DAG{
		Spec:         spec,
		TasksByID:    tasksByID,
		Dependents:   dependents,
		TopoOrder:    topo,
		WorkflowHash: hex.EncodeToString(sum[:]),
	}, nil
}
