package runtime

import (
	"net/http"
	"sort"
)

// ListWorkersResponse is the JSON payload returned by GET /v1/workers.
type ListWorkersResponse struct {
	Workers []WorkerInfo `json:"workers"`
}

// ListWorkers returns a snapshot of all registered workers sorted ascending by WorkerID.
func (c *Coordinator) ListWorkers() []WorkerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]WorkerInfo, 0, len(c.workers))
	for _, w := range c.workers {
		if w == nil {
			continue
		}
		info := *w
		info.Resources = cloneResources(w.Resources)
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].WorkerID < out[j].WorkerID
	})
	return out
}

func (c *Coordinator) handleListWorkers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, ListWorkersResponse{
		Workers: c.ListWorkers(),
	})
}
