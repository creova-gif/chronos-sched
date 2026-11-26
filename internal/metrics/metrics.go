package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

type latencyBucket struct {
	upperBoundMs float64
	label        string
}

var latencyBuckets = [...]latencyBucket{
	{upperBoundMs: 0.25, label: "0.25"},
	{upperBoundMs: 0.5, label: "0.5"},
	{upperBoundMs: 1, label: "1"},
	{upperBoundMs: 2.5, label: "2.5"},
	{upperBoundMs: 5, label: "5"},
	{upperBoundMs: 10, label: "10"},
	{upperBoundMs: 25, label: "25"},
	{upperBoundMs: 50, label: "50"},
	{upperBoundMs: 100, label: "100"},
}

// Recorder collects scheduler counters and latency histograms using only the Go
// standard library and renders them in Prometheus text exposition format 0.0.4.
// The zero value is ready for use.
type Recorder struct {
	runsSubmitted       atomic.Uint64
	tasksLeased         atomic.Uint64
	tasksCompleted      atomic.Uint64
	tasksFailed         atomic.Uint64
	leasesExpired       atomic.Uint64
	ambiguousOutcomes   atomic.Uint64
	staleFencingTokens  atomic.Uint64
	timelinesForked     atomic.Uint64
	timelinesMerged     atomic.Uint64
	deadLettersRecorded atomic.Int64
	workersDrained      atomic.Int64
	rateLimited         atomic.Int64
	tenantDenied        atomic.Int64

	histMu              sync.RWMutex
	latencyBucketCounts [len(latencyBuckets)]uint64
	latencyCount        uint64
	latencySumMs        float64
}

// NewRecorder returns an initialized metrics Recorder.
func NewRecorder() *Recorder {
	return &Recorder{}
}

// IncRunsSubmitted increments chronos_runs_submitted_total.
func (r *Recorder) IncRunsSubmitted() {
	if r == nil {
		return
	}
	r.runsSubmitted.Add(1)
}

// IncTasksLeased increments chronos_tasks_leased_total.
func (r *Recorder) IncTasksLeased() {
	if r == nil {
		return
	}
	r.tasksLeased.Add(1)
}

// IncTasksCompleted increments chronos_tasks_completed_total.
func (r *Recorder) IncTasksCompleted() {
	if r == nil {
		return
	}
	r.tasksCompleted.Add(1)
}

// IncTasksFailed increments chronos_tasks_failed_total.
func (r *Recorder) IncTasksFailed() {
	if r == nil {
		return
	}
	r.tasksFailed.Add(1)
}

// IncLeasesExpired increments chronos_leases_expired_total.
func (r *Recorder) IncLeasesExpired() {
	if r == nil {
		return
	}
	r.leasesExpired.Add(1)
}

// IncAmbiguousOutcomes increments chronos_ambiguous_outcomes_total.
func (r *Recorder) IncAmbiguousOutcomes() {
	if r == nil {
		return
	}
	r.ambiguousOutcomes.Add(1)
}

// IncStaleFencingTokens increments chronos_stale_fencing_tokens_total.
func (r *Recorder) IncStaleFencingTokens() {
	if r == nil {
		return
	}
	r.staleFencingTokens.Add(1)
}

// IncTimelinesForked increments chronos_timelines_forked_total.
func (r *Recorder) IncTimelinesForked() {
	if r == nil {
		return
	}
	r.timelinesForked.Add(1)
}

// IncTimelinesMerged increments chronos_timelines_merged_total.
func (r *Recorder) IncTimelinesMerged() {
	if r == nil {
		return
	}
	r.timelinesMerged.Add(1)
}

// IncDeadLettersRecorded increments chronos_dead_letters_recorded_total.
func (r *Recorder) IncDeadLettersRecorded() {
	if r == nil {
		return
	}
	r.deadLettersRecorded.Add(1)
}

// IncWorkersDrained increments chronos_workers_drained_total.
func (r *Recorder) IncWorkersDrained() {
	if r == nil {
		return
	}
	r.workersDrained.Add(1)
}

// IncRateLimited increments chronos_rate_limited_total.
func (r *Recorder) IncRateLimited() {
	if r == nil {
		return
	}
	r.rateLimited.Add(1)
}

// IncTenantDenied increments chronos_tenant_denied_total.
func (r *Recorder) IncTenantDenied() {
	if r == nil {
		return
	}
	r.tenantDenied.Add(1)
}

// ObserveScheduleLatency records a scheduling decision duration into the
// chronos_schedule_latency_ms histogram. Negative durations are clamped to 0.
func (r *Recorder) ObserveScheduleLatency(d time.Duration) {
	if r == nil {
		return
	}
	ms := float64(d) / float64(time.Millisecond)
	if ms < 0 {
		ms = 0
	}

	r.histMu.Lock()
	r.latencyCount++
	r.latencySumMs += ms
	for i, b := range latencyBuckets {
		if ms <= b.upperBoundMs {
			r.latencyBucketCounts[i]++
			break
		}
	}
	r.histMu.Unlock()
}

// RenderPrometheus formats all metrics in Prometheus text exposition format 0.0.4.
func (r *Recorder) RenderPrometheus() string {
	if r == nil {
		return ""
	}

	var sb strings.Builder

	writeCounter(&sb, "chronos_runs_submitted_total", "Total number of DAG runs submitted.", r.runsSubmitted.Load())
	writeCounter(&sb, "chronos_tasks_leased_total", "Total number of task leases granted to workers.", r.tasksLeased.Load())
	writeCounter(&sb, "chronos_tasks_completed_total", "Total number of tasks completed successfully.", r.tasksCompleted.Load())
	writeCounter(&sb, "chronos_tasks_failed_total", "Total number of tasks that terminated in failure.", r.tasksFailed.Load())
	writeCounter(&sb, "chronos_leases_expired_total", "Total number of task leases that expired before completion.", r.leasesExpired.Load())
	writeCounter(&sb, "chronos_ambiguous_outcomes_total", "Total number of task attempts that ended with ambiguous outcomes.", r.ambiguousOutcomes.Load())
	writeCounter(&sb, "chronos_stale_fencing_tokens_total", "Total number of worker reports rejected due to stale fencing tokens.", r.staleFencingTokens.Load())
	writeCounter(&sb, "chronos_timelines_forked_total", "Total number of timelines forked.", r.timelinesForked.Load())
	writeCounter(&sb, "chronos_timelines_merged_total", "Total number of timelines merged.", r.timelinesMerged.Load())
	writeCounter(&sb, "chronos_dead_letters_recorded_total", "Total number of dead-lettered tasks recorded.", uint64(r.deadLettersRecorded.Load()))
	writeCounter(&sb, "chronos_workers_drained_total", "Total number of workers drained.", uint64(r.workersDrained.Load()))
	writeCounter(&sb, "chronos_rate_limited_total", "Total number of requests rejected due to rate limiting.", uint64(r.rateLimited.Load()))
	writeCounter(&sb, "chronos_tenant_denied_total", "Total number of requests denied due to tenant authorization.", uint64(r.tenantDenied.Load()))

	r.histMu.RLock()
	bucketCounts := r.latencyBucketCounts
	totalCount := r.latencyCount
	totalSum := r.latencySumMs
	r.histMu.RUnlock()

	sb.WriteString("# HELP chronos_schedule_latency_ms Scheduling decision latency in milliseconds.\n")
	sb.WriteString("# TYPE chronos_schedule_latency_ms histogram\n")
	var cumulative uint64
	for i, b := range latencyBuckets {
		cumulative += bucketCounts[i]
		fmt.Fprintf(&sb, "chronos_schedule_latency_ms_bucket{le=\"%s\"} %d\n", b.label, cumulative)
	}
	fmt.Fprintf(&sb, "chronos_schedule_latency_ms_bucket{le=\"+Inf\"} %d\n", totalCount)
	fmt.Fprintf(&sb, "chronos_schedule_latency_ms_sum %.12g\n", totalSum)
	fmt.Fprintf(&sb, "chronos_schedule_latency_ms_count %d\n", totalCount)

	return sb.String()
}

// Handler returns an http.Handler serving Prometheus metrics at text/plain; version=0.0.4.
func (r *Recorder) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", prometheusContentType)
		w.WriteHeader(http.StatusOK)
		if req.Method == http.MethodHead {
			return
		}
		if _, err := w.Write([]byte(r.RenderPrometheus())); err != nil {
			return
		}
	})
}

func writeCounter(sb *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(sb, "# HELP %s %s\n", name, help)
	fmt.Fprintf(sb, "# TYPE %s counter\n", name)
	fmt.Fprintf(sb, "%s %d\n", name, value)
}
