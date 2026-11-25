package metrics

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecorderCountersAndPrometheusFormat(t *testing.T) {
	rec := NewRecorder()

	for range 1 {
		rec.IncRunsSubmitted()
	}
	for range 2 {
		rec.IncTasksLeased()
	}
	for range 3 {
		rec.IncTasksCompleted()
	}
	for range 4 {
		rec.IncTasksFailed()
	}
	for range 5 {
		rec.IncLeasesExpired()
	}
	for range 6 {
		rec.IncAmbiguousOutcomes()
	}
	for range 7 {
		rec.IncStaleFencingTokens()
	}
	for range 8 {
		rec.IncTimelinesForked()
	}
	for range 9 {
		rec.IncTimelinesMerged()
	}

	out := rec.RenderPrometheus()

	expectedLines := []string{
		"# TYPE chronos_runs_submitted_total counter",
		"chronos_runs_submitted_total 1",
		"# TYPE chronos_tasks_leased_total counter",
		"chronos_tasks_leased_total 2",
		"# TYPE chronos_tasks_completed_total counter",
		"chronos_tasks_completed_total 3",
		"# TYPE chronos_tasks_failed_total counter",
		"chronos_tasks_failed_total 4",
		"# TYPE chronos_leases_expired_total counter",
		"chronos_leases_expired_total 5",
		"# TYPE chronos_ambiguous_outcomes_total counter",
		"chronos_ambiguous_outcomes_total 6",
		"# TYPE chronos_stale_fencing_tokens_total counter",
		"chronos_stale_fencing_tokens_total 7",
		"# TYPE chronos_timelines_forked_total counter",
		"chronos_timelines_forked_total 8",
		"# TYPE chronos_timelines_merged_total counter",
		"chronos_timelines_merged_total 9",
		"# TYPE chronos_schedule_latency_ms histogram",
		"chronos_schedule_latency_ms_bucket{le=\"+Inf\"} 0",
		"chronos_schedule_latency_ms_sum 0",
		"chronos_schedule_latency_ms_count 0",
	}

	for _, line := range expectedLines {
		if !strings.Contains(out, line+"\n") {
			t.Errorf("expected Prometheus output to contain %q, got:\n%s", line, out)
		}
	}
}

func TestHistogramBucketsMonotonicityAndBoundaries(t *testing.T) {
	rec := NewRecorder()

	// Observations spanning every bucket, exact boundaries, negative clamp, and >100ms overflow.
	observations := []time.Duration{
		-5 * time.Millisecond,   // clamped to 0ms -> <= 0.25
		250 * time.Microsecond,  // 0.25ms -> <= 0.25
		500 * time.Microsecond,  // 0.5ms  -> <= 0.5
		1 * time.Millisecond,    // 1ms    -> <= 1
		2500 * time.Microsecond, // 2.5ms  -> <= 2.5
		5 * time.Millisecond,    // 5ms    -> <= 5
		10 * time.Millisecond,   // 10ms   -> <= 10
		25 * time.Millisecond,   // 25ms   -> <= 25
		50 * time.Millisecond,   // 50ms   -> <= 50
		100 * time.Millisecond,  // 100ms  -> <= 100
		250 * time.Millisecond,  // 250ms  -> +Inf only
	}
	for _, d := range observations {
		rec.ObserveScheduleLatency(d)
	}

	out := rec.RenderPrometheus()
	buckets, sum, count := parseHistogram(t, out)

	expectedLabels := []string{"0.25", "0.5", "1", "2.5", "5", "10", "25", "50", "100", "+Inf"}
	expectedCumulative := []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

	if len(buckets) != len(expectedLabels) {
		t.Fatalf("expected %d buckets, got %d", len(expectedLabels), len(buckets))
	}

	var prev uint64
	for i, b := range buckets {
		if b.label != expectedLabels[i] {
			t.Errorf("bucket[%d] label = %q, want %q", i, b.label, expectedLabels[i])
		}
		if b.count < prev {
			t.Errorf("bucket[%d] (%s) count %d < previous %d (not monotonic)", i, b.label, b.count, prev)
		}
		if b.count != expectedCumulative[i] {
			t.Errorf("bucket[%d] (%s) count = %d, want %d", i, b.label, b.count, expectedCumulative[i])
		}
		prev = b.count
	}

	if count != uint64(len(observations)) {
		t.Errorf("histogram count = %d, want %d", count, len(observations))
	}
	if buckets[len(buckets)-1].count != count {
		t.Errorf("+Inf bucket = %d, want count = %d", buckets[len(buckets)-1].count, count)
	}

	// Expected sum: 0 + 0.25 + 0.5 + 1 + 2.5 + 5 + 10 + 25 + 50 + 100 + 250 = 444.25ms
	const wantSum = 444.25
	if diff := sum - wantSum; diff < -1e-6 || diff > 1e-6 {
		t.Errorf("histogram sum = %v, want %v", sum, wantSum)
	}
}

func TestRecorderConcurrentUpdates(t *testing.T) {
	rec := NewRecorder()

	const (
		goroutines = 24
		iterations = 200
	)

	var wg sync.WaitGroup
	wg.Add(goroutines + 2)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range iterations {
				rec.IncRunsSubmitted()
				rec.IncTasksLeased()
				rec.IncTasksCompleted()
				rec.IncTasksFailed()
				rec.IncLeasesExpired()
				rec.IncAmbiguousOutcomes()
				rec.IncStaleFencingTokens()
				rec.IncTimelinesForked()
				rec.IncTimelinesMerged()
				rec.ObserveScheduleLatency(time.Duration((id+i)%120) * time.Millisecond)
			}
		}(g)
	}

	// Concurrent readers verifying snapshot monotonicity under contention.
	for range 2 {
		go func() {
			defer wg.Done()
			for range 100 {
				snap := rec.RenderPrometheus()
				buckets, _, count := parseHistogram(t, snap)
				if len(buckets) != len(latencyBuckets)+1 {
					t.Errorf("unexpected bucket count %d", len(buckets))
					return
				}
				var prev uint64
				for _, b := range buckets {
					if b.count < prev {
						t.Errorf("non-monotonic bucket snapshot: %d < %d", b.count, prev)
						return
					}
					prev = b.count
				}
				if buckets[len(buckets)-1].count != count {
					t.Errorf("+Inf bucket %d != count %d", buckets[len(buckets)-1].count, count)
					return
				}
			}
		}()
	}

	wg.Wait()

	wantTotal := uint64(goroutines * iterations)
	out := rec.RenderPrometheus()

	counters := []string{
		"chronos_runs_submitted_total",
		"chronos_tasks_leased_total",
		"chronos_tasks_completed_total",
		"chronos_tasks_failed_total",
		"chronos_leases_expired_total",
		"chronos_ambiguous_outcomes_total",
		"chronos_stale_fencing_tokens_total",
		"chronos_timelines_forked_total",
		"chronos_timelines_merged_total",
	}
	for _, name := range counters {
		wantLine := name + " " + strconv.FormatUint(wantTotal, 10) + "\n"
		if !strings.Contains(out, wantLine) {
			t.Errorf("expected %q in output", strings.TrimSpace(wantLine))
		}
	}

	buckets, _, count := parseHistogram(t, out)
	if count != wantTotal {
		t.Errorf("final histogram count = %d, want %d", count, wantTotal)
	}
	if buckets[len(buckets)-1].count != wantTotal {
		t.Errorf("final +Inf bucket = %d, want %d", buckets[len(buckets)-1].count, wantTotal)
	}
}

func TestRecorderHTTPHandler(t *testing.T) {
	rec := NewRecorder()
	rec.IncRunsSubmitted()
	rec.ObserveScheduleLatency(2 * time.Millisecond)

	handler := rec.Handler()

	// GET /metrics
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", rr.Code, http.StatusOK)
	}
	if ct := rr.Header().Get("Content-Type"); ct != prometheusContentType {
		t.Errorf("Content-Type = %q, want %q", ct, prometheusContentType)
	}
	if body := rr.Body.String(); body != rec.RenderPrometheus() {
		t.Errorf("GET /metrics body mismatch:\ngot:\n%s\nwant:\n%s", body, rec.RenderPrometheus())
	}

	// HEAD /metrics
	headReq := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	headRR := httptest.NewRecorder()
	handler.ServeHTTP(headRR, headReq)

	if headRR.Code != http.StatusOK {
		t.Errorf("HEAD /metrics status = %d, want %d", headRR.Code, http.StatusOK)
	}
	if ct := headRR.Header().Get("Content-Type"); ct != prometheusContentType {
		t.Errorf("HEAD Content-Type = %q, want %q", ct, prometheusContentType)
	}
	if headRR.Body.Len() != 0 {
		t.Errorf("HEAD /metrics body length = %d, want 0", headRR.Body.Len())
	}

	// POST /metrics -> 405 Method Not Allowed
	postReq := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	postRR := httptest.NewRecorder()
	handler.ServeHTTP(postRR, postReq)

	if postRR.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics status = %d, want %d", postRR.Code, http.StatusMethodNotAllowed)
	}
}

type parsedBucket struct {
	label string
	count uint64
}

func parseHistogram(t *testing.T, body string) ([]parsedBucket, float64, uint64) {
	t.Helper()
	var buckets []parsedBucket
	var sum float64
	var count uint64
	var foundSum, foundCount bool

	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "chronos_schedule_latency_ms_bucket{le=\"") {
			rest := strings.TrimPrefix(line, "chronos_schedule_latency_ms_bucket{le=\"")
			parts := strings.SplitN(rest, "\"} ", 2)
			if len(parts) != 2 {
				t.Fatalf("malformed bucket line: %q", line)
			}
			v, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				t.Fatalf("invalid bucket count in %q: %v", line, err)
			}
			buckets = append(buckets, parsedBucket{label: parts[0], count: v})
		} else if strings.HasPrefix(line, "chronos_schedule_latency_ms_sum ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, "chronos_schedule_latency_ms_sum "), 64)
			if err != nil {
				t.Fatalf("invalid sum in %q: %v", line, err)
			}
			sum = v
			foundSum = true
		} else if strings.HasPrefix(line, "chronos_schedule_latency_ms_count ") {
			v, err := strconv.ParseUint(strings.TrimPrefix(line, "chronos_schedule_latency_ms_count "), 10, 64)
			if err != nil {
				t.Fatalf("invalid count in %q: %v", line, err)
			}
			count = v
			foundCount = true
		}
	}

	if !foundSum || !foundCount {
		t.Fatalf("missing histogram sum (%v) or count (%v) in output:\n%s", foundSum, foundCount, body)
	}
	return buckets, sum, count
}
