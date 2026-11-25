package runtime

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestObservabilityMiddleware_TraceparentGenerationAndPropagation(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	h := ObservabilityMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}))

	t.Run("generates traceparent when absent", func(t *testing.T) {
		logBuf.Reset()
		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		tp := rec.Header().Get("Traceparent")
		tid, _, ok := parseTraceparent(tp)
		if !ok {
			t.Fatalf("invalid generated Traceparent %q", tp)
		}
		reqID := rec.Header().Get("X-Request-Id")
		if reqID == "" {
			t.Fatal("expected non-empty X-Request-Id header")
		}
		if !strings.Contains(logBuf.String(), tid) {
			t.Fatalf("expected log to contain trace_id %q, got %s", tid, logBuf.String())
		}
	})

	t.Run("generates traceparent when malformed", func(t *testing.T) {
		logBuf.Reset()
		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		req.Header.Set("Traceparent", "not-a-valid-traceparent")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		tp := rec.Header().Get("Traceparent")
		if _, _, ok := parseTraceparent(tp); !ok {
			t.Fatalf("expected valid regenerated Traceparent, got %q", tp)
		}
	})

	t.Run("preserves incoming trace ID with fresh span ID", func(t *testing.T) {
		logBuf.Reset()
		inTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
		inSpanID := "00f067aa0ba902b7"
		inTP := "00-" + inTraceID + "-" + inSpanID + "-01"

		req := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", nil)
		req.Header.Set("Traceparent", inTP)
		req.Header.Set("X-Request-Id", "req-123")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		outTP := rec.Header().Get("Traceparent")
		outTraceID, outFlags, ok := parseTraceparent(outTP)
		if !ok {
			t.Fatalf("invalid propagated Traceparent %q", outTP)
		}
		if outTraceID != inTraceID {
			t.Fatalf("trace_id = %q, want %q", outTraceID, inTraceID)
		}
		if outFlags != "01" {
			t.Fatalf("flags = %q, want 01", outFlags)
		}
		parts := strings.Split(outTP, "-")
		if parts[2] == inSpanID {
			t.Fatalf("expected fresh span ID, got same %q", inSpanID)
		}
		if got := rec.Header().Get("X-Request-Id"); got != "req-123" {
			t.Fatalf("X-Request-Id = %q, want req-123", got)
		}
	})
}

func TestObservabilityMiddleware_PanicRecovery(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	h := ObservabilityMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/runs/r1", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var errResp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.Error != "internal server error" {
		t.Fatalf("error = %q, want %q", errResp.Error, "internal server error")
	}

	lines := strings.Split(strings.TrimSpace(logBuf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected error + completion log lines, got %d: %s", len(lines), logBuf.String())
	}
	var panicEntry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &panicEntry); err != nil {
		t.Fatalf("unmarshal panic log: %v", err)
	}
	if panicEntry["level"] != "ERROR" {
		t.Fatalf("panic log level = %v, want ERROR", panicEntry["level"])
	}
	if panicEntry["panic"] != "boom" {
		t.Fatalf("panic log field = %v, want boom", panicEntry["panic"])
	}
}

func TestObservabilityMiddleware_FlusherPreserved(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	var flushed bool
	h := ObservabilityMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("wrapped ResponseWriter does not implement http.Flusher")
		}
		_, _ = w.Write([]byte("data: ok\n\n"))
		flusher.Flush()
		flushed = true
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/runs/r1/events", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !flushed {
		t.Fatal("expected SSE handler to flush")
	}
	if !rec.Flushed {
		t.Fatal("expected underlying httptest.ResponseRecorder to be flushed")
	}
}

func TestObservabilityMiddleware_StructuredJSONLogging(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	h := ObservabilityMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"id": "123"})
	}))

	// /healthz is logged at Debug, so it should be suppressed at LevelInfo.
	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.ServeHTTP(httptest.NewRecorder(), healthReq)
	if logBuf.Len() != 0 {
		t.Fatalf("expected /healthz to be skipped at INFO level, got %s", logBuf.String())
	}

	// Normal route is logged at Info with required structured fields.
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var entry map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log line: %v (raw: %s)", err, logBuf.String())
	}
	for _, key := range []string{"method", "path", "status", "duration_ms", "trace_id", "request_id"} {
		if _, exists := entry[key]; !exists {
			t.Fatalf("missing required log field %q in %v", key, entry)
		}
	}
	if entry["method"] != "POST" || entry["path"] != "/v1/workflows/run" {
		t.Fatalf("unexpected method/path in log: %v", entry)
	}
	if entry["status"] != float64(http.StatusCreated) {
		t.Fatalf("status = %v, want 201", entry["status"])
	}
}
