package runtime

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

const (
	headerTraceparent = "Traceparent"
	headerRequestID   = "X-Request-Id"
)

// ObservabilityMiddleware wraps next with W3C traceparent / X-Request-Id
// propagation, panic recovery returning HTTP 500 JSON, and structured slog
// request logging while preserving http.Flusher for SSE streams.
func ObservabilityMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		traceID, flags, ok := parseTraceparent(r.Header.Get(headerTraceparent))
		if !ok {
			traceID = randomHex(16)
			flags = "01"
		}
		spanID := randomHex(8)
		traceparent := fmt.Sprintf("00-%s-%s-%s", traceID, spanID, flags)

		requestID := strings.TrimSpace(r.Header.Get(headerRequestID))
		if requestID == "" {
			requestID = randomHex(16)
		}

		w.Header().Set(headerTraceparent, traceparent)
		w.Header().Set(headerRequestID, requestID)

		rw := &statusResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		defer func() {
			durationMs := time.Since(start).Milliseconds()
			if r.URL.Path == "/healthz" {
				logger.DebugContext(r.Context(), "http request",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", rw.status),
					slog.Int64("bytes", rw.bytes),
					slog.Int64("duration_ms", durationMs),
					slog.String("trace_id", traceID),
					slog.String("request_id", requestID),
				)
				return
			}
			logger.InfoContext(r.Context(), "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rw.status),
				slog.Int64("bytes", rw.bytes),
				slog.Int64("duration_ms", durationMs),
				slog.String("trace_id", traceID),
				slog.String("request_id", requestID),
			)
		}()

		defer func() {
			if rec := recover(); rec != nil {
				logger.ErrorContext(r.Context(), "http handler panic",
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("trace_id", traceID),
					slog.String("request_id", requestID),
				)
				if !rw.wroteHeader {
					writeJSON(rw, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
				}
			}
		}()

		next.ServeHTTP(rw, r)
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	bytes       int64
}

func (w *statusResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.status = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *statusResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func parseTraceparent(header string) (traceID, flags string, ok bool) {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) != 4 {
		return "", "", false
	}
	version, tid, pid, fl := parts[0], parts[1], parts[2], parts[3]
	if len(version) != 2 || len(tid) != 32 || len(pid) != 16 || len(fl) != 2 {
		return "", "", false
	}
	if version == "ff" {
		return "", "", false
	}
	if !isLowerHex(version) || !isLowerHex(tid) || !isLowerHex(pid) || !isLowerHex(fl) {
		return "", "", false
	}
	if isAllZero(tid) || isAllZero(pid) {
		return "", "", false
	}
	return tid, fl, true
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func isAllZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
