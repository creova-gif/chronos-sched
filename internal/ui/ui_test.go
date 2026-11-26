package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestHandlerServesEmbeddedUI(t *testing.T) {
	h := Handler()

	for _, path := range []string{"/", "/ui", "/ui/"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusOK)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
			t.Fatalf("GET %s Content-Type = %q, want text/html", path, ct)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("GET %s Cache-Control = %q, want no-store", path, cc)
		}
		if body := rec.Body.String(); !strings.Contains(body, "ChronosSched") {
			t.Fatalf("GET %s body does not contain ChronosSched", path)
		}
	}
}

func TestHandlerRejectsUnknownPathAndMethod(t *testing.T) {
	h := Handler()

	notFoundReq := httptest.NewRequest(http.MethodGet, "/ui/missing", nil)
	notFoundRec := httptest.NewRecorder()
	h.ServeHTTP(notFoundRec, notFoundReq)
	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("GET /ui/missing status = %d, want %d", notFoundRec.Code, http.StatusNotFound)
	}

	postReq := httptest.NewRequest(http.MethodPost, "/ui/", nil)
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /ui/ status = %d, want %d", postRec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandlerConcurrentRequests(t *testing.T) {
	h := Handler()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("concurrent GET /ui/ status = %d, want %d", rec.Code, http.StatusOK)
			}
		}()
	}
	wg.Wait()
}
