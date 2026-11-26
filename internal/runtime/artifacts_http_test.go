package runtime

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func newArtifactsHTTPTestMux(t *testing.T) *http.ServeMux {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "artifacts-http.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := NewCoordinator(st, CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runs/{id}/tasks/{task_id}/artifacts", coord.handlePutArtifact)
	mux.HandleFunc("GET /v1/runs/{id}/artifacts", coord.handleListRunArtifacts)
	mux.HandleFunc("GET /v1/runs/{id}/tasks/{task_id}/artifacts/{name}", coord.handleGetArtifact)
	return mux
}

func doArtifactJSONRequest(t *testing.T, handler http.Handler, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode request body: %v", err)
		}
	}
	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestArtifactsHTTP_PutListGetAndSizeLimit(t *testing.T) {
	mux := newArtifactsHTTPTestMux(t)

	// Empty list returns [] rather than null.
	emptyRec := doArtifactJSONRequest(t, mux, http.MethodGet, "/v1/runs/run-1/artifacts", nil)
	if emptyRec.Code != http.StatusOK {
		t.Fatalf("GET empty list status: got %d, want %d (%s)", emptyRec.Code, http.StatusOK, emptyRec.Body.String())
	}
	var emptyResp ListArtifactsResponse
	if err := json.NewDecoder(emptyRec.Body).Decode(&emptyResp); err != nil {
		t.Fatalf("decode empty list: %v", err)
	}
	if emptyResp.Artifacts == nil || len(emptyResp.Artifacts) != 0 {
		t.Fatalf("expected non-nil empty artifacts slice, got %+v", emptyResp.Artifacts)
	}

	// 1. PUT artifact -> 201 Created without Data in JSON response.
	payload := []byte(`{"status":"ok","rows":42}`)
	putRec := doArtifactJSONRequest(t, mux, http.MethodPost, "/v1/runs/run-1/tasks/extract/artifacts", PutArtifactRequest{
		Name:        "summary.json",
		ContentType: "application/json",
		DataBase64:  base64.StdEncoding.EncodeToString(payload),
	})
	if putRec.Code != http.StatusCreated {
		t.Fatalf("POST artifact status: got %d, want %d (%s)", putRec.Code, http.StatusCreated, putRec.Body.String())
	}
	var created store.ArtifactRecord
	if err := json.NewDecoder(putRec.Body).Decode(&created); err != nil {
		t.Fatalf("decode created artifact: %v", err)
	}
	if created.ID <= 0 || created.RunID != "run-1" || created.TaskID != "extract" || created.Name != "summary.json" {
		t.Fatalf("unexpected created metadata: %+v", created)
	}
	if created.SizeBytes != int64(len(payload)) || created.SHA256 == "" || len(created.Data) != 0 {
		t.Fatalf("unexpected created size/hash/data: %+v", created)
	}

	// 2. List run artifacts -> 200 OK.
	listRec := doArtifactJSONRequest(t, mux, http.MethodGet, "/v1/runs/run-1/artifacts", nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET list status: got %d, want %d", listRec.Code, http.StatusOK)
	}
	var listResp ListArtifactsResponse
	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResp.Artifacts) != 1 || listResp.Artifacts[0].Name != "summary.json" {
		t.Fatalf("unexpected list response: %+v", listResp.Artifacts)
	}

	// 3. Raw download -> 200 OK with Content-Type, ETag, and raw bytes.
	rawRec := doArtifactJSONRequest(t, mux, http.MethodGet, "/v1/runs/run-1/tasks/extract/artifacts/summary.json", nil)
	if rawRec.Code != http.StatusOK {
		t.Fatalf("GET raw status: got %d, want %d (%s)", rawRec.Code, http.StatusOK, rawRec.Body.String())
	}
	if gotCT := rawRec.Header().Get("Content-Type"); gotCT != "application/json" {
		t.Fatalf("Content-Type: got %q, want %q", gotCT, "application/json")
	}
	wantETag := fmt.Sprintf("%q", created.SHA256)
	if gotETag := rawRec.Header().Get("ETag"); gotETag != wantETag {
		t.Fatalf("ETag: got %q, want %q", gotETag, wantETag)
	}
	if !bytes.Equal(rawRec.Body.Bytes(), payload) {
		t.Fatalf("raw body mismatch: got %q, want %q", rawRec.Body.Bytes(), payload)
	}

	// 4. Metadata-only download (?meta=true) -> 200 OK JSON without Data.
	metaRec := doArtifactJSONRequest(t, mux, http.MethodGet, "/v1/runs/run-1/tasks/extract/artifacts/summary.json?meta=true", nil)
	if metaRec.Code != http.StatusOK {
		t.Fatalf("GET meta status: got %d, want %d (%s)", metaRec.Code, http.StatusOK, metaRec.Body.String())
	}
	var meta store.ArtifactRecord
	if err := json.NewDecoder(metaRec.Body).Decode(&meta); err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	if meta.SHA256 != created.SHA256 || meta.SizeBytes != created.SizeBytes || len(meta.Data) != 0 {
		t.Fatalf("unexpected meta response: %+v", meta)
	}

	// 5. Missing artifact -> 404 Not Found.
	missingRec := doArtifactJSONRequest(t, mux, http.MethodGet, "/v1/runs/run-1/tasks/extract/artifacts/missing.json", nil)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("GET missing status: got %d, want %d", missingRec.Code, http.StatusNotFound)
	}

	// 6. Oversized artifact (> maxArtifactBytes) -> 413 Request Entity Too Large.
	oversized := bytes.Repeat([]byte("a"), maxArtifactBytes+1)
	oversizedRec := doArtifactJSONRequest(t, mux, http.MethodPost, "/v1/runs/run-1/tasks/extract/artifacts", PutArtifactRequest{
		Name:       "too-big.bin",
		DataBase64: base64.StdEncoding.EncodeToString(oversized),
	})
	if oversizedRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("POST oversized status: got %d, want %d (%s)", oversizedRec.Code, http.StatusRequestEntityTooLarge, oversizedRec.Body.String())
	}
}

func TestArtifactDownloadSecurityHeaders(t *testing.T) {
	mux := newArtifactsHTTPTestMux(t)

	cases := []struct {
		name         string
		artifactName string
		wantHeader   string
	}{
		{
			name:         "standard filename",
			artifactName: "report.bin",
			wantHeader:   `attachment; filename="report.bin"`,
		},
		{
			name:         "quotes semicolons backslashes and path components",
			artifactName: `../../dir/bad";name\test.txt`,
			wantHeader:   `attachment; filename="badnametest.txt"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			putRec := doArtifactJSONRequest(t, mux, http.MethodPost, "/v1/runs/run-sec/tasks/t1/artifacts", PutArtifactRequest{
				Name:        tc.artifactName,
				ContentType: "application/octet-stream",
				DataBase64:  base64.StdEncoding.EncodeToString([]byte("payload")),
			})
			if putRec.Code != http.StatusCreated {
				t.Fatalf("POST artifact status: got %d, want %d (%s)", putRec.Code, http.StatusCreated, putRec.Body.String())
			}

			target := "/v1/runs/run-sec/tasks/t1/artifacts/" + url.PathEscape(tc.artifactName)
			getRec := doArtifactJSONRequest(t, mux, http.MethodGet, target, nil)
			if getRec.Code != http.StatusOK {
				t.Fatalf("GET artifact status: got %d, want %d (%s)", getRec.Code, http.StatusOK, getRec.Body.String())
			}
			if gotCD := getRec.Header().Get("Content-Disposition"); gotCD != tc.wantHeader {
				t.Fatalf("Content-Disposition: got %q, want %q", gotCD, tc.wantHeader)
			}
			if gotCC := getRec.Header().Get("Cache-Control"); gotCC != "private, no-store" {
				t.Fatalf("Cache-Control: got %q, want %q", gotCC, "private, no-store")
			}
		})
	}

	if got := safeAttachmentFilename(".."); got != "artifact" {
		t.Fatalf("safeAttachmentFilename(\"..\"): got %q, want %q", got, "artifact")
	}
	if got := safeAttachmentFilename("a\r\n\x00;\"\\b.txt"); got != "ab.txt" {
		t.Fatalf("safeAttachmentFilename control chars: got %q, want %q", got, "ab.txt")
	}
}
