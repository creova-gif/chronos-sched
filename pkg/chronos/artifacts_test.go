package chronos_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/creova-gif/chronos-sched/pkg/chronos"
)

func TestClientArtifactsPutListGet(t *testing.T) {
	const wantToken = "secret-token"
	payload := []byte(`{"summary":"ok","count":42}`)

	var stored []byte
	var storedContentType string
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/runs/{id}/tasks/{task_id}/artifacts", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		var req struct {
			Name        string `json:"name"`
			ContentType string `json:"content_type"`
			Data        string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Name == "too-big.bin" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_, _ = w.Write([]byte(`{"error":"artifact exceeds max size"}`))
			return
		}
		raw, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		stored = raw
		storedContentType = req.ContentType

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(chronos.ArtifactRecord{
			ID:          7,
			RunID:       r.PathValue("id"),
			TaskID:      r.PathValue("task_id"),
			Name:        req.Name,
			ContentType: req.ContentType,
			SHA256:      "deadbeef",
			SizeBytes:   int64(len(raw)),
			CreatedAtNs: 123456789,
		})
	})

	mux.HandleFunc("GET /v1/runs/{id}/artifacts", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifacts": []chronos.ArtifactRecord{
				{
					ID:          7,
					RunID:       r.PathValue("id"),
					TaskID:      "task-a",
					Name:        "report.json",
					ContentType: storedContentType,
					SHA256:      "deadbeef",
					SizeBytes:   int64(len(stored)),
					CreatedAtNs: 123456789,
				},
			},
		})
	})

	mux.HandleFunc("GET /v1/runs/{id}/tasks/{task_id}/artifacts/{name}", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		if r.PathValue("name") != "report.json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"artifact not found"}`))
			return
		}
		w.Header().Set("Content-Type", storedContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(stored)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := chronos.NewClient(srv.URL).WithBearerToken(wantToken)
	ctx := context.Background()

	rec, err := client.PutArtifact(ctx, "run-1", "task-a", "report.json", "application/json", payload)
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	if rec.ID != 7 || rec.RunID != "run-1" || rec.TaskID != "task-a" || rec.Name != "report.json" || rec.SizeBytes != int64(len(payload)) {
		t.Fatalf("unexpected ArtifactRecord: %+v", rec)
	}

	list, err := client.ListRunArtifacts(ctx, "run-1")
	if err != nil {
		t.Fatalf("ListRunArtifacts: %v", err)
	}
	if len(list) != 1 || list[0].Name != "report.json" {
		t.Fatalf("unexpected ListRunArtifacts result: %+v", list)
	}

	gotBytes, err := client.GetArtifact(ctx, "run-1", "task-a", "report.json")
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if !bytes.Equal(gotBytes, payload) {
		t.Fatalf("GetArtifact bytes = %q, want %q", gotBytes, payload)
	}

	// Verify 404 returns *APIError.
	_, err = client.GetArtifact(ctx, "run-1", "task-a", "missing.txt")
	var apiErr *chronos.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		t.Fatalf("GetArtifact(missing) err = %v, want *APIError with status 404", err)
	}

	// Verify 413 returns *APIError.
	_, err = client.PutArtifact(ctx, "run-1", "task-a", "too-big.bin", "application/octet-stream", []byte("x"))
	apiErr = nil
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("PutArtifact(too-big) err = %v, want *APIError with status 413", err)
	}
}
