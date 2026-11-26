package runtime

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/creova-gif/chronos-sched/internal/store"
)

const maxArtifactBytes = 4 << 20 // 4 MiB

// PutArtifactRequest is the JSON payload accepted by POST /v1/runs/{id}/tasks/{task_id}/artifacts.
type PutArtifactRequest struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type,omitempty"`
	DataBase64  string `json:"data_base64"`
}

// ListArtifactsResponse is the JSON payload returned by GET /v1/runs/{id}/artifacts.
type ListArtifactsResponse struct {
	Artifacts []store.ArtifactRecord `json:"artifacts"`
}

func (c *Coordinator) handlePutArtifact(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if !c.authorizeRunTenant(w, r, runID) {
		return
	}
	artStore, ok := c.store.(store.ArtifactStore)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "store does not support artifacts"})
		return
	}

	taskID := r.PathValue("task_id")
	if runID == "" || taskID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "run_id and task_id are required"})
		return
	}

	var req PutArtifactRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxArtifactBytes*2)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid request body"})
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "artifact name is required"})
		return
	}

	data, err := base64.StdEncoding.DecodeString(req.DataBase64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid base64 data"})
		return
	}
	if len(data) > maxArtifactBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
			Error: fmt.Sprintf("artifact exceeds max size of %d bytes", maxArtifactBytes),
		})
		return
	}

	rec, err := artStore.PutArtifact(r.Context(), store.ArtifactRecord{
		RunID:       runID,
		TaskID:      taskID,
		Name:        req.Name,
		ContentType: req.ContentType,
		Data:        data,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	rec.Data = nil

	writeJSON(w, http.StatusCreated, rec)
}

func (c *Coordinator) handleListRunArtifacts(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if !c.authorizeRunTenant(w, r, runID) {
		return
	}
	artStore, ok := c.store.(store.ArtifactStore)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "store does not support artifacts"})
		return
	}

	artifacts, err := artStore.ListRunArtifacts(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	if artifacts == nil {
		artifacts = []store.ArtifactRecord{}
	}

	writeJSON(w, http.StatusOK, ListArtifactsResponse{Artifacts: artifacts})
}

func (c *Coordinator) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if !c.authorizeRunTenant(w, r, runID) {
		return
	}
	artStore, ok := c.store.(store.ArtifactStore)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "store does not support artifacts"})
		return
	}

	art, err := artStore.GetArtifact(
		r.Context(),
		r.PathValue("id"),
		r.PathValue("task_id"),
		r.PathValue("name"),
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "artifact not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	if r.URL.Query().Get("meta") == "true" {
		art.Data = nil
		writeJSON(w, http.StatusOK, art)
		return
	}

	if art.ContentType != "" {
		w.Header().Set("Content-Type", art.ContentType)
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", safeAttachmentFilename(art.Name)))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("ETag", fmt.Sprintf("%q", art.SHA256))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(art.Data)
}

func safeAttachmentFilename(name string) string {
	base := filepath.Base(name)
	var b strings.Builder
	b.Grow(len(base))
	for _, r := range base {
		if r == '"' || r == '\\' || r == '/' || r == ';' || r == '\r' || r == '\n' || unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
	}
	cleaned := b.String()
	if cleaned == "" || cleaned == "." || cleaned == ".." {
		return "artifact"
	}
	return cleaned
}
