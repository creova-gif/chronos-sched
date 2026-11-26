package chronos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ArtifactRecord represents metadata for a task execution artifact stored by the Coordinator.
type ArtifactRecord struct {
	ID          int64  `json:"id"`
	RunID       string `json:"run_id"`
	TaskID      string `json:"task_id"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	CreatedAtNs int64  `json:"created_at_ns"`
}

type putArtifactRequest struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type,omitempty"`
	Data        string `json:"data"`
}

// PutArtifact uploads a base64-encoded artifact for a task within a run.
func (c *Client) PutArtifact(ctx context.Context, runID, taskID, name, contentType string, data []byte) (ArtifactRecord, error) {
	path := fmt.Sprintf("/v1/runs/%s/tasks/%s/artifacts", url.PathEscape(runID), url.PathEscape(taskID))
	req := putArtifactRequest{
		Name:        name,
		ContentType: contentType,
		Data:        base64.StdEncoding.EncodeToString(data),
	}
	var resp ArtifactRecord
	if err := c.doJSON(ctx, http.MethodPost, path, req, &resp); err != nil {
		return ArtifactRecord{}, err
	}
	return resp, nil
}

// ListRunArtifacts returns metadata for all artifacts associated with runID.
func (c *Client) ListRunArtifacts(ctx context.Context, runID string) ([]ArtifactRecord, error) {
	path := fmt.Sprintf("/v1/runs/%s/artifacts", url.PathEscape(runID))
	var resp struct {
		Artifacts []ArtifactRecord `json:"artifacts"`
	}
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Artifacts, nil
}

// GetArtifact downloads the raw bytes of the named artifact for a task within a run.
func (c *Client) GetArtifact(ctx context.Context, runID, taskID, name string) ([]byte, error) {
	path := fmt.Sprintf(
		"/v1/runs/%s/tasks/%s/artifacts/%s",
		url.PathEscape(runID),
		url.PathEscape(taskID),
		url.PathEscape(name),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp errorResponse
		if json.Unmarshal(data, &errResp) == nil && errResp.Error != "" {
			return nil, &APIError{StatusCode: resp.StatusCode, Message: errResp.Error}
		}
		return nil, &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	}
	return data, nil
}
