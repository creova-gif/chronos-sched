package chronos

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const sseMaxTokenSize = 1024 * 1024 // 1 MiB

// StreamEvent represents a single live workflow event delivered over SSE.
type StreamEvent struct {
	Seq          int64           `json:"seq"`
	TimestampNs  int64           `json:"ts_ns"`
	Type         string          `json:"event_type"`
	TaskID       string          `json:"task_id,omitempty"`
	Attempt      int             `json:"attempt,omitempty"`
	FencingToken int64           `json:"fencing_token,omitempty"`
	WorkerID     string          `json:"worker_id,omitempty"`
	RunStatus    RunStatus       `json:"run_status,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

type sseFrame struct {
	Event struct {
		Seq          int64           `json:"seq"`
		TimestampNs  int64           `json:"ts_ns"`
		Type         string          `json:"event_type"`
		TaskID       string          `json:"task_id,omitempty"`
		Attempt      int             `json:"attempt,omitempty"`
		FencingToken int64           `json:"fencing_token,omitempty"`
		WorkerID     string          `json:"worker_id,omitempty"`
		Payload      json.RawMessage `json:"payload,omitempty"`
	} `json:"event"`
	RunStatus RunStatus `json:"run_status"`
}

// StreamRunEvents connects to GET /v1/runs/<runID>/events over Server-Sent Events,
// parses each `data: <json>` frame into a StreamEvent, and invokes onEvent.
// It exits cleanly (nil) on ctx cancellation or EOF, and returns any non-nil onEvent error.
func (c *Client) StreamRunEvents(ctx context.Context, runID string, onEvent func(StreamEvent) error) error {
	if onEvent == nil {
		return fmt.Errorf("onEvent callback must not be nil")
	}

	streamURL := fmt.Sprintf("%s/v1/runs/%s/events", c.baseURL, url.PathEscape(runID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	c.applyHeaders(req)

	// Dedicated streaming client without the 10s request timeout so long-lived streams stay open.
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("connect to SSE stream: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		var errResp errorResponse
		if json.Unmarshal(data, &errResp) == nil && errResp.Error != "" {
			return &APIError{StatusCode: resp.StatusCode, Message: errResp.Error}
		}
		return &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), sseMaxTokenSize)

	var dataLines []string
	flush := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		if strings.TrimSpace(payload) == "" {
			return nil
		}

		var frame sseFrame
		if err := json.Unmarshal([]byte(payload), &frame); err != nil {
			return fmt.Errorf("decode SSE event: %w", err)
		}

		ev := StreamEvent{
			Seq:          frame.Event.Seq,
			TimestampNs:  frame.Event.TimestampNs,
			Type:         frame.Event.Type,
			TaskID:       frame.Event.TaskID,
			Attempt:      frame.Event.Attempt,
			FencingToken: frame.Event.FencingToken,
			WorkerID:     frame.Event.WorkerID,
			RunStatus:    frame.RunStatus,
			Payload:      frame.Event.Payload,
		}
		if err := onEvent(ev); err != nil {
			return err
		}
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			// SSE comment / keep-alive.
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimPrefix(line, "data:")
			if strings.HasPrefix(data, " ") {
				data = data[1:]
			}
			dataLines = append(dataLines, data)
		}
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("read SSE stream: %w", err)
	}

	if err := flush(); err != nil {
		return err
	}
	return nil
}
