package tui

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/creova-gif/chronos-sched/internal/runtime"
)

const sseMaxTokenSize = 1024 * 1024 // 1 MiB

// StreamEvents connects to an SSE endpoint (GET /v1/runs/{id}/events),
// parses `data: <json>` frames into runtime.SSEMessage, sends them to out,
// and exits cleanly when ctx is cancelled or the server closes the stream.
func StreamEvents(ctx context.Context, sseURL string, out chan<- runtime.SSEMessage) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("connect to SSE stream: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected SSE status: %s", resp.Status)
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
		var msg runtime.SSEMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			return fmt.Errorf("decode SSE message: %w", err)
		}
		select {
		case out <- msg:
			return nil
		case <-ctx.Done():
			return nil
		}
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
