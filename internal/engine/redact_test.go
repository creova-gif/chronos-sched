package engine

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRedactIsSensitiveKey(t *testing.T) {
	sensitive := []string{
		"secret",
		"client_secret",
		"API_KEY",
		"apiKey",
		"db_password",
		"PASSWD",
		"authHeader",
		"bearer_token",
		"credential_id",
		"private_key_pem",
		"aws_access_key",
	}
	for _, k := range sensitive {
		if !IsSensitiveKey(k) {
			t.Errorf("expected IsSensitiveKey(%q) = true, got false", k)
		}
	}

	safe := []string{
		"mode",
		"env",
		"rows",
		"fencing_token",
		"FENCING_TOKEN",
		"task_id",
		"workflow_name",
	}
	for _, k := range safe {
		if IsSensitiveKey(k) {
			t.Errorf("expected IsSensitiveKey(%q) = false, got true", k)
		}
	}
}

func TestRedactMapNestedAndImmutability(t *testing.T) {
	if got := RedactMap(nil); got != nil {
		t.Fatalf("expected nil for nil input, got %v", got)
	}

	input := map[string]any{
		"mode":        "prod",
		"env":         "us-east-1",
		"rows":        42,
		"API_KEY":     "sk-12345",
		"db_password": "hunter2",
		"nested": map[string]any{
			"authHeader": "Bearer xyz",
			"safe":       "ok",
			"deep_slice": []any{
				map[string]any{
					"client_secret": "s3cr3t",
					"count":         3,
				},
				"plain",
			},
		},
	}

	snapshot := map[string]any{
		"mode":        "prod",
		"env":         "us-east-1",
		"rows":        42,
		"API_KEY":     "sk-12345",
		"db_password": "hunter2",
		"nested": map[string]any{
			"authHeader": "Bearer xyz",
			"safe":       "ok",
			"deep_slice": []any{
				map[string]any{
					"client_secret": "s3cr3t",
					"count":         3,
				},
				"plain",
			},
		},
	}

	got := RedactMap(input)

	// Verify input was not mutated.
	if !reflect.DeepEqual(input, snapshot) {
		t.Fatalf("input map was mutated:\ngot:  %#v\nwant: %#v", input, snapshot)
	}

	if got["mode"] != "prod" || got["env"] != "us-east-1" || got["rows"] != 42 {
		t.Fatalf("non-sensitive top-level keys altered: %#v", got)
	}
	if got["API_KEY"] != RedactedValue || got["db_password"] != RedactedValue {
		t.Fatalf("sensitive top-level keys not redacted: %#v", got)
	}

	nested, ok := got["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map, got %T", got["nested"])
	}
	if nested["authHeader"] != RedactedValue || nested["safe"] != "ok" {
		t.Fatalf("unexpected nested map values: %#v", nested)
	}

	slice, ok := nested["deep_slice"].([]any)
	if !ok || len(slice) != 2 {
		t.Fatalf("unexpected deep_slice: %#v", nested["deep_slice"])
	}
	inner, ok := slice[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map in slice[0], got %T", slice[0])
	}
	if inner["client_secret"] != RedactedValue || inner["count"] != 3 {
		t.Fatalf("unexpected inner map values: %#v", inner)
	}
	if slice[1] != "plain" {
		t.Fatalf("unexpected slice[1]: %v", slice[1])
	}
}

func TestRedactEventPayloadAndEvents(t *testing.T) {
	payloadBytes, err := json.Marshal(map[string]any{
		"output": map[string]any{
			"API_KEY":       "top-secret",
			"db_password":   "pw123",
			"rows":          100,
			"fencing_token": 7,
		},
		"items": []any{
			map[string]any{"authHeader": "Bearer abc", "mode": "fast"},
		},
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	evs := []Event{
		{
			RunID:        "run-1",
			Seq:          10,
			TimestampNs:  1700000000000000001,
			Type:         EventTaskCompleted,
			TaskID:       "t1",
			Attempt:      1,
			FencingToken: 42,
			WorkerID:     "w1",
			Payload:      payloadBytes,
		},
		{
			RunID:        "run-1",
			Seq:          11,
			Type:         EventRunCompleted,
			FencingToken: 43,
			Payload:      nil,
		},
		{
			RunID:   "run-1",
			Seq:     12,
			Type:    EventRunPaused,
			Payload: json.RawMessage(`["not-an-object"]`),
		},
	}

	redacted := RedactEvents(evs)
	if len(redacted) != len(evs) {
		t.Fatalf("expected %d events, got %d", len(evs), len(redacted))
	}

	// Check metadata preserved.
	if redacted[0].FencingToken != 42 || redacted[0].Seq != 10 || redacted[0].Type != EventTaskCompleted {
		t.Fatalf("event metadata not preserved: %+v", redacted[0])
	}

	var decoded map[string]any
	if err := json.Unmarshal(redacted[0].Payload, &decoded); err != nil {
		t.Fatalf("unmarshal redacted payload failed: %v", err)
	}
	outMap := decoded["output"].(map[string]any)
	if outMap["API_KEY"] != RedactedValue || outMap["db_password"] != RedactedValue {
		t.Fatalf("expected payload secrets redacted, got %#v", outMap)
	}
	if outMap["rows"] != float64(100) {
		t.Fatalf("expected rows=100, got %v", outMap["rows"])
	}
	if outMap["fencing_token"] != float64(7) {
		t.Fatalf("expected fencing_token preserved, got %v", outMap["fencing_token"])
	}

	items := decoded["items"].([]any)
	item0 := items[0].(map[string]any)
	if item0["authHeader"] != RedactedValue || item0["mode"] != "fast" {
		t.Fatalf("unexpected item0: %#v", item0)
	}

	// Empty and non-object payloads remain unchanged.
	if redacted[1].Payload != nil || redacted[1].FencingToken != 43 {
		t.Fatalf("expected empty payload event unchanged, got %+v", redacted[1])
	}
	if string(redacted[2].Payload) != `["not-an-object"]` {
		t.Fatalf("expected non-object payload unchanged, got %s", string(redacted[2].Payload))
	}
}
