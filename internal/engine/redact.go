package engine

import (
	"bytes"
	"encoding/json"
	"strings"
)

const RedactedValue = "[REDACTED]"

var sensitiveKeySubstrings = []string{
	"secret",
	"token",
	"password",
	"passwd",
	"api_key",
	"apikey",
	"auth",
	"credential",
	"private_key",
	"access_key",
}

// IsSensitiveKey reports whether key matches any sensitive credential/secret
// substring case-insensitively, while explicitly preserving internal fencing_token.
func IsSensitiveKey(key string) bool {
	if strings.EqualFold(key, "fencing_token") {
		return false
	}
	lower := strings.ToLower(key)
	for _, sub := range sensitiveKeySubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

// RedactMap returns a deep copy of m with all sensitive keys' values replaced
// by "[REDACTED]", recursively traversing nested maps and slices without mutating m.
func RedactMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if IsSensitiveKey(k) {
			out[k] = RedactedValue
			continue
		}
		out[k] = redactValue(v)
	}
	return out
}

func redactValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return RedactMap(val)
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = redactValue(item)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(val))
		for i, item := range val {
			out[i] = RedactMap(item)
		}
		return out
	default:
		return v
	}
}

// RedactEventPayload returns a copy of ev with sensitive keys inside its JSON
// object Payload redacted. Empty or non-object payloads are returned unchanged.
func RedactEventPayload(ev Event) Event {
	trimmed := bytes.TrimSpace(ev.Payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ev
	}

	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil || m == nil {
		return ev
	}

	redacted := RedactMap(m)
	b, err := json.Marshal(redacted)
	if err != nil {
		return ev
	}
	ev.Payload = json.RawMessage(b)
	return ev
}

// RedactEvents returns a new slice with RedactEventPayload applied to each event.
func RedactEvents(evs []Event) []Event {
	if evs == nil {
		return nil
	}
	out := make([]Event, len(evs))
	for i, ev := range evs {
		out[i] = RedactEventPayload(ev)
	}
	return out
}
