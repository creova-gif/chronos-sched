package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
)

const defaultMaxJSONBodyBytes int64 = 2 << 20 // 2 MiB

// decodeJSONBody bounds r.Body with http.MaxBytesReader (defaulting to
// defaultMaxJSONBodyBytes when maxBytes <= 0) and decodes JSON into dst.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) error {
	if maxBytes <= 0 {
		maxBytes = defaultMaxJSONBodyBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	return json.NewDecoder(r.Body).Decode(dst)
}

// writeJSONDecodeError writes HTTP 413 when err is a *http.MaxBytesError,
// or HTTP 400 for other JSON decode errors.
func writeJSONDecodeError(w http.ResponseWriter, err error) {
	var maxErr *http.MaxBytesError
	status := http.StatusBadRequest
	if errors.As(err, &maxErr) {
		status = http.StatusRequestEntityTooLarge
	}
	writeJSON(w, status, ErrorResponse{Error: err.Error()})
}
