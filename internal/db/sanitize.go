// db/sanitize.go
package db

import (
	"encoding/json"
	"strings"
)

// sanitizeNulls strips embedded NUL bytes from every string value in a
// decoded JSON structure — Postgres text/jsonb cannot store \u0000 at
// all (SQLSTATE 22P05), so this has to happen before re-marshaling for
// insert. Recurses into nested maps/arrays since PLC ASCII decode can
// leave NULs anywhere a string field went unpopulated.
func sanitizeNulls(v any) any {
	switch val := v.(type) {
	case string:
		return strings.ReplaceAll(val, "\x00", "")
	case map[string]any:
		for k, vv := range val {
			val[k] = sanitizeNulls(vv)
		}
		return val
	case []any:
		for i, vv := range val {
			val[i] = sanitizeNulls(vv)
		}
		return val
	default:
		return v
	}
}

// sanitizeRawJSON decode→sanitize→re-encodes a json.RawMessage. Needed
// specifically for InsertTelemetryRaw/InsertEventMetric/InsertRealtimeMetric,
// which pass readings/output/status/limits/energy through as raw
// validated JSON strings without ever decoding into a Go map — sanitizeNulls
// alone can't touch bytes hidden inside an un-decoded string.
func sanitizeRawJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw // malformed JSON — ValidateJSON already gated this upstream, leave as-is
	}
	cleaned, err := json.Marshal(sanitizeNulls(v))
	if err != nil {
		return raw
	}
	return cleaned
}
