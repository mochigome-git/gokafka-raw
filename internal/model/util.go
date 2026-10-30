package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// StringFloat64 stays — devices might still send numbers as strings
type StringFloat64 float64

func (s *StringFloat64) UnmarshalJSON(b []byte) error {
	var f float64
	if err := json.Unmarshal(b, &f); err == nil {
		*s = StringFloat64(f)
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return fmt.Errorf("StringFloat64: cannot unmarshal %s", string(b))
	}
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return fmt.Errorf("StringFloat64: cannot parse %q to float64", str)
	}
	*s = StringFloat64(f)
	return nil
}

func parseStringFloat(v any) *float64 {
	var f float64
	switch val := v.(type) {
	case float64:
		f = val
	case string:
		parsed, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		f = parsed
	default:
		return nil
	}
	return &f
}

// localTimeLayouts are accepted for offset-less wall-clock times (PLC
// registers). gopub-edge sends the first; the others are tolerated.
var localTimeLayouts = []string{
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// parseJobTime reads a JSON string timestamp. An RFC3339 value (with Z or
// an offset) is a real instant: local=false. A value without an offset is
// factory wall-clock time: it is returned with a placeholder UTC zone and
// local=true, and must be converted with the tenant's timezone in SQL.
func parseJobTime(v json.RawMessage) (t time.Time, local bool, ok bool) {
	var s string
	if json.Unmarshal(v, &s) != nil || s == "" {
		return time.Time{}, false, false
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, false, true
	}
	for _, layout := range localTimeLayouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, true, true
		}
	}
	return time.Time{}, false, false
}

// Custom unmarshaler only needed for metric_a/b/c — they might arrive as strings
// Everything else (sections) is passed through as raw JSON untouched
func (tm *TelemetryMessage) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	if v, ok := raw["tenant_id"]; ok {
		json.Unmarshal(v, &tm.TenantID)
	}
	if v, ok := raw["device_id"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			tm.DeviceID = &s
		}
	}
	if v, ok := raw["machine_id"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			tm.MachineID = &s
		}
	}
	if v, ok := raw["lot_id"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			tm.LotID = &s
		}
	}

	if v, ok := raw["kind"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			tm.Kind = &s
		}
	}
	if v, ok := raw["job_ref"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			tm.JobRef = &s
		}
	}
	if v, ok := raw["started_at"]; ok {
		if t, local, ok := parseJobTime(v); ok {
			tm.StartedAt = &t
			tm.StartedAtLocal = local
		}
	}
	if v, ok := raw["ended_at"]; ok {
		if t, local, ok := parseJobTime(v); ok {
			tm.EndedAt = &t
			tm.EndedAtLocal = local
		}
	}

	// metric_a/b/c: handle string or number
	for _, key := range []string{"metric_a", "metric_b", "metric_c"} {
		v, ok := raw[key]
		if !ok {
			continue
		}
		var anything any
		json.Unmarshal(v, &anything)
		f := parseStringFloat(anything)
		switch key {
		case "metric_a":
			tm.MetricA = f
		case "metric_b":
			tm.MetricB = f
		case "metric_c":
			tm.MetricC = f
		}
	}

	// Sections: zero-copy, pass raw bytes straight through
	tm.Readings = raw["readings"]
	tm.Output = raw["output"]
	tm.Status = raw["status"]
	tm.Limits = raw["limits"]
	tm.Energy = raw["energy"]

	return nil
}
