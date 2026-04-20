package model

import (
	"encoding/json"
	"fmt"
	"strconv"
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

	return nil
}
