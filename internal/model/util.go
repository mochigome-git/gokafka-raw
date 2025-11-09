package model

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type StringFloat64 float64

// UnmarshalJSON allows StringFloat64 to accept both string and float in JSON
func (s *StringFloat64) UnmarshalJSON(b []byte) error {
	// try number
	var f float64
	if err := json.Unmarshal(b, &f); err == nil {
		*s = StringFloat64(f)
		return nil
	}

	// try string
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

// UnmarshalJSON handles dynamic types and collects unknown fields into Data
func (tm *TelemetryMessage) UnmarshalJSON(b []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	tm.Data = make(map[string]any)

	for k, v := range raw {
		switch k {
		case "tenant_id":
			if s, ok := v.(string); ok {
				tm.TenantID = s
			}
		case "device_id":
			if s, ok := v.(string); ok {
				tm.DeviceID = &s
			}
		case "machine_id":
			if s, ok := v.(string); ok {
				tm.MachineID = &s
			}
		case "core_1":
			tm.Core1 = parseStringFloat(v)
		case "core_2":
			tm.Core2 = parseStringFloat(v)
		case "core_3":
			tm.Core3 = parseStringFloat(v)
		case "lot_id":
			if s, ok := v.(string); ok {
				tm.LotID = &s
			}
		case "data":
			if m, ok := v.(map[string]any); ok {
				tm.Data = m
			}
		default:
			tm.Data[k] = v
		}
	}

	return nil
}

// parseStringFloat converts any number/string to *StringFloat64
func parseStringFloat(v any) *StringFloat64 {
	var f float64
	switch val := v.(type) {
	case float64:
		f = val
	case string:
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			f = parsed
		} else {
			return nil
		}
	default:
		return nil
	}
	sf := StringFloat64(f)
	return &sf
}

// KnownFields lists all the fields that are part of the main struct
var KnownFields = map[string]bool{
	"tenant_id":  true,
	"device_id":  true,
	"machine_id": true,
	"core_1":     true,
	"core_2":     true,
	"core_3":     true,
	"lot_id":     true,
	"data":       true,
}

// Helper function to populate Data field
func (tm *TelemetryMessage) PopulateData(rawJSON []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(rawJSON, &raw); err != nil {
		return err
	}

	if tm.Data == nil {
		tm.Data = make(map[string]any)
	}

	for key, value := range raw {
		if !KnownFields[key] {
			tm.Data[key] = value
		}
	}

	return nil
}
