package model

import (
	"encoding/json"
	"math"
)

// cleanJSONData recursively cleans data to ensure it's JSON serializable
func cleanJSONData(data map[string]any) map[string]any {
	cleaned := make(map[string]any)
	for k, v := range data {
		switch val := v.(type) {
		case float64:
			// Handle special float values that can't be JSON serialized
			if math.IsInf(val, 0) || math.IsNaN(val) {
				cleaned[k] = nil
			} else {
				cleaned[k] = val
			}
		case map[string]any:
			cleaned[k] = cleanJSONData(val)
		case []any:
			cleaned[k] = cleanJSONSlice(val)
		default:
			cleaned[k] = v
		}
	}
	return cleaned
}

func cleanJSONSlice(slice []any) []any {
	cleaned := make([]any, len(slice))
	for i, v := range slice {
		switch val := v.(type) {
		case float64:
			if math.IsInf(val, 0) || math.IsNaN(val) {
				cleaned[i] = nil
			} else {
				cleaned[i] = val
			}
		case map[string]any:
			cleaned[i] = cleanJSONData(val)
		case []any:
			cleaned[i] = cleanJSONSlice(val)
		default:
			cleaned[i] = v
		}
	}
	return cleaned
}

// validateJSON ensures the data can be marshaled to JSON
func ValidateJSON(data map[string]any) ([]byte, error) {
	cleaned := cleanJSONData(data)
	return json.Marshal(cleaned)
}
