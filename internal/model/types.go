package model

import (
	"encoding/json"
)

type TelemetryMessage struct {
	TenantID  string  `json:"tenant_id"`
	DeviceID  *string `json:"device_id"`
	MachineID *string `json:"machine_id"`
	LotID     *string `json:"lot_id"`
	Kind      *string `json:"kind"`

	// Fast-path aggregatable slots
	MetricA *float64 `json:"metric_a"`
	MetricB *float64 `json:"metric_b"`
	MetricC *float64 `json:"metric_c"`

	// Structured payload sections
	Readings json.RawMessage `json:"readings"` // raw sensor values
	Output   json.RawMessage `json:"output"`   // pass/fail, counts (nil = pure sensor)
	Status   json.RawMessage `json:"status"`   // device state, counters
	Limits   json.RawMessage `json:"limits"`   // thresholds at time of capture
	Energy   json.RawMessage `json:"energy"`   // electrical telemetry (V/I/P/kWh)
}

// Event metric message — subset used for event inserts
type EventMetricMessage struct {
	TenantID  string          `json:"tenant_id"`
	DeviceID  *string         `json:"device_id"`
	MachineID *string         `json:"machine_id"`
	LotID     *string         `json:"lot_id"`
	Kind      *string         `json:"kind"`
	MetricA   *float64        `json:"metric_a"`
	MetricB   *float64        `json:"metric_b"`
	MetricC   *float64        `json:"metric_c"`
	Readings  json.RawMessage `json:"readings"`
	Output    json.RawMessage `json:"output"`
	Status    json.RawMessage `json:"status"`
	Limits    json.RawMessage `json:"limits"`
	Energy    json.RawMessage `json:"energy"`
}

// Added the key needed
type KafkaWrapper struct {
	Payload string `json:"payload"`
	// ignore other fields if not needed
}
