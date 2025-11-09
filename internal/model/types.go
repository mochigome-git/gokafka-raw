package model

type TelemetryMessage struct {
	TenantID  string         `json:"tenant_id"`
	DeviceID  *string        `json:"device_id,omitempty"`
	MachineID *string        `json:"machine_id,omitempty"`
	Core1     *StringFloat64 `json:"core_1,omitempty"`
	Core2     *StringFloat64 `json:"core_2,omitempty"`
	Core3     *StringFloat64 `json:"core_3,omitempty"`
	LotID     *string        `json:"lot_id,omitempty"`
	Data      map[string]any `json:"data"`
}

type EventMetricMessage struct {
	TenantID  string         `json:"tenant_id"`
	DeviceID  *string        `json:"device_id,omitempty"`
	MachineID *string        `json:"machine_id,omitempty"`
	LotID     *string        `json:"lot_id,omitempty"`
	Data      map[string]any `json:"data"`
}

// Added the key needed
type KafkaWrapper struct {
	Payload string `json:"payload"`
	// ignore other fields if not needed
}
