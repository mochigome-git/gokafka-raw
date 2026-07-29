// db/telemetry.go
package db

import (
	"context"
	"encoding/json"

	"gokafka-raw/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func InsertTelemetryRaw(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	if msg.TenantID == "" {
		logger.Warnw("skipping telemetry_raw insert: empty tenant_id", "device_id", msg.DeviceID)
		return nil
	}

	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)
	energy, _ := model.ValidateJSON(msg.Energy)

	readings = sanitizeRawJSON(readings) // ← added
	output = sanitizeRawJSON(output)     // ← added
	status = sanitizeRawJSON(status)     // ← added
	limits = sanitizeRawJSON(limits)     // ← added
	energy = sanitizeRawJSON(energy)     // ← added

	_, err := pool.Exec(ctx, `
        INSERT INTO telemetry.telemetry_raw
            (tenant_id, device_id, lot_id,
             metric_a, metric_b, metric_c,
             readings, output, status, limits, energy,
             created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    `,
		msg.TenantID, nullUUID(msg.DeviceID), nullUUID(msg.LotID),
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
		nullableJSON(energy),
	)
	if err != nil {
		logger.Errorw("failed to insert telemetry_raw", "error", err)
		return err
	}

	_ = UpdateDeviceOnline(ctx, pool, msg.DeviceID, logger)
	return nil
}

// InsertEventMetric and InsertRealtimeMetric: identical pattern — same
// five sanitizeRawJSON(...) lines inserted right after the ValidateJSON
// block, before the pool.Exec call, in both functions. (Omitted here for
// length — same five lines, same position, in each.)

func nullableJSON(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	s := string(raw)
	return &s
}

func nullUUID(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}
