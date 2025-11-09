package db

import (
	"context"
	"time"

	"gokafka-raw/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// InsertTelemetryRaw inserts a telemetry message into telemetry_raw table
func InsertTelemetryRaw(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	// Use validated JSON marshaling
	dataJSON, err := model.ValidateJSON(msg.Data)
	if err != nil {
		logger.Errorw("failed to marshal telemetry data", "error", err, "msg", msg)
		return err
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO telemetry.telemetry_raw 
			(tenant_id, device_id, machine_id, core_1, core_2, core_3, data, lot_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
	`, msg.TenantID, msg.DeviceID, msg.MachineID, msg.Core1, msg.Core2, msg.Core3, string(dataJSON), msg.LotID)

	if err != nil {
		logger.Errorw("failed to insert telemetry_raw", "error", err, "msg", msg)
	}

	return err
}

// InsertEventMetric inserts an event metric message into event_metrics table
func InsertEventMetric(ctx context.Context, pool *pgxpool.Pool, msg model.EventMetricMessage, createdAt time.Time, logger *zap.SugaredLogger) error {
	if msg.DeviceID == nil && msg.MachineID == nil {
		logger.Warnw("skipping event metric: either device_id or machine_id must be provided", "msg", msg)
		return nil
	}

	dataJSON, err := model.ValidateJSON(msg.Data)
	if err != nil {
		logger.Errorw("failed to marshal event metric data", "error", err, "msg", msg)
		return err
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO analytics.event_metrics
			(tenant_id, machine_id, device_id, data, lot_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, msg.TenantID, msg.MachineID, msg.DeviceID, string(dataJSON), msg.LotID, createdAt)

	if err != nil {
		logger.Errorw("failed to insert event_metric", "error", err, "msg", msg)
	}

	return err
}
