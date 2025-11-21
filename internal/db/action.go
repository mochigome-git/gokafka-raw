package db

import (
	"context"
	"fmt"
	"strings"
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

// Realtime
func InsertRealtimeMetric(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	// Use validated JSON marshaling
	dataJSON, err := model.ValidateJSON(msg.Data)
	if err != nil {
		logger.Errorw("failed to marshal realtime data", "error", err, "msg", msg)
		return err
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO analytics.realtime_metrics 
			(tenant_id, device_id, machine_id, core_1, core_2, core_3, data, lot_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
	`, msg.TenantID, msg.DeviceID, msg.MachineID, msg.Core1, msg.Core2, msg.Core3, string(dataJSON), msg.LotID)

	if err != nil {
		logger.Errorw("failed to insert analytics.realtime_metrics ", "error", err, "msg", msg)
	}

	return err
}

// InsertRealtimeTrigger inserts a realtime signal into device.realtime_<device_name>
// creates device.realtime_<device_id> if needed,
// then inserts a single TRUE row to trigger Supabase Realtime.
func InsertRealtimeTrigger(ctx context.Context, pool *pgxpool.Pool, deviceID string, logger *zap.SugaredLogger) error {
	if deviceID == "" {
		return fmt.Errorf("deviceID is required")
	}

	// sanitize: replace any illegal chars to make a valid table name
	safeDeviceID := strings.ToLower(strings.ReplaceAll(deviceID, "-", "_"))
	tableName := fmt.Sprintf("device.realtime_%s", safeDeviceID)

	// ensure schema exists
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS device`); err != nil {
		logger.Errorw("failed to ensure schema exists", "error", err)
		return err
	}

	// ensure realtime table exists
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			inserted BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)
	`, tableName)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		logger.Errorw("failed to create realtime table", "table", tableName, "error", err)
		return err
	}

	// insert TRUE to trigger Supabase Realtime
	insertSQL := fmt.Sprintf(`INSERT INTO %s (inserted) VALUES (TRUE)`, tableName)
	if _, err := pool.Exec(ctx, insertSQL); err != nil {
		logger.Errorw("failed to insert realtime trigger", "table", tableName, "error", err)
		return err
	}

	// optional: keep only last 1000 rows
	cleanupSQL := fmt.Sprintf(`
		DELETE FROM %s
		WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT 1000)
	`, tableName, tableName)
	if _, err := pool.Exec(ctx, cleanupSQL); err != nil {
		logger.Warnw("cleanup failed", "table", tableName, "error", err)
	}

	// logger.Infow("realtime trigger inserted", "table", tableName)
	return nil
}

func SelectTenantIDByUserID(ctx context.Context, pool *pgxpool.Pool, userID string,
) (string, error) {

	var tenantID string

	err := pool.QueryRow(ctx, `
		SELECT tenant_id
		FROM user_tenants
		WHERE user_id = $1
		LIMIT 1
	`, userID).Scan(&tenantID)

	if err != nil {
		return "", err
	}

	return tenantID, nil
}
