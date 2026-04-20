package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gokafka-raw/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// InsertTelemetryRaw inserts into telemetry.telemetry_raw (new structure)
func InsertTelemetryRaw(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)

	_, err := pool.Exec(ctx, `
        INSERT INTO telemetry.telemetry_raw
            (tenant_id, device_id, machine_id, lot_id,
             metric_a, metric_b, metric_c,
             readings, output, status, limits,
             created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    `,
		msg.TenantID, msg.DeviceID, msg.MachineID, msg.LotID,
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
	)

	if err != nil {
		logger.Errorw("failed to insert telemetry_raw", "error", err)
		return err
	}

	_ = UpdateDeviceOnline(ctx, pool, msg.DeviceID, logger)
	return nil
}

// InsertEventMetric inserts into analytics.metrics with resolution='event'
func InsertEventMetric(ctx context.Context, pool *pgxpool.Pool, msg model.EventMetricMessage, createdAt time.Time, logger *zap.SugaredLogger) error {
	if msg.DeviceID == nil && msg.MachineID == nil {
		logger.Warnw("skipping event metric: device_id or machine_id required")
		return nil
	}

	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)

	_, err := pool.Exec(ctx, `
        INSERT INTO analytics.metrics
            (tenant_id, device_id, machine_id, lot_id,
             resolution, created_at,
             metric_a, metric_b, metric_c,
             readings, output, status, limits)
        VALUES ($1,$2,$3,$4,'event',$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (tenant_id, entity_id, resolution, created_at)
        DO UPDATE SET
            metric_a = COALESCE(EXCLUDED.metric_a, analytics.metrics.metric_a),
            metric_b = COALESCE(EXCLUDED.metric_b, analytics.metrics.metric_b),
            metric_c = COALESCE(EXCLUDED.metric_c, analytics.metrics.metric_c),
            readings = COALESCE(EXCLUDED.readings, analytics.metrics.readings),
            output   = COALESCE(EXCLUDED.output,   analytics.metrics.output),
            status   = COALESCE(EXCLUDED.status,   analytics.metrics.status),
            limits   = COALESCE(EXCLUDED.limits,   analytics.metrics.limits),
            lot_id   = COALESCE(EXCLUDED.lot_id,   analytics.metrics.lot_id)
    `,
		msg.TenantID, msg.DeviceID, msg.MachineID, msg.LotID,
		createdAt,
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
	)

	if err != nil {
		logger.Errorw("failed to insert event metric", "error", err)
	}
	return err
}

// InsertRealtimeMetric inserts into analytics.raw_metrics (new structure)
func InsertRealtimeMetric(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)

	_, err := pool.Exec(ctx, `
        INSERT INTO analytics.raw_metrics
            (tenant_id, device_id, machine_id, lot_id,
             metric_a, metric_b, metric_c,
             readings, output, status, limits,
             created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
        ON CONFLICT (tenant_id, entity_id, created_at)
        DO UPDATE SET
            metric_a = EXCLUDED.metric_a,
            metric_b = EXCLUDED.metric_b,
            metric_c = EXCLUDED.metric_c,
            readings = EXCLUDED.readings,
            output   = EXCLUDED.output,
            status   = EXCLUDED.status,
            limits   = EXCLUDED.limits,
            lot_id   = EXCLUDED.lot_id
    `,
		msg.TenantID, msg.DeviceID, msg.MachineID, msg.LotID,
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
	)

	if err != nil {
		logger.Errorw("failed to insert realtime metric", "error", err)
	}
	return err
}

// nullableJSON returns nil if the JSON is empty/null, otherwise returns the string
func nullableJSON(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	s := string(raw)
	return &s
}

// InsertRealtimeTrigger — unchanged, still creates device.realtime_<device_id> table
func InsertRealtimeTrigger(ctx context.Context, pool *pgxpool.Pool, deviceID string, logger *zap.SugaredLogger) error {
	if deviceID == "" {
		return fmt.Errorf("deviceID is required")
	}

	safeDeviceID := strings.ToLower(strings.ReplaceAll(deviceID, "-", "_"))
	tableName := fmt.Sprintf("device.realtime_%s", safeDeviceID)

	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS device`); err != nil {
		logger.Errorw("failed to ensure schema exists", "error", err)
		return err
	}

	createSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id         BIGSERIAL PRIMARY KEY,
            inserted   BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    `, tableName)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		logger.Errorw("failed to create realtime table", "table", tableName, "error", err)
		return err
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (inserted) VALUES (TRUE)`, tableName)); err != nil {
		logger.Errorw("failed to insert realtime trigger", "table", tableName, "error", err)
		return err
	}

	cleanupSQL := fmt.Sprintf(`
        DELETE FROM %s
        WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT 1000)
    `, tableName, tableName)
	if _, err := pool.Exec(ctx, cleanupSQL); err != nil {
		logger.Warnw("cleanup failed", "table", tableName, "error", err)
	}

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

// UpdateDeviceOnline updates last_seen and status to online for a device
func UpdateDeviceOnline(ctx context.Context, pool *pgxpool.Pool, deviceID *string, logger *zap.SugaredLogger) error {
	if deviceID == nil || *deviceID == "" {
		return nil
	}

	_, err := pool.Exec(ctx, `
		UPDATE device.device_list
		SET 
			last_seen = NOW(),
			status = 'online'
		WHERE id = $1
		AND (last_seen IS NULL OR last_seen < NOW() - INTERVAL '1 minute')
	`, *deviceID)

	if err != nil {
		logger.Errorw("failed to update device online status", "device_id", *deviceID, "error", err)
	}

	return err
}
