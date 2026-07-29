package db

import (
	"context"
	"time"

	"gokafka-raw/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// =====================================================================
// Event inserts
// =====================================================================

func InsertEventMetric(ctx context.Context, pool *pgxpool.Pool, msg model.EventMetricMessage, createdAt time.Time, logger *zap.SugaredLogger) error {
	if msg.DeviceID == nil && msg.MachineID == nil {
		logger.Warnw("skipping event metric: device_id or machine_id required")
		return nil
	}

	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)
	energy, _ := model.ValidateJSON(msg.Energy)

	kind := "event"
	if msg.Kind != nil && *msg.Kind != "" {
		switch *msg.Kind {
		case "agg", "event", "output":
			kind = *msg.Kind
		default:
			logger.Warnw("invalid kind from device, defaulting to 'event'",
				"received", *msg.Kind,
				"tenant_id", msg.TenantID,
				"device_id", msg.DeviceID)
		}
	}

	_, err := pool.Exec(ctx, `
        INSERT INTO analytics.metrics
            (tenant_id, device_id, machine_id, lot_id,
             resolution, kind, created_at,
             metric_a, metric_b, metric_c,
             readings, output, status, limits, energy)
        VALUES ($1,$2,$3,$4,'event',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (tenant_id, entity_id, resolution, created_at)
        DO UPDATE SET
            kind     = EXCLUDED.kind,
            metric_a = COALESCE(EXCLUDED.metric_a, analytics.metrics.metric_a),
            metric_b = COALESCE(EXCLUDED.metric_b, analytics.metrics.metric_b),
            metric_c = COALESCE(EXCLUDED.metric_c, analytics.metrics.metric_c),
            readings = COALESCE(EXCLUDED.readings, analytics.metrics.readings),
            output   = COALESCE(EXCLUDED.output,   analytics.metrics.output),
            status   = COALESCE(EXCLUDED.status,   analytics.metrics.status),
            limits   = COALESCE(EXCLUDED.limits,   analytics.metrics.limits),
            energy   = COALESCE(EXCLUDED.energy,   analytics.metrics.energy),
            lot_id   = COALESCE(EXCLUDED.lot_id,   analytics.metrics.lot_id)
    `,
		msg.TenantID, msg.DeviceID, msg.MachineID, nullUUID(msg.LotID),
		kind, createdAt,
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
		nullableJSON(energy),
	)

	if err != nil {
		logger.Errorw("failed to insert event metric", "error", err)
		return err
	}

	if msg.DeviceID != nil {
		ringDoorbell(ctx, pool, *msg.DeviceID, logger)
	}
	return nil
}

// =====================================================================
// Realtime inserts
// =====================================================================

func InsertRealtimeMetric(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)
	energy, _ := model.ValidateJSON(msg.Energy)

	_, err := pool.Exec(ctx, `
        INSERT INTO analytics.raw_metrics
            (tenant_id, device_id, lot_id,
             metric_a, metric_b, metric_c,
             readings, output, status, limits, energy,
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
            energy   = EXCLUDED.energy,
            lot_id   = EXCLUDED.lot_id
    `,
		msg.TenantID, msg.DeviceID, nullUUID(msg.LotID),
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
		nullableJSON(energy),
	)

	if err != nil {
		logger.Errorw("failed to insert realtime metric", "error", err)
		return err
	}

	if msg.DeviceID != nil {
		ringDoorbell(ctx, pool, *msg.DeviceID, logger)
	}
	return nil
}
