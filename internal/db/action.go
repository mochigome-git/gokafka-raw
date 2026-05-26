package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"gokafka-raw/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// =====================================================================
// Telemetry inserts
// =====================================================================

// InsertTelemetryRaw inserts into telemetry.telemetry_raw (new structure)
func InsertTelemetryRaw(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage, logger *zap.SugaredLogger) error {
	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)
	energy, _ := model.ValidateJSON(msg.Energy)

	_, err := pool.Exec(ctx, `
        INSERT INTO telemetry.telemetry_raw
            (tenant_id, device_id, lot_id,
             metric_a, metric_b, metric_c,
             readings, output, status, limits, energy,
             created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
    `,
		msg.TenantID, msg.DeviceID, msg.LotID,
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
	energy, _ := model.ValidateJSON(msg.Energy)

	// Resolve kind: use what the device sent, otherwise default to 'event'.
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
		msg.TenantID, msg.DeviceID, msg.MachineID, msg.LotID,
		kind, createdAt,
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
		nullableJSON(energy),
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
		msg.TenantID, msg.DeviceID, msg.LotID,
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
		nullableJSON(energy),
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

// =====================================================================
// Realtime doorbell trigger
//
// Each device gets its own tiny table `device.realtime_<uuid>`. The
// backend INSERTs a row whenever fresh telemetry lands; Supabase
// Realtime broadcasts that INSERT to subscribed dashboards, which then
// invalidate their React Query cache and re-fetch the actual data.
//
// Three setup steps are needed PER DEVICE and they all need to happen
// exactly once:
//   1. CREATE TABLE
//   2. ENABLE RLS + permissive read policy (gated through
//      metric_method_config.tenant_id + has_permission())
//   3. ADD TABLE to supabase_realtime publication
//
// We cache "setup done" in-process so we only run those steps the
// first time we see a device after a restart. Steady-state inserts
// run the hot path only (one INSERT + occasional cleanup).
// =====================================================================

// realtimeSetupDone tracks which devices have had their doorbell table
// fully set up since this process started. Keyed by deviceID.
var realtimeSetupDone sync.Map // map[string]struct{}

// insertCounter — how often we should bother running cleanup.
// Cleanup runs on ~1 in N inserts to keep the hot path cheap.
const cleanupEveryNInserts = 100

// InsertRealtimeTrigger inserts a ping row into the per-device doorbell
// table, setting it up (with RLS + publication) on first use. Cleanup
// runs probabilistically to keep the hot path cheap.
func InsertRealtimeTrigger(ctx context.Context, pool *pgxpool.Pool, deviceID string, logger *zap.SugaredLogger) error {
	if deviceID == "" {
		return fmt.Errorf("deviceID is required")
	}

	safeDeviceID := strings.ToLower(strings.ReplaceAll(deviceID, "-", "_"))
	tableName := fmt.Sprintf("device.realtime_%s", safeDeviceID)

	// ── One-time setup per device per process lifetime ────────────────
	if _, alreadyDone := realtimeSetupDone.Load(deviceID); !alreadyDone {
		if err := setupDoorbellTable(ctx, pool, tableName, safeDeviceID, deviceID, logger); err != nil {
			// Don't mark as done — we'll retry on the next insert. Log
			// loudly so this device's broken realtime is visible.
			logger.Errorw("doorbell setup failed — realtime will not work for this device until next attempt",
				"device_id", deviceID,
				"table", tableName,
				"error", err)
			return err
		}
		realtimeSetupDone.Store(deviceID, struct{}{})
	}

	// ── Hot path: insert the ping ─────────────────────────────────────
	if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (inserted) VALUES (TRUE)`, tableName)); err != nil {
		logger.Errorw("failed to insert realtime trigger", "table", tableName, "error", err)
		return err
	}

	// ── Cleanup ~1% of the time ───────────────────────────────────────
	// At 10 inserts/sec/device this runs ~6 times per minute, keeping
	// the table within ~100 rows of the 1000 cap on average. Probabilistic
	// (rather than every-N counter) avoids needing per-device locks.
	if rand.Intn(cleanupEveryNInserts) == 0 {
		cleanupSQL := fmt.Sprintf(`
            DELETE FROM %s
            WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT 1000)
        `, tableName, tableName)
		if _, err := pool.Exec(ctx, cleanupSQL); err != nil {
			logger.Warnw("cleanup failed", "table", tableName, "error", err)
		}
	}

	return nil
}

// setupDoorbellTable runs the one-time DDL for a device's doorbell:
// schema, table, RLS policy, and publication entry. Idempotent —
// safe to call again if it fails partway through.
func setupDoorbellTable(ctx context.Context, pool *pgxpool.Pool, tableName, safeDeviceID, deviceID string, logger *zap.SugaredLogger) error {
	// 1. Schema
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS device`); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	// 2. Table
	createSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id         BIGSERIAL PRIMARY KEY,
            inserted   BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    `, tableName)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// 3. RLS + permissive read policy
	// The doorbell carries no payload (id, bool, timestamp), so the
	// read gate is whether the user has dashboard 'view' permission
	// on the device's tenant. has_permission() is the existing helper.
	rlsSQL := fmt.Sprintf(`
        ALTER TABLE %s ENABLE ROW LEVEL SECURITY;

        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies
                WHERE schemaname = 'device'
                  AND tablename  = 'realtime_%s'
                  AND policyname = 'doorbell_read_authorized'
            ) THEN
                CREATE POLICY doorbell_read_authorized
                ON %s
                FOR SELECT TO authenticated
                USING (
                    EXISTS (
                        SELECT 1
                        FROM public.metric_method_config c
                        WHERE c.device_id = '%s'::uuid
                          AND has_permission(c.tenant_id, 'dashboards', 'view')
                    )
                );
            END IF;
        END
        $$;
    `, tableName, safeDeviceID, tableName, deviceID)
	if _, err := pool.Exec(ctx, rlsSQL); err != nil {
		return fmt.Errorf("setup RLS: %w", err)
	}

	// 4. Add to realtime publication
	// duplicate_object means it's already added — that's fine.
	pubSQL := fmt.Sprintf(`
        DO $$
        BEGIN
            ALTER PUBLICATION supabase_realtime ADD TABLE %s;
        EXCEPTION WHEN duplicate_object THEN NULL;
        END
        $$;
    `, tableName)
	if _, err := pool.Exec(ctx, pubSQL); err != nil {
		return fmt.Errorf("add to publication: %w", err)
	}

	logger.Infow("doorbell table set up", "table", tableName)
	return nil
}

// =====================================================================
// Misc helpers
// =====================================================================

func SelectTenantIDByUserID(ctx context.Context, pool *pgxpool.Pool, userID string) (string, error) {
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
