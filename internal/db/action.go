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

func nullableJSON(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	s := string(raw)
	return &s
}

// =====================================================================
// Realtime doorbell trigger
// =====================================================================

var realtimeSetupDone sync.Map // map[string]struct{}

type socketFlagEntry struct {
	enabled  bool
	expireAt time.Time
}

var (
	socketFlagCache    sync.Map // map[string]socketFlagEntry
	socketFlagCacheTTL = 60 * time.Second
)

const cleanupEveryNInserts = 100

// StartCacheEviction runs a background goroutine that removes expired
// entries from socketFlagCache every 5 minutes.
// Call this once at startup (e.g. from main or NewDBManager).
// FIX #2: prevents socketFlagCache from growing forever with stale device entries.
func StartCacheEviction(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				socketFlagCache.Range(func(k, v any) bool {
					if v.(socketFlagEntry).expireAt.Before(now) {
						socketFlagCache.Delete(k)
					}
					return true
				})
			}
		}
	}()
}

func ringDoorbell(ctx context.Context, pool *pgxpool.Pool, deviceID string, logger *zap.SugaredLogger) {
	if deviceID == "" {
		return
	}

	enabled, err := isSocketEnabled(ctx, pool, deviceID)
	if err != nil {
		logger.Warnw("doorbell: failed to check socket flag, skipping",
			"device_id", deviceID, "error", err)
		return
	}
	if !enabled {
		return
	}

	safeDeviceID := strings.ToLower(strings.ReplaceAll(deviceID, "-", "_"))
	tableName := fmt.Sprintf("device.realtime_%s", safeDeviceID)

	if _, alreadyDone := realtimeSetupDone.Load(deviceID); !alreadyDone {
		if err := setupDoorbellTable(ctx, pool, tableName, safeDeviceID, deviceID, logger); err != nil {
			logger.Errorw("doorbell setup failed — will retry on next message",
				"device_id", deviceID, "table", tableName, "error", err)
			return
		}
		realtimeSetupDone.Store(deviceID, struct{}{})
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (inserted) VALUES (TRUE)`, tableName)); err != nil {
		logger.Errorw("failed to insert realtime trigger — clearing setup cache for retry",
			"table", tableName, "error", err)
		realtimeSetupDone.Delete(deviceID)
		return
	}

	if rand.Intn(cleanupEveryNInserts) == 0 {
		cleanupSQL := fmt.Sprintf(`
            DELETE FROM %s
            WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT 1000)
        `, tableName, tableName)
		if _, err := pool.Exec(ctx, cleanupSQL); err != nil {
			logger.Warnw("doorbell: cleanup failed", "table", tableName, "error", err)
		}
	}
}

func isSocketEnabled(ctx context.Context, pool *pgxpool.Pool, deviceID string) (bool, error) {
	if v, ok := socketFlagCache.Load(deviceID); ok {
		entry := v.(socketFlagEntry)
		if time.Now().Before(entry.expireAt) {
			return entry.enabled, nil
		}
		// Expired — fall through to re-query and let eviction goroutine clean up
	}

	var enabled bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM public.metric_method_config
			WHERE device_id = $1 AND is_realtime_socket = TRUE
		)
	`, deviceID).Scan(&enabled)

	if err != nil {
		return false, err
	}

	socketFlagCache.Store(deviceID, socketFlagEntry{
		enabled:  enabled,
		expireAt: time.Now().Add(socketFlagCacheTTL),
	})
	return enabled, nil
}

func InvalidateSocketFlagCache(deviceID string) {
	socketFlagCache.Delete(deviceID)
}

func setupDoorbellTable(ctx context.Context, pool *pgxpool.Pool, tableName, safeDeviceID, deviceID string, logger *zap.SugaredLogger) error {
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS device`); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	logger.Infow("doorbell: schema ok", "table", tableName)

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
	logger.Infow("doorbell: table ok", "table", tableName)

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
	logger.Infow("doorbell: RLS ok", "table", tableName)

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
	logger.Infow("doorbell: publication ok", "table", tableName)

	logger.Infow("doorbell table set up", "table", tableName)
	return nil
}

// Deprecated: use ringDoorbell via the standard insert paths instead.
func InsertRealtimeTrigger(ctx context.Context, pool *pgxpool.Pool, deviceID string, logger *zap.SugaredLogger) error {
	if deviceID == "" {
		return fmt.Errorf("deviceID is required")
	}
	ringDoorbell(ctx, pool, deviceID, logger)
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

func nullUUID(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}
