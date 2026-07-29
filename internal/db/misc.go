// db/misc.go

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

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
