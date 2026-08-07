package db

import (
	"context"
	"errors"

	"gokafka-raw/internal/model"

	"go.uber.org/zap"
	// NOTE: match the pool type your other Insert* functions use
	// (e.g. check the signature of InsertTelemetryRaw / InsertJobSummary
	// in this package) — swap this import/type if it differs.
	"github.com/jackc/pgx/v5/pgxpool"
)

// InsertLot inserts a new lot row keyed only by machine_id, matching:
//
//	INSERT INTO production.lot (machine_id) VALUES ($1)
func InsertLot(ctx context.Context, pool *pgxpool.Pool, msg model.LotMessage, logger *zap.SugaredLogger) error {
	const query = `INSERT INTO production.lot (machine_id) VALUES ($1)`

	if msg.MachineID == "" {
		return errors.New("lot insert: empty machine_id")
	}

	if _, err := pool.Exec(ctx, query, msg.MachineID); err != nil {
		logger.Errorw("failed to insert lot", "machine_id", msg.MachineID, "error", err)
		return err
	}
	return nil
}
