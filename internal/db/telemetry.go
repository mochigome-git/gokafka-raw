// db/telemetry.go
package db

import (
	"context"
	"encoding/json"
	"errors"

	"gokafka-raw/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// InsertTelemetryRaw writes one telemetry row, idempotently.
//
// telemetry_raw is a PARTITIONED table (partition key: created_at), so
// Postgres won't allow a unique index on (kafka_partition, kafka_offset)
// alone — and even if it did, created_at=NOW() would differ across
// redelivery attempts and defeat the dedup anyway. Instead, idempotency
// is enforced via ingest.processed_offsets: a small, non-partitioned
// ledger table, checked and written in the SAME transaction as the real
// insert, so the two can never disagree.
//
// topic/kafkaPartition/kafkaOffset (CHANGED — new params) identify the
// Kafka message for both the ledger check and the DLQ, should this fail
// permanently upstream.
func InsertTelemetryRaw(ctx context.Context, pool *pgxpool.Pool, msg model.TelemetryMessage,
	topic string, kafkaPartition int, kafkaOffset int64, logger *zap.SugaredLogger) error {

	if msg.TenantID == "" {
		// By-design safety guard: never insert tenant-less rows.
		logger.Warnw("skipping telemetry_raw insert: empty tenant_id", "device_id", msg.DeviceID)
		return ErrSkipped
	}

	readings, _ := model.ValidateJSON(msg.Readings)
	output, _ := model.ValidateJSON(msg.Output)
	status, _ := model.ValidateJSON(msg.Status)
	limits, _ := model.ValidateJSON(msg.Limits)
	energy, _ := model.ValidateJSON(msg.Energy)

	readings = sanitizeRawJSON(readings)
	output = sanitizeRawJSON(output)
	status = sanitizeRawJSON(status)
	limits = sanitizeRawJSON(limits)
	energy = sanitizeRawJSON(energy)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	// Claim this offset in the ledger first. ON CONFLICT DO NOTHING means
	// a redelivery affects 0 rows — that's our idempotent-skip signal.
	tag, err := tx.Exec(ctx, `
        INSERT INTO ingest.processed_offsets (topic, partition, kafka_offset, insert_type)
        VALUES ($1,$2,$3,'telemetry_raw')
        ON CONFLICT DO NOTHING
    `, topic, kafkaPartition, kafkaOffset)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Already processed by a prior attempt — nothing to do. Rollback
		// (via defer) is harmless since nothing else was written.
		return ErrAlreadyProcessed
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO telemetry.telemetry_raw
            (tenant_id, device_id, lot_id,
             metric_a, metric_b, metric_c,
             readings, output, status, limits, energy,
             kafka_partition, kafka_offset,
             created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
    `,
		msg.TenantID, nullUUID(msg.DeviceID), nullUUID(msg.LotID),
		msg.MetricA, msg.MetricB, msg.MetricC,
		nullableJSON(readings), nullableJSON(output), nullableJSON(status), nullableJSON(limits),
		nullableJSON(energy),
		kafkaPartition, kafkaOffset,
	)
	if err != nil {
		logger.Errorw("failed to insert telemetry_raw", "error", err)
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	_ = UpdateDeviceOnline(ctx, pool, msg.DeviceID, logger)
	return nil
}

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

// IsAlreadyProcessed is a small helper for callers (kafka.go's runInsert)
// that don't want to import errors.Is + the sentinel directly at every
// call site.
func IsAlreadyProcessed(err error) bool {
	return errors.Is(err, ErrAlreadyProcessed)
}
