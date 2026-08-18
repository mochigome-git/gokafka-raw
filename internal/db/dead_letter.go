package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// DeadLetter captures a message that permanently failed to insert
// (exhausted retries or hit a non-retryable error). The raw payload is
// preserved so a replay tool can re-run the insert after the underlying
// cause (bad data, constraint, schema drift) is fixed.
type DeadLetter struct {
	Topic       string
	Partition   int
	KafkaOffset int64
	TenantID    string // may be empty if parse failed before extraction
	DeviceID    *string
	JobRef      *string
	InsertType  string // "telemetry_raw" | "job_summary" | "event_metric" | "realtime_metric" | "realtime_trigger" | "lot" | "parse"
	RawPayload  []byte // original kafka message value, verbatim
	LastError   string
}

// InsertDeadLetter is the last line of defense — if even this insert
// fails, the caller logs the full payload so it at least lands in the
// log stream. Uses ON CONFLICT DO NOTHING keyed on (topic, partition,
// kafka_offset, insert_type) so a redelivered message that dead-letters
// twice doesn't create duplicate DLQ rows.
func InsertDeadLetter(ctx context.Context, pool *pgxpool.Pool, dl DeadLetter, logger *zap.SugaredLogger) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO ingest.dead_letters
			(topic, partition, kafka_offset, tenant_id, device_id, job_ref,
			 insert_type, raw_payload, last_error)
		VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,$9)
		ON CONFLICT (topic, partition, kafka_offset, insert_type) DO UPDATE SET
			last_error = EXCLUDED.last_error,
			failed_at  = now()
	`,
		dl.Topic, dl.Partition, dl.KafkaOffset, dl.TenantID,
		nullUUID(dl.DeviceID), dl.JobRef,
		dl.InsertType, string(dl.RawPayload), dl.LastError,
	)
	if err != nil {
		logger.Errorw("CRITICAL: dead-letter insert failed — message only preserved in this log line",
			"error", err,
			"topic", dl.Topic, "partition", dl.Partition, "offset", dl.KafkaOffset,
			"insert_type", dl.InsertType, "last_error", dl.LastError,
			"raw_payload", string(dl.RawPayload))
		return err
	}
	logger.Warnw("message dead-lettered",
		"topic", dl.Topic, "partition", dl.Partition, "offset", dl.KafkaOffset,
		"insert_type", dl.InsertType, "tenant_id", dl.TenantID, "last_error", dl.LastError)
	return nil
}
