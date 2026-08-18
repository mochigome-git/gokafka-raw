package db

import "errors"

// ErrSkipped marks a by-design skip (e.g. empty tenant_id). Never
// retried, never dead-lettered, never counted as a successful insert.
var ErrSkipped = errors.New("insert skipped by design")

// ErrAlreadyProcessed marks a message whose (topic, partition, offset,
// insert_type) is already recorded in ingest.processed_offsets — a
// redelivery (retry, rebalance, restart) of something already
// successfully inserted. Never retried, never dead-lettered, never
// double-counted in stats. Distinguishing this from a fresh success is
// what makes telemetry_raw safe under at-least-once Kafka delivery
// despite it being a partitioned table that can't carry a normal
// (kafka_partition, kafka_offset) unique index.
var ErrAlreadyProcessed = errors.New("message already processed (idempotent skip)")
