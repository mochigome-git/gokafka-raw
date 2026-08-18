package service

import (
	"context"
	"errors"
	"time"

	"gokafka-raw/internal/db"

	"github.com/jackc/pgx/v5/pgconn"
)

// isRetryable classifies DB errors. Permanent errors (constraint
// violations, bad data, SQL errors) will fail identically on every
// attempt — retrying them just burns a worker + pool connection for
// ~600ms per doomed message. Only transient errors (network blips,
// pool timeouts, serialization conflicts, admin disconnects) retry.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	// By-design skips and idempotent redeliveries are never retried
	// (and never dead-lettered) — retrying either would be pointless
	// (skip) or would just re-check the ledger and skip again anyway
	// (already processed).
	if errors.Is(err, db.ErrSkipped) || errors.Is(err, db.ErrAlreadyProcessed) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "23", // integrity constraint violation (e.g. 23514 job_summary_time_check)
			"22", // data exception (bad numeric, invalid text representation)
			"42": // syntax error / undefined table / undefined column
			return false
		}
		// Everything else — 08 (connection), 53 (resources / too many
		// connections), 57 (admin shutdown), 40 (serialization) — is
		// worth retrying.
	}
	// Non-pg errors: net errors, pool acquire timeouts → retryable.
	return true
}

// retryDB retries fn with exponential backoff, bailing out immediately
// on permanent (non-retryable) errors and on ctx cancellation.
// Returns nil on success, or the last error.
func retryDB(ctx context.Context, maxAttempts int, baseDelay time.Duration, fn func() error) error {
	var err error
	delay := baseDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err // permanent — fail fast, caller decides (skip vs dead-letter)
		}
		if attempt == maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return err
}
