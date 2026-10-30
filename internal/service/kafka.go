package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"gokafka-raw/internal/config"
	"gokafka-raw/internal/db"
	"gokafka-raw/internal/model"
	"gokafka-raw/internal/realtime"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Retry policy for DB inserts. Only transient errors retry (see
// isRetryable in retry.go); permanent errors fail fast and dead-letter.
const (
	dbRetryMaxAttempts = 3
	dbRetryBaseDelay   = 200 * time.Millisecond
)

type KafkaService struct {
	DBMgr         *db.DBManager
	mu            sync.RWMutex
	Logger        *zap.SugaredLogger
	MetricConfigs []config.MetricConfig
	RealtimeHub   *realtime.Hub
	offsets       *offsetTracker

	// Channels
	processCh   chan ProcessJob
	telemetryCh chan func()
	realtimeCh  chan func()
	eventCh     chan func()
	jobCh       chan func()
	commitCh    chan commitUpdate // CHANGED — feeds the single committer goroutine

	// Per-type semaphores.
	//
	// IMPORTANT: total workers (currently 4+4+4+3 = 15) should stay
	// BELOW pgxpool MaxConns in db_manager.go (raise MaxConns to 20+),
	// otherwise excess workers block inside pool.Exec waiting for a
	// connection under burst load — a source of intermittent transient
	// errors. Size these two together, never independently.
	telemetrySem chan struct{}
	realtimeSem  chan struct{}
	eventSem     chan struct{}
	jobSem       chan struct{}

	activeProcessWorkers int32
	activeInsertWorkers  int32
	RealtimeCount        int32

	pending sync.WaitGroup // tracks in-flight messages for graceful drain
}

type ProcessJob struct {
	Msg   kafka.Message
	Ctx   context.Context
	Stats *db.InsertStats
	Wg    *sync.WaitGroup
}

// Constructor
func NewKafkaService(dbMgr *db.DBManager, logger *zap.SugaredLogger, metricConfigs []config.MetricConfig, hub *realtime.Hub) *KafkaService {
	// Channel buffers CHANGED — shrunk from 1000/1000/500/500/200.
	// Big buffers don't add throughput when DB is the bottleneck; they
	// only add memory pressure (each queued closure holds its full
	// message copy) and widen the in-memory loss window on a hard kill.
	// Backpressure now reaches the consume loop sooner, which is the
	// safe direction: unread messages stay in Kafka, not in RAM.
	const (
		telemetryWorkers = 4
		realtimeWorkers  = 4
		eventWorkers     = 4
		jobWorkers       = 3
	)

	s := &KafkaService{
		DBMgr:         dbMgr,
		Logger:        logger,
		MetricConfigs: metricConfigs,
		RealtimeHub:   hub,
		offsets:       newOffsetTracker(),

		processCh:   make(chan ProcessJob, 200),
		telemetryCh: make(chan func(), 200),
		realtimeCh:  make(chan func(), 100),
		eventCh:     make(chan func(), 100),
		jobCh:       make(chan func(), 50),
		commitCh:    make(chan commitUpdate, 256),

		telemetrySem: make(chan struct{}, telemetryWorkers),
		realtimeSem:  make(chan struct{}, realtimeWorkers),
		eventSem:     make(chan struct{}, eventWorkers),
		jobSem:       make(chan struct{}, jobWorkers),
	}

	// Start JSON/Data workers
	for range 10 {
		go s.processWorker()
	}

	// Start insert workers per type, each with its own semaphore
	for range telemetryWorkers {
		go s.insertWorker(s.telemetryCh, s.telemetrySem)
	}
	for range realtimeWorkers {
		go s.insertWorker(s.realtimeCh, s.realtimeSem)
	}
	for range eventWorkers {
		go s.insertWorker(s.eventCh, s.eventSem)
	}
	for range jobWorkers {
		go s.insertWorker(s.jobCh, s.jobSem)
	}

	return s
}

func (k *KafkaService) UpdateMetricConfigs(newConfigs []config.MetricConfig) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.MetricConfigs = newConfigs
	k.Logger.Infow("KafkaService metric configs updated", "count", len(newConfigs))
}

// runInsert wraps every DB insert with the full reliability stack:
// retry (transient only) → by-design-skip detection → dead-letter on
// permanent failure. Returns true iff the row was actually inserted,
// so callers only increment stats on real inserts (CHANGED — previously
// a skip returned nil and inflated the counters).
func (s *KafkaService) runInsert(ctx context.Context, m kafka.Message, insertType string,
	tenantID string, deviceID *string, jobRef *string, fn func() error) bool {

	err := retryDB(ctx, dbRetryMaxAttempts, dbRetryBaseDelay, fn)
	if err == nil {
		return true
	}
	if errors.Is(err, db.ErrSkipped) {
		// By-design skip (e.g. empty tenant_id): not an insert, not a
		// failure. No stats, no DLQ, no retry.
		return false
	}
	if errors.Is(err, db.ErrAlreadyProcessed) {
		// Idempotent redelivery (retry/rebalance/restart already
		// inserted this offset). Not a fresh insert — don't double-count
		// stats — but also not a failure worth logging as one.
		return false
	}
	s.Logger.Errorw("insert permanently failed — dead-lettering",
		"insert_type", insertType, "error", err,
		"tenant_id", tenantID, "device_id", deviceID, "job_ref", jobRef,
		"partition", m.Partition, "offset", m.Offset)
	s.deadLetter(m, insertType, tenantID, deviceID, jobRef, err)
	return false
}

func (s *KafkaService) queueInserts(msg model.TelemetryMessage, m kafka.Message, ctx context.Context, stats *db.InsertStats, wg *sync.WaitGroup) {

	if msg.TenantID == "" {
		// By-design safety guard (kept): tenant-less messages are never
		// inserted. Logged with offsets so they're traceable, then
		// dropped — the offset still commits (wg has no pending adds).
		s.Logger.Warnw("skipping message: empty tenant_id",
			"device_id", msg.DeviceID, "partition", m.Partition, "offset", m.Offset)
		return
	}

	entityID := resolveEntityID(msg)

	if isHeartbeat(msg) {
		wg.Add(1)
		s.telemetryCh <- func() {
			defer wg.Done()
			if s.runInsert(ctx, m, "telemetry_raw", msg.TenantID, msg.DeviceID, nil, func() error {
				return db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, m.Topic, m.Partition, m.Offset, s.Logger)
			}) {
				stats.IncrementTelemetry()
			}
		}
		return
	}

	if msg.Kind != nil && *msg.Kind == "job" {
		wg.Add(1)
		s.telemetryCh <- func() {
			defer wg.Done()
			if s.runInsert(ctx, m, "telemetry_raw", msg.TenantID, msg.DeviceID, msg.JobRef, func() error {
				return db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, m.Topic, m.Partition, m.Offset, s.Logger)
			}) {
				stats.IncrementTelemetry()
			}
		}

		if msg.DeviceID == nil || *msg.DeviceID == "" ||
			msg.StartedAt == nil || msg.EndedAt == nil {
			s.Logger.Warnw("skipping job summary: missing required fields",
				"tenant_id", msg.TenantID, "device_id", msg.DeviceID,
				"started_at", msg.StartedAt, "ended_at", msg.EndedAt,
				"partition", m.Partition, "offset", m.Offset)
			return
		}

		jobRef := ""
		if msg.JobRef != nil && *msg.JobRef != "" {
			jobRef = *msg.JobRef
		} else {
			jobRef = fmt.Sprintf("auto-%s-%d", *msg.DeviceID, msg.EndedAt.Unix())
			s.Logger.Warnw("job_ref missing, using generated fallback",
				"tenant_id", msg.TenantID, "device_id", msg.DeviceID, "generated_job_ref", jobRef,
				"partition", m.Partition, "offset", m.Offset)
		}

		jobMsg := model.JobSummaryMessage{
			TenantID:  msg.TenantID,
			DeviceID:  *msg.DeviceID,
			LotID:     msg.LotID,
			JobRef:    jobRef,
			StartedAt: *msg.StartedAt,
			EndedAt:   *msg.EndedAt,
			// PLC wall-clock values get the tenant timezone in SQL
			StartedAtLocal: msg.StartedAtLocal,
			EndedAtLocal:   msg.EndedAtLocal,
			Output:         msg.Output,
		}
		wg.Add(1)
		s.jobCh <- func() {
			defer wg.Done()
			if s.runInsert(ctx, m, "job_summary", jobMsg.TenantID, &jobMsg.DeviceID, &jobMsg.JobRef, func() error {
				return db.InsertJobSummary(ctx, s.DBMgr.Pool(), jobMsg, s.Logger)
			}) {
				stats.IncrementEvent()
			}
		}
		return
	}

	if msg.Kind != nil && *msg.Kind == "lot" {
		if msg.MachineID == nil || *msg.MachineID == "" {
			s.Logger.Warnw("skipping lot: missing machine_id", "tenant_id", msg.TenantID,
				"partition", m.Partition, "offset", m.Offset)
			return
		}

		lotMsg := model.LotMessage{
			MachineID: *msg.MachineID,
		}

		wg.Add(1)
		s.jobCh <- func() {
			defer wg.Done()
			if s.runInsert(ctx, m, "lot", msg.TenantID, msg.MachineID, nil, func() error {
				return db.InsertLot(ctx, s.DBMgr.Pool(), lotMsg, s.Logger)
			}) {
				stats.IncrementEvent()
			}
		}
		return
	}

	s.mu.RLock()
	configs := make([]config.MetricConfig, len(s.MetricConfigs))
	copy(configs, s.MetricConfigs)
	s.mu.RUnlock()

	wg.Add(1)
	s.telemetryCh <- func() {
		defer wg.Done()
		if s.runInsert(ctx, m, "telemetry_raw", msg.TenantID, msg.DeviceID, nil, func() error {
			return db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, m.Topic, m.Partition, m.Offset, s.Logger)
		}) {
			stats.IncrementTelemetry()
		}
	}

	for _, cfg := range configs {
		if cfg.TenantID != msg.TenantID || cfg.DeviceID != entityID {
			continue
		}
		switch cfg.Method {
		case "realtime":
			wg.Add(1)
			s.realtimeCh <- func() {
				defer wg.Done()
				if !s.runInsert(ctx, m, "realtime_metric", msg.TenantID, msg.DeviceID, nil, func() error {
					return db.InsertRealtimeMetric(ctx, s.DBMgr.Pool(), msg, s.Logger)
				}) {
					return
				}
				stats.IncrementRealtime()
				if s.RealtimeHub != nil && msg.DeviceID != nil {
					payload, _ := json.Marshal(msg)
					go func(tid string, did string, p []byte) {
						s.RealtimeHub.BroadcastTo(tid, did, p)
					}(msg.TenantID, *msg.DeviceID, payload)
				}
			}

		case "event":
			eventMsg := model.EventMetricMessage{
				TenantID: msg.TenantID,
				DeviceID: msg.DeviceID,
				LotID:    msg.LotID,
				MetricA:  msg.MetricA,
				MetricB:  msg.MetricB,
				MetricC:  msg.MetricC,
				Readings: msg.Readings,
				Output:   msg.Output,
				Status:   msg.Status,
				Limits:   msg.Limits,
				Energy:   msg.Energy,
				Kind:     msg.Kind,
			}
			kafkaTime := m.Time
			wg.Add(1)
			s.eventCh <- func() {
				defer wg.Done()
				if s.runInsert(ctx, m, "event_metric", eventMsg.TenantID, eventMsg.DeviceID, nil, func() error {
					return db.InsertEventMetric(ctx, s.DBMgr.Pool(), eventMsg, kafkaTime, s.Logger)
				}) {
					stats.IncrementEvent()
				}
			}
		}
	}

	if msg.DeviceID != nil && *msg.DeviceID != "" {
		deviceID := *msg.DeviceID
		for _, cfg := range configs {
			if cfg.DeviceID == deviceID && cfg.IsRealtime && cfg.IsActive {
				wg.Add(1)
				s.realtimeCh <- func() {
					defer wg.Done()
					s.runInsert(ctx, m, "realtime_trigger", msg.TenantID, &deviceID, nil, func() error {
						return db.InsertRealtimeTrigger(ctx, s.DBMgr.Pool(), deviceID, s.Logger)
					})
				}
				break
			}
		}
	}
}

func (s *KafkaService) EnqueueMessage(ctx context.Context, m kafka.Message, stats *db.InsertStats) {
	if resynced := s.offsets.registerRead(m.Partition, m.Offset); resynced {
		s.Logger.Warnw("offset tracker resynced after partition rewind (rebalance?)",
			"partition", m.Partition, "offset", m.Offset)
	}
	wg := &sync.WaitGroup{}
	s.processCh <- ProcessJob{Msg: m, Ctx: ctx, Stats: stats, Wg: wg}
}

func (s *KafkaService) consumeLoop(ctx context.Context, reader *kafka.Reader, stats *db.InsertStats) error {
	if reader != nil {
		cfg := reader.Config()
		s.Logger.Infow("starting Kafka consumer", "brokers", cfg.Brokers, "topic", cfg.Topic, "groupID", cfg.GroupID)
	}

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.Logger.Info("consumer context canceled, stopping consumer loop")
				return nil
			}
			if errors.Is(err, io.EOF) {
				s.Logger.Debug("Kafka EOF reached, waiting for new messages...")
				time.Sleep(2 * time.Second)
				continue
			}
			return fmt.Errorf("error reading message: %w", err)
		}

		s.EnqueueMessage(ctx, m, stats)
	}
}

// WaitPending blocks until all in-flight messages have finished their
// fan-out inserts (or dead-lettered), or the timeout elapses. Call this
// on shutdown BEFORE closing commitCh so the committer can flush the
// final offsets.
func (s *KafkaService) WaitPending(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.pending.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Shutdown drains in-flight work and flushes the last commits.
// Call from your app's signal handler AFTER canceling the consumer ctx:
//
//	cancel()                      // stops FetchMessage
//	svc.Shutdown(30*time.Second)  // drain inserts, flush commits
func (s *KafkaService) Shutdown(timeout time.Duration) {
	if ok := s.WaitPending(timeout); !ok {
		s.Logger.Warnw("shutdown: timed out waiting for in-flight inserts — some offsets will redeliver on restart (safe: inserts are idempotent)")
	}
	close(s.commitCh) // committer does a final flush and exits
	time.Sleep(500 * time.Millisecond)
}

func (s *KafkaService) StartConsumer(ctx context.Context, reader *kafka.Reader, stats *db.InsertStats) {
	if reader == nil {
		fmt.Println("⚠️ Kafka reader is nil. Consumer not started.")
		return
	}

	// Single committer goroutine — the only writer to CommitMessages
	// (CHANGED: removes the s.Reader data race and per-message commit
	// RPCs). Depth logger makes backpressure stalls visible.
	go s.committer(reader)
	s.StartDepthLogger(ctx, 30*time.Second)

	backoff := 5 * time.Second
	maxBackoff := 2 * time.Minute

	cfg := reader.Config()
	fmt.Printf("✅ Connecting to Kafka brokers: %v\n", cfg.Brokers)
	fmt.Printf("✅ Subscribing to topic: %s\n🟢 Consumer group: %s\n", cfg.Topic, cfg.GroupID)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("🛑 Kafka consumer context canceled, stopping...")
			return
		default:
		}

		consumeErr := s.consumeLoop(ctx, reader, stats)

		if consumeErr != nil {
			if errors.Is(consumeErr, context.Canceled) || errors.Is(consumeErr, context.DeadlineExceeded) {
				fmt.Println("🛑 Kafka consumer stopped due to context cancellation")
				return
			}

			fmt.Printf("⚠️ Kafka consumer error: %v\n", consumeErr)
			fmt.Printf("⏳ Retrying in %s...\n", backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		} else {
			return
		}
	}
}
