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

type KafkaService struct {
	DBMgr         *db.DBManager
	mu            sync.RWMutex
	Logger        *zap.SugaredLogger
	MetricConfigs []config.MetricConfig
	RealtimeHub   *realtime.Hub
	Reader        *kafka.Reader
	offsets       *offsetTracker

	// Channels
	processCh   chan ProcessJob
	telemetryCh chan func()
	realtimeCh  chan func()
	eventCh     chan func()
	jobCh       chan func()

	// Per-type semaphores — CHANGED from one shared insertSem.
	// Sized conservatively for 0.25 vCPU; raise these once you bump
	// the task's CPU allocation and confirm lag actually responds.
	telemetrySem chan struct{}
	realtimeSem  chan struct{}
	eventSem     chan struct{}
	jobSem       chan struct{}

	activeProcessWorkers int32
	activeInsertWorkers  int32
	RealtimeCount        int32

	pending sync.WaitGroup // tracks in-flight jobs for graceful drain
}

type ProcessJob struct {
	Msg   kafka.Message
	Ctx   context.Context
	Stats *db.InsertStats
	Wg    *sync.WaitGroup
}

// Constructor
func NewKafkaService(dbMgr *db.DBManager, logger *zap.SugaredLogger, metricConfigs []config.MetricConfig, hub *realtime.Hub) *KafkaService {
	// Worker counts per channel — kept modest to match 0.25 vCPU.
	// These control how many goroutines PULL from each channel;
	// the semaphores below control how many can actually be doing
	// DB work (network I/O) at once, which is the real limiter.
	const (
		telemetryWorkers = 6
		realtimeWorkers  = 6
		eventWorkers     = 6
		jobWorkers       = 3
	)

	s := &KafkaService{
		DBMgr:         dbMgr,
		Logger:        logger,
		MetricConfigs: metricConfigs,
		RealtimeHub:   hub,
		offsets:       newOffsetTracker(),

		processCh:   make(chan ProcessJob, 1000),
		telemetryCh: make(chan func(), 1000),
		realtimeCh:  make(chan func(), 500),
		eventCh:     make(chan func(), 500),
		jobCh:       make(chan func(), 200),

		telemetrySem: make(chan struct{}, telemetryWorkers),
		realtimeSem:  make(chan struct{}, realtimeWorkers),
		eventSem:     make(chan struct{}, eventWorkers),
		jobSem:       make(chan struct{}, jobWorkers),
	}

	// Start JSON/Data workers
	for range 10 {
		go s.processWorker()
	}

	// Start insert workers per type, each with its OWN semaphore now
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

func (s *KafkaService) queueInserts(msg model.TelemetryMessage, m kafka.Message, ctx context.Context, stats *db.InsertStats, wg *sync.WaitGroup) {

	if msg.TenantID == "" {
		s.Logger.Warnw("skipping message: empty tenant_id",
			"device_id", msg.DeviceID, "partition", m.Partition, "offset", m.Offset)
		return
	}

	entityID := resolveEntityID(msg)

	if isHeartbeat(msg) {
		wg.Add(1)
		s.telemetryCh <- func() {
			defer wg.Done()
			if err := db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
				s.Logger.Errorw("failed to insert telemetry_raw (heartbeat)", "error", err)
			} else {
				stats.IncrementTelemetry()
			}
		}
		return
	}

	/*
		if msg.DeviceID != nil && *msg.DeviceID == "982a3960-9647-447a-97f0-55031607a47a" {
			if b, err := json.MarshalIndent(msg, "", "  "); err == nil {
				fmt.Printf("📩 parsed msg (offset=%d ts=%s): %s\n", m.Offset, m.Time, string(b))
			}
		}
	*/

	if msg.Kind != nil && *msg.Kind == "job" {
		wg.Add(1)
		s.telemetryCh <- func() {
			defer wg.Done()
			if err := db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
				s.Logger.Errorw("failed to insert telemetry_raw (job)", "error", err)
			} else {
				stats.IncrementTelemetry()
			}
		}

		if msg.DeviceID == nil || *msg.DeviceID == "" ||
			msg.StartedAt == nil || msg.EndedAt == nil {
			s.Logger.Warnw("skipping job summary: missing required fields",
				"tenant_id", msg.TenantID, "device_id", msg.DeviceID,
				"started_at", msg.StartedAt, "ended_at", msg.EndedAt)
			return
		}

		jobRef := ""
		if msg.JobRef != nil && *msg.JobRef != "" {
			jobRef = *msg.JobRef
		} else {
			jobRef = fmt.Sprintf("auto-%s-%d", *msg.DeviceID, msg.EndedAt.Unix())
			s.Logger.Warnw("job_ref missing, using generated fallback",
				"tenant_id", msg.TenantID, "device_id", msg.DeviceID, "generated_job_ref", jobRef)
		}

		jobMsg := model.JobSummaryMessage{
			TenantID:  msg.TenantID,
			DeviceID:  *msg.DeviceID,
			LotID:     msg.LotID,
			JobRef:    jobRef,
			StartedAt: *msg.StartedAt,
			EndedAt:   *msg.EndedAt,
			Output:    msg.Output,
		}
		wg.Add(1)
		s.jobCh <- func() {
			defer wg.Done()
			if err := db.InsertJobSummary(ctx, s.DBMgr.Pool(), jobMsg, s.Logger); err != nil {
				s.Logger.Errorw("failed to insert job summary", "error", err)
			} else {
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
		if err := db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
			s.Logger.Errorw("failed to insert telemetry_raw", "error", err)
		} else {
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
				if err := db.InsertRealtimeMetric(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
					s.Logger.Errorw("failed to insert realtime metric", "error", err)
				} else {
					stats.IncrementRealtime()
					if s.RealtimeHub != nil && msg.DeviceID != nil {
						payload, _ := json.Marshal(msg)
						go func(tid string, did string, p []byte) {
							s.RealtimeHub.BroadcastTo(tid, did, p)
						}(msg.TenantID, *msg.DeviceID, payload)
					}
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
				if err := db.InsertEventMetric(ctx, s.DBMgr.Pool(), eventMsg, kafkaTime, s.Logger); err != nil {
					s.Logger.Errorw("failed to insert event metric", "error", err)
				} else {
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
					if err := db.InsertRealtimeTrigger(ctx, s.DBMgr.Pool(), deviceID, s.Logger); err != nil {
						s.Logger.Errorw("failed to insert realtime trigger", "error", err)
					}
				}
				break
			}
		}
	}
}

func (s *KafkaService) EnqueueMessage(ctx context.Context, m kafka.Message, stats *db.InsertStats) {
	s.offsets.registerRead(m.Partition, m.Offset)
	wg := &sync.WaitGroup{}
	s.processCh <- ProcessJob{Msg: m, Ctx: ctx, Stats: stats, Wg: wg}
}

func (s *KafkaService) consumeLoop(ctx context.Context, reader *kafka.Reader, stats *db.InsertStats) error {
	s.Reader = reader

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

// WaitPending blocks until all in-flight jobs have committed (or errored),
// or the timeout elapses.
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

func (s *KafkaService) StartConsumer(ctx context.Context, reader *kafka.Reader, stats *db.InsertStats) {
	if reader == nil {
		fmt.Println("⚠️ Kafka reader is nil. Consumer not started.")
		return
	}

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
