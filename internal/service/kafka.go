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

	// Channels
	processCh   chan ProcessJob
	telemetryCh chan func()   // telemetry insert
	realtimeCh  chan func()   // realtime insert
	eventCh     chan func()   // event insert
	insertSem   chan struct{} // semaphore to limit concurrent inserts

	// Counters
	activeProcessWorkers int32
	activeInsertWorkers  int32
	RealtimeCount        int32
}

type ProcessJob struct {
	Msg   kafka.Message
	Ctx   context.Context
	Stats *db.InsertStats
}

// Constructor
func NewKafkaService(dbMgr *db.DBManager, logger *zap.SugaredLogger, metricConfigs []config.MetricConfig, hub *realtime.Hub) *KafkaService {
	maxInsertWorkers := 10

	s := &KafkaService{
		DBMgr:         dbMgr,
		Logger:        logger,
		MetricConfigs: metricConfigs,
		RealtimeHub:   hub,

		processCh:   make(chan ProcessJob, 1000),
		telemetryCh: make(chan func(), 1000),
		realtimeCh:  make(chan func(), 500),
		eventCh:     make(chan func(), 500),
		insertSem:   make(chan struct{}, maxInsertWorkers),
	}

	// Start JSON/Data workers
	for i := 0; i < 10; i++ {
		go s.processWorker()
	}

	// Start insert workers per type
	for i := 0; i < maxInsertWorkers; i++ {
		go s.insertWorker(s.telemetryCh)
		go s.insertWorker(s.realtimeCh)
		go s.insertWorker(s.eventCh)
	}

	return s
}

func (k *KafkaService) UpdateMetricConfigs(newConfigs []config.MetricConfig) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.MetricConfigs = newConfigs
	k.Logger.Infow("KafkaService metric configs updated", "count", len(newConfigs))
}

func (s *KafkaService) queueInserts(msg model.TelemetryMessage, m kafka.Message, ctx context.Context, stats *db.InsertStats) {
	entityID := resolveEntityID(msg)

	// Telemetry raw
	s.telemetryCh <- func() {
		if err := db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
			s.Logger.Errorw("failed to insert telemetry_raw", "error", err)
		} else {
			stats.IncrementTelemetry()
		}
	}

	// Metrics
	for _, cfg := range s.MetricConfigs {
		if cfg.TenantID != msg.TenantID || cfg.EntityID != entityID {
			continue
		}

		switch cfg.Method {
		case "realtime":
			// atomic.AddInt32(&s.RealtimeCount, 1)
			// fmt.Println(atomic.LoadInt32(&s.RealtimeCount), "realtime go")

			s.realtimeCh <- func() {
				if err := db.InsertRealtimeMetric(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
					s.Logger.Errorw("failed to insert realtime metric", "error", err)
				} else {
					stats.IncrementRealtime()

					if s.RealtimeHub != nil && msg.DeviceID != nil {
						payload, _ := json.Marshal(msg)
						// Fully non-blocking broadcast

						go func(tid string, did string, p []byte) {
							s.RealtimeHub.BroadcastTo(tid, did, p)
						}(msg.TenantID, *msg.DeviceID, payload)
					}
				}

			}

		case "event":
			eventMsg := model.EventMetricMessage{
				TenantID:  msg.TenantID,
				DeviceID:  msg.DeviceID,
				MachineID: msg.MachineID,
				LotID:     msg.LotID,
				Data:      msg.Data,
			}

			s.eventCh <- func() {
				if err := db.InsertEventMetric(ctx, s.DBMgr.Pool(), eventMsg, m.Time, s.Logger); err != nil {
					s.Logger.Errorw("failed to insert event metric", "error", err)
				} else {
					stats.IncrementEvent()
				}
			}
		}
	}

	// Realtime trigger
	if msg.DeviceID != nil && *msg.DeviceID != "" {
		deviceID := *msg.DeviceID
		for _, cfg := range s.MetricConfigs {
			if cfg.DeviceID == deviceID && cfg.IsRealtime && cfg.IsActive {
				s.realtimeCh <- func() {
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
	s.processCh <- ProcessJob{Msg: m, Ctx: ctx, Stats: stats}
}

// Internal consumer loop
func (s *KafkaService) consumeLoop(ctx context.Context, reader *kafka.Reader, stats *db.InsertStats) error {

	if reader != nil {
		cfg := reader.Config()
		s.Logger.Infow("starting Kafka consumer", "brokers", cfg.Brokers, "topic", cfg.Topic, "groupID", cfg.GroupID)
	}

	for {

		m, err := reader.ReadMessage(ctx)
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

// Public RunConsumer with reconnect/backoff
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

		// Consume messages using the real reader
		consumeErr := s.consumeLoop(ctx, reader, stats)

		if consumeErr != nil {
			// Stop on context cancellation
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
			continue // retry consumeLoop with the same reader
		} else {
			return
		}
	}
}
