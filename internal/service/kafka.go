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

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type KafkaService struct {
	DBMgr         *db.DBManager
	mu            sync.RWMutex
	Logger        *zap.SugaredLogger
	MetricConfigs []config.MetricConfig
}

func NewKafkaService(dbMgr *db.DBManager, logger *zap.SugaredLogger, metricConfigs []config.MetricConfig) *KafkaService {
	return &KafkaService{
		DBMgr:         dbMgr,
		Logger:        logger,
		MetricConfigs: metricConfigs,
	}
}

func (k *KafkaService) UpdateMetricConfigs(newConfigs []config.MetricConfig) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.MetricConfigs = newConfigs
	k.Logger.Infow("KafkaService metric configs updated", "count", len(newConfigs))
}

// ProcessMessage keeps using logger internally
func (s *KafkaService) ProcessMessage(ctx context.Context, m kafka.Message, stats *db.InsertStats) {

	var wrapper model.KafkaWrapper
	if err := json.Unmarshal(m.Value, &wrapper); err != nil {
		s.Logger.Errorw("failed to parse wrapper message", "error", err)
		return
	}

	var msg model.TelemetryMessage
	if err := json.Unmarshal([]byte(wrapper.Payload), &msg); err != nil {
		s.Logger.Errorw("failed to parse telemetry payload", "error", err)
		return
	}

	if err := msg.PopulateData([]byte(wrapper.Payload)); err != nil {
		s.Logger.Errorw("failed to populate data field", "error", err)
		return
	}

	if err := db.InsertTelemetryRaw(ctx, s.DBMgr.Pool(), msg, s.Logger); err != nil {
		s.Logger.Errorw("failed to insert telemetry_raw", "error", err, "msg", msg)
	} else {
		stats.IncrementTelemetry()
	}

	var entityID string
	if msg.DeviceID != nil && *msg.DeviceID != "" {
		entityID = *msg.DeviceID
	} else if msg.MachineID != nil && *msg.MachineID != "" {
		entityID = *msg.MachineID
	}

	for _, cfg := range s.MetricConfigs {
		if cfg.TenantID == msg.TenantID && cfg.Method == "event" && cfg.EntityID == entityID {
			var deviceIDPtr, machineIDPtr *string
			if msg.DeviceID != nil && *msg.DeviceID != "" {
				deviceIDPtr = msg.DeviceID
			}
			if msg.MachineID != nil && *msg.MachineID != "" {
				machineIDPtr = msg.MachineID
			}

			eventMsg := model.EventMetricMessage{
				TenantID:  msg.TenantID,
				DeviceID:  deviceIDPtr,
				MachineID: machineIDPtr,
				LotID:     msg.LotID,
				Data:      msg.Data,
			}

			if err := db.InsertEventMetric(ctx, s.DBMgr.Pool(), eventMsg, m.Time, s.Logger); err != nil {
				s.Logger.Errorw("failed to insert event metric", "error", err, "msg", eventMsg)
			} else {
				stats.IncrementEvent()
			}

			break
		}

	}

	// Handle realtime socket trigger for this device only
	if msg.DeviceID != nil && *msg.DeviceID != "" {
		deviceID := *msg.DeviceID
		for _, cfg := range s.MetricConfigs {
			// only trigger if the device in config matches the message
			if cfg.DeviceID == deviceID && cfg.IsRealtime && cfg.IsActive {
				if err := db.InsertRealtimeTrigger(ctx, s.DBMgr.Pool(), deviceID, s.Logger); err != nil {
					s.Logger.Errorw("failed to insert realtime trigger", "error", err, "device", cfg.DeviceID)
				}
				break // found the matching device, no need to continue loop
			}
		}
	}

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

		s.ProcessMessage(ctx, m, stats)
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
