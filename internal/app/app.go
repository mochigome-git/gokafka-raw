package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gokafka-raw/internal/config"
	"gokafka-raw/internal/db"
	"gokafka-raw/internal/realtime"
	"gokafka-raw/internal/service"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// RunKafkaApp handles Kafka reader setup, consumer start, and graceful shutdown
func StartKafkaApp(ctx context.Context, dbMgr *db.DBManager, cfg *config.Config, logger *zap.SugaredLogger, rtSvc *service.RealtimeService, hub *realtime.Hub) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Kafka Reader Setup
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.KafkaBrokers,
		Topic:             cfg.KafkaTopic,
		GroupID:           "telemetry-consumer",
		StartOffset:       kafka.FirstOffset,
		ReadLagInterval:   -1,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		Dialer: &kafka.Dialer{
			TLS: cfg.CreateKafkaTLSConfig(),
		},
	})
	defer kafkaReader.Close()

	// Start insert summary monitor
	stats := db.NewInsertStats()
	fmt.Println("🟢🚀 Insert summary monitor started! Tracking inserts every 30 minutes...")

	metricConfigs := rtSvc.GetMetricConfigs()

	kafkaSvc := service.NewKafkaService(dbMgr, logger, metricConfigs, hub)
	// listen for realtime config updates
	rtSvc.OnConfigUpdate(func(updated []config.MetricConfig) {
		// logger.Infow("KafkaService updating metric configs", "count", len(updated))
		kafkaSvc.UpdateMetricConfigs(updated)
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		kafkaSvc.StartConsumer(ctx, kafkaReader, stats)
	}()

	// --- Graceful shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigChan:
		logger.Infow("signal received, shutting down Kafka consumer", "signal", sig)
		cancel()
	case <-done:
		logger.Info("Kafka consumer finished, exiting")
	}

	// Wait for consumer goroutine to finish
	select {
	case <-done:
		logger.Info("Kafka consumer stopped gracefully")
	case <-time.After(30 * time.Second):
		logger.Warn("timeout waiting for Kafka consumer to stop")
	}

	fmt.Println("✅ Kafka application shutdown completed")
}

// RunRealtimeApp initializes the Realtime service, starts the config watcher
// in a goroutine, and loads initial metric configs. Returns the service instance.
func StartRealtimeApp(ctx context.Context, cfg *config.Config, logger *zap.SugaredLogger) (*service.RealtimeService, error) {
	rtSvc := service.NewRealtimeService(cfg, logger)

	if err := rtSvc.CreateRealtimeClient(cfg.DBRealtimeURL, cfg.DBSupabaseKey); err != nil {
		return nil, err
	}

	// Start watcher in a separate goroutine
	go func() {
		if err := rtSvc.StartConfigWatcher(ctx); err != nil {
			logger.Fatalw("failed to start realtime watcher", "error", err)
		}
	}()

	// Load initial metric configs (blocking)
	if err := rtSvc.LoadInitialMetricConfigs(); err != nil {
		return nil, err
	}

	return rtSvc, nil
}

func StartWebsocketApp(ctx context.Context, cfg *config.Config, logger *zap.SugaredLogger) error {
	jwksURL := fmt.Sprintf(cfg.DBRealtimeURL)
	jwks, err := realtime.FetchJWKS(jwksURL)
	if err != nil {
		logger.Fatalw("failed to fetch Supabase JWKS", "error", err)
		return err
	}

	// Store it globally
	realtime.CachedJWKS = jwks // <- make sure this is your global variable

	logger.Infow("Supabase JWKS fetched successfully")
	return nil
}
