package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

	// Resolve which numbered slot this instance should use
	groupID := buildGroupID("gokafka-consumer", cfg.KafkaBrokers)
	logger.Infow("kafka consumer group resolved", "groupID", groupID)

	// Kafka Reader Setup
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.KafkaBrokers,
		Topic:             cfg.KafkaTopic,
		GroupID:           groupID,
		StartOffset:       kafka.LastOffset,
		ReadLagInterval:   -1,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
			TLS:       cfg.CreateKafkaTLSConfig(),
		},
		Logger:      kafka.LoggerFunc(logger.Debugf),
		ErrorLogger: kafka.LoggerFunc(logger.Errorf),
	})

	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				st := kafkaReader.Stats()
				logger.Infow("reader stats",
					"messages", st.Messages,
					"lag", st.Lag,
					"offset", st.Offset,
					"rebalances", st.Rebalances,
					"timeouts", st.Timeouts,
					"errors", st.Errors,
					"dialTime_avg", st.DialTime.Avg,
				)
			}
		}
	}()

	// Start insert summary monitor
	stats := db.NewInsertStats()
	fmt.Println("🟢🚀 Insert summary monitor started! Tracking inserts every 30 minutes...")

	metricConfigs := rtSvc.GetMetricConfigs()

	// kafkaSvc must exist before anything below references it
	kafkaSvc := service.NewKafkaService(dbMgr, logger, metricConfigs, hub)

	rtSvc.OnConfigUpdate(func(updated []config.MetricConfig) {
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

	// Wait for consumer goroutine to finish reading
	select {
	case <-done:
		logger.Info("Kafka consumer stopped gracefully")
	case <-time.After(30 * time.Second):
		logger.Warn("timeout waiting for Kafka consumer to stop")
	}

	// NEW — drain in-flight jobs (kafkaSvc now exists, and the reader is
	// still open) before closing the reader. This must happen here, not
	// above, both because kafkaSvc didn't exist yet up there, and because
	// draining only makes sense after the read loop has actually stopped.
	if kafkaSvc.WaitPending(15 * time.Second) {
		logger.Info("all pending Kafka jobs drained cleanly")
	} else {
		logger.Warn("timed out waiting for pending Kafka jobs to drain")
	}

	kafkaReader.Close()

	fmt.Println("✅ Kafka application shutdown completed")
}

// RunRealtimeApp initializes the Realtime service, starts the config watcher
// in a goroutine, and loads initial metric configs. Returns the service instance.
func StartRealtimeApp(ctx context.Context, cfg *config.Config, logger *zap.SugaredLogger) (*service.RealtimeService, error) {
	rtSvc := service.NewRealtimeService(cfg, logger)

	if err := rtSvc.CreateRealtimeClient(cfg.DBRealtimeURL, cfg.DBSupabaseKey); err != nil {
		return nil, err
	}

	go func() {
		if err := rtSvc.StartConfigWatcher(ctx); err != nil {
			logger.Fatalw("failed to start realtime watcher", "error", err)
		}
	}()

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

	realtime.CachedJWKS = jwks

	logger.Infow("Supabase JWKS fetched successfully")
	return nil
}

// buildGroupID picks a slot from a numbered pool (base-1, base-2, ...).
// It lists existing consumer groups on the broker, reuses the lowest-
// numbered slot that isn't currently active (so a restart resumes that
// slot's committed offsets instead of starting fresh), and only
// allocates a brand-new number if every existing slot is currently in
// use. NOTE: each slot is an independent consumer group — if more than
// one slot is active at the same time, each reads the ENTIRE topic
// independently, so downstream inserts must be idempotent.
func buildGroupID(base string, brokers []string) string {
	if gid := os.Getenv("KAFKA_GROUP_ID"); gid != "" {
		return gid
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		panic(fmt.Sprintf("buildGroupID: cannot dial broker: %v", err))
	}
	defer conn.Close()

	client := &kafka.Client{Addr: conn.RemoteAddr()}
	ctx := context.Background()

	listResp, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		panic(fmt.Sprintf("buildGroupID: failed to list consumer groups: %v", err))
	}

	prefix := base + "-"
	existing := map[int]bool{}
	maxN := 0
	for _, g := range listResp.Groups {
		if !strings.HasPrefix(g.GroupID, prefix) {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(g.GroupID, prefix)); err == nil {
			existing[n] = true
			if n > maxN {
				maxN = n
			}
		}
	}

	for n := 1; n <= maxN; n++ {
		if !existing[n] {
			continue
		}
		gid := fmt.Sprintf("%s%d", prefix, n)
		desc, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{gid}})
		if err != nil || len(desc.Groups) == 0 {
			continue
		}
		if desc.Groups[0].GroupState != "Stable" {
			return gid
		}
	}

	return fmt.Sprintf("%s%d", prefix, maxN+1)
}
