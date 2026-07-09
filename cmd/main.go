package main

import (
	"context"

	"go.uber.org/zap"

	"gokafka-raw/internal/app"
	"gokafka-raw/internal/config"
	"gokafka-raw/internal/db"
	"gokafka-raw/internal/monitor"
	"gokafka-raw/internal/realtime"
)

func main() {
	logger, _ := zap.NewProduction(zap.AddStacktrace(zap.FatalLevel))
	defer logger.Sync()
	sugar := logger.Sugar()

	ctx := context.Background()
	cfg := config.LoadConfig(ctx)

	// --- Initialize DBManager ---
	dbMgr, err := db.NewDBManager(ctx, cfg, sugar)
	if err != nil {
		sugar.Fatalw("failed to create DBManager", "error", err)
	}
	defer dbMgr.Shutdown()
	dbMgr.StartAutoReconnect(ctx)

	// FIX #2: start background eviction for socketFlagCache so stale
	// device entries don't accumulate in memory indefinitely.
	db.StartCacheEviction(ctx)

	// --- Initialize Realtime Service ---
	rtSvc, err := app.StartRealtimeApp(ctx, cfg, sugar)
	if err != nil {
		sugar.Fatalw("failed to run realtime app", "error", err)
	}
	defer rtSvc.Shutdown()

	// --- Initialize Realtime Hub ---
	app.StartWebsocketApp(ctx, cfg, sugar)
	hub := realtime.NewHub(sugar)

	// --- Start Health Check ---
	monitor.StartHealthCheck(dbMgr, rtSvc, sugar, hub, ":8080")

	// --- Run Kafka consumer app (blocking) ---
	app.StartKafkaApp(ctx, dbMgr, cfg, sugar, rtSvc, hub)
}
