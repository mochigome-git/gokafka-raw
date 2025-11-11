package main

import (
	"context"

	"go.uber.org/zap"

	"gokafka-raw/internal/app"
	"gokafka-raw/internal/config"
	"gokafka-raw/internal/db"
	"gokafka-raw/internal/monitor"
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

	// --- Initialize Realtime Service ---
	rtSvc, err := app.StartRealtimeApp(ctx, cfg, sugar)
	if err != nil {
		sugar.Fatalw("failed to run realtime app", "error", err)
	}
	defer rtSvc.Shutdown()

	// --- Start Health Check ---
	monitor.StartHealthCheck(dbMgr, rtSvc, sugar, ":8080")

	// --- Run Kafka consumer app (blocking) ---
	app.StartKafkaApp(ctx, dbMgr, cfg, sugar, rtSvc)

}
