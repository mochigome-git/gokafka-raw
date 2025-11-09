package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gokafka-raw/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type DBManager struct {
	pool          *pgxpool.Pool
	metricConfigs []config.MetricConfig
	mu            sync.RWMutex
	shutdownChan  chan struct{}
	wg            sync.WaitGroup
	logger        *zap.SugaredLogger
	shutdownOnce  sync.Once
}

func NewDBManager(ctx context.Context, cfg *config.Config, logger *zap.SugaredLogger) (*DBManager, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DBURL)
	if err != nil {
		return nil, err
	}

	// Disable prepared statements to avoid the "prepared statement already exists" error
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return nil
	}
	poolConfig.ConnConfig.TLSConfig = cfg.CreatePostgresTLSConfig()
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute
	poolConfig.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	// Rest of your function remains the same...
	manager := &DBManager{
		pool:         pool,
		shutdownChan: make(chan struct{}),
		logger:       logger,
	}

	manager.StartAutoReconnect(ctx)
	return manager, nil
}

func (d *DBManager) Pool() *pgxpool.Pool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.pool
}

// StartAutoReconnect periodically pings DB and reconnects if needed
func (d *DBManager) StartAutoReconnect(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.shutdownChan:
				d.logger.Info("Auto-reconnect stopped: shutdown signal received")
				return
			case <-ticker.C:
				if err := d.pool.Ping(ctx); err != nil {
					d.logger.Errorw("DB ping failed", "error", err)
				} else {
					d.logger.Debug("DB ping successful")
				}
			}
		}
	}()
}

// Shutdown gracefully stops the DBManager
func (d *DBManager) Shutdown() {
	d.shutdownOnce.Do(func() {
		d.logger.Info("Initiating DBManager graceful shutdown...")

		// Signal all goroutines to stop
		close(d.shutdownChan)

		// Wait for goroutines to finish
		d.wg.Wait()

		// Close the connection pool safely
		d.mu.Lock()
		if d.pool != nil {
			d.pool.Close()
			d.logger.Info("Database connection pool closed")
		}
		d.mu.Unlock()

		d.logger.Info("DBManager shutdown completed")
		fmt.Println("✅ DBManager shutdown completed")
	})
}

// IsShuttingDown returns true if shutdown has been initiated
func (d *DBManager) IsShuttingDown() bool {
	select {
	case <-d.shutdownChan:
		return true
	default:
		return false
	}
}

// GetMetricConfigs returns a copy of the current metric configs
func (d *DBManager) GetMetricConfigs() []config.MetricConfig {
	d.mu.RLock()
	defer d.mu.RUnlock()

	configs := make([]config.MetricConfig, len(d.metricConfigs))
	copy(configs, d.metricConfigs)
	return configs
}

func (m *DBManager) Ping(ctx context.Context) error {
	return m.pool.Ping(ctx)
}
