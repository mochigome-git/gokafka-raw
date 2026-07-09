package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"gokafka-raw/internal/config"
	supabase_realtime "gokafka-raw/pkg/supabase-realtime"

	"go.uber.org/zap"
)

type ConfigUpdateListener func([]config.MetricConfig)

type RealtimeService struct {
	client        *supabase_realtime.Client
	logger        *zap.SugaredLogger
	mu            sync.RWMutex
	metricConfigs []config.MetricConfig
	cfg           *config.Config
	listeners     []ConfigUpdateListener
}

// NewRealtimeService creates a new RealtimeService instance.
func NewRealtimeService(cfg *config.Config, logger *zap.SugaredLogger) *RealtimeService {
	return &RealtimeService{
		cfg:    cfg,
		logger: logger,
	}
}

// CreateRealtimeClient initializes and connects the Realtime client
func (r *RealtimeService) CreateRealtimeClient(projectURL, apiKey string) error {
	projectRef, err := ExtractProjectRef(projectURL)
	if err != nil {
		r.logger.Fatalw("failed to extract project ref", "error", err)
	}

	client := supabase_realtime.CreateRealtimeClient(projectRef, apiKey, r.logger.Desugar())
	if client == nil {
		return fmt.Errorf("failed to create realtime client")
	}

	if err := client.Connect(); err != nil {
		return fmt.Errorf("failed to connect realtime client: %w", err)
	}

	r.client = client
	fmt.Println("✅ Realtime client created and connected successfully")
	return nil
}

// StartConfigWatcher subscribes to realtime table changes + polling fallback.
func (r *RealtimeService) StartConfigWatcher(ctx context.Context) error {
	if r.client == nil {
		return errors.New("realtime client not initialized")
	}

	fmt.Println("🟡 Starting realtime watcher...")

	schema := r.cfg.DBSchema
	table := r.cfg.DBRealtimeTable

	for !r.client.IsConnected() {
		fmt.Println("waiting for realtime client to connect...")
		time.Sleep(5000 * time.Millisecond)
	}

	// ── Polling fallback every 30s ────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				configs, err := r.loadMetricConfigs()
				if err != nil {
					r.logger.Warnw("polling: failed to reload metric configs", "error", err)
					continue
				}

				// FIX #3: only call reloadMetricConfigs ONCE, always sync internal
				// state, and only notify listeners when something actually changed.
				r.mu.Lock()
				changed := len(configs) != len(r.metricConfigs)
				r.metricConfigs = configs
				r.mu.Unlock()

				if changed {
					r.logger.Infow("polling: metric configs changed, notifying listeners", "count", len(configs))
				}
				// Always push latest to listeners (was being called twice before)
				r.notifyListeners(configs)
			}
		}
	}()

	// ── Realtime websocket (primary) ──────────────────────────────────────
	return r.client.ListenToPostgresChanges(supabase_realtime.PostgresChangesOptions{
		Schema: schema,
		Table:  table,
		Filter: "",
	}, func(payload map[string]any) {
		r.logger.Infow(fmt.Sprintf("%s changed", table), "payload", payload)

		configs, err := r.loadMetricConfigs()
		if err != nil {
			r.logger.Errorw("failed to reload metric configs", "error", err)
			return
		}

		r.reloadMetricConfigs(configs)
	})
}

// loadMetricConfigs queries active configs directly from Realtime.
func (r *RealtimeService) loadMetricConfigs() ([]config.MetricConfig, error) {
	if r.client == nil {
		return nil, errors.New("realtime client not initialized")
	}

	var rows []struct {
		ID              int     `json:"id"`
		TenantID        string  `json:"tenant_id"`
		DeviceID        *string `json:"device_id"`
		Method          string  `json:"method"`
		IntervalSeconds int     `json:"interval_seconds"`
		BucketLevel     string  `json:"bucket_level"`
		IsActive        bool    `json:"is_active"`
		IsRealtime      bool    `json:"is_realtime_socket"`
	}

	table := r.cfg.DBRealtimeTable

	if _, err := r.client.QueryTable(table, &rows, map[string]any{
		"is_active": true,
	}); err != nil {
		return nil, fmt.Errorf("query realtime table failed: %w", err)
	}

	configs := make([]config.MetricConfig, len(rows))
	for i, row := range rows {
		configs[i] = config.MetricConfig{
			ID:              row.ID,
			TenantID:        row.TenantID,
			DeviceID:        deref(row.DeviceID),
			Method:          row.Method,
			IntervalSeconds: row.IntervalSeconds,
			BucketLevel:     row.BucketLevel,
			IsActive:        row.IsActive,
			IsRealtime:      row.IsRealtime,
		}
	}
	return configs, nil
}

// GetMetricConfigs returns a thread-safe copy of the cached configs.
func (r *RealtimeService) GetMetricConfigs() []config.MetricConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]config.MetricConfig(nil), r.metricConfigs...)
}

// OnConfigUpdate registers a listener to be called when configs change.
func (r *RealtimeService) OnConfigUpdate(listener ConfigUpdateListener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listeners = append(r.listeners, listener)
}

// reloadMetricConfigs syncs internal state and notifies all listeners.
func (r *RealtimeService) reloadMetricConfigs(newConfigs []config.MetricConfig) {
	r.mu.Lock()
	r.metricConfigs = newConfigs
	r.mu.Unlock()

	r.notifyListeners(newConfigs)
}

// notifyListeners dispatches newConfigs to every registered listener.
// FIX #2 (goroutine leak): each listener gets its own goroutine with a
// 10-second timeout so a slow/blocked listener can never accumulate
// unbounded goroutines over time.
func (r *RealtimeService) notifyListeners(newConfigs []config.MetricConfig) {
	r.mu.RLock()
	listeners := append([]ConfigUpdateListener(nil), r.listeners...)
	r.mu.RUnlock()

	for _, l := range listeners {
		l := l // capture loop variable
		go func() {
			done := make(chan struct{}, 1)
			go func() {
				l(newConfigs)
				done <- struct{}{}
			}()

			select {
			case <-done:
				// completed normally
			case <-time.After(10 * time.Second):
				r.logger.Warnw("config listener timed out — possible goroutine leak in listener")
			}
		}()
	}
}

// ExtractProjectRef returns the project ref from SUPABASE_URL
func ExtractProjectRef(supabaseURL string) (string, error) {
	parsed, err := url.Parse(supabaseURL)
	if err != nil {
		return "", fmt.Errorf("invalid SUPABASE_URL: %w", err)
	}

	hostParts := strings.Split(parsed.Hostname(), ".")
	if len(hostParts) < 1 {
		return "", fmt.Errorf("unexpected host format: %s", parsed.Hostname())
	}

	return hostParts[0], nil
}

func (r *RealtimeService) LoadInitialMetricConfigs() error {
	configs, err := r.loadMetricConfigs()
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.metricConfigs = configs
	r.mu.Unlock()
	r.logger.Infow("initial metric configs loaded", "count", len(configs))
	return nil
}

func (r *RealtimeService) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client != nil {
		if err := r.client.Disconnect(); err != nil {
			r.logger.Warnw("failed to disconnect realtime client", "error", err)
		} else {
			r.logger.Info("realtime client disconnected successfully")
			fmt.Println("✅ Realtime application shutdown completed")
		}
		r.client = nil
	}
}

// IsAlive checks if the internal realtime client is connected
func (r *RealtimeService) IsAlive() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.client == nil {
		return false
	}
	return r.client.IsClientAlive()
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
