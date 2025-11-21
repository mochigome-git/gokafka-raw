package db

import (
	"fmt"
	"sync"
	"time"
)

// InsertStats keeps track of insert counts
type InsertStats struct {
	sync.Mutex
	TelemetryCount int
	EventCount     int
	RealtimeCount  int
}

// NewInsertStats starts the periodic logger
func NewInsertStats() *InsertStats {
	stats := &InsertStats{}
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			stats.Lock()
			fmt.Printf("📊 Telemetry inserts: %d | Event inserts: %d 🕒\n", stats.TelemetryCount, stats.EventCount)
			stats.Unlock()
		}
	}()
	return stats
}

// IncrementTelemetry increments telemetry counter
func (s *InsertStats) IncrementTelemetry() {
	s.Lock()
	s.TelemetryCount++
	s.Unlock()
}

// IncrementEvent increments event metric counter
func (s *InsertStats) IncrementEvent() {
	s.Lock()
	s.EventCount++
	s.Unlock()
}

// IncrementRealtime increments realtime counter
func (s *InsertStats) IncrementRealtime() {
	s.Lock()
	s.RealtimeCount++
	s.Unlock()
}
