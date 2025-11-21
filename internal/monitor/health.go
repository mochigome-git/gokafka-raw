package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"gokafka-raw/internal/db"
	"gokafka-raw/internal/realtime"
	"gokafka-raw/internal/service"

	"go.uber.org/zap"
)

// HealthResponse represents the health check response structure
type HealthResponse struct {
	Status  string            `json:"status"`
	Message string            `json:"message,omitempty"`
	Details map[string]string `json:"details,omitempty"`
}

// StartHealthCheck starts an HTTP server for health checks
func StartHealthCheck(dbMgr *db.DBManager, rtSvc *service.RealtimeService, logger *zap.SugaredLogger, hub *realtime.Hub, addr string) {
	// --- Liveness ---
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(HealthResponse{
			Status:  "alive",
			Message: "Service is running",
		})
	})

	// --- Readiness ---
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		w.Header().Set("Content-Type", "application/json")

		healthDetails := make(map[string]string)
		var errors []string

		if err := dbMgr.Ping(ctx); err != nil {
			healthDetails["database"] = "unhealthy"
			errors = append(errors, fmt.Sprintf("DBManager unhealthy: %v", err))
		} else {
			healthDetails["database"] = "healthy"
		}

		if !rtSvc.IsAlive() {
			healthDetails["realtime_service"] = "unhealthy"
			errors = append(errors, "Realtime client unhealthy")
		} else {
			healthDetails["realtime_service"] = "healthy"
		}

		statusCode := http.StatusOK
		statusMsg := "ready"
		if len(errors) > 0 {
			statusCode = http.StatusServiceUnavailable
			statusMsg = fmt.Sprintf("%d component(s) failing", len(errors))
		}

		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(HealthResponse{
			Status:  statusMsg,
			Details: healthDetails,
		})
	})

	// --- WebSocket endpoint ---
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		realtime.ServeWS(hub, dbMgr, w, r)
	})

	logger.Infof("starting health check server on %s", addr)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil && err != http.ErrServerClosed {
			logger.Errorw("health check server stopped", "error", err)
		}
	}()
}
