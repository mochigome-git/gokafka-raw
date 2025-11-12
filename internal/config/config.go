package config

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// MetricConfig represents a metric configuration row
type MetricConfig struct {
	ID              int
	TenantID        string
	DeviceID        string
	EntityID        string
	Method          string // "realtime", "fast", "event", "hour"
	IntervalSeconds int
	BucketLevel     string // "second", "minute", etc.
	IsActive        bool
	IsRealtime      bool
}

// Config holds all configuration values
type Config struct {
	// PostgreSQL / Supabase
	DBHost            string
	DBPort            string
	DBUser            string
	DBPassword        string
	DBName            string
	DBURL             string
	DBCACert          string
	DBSchema          string
	DBTable           string
	DBRealtimeTable   string
	DBForeignKey      string
	DBForeignKeyCheck string
	DBSupabaseKey     string
	DBRealtimeURL     string

	// Kafka
	KafkaBrokers []string
	KafkaTopic   string
	KafkaCACert  string
	KafkaCert    string // optional client cert
	KafkaKey     string // optional client key
}

func LoadConfig(ctx context.Context) *Config {
	// Load .env if exists
	_ = godotenv.Load() // ignore error, fallback to env vars

	cfg := &Config{
		DBHost:     os.Getenv("SUPABASE_DB_HOST"),
		DBPort:     os.Getenv("SUPABASE_DB_PORT"),
		DBUser:     os.Getenv("SUPABASE_DB_USER"),
		DBPassword: os.Getenv("SUPABASE_DB_PASSWORD"),
		DBName:     os.Getenv("SUPABASE_DB_NAME"),
		DBCACert:   os.Getenv("SUPABASE_CA_CERT"),
		DBTable:    os.Getenv("SUPABASE_TABLE"),

		// Realtime function
		DBSchema:          os.Getenv("SUPABASE_REALTIME_SCHEMA"),
		DBRealtimeTable:   os.Getenv("SUPABASE_REALTIME_TABLE"),
		DBForeignKey:      os.Getenv("SUPABASE_REALTIME_FOREIGN_KEY"),
		DBForeignKeyCheck: os.Getenv("SUPABASE_FOREIGN_KEY_CHECK"),
		DBSupabaseKey:     os.Getenv("SUPABASE_KEY"),
		DBRealtimeURL:     os.Getenv("SUPABASE_URL"),

		KafkaBrokers: strings.Split(os.Getenv("KAFKA_BROKER"), ","),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		KafkaCACert:  os.Getenv("KAFKA_CA_CERT"),
		KafkaCert:    os.Getenv("KAFKA_CLIENT_CERT"),
		KafkaKey:     os.Getenv("KAFKA_CLIENT_KEY"),
	}

	// Build DB URL if not provided
	if cfg.DBURL == "" {
		cfg.DBURL = fmt.Sprintf(
			"postgresql://%s:%s@%s:%s/%s?sslmode=verify-full",
			cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName,
		)
	}

	return cfg
}
