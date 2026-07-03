package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration.
type Config struct {
	// Redis
	RedisAddr    string
	RedisCluster bool
	RedisAddrs   []string // For cluster mode: comma-separated list of host:port

	// Stream
	StreamName     string
	GroupName      string
	ConsumerName   string
	WorkerCount    int
	FetchBatchSize int64
	BlockTimeout   time.Duration

	// Server
	APIPort     string
	MetricsPort string

	// Security
	CORSOrigins []string
	APIKey      string

	// Behavior
	MockTaskInterval time.Duration
	MaxRetries       int
	EnableMockTasks  bool
	EnablePriority   bool // Use priority-based stream routing

	// DLQ
	DLQStreamName string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		RedisAddr:        envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisCluster:     envBoolOrDefault("REDIS_CLUSTER", false),
		RedisAddrs:       envListOrDefault("REDIS_ADDRS", []string{"localhost:6379"}),
		StreamName:       envOrDefault("STREAM_NAME", "buraq_tasks"),
		GroupName:        envOrDefault("GROUP_NAME", "buraq_workers"),
		ConsumerName:     envOrDefault("CONSUMER_NAME", "worker_node_1"),
		WorkerCount:      envIntOrDefault("WORKER_COUNT", 5),
		FetchBatchSize:   int64(envIntOrDefault("FETCH_BATCH_SIZE", 10)),
		BlockTimeout:     envDurationOrDefault("BLOCK_TIMEOUT", 2*time.Second),
		APIPort:          envOrDefault("API_PORT", ":8080"),
		MetricsPort:      envOrDefault("METRICS_PORT", ":2112"),
		CORSOrigins:      envListOrDefault("CORS_ORIGINS", []string{"http://localhost:3000"}),
		APIKey:           envOrDefault("API_KEY", ""),
		MockTaskInterval: envDurationOrDefault("MOCK_TASK_INTERVAL", 2*time.Second),
		MaxRetries:       envIntOrDefault("MAX_RETRIES", 3),
		EnableMockTasks:  envBoolOrDefault("ENABLE_MOCK_TASKS", true),
		EnablePriority:   envBoolOrDefault("ENABLE_PRIORITY", false),
		DLQStreamName:    envOrDefault("DLQ_STREAM_NAME", "buraq_tasks_dlq"),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func envListOrDefault(key string, fallback []string) []string {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				result = append(result, p)
			}
		}
		return result
	}
	return fallback
}
