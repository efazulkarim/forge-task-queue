package config

import (
	"os"
	"testing"
	"time"
)

func clearEnv(keys ...string) {
	for _, k := range keys {
		os.Unsetenv(k)
	}
}

func TestLoad_DefaultValues(t *testing.T) {
	clearEnv(
		"REDIS_ADDR", "STREAM_NAME", "GROUP_NAME", "CONSUMER_NAME",
		"WORKER_COUNT", "FETCH_BATCH_SIZE", "BLOCK_TIMEOUT",
		"API_PORT", "METRICS_PORT", "CORS_ORIGINS", "API_KEY",
		"MOCK_TASK_INTERVAL", "MAX_RETRIES", "ENABLE_MOCK_TASKS",
		"DLQ_STREAM_NAME",
	)

	cfg := Load()

	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr: got %q, want %q", cfg.RedisAddr, "localhost:6379")
	}
	if cfg.StreamName != "buraq_tasks" {
		t.Errorf("StreamName: got %q, want %q", cfg.StreamName, "buraq_tasks")
	}
	if cfg.GroupName != "buraq_workers" {
		t.Errorf("GroupName: got %q, want %q", cfg.GroupName, "buraq_workers")
	}
	if cfg.ConsumerName != "worker_node_1" {
		t.Errorf("ConsumerName: got %q, want %q", cfg.ConsumerName, "worker_node_1")
	}
	if cfg.WorkerCount != 5 {
		t.Errorf("WorkerCount: got %d, want %d", cfg.WorkerCount, 5)
	}
	if cfg.FetchBatchSize != 10 {
		t.Errorf("FetchBatchSize: got %d, want %d", cfg.FetchBatchSize, 10)
	}
	if cfg.BlockTimeout != 2*time.Second {
		t.Errorf("BlockTimeout: got %v, want %v", cfg.BlockTimeout, 2*time.Second)
	}
	if cfg.APIPort != ":8080" {
		t.Errorf("APIPort: got %q, want %q", cfg.APIPort, ":8080")
	}
	if cfg.MetricsPort != ":2112" {
		t.Errorf("MetricsPort: got %q, want %q", cfg.MetricsPort, ":2112")
	}
	if len(cfg.CORSOrigins) != 1 || cfg.CORSOrigins[0] != "http://localhost:3000" {
		t.Errorf("CORSOrigins: got %v, want [http://localhost:3000]", cfg.CORSOrigins)
	}
	if cfg.APIKey != "" {
		t.Errorf("APIKey: got %q, want empty", cfg.APIKey)
	}
	if cfg.MockTaskInterval != 2*time.Second {
		t.Errorf("MockTaskInterval: got %v, want %v", cfg.MockTaskInterval, 2*time.Second)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries: got %d, want %d", cfg.MaxRetries, 3)
	}
	if !cfg.EnableMockTasks {
		t.Errorf("EnableMockTasks: got %v, want true", cfg.EnableMockTasks)
	}
	if cfg.DLQStreamName != "buraq_tasks_dlq" {
		t.Errorf("DLQStreamName: got %q, want %q", cfg.DLQStreamName, "buraq_tasks_dlq")
	}
}

func TestLoad_CustomValuesFromEnv(t *testing.T) {
	clearEnv(
		"REDIS_ADDR", "STREAM_NAME", "GROUP_NAME", "CONSUMER_NAME",
		"WORKER_COUNT", "FETCH_BATCH_SIZE", "BLOCK_TIMEOUT",
		"API_PORT", "METRICS_PORT", "CORS_ORIGINS", "API_KEY",
		"MOCK_TASK_INTERVAL", "MAX_RETRIES", "ENABLE_MOCK_TASKS",
		"DLQ_STREAM_NAME",
	)

	os.Setenv("REDIS_ADDR", "redis.example.com:6380")
	os.Setenv("STREAM_NAME", "custom_stream")
	os.Setenv("GROUP_NAME", "custom_group")
	os.Setenv("CONSUMER_NAME", "custom_worker")
	os.Setenv("WORKER_COUNT", "8")
	os.Setenv("FETCH_BATCH_SIZE", "50")
	os.Setenv("BLOCK_TIMEOUT", "5s")
	os.Setenv("API_PORT", ":9090")
	os.Setenv("METRICS_PORT", ":3000")
	os.Setenv("CORS_ORIGINS", "https://app.example.com, https://admin.example.com")
	os.Setenv("API_KEY", "super-secret-key")
	os.Setenv("MOCK_TASK_INTERVAL", "10s")
	os.Setenv("MAX_RETRIES", "5")
	os.Setenv("ENABLE_MOCK_TASKS", "false")
	os.Setenv("DLQ_STREAM_NAME", "custom_dlq")
	defer clearEnv(
		"REDIS_ADDR", "STREAM_NAME", "GROUP_NAME", "CONSUMER_NAME",
		"WORKER_COUNT", "FETCH_BATCH_SIZE", "BLOCK_TIMEOUT",
		"API_PORT", "METRICS_PORT", "CORS_ORIGINS", "API_KEY",
		"MOCK_TASK_INTERVAL", "MAX_RETRIES", "ENABLE_MOCK_TASKS",
		"DLQ_STREAM_NAME",
	)

	cfg := Load()

	if cfg.RedisAddr != "redis.example.com:6380" {
		t.Errorf("RedisAddr: got %q, want %q", cfg.RedisAddr, "redis.example.com:6380")
	}
	if cfg.StreamName != "custom_stream" {
		t.Errorf("StreamName: got %q, want %q", cfg.StreamName, "custom_stream")
	}
	if cfg.GroupName != "custom_group" {
		t.Errorf("GroupName: got %q, want %q", cfg.GroupName, "custom_group")
	}
	if cfg.ConsumerName != "custom_worker" {
		t.Errorf("ConsumerName: got %q, want %q", cfg.ConsumerName, "custom_worker")
	}
	if cfg.WorkerCount != 8 {
		t.Errorf("WorkerCount: got %d, want 8", cfg.WorkerCount)
	}
	if cfg.FetchBatchSize != 50 {
		t.Errorf("FetchBatchSize: got %d, want 50", cfg.FetchBatchSize)
	}
	if cfg.BlockTimeout != 5*time.Second {
		t.Errorf("BlockTimeout: got %v, want 5s", cfg.BlockTimeout)
	}
	if cfg.APIPort != ":9090" {
		t.Errorf("APIPort: got %q, want :9090", cfg.APIPort)
	}
	if cfg.MetricsPort != ":3000" {
		t.Errorf("MetricsPort: got %q, want :3000", cfg.MetricsPort)
	}
	if len(cfg.CORSOrigins) != 2 {
		t.Errorf("CORSOrigins: expected 2 origins, got %d", len(cfg.CORSOrigins))
	}
	if cfg.CORSOrigins[0] != "https://app.example.com" {
		t.Errorf("CORSOrigins[0]: got %q, want %q", cfg.CORSOrigins[0], "https://app.example.com")
	}
	if cfg.CORSOrigins[1] != "https://admin.example.com" {
		t.Errorf("CORSOrigins[1]: got %q, want %q", cfg.CORSOrigins[1], "https://admin.example.com")
	}
	if cfg.APIKey != "super-secret-key" {
		t.Errorf("APIKey: got %q, want %q", cfg.APIKey, "super-secret-key")
	}
	if cfg.MockTaskInterval != 10*time.Second {
		t.Errorf("MockTaskInterval: got %v, want 10s", cfg.MockTaskInterval)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("MaxRetries: got %d, want 5", cfg.MaxRetries)
	}
	if cfg.EnableMockTasks {
		t.Errorf("EnableMockTasks: got true, want false")
	}
	if cfg.DLQStreamName != "custom_dlq" {
		t.Errorf("DLQStreamName: got %q, want %q", cfg.DLQStreamName, "custom_dlq")
	}
}

func TestEnvIntOrDefault_ValidInt(t *testing.T) {
	os.Setenv("TEST_INT", "42")
	defer os.Unsetenv("TEST_INT")

	result := envIntOrDefault("TEST_INT", 10)
	if result != 42 {
		t.Errorf("got %d, want 42", result)
	}
}

func TestEnvIntOrDefault_InvalidInt(t *testing.T) {
	os.Setenv("TEST_INT", "not-a-number")
	defer os.Unsetenv("TEST_INT")

	result := envIntOrDefault("TEST_INT", 10)
	if result != 10 {
		t.Errorf("expected fallback 10, got %d", result)
	}
}

func TestEnvIntOrDefault_EmptyEnv(t *testing.T) {
	os.Unsetenv("TEST_INT_MISSING")

	result := envIntOrDefault("TEST_INT_MISSING", 7)
	if result != 7 {
		t.Errorf("expected fallback 7, got %d", result)
	}
}

func TestEnvBoolOrDefault_ValidTrue(t *testing.T) {
	os.Setenv("TEST_BOOL", "true")
	defer os.Unsetenv("TEST_BOOL")

	result := envBoolOrDefault("TEST_BOOL", false)
	if result != true {
		t.Errorf("got %v, want true", result)
	}
}

func TestEnvBoolOrDefault_ValidFalse(t *testing.T) {
	os.Setenv("TEST_BOOL", "false")
	defer os.Unsetenv("TEST_BOOL")

	result := envBoolOrDefault("TEST_BOOL", true)
	if result != false {
		t.Errorf("got %v, want false", result)
	}
}

func TestEnvBoolOrDefault_InvalidBool(t *testing.T) {
	os.Setenv("TEST_BOOL", "notabool")
	defer os.Unsetenv("TEST_BOOL")

	result := envBoolOrDefault("TEST_BOOL", true)
	if result != true {
		t.Errorf("expected fallback true, got %v", result)
	}
}

func TestEnvBoolOrDefault_EmptyEnv(t *testing.T) {
	os.Unsetenv("TEST_BOOL_MISSING")

	result := envBoolOrDefault("TEST_BOOL_MISSING", false)
	if result != false {
		t.Errorf("expected fallback false, got %v", result)
	}
}

func TestEnvDurationOrDefault_ValidDuration(t *testing.T) {
	os.Setenv("TEST_DUR", "3s")
	defer os.Unsetenv("TEST_DUR")

	result := envDurationOrDefault("TEST_DUR", time.Second)
	if result != 3*time.Second {
		t.Errorf("got %v, want 3s", result)
	}
}

func TestEnvDurationOrDefault_InvalidDuration(t *testing.T) {
	os.Setenv("TEST_DUR", "notaduration")
	defer os.Unsetenv("TEST_DUR")

	result := envDurationOrDefault("TEST_DUR", time.Second)
	if result != time.Second {
		t.Errorf("expected fallback 1s, got %v", result)
	}
}

func TestEnvDurationOrDefault_EmptyEnv(t *testing.T) {
	os.Unsetenv("TEST_DUR_MISSING")

	result := envDurationOrDefault("TEST_DUR_MISSING", 5*time.Second)
	if result != 5*time.Second {
		t.Errorf("expected fallback 5s, got %v", result)
	}
}

func TestEnvListOrDefault_CommaSeparated(t *testing.T) {
	os.Setenv("TEST_LIST", "a,b,c")
	defer os.Unsetenv("TEST_LIST")

	result := envListOrDefault("TEST_LIST", []string{"default"})
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("got %v, want [a b c]", result)
	}
}

func TestEnvListOrDefault_WithSpaces(t *testing.T) {
	os.Setenv("TEST_LIST", "  hello , world  , foo ")
	defer os.Unsetenv("TEST_LIST")

	result := envListOrDefault("TEST_LIST", []string{"default"})
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	if result[0] != "hello" || result[1] != "world" || result[2] != "foo" {
		t.Errorf("got %v, want [hello world foo]", result)
	}
}

func TestEnvListOrDefault_EmptyItemsFiltered(t *testing.T) {
	os.Setenv("TEST_LIST", "a,,b,,,c")
	defer os.Unsetenv("TEST_LIST")

	result := envListOrDefault("TEST_LIST", []string{"default"})
	if len(result) != 3 {
		t.Fatalf("expected 3 items (empty filtered), got %d: %v", len(result), result)
	}
}

func TestEnvListOrDefault_EmptyEnv(t *testing.T) {
	os.Unsetenv("TEST_LIST_MISSING")

	result := envListOrDefault("TEST_LIST_MISSING", []string{"fallback1", "fallback2"})
	if len(result) != 2 {
		t.Fatalf("expected 2 fallback items, got %d", len(result))
	}
	if result[0] != "fallback1" || result[1] != "fallback2" {
		t.Errorf("got %v, want [fallback1 fallback2]", result)
	}
}

func TestEnvOrDefault_EmptyEnv(t *testing.T) {
	os.Unsetenv("TEST_STR_MISSING")

	result := envOrDefault("TEST_STR_MISSING", "default-val")
	if result != "default-val" {
		t.Errorf("got %q, want %q", result, "default-val")
	}
}

func TestEnvOrDefault_SetEnv(t *testing.T) {
	os.Setenv("TEST_STR", "custom-val")
	defer os.Unsetenv("TEST_STR")

	result := envOrDefault("TEST_STR", "default-val")
	if result != "custom-val" {
		t.Errorf("got %q, want %q", result, "custom-val")
	}
}
