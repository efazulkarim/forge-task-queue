package benchmarks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"buraq/event"
	"buraq/producer"
	"buraq/redisadapter"
	"buraq/task"

	"github.com/redis/go-redis/v9"
)

type BenchmarkResult struct {
	Name       string  `json:"name"`
	TPS        float64 `json:"tps"`
	P50Latency float64 `json:"p50_latency_ms"`
	P99Latency float64 `json:"p99_latency_ms"`
}

func BenchmarkBuraqQueue(b *testing.B) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skip("Redis is not running", err)
	}

	streamName := fmt.Sprintf("buraq_bench_%d", time.Now().UnixNano())
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	store := redisadapter.NewStore(rdb)
	publisher := event.NewPublisher(rdb, "buraq_events", logger)
	p := producer.New(store, publisher, streamName, logger)

	b.ResetTimer()

	start := time.Now()
	var latencies []float64

	for i := 0; i < b.N; i++ {
		t := &task.Task{
			ID:         fmt.Sprintf("bench-task-%d", i),
			Type:       "email_notification",
			Payload:    json.RawMessage(`{}`),
			CreatedAt:  time.Now().UTC(),
			MaxRetries: 3,
		}

		taskStart := time.Now()
		p.Produce(ctx, t)
		latencies = append(latencies, float64(time.Since(taskStart).Milliseconds()))
	}

	duration := time.Since(start).Seconds()

	sort.Float64s(latencies)
	p50 := 0.0
	p99 := 0.0
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)/2]
		p99 = latencies[int(float64(len(latencies))*0.99)]
	}

	tps := float64(b.N) / duration

	res := []BenchmarkResult{
		{
			Name:       "Publish Tasks",
			TPS:        tps,
			P50Latency: p50,
			P99Latency: p99,
		},
	}

	file, _ := json.MarshalIndent(res, "", "  ")
	os.WriteFile("../benchmark_results.json", file, 0644)

	// Cleanup
	rdb.Del(ctx, streamName)
}
