package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"buraq/api"
	"buraq/config"
	"buraq/consumer"
	"buraq/event"
	"buraq/producer"
	"buraq/redisadapter"
	"buraq/task"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := config.Load()

	// Graceful shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, initiating graceful shutdown", "signal", sig)
		cancel()
	}()

	// Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("starting Prometheus metrics server", "addr", cfg.MetricsPort)
		if err := http.ListenAndServe(cfg.MetricsPort, nil); err != nil {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	// Initialize Redis client (standalone or cluster)
	var rdb *redis.Client
	var clusterRdb *redis.ClusterClient
	var store task.StreamStore

	if cfg.RedisCluster {
		clusterRdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: cfg.RedisAddrs,
		})
		if err := clusterRdb.Ping(ctx).Err(); err != nil {
			logger.Warn("could not connect to Redis Cluster, continuing anyway", "error", err)
		} else {
			logger.Info("connected to Redis Cluster", "addrs", cfg.RedisAddrs)
		}
		store = redisadapter.NewClusterStore(clusterRdb)
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("could not connect to Redis, continuing anyway", "error", err)
		} else {
			logger.Info("connected to Redis", "addr", cfg.RedisAddr)
		}
		store = redisadapter.NewStore(rdb)
	}

	// Event publisher (uses the standalone client for Pub/Sub — Cluster doesn't support SUBSCRIBE well)
	pubClient := rdb
	if pubClient == nil {
		// In cluster mode, use a standalone client for pub/sub if available
		pubClient = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	}
	publisher := event.NewPublisher(pubClient, "buraq_events", logger)
	p := producer.New(store, publisher, cfg.StreamName, logger)

	// API Server (runs in background, respects ctx for graceful shutdown)
	apiSrv := api.NewServer(store, publisher, pubClient, cfg.StreamName, cfg.DLQStreamName, cfg.APIKey, cfg.CORSOrigins, logger)
	go func() {
		if err := apiSrv.Start(ctx, cfg.APIPort); err != nil {
			logger.Error("API server failed", "error", err)
		}
	}()

	// Mock task producer
	if cfg.EnableMockTasks {
		go produceMockTasks(ctx, p, cfg, logger)
	}

	// Task processor — simulates work with configurable failure rate
	processor := task.TaskProcessorFunc(func(ctx context.Context, t *task.Task) error {
		time.Sleep(500 * time.Millisecond)
		if rand.Float32() < 0.05 {
			return fmt.Errorf("simulated network timeout while calling external API")
		}
		return nil
	})

	// Initialize and start Consumer
	c := consumer.New(
		store, publisher, processor,
		cfg.StreamName, cfg.GroupName, cfg.ConsumerName,
		cfg.WorkerCount, cfg.FetchBatchSize, cfg.BlockTimeout,
		cfg.DLQStreamName, cfg.MaxRetries,
		logger,
	)

	// Enable idempotency if Redis is available (standalone mode)
	if rdb != nil {
		idempStore := redisadapter.NewIdempotencyStore(rdb, 24*time.Hour)
		c.WithIdempotency(idempStore)
	}

	logger.Info("starting Buraq consumer",
		"workers", cfg.WorkerCount,
		"stream", cfg.StreamName,
		"group", cfg.GroupName,
	)

	if err := c.Start(ctx); err != nil {
		logger.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("Buraq shutdown gracefully")
}

func produceMockTasks(ctx context.Context, p *producer.Producer, cfg *config.Config, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.MockTaskInterval)
	defer ticker.Stop()

	taskCounter := 1

	for {
		select {
		case <-ctx.Done():
			logger.Info("mock producer stopping")
			return
		case <-ticker.C:
			t := &task.Task{
				ID:         fmt.Sprintf("task-%d", taskCounter),
				Type:       "email_notification",
				Payload:    json.RawMessage(`{"user_id": 123, "template": "welcome"}`),
				CreatedAt:  time.Now().UTC(),
				MaxRetries: cfg.MaxRetries,
				Priority:   task.PriorityNormal,
			}

			msgID, err := p.Produce(ctx, t)
			if err != nil {
				logger.Error("failed to produce task", "error", err, "task_num", taskCounter)
			} else {
				logger.Info("produced task", "task_id", t.ID, "msg_id", msgID)
			}
			taskCounter++
		}
	}
}
