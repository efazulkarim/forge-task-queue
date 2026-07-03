package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"buraq/consumer"
	"buraq/event"
	"buraq/producer"
	"buraq/redisadapter"
	"buraq/task"

	"github.com/redis/go-redis/v9"
)

func setupRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skip("Redis not available, skipping integration test:", err)
	}
	return rdb
}

func uniqueStream(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("buraq_test_%s_%d", t.Name(), time.Now().UnixNano())
}

func TestIntegration_ProduceAndConsume(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := uniqueStream(t)
	group := stream + "_group"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	store := redisadapter.NewStore(rdb)
	publisher := event.NewPublisher(rdb, "buraq_events", logger)
	p := producer.New(store, publisher, stream, logger)

	// Track processed tasks
	var mu sync.Mutex
	processed := make(map[string]bool)

	processor := task.TaskProcessorFunc(func(ctx context.Context, t *task.Task) error {
		mu.Lock()
		processed[t.ID] = true
		mu.Unlock()
		return nil
	})

	// Start consumer
	c := consumer.New(store, publisher, processor, stream, group, "test-worker", 2, 5, 1*time.Second, stream+"_dlq", 3, logger)
	go c.Start(ctx)

	// Produce tasks
	for i := 0; i < 5; i++ {
		tt := &task.Task{
			ID:         fmt.Sprintf("integ-task-%d", i),
			Type:       "test",
			Payload:    json.RawMessage(`{}`),
			CreatedAt:  time.Now().UTC(),
			MaxRetries: 3,
		}
		_, err := p.Produce(ctx, tt)
		if err != nil {
			t.Fatalf("failed to produce task %d: %v", i, err)
		}
	}

	// Wait for processing
	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		count := len(processed)
		mu.Unlock()
		if count >= 5 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timed out waiting for tasks, processed %d/5: %v", count, processed)
			mu.Unlock()
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Verify all tasks were processed
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("integ-task-%d", i)
		mu.Lock()
		ok := processed[id]
		mu.Unlock()
		if !ok {
			t.Errorf("task %s was not processed", id)
		}
	}

	cancel()
	rdb.Del(context.Background(), stream, stream+"_dlq")
}

func TestIntegration_RetryAndDLQ(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := uniqueStream(t)
	group := stream + "_group"
	dlqStream := stream + "_dlq"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	store := redisadapter.NewStore(rdb)
	publisher := event.NewPublisher(rdb, "buraq_events", logger)
	p := producer.New(store, publisher, stream, logger)

	// Processor that always fails
	processor := task.TaskProcessorFunc(func(ctx context.Context, t *task.Task) error {
		return fmt.Errorf("always fails")
	})

	c := consumer.New(store, publisher, processor, stream, group, "test-worker", 2, 5, 1*time.Second, dlqStream, 2, logger)
	go c.Start(ctx)

	// Produce a task with max 2 retries
	tt := &task.Task{
		ID:         "dlq-task-1",
		Type:       "test",
		Payload:    json.RawMessage(`{}`),
		CreatedAt:  time.Now().UTC(),
		MaxRetries: 2,
	}
	_, err := p.Produce(ctx, tt)
	if err != nil {
		t.Fatalf("failed to produce task: %v", err)
	}

	// Wait for DLQ
	deadline := time.After(15 * time.Second)
	for {
		messages, err := store.Range(ctx, dlqStream, "-", "+", 10)
		if err == nil && len(messages) > 0 {
			// Verify the DLQ task has the right ID
			payload, ok := messages[0].Values["payload"].(string)
			if ok {
				var dt task.Task
				if json.Unmarshal([]byte(payload), &dt) == nil && dt.ID == "dlq-task-1" {
					break
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for task to reach DLQ")
		case <-time.After(500 * time.Millisecond):
		}
	}

	cancel()
	rdb.Del(context.Background(), stream, dlqStream)
}

func TestIntegration_EventPublishing(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	publisher := event.NewPublisher(rdb, "buraq_test_events", logger)

	// Subscribe to events
	pubsub := rdb.Subscribe(ctx, "buraq_test_events")
	defer pubsub.Close()
	ch := pubsub.Channel()

	// Publish an event
	testEvent := task.Event{
		Type:   "Completed",
		TaskID: "test-task-1",
	}
	err := publisher.Publish(ctx, testEvent)
	if err != nil {
		t.Fatalf("failed to publish event: %v", err)
	}

	// Receive the event
	select {
	case msg := <-ch:
		var received task.Event
		if err := json.Unmarshal([]byte(msg.Payload), &received); err != nil {
			t.Fatalf("failed to unmarshal received event: %v", err)
		}
		if received.Type != "Completed" {
			t.Errorf("expected type Completed, got %s", received.Type)
		}
		if received.TaskID != "test-task-1" {
			t.Errorf("expected task_id test-task-1, got %s", received.TaskID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestIntegration_StreamOperations(t *testing.T) {
	rdb := setupRedis(t)
	defer rdb.Close()
	ctx := context.Background()

	stream := uniqueStream(t)
	group := stream + "_group"
	store := redisadapter.NewStore(rdb)

	// Create group
	if err := store.CreateGroup(ctx, stream, group); err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}

	// Add a task
	tt := &task.Task{
		ID:      "stream-test-1",
		Type:    "test",
		Payload: json.RawMessage(`{"key":"value"}`),
	}
	msgID, err := store.Add(ctx, stream, tt)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Read the task
	messages, err := store.ReadGroup(ctx, stream, group, "test-consumer", 10, 1*time.Second)
	if err != nil {
		t.Fatalf("ReadGroup failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Verify payload
	payload, ok := messages[0].Values["payload"].(string)
	if !ok {
		t.Fatal("expected payload to be a string")
	}
	var parsed task.Task
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if parsed.ID != "stream-test-1" {
		t.Errorf("expected ID stream-test-1, got %s", parsed.ID)
	}

	// Ack the task
	if err := store.Ack(ctx, stream, group, messages[0].ID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// Read again — should be empty
	messages, err = store.ReadGroup(ctx, stream, group, "test-consumer", 10, 1*time.Second)
	if err != nil {
		t.Fatalf("second ReadGroup failed: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages after ack, got %d", len(messages))
	}

	rdb.Del(ctx, stream)
}
