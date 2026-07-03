package producer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"

	"buraq/task"
)

// --- Mock implementations ---

type mockStore struct {
	mu       sync.Mutex
	addCalls []addCall
	addErr   error
}

type addCall struct {
	stream string
	task   *task.Task
}

func (m *mockStore) Add(ctx context.Context, stream string, t *task.Task) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addCalls = append(m.addCalls, addCall{stream: stream, task: t})
	if m.addErr != nil {
		return "", m.addErr
	}
	return "msg-" + t.ID, nil
}

func (m *mockStore) ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block interface{}) ([]task.StreamMessage, error) {
	return nil, nil
}

func (m *mockStore) Ack(ctx context.Context, stream, group, msgID string) error {
	return nil
}

func (m *mockStore) CreateGroup(ctx context.Context, stream, group string) error {
	return nil
}

func (m *mockStore) Range(ctx context.Context, stream, start, stop string, count int64) ([]task.StreamMessage, error) {
	return nil, nil
}

func (m *mockStore) Delete(ctx context.Context, stream string, msgIDs ...string) error {
	return nil
}

type mockPublisher struct {
	mu         sync.Mutex
	events     []task.Event
	publishErr error
}

func (m *mockPublisher) Publish(ctx context.Context, e task.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return m.publishErr
}

// --- Tests ---

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestProduce_SuccessfulCall(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	p := New(store, publisher, "test-stream", newTestLogger())

	taskMsg := &task.Task{
		ID:      "task-1",
		Type:    "send_email",
		Payload: json.RawMessage(`{"to":"user@test.com"}`),
	}

	msgID, err := p.Produce(context.Background(), taskMsg)
	if err != nil {
		t.Fatalf("Produce returned error: %v", err)
	}
	if msgID != "msg-task-1" {
		t.Errorf("expected msgID msg-task-1, got %s", msgID)
	}

	// Verify store.Add was called
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.addCalls) != 1 {
		t.Fatalf("expected 1 Add call, got %d", len(store.addCalls))
	}
	if store.addCalls[0].stream != "test-stream" {
		t.Errorf("expected stream test-stream, got %s", store.addCalls[0].stream)
	}
	if store.addCalls[0].task.ID != "task-1" {
		t.Errorf("expected task ID task-1, got %s", store.addCalls[0].task.ID)
	}

	// Verify publisher.Publish was called with Pending event
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(publisher.events))
	}
	if publisher.events[0].Type != "Pending" {
		t.Errorf("expected event type Pending, got %s", publisher.events[0].Type)
	}
	if publisher.events[0].TaskID != "task-1" {
		t.Errorf("expected event TaskID task-1, got %s", publisher.events[0].TaskID)
	}
}

func TestProduce_StoreAddFails(t *testing.T) {
	store := &mockStore{
		addErr: errors.New("redis connection refused"),
	}
	publisher := &mockPublisher{}
	p := New(store, publisher, "test-stream", newTestLogger())

	taskMsg := &task.Task{
		ID:   "task-2",
		Type: "send_email",
	}

	msgID, err := p.Produce(context.Background(), taskMsg)
	if err == nil {
		t.Fatal("expected error when store.Add fails")
	}
	if msgID != "" {
		t.Errorf("expected empty msgID on error, got %q", msgID)
	}

	// Publisher should NOT have been called
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.events) != 0 {
		t.Errorf("expected 0 events on store failure, got %d", len(publisher.events))
	}
}

func TestProduce_PublisherFailsTaskStillEnqueued(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{
		publishErr: errors.New("pubsub unavailable"),
	}
	p := New(store, publisher, "test-stream", newTestLogger())

	taskMsg := &task.Task{
		ID:   "task-3",
		Type: "send_email",
	}

	msgID, err := p.Produce(context.Background(), taskMsg)
	// Should succeed because task is already enqueued
	if err != nil {
		t.Fatalf("Produce should succeed when publisher fails: %v", err)
	}
	if msgID != "msg-task-3" {
		t.Errorf("expected msgID msg-task-3, got %s", msgID)
	}

	// Store should still have been called
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.addCalls) != 1 {
		t.Fatalf("expected 1 Add call, got %d", len(store.addCalls))
	}

	// Publisher was called (even though it failed)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 event call, got %d", len(publisher.events))
	}
}

func TestProduce_CorrectStreamUsed(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	p := New(store, publisher, "my_custom_stream", newTestLogger())

	taskMsg := &task.Task{
		ID:   "task-4",
		Type: "process_image",
	}

	_, err := p.Produce(context.Background(), taskMsg)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.addCalls[0].stream != "my_custom_stream" {
		t.Errorf("expected stream my_custom_stream, got %s", store.addCalls[0].stream)
	}
}

func TestProduce_PublishesPendingEventWithCorrectTaskID(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	p := New(store, publisher, "stream", newTestLogger())

	taskMsg := &task.Task{
		ID:   "unique-task-id-999",
		Type: "test",
	}

	_, err := p.Produce(context.Background(), taskMsg)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(publisher.events))
	}
	if publisher.events[0].TaskID != "unique-task-id-999" {
		t.Errorf("expected TaskID unique-task-id-999, got %s", publisher.events[0].TaskID)
	}
	if publisher.events[0].Type != "Pending" {
		t.Errorf("expected Type Pending, got %s", publisher.events[0].Type)
	}
}
