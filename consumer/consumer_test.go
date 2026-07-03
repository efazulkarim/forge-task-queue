package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"buraq/task"
)

// --- Mock implementations ---

type mockStore struct {
	mu          sync.Mutex
	addCalls    []addCall
	ackCalls    []ackCall
	groupErr    error
	addErr      error
	ackErr      nil_able // typed to allow nil error assignment
}

type nil_able struct {
	err error
}

type addCall struct {
	stream string
	task   *task.Task
}

type ackCall struct {
	stream  string
	group   string
	msgID   string
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalls = append(m.ackCalls, ackCall{stream: stream, group: group, msgID: msgID})
	return m.ackErr.err
}

func (m *mockStore) CreateGroup(ctx context.Context, stream, group string) error {
	return m.groupErr
}

func (m *mockStore) Range(ctx context.Context, stream, start, stop string, count int64) ([]task.StreamMessage, error) {
	return nil, nil
}

func (m *mockStore) Delete(ctx context.Context, stream string, msgIDs ...string) error {
	return nil
}

type mockPublisher struct {
	mu       sync.Mutex
	events   []task.Event
	publishErr error
}

func (m *mockPublisher) Publish(ctx context.Context, e task.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return m.publishErr
}

type mockProcessor struct {
	mu          sync.Mutex
	processCalls []*task.Task
	processErr   error
}

func (m *mockProcessor) Process(ctx context.Context, t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processCalls = append(m.processCalls, t)
	return m.processErr
}

// --- Test helpers ---

func newTestConsumer(store task.StreamStore, publisher task.EventPublisher, processor task.TaskProcessor) *Consumer {
	return New(
		store,
		publisher,
		processor,
		"test-stream",
		"test-group",
		"test-consumer",
		1,          // workerCount
		10,         // fetchBatchSize
		time.Second, // blockTimeout
		"test-dlq",
		3,          // maxRetries
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	)
}

func makeStreamMsg(id string, t *task.Task) task.StreamMessage {
	payload, _ := json.Marshal(t)
	return task.StreamMessage{
		ID:     id,
		Values: map[string]interface{}{"payload": string(payload)},
	}
}

// --- Tests ---

func TestProcessMessage_SuccessfulProcessing(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{}

	c := newTestConsumer(store, publisher, processor)

	taskMsg := &task.Task{
		ID:         "task-1",
		Type:       "send_email",
		Payload:    json.RawMessage(`{"to":"test@example.com"}`),
		MaxRetries: 3,
	}
	msg := makeStreamMsg("msg-001", taskMsg)

	c.processMessage(context.Background(), 1, msg)

	// Processor should have been called once
	if len(processor.processCalls) != 1 {
		t.Fatalf("expected 1 process call, got %d", len(processor.processCalls))
	}
	if processor.processCalls[0].ID != "task-1" {
		t.Errorf("expected task ID task-1, got %s", processor.processCalls[0].ID)
	}

	// Should have published Processing and Completed events
	if len(publisher.events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(publisher.events))
	}
	if publisher.events[0].Type != "Processing" {
		t.Errorf("expected first event type Processing, got %s", publisher.events[0].Type)
	}
	if publisher.events[1].Type != "Completed" {
		t.Errorf("expected second event type Completed, got %s", publisher.events[1].Type)
	}

	// Message should have been acknowledged
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ackCalls) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(store.ackCalls))
	}
	if store.ackCalls[0].msgID != "msg-001" {
		t.Errorf("expected ack for msg-001, got %s", store.ackCalls[0].msgID)
	}
}

func TestProcessMessage_TaskFailedGetsRetried(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{
		processErr: errors.New("temporary failure"),
	}

	c := newTestConsumer(store, publisher, processor)

	taskMsg := &task.Task{
		ID:             "task-2",
		Type:           "send_email",
		Payload:        json.RawMessage(`{}`),
		MaxRetries:     3,
		CurrentRetries: 0,
	}
	msg := makeStreamMsg("msg-002", taskMsg)

	c.processMessage(context.Background(), 1, msg)

	// Processor should have been called
	if len(processor.processCalls) != 1 {
		t.Fatalf("expected 1 process call, got %d", len(processor.processCalls))
	}

	// Task should be re-queued (Add called on main stream)
	store.mu.Lock()
	addCount := len(store.addCalls)
	store.mu.Unlock()
	if addCount != 1 {
		t.Fatalf("expected 1 add call for re-queue, got %d", addCount)
	}

	store.mu.Lock()
	requeued := store.addCalls[0]
	store.mu.Unlock()
	if requeued.stream != "test-stream" {
		t.Errorf("expected re-queue on test-stream, got %s", requeued.stream)
	}
	if requeued.task.CurrentRetries != 1 {
		t.Errorf("expected CurrentRetries incremented to 1, got %d", requeued.task.CurrentRetries)
	}
	if requeued.task.Error != "temporary failure" {
		t.Errorf("expected error to be set, got %q", requeued.task.Error)
	}

	// Should have published Processing, Failed events
	if len(publisher.events) < 2 {
		t.Fatalf("expected at least 2 events, got %d", len(publisher.events))
	}
	if publisher.events[0].Type != "Processing" {
		t.Errorf("expected first event Processing, got %s", publisher.events[0].Type)
	}
	if publisher.events[1].Type != "Failed" {
		t.Errorf("expected second event Failed, got %s", publisher.events[1].Type)
	}

	// Message should be acknowledged
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ackCalls) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(store.ackCalls))
	}
}

func TestProcessMessage_TaskGoesToDLQAfterMaxRetries(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{
		processErr: errors.New("permanent failure"),
	}

	c := newTestConsumer(store, publisher, processor)

	taskMsg := &task.Task{
		ID:             "task-3",
		Type:           "send_email",
		Payload:        json.RawMessage(`{}`),
		MaxRetries:     3,
		CurrentRetries: 3, // already at max
	}
	msg := makeStreamMsg("msg-003", taskMsg)

	c.processMessage(context.Background(), 1, msg)

	// Task should be added to DLQ stream
	store.mu.Lock()
	addCount := len(store.addCalls)
	store.mu.Unlock()
	if addCount != 1 {
		t.Fatalf("expected 1 add call for DLQ, got %d", addCount)
	}

	store.mu.Lock()
	dlqCall := store.addCalls[0]
	store.mu.Unlock()
	if dlqCall.stream != "test-dlq" {
		t.Errorf("expected DLQ stream, got %s", dlqCall.stream)
	}

	// Should have published Processing, Failed, and DLQ events
	if len(publisher.events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(publisher.events))
	}
	if publisher.events[0].Type != "Processing" {
		t.Errorf("expected first event Processing, got %s", publisher.events[0].Type)
	}
	if publisher.events[1].Type != "Failed" {
		t.Errorf("expected second event Failed, got %s", publisher.events[1].Type)
	}
	if publisher.events[2].Type != "DLQ" {
		t.Errorf("expected third event DLQ, got %s", publisher.events[2].Type)
	}

	// Message should be acknowledged
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ackCalls) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(store.ackCalls))
	}
}

func TestProcessMessage_InvalidPayloadAcknowledgedAndSkipped(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{}

	c := newTestConsumer(store, publisher, processor)

	// Message with non-string payload (invalid format)
	msg := task.StreamMessage{
		ID:     "msg-bad-payload",
		Values: map[string]interface{}{"payload": 12345}, // not a string
	}

	c.processMessage(context.Background(), 1, msg)

	// Processor should NOT have been called
	if len(processor.processCalls) != 0 {
		t.Errorf("expected 0 process calls, got %d", len(processor.processCalls))
	}

	// No events should be published
	if len(publisher.events) != 0 {
		t.Errorf("expected 0 events, got %d", len(publisher.events))
	}

	// Message should be acknowledged (skipped)
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ackCalls) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(store.ackCalls))
	}
	if store.ackCalls[0].msgID != "msg-bad-payload" {
		t.Errorf("expected ack for msg-bad-payload, got %s", store.ackCalls[0].msgID)
	}
}

func TestProcessMessage_MissingPayloadAcknowledgedAndSkipped(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{}

	c := newTestConsumer(store, publisher, processor)

	// Message with no "payload" key
	msg := task.StreamMessage{
		ID:     "msg-no-payload",
		Values: map[string]interface{}{"other_field": "value"},
	}

	c.processMessage(context.Background(), 1, msg)

	if len(processor.processCalls) != 0 {
		t.Errorf("expected 0 process calls, got %d", len(processor.processCalls))
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ackCalls) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(store.ackCalls))
	}
}

func TestProcessMessage_UnmarshalErrorAcknowledgedAndSkipped(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{}

	c := newTestConsumer(store, publisher, processor)

	// Message with invalid JSON payload
	msg := task.StreamMessage{
		ID:     "msg-bad-json",
		Values: map[string]interface{}{"payload": "{invalid json!!!"},
	}

	c.processMessage(context.Background(), 1, msg)

	// Processor should NOT have been called
	if len(processor.processCalls) != 0 {
		t.Errorf("expected 0 process calls, got %d", len(processor.processCalls))
	}

	// No events should be published
	if len(publisher.events) != 0 {
		t.Errorf("expected 0 events, got %d", len(publisher.events))
	}

	// Message should be acknowledged
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ackCalls) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(store.ackCalls))
	}
}

func TestProcessMessage_RetryCountIncrementedCorrectly(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}
	processor := &mockProcessor{
		processErr: errors.New("fail"),
	}

	c := newTestConsumer(store, publisher, processor)

	taskMsg := &task.Task{
		ID:             "task-retry",
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		MaxRetries:     3,
		CurrentRetries: 2, // one below max
	}
	msg := makeStreamMsg("msg-retry", taskMsg)

	c.processMessage(context.Background(), 1, msg)

	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.addCalls) != 1 {
		t.Fatalf("expected 1 add call, got %d", len(store.addCalls))
	}

	requeued := store.addCalls[0]
	if requeued.stream != "test-stream" {
		t.Errorf("expected re-queue on test-stream, got %s", requeued.stream)
	}
	if requeued.task.CurrentRetries != 3 {
		t.Errorf("expected CurrentRetries=3, got %d", requeued.task.CurrentRetries)
	}
}

type ctxCapturingProcessor struct {
	ctx *context.Context
}

func (p *ctxCapturingProcessor) Process(ctx context.Context, t *task.Task) error {
	*p.ctx = ctx
	return nil
}

func TestProcessMessage_TimeoutApplied(t *testing.T) {
	store := &mockStore{}
	publisher := &mockPublisher{}

	var receivedCtx context.Context
	processor := &ctxCapturingProcessor{ctx: &receivedCtx}

	c := newTestConsumer(store, publisher, processor)

	timeout := 5 * time.Second
	taskMsg := &task.Task{
		ID:         "task-timeout",
		Type:       "test",
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
		Timeout:    &timeout,
	}
	msg := makeStreamMsg("msg-timeout", taskMsg)

	c.processMessage(context.Background(), 1, msg)

	if receivedCtx == nil {
		t.Fatal("processor was not called")
	}

	// The context should have a deadline set
	deadline, ok := receivedCtx.Deadline()
	if !ok {
		t.Fatal("expected context with deadline, got none")
	}

	// Deadline should be roughly 5 seconds from now (within tolerance)
	remaining := time.Until(deadline)
	if remaining > 6*time.Second || remaining < 3*time.Second {
		t.Errorf("expected deadline ~5s from now, got %v remaining", remaining)
	}
}

func TestStart_CreateGroupError(t *testing.T) {
	store := &mockStore{
		groupErr: errors.New("redis connection refused"),
	}
	publisher := &mockPublisher{}
	processor := &mockProcessor{}

	c := newTestConsumer(store, publisher, processor)

	err := c.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from Start when CreateGroup fails")
	}

	expected := "failed to create consumer group"
	if !contains(err.Error(), expected) {
		t.Errorf("expected error containing %q, got %q", expected, err.Error())
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
