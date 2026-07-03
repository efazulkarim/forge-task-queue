package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"buraq/task"

	"github.com/redis/go-redis/v9"
)

// --- Mock implementations ---

type mockStore struct {
	mu       sync.Mutex
	addCalls []addCall
	ackCalls []ackCall
	rangeMsg []task.StreamMessage
	rangeErr error
	addErr   error
	delCalls []delCall
}

type addCall struct {
	stream string
	task   *task.Task
}

type ackCall struct {
	stream string
	group  string
	msgID  string
}

type delCall struct {
	stream string
	msgIDs []string
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
	return nil
}

func (m *mockStore) CreateGroup(ctx context.Context, stream, group string) error {
	return nil
}

func (m *mockStore) Range(ctx context.Context, stream, start, stop string, count int64) ([]task.StreamMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rangeErr != nil {
		return nil, m.rangeErr
	}
	return m.rangeMsg, nil
}

func (m *mockStore) Delete(ctx context.Context, stream string, msgIDs ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delCalls = append(m.delCalls, delCall{stream: stream, msgIDs: msgIDs})
	return nil
}

type mockPublisher struct {
	mu     sync.Mutex
	events []task.Event
}

func (m *mockPublisher) Publish(ctx context.Context, e task.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}

// --- Helper to create server for testing ---

func newTestServer(store *mockStore, publisher *mockPublisher, apiKey string) *Server {
	// Use a Redis client pointing to a non-existent server.
	// This is only used by handleHealth and handleStream; tests that
	// exercise those endpoints control the expected behavior.
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:1", // guaranteed to fail Ping
	})

	return &Server{
		store:     store,
		publisher: publisher,
		client:    client,
		stream:    "test-stream",
		dlqStream: "test-dlq",
		apiKey:    apiKey,
		origins:   []string{"http://localhost:3000"},
		logger:    slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		startTime: time.Now(),
	}
}

// --- Health endpoint tests ---

func TestHealth_DegradedWhenRedisDown(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "")

	// The Redis client points to localhost:1 (non-existent), so Ping will fail
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rr := httptest.NewRecorder()

	srv.handleHealth(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when Redis is down, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}
	if body["status"] != "degraded" {
		t.Errorf("expected status degraded, got %v", body["status"])
	}
	if body["redis"] != false {
		t.Errorf("expected redis false, got %v", body["redis"])
	}
	if body["uptime"] == nil {
		t.Error("expected uptime to be set")
	}
}

func TestHealth_ResponseIsJSON(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "")

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rr := httptest.NewRecorder()

	srv.handleHealth(rr, req)

	ct := rr.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}

	// Verify it's valid JSON
	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Errorf("response is not valid JSON: %v", err)
	}
}

// --- Retry-DLQ endpoint tests ---

func TestRetryDLQ_RejectsNonPOST(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "secret-key")

	methods := []string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/api/retry-dlq", nil)
			req.Header.Set("Authorization", "Bearer secret-key")
			rr := httptest.NewRecorder()

			srv.handleRetryDLQ(rr, req)

			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("expected 405 for %s, got %d", method, rr.Code)
			}
			if !strings.Contains(rr.Body.String(), "Method not allowed") {
				t.Errorf("expected error message, got %q", rr.Body.String())
			}
		})
	}
}

func TestRetryDLQ_RequiresAuthWhenKeySet(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "secret-key")

	// No Authorization header
	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Invalid or missing API key") {
		t.Errorf("expected auth error message, got %q", rr.Body.String())
	}
}

func TestRetryDLQ_RejectsWrongKey(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "secret-key")

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestRetryDLQ_AllowsValidKey(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "secret-key")

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	// Should succeed (200 with retried count)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if body["success"] != true {
		t.Errorf("expected success true, got %v", body["success"])
	}
}

func TestRetryDLQ_NoAuthWhenKeyEmpty(t *testing.T) {
	store := &mockStore{}
	srv := newTestServer(store, &mockPublisher{}, "") // no API key

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	// No Authorization header
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	// Should succeed without auth
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 when API key is empty, got %d", rr.Code)
	}
}

func TestRetryDLQ_RetriesDLQMessages(t *testing.T) {
	task1 := &task.Task{ID: "dlq-task-1", Type: "email", Payload: json.RawMessage(`{}`), MaxRetries: 3, CurrentRetries: 3}
	task2 := &task.Task{ID: "dlq-task-2", Type: "image", Payload: json.RawMessage(`{}`), MaxRetries: 3, CurrentRetries: 3}

	payload1, _ := json.Marshal(task1)
	payload2, _ := json.Marshal(task2)

	store := &mockStore{
		rangeMsg: []task.StreamMessage{
			{ID: "dlq-msg-1", Values: map[string]interface{}{"payload": string(payload1)}},
			{ID: "dlq-msg-2", Values: map[string]interface{}{"payload": string(payload2)}},
		},
	}
	publisher := &mockPublisher{}
	srv := newTestServer(store, publisher, "")

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if body["retried"] != float64(2) {
		t.Errorf("expected retried 2, got %v", body["retried"])
	}

	// Both tasks should be re-enqueued on main stream
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.addCalls) != 2 {
		t.Fatalf("expected 2 add calls, got %d", len(store.addCalls))
	}
	if store.addCalls[0].stream != "test-stream" {
		t.Errorf("expected re-enqueue on test-stream, got %s", store.addCalls[0].stream)
	}

	// Retries should be reset
	if store.addCalls[0].task.CurrentRetries != 0 {
		t.Errorf("expected CurrentRetries reset to 0, got %d", store.addCalls[0].task.CurrentRetries)
	}
	if store.addCalls[1].task.CurrentRetries != 0 {
		t.Errorf("expected CurrentRetries reset to 0, got %d", store.addCalls[1].task.CurrentRetries)
	}

	// Both should be deleted from DLQ
	if len(store.delCalls) != 2 {
		t.Fatalf("expected 2 delete calls, got %d", len(store.delCalls))
	}
	if store.delCalls[0].stream != "test-dlq" {
		t.Errorf("expected delete from test-dlq, got %s", store.delCalls[0].stream)
	}

	// Pending events should be published
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.events) != 2 {
		t.Fatalf("expected 2 Pending events, got %d", len(publisher.events))
	}
	for _, e := range publisher.events {
		if e.Type != "Pending" {
			t.Errorf("expected event type Pending, got %s", e.Type)
		}
	}
}

func TestRetryDLQ_EmptyDLQReturnsZeroRetried(t *testing.T) {
	store := &mockStore{
		rangeMsg: []task.StreamMessage{}, // empty DLQ
	}
	srv := newTestServer(store, &mockPublisher{}, "")

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if body["retried"] != float64(0) {
		t.Errorf("expected retried 0, got %v", body["retried"])
	}
}

func TestRetryDLQ_RangeErrorReturns500(t *testing.T) {
	store := &mockStore{
		rangeErr: context.DeadlineExceeded,
	}
	srv := newTestServer(store, &mockPublisher{}, "")

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on Range error, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Failed to read DLQ") {
		t.Errorf("expected DLQ error message, got %q", rr.Body.String())
	}
}

func TestRetryDLQ_SkipsInvalidPayload(t *testing.T) {
	store := &mockStore{
		rangeMsg: []task.StreamMessage{
			{ID: "msg-bad", Values: map[string]interface{}{"payload": 12345}}, // not a string
			{ID: "msg-bad-json", Values: map[string]interface{}{"payload": "!!!invalid"}},
		},
	}
	publisher := &mockPublisher{}
	srv := newTestServer(store, publisher, "")

	req := httptest.NewRequest(http.MethodPost, "/api/retry-dlq", nil)
	rr := httptest.NewRecorder()

	srv.handleRetryDLQ(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if body["retried"] != float64(0) {
		t.Errorf("expected retried 0 for invalid payloads, got %v", body["retried"])
	}
}

// --- Workers endpoint test ---

func TestWorkers_ReturnsJSON(t *testing.T) {
	srv := newTestServer(&mockStore{}, &mockPublisher{}, "")

	req := httptest.NewRequest(http.MethodGet, "/api/workers", nil)
	rr := httptest.NewRecorder()

	srv.handleWorkers(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", rr.Header().Get("Content-Type"))
	}

	var workers []map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &workers); err != nil {
		t.Fatalf("failed to parse workers response: %v", err)
	}
	if len(workers) != 5 {
		t.Errorf("expected 5 workers, got %d", len(workers))
	}
}
