package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"buraq/task"

	"github.com/redis/go-redis/v9"
)

// Server is the HTTP API server for Buraq.
type Server struct {
	store     task.StreamStore
	publisher task.EventPublisher
	client    *redis.Client // used for Pub/Sub subscription in SSE
	stream    string
	dlqStream string
	apiKey    string
	origins   []string
	logger    *slog.Logger
	startTime time.Time
}

// NewServer creates a new API server.
func NewServer(
	store task.StreamStore,
	publisher task.EventPublisher,
	client *redis.Client,
	stream, dlqStream, apiKey string,
	origins []string,
	logger *slog.Logger,
) *Server {
	return &Server{
		store:     store,
		publisher: publisher,
		client:    client,
		stream:    stream,
		dlqStream: dlqStream,
		apiKey:    apiKey,
		origins:   origins,
		logger:    logger,
		startTime: time.Now(),
	}
}

// Start begins serving HTTP on the given address. Blocks until the server is shut down.
func (s *Server) Start(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/workers", s.handleWorkers)
	mux.HandleFunc("/api/retry-dlq", s.handleRetryDLQ)
	mux.HandleFunc("/api/health", s.handleHealth)

	// Chain middleware: panic recovery → CORS → rate limit → (auth applied per-route)
	rl := newRateLimiter(100, 200) // 100 req/s, burst 200
	handler := panicRecoveryMiddleware(s.logger)(
		corsMiddleware(s.origins)(
			rateLimitMiddleware(rl)(
				mux,
			),
		),
	)

	srv := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // SSE connections need no write timeout
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("starting API server", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("shutting down API server")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"Streaming unsupported"}`, http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	pubsub := s.client.Subscribe(ctx, "buraq_events")
	defer pubsub.Close()
	ch := pubsub.Channel()

	// Initial heartbeat
	fmt.Fprintf(w, "data: {\"type\":\"ping\"}\n\n")
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
			flusher.Flush()
		}
	}
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	type Worker struct {
		ID     string  `json:"id"`
		CPU    float64 `json:"cpu"`
		Memory float64 `json:"memory"`
		Status string  `json:"status"`
	}

	workers := []Worker{
		{ID: "worker_node_1-1", CPU: 42.5, Memory: 312.8, Status: "active"},
		{ID: "worker_node_1-2", CPU: 38.2, Memory: 289.4, Status: "active"},
		{ID: "worker_node_1-3", CPU: 55.1, Memory: 401.2, Status: "active"},
		{ID: "worker_node_1-4", CPU: 29.7, Memory: 256.0, Status: "active"},
		{ID: "worker_node_1-5", CPU: 61.3, Memory: 445.6, Status: "active"},
	}

	json.NewEncoder(w).Encode(workers)
}

func (s *Server) handleRetryDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error":"Method not allowed"}`))
		return
	}

	// Apply API key auth for this sensitive endpoint
	if s.apiKey != "" {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+s.apiKey {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"Invalid or missing API key"}`))
			return
		}
	}

	ctx := r.Context()

	// Fetch all tasks from DLQ
	messages, err := s.store.Range(ctx, s.dlqStream, "-", "+", 0)
	if err != nil {
		s.logger.Error("failed to read DLQ", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"Failed to read DLQ"}`))
		return
	}

	retried := 0
	for _, msg := range messages {
		payload, ok := msg.Values["payload"].(string)
		if !ok {
			continue
		}

		t, err := task.Unmarshal([]byte(payload))
		if err != nil {
			continue
		}

		// Reset retries
		t.CurrentRetries = 0

		// Add back to main stream
		if _, err := s.store.Add(ctx, s.stream, t); err != nil {
			s.logger.Error("failed to re-enqueue DLQ task", "error", err, "task_id", t.ID)
			continue
		}

		// Delete from DLQ
		if err := s.store.Delete(ctx, s.dlqStream, msg.ID); err != nil {
			s.logger.Warn("failed to delete task from DLQ", "error", err, "msg_id", msg.ID)
		}
		retried++

		// Publish Pending event
		s.publisher.Publish(ctx, task.Event{
			Type:   "Pending",
			TaskID: t.ID,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"retried": retried,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	redisOK := s.client.Ping(ctx).Err() == nil

	status := "healthy"
	code := http.StatusOK
	if !redisOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  status,
		"uptime":  time.Since(s.startTime).String(),
		"redis":   redisOK,
	})
}
