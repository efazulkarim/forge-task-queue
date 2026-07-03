package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"buraq/metrics"
	"buraq/task"
)

// Consumer manages fetching tasks from a stream and processing them via a worker pool.
type Consumer struct {
	store         task.StreamStore
	publisher     task.EventPublisher
	processor     task.TaskProcessor
	idempotency   task.IdempotencyChecker // optional, nil to disable
	stream        string
	group         string
	consumer      string
	workerCount   int
	fetchBatchSize int64
	blockTimeout  time.Duration
	dlqStream     string
	maxRetries    int
	logger        *slog.Logger
}

// New creates a new Consumer instance.
func New(
	store task.StreamStore,
	publisher task.EventPublisher,
	processor task.TaskProcessor,
	stream, group, consumer string,
	workerCount int,
	fetchBatchSize int64,
	blockTimeout time.Duration,
	dlqStream string,
	maxRetries int,
	logger *slog.Logger,
) *Consumer {
	return &Consumer{
		store:          store,
		publisher:      publisher,
		processor:      processor,
		stream:         stream,
		group:          group,
		consumer:       consumer,
		workerCount:    workerCount,
		fetchBatchSize: fetchBatchSize,
		blockTimeout:   blockTimeout,
		dlqStream:      dlqStream,
		maxRetries:     maxRetries,
		logger:         logger,
	}
}

// WithIdempotency sets the idempotency checker for duplicate prevention.
func (c *Consumer) WithIdempotency(checker task.IdempotencyChecker) *Consumer {
	c.idempotency = checker
	return c
}

// Start creates the consumer group (if necessary) and begins fetching/processing tasks.
func (c *Consumer) Start(ctx context.Context) error {
	if err := c.store.CreateGroup(ctx, c.stream, c.group); err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Buffered channel to decouple fetch speed from processing speed
	tasksCh := make(chan task.StreamMessage, c.workerCount*10)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 1; i <= c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, &wg, tasksCh, i)
	}

	// Start task fetcher
	wg.Add(1)
	go c.fetchTasks(ctx, &wg, tasksCh)

	wg.Wait()
	return nil
}

// fetchTasks continuously reads messages from the stream and sends them to workers.
func (c *Consumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup, tasksCh chan<- task.StreamMessage) {
	defer wg.Done()
	defer close(tasksCh)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("context cancelled, consumer fetcher stopping")
			return
		default:
			messages, err := c.store.ReadGroup(ctx, c.stream, c.group, c.consumer, c.fetchBatchSize, c.blockTimeout)
			if err != nil {
				if ctx.Err() != nil {
					c.logger.Info("context cancelled during read, consumer fetcher stopping")
					return
				}
				c.logger.Error("error reading from stream", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range messages {
				select {
				case tasksCh <- msg:
				case <-ctx.Done():
					c.logger.Info("context cancelled while dispatching, stopping fetcher")
					return
				}
			}
		}
	}
}

// worker represents a single goroutine in the worker pool processing tasks.
func (c *Consumer) worker(ctx context.Context, wg *sync.WaitGroup, tasksCh <-chan task.StreamMessage, id int) {
	defer wg.Done()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("worker panicked", "worker_id", id, "panic", r)
			metrics.WorkerPanicsTotal.Inc()
		}
	}()

	c.logger.Info("worker started", "worker_id", id)

	for msg := range tasksCh {
		c.processMessage(context.Background(), id, msg)
	}

	c.logger.Info("worker stopped gracefully", "worker_id", id)
}

// processMessage extracts the task payload, processes it, records metrics, and acknowledges it.
func (c *Consumer) processMessage(ctx context.Context, workerID int, msg task.StreamMessage) {
	startTime := time.Now()

	payload, ok := msg.Values["payload"].(string)
	if !ok {
		c.logger.Warn("invalid payload format", "worker_id", workerID, "msg_id", msg.ID)
		c.acknowledge(context.Background(), msg.ID)
		return
	}

	t, err := task.Unmarshal([]byte(payload))
	if err != nil {
		c.logger.Error("failed to unmarshal task", "error", err, "worker_id", workerID, "msg_id", msg.ID)
		c.acknowledge(context.Background(), msg.ID)
		return
	}

	// Idempotency check — skip duplicate tasks
	if c.idempotency != nil && t.IdempotencyKey != "" {
		isNew, checkErr := c.idempotency.Check(ctx, t.IdempotencyKey)
		if checkErr != nil {
			c.logger.Error("idempotency check failed", "error", checkErr, "task_id", t.ID)
			// Continue processing on check failure — safer than dropping
		} else if !isNew {
			c.logger.Info("skipping duplicate task", "task_id", t.ID, "idempotency_key", t.IdempotencyKey)
			metrics.TasksDuplicateTotal.Inc()
			c.acknowledge(context.Background(), msg.ID)
			return
		}
	}

	workerStrID := fmt.Sprintf("%s-%d", c.consumer, workerID)
	c.publishEvent(ctx, task.Event{
		Type:     "Processing",
		TaskID:   t.ID,
		WorkerID: workerStrID,
	})

	c.logger.Info("processing task",
		"worker_id", workerID,
		"task_id", t.ID,
		"task_type", t.Type,
		"retry", fmt.Sprintf("%d/%d", t.CurrentRetries, t.MaxRetries),
	)

	// Use injectable processor
	processCtx := ctx
	if t.Timeout != nil {
		var cancel context.CancelFunc
		processCtx, cancel = context.WithTimeout(ctx, *t.Timeout)
		defer cancel()
	}

	err = c.processor.Process(processCtx, t)
	duration := time.Since(startTime).Seconds()
	metrics.TaskDurationSeconds.WithLabelValues(t.Type).Observe(duration)

	if err != nil {
		c.logger.Error("task failed", "error", err, "worker_id", workerID, "task_id", t.ID)
		metrics.TasksProcessedTotal.WithLabelValues(t.Type, "error").Inc()
		metrics.TasksFailedTotal.WithLabelValues(t.Type).Inc()

		c.publishEvent(ctx, task.Event{
			Type:     "Failed",
			TaskID:   t.ID,
			WorkerID: workerStrID,
		})

		c.handleTaskFailure(context.Background(), workerID, t, msg.ID, err, workerStrID)
		return
	}

	metrics.TasksProcessedTotal.WithLabelValues(t.Type, "success").Inc()
	c.logger.Info("completed task", "worker_id", workerID, "task_id", t.ID, "duration_s", duration)

	c.publishEvent(ctx, task.Event{
		Type:     "Completed",
		TaskID:   t.ID,
		WorkerID: workerStrID,
	})

	c.acknowledge(context.Background(), msg.ID)
}

func (c *Consumer) handleTaskFailure(ctx context.Context, workerID int, t *task.Task, originalMsgID string, processErr error, workerStrID string) {
	t.Error = processErr.Error()

	if t.CurrentRetries < t.MaxRetries {
		t.CurrentRetries++
		c.logger.Info("re-queueing task",
			"worker_id", workerID,
			"task_id", t.ID,
			"retry", fmt.Sprintf("%d/%d", t.CurrentRetries, t.MaxRetries),
		)

		if _, err := c.store.Add(ctx, c.stream, t); err != nil {
			c.logger.Error("failed to re-queue task", "error", err, "worker_id", workerID, "task_id", t.ID)
			return
		}
	} else {
		c.logger.Warn("task exceeded max retries, moving to DLQ",
			"worker_id", workerID,
			"task_id", t.ID,
		)
		metrics.TasksDLQTotal.WithLabelValues(t.Type).Inc()

		c.publishEvent(ctx, task.Event{
			Type:     "DLQ",
			TaskID:   t.ID,
			WorkerID: workerStrID,
		})

		if _, err := c.store.Add(ctx, c.dlqStream, t); err != nil {
			c.logger.Error("failed to move task to DLQ", "error", err, "worker_id", workerID, "task_id", t.ID)
			return
		}
	}

	c.acknowledge(ctx, originalMsgID)
}

func (c *Consumer) acknowledge(ctx context.Context, msgID string) {
	if err := c.store.Ack(ctx, c.stream, c.group, msgID); err != nil {
		c.logger.Error("failed to acknowledge message", "error", err, "msg_id", msgID)
	}
}

func (c *Consumer) publishEvent(ctx context.Context, e task.Event) {
	if err := c.publisher.Publish(ctx, e); err != nil {
		c.logger.Warn("failed to publish event", "error", err, "type", e.Type, "task_id", e.TaskID)
	}
}
