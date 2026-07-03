package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"buraq/task"
)

// PriorityConsumer reads from multiple streams in priority order.
// It checks high-priority streams first, then normal, then low.
type PriorityConsumer struct {
	store          task.StreamStore
	publisher      task.EventPublisher
	processor      task.TaskProcessor
	baseStream     string
	group          string
	consumer       string
	workerCount    int
	fetchBatchSize int64
	blockTimeout   time.Duration
	maxRetries     int
	logger         *slog.Logger
}

// NewPriorityConsumer creates a consumer that reads from priority-ordered streams.
func NewPriorityConsumer(
	store task.StreamStore,
	publisher task.EventPublisher,
	processor task.TaskProcessor,
	baseStream, group, consumer string,
	workerCount int,
	fetchBatchSize int64,
	blockTimeout time.Duration,
	maxRetries int,
	logger *slog.Logger,
) *PriorityConsumer {
	return &PriorityConsumer{
		store:          store,
		publisher:      publisher,
		processor:      processor,
		baseStream:     baseStream,
		group:          group,
		consumer:       consumer,
		workerCount:    workerCount,
		fetchBatchSize: fetchBatchSize,
		blockTimeout:   blockTimeout,
		maxRetries:     maxRetries,
		logger:         logger,
	}
}

// Start creates consumer groups for all priority streams and begins processing.
func (pc *PriorityConsumer) Start(ctx context.Context) error {
	streams := []string{
		fmt.Sprintf("%s_high", pc.baseStream),
		pc.baseStream,
		fmt.Sprintf("%s_low", pc.baseStream),
	}

	// Create consumer groups for all streams
	for _, s := range streams {
		if err := pc.store.CreateGroup(ctx, s, pc.group); err != nil {
			return fmt.Errorf("failed to create consumer group for %s: %w", s, err)
		}
	}

	tasksCh := make(chan task.StreamMessage, pc.workerCount*10)
	var wg sync.WaitGroup

	// Start workers
	for i := 1; i <= pc.workerCount; i++ {
		wg.Add(1)
		go pc.worker(ctx, &wg, tasksCh, i)
	}

	// Start priority-aware fetcher
	wg.Add(1)
	go pc.fetchTasks(ctx, &wg, tasksCh, streams)

	wg.Wait()
	return nil
}

// fetchTasks reads from streams in priority order.
func (pc *PriorityConsumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup, tasksCh chan<- task.StreamMessage, streams []string) {
	defer wg.Done()
	defer close(tasksCh)

	for {
		select {
		case <-ctx.Done():
			pc.logger.Info("priority fetcher stopping")
			return
		default:
			foundAny := false
			for _, stream := range streams {
				messages, err := pc.store.ReadGroup(ctx, stream, pc.group, pc.consumer, pc.fetchBatchSize, pc.blockTimeout)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					// BUSYGROUP or Nil is expected when no new messages
					continue
				}

				for _, msg := range messages {
					foundAny = true
					select {
					case tasksCh <- msg:
					case <-ctx.Done():
						return
					}
				}
			}

			// If no messages found in any stream, sleep briefly to avoid busy loop
			if !foundAny {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (pc *PriorityConsumer) worker(ctx context.Context, wg *sync.WaitGroup, tasksCh <-chan task.StreamMessage, id int) {
	defer wg.Done()
	defer func() {
		if r := recover(); r != nil {
			pc.logger.Error("worker panicked", "worker_id", id, "panic", r)
		}
	}()

	pc.logger.Info("priority worker started", "worker_id", id)

	for msg := range tasksCh {
		pc.processMessage(context.Background(), id, msg)
	}

	pc.logger.Info("priority worker stopped", "worker_id", id)
}

func (pc *PriorityConsumer) processMessage(ctx context.Context, workerID int, msg task.StreamMessage) {
	startTime := time.Now()

	payload, ok := msg.Values["payload"].(string)
	if !ok {
		pc.logger.Warn("invalid payload format", "worker_id", workerID, "msg_id", msg.ID)
		pc.store.Ack(context.Background(), pc.baseStream, pc.group, msg.ID)
		return
	}

	t, err := task.Unmarshal([]byte(payload))
	if err != nil {
		pc.logger.Error("failed to unmarshal task", "error", err, "worker_id", workerID)
		pc.store.Ack(context.Background(), pc.baseStream, pc.group, msg.ID)
		return
	}

	// Determine which stream this task came from
	sourceStream := pc.streamForPriority(t.Priority)

	err = pc.processor.Process(ctx, t)
	duration := time.Since(startTime).Seconds()

	if err != nil {
		pc.logger.Error("task failed", "error", err, "worker_id", workerID, "task_id", t.ID)

		if t.CurrentRetries < t.MaxRetries {
			t.CurrentRetries++
			pc.store.Add(ctx, sourceStream, t)
		} else {
			dlqStream := fmt.Sprintf("%s_dlq", pc.baseStream)
			pc.store.Add(ctx, dlqStream, t)
			pc.publisher.Publish(ctx, task.Event{Type: "DLQ", TaskID: t.ID})
		}
	} else {
		pc.logger.Info("completed task", "worker_id", workerID, "task_id", t.ID, "duration_s", duration)
	}

	pc.store.Ack(ctx, sourceStream, pc.group, msg.ID)
}

func (pc *PriorityConsumer) streamForPriority(priority string) string {
	switch priority {
	case task.PriorityHigh:
		return fmt.Sprintf("%s_high", pc.baseStream)
	case task.PriorityLow:
		return fmt.Sprintf("%s_low", pc.baseStream)
	default:
		return pc.baseStream
	}
}
