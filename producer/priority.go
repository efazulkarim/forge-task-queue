package producer

import (
	"context"
	"fmt"
	"log/slog"

	"buraq/task"
)

// PriorityProducer wraps a base Producer and routes tasks to priority-specific streams.
type PriorityProducer struct {
	store     task.StreamStore
	publisher task.EventPublisher
	baseStream string
	logger    *slog.Logger
}

// NewPriorityProducer creates a producer that routes tasks to priority-based streams.
// High-priority tasks go to "{baseStream}_high", normal to "{baseStream}", low to "{baseStream}_low".
func NewPriorityProducer(store task.StreamStore, publisher task.EventPublisher, baseStream string, logger *slog.Logger) *PriorityProducer {
	return &PriorityProducer{
		store:      store,
		publisher:  publisher,
		baseStream: baseStream,
		logger:     logger,
	}
}

// Produce routes the task to the appropriate stream based on its priority.
func (pp *PriorityProducer) Produce(ctx context.Context, t *task.Task) (string, error) {
	stream := pp.streamForPriority(t.Priority)

	msgID, err := pp.store.Add(ctx, stream, t)
	if err != nil {
		pp.logger.Error("failed to enqueue task", "error", err, "task_id", t.ID, "priority", t.Priority)
		return "", err
	}

	if pubErr := pp.publisher.Publish(ctx, task.Event{
		Type:   "Pending",
		TaskID: t.ID,
	}); pubErr != nil {
		pp.logger.Warn("failed to publish Pending event", "error", pubErr, "task_id", t.ID)
	}

	return msgID, nil
}

func (pp *PriorityProducer) streamForPriority(priority string) string {
	switch priority {
	case task.PriorityHigh:
		return fmt.Sprintf("%s_high", pp.baseStream)
	case task.PriorityLow:
		return fmt.Sprintf("%s_low", pp.baseStream)
	default:
		return pp.baseStream
	}
}

// Streams returns all stream names used by this producer (for consumer setup).
func (pp *PriorityProducer) Streams() []string {
	return []string{
		fmt.Sprintf("%s_high", pp.baseStream),
		pp.baseStream,
		fmt.Sprintf("%s_low", pp.baseStream),
	}
}
