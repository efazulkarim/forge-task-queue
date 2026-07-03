package producer

import (
	"context"
	"log/slog"

	"buraq/task"
)

// Producer handles enqueueing tasks to a stream.
type Producer struct {
	store      task.StreamStore
	publisher  task.EventPublisher
	stream     string
	logger     *slog.Logger
}

// New creates a new Producer instance.
func New(store task.StreamStore, publisher task.EventPublisher, stream string, logger *slog.Logger) *Producer {
	return &Producer{
		store:     store,
		publisher: publisher,
		stream:    stream,
		logger:    logger,
	}
}

// Produce serializes the task, adds it to the stream, and publishes a Pending event.
func (p *Producer) Produce(ctx context.Context, t *task.Task) (string, error) {
	msgID, err := p.store.Add(ctx, p.stream, t)
	if err != nil {
		p.logger.Error("failed to enqueue task", "error", err, "task_id", t.ID)
		return "", err
	}

	if pubErr := p.publisher.Publish(ctx, task.Event{
		Type:   "Pending",
		TaskID: t.ID,
	}); pubErr != nil {
		// Log but don't fail — the task is already enqueued
		p.logger.Warn("failed to publish Pending event", "error", pubErr, "task_id", t.ID)
	}

	return msgID, nil
}
