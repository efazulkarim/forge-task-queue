package event

import (
	"context"
	"encoding/json"
	"log/slog"

	"buraq/task"

	"github.com/redis/go-redis/v9"
)

// Publisher implements task.EventPublisher using Redis Pub/Sub.
type Publisher struct {
	client  *redis.Client
	channel string
	logger  *slog.Logger
}

// NewPublisher creates a new event publisher.
func NewPublisher(client *redis.Client, channel string, logger *slog.Logger) *Publisher {
	return &Publisher{
		client:  client,
		channel: channel,
		logger:  logger,
	}
}

// Publish serializes and broadcasts an event via Redis Pub/Sub.
func (p *Publisher) Publish(ctx context.Context, e task.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		p.logger.Error("failed to marshal event", "error", err, "task_id", e.TaskID)
		return err
	}

	if err := p.client.Publish(ctx, p.channel, string(data)).Err(); err != nil {
		p.logger.Error("failed to publish event", "error", err, "task_id", e.TaskID, "type", e.Type)
		return err
	}
	return nil
}
