package task

import "context"

// StreamStore abstracts Redis Stream operations for testability.
type StreamStore interface {
	// Add appends a task to the named stream and returns the message ID.
	Add(ctx context.Context, stream string, t *Task) (string, error)
	// ReadGroup reads messages from a stream using a consumer group.
	ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block interface{}) ([]StreamMessage, error)
	// Ack acknowledges a message in a consumer group.
	Ack(ctx context.Context, stream, group, msgID string) error
	// CreateGroup creates a consumer group, ignoring "already exists" errors.
	CreateGroup(ctx context.Context, stream, group string) error
	// Range reads messages from a stream within an ID range.
	Range(ctx context.Context, stream, start, stop string, count int64) ([]StreamMessage, error)
	// Delete removes messages from a stream.
	Delete(ctx context.Context, stream string, msgIDs ...string) error
}

// StreamMessage is a platform-agnostic message from a stream.
type StreamMessage struct {
	ID      string
	Values  map[string]interface{}
}

// EventPublisher abstracts event broadcasting for testability.
type EventPublisher interface {
	// Publish sends an event to all subscribers.
	Publish(ctx context.Context, e Event) error
}

// TaskProcessor defines the contract for task execution logic.
// Implement this interface to inject custom processing into the consumer.
type TaskProcessor interface {
	// Process executes the task. Returns nil on success, error on failure.
	Process(ctx context.Context, t *Task) error
}

// TaskProcessorFunc is an adapter to allow the use of ordinary functions as TaskProcessor.
type TaskProcessorFunc func(ctx context.Context, t *Task) error

// Process calls f(ctx, t).
func (f TaskProcessorFunc) Process(ctx context.Context, t *Task) error {
	return f(ctx, t)
}

// IdempotencyChecker prevents duplicate task processing.
type IdempotencyChecker interface {
	// Check returns true if this key has NOT been seen before (and marks it as seen).
	// Returns false if the key was already seen (duplicate).
	Check(ctx context.Context, key string) (bool, error)
}
