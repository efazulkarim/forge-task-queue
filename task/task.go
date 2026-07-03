package task

import (
	"encoding/json"
	"time"
)

// Priority levels for task scheduling.
const (
	PriorityHigh   = "high"
	PriorityNormal = "normal"
	PriorityLow    = "low"
)

// Task represents a unit of work in Buraq.
type Task struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	CreatedAt      time.Time       `json:"created_at"`
	MaxRetries     int             `json:"max_retries"`
	CurrentRetries int             `json:"current_retries"`
	Error          string          `json:"error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Priority       string          `json:"priority,omitempty"`
	Timeout        *time.Duration  `json:"timeout,omitempty"`
}

// Event represents a state change for a task, broadcasted via Redis Pub/Sub.
type Event struct {
	Type     string `json:"type"` // Pending, Processing, Completed, Failed, DLQ
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"` // Used by dashboard to animate the "worker map"
}

// Marshal converts the Task into a JSON byte slice.
func (t *Task) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

// Unmarshal parses a JSON byte slice into a Task.
func Unmarshal(data []byte) (*Task, error) {
	var t Task
	err := json.Unmarshal(data, &t)
	return &t, err
}
