package task

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMarshalUnmarshalRoundtrip(t *testing.T) {
	timeout := 30 * time.Second
	original := &Task{
		ID:             "task-123",
		Type:           "send_email",
		Payload:        json.RawMessage(`{"to":"user@example.com","subject":"Hello"}`),
		CreatedAt:      time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
		MaxRetries:     3,
		CurrentRetries: 1,
		Error:          "timeout error",
		IdempotencyKey: "idem-abc-123",
		Priority:       PriorityHigh,
		Timeout:        &timeout,
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if restored.ID != original.ID {
		t.Errorf("ID mismatch: got %q, want %q", restored.ID, original.ID)
	}
	if restored.Type != original.Type {
		t.Errorf("Type mismatch: got %q, want %q", restored.Type, original.Type)
	}
	if string(restored.Payload) != string(original.Payload) {
		t.Errorf("Payload mismatch: got %s, want %s", restored.Payload, original.Payload)
	}
	if !restored.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt mismatch: got %v, want %v", restored.CreatedAt, original.CreatedAt)
	}
	if restored.MaxRetries != original.MaxRetries {
		t.Errorf("MaxRetries mismatch: got %d, want %d", restored.MaxRetries, original.MaxRetries)
	}
	if restored.CurrentRetries != original.CurrentRetries {
		t.Errorf("CurrentRetries mismatch: got %d, want %d", restored.CurrentRetries, original.CurrentRetries)
	}
	if restored.Error != original.Error {
		t.Errorf("Error mismatch: got %q, want %q", restored.Error, original.Error)
	}
	if restored.IdempotencyKey != original.IdempotencyKey {
		t.Errorf("IdempotencyKey mismatch: got %q, want %q", restored.IdempotencyKey, original.IdempotencyKey)
	}
	if restored.Priority != original.Priority {
		t.Errorf("Priority mismatch: got %q, want %q", restored.Priority, original.Priority)
	}
	if restored.Timeout == nil {
		t.Fatal("Timeout is nil after roundtrip")
	}
	if *restored.Timeout != *original.Timeout {
		t.Errorf("Timeout mismatch: got %v, want %v", *restored.Timeout, *original.Timeout)
	}
}

func TestUnmarshalInvalidJSON(t *testing.T) {
	invalid := []byte(`{"id": "broken", "type":`)
	_, err := Unmarshal(invalid)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestUnmarshalEmptyBytes(t *testing.T) {
	_, err := Unmarshal([]byte{})
	if err == nil {
		t.Fatal("expected error for empty bytes, got nil")
	}
}

func TestMarshalEmptyTask(t *testing.T) {
	empty := &Task{}
	data, err := empty.Marshal()
	if err != nil {
		t.Fatalf("Marshal of empty task failed: %v", err)
	}

	var restored Task
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal of empty task failed: %v", err)
	}

	if restored.ID != "" {
		t.Errorf("expected empty ID, got %q", restored.ID)
	}
	if restored.Type != "" {
		t.Errorf("expected empty Type, got %q", restored.Type)
	}
	if restored.MaxRetries != 0 {
		t.Errorf("expected MaxRetries 0, got %d", restored.MaxRetries)
	}
}

func TestIdempotencyKeySerialization(t *testing.T) {
	task := &Task{
		ID:             "task-1",
		Type:           "test",
		IdempotencyKey: "unique-key-42",
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify the JSON contains the idempotency_key field
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if raw["idempotency_key"] != "unique-key-42" {
		t.Errorf("idempotency_key not serialized correctly: got %v", raw["idempotency_key"])
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if restored.IdempotencyKey != "unique-key-42" {
		t.Errorf("IdempotencyKey mismatch: got %q, want %q", restored.IdempotencyKey, "unique-key-42")
	}
}

func TestIdempotencyKeyOmitEmpty(t *testing.T) {
	task := &Task{
		ID:   "task-1",
		Type: "test",
		// IdempotencyKey not set
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if _, exists := raw["idempotency_key"]; exists {
		t.Error("idempotency_key should be omitted when empty")
	}
}

func TestPrioritySerialization(t *testing.T) {
	tests := []struct {
		name     string
		priority string
	}{
		{"high", PriorityHigh},
		{"normal", PriorityNormal},
		{"low", PriorityLow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &Task{
				ID:       "task-1",
				Type:     "test",
				Priority: tt.priority,
			}

			data, err := task.Marshal()
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			restored, err := Unmarshal(data)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}
			if restored.Priority != tt.priority {
				t.Errorf("Priority mismatch: got %q, want %q", restored.Priority, tt.priority)
			}
		})
	}
}

func TestPriorityOmitEmpty(t *testing.T) {
	task := &Task{
		ID:   "task-1",
		Type: "test",
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if _, exists := raw["priority"]; exists {
		t.Error("priority should be omitted when empty")
	}
}

func TestTimeoutSerialization(t *testing.T) {
	timeout := 45 * time.Second
	task := &Task{
		ID:      "task-1",
		Type:    "test",
		Timeout: &timeout,
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if restored.Timeout == nil {
		t.Fatal("Timeout is nil after roundtrip")
	}
	if *restored.Timeout != 45*time.Second {
		t.Errorf("Timeout mismatch: got %v, want %v", *restored.Timeout, 45*time.Second)
	}
}

func TestTimeoutOmitEmpty(t *testing.T) {
	task := &Task{
		ID:   "task-1",
		Type: "test",
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if _, exists := raw["timeout"]; exists {
		t.Error("timeout should be omitted when nil")
	}
}

func TestErrorOmitEmpty(t *testing.T) {
	taskNoError := &Task{
		ID:   "task-1",
		Type: "test",
	}

	data, err := taskNoError.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if _, exists := raw["error"]; exists {
		t.Error("error should be omitted when empty")
	}
}

func TestErrorIncludedWhenSet(t *testing.T) {
	task := &Task{
		ID:    "task-1",
		Type:  "test",
		Error: "something went wrong",
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if restored.Error != "something went wrong" {
		t.Errorf("Error mismatch: got %q, want %q", restored.Error, "something went wrong")
	}
}

func TestPayloadPreservesRawJSON(t *testing.T) {
	payload := `{"nested":{"deep":true},"array":[1,2,3]}`
	task := &Task{
		ID:      "task-1",
		Type:    "test",
		Payload: json.RawMessage(payload),
	}

	data, err := task.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if string(restored.Payload) != payload {
		t.Errorf("Payload mismatch:\n  got:  %s\n  want: %s", string(restored.Payload), payload)
	}
}

func TestEventSerialization(t *testing.T) {
	e := Event{
		Type:     "Processing",
		TaskID:   "task-42",
		WorkerID: "worker-1",
	}

	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var restored Event
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if restored.Type != e.Type {
		t.Errorf("Type mismatch: got %q, want %q", restored.Type, e.Type)
	}
	if restored.TaskID != e.TaskID {
		t.Errorf("TaskID mismatch: got %q, want %q", restored.TaskID, e.TaskID)
	}
	if restored.WorkerID != e.WorkerID {
		t.Errorf("WorkerID mismatch: got %q, want %q", restored.WorkerID, e.WorkerID)
	}
}
