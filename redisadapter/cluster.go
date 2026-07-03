package redisadapter

import (
	"context"
	"time"

	"buraq/task"

	"github.com/redis/go-redis/v9"
)

// ClusterStore implements task.StreamStore using a Redis Cluster client.
type ClusterStore struct {
	client *redis.ClusterClient
}

// NewClusterStore creates a new Redis Cluster-backed stream store.
func NewClusterStore(client *redis.ClusterClient) *ClusterStore {
	return &ClusterStore{client: client}
}

// Add appends a task to the named stream via XADD.
func (s *ClusterStore) Add(ctx context.Context, stream string, t *task.Task) (string, error) {
	data, err := t.Marshal()
	if err != nil {
		return "", err
	}

	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"payload": data,
		},
	}).Result()
}

// ReadGroup reads messages from a stream using XREADGROUP.
func (s *ClusterStore) ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block interface{}) ([]task.StreamMessage, error) {
	blockDuration, ok := block.(time.Duration)
	if !ok {
		blockDuration = 2 * time.Second
	}

	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    blockDuration,
	}).Result()

	if err != nil {
		return nil, err
	}

	var messages []task.StreamMessage
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			messages = append(messages, task.StreamMessage{
				ID:     msg.ID,
				Values: msg.Values,
			})
		}
	}
	return messages, nil
}

// Ack acknowledges a message via XACK.
func (s *ClusterStore) Ack(ctx context.Context, stream, group, msgID string) error {
	return s.client.XAck(ctx, stream, group, msgID).Err()
}

// CreateGroup creates a consumer group via XGROUP CREATE MKSTREAM.
func (s *ClusterStore) CreateGroup(ctx context.Context, stream, group string) error {
	err := s.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Range reads messages from a stream via XRANGE.
func (s *ClusterStore) Range(ctx context.Context, stream, start, stop string, count int64) ([]task.StreamMessage, error) {
	messages, err := s.client.XRangeN(ctx, stream, start, stop, count).Result()
	if err != nil {
		return nil, err
	}

	var result []task.StreamMessage
	for _, msg := range messages {
		result = append(result, task.StreamMessage{
			ID:     msg.ID,
			Values: msg.Values,
		})
	}
	return result, nil
}

// Delete removes messages from a stream via XDEL.
func (s *ClusterStore) Delete(ctx context.Context, stream string, msgIDs ...string) error {
	return s.client.XDel(ctx, stream, msgIDs...).Err()
}
