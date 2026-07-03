package redisadapter

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// IdempotencyStore implements task.IdempotencyChecker using Redis SET NX.
type IdempotencyStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewIdempotencyStore creates a new idempotency checker backed by Redis.
// The ttl controls how long an idempotency key is remembered (default: 24h).
func NewIdempotencyStore(client *redis.Client, ttl time.Duration) *IdempotencyStore {
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	return &IdempotencyStore{client: client, ttl: ttl}
}

// Check returns true if the key is new (was set successfully), false if it already existed.
func (s *IdempotencyStore) Check(ctx context.Context, key string) (bool, error) {
	ok, err := s.client.SetNX(ctx, "idemp:"+key, "1", s.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}
