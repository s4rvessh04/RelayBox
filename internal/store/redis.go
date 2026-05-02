package store

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStoreInterface interface {
	SetIdempotencyKey(ctx context.Context, key string, expiration time.Duration) (bool, error)
	DeleteIdempotencyKey(ctx context.Context, key string) error
	Close() error
}

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(ctx context.Context, addr string) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &RedisStore{client: client}, nil
}

func (s *RedisStore) SetIdempotencyKey(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	// Use Set with NX option for idempotency
	set, err := s.client.SetNX(ctx, "processed:"+key, "1", expiration).Result()
	return set, err
}

func (s *RedisStore) DeleteIdempotencyKey(ctx context.Context, key string) error {
	return s.client.Del(ctx, "processed:"+key).Err()
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}
