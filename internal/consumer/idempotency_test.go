package consumer

import (
	"context"
	"testing"
	"time"
)

type MockRedisStore struct {
	SetRes bool
	SetErr error
}

func (m *MockRedisStore) SetIdempotencyKey(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return m.SetRes, m.SetErr
}
func (m *MockRedisStore) DeleteIdempotencyKey(ctx context.Context, key string) error { return nil }
func (m *MockRedisStore) Close() error                                               { return nil }

func BenchmarkProcess(b *testing.B) {
	ctx := context.Background()
	mockRedis := &MockRedisStore{SetRes: true}
	handler := func(ctx context.Context, e *Event) error { return nil }

	cons := NewIdempotentConsumer(mockRedis, handler, 24*time.Hour)
	event := &Event{
		ID:             "1",
		IdempotencyKey: "key-1",
		Payload:        []byte("payload"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cons.Process(ctx, event)
	}
}
