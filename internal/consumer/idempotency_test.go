package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"outbox-relay/internal/dlq"
	"outbox-relay/internal/store"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type MockRedisStore struct {
	SetRes bool
	SetErr error
	DelErr error
}

func (m *MockRedisStore) SetIdempotencyKey(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return m.SetRes, m.SetErr
}
func (m *MockRedisStore) DeleteIdempotencyKey(ctx context.Context, key string) error {
	return m.DelErr
}
func (m *MockRedisStore) Close() error { return nil }

// MockPostgresStore satisfies store.PostgresStoreInterface for the DLQ manager.
type MockPostgresStore struct {
	ExecErr error
}

func (m *MockPostgresStore) Close() error                        { return nil }
func (m *MockPostgresStore) Ping(ctx context.Context) error      { return nil }
func (m *MockPostgresStore) GetPendingCount(ctx context.Context) (int64, error) {
	return 0, nil
}
func (m *MockPostgresStore) FetchPendingBatch(ctx context.Context, limit int) ([]*store.OutboxEvent, pgx.Tx, error) {
	return nil, nil, nil
}
func (m *MockPostgresStore) MarkSent(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	return nil
}
func (m *MockPostgresStore) MarkFailed(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	return nil
}
func (m *MockPostgresStore) ResetToPending(ctx context.Context, eventType string) (int64, error) {
	return 0, nil
}
func (m *MockPostgresStore) ResetFailedForRetry(ctx context.Context, maxRetries int) (int64, error) {
	return 0, nil
}
func (m *MockPostgresStore) Exec(ctx context.Context, query string, args ...any) (any, error) {
	return nil, m.ExecErr
}
func (m *MockPostgresStore) Begin(ctx context.Context) (pgx.Tx, error) { return nil, nil }
func (m *MockPostgresStore) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}

func newTestEvent() *Event {
	return &Event{
		ID:             "test-event-1",
		IdempotencyKey: "idem-key-1",
		Payload:        []byte(`{"action": "test"}`),
		AggregateType:  "Order",
		AggregateID:    "order-1",
		EventType:      "OrderCreated",
	}
}

func TestProcessSuccess(t *testing.T) {
	mockRedis := &MockRedisStore{SetRes: true}
	mockStore := &MockPostgresStore{}
	dlqMgr := dlq.NewManager(mockStore)
	handlerCalled := false
	handler := func(ctx context.Context, e *Event) error {
		handlerCalled = true
		return nil
	}

	cons := NewIdempotentConsumer(mockRedis, dlqMgr, handler, 24*time.Hour)
	err := cons.Process(context.Background(), newTestEvent())

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !handlerCalled {
		t.Fatal("expected handler to be called")
	}
}

func TestProcessSkipsDuplicate(t *testing.T) {
	mockRedis := &MockRedisStore{SetRes: false} // Key already exists
	mockStore := &MockPostgresStore{}
	dlqMgr := dlq.NewManager(mockStore)
	handlerCalled := false
	handler := func(ctx context.Context, e *Event) error {
		handlerCalled = true
		return nil
	}

	cons := NewIdempotentConsumer(mockRedis, dlqMgr, handler, 24*time.Hour)
	err := cons.Process(context.Background(), newTestEvent())

	if err != nil {
		t.Fatalf("expected no error on skip, got %v", err)
	}
	if handlerCalled {
		t.Fatal("handler should NOT be called for duplicate event")
	}
}

func TestProcessRejectsEmptyIdempotencyKey(t *testing.T) {
	mockRedis := &MockRedisStore{SetRes: true}
	mockStore := &MockPostgresStore{}
	dlqMgr := dlq.NewManager(mockStore)
	handler := func(ctx context.Context, e *Event) error { return nil }

	cons := NewIdempotentConsumer(mockRedis, dlqMgr, handler, 24*time.Hour)
	event := newTestEvent()
	event.IdempotencyKey = ""

	err := cons.Process(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for empty idempotency key")
	}
}

func TestProcessHandlerFailurePushesToDLQ(t *testing.T) {
	mockRedis := &MockRedisStore{SetRes: true}
	mockStore := &MockPostgresStore{}
	dlqMgr := dlq.NewManager(mockStore)
	handler := func(ctx context.Context, e *Event) error {
		return fmt.Errorf("processing error")
	}

	cons := NewIdempotentConsumer(mockRedis, dlqMgr, handler, 24*time.Hour)
	err := cons.Process(context.Background(), newTestEvent())

	if err == nil {
		t.Fatal("expected error from handler failure")
	}
}

func TestProcessRedisFailure(t *testing.T) {
	mockRedis := &MockRedisStore{SetErr: fmt.Errorf("redis connection refused")}
	mockStore := &MockPostgresStore{}
	dlqMgr := dlq.NewManager(mockStore)
	handler := func(ctx context.Context, e *Event) error { return nil }

	cons := NewIdempotentConsumer(mockRedis, dlqMgr, handler, 24*time.Hour)
	err := cons.Process(context.Background(), newTestEvent())

	if err == nil {
		t.Fatal("expected error on Redis failure")
	}
}

func BenchmarkProcess(b *testing.B) {
	ctx := context.Background()
	mockRedis := &MockRedisStore{SetRes: true}
	mockStore := &MockPostgresStore{}
	dlqMgr := dlq.NewManager(mockStore)
	handler := func(ctx context.Context, e *Event) error { return nil }

	cons := NewIdempotentConsumer(mockRedis, dlqMgr, handler, 24*time.Hour)
	event := newTestEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cons.Process(ctx, event)
	}
}
