package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxEvent struct {
	ID             uuid.UUID
	AggregateType  string
	AggregateID    string
	EventType      string
	Payload        json.RawMessage
	Status         string
	IdempotencyKey string
	CreatedAt      time.Time
	SentAt         *time.Time
	RetryCount     int
}

type PostgresStoreInterface interface {
	Close() error
	GetPendingCount(ctx context.Context) (int64, error)
	Ping(ctx context.Context) error
	FetchPendingBatch(ctx context.Context, limit int) ([]*OutboxEvent, pgx.Tx, error)
	MarkSent(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error
	MarkFailed(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error
	ResetToPending(ctx context.Context, eventType string) (int64, error)
	ResetFailedForRetry(ctx context.Context, maxRetries int) (int64, error)
	Exec(ctx context.Context, query string, args ...any) (any, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Exec(ctx context.Context, query string, args ...any) (any, error) {
	return s.pool.Exec(ctx, query, args...)
}

func (s *PostgresStore) Begin(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

func (s *PostgresStore) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return s.pool.Query(ctx, sql, args...)
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) GetPendingCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status = 'PENDING'`).Scan(&count)
	return count, err
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresStore) FetchPendingBatch(ctx context.Context, limit int) ([]*OutboxEvent, pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, status, idempotency_key, created_at, sent_at, retry_count
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		tx.Rollback(ctx)
		return nil, nil, err
	}
	defer rows.Close()

	var events []*OutboxEvent
	for rows.Next() {
		e := &OutboxEvent{}
		err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload, &e.Status, &e.IdempotencyKey, &e.CreatedAt, &e.SentAt, &e.RetryCount)
		if err != nil {
			tx.Rollback(ctx)
			return nil, nil, err
		}
		events = append(events, e)
	}

	return events, tx, nil
}

func (s *PostgresStore) MarkSent(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'SENT', sent_at = NOW()
		WHERE id = ANY($1)
	`, ids)
	return err
}

func (s *PostgresStore) MarkFailed(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'FAILED', retry_count = retry_count + 1
		WHERE id = ANY($1)
	`, ids)
	return err
}

func (s *PostgresStore) ResetToPending(ctx context.Context, eventType string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING', retry_count = 0
		WHERE status = 'FAILED' AND ($1 = '' OR event_type = $1)
	`, eventType)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ResetFailedForRetry resets FAILED events with retry_count below maxRetries back to PENDING.
// This enables automatic bounded retries without manual intervention.
func (s *PostgresStore) ResetFailedForRetry(ctx context.Context, maxRetries int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING'
		WHERE status = 'FAILED' AND retry_count < $1
	`, maxRetries)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
