package dlq

import (
	"context"

	"github.com/google/uuid"
	"outbox-relay/internal/store"
)

type Event struct {
	ID            uuid.UUID
	OriginalID    uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	ErrorMessage  string
}

type Manager struct {
	store store.PostgresStoreInterface
}

func NewManager(s store.PostgresStoreInterface) *Manager {
	return &Manager{store: s}
}

func (m *Manager) Push(ctx context.Context, e Event) error {
	_, err := m.store.Exec(ctx, `
		INSERT INTO outbox_dlq (id, original_id, aggregate_type, aggregate_id, event_type, payload, error_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.ID, e.OriginalID, e.AggregateType, e.AggregateID, e.EventType, e.Payload, e.ErrorMessage)
	return err
}

func (m *Manager) RetryBatch(ctx context.Context, limit int) (int64, error) {
	tx, err := m.store.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var ids []uuid.UUID
	rows, err := tx.Query(ctx, `
		SELECT id FROM outbox_dlq 
		WHERE deleted_at IS NULL 
		LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return 0, nil
	}

	// Soft delete records being retried
	_, err = tx.Exec(ctx, `UPDATE outbox_dlq SET deleted_at = NOW() WHERE id = ANY($1)`, ids)
	if err != nil {
		return 0, err
	}

	return int64(len(ids)), tx.Commit(ctx)
}

func (m *Manager) Purge(ctx context.Context) error {
	_, err := m.store.Exec(ctx, `UPDATE outbox_dlq SET deleted_at = NOW() WHERE deleted_at IS NULL`)
	return err
}
