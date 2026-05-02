package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"outbox-relay/internal/dlq"
	"outbox-relay/internal/metrics"
	"outbox-relay/internal/store"

	"github.com/google/uuid"
)

type Event struct {
	ID             string
	IdempotencyKey string
	Payload        []byte
	AggregateType  string
	AggregateID    string
	EventType      string
}

type EventHandler func(ctx context.Context, event *Event) error

type IdempotentConsumer struct {
	redis   store.RedisStoreInterface
	dlq     *dlq.Manager
	handler EventHandler
	ttl     time.Duration
}

func NewIdempotentConsumer(r store.RedisStoreInterface, dlqMgr *dlq.Manager, h EventHandler, ttl time.Duration) *IdempotentConsumer {
	return &IdempotentConsumer{
		redis:   r,
		dlq:     dlqMgr,
		handler: h,
		ttl:     ttl,
	}
}

func (c *IdempotentConsumer) Process(ctx context.Context, event *Event) error {
	if event.IdempotencyKey == "" {
		return fmt.Errorf("event missing idempotency key")
	}

	// Try to set idempotency key in Redis (SETNX)
	ok, err := c.redis.SetIdempotencyKey(ctx, event.IdempotencyKey, c.ttl)
	if err != nil {
		return fmt.Errorf("redis idempotency check failed: %w", err)
	}

	if !ok {
		log.Printf("Skipping already processed event: %s", event.IdempotencyKey)
		metrics.IdempotencySkipsTotal.Inc()
		metrics.EventsProcessedTotal.Inc()
		return nil
	}

	// Execute business logic
	start := time.Now()
	err = c.handler(ctx, event)
	metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		// On failure, remove key to allow retry
		if deleteErr := c.redis.DeleteIdempotencyKey(ctx, event.IdempotencyKey); deleteErr != nil {
			log.Printf("Warning: failed to delete idempotency key after processing failure: %v", deleteErr)
		}

		// Move to DLQ
		id, _ := uuid.Parse(event.ID)
		if id == (uuid.UUID{}) {
			id = uuid.New()
		}
		_ = c.dlq.Push(ctx, dlq.Event{
			ID:            id,
			OriginalID:    id,
			AggregateType: event.AggregateType,
			AggregateID:   event.AggregateID,
			EventType:      event.EventType,
			Payload:       event.Payload,
			ErrorMessage:  err.Error(),
		})

		return fmt.Errorf("handler processing failed: %w", err)
	}

	metrics.EventsProcessedTotal.Inc()
	return nil
}
