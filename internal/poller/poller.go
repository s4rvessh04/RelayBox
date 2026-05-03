package poller

import (
	"context"
	"log"
	"time"

	"outbox-relay/internal/metrics"
	"outbox-relay/internal/publisher"
	"outbox-relay/internal/store"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Store defines the interface the poller needs from the data layer.
// This enables unit testing with mocks.
type Store interface {
	GetPendingCount(ctx context.Context) (int64, error)
	FetchPendingBatch(ctx context.Context, limit int) ([]*store.OutboxEvent, pgx.Tx, error)
	MarkSent(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error
	MarkFailed(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error
	ResetFailedForRetry(ctx context.Context, maxRetries int) (int64, error)
}

// Publisher defines the interface the poller needs from the event publisher.
type Publisher interface {
	Publish(msg publisher.OutboxMessage) error
}

type Poller struct {
	store      Store
	publisher  Publisher
	batchSize  int
	interval   time.Duration
	topic      string
	maxRetries int
}

func NewPoller(s Store, p Publisher, batchSize int, interval time.Duration, topic string) *Poller {
	return &Poller{
		store:      s,
		publisher:  p,
		batchSize:  batchSize,
		interval:   interval,
		topic:      topic,
		maxRetries: 5, // Default max retries before events stay FAILED
	}
}

// SetMaxRetries overrides the default max retry count for failed events.
func (p *Poller) SetMaxRetries(n int) {
	p.maxRetries = n
}

func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Separate ticker for metrics and retry — runs every 5 seconds instead of every poll tick
	metricsTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()

	log.Printf("Poller started: batchSize=%d, interval=%v, topic=%s, maxRetries=%d",
		p.batchSize, p.interval, p.topic, p.maxRetries)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-metricsTicker.C:
			// Update pending count metric (less frequently to reduce DB load)
			if count, err := p.store.GetPendingCount(ctx); err == nil {
				metrics.PendingEventsCount.Set(float64(count))
			}
			// Auto-retry failed events that haven't exceeded max retries
			if resetCount, err := p.store.ResetFailedForRetry(ctx, p.maxRetries); err != nil {
				log.Printf("Failed to reset events for retry: %v", err)
			} else if resetCount > 0 {
				log.Printf("Auto-retried %d failed events (retry_count < %d)", resetCount, p.maxRetries)
			}
		case <-ticker.C:
			if err := p.processBatch(ctx); err != nil {
				log.Printf("Batch processing error: %v", err)
			}
		}
	}
}

func (p *Poller) processBatch(ctx context.Context) error {
	start := time.Now()
	defer func() {
		metrics.BatchDuration.Observe(time.Since(start).Seconds())
	}()

	events, tx, err := p.store.FetchPendingBatch(ctx, p.batchSize)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if len(events) == 0 {
		return nil
	}

	var sentIds []uuid.UUID
	var failedIds []uuid.UUID

	for _, e := range events {
		publishStart := time.Now()
		err := p.publisher.Publish(publisher.OutboxMessage{
			Topic:          p.topic,
			Key:            e.AggregateID,
			Value:          e.Payload,
			EventID:        e.ID.String(),
			AggregateType:  e.AggregateType,
			EventType:      e.EventType,
			IdempotencyKey: e.IdempotencyKey,
		})
		metrics.KafkaPublishDuration.Observe(time.Since(publishStart).Seconds())

		if err != nil {
			log.Printf("Failed to publish event %s: %v", e.ID, err)
			failedIds = append(failedIds, e.ID)
			metrics.EventsFailedTotal.Inc()
		} else {
			sentIds = append(sentIds, e.ID)
			metrics.EventsPublishedTotal.Inc()
			metrics.EventsByType.WithLabelValues(e.EventType).Inc()
		}
	}

	if len(sentIds) > 0 {
		if err := p.store.MarkSent(ctx, tx, sentIds); err != nil {
			return err
		}
	}

	if len(failedIds) > 0 {
		if err := p.store.MarkFailed(ctx, tx, failedIds); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
