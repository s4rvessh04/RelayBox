package poller

import (
	"context"
	"log"
	"time"

	"outbox-relay/internal/metrics"
	"outbox-relay/internal/publisher"
	"outbox-relay/internal/store"

	"github.com/google/uuid"
)

type Poller struct {
	store     *store.PostgresStore
	publisher *publisher.KafkaPublisher
	batchSize int
	interval  time.Duration
	topic     string
}

func NewPoller(s *store.PostgresStore, p *publisher.KafkaPublisher, batchSize int, interval time.Duration, topic string) *Poller {
	return &Poller{
		store:     s,
		publisher: p,
		batchSize: batchSize,
		interval:  interval,
		topic:     topic,
	}
}

func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	log.Printf("Poller started: batchSize=%d, interval=%v, topic=%s", p.batchSize, p.interval, p.topic)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if count, err := p.store.GetPendingCount(ctx); err == nil {
				metrics.PendingEventsCount.Set(float64(count))
			}
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
		err := p.publisher.Publish(p.topic, e.AggregateID, e.Payload)
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
