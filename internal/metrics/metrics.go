package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Relay Metrics
	PendingEventsCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_pending_events_count",
		Help: "Current number of events in PENDING status in the outbox table",
	})

	EventsPublishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_events_published_total",
		Help: "Total number of events successfully published to Kafka",
	})

	EventsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_events_failed_total",
		Help: "Total number of events that failed to be published to Kafka",
	})

	BatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "relay_batch_duration_seconds",
		Help:    "Time taken to process a single polling batch",
		Buckets: prometheus.DefBuckets,
	})

	KafkaPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "relay_kafka_publish_duration_seconds",
		Help:    "Time taken to publish a single message to Kafka",
		Buckets: prometheus.DefBuckets,
	})

	EventsByType = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_events_total",
		Help: "Total number of events relayed, partitioned by event type",
	}, []string{"event_type"})

	// Consumer Metrics
	IdempotencySkipsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_idempotency_skips_total",
		Help: "Total number of events skipped because they were already processed",
	})

	ProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_processing_duration_seconds",
		Help:    "Time taken by the business logic handler to process an event",
		Buckets: prometheus.DefBuckets,
	})

	EventsProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_events_processed_total",
		Help: "Total number of events successfully processed by the consumer",
	})
)
