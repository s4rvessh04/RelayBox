package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
}

// NewKafkaConsumer initializes a new Kafka consumer and subscribes to the provided topics.
// Auto-commit is disabled to ensure offsets are only committed after successful processing.
func NewKafkaConsumer(brokers, groupID string, topics []string) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	return &KafkaConsumer{
		consumer: c,
	}, nil
}

// Start polls Kafka for messages, extracts event metadata from headers,
// and passes structured events to the provided handler.
// Offsets are committed only after the handler returns successfully.
func (kc *KafkaConsumer) Start(ctx context.Context, handler func(ctx context.Context, event *Event) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// ReadMessage blocks until a message is available or timeout (1s).
			msg, err := kc.consumer.ReadMessage(1000)
			if err != nil {
				// Timeout errors are expected during polling
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer read error: %v", err)
				continue
			}

			// Parse event metadata from Kafka headers
			event := &Event{
				AggregateID: string(msg.Key),
				Payload:     msg.Value,
			}
			for _, h := range msg.Headers {
				switch h.Key {
				case "event_id":
					event.ID = string(h.Value)
				case "aggregate_type":
					event.AggregateType = string(h.Value)
				case "event_type":
					event.EventType = string(h.Value)
				case "idempotency_key":
					event.IdempotencyKey = string(h.Value)
				}
			}

			// Fallback: use Kafka key if headers are missing (backward compat)
			if event.ID == "" {
				event.ID = string(msg.Key)
			}
			if event.IdempotencyKey == "" {
				event.IdempotencyKey = string(msg.Key)
			}

			if err := handler(ctx, event); err != nil {
				log.Printf("Event processing error for event %s: %v", event.ID, err)
				// Don't commit offset on failure — message will be redelivered
				continue
			}

			// Commit offset only after successful processing
			if _, err := kc.consumer.CommitMessage(msg); err != nil {
				log.Printf("Failed to commit offset for event %s: %v", event.ID, err)
			}
		}
	}
}

// Close closes the underlying Kafka consumer.
func (kc *KafkaConsumer) Close() {
	kc.consumer.Close()
}
