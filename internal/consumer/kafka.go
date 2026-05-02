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
func NewKafkaConsumer(brokers, groupID string, topics []string) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
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

// Start polls Kafka for messages and passes them to the provided handler.
func (kc *KafkaConsumer) Start(ctx context.Context, handler func(ctx context.Context, event *Event) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// ReadMessage blocks until a message is available or timeout occurs.
			msg, err := kc.consumer.ReadMessage(100)
			if err != nil {
				// Check if it's a timeout error (which is expected during polling)
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer read error: %v", err)
				continue
			}

			// Mapping the Kafka key to IdempotencyKey and ID for the sample implementation.
			// In a production system, the payload (msg.Value) would be parsed as JSON
			// to extract the actual event ID and idempotency key.
			event := &Event{
				ID:             string(msg.Key),
				IdempotencyKey: string(msg.Key),
				Payload:        msg.Value,
			}

			if err := handler(ctx, event); err != nil {
				log.Printf("Event processing error for event %s: %v", event.ID, err)
			}
		}
	}
}

// Close closes the underlying Kafka consumer.
func (kc *KafkaConsumer) Close() {
	kc.consumer.Close()
}
