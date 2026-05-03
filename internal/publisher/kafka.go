package publisher

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// OutboxMessage represents a structured message for publishing to Kafka.
type OutboxMessage struct {
	Topic          string
	Key            string
	Value          []byte
	EventID        string
	AggregateType  string
	EventType      string
	IdempotencyKey string
}

// PublisherInterface defines the contract for an event publisher.
type PublisherInterface interface {
	Publish(msg OutboxMessage) error
	Close()
}

type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
	Close()
}

type KafkaPublisher struct {
	producer Producer
}

func NewKafkaPublisher(brokers string) (*KafkaPublisher, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     brokers,
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
		"retries":                               10,
		"queue.buffering.max.messages":           100000,
		"queue.buffering.max.kbytes":             102400,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &KafkaPublisher{producer: p}, nil
}

// Publish sends a message to Kafka and waits for delivery confirmation.
// Event metadata is transmitted as Kafka headers for the consumer to parse.
func (kp *KafkaPublisher) Publish(msg OutboxMessage) error {
	headers := []kafka.Header{
		{Key: "event_id", Value: []byte(msg.EventID)},
		{Key: "aggregate_type", Value: []byte(msg.AggregateType)},
		{Key: "event_type", Value: []byte(msg.EventType)},
		{Key: "idempotency_key", Value: []byte(msg.IdempotencyKey)},
	}

	deliveryChan := make(chan kafka.Event, 1)
	err := kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(msg.Key),
		Value:          msg.Value,
		Headers:        headers,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("produce enqueue failed: %w", err)
	}

	// Block until Kafka confirms delivery or reports failure
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

func (kp *KafkaPublisher) Close() {
	kp.producer.Flush(15000)
	kp.producer.Close()
}
