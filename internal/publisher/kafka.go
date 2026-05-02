package publisher

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

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
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &KafkaPublisher{producer: p}, nil
}

func (kp *KafkaPublisher) Publish(topic string, key string, value []byte) error {
	err := kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}, nil)

	if err != nil {
		return fmt.Errorf("produce failed: %w", err)
	}

	return nil
}

func (kp *KafkaPublisher) Close() {
	kp.producer.Flush(15000)
	kp.producer.Close()
}
