package publisher

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MockProducer struct {
	ProduceErr  error
	DeliveryErr error
}

func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	if m.ProduceErr != nil {
		return m.ProduceErr
	}
	if deliveryChan != nil {
		// Simulate delivery report
		if m.DeliveryErr != nil {
			msg.TopicPartition.Error = m.DeliveryErr
		}
		deliveryChan <- msg
	}
	return nil
}
func (m *MockProducer) Flush(timeoutMs int) int { return 0 }
func (m *MockProducer) Close()                  {}

func TestPublishSuccess(t *testing.T) {
	kp := &KafkaPublisher{producer: &MockProducer{}}
	err := kp.Publish(OutboxMessage{
		Topic:          "test-topic",
		Key:            "test-key",
		Value:          []byte("test-value"),
		EventID:        "ev-1",
		IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestPublishDeliveryFailure(t *testing.T) {
	kp := &KafkaPublisher{producer: &MockProducer{
		DeliveryErr: fmt.Errorf("broker unavailable"),
	}}
	err := kp.Publish(OutboxMessage{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test-value"),
	})
	if err == nil {
		t.Fatal("expected delivery error, got nil")
	}
}

func TestPublishEnqueueFailure(t *testing.T) {
	kp := &KafkaPublisher{producer: &MockProducer{
		ProduceErr: fmt.Errorf("queue full"),
	}}
	err := kp.Publish(OutboxMessage{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test-value"),
	})
	if err == nil {
		t.Fatal("expected enqueue error, got nil")
	}
}

func BenchmarkPublish(b *testing.B) {
	kp := &KafkaPublisher{
		producer: &MockProducer{},
	}
	msg := OutboxMessage{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = kp.Publish(msg)
	}
}
