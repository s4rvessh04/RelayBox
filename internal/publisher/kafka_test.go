package publisher

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MockProducer struct{}

func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return nil
}
func (m *MockProducer) Flush(timeoutMs int) int { return 0 }
func (m *MockProducer) Close()                     {}

func BenchmarkPublish(b *testing.B) {
	kp := &KafkaPublisher{
		producer: &MockProducer{},
	}
	topic := "test-topic"
	key := "test-key"
	val := []byte("test-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = kp.Publish(topic, key, val)
	}
}
