package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher implements Publisher using Kafka.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher creates a new Kafka publisher.
func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	if topic == "" {
		topic = "aigw-events-policy"
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        true, // Fire-and-forget for performance
	}
	return &KafkaPublisher{writer: writer}
}

// Publish sends an event to Kafka.
func (p *KafkaPublisher) Publish(ctx context.Context, event *OpsEvent) error {
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().Unix()
	}

	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventType),
		Value: value,
	})
}

// Close closes the Kafka writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
