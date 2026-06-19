package events

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// PublisherConfig holds event publisher configuration.
type PublisherConfig struct {
	Enabled bool   `yaml:"enabled"`
	Type    string `yaml:"type"`  // redis | kafka
	Topic   string `yaml:"topic"` // stream key (Redis) or topic (Kafka)
	Kafka   struct {
		Brokers []string `yaml:"brokers"`
	} `yaml:"kafka"`
}

// NewPublisher creates the appropriate Publisher based on config.
func NewPublisher(cfg PublisherConfig, redisClient *redis.Client) Publisher {
	if !cfg.Enabled {
		return &noopPublisher{}
	}

	switch cfg.Type {
	case "kafka":
		return NewKafkaPublisher(cfg.Kafka.Brokers, cfg.Topic)
	default: // "redis"
		return NewRedisPublisher(redisClient, cfg.Topic)
	}
}

// noopPublisher is a no-op implementation for when events are disabled.
type noopPublisher struct{}

func (n *noopPublisher) Publish(_ context.Context, _ *OpsEvent) error { return nil }
func (n *noopPublisher) Close() error                              { return nil }
