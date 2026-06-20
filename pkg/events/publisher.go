package events

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// EventsConfig defines the toggle switches for each event type.
type EventsConfig struct {
	CircuitBreak   *bool `yaml:"circuit_break" mapstructure:"circuit_break"`
	RateLimit      *bool `yaml:"rate_limit" mapstructure:"rate_limit"`
	InvocationFail *bool `yaml:"invocation_fail" mapstructure:"invocation_fail"`
	LBSwitch       *bool `yaml:"lb_switch" mapstructure:"lb_switch"`
}

// PublisherConfig holds event publisher configuration.
type PublisherConfig struct {
	Enabled bool         `yaml:"enabled"`
	Type    string       `yaml:"type"`  // redis | kafka
	Topic   string       `yaml:"topic"` // stream key (Redis) or topic (Kafka)
	Kafka   struct {
		Brokers []string `yaml:"brokers"`
	} `yaml:"kafka"`
	Types   EventsConfig `yaml:"types" mapstructure:"types"`
}

// NewPublisher creates the appropriate Publisher based on config.
func NewPublisher(cfg PublisherConfig, redisClient *redis.Client) Publisher {
	if !cfg.Enabled {
		return &noopPublisher{}
	}

	var delegate Publisher
	switch cfg.Type {
	case "kafka":
		delegate = NewKafkaPublisher(cfg.Kafka.Brokers, cfg.Topic)
	default: // "redis"
		delegate = NewRedisPublisher(redisClient, cfg.Topic)
	}

	// 强制包装成 AsyncPublisher，保证高并发时绝不阻塞主流程，并在积压时安全丢弃
	asyncPub := NewAsyncPublisher(delegate, 1024)
	asyncPub.SetEventsConfig(cfg.Types)
	return asyncPub
}

// noopPublisher is a no-op implementation for when events are disabled.
type noopPublisher struct{}

func (n *noopPublisher) Publish(_ context.Context, _ *OpsEvent) error { return nil }
func (n *noopPublisher) Close() error                              { return nil }
