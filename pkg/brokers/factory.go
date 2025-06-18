package brokers

import (
	"fmt"

	"github.com/benmanns/goworker/pkg/brokers/rabbitmq"
	"github.com/benmanns/goworker/pkg/brokers/redis"
	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/benmanns/goworker/pkg/serializers/sneakers"
	"github.com/benmanns/goworker/pkg/serializers/resque"
)

// BrokerType represents the type of broker
type BrokerType string

const (
	// Redis broker type
	Redis BrokerType = "redis"
	// RabbitMQ broker type
	RabbitMQ BrokerType = "rabbitmq"
	// SQS broker type (not implemented yet)
	SQS BrokerType = "sqs"
	// Kafka broker type (not implemented yet)
	Kafka BrokerType = "kafka"
	// Memory broker type (not implemented yet)
	Memory BrokerType = "memory"
)

// Config is a generic broker configuration
type Config struct {
	Type       BrokerType
	URI        string
	Namespace  string
	Options    map[string]interface{}
	Serializer interfaces.Serializer
}

// NewBroker creates a broker based on the configuration
func NewBroker(config Config) (interfaces.Broker, error) {
	// Default serializer based on broker type if not provided
	if config.Serializer == nil {
		switch config.Type {
		case Redis:
			config.Serializer = resque.NewSerializer()
		case RabbitMQ:
			config.Serializer = sneakers.NewSerializer()
		default:
			config.Serializer = json.NewSerializer()
		}
	}

	switch config.Type {
	case Redis:
		opts := redis.DefaultOptions()
		opts.URI = config.URI
		opts.Namespace = config.Namespace

		// Apply custom options
		if maxConn, ok := config.Options["maxConnections"].(int); ok {
			opts.MaxConnections = maxConn
		}
		if useTLS, ok := config.Options["useTLS"].(bool); ok {
			opts.UseTLS = useTLS
		}

		return redis.NewBroker(opts, config.Serializer), nil

	case RabbitMQ:
		opts := rabbitmq.DefaultOptions()
		opts.URI = config.URI

		// Apply custom options
		if prefetch, ok := config.Options["prefetchCount"].(int); ok {
			opts.PrefetchCount = prefetch
		}
		if exchange, ok := config.Options["exchange"].(string); ok {
			opts.Exchange = exchange
		}
		if exchangeType, ok := config.Options["exchangeType"].(string); ok {
			opts.ExchangeType = exchangeType
		}

		return rabbitmq.NewBroker(opts, config.Serializer), nil

	case SQS:
		return nil, fmt.Errorf("SQS broker not implemented yet")
	case Kafka:
		return nil, fmt.Errorf("Kafka broker not implemented yet")
	case Memory:
		return nil, fmt.Errorf("Memory broker not implemented yet")
	default:
		return nil, fmt.Errorf("unknown broker type: %s", config.Type)
	}
}
