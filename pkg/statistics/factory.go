package statistics

import (
	"fmt"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/benmanns/goworker/pkg/statistics/noop"
	"github.com/benmanns/goworker/pkg/statistics/redis"
	"github.com/benmanns/goworker/pkg/statistics/rabbitmq"
)

// StatsType represents the type of statistics backend
type StatsType string

const (
	// Redis statistics type
	Redis StatsType = "redis"
	// RMQ (RabbitMQ) statistics type
	RabbitMQ StatsType = "rabbitmq"
	// Prometheus statistics type (not implemented yet)
	Prometheus StatsType = "prometheus"
	// CloudWatch statistics type (not implemented yet)
	CloudWatch StatsType = "cloudwatch"
	// Database statistics type (not implemented yet)
	Database StatsType = "database"
	// NoOp statistics type
	NoOp StatsType = "noop"
)

// Config is a generic statistics configuration
type Config struct {
	Type      StatsType
	URI       string
	Namespace string
	Options   map[string]interface{}
}

// NewStatistics creates a statistics backend based on the configuration
func NewStatistics(config Config) (interfaces.Statistics, error) {
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
		if timeout, ok := config.Options["connectTimeout"].(time.Duration); ok {
			opts.ConnectTimeout = timeout
		}

		return redis.NewStatistics(opts), nil

	case RabbitMQ:
		opts := rabbitmq.DefaultOptions()
		opts.URI = config.URI
		opts.Namespace = config.Namespace

		// Apply custom options
		if maxConn, ok := config.Options["maxConnections"].(int); ok {
			opts.MaxConnections = maxConn
		}
		if maxChan, ok := config.Options["maxChannels"].(int); ok {
			opts.MaxChannels = maxChan
		}
		if useTLS, ok := config.Options["useTLS"].(bool); ok {
			opts.UseTLS = useTLS
		}
		if timeout, ok := config.Options["connectTimeout"].(time.Duration); ok {
			opts.ConnectTimeout = timeout
		}
		if heartbeat, ok := config.Options["heartbeat"].(time.Duration); ok {
			opts.Heartbeat = heartbeat
		}
		if persistInterval, ok := config.Options["statsPersistInterval"].(time.Duration); ok {
			opts.StatsPersistInterval = persistInterval
		}
		if durable, ok := config.Options["exchangeDurable"].(bool); ok {
			opts.ExchangeDurable = durable
		}
		if durable, ok := config.Options["queueDurable"].(bool); ok {
			opts.QueueDurable = durable
		}
		if prefetch, ok := config.Options["prefetchCount"].(int); ok {
			opts.PrefetchCount = prefetch
		}

		return rabbitmq.NewStatistics(opts), nil

	case NoOp:
		return noop.NewStatistics(), nil

	case Prometheus:
		return nil, fmt.Errorf("Prometheus statistics not implemented yet")

	case CloudWatch:
		return nil, fmt.Errorf("CloudWatch statistics not implemented yet")

	case Database:
		return nil, fmt.Errorf("Database statistics not implemented yet")

	default:
		return nil, fmt.Errorf("unknown statistics type: %s", config.Type)
	}
}