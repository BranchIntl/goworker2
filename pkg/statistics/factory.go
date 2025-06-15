package statistics

import (
	"fmt"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/benmanns/goworker/pkg/statistics/redis"
)

// StatsType represents the type of statistics backend
type StatsType string

const (
	// Redis statistics type
	Redis StatsType = "redis"
	// Prometheus statistics type (not implemented yet)
	Prometheus StatsType = "prometheus"
	// CloudWatch statistics type (not implemented yet)
	CloudWatch StatsType = "cloudwatch"
	// Database statistics type (not implemented yet)
	Database StatsType = "database"
	// NoOp statistics type (not implemented yet)
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

		return redis.NewStatistics(opts), nil

	case Prometheus:
		return nil, fmt.Errorf("Prometheus statistics not implemented yet")

	case CloudWatch:
		return nil, fmt.Errorf("CloudWatch statistics not implemented yet")

	case Database:
		return nil, fmt.Errorf("Database statistics not implemented yet")

	case NoOp:
		return nil, fmt.Errorf("NoOp statistics not implemented yet")

	default:
		return nil, fmt.Errorf("unknown statistics type: %s", config.Type)
	}
}
