// Package engines provides pre-configured engine setups for common background
// job processing scenarios. These engines combine compatible broker, serializer,
// and statistics components with sensible defaults.
//
// The engines package offers two main configurations:
//
//   - ResqueEngine: Redis-based with Resque compatibility
//   - SneakersEngine: RabbitMQ-based with ActiveJob/Sneakers compatibility
//
// Example usage:
//
//	// Resque-compatible engine
//	engine := engines.NewResqueEngine(engines.DefaultResqueOptions())
//	engine.Register("EmailJob", emailHandler)
//	engine.Run(ctx)
//
//	// ActiveJob-compatible engine
//	engine := engines.NewSneakersEngine(engines.DefaultSneakersOptions())
//	engine.Register("MailJob", mailHandler)
//	engine.Run(ctx)
package engines

import (
	"context"

	"github.com/benmanns/goworker/brokers/redis"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/interfaces"
	"github.com/benmanns/goworker/registry"
	"github.com/benmanns/goworker/serializers/resque"
	resqueStats "github.com/benmanns/goworker/statistics/resque"
)

// ResqueEngine provides a pre-configured engine for Resque compatibility.
// It uses Redis for queuing, Resque JSON serialization, and Redis-based statistics.
//
// This engine is designed to be compatible with Ruby Resque workers and can
// process jobs enqueued by Resque clients.
type ResqueEngine struct {
	*core.Engine
	broker     *redis.RedisBroker
	stats      *resqueStats.ResqueStatistics
	registry   *registry.Registry
	serializer *resque.JSONSerializer
}

// ResqueOptions contains configuration for ResqueEngine.
type ResqueOptions struct {
	// RedisURI is the Redis connection URI (default: redis://localhost:6379/)
	RedisURI string

	// RedisOptions provides detailed Redis configuration
	RedisOptions redis.Options

	// EngineOptions contains core engine configuration options
	EngineOptions []core.EngineOption
}

// DefaultResqueOptions returns default configuration for ResqueEngine.
// The defaults provide a working configuration for local development.
func DefaultResqueOptions() ResqueOptions {
	return ResqueOptions{
		RedisURI:     "redis://localhost:6379/",
		RedisOptions: redis.DefaultOptions(),
		EngineOptions: []core.EngineOption{
			core.WithConcurrency(25),
			core.WithQueues([]string{"default"}),
		},
	}
}

// NewResqueEngine creates a new pre-configured Resque-compatible engine.
// The engine automatically configures Redis broker, Resque serializer,
// and Redis-based statistics with compatible settings.
func NewResqueEngine(options ResqueOptions) *ResqueEngine {
	// Create serializer
	serializer := resque.NewSerializer()

	// Create Redis broker with custom options
	brokerOpts := options.RedisOptions
	if options.RedisURI != "" {
		brokerOpts.URI = options.RedisURI
	}
	broker := redis.NewBroker(brokerOpts, serializer)

	// Create Resque statistics with same Redis config
	statsOpts := resqueStats.DefaultOptions()
	statsOpts.URI = brokerOpts.URI
	statsOpts.Namespace = brokerOpts.Namespace
	stats := resqueStats.NewStatistics(statsOpts)

	// Create registry
	registry := registry.NewRegistry()

	// Create core engine
	engine := core.NewEngine(
		broker,
		stats,
		registry,
		serializer,
		options.EngineOptions...,
	)

	return &ResqueEngine{
		Engine:     engine,
		broker:     broker,
		stats:      stats,
		registry:   registry,
		serializer: serializer,
	}
}

// Register adds a worker function for the given job class.
// The class name should match the job class used when enqueuing jobs.
func (e *ResqueEngine) Register(class string, worker interfaces.WorkerFunc) error {
	return e.registry.Register(class, worker)
}

// GetRegistry returns the worker registry for advanced usage.
func (e *ResqueEngine) GetRegistry() interfaces.Registry {
	return e.registry
}

// GetBroker returns the Redis broker for advanced usage.
func (e *ResqueEngine) GetBroker() interfaces.Broker {
	return e.broker
}

// GetStats returns the Resque statistics backend for monitoring.
func (e *ResqueEngine) GetStats() interfaces.Statistics {
	return e.stats
}

// GetSerializer returns the Resque JSON serializer.
func (e *ResqueEngine) GetSerializer() interfaces.Serializer {
	return e.serializer
}

// MustStart starts the engine and panics on error.
// This is a convenience method for applications that want to fail fast.
func (e *ResqueEngine) MustStart(ctx context.Context) {
	if err := e.Start(ctx); err != nil {
		panic(err)
	}
}

// MustRun runs the engine with signal handling and panics on error.
// This is a convenience method for applications that want to fail fast.
func (e *ResqueEngine) MustRun(ctx context.Context) {
	if err := e.Run(ctx); err != nil {
		panic(err)
	}
}
