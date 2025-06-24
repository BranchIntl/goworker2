package engines

import (
	"context"

	"github.com/benmanns/goworker/brokers/rabbitmq"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/interfaces"
	"github.com/benmanns/goworker/registry"
	"github.com/benmanns/goworker/serializers/sneakers"
	"github.com/benmanns/goworker/statistics/noop"
)

// SneakersEngine provides a pre-configured engine for Sneakers/ActiveJob compatibility.
// It uses RabbitMQ for queuing, ActiveJob JSON serialization, and NoOp statistics by default.
//
// This engine is designed to be compatible with Rails ActiveJob and Sneakers workers,
// and can process jobs enqueued by ActiveJob clients.
type SneakersEngine struct {
	*core.Engine
	broker     *rabbitmq.RabbitMQBroker
	stats      *noop.NoOpStatistics
	registry   *registry.Registry
	serializer *sneakers.JSONSerializer
}

// SneakersOptions contains configuration for SneakersEngine.
type SneakersOptions struct {
	// RabbitMQURI is the RabbitMQ connection URI (default: amqp://guest:guest@localhost:5672/)
	RabbitMQURI string

	// RabbitMQOptions provides detailed RabbitMQ configuration
	RabbitMQOptions rabbitmq.Options

	// Statistics backend (optional, defaults to NoOp for performance)
	Statistics interfaces.Statistics

	// EngineOptions contains core engine configuration options
	EngineOptions []core.EngineOption
}

// DefaultSneakersOptions returns default configuration for SneakersEngine.
// The defaults provide a working configuration for local development with RabbitMQ.
func DefaultSneakersOptions() SneakersOptions {
	return SneakersOptions{
		RabbitMQURI:     "amqp://guest:guest@localhost:5672/",
		RabbitMQOptions: rabbitmq.DefaultOptions(),
		Statistics:      nil, // Will default to noop
		EngineOptions: []core.EngineOption{
			core.WithConcurrency(25),
			core.WithQueues([]string{"default"}),
		},
	}
}

// NewSneakersEngine creates a new pre-configured Sneakers-compatible engine.
// The engine automatically configures RabbitMQ broker, ActiveJob serializer,
// and statistics backend (NoOp by default for better performance).
func NewSneakersEngine(options SneakersOptions) *SneakersEngine {
	// Create serializer
	serializer := sneakers.NewSerializer()

	// Create RabbitMQ broker with custom options
	brokerOpts := options.RabbitMQOptions
	if options.RabbitMQURI != "" {
		brokerOpts.URI = options.RabbitMQURI
	}
	broker := rabbitmq.NewBroker(brokerOpts, serializer)

	// Create statistics backend (default to noop)
	var stats interfaces.Statistics
	if options.Statistics != nil {
		stats = options.Statistics
	} else {
		stats = noop.NewStatistics()
	}

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

	// Type assertion for concrete types (for getter methods)
	var noopStats *noop.NoOpStatistics
	if ns, ok := stats.(*noop.NoOpStatistics); ok {
		noopStats = ns
	}

	return &SneakersEngine{
		Engine:     engine,
		broker:     broker,
		stats:      noopStats,
		registry:   registry,
		serializer: serializer,
	}
}

// Register adds a worker function for the given job class.
// The class name should match the job class used when enqueuing ActiveJob jobs.
func (e *SneakersEngine) Register(class string, worker interfaces.WorkerFunc) error {
	return e.registry.Register(class, worker)
}

// GetRegistry returns the worker registry for advanced usage.
func (e *SneakersEngine) GetRegistry() interfaces.Registry {
	return e.registry
}

// GetBroker returns the RabbitMQ broker for advanced usage.
func (e *SneakersEngine) GetBroker() interfaces.Broker {
	return e.broker
}

// GetStats returns the statistics backend (NoOp by default).
// If you provided a custom statistics backend in options, it will be returned.
func (e *SneakersEngine) GetStats() interfaces.Statistics {
	if e.stats != nil {
		return e.stats
	}
	// Return noop stats as fallback
	return noop.NewStatistics()
}

// GetSerializer returns the ActiveJob/Sneakers JSON serializer.
func (e *SneakersEngine) GetSerializer() interfaces.Serializer {
	return e.serializer
}

// MustStart starts the engine and panics on error.
// This is a convenience method for applications that want to fail fast.
func (e *SneakersEngine) MustStart(ctx context.Context) {
	if err := e.Start(ctx); err != nil {
		panic(err)
	}
}

// MustRun runs the engine with signal handling and panics on error.
// This is a convenience method for applications that want to fail fast.
func (e *SneakersEngine) MustRun(ctx context.Context) {
	if err := e.Run(ctx); err != nil {
		panic(err)
	}
}
