package engines

import (
	"context"
	"fmt"

	"github.com/benmanns/goworker/brokers/rabbitmq"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/registry"
	"github.com/benmanns/goworker/serializers/sneakers"
	"github.com/benmanns/goworker/statistics/noop"
)

// SneakersOptions holds configuration for the Sneakers-compatible engine
type SneakersOptions struct {
	RabbitMQURI     string
	RabbitMQOptions rabbitmq.Options
	Statistics      core.Statistics
	EngineOptions   []core.EngineOption
}

// DefaultSneakersOptions returns default options for Sneakers engine
func DefaultSneakersOptions() SneakersOptions {
	return SneakersOptions{
		RabbitMQURI:     "amqp://guest:guest@localhost:5672/",
		RabbitMQOptions: rabbitmq.DefaultOptions(),
		Statistics:      noop.NewStatistics(),
		EngineOptions:   []core.EngineOption{},
	}
}

// SneakersEngine provides a pre-configured engine for Sneakers/ActiveJob compatibility
type SneakersEngine struct {
	engine     *core.Engine
	broker     *rabbitmq.RabbitMQBroker
	stats      core.Statistics
	registry   *registry.Registry
	serializer *sneakers.JSONSerializer
}

// NewSneakersEngine creates a new Sneakers-compatible engine
func NewSneakersEngine(options SneakersOptions) *SneakersEngine {
	// Override URI if provided
	if options.RabbitMQURI != "" {
		options.RabbitMQOptions.URI = options.RabbitMQURI
	}

	// Create components
	serializer := sneakers.NewSerializer()
	broker := rabbitmq.NewBroker(options.RabbitMQOptions, serializer)

	stats := options.Statistics
	if stats == nil {
		stats = noop.NewStatistics()
	}

	registry := registry.NewRegistry()

	// Create engine
	engine := core.NewEngine(
		broker,
		stats,
		registry,
		serializer,
		options.EngineOptions...,
	)

	return &SneakersEngine{
		engine:     engine,
		broker:     broker,
		stats:      stats,
		registry:   registry,
		serializer: serializer,
	}
}

// Register adds a worker function for a job class
func (e *SneakersEngine) Register(class string, worker core.WorkerFunc) error {
	return e.registry.Register(class, worker)
}

// Run starts the engine and blocks until shutdown
func (e *SneakersEngine) Run(ctx context.Context) error {
	return e.engine.Run(ctx)
}

// Start begins processing jobs
func (e *SneakersEngine) Start(ctx context.Context) error {
	return e.engine.Start(ctx)
}

// Stop gracefully shuts down the engine
func (e *SneakersEngine) Stop() error {
	return e.engine.Stop()
}

// MustRun starts the engine and panics on error
func (e *SneakersEngine) MustRun(ctx context.Context) {
	if err := e.Run(ctx); err != nil {
		panic(fmt.Sprintf("SneakersEngine.Run failed: %v", err))
	}
}

// MustStart begins processing and panics on error
func (e *SneakersEngine) MustStart(ctx context.Context) {
	if err := e.Start(ctx); err != nil {
		panic(fmt.Sprintf("SneakersEngine.Start failed: %v", err))
	}
}

// Health returns the engine health status
func (e *SneakersEngine) Health() core.HealthStatus {
	return e.engine.Health()
}

// Component accessors

// GetBroker returns the RabbitMQ broker
func (e *SneakersEngine) GetBroker() *rabbitmq.RabbitMQBroker {
	return e.broker
}

// GetStats returns the statistics backend
func (e *SneakersEngine) GetStats() core.Statistics {
	return e.stats
}

// GetRegistry returns the worker registry
func (e *SneakersEngine) GetRegistry() *registry.Registry {
	return e.registry
}

// GetSerializer returns the Sneakers serializer
func (e *SneakersEngine) GetSerializer() *sneakers.JSONSerializer {
	return e.serializer
}
