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
	"fmt"

	"github.com/benmanns/goworker/brokers/redis"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/registry"
	"github.com/benmanns/goworker/serializers/resque"
	resqueStats "github.com/benmanns/goworker/statistics/resque"
)

// ResqueOptions holds configuration for the Resque-compatible engine
type ResqueOptions struct {
	RedisURI      string
	RedisOptions  redis.Options
	EngineOptions []core.EngineOption
}

// DefaultResqueOptions returns default options for Resque engine
func DefaultResqueOptions() ResqueOptions {
	return ResqueOptions{
		RedisURI:      "redis://localhost:6379/",
		RedisOptions:  redis.DefaultOptions(),
		EngineOptions: []core.EngineOption{},
	}
}

// ResqueEngine provides a pre-configured engine for Resque compatibility
type ResqueEngine struct {
	engine     *core.Engine
	broker     *redis.RedisBroker
	stats      *resqueStats.ResqueStatistics
	registry   *registry.Registry
	serializer *resque.JSONSerializer
}

// NewResqueEngine creates a new Resque-compatible engine
func NewResqueEngine(options ResqueOptions) *ResqueEngine {
	// Override URI if provided
	if options.RedisURI != "" {
		options.RedisOptions.URI = options.RedisURI
	}

	// Create components
	serializer := resque.NewSerializer()
	broker := redis.NewBroker(options.RedisOptions, serializer)

	statsOptions := resqueStats.DefaultOptions()
	statsOptions.URI = options.RedisOptions.URI
	statsOptions.Namespace = options.RedisOptions.Namespace
	stats := resqueStats.NewStatistics(statsOptions)

	registry := registry.NewRegistry()

	// Create engine
	engine := core.NewEngine(
		broker,
		stats,
		registry,
		serializer,
		options.EngineOptions...,
	)

	return &ResqueEngine{
		engine:     engine,
		broker:     broker,
		stats:      stats,
		registry:   registry,
		serializer: serializer,
	}
}

// Register adds a worker function for a job class
func (e *ResqueEngine) Register(class string, worker core.WorkerFunc) error {
	return e.registry.Register(class, worker)
}

// Run starts the engine and blocks until shutdown
func (e *ResqueEngine) Run(ctx context.Context) error {
	return e.engine.Run(ctx)
}

// Start begins processing jobs
func (e *ResqueEngine) Start(ctx context.Context) error {
	return e.engine.Start(ctx)
}

// Stop gracefully shuts down the engine
func (e *ResqueEngine) Stop() error {
	return e.engine.Stop()
}

// MustRun starts the engine and panics on error
func (e *ResqueEngine) MustRun(ctx context.Context) {
	if err := e.Run(ctx); err != nil {
		panic(fmt.Sprintf("ResqueEngine.Run failed: %v", err))
	}
}

// MustStart begins processing and panics on error
func (e *ResqueEngine) MustStart(ctx context.Context) {
	if err := e.Start(ctx); err != nil {
		panic(fmt.Sprintf("ResqueEngine.Start failed: %v", err))
	}
}

// Health returns the engine health status
func (e *ResqueEngine) Health() core.HealthStatus {
	return e.engine.Health()
}

// Component accessors

// GetBroker returns the Redis broker
func (e *ResqueEngine) GetBroker() *redis.RedisBroker {
	return e.broker
}

// GetStats returns the Resque statistics
func (e *ResqueEngine) GetStats() *resqueStats.ResqueStatistics {
	return e.stats
}

// GetRegistry returns the worker registry
func (e *ResqueEngine) GetRegistry() *registry.Registry {
	return e.registry
}

// GetSerializer returns the Resque serializer
func (e *ResqueEngine) GetSerializer() *resque.JSONSerializer {
	return e.serializer
}
