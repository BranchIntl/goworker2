package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/cihub/seelog"
)

// Engine is the main orchestration engine
type Engine struct {
	broker     interfaces.Broker
	stats      interfaces.Statistics
	registry   interfaces.Registry
	serializer interfaces.Serializer
	config     *Config

	poller     *Poller
	workerPool *WorkerPool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger seelog.LoggerInterface
}

// NewEngine creates a new engine with dependency injection
func NewEngine(
	broker interfaces.Broker,
	stats interfaces.Statistics,
	registry interfaces.Registry,
	serializer interfaces.Serializer,
	options ...EngineOption,
) *Engine {
	config := defaultConfig()
	for _, opt := range options {
		opt(config)
	}

	logger, _ := seelog.LoggerFromWriterWithMinLevel(config.LogOutput, config.LogLevel)

	return &Engine{
		broker:     broker,
		stats:      stats,
		registry:   registry,
		serializer: serializer,
		config:     config,
		logger:     logger,
	}
}

// Start begins processing jobs
func (e *Engine) Start(ctx context.Context) error {
	e.ctx, e.cancel = context.WithCancel(ctx)

	// Connect broker and statistics
	if err := e.broker.Connect(e.ctx); err != nil {
		return fmt.Errorf("failed to connect broker: %w", err)
	}

	if err := e.stats.Connect(e.ctx); err != nil {
		return fmt.Errorf("failed to connect statistics: %w", err)
	}

	// Create job channel
	jobChan := make(chan interfaces.Job, e.config.JobBufferSize)

	// Create and start poller
	e.poller = NewPoller(
		e.broker,
		e.stats,
		e.config.Queues,
		e.config.PollInterval,
		jobChan,
		e.logger,
	)

	// Create and start worker pool
	e.workerPool = NewWorkerPool(
		e.registry,
		e.stats,
		e.serializer,
		e.config.Concurrency,
		jobChan,
		e.logger,
		e.broker,
	)

	// Start components
	e.wg.Add(2)
	go func() {
		defer e.wg.Done()
		if err := e.poller.Start(e.ctx); err != nil {
			e.logger.Errorf("Poller error: %v", err)
		}
	}()

	go func() {
		defer e.wg.Done()
		if err := e.workerPool.Start(e.ctx); err != nil {
			e.logger.Errorf("Worker pool error: %v", err)
		}
	}()

	e.logger.Info("Engine started")
	return nil
}

// Stop gracefully shuts down the engine
func (e *Engine) Stop() error {
	if e.cancel != nil {
		e.cancel()
	}

	// Wait for graceful shutdown
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.logger.Info("Engine stopped gracefully")
	case <-time.After(e.config.ShutdownTimeout):
		e.logger.Warn("Engine shutdown timeout exceeded")
	}

	// Close connections
	if err := e.broker.Close(); err != nil {
		e.logger.Errorf("Error closing broker: %v", err)
	}

	if err := e.stats.Close(); err != nil {
		e.logger.Errorf("Error closing statistics: %v", err)
	}

	return nil
}

// Health returns the current health status
func (e *Engine) Health() interfaces.HealthStatus {
	queuedJobs := make(map[string]int64)
	for _, queue := range e.config.Queues {
		if length, err := e.broker.QueueLength(e.ctx, queue); err == nil {
			queuedJobs[queue] = length
		}
	}

	brokerHealth := e.broker.Health()
	statsHealth := e.stats.Health()

	return interfaces.HealthStatus{
		Healthy:       brokerHealth == nil && statsHealth == nil,
		BrokerHealth:  brokerHealth,
		StatsHealth:   statsHealth,
		ActiveWorkers: e.workerPool.ActiveWorkers(),
		QueuedJobs:    queuedJobs,
		LastCheck:     time.Now(),
	}
}

// Enqueue adds a job to the queue
func (e *Engine) Enqueue(job interfaces.Job) error {
	return e.broker.Enqueue(e.ctx, job)
}

// Register adds a worker function
func (e *Engine) Register(class string, worker interfaces.WorkerFunc) error {
	return e.registry.Register(class, worker)
}
