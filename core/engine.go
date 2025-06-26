package core

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
	"github.com/cihub/seelog"
)

// Engine is the main orchestration engine
type Engine struct {
	broker     Broker
	stats      Statistics
	registry   Registry
	serializer Serializer
	config     *Config

	workerPool *WorkerPool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger seelog.LoggerInterface
}

// NewEngine creates a new engine with dependency injection
func NewEngine(
	broker Broker,
	stats Statistics,
	registry Registry,
	serializer Serializer,
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

	// Set logger on broker
	e.broker.SetLogger(e.logger)

	// If broker implements SetConsumerQueues (for push-based consumption), set the queues
	if setQueues, ok := e.broker.(interface{ SetConsumerQueues([]string) }); ok {
		setQueues.SetConsumerQueues(e.config.Queues)
	}

	// Connect broker and statistics
	if err := e.broker.Connect(e.ctx); err != nil {
		return errors.NewConnectionError("",
			fmt.Errorf("failed to connect broker: %w", err))
	}

	if err := e.stats.Connect(e.ctx); err != nil {
		return errors.NewConnectionError("",
			fmt.Errorf("failed to connect statistics: %w", err))
	}

	// Create job channel
	jobChan := make(chan job.Job, e.config.JobBufferSize)

	// Check if broker implements Poller interface
	var poller Poller
	if brokerPoller, ok := e.broker.(Poller); ok {
		// Broker can poll/consume directly
		poller = brokerPoller
	} else {
		// Use StandardPoller wrapper for pull-based brokers
		poller = NewStandardPoller(
			e.broker,
			e.stats,
			e.config.Queues,
			e.config.PollInterval,
			e.logger,
		)
	}

	// Create and start worker pool
	e.workerPool = NewWorkerPool(
		e.registry,
		e.stats,
		e.serializer,
		e.config.Concurrency,
		e.config.Queues,
		jobChan,
		e.logger,
		e.broker,
	)

	// Start components
	e.wg.Add(2)
	go func() {
		defer e.wg.Done()
		if err := poller.Start(e.ctx, jobChan); err != nil {
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
func (e *Engine) Health() HealthStatus {
	queuedJobs := make(map[string]int64)
	for _, queue := range e.config.Queues {
		if length, err := e.broker.QueueLength(e.ctx, queue); err == nil {
			queuedJobs[queue] = length
		}
	}

	brokerHealth := e.broker.Health()
	statsHealth := e.stats.Health()

	return HealthStatus{
		Healthy:       brokerHealth == nil && statsHealth == nil,
		BrokerHealth:  brokerHealth,
		StatsHealth:   statsHealth,
		ActiveWorkers: e.workerPool.ActiveWorkers(),
		QueuedJobs:    queuedJobs,
		LastCheck:     time.Now(),
	}
}

// Enqueue adds a job to the queue
func (e *Engine) Enqueue(job job.Job) error {
	return e.broker.Enqueue(e.ctx, job)
}

// Register adds a worker function
func (e *Engine) Register(class string, worker WorkerFunc) error {
	return e.registry.Register(class, worker)
}

// Run starts the engine and blocks until shutdown signals are received
// This is a convenience method that combines Start() + signal handling + Stop()
func (e *Engine) Run(ctx context.Context) error {
	// Start the engine
	if err := e.Start(ctx); err != nil {
		return err
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for either context cancellation or signal
	select {
	case <-ctx.Done():
		e.logger.Info("Context cancelled, shutting down...")
	case sig := <-sigChan:
		e.logger.Infof("Received signal %v, shutting down...", sig)
	}

	// Graceful shutdown
	return e.Stop()
}
