package core

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
)

// Engine is the main orchestration engine
type Engine struct {
	broker   Broker
	stats    Statistics
	registry Registry
	config   *Config

	workerPool *WorkerPool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewEngine creates a new engine with dependency injection
func NewEngine(
	broker Broker,
	stats Statistics,
	registry Registry,
	options ...EngineOption,
) *Engine {
	config := defaultConfig()
	for _, opt := range options {
		opt(config)
	}

	return &Engine{
		broker:   broker,
		stats:    stats,
		registry: registry,
		config:   config,
	}
}

// Start begins processing jobs
func (e *Engine) Start(ctx context.Context) error {
	e.ctx, e.cancel = context.WithCancel(ctx)

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

	// Create and start worker pool
	e.workerPool = NewWorkerPool(
		e.registry,
		e.stats,
		e.config.Concurrency,
		jobChan,
		e.broker,
	)

	// Start components
	e.wg.Add(2)
	go func() {
		defer e.wg.Done()
		if err := e.broker.Start(e.ctx, jobChan); err != nil {
			slog.Error("Broker error", "error", err)
		}
	}()

	go func() {
		defer e.wg.Done()
		if err := e.workerPool.Start(e.ctx); err != nil {
			slog.Error("Worker pool error", "error", err)
		}
	}()

	slog.Info("Engine started")
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
		slog.Info("Engine stopped gracefully")
	case <-time.After(e.config.ShutdownTimeout):
		slog.Warn("Engine shutdown timeout exceeded")
	}

	// Close connections
	if err := e.broker.Close(); err != nil {
		slog.Error("Error closing broker", "error", err)
	}

	if err := e.stats.Close(); err != nil {
		slog.Error("Error closing statistics", "error", err)
	}

	return nil
}

// Health returns the current health status
func (e *Engine) Health() HealthStatus {
	queuedJobs := make(map[string]int64)
	for _, queue := range e.broker.Queues() {
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
		slog.Info("Context cancelled, shutting down...")
	case sig := <-sigChan:
		slog.Info("Received signal, shutting down...", "signal", sig)
	}

	// Graceful shutdown
	return e.Stop()
}
