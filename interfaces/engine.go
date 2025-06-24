package interfaces

import (
	"context"
	"time"
)

// Engine interface for the main orchestration engine
type Engine interface {
	// Start begins processing jobs
	Start(ctx context.Context) error

	// Run starts the engine and blocks until shutdown signals are received
	// This is a convenience method that combines Start() + signal handling + Stop()
	Run(ctx context.Context) error

	// Stop gracefully shuts down the engine
	Stop() error

	// Health returns the current health status
	Health() HealthStatus

	// Enqueue adds a job to the queue
	Enqueue(job Job) error

	// Register adds a worker function
	Register(class string, worker WorkerFunc) error
}

// HealthStatus represents the health of the engine
type HealthStatus struct {
	Healthy       bool
	BrokerHealth  error
	StatsHealth   error
	ActiveWorkers int
	QueuedJobs    map[string]int64
	LastCheck     time.Time
}
