package interfaces

import (
	"context"
	"time"
)

// Worker interface for individual workers
type Worker interface {
	// GetID returns the worker's unique ID
	GetID() string

	// GetQueues returns the queues this worker processes
	GetQueues() []string

	// Work starts processing jobs
	Work(ctx context.Context, jobs <-chan Job) error

	// GetStats returns current worker statistics
	GetStats() WorkerStats
}

// WorkerProcess represents a worker process
type WorkerProcess struct {
	Hostname string
	Pid      int
	ID       string
	Queues   []string
}

// Work represents a job in progress
type Work struct {
	Queue   string    `json:"queue"`
	RunAt   time.Time `json:"run_at"`
	Payload Payload   `json:"payload"`
}
