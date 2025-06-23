package noop

import (
	"context"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
)

// NoOpStatistics implements the Statistics interface with no-op operations
type NoOpStatistics struct{}

// NewStatistics creates a new no-op statistics backend
func NewStatistics() *NoOpStatistics {
	return &NoOpStatistics{}
}

// Connect establishes connection (no-op)
func (n *NoOpStatistics) Connect(ctx context.Context) error {
	return nil
}

// Close closes the connection (no-op)
func (n *NoOpStatistics) Close() error {
	return nil
}

// Health checks connection health
func (n *NoOpStatistics) Health() error {
	return nil
}

// Type returns the statistics backend type
func (n *NoOpStatistics) Type() string {
	return "noop"
}

// RegisterWorker registers a worker (no-op)
func (n *NoOpStatistics) RegisterWorker(ctx context.Context, worker interfaces.WorkerInfo) error {
	return nil
}

// UnregisterWorker removes a worker (no-op)
func (n *NoOpStatistics) UnregisterWorker(ctx context.Context, workerID string) error {
	return nil
}

// RecordJobStarted records that a job has started (no-op)
func (n *NoOpStatistics) RecordJobStarted(ctx context.Context, job interfaces.JobInfo) error {
	return nil
}

// RecordJobCompleted records successful job completion (no-op)
func (n *NoOpStatistics) RecordJobCompleted(ctx context.Context, job interfaces.JobInfo, duration time.Duration) error {
	return nil
}

// RecordJobFailed records job failure (no-op)
func (n *NoOpStatistics) RecordJobFailed(ctx context.Context, job interfaces.JobInfo, err error, duration time.Duration) error {
	return nil
}

// GetWorkerStats returns empty statistics
func (n *NoOpStatistics) GetWorkerStats(ctx context.Context, workerID string) (interfaces.WorkerStats, error) {
	return interfaces.WorkerStats{
		ID: workerID,
	}, nil
}

// GetQueueStats returns empty statistics
func (n *NoOpStatistics) GetQueueStats(ctx context.Context, queue string) (interfaces.QueueStats, error) {
	return interfaces.QueueStats{
		Name: queue,
	}, nil
}

// GetGlobalStats returns empty statistics
func (n *NoOpStatistics) GetGlobalStats(ctx context.Context) (interfaces.GlobalStats, error) {
	return interfaces.GlobalStats{
		QueueStats: make(map[string]interfaces.QueueStats),
	}, nil
}
