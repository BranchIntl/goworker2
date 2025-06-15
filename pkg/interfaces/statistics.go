package interfaces

import (
	"context"
	"time"
)

// Statistics interface for metrics and monitoring
type Statistics interface {
	// Worker lifecycle
	RegisterWorker(ctx context.Context, worker WorkerInfo) error
	UnregisterWorker(ctx context.Context, workerID string) error

	// Job metrics
	RecordJobStarted(ctx context.Context, job JobInfo) error
	RecordJobCompleted(ctx context.Context, job JobInfo, duration time.Duration) error
	RecordJobFailed(ctx context.Context, job JobInfo, err error, duration time.Duration) error

	// Statistics queries
	GetWorkerStats(ctx context.Context, workerID string) (WorkerStats, error)
	GetQueueStats(ctx context.Context, queue string) (QueueStats, error)
	GetGlobalStats(ctx context.Context) (GlobalStats, error)

	// Health and connection
	Connect(ctx context.Context) error
	Close() error
	Health() error
	Type() string
}

// WorkerInfo describes a worker
type WorkerInfo struct {
	ID       string
	Hostname string
	Pid      int
	Queues   []string
	Started  time.Time
}

// JobInfo describes a job for statistics
type JobInfo struct {
	ID       string
	Queue    string
	Class    string
	WorkerID string
}

// WorkerStats contains statistics for a worker
type WorkerStats struct {
	ID         string
	Processed  int64
	Failed     int64
	InProgress int64
	StartTime  time.Time
	LastJob    time.Time
}

// QueueStats contains statistics for a queue
type QueueStats struct {
	Name      string
	Length    int64
	Processed int64
	Failed    int64
	Workers   int64
}

// GlobalStats contains global statistics
type GlobalStats struct {
	TotalProcessed int64
	TotalFailed    int64
	ActiveWorkers  int64
	QueueStats     map[string]QueueStats
}
