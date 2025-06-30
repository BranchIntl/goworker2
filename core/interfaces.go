package core

import (
	"context"
	"time"

	"github.com/BranchIntl/goworker2/job"
)

// WorkerFunc is the function signature for workers
type WorkerFunc func(queue string, args ...interface{}) error

// Broker interface defines what core needs from a queue broker
type Broker interface {
	// Start begins consuming jobs and sending them to the job channel
	Start(ctx context.Context, jobChan chan<- job.Job) error

	// Acknowledge a job
	Ack(ctx context.Context, job job.Job) error

	// Negative acknowledge a job (retry or dead letter)
	Nack(ctx context.Context, job job.Job, requeue bool) error

	// Get the list of queues
	Queues() []string

	// Get the length of a queue
	QueueLength(ctx context.Context, name string) (int64, error)

	// Connect to the broker
	Connect(ctx context.Context) error

	// Close the broker
	Close() error

	// Health check the broker
	Health() error

	// Type returns the broker type
	Type() string
}

// Statistics interface defines what core needs from a statistics backend
type Statistics interface {
	// Register a worker
	RegisterWorker(ctx context.Context, worker WorkerInfo) error

	// Unregister a worker
	UnregisterWorker(ctx context.Context, workerID string) error

	// Record a job started
	RecordJobStarted(ctx context.Context, job job.Job, worker WorkerInfo) error

	// Record a job completed
	RecordJobCompleted(ctx context.Context, job job.Job, worker WorkerInfo, duration time.Duration) error

	// Record a job failed
	RecordJobFailed(ctx context.Context, job job.Job, worker WorkerInfo, err error, duration time.Duration) error

	// Get worker statistics
	GetWorkerStats(ctx context.Context, workerID string) (WorkerStats, error)

	// Get queue statistics
	GetQueueStats(ctx context.Context, queue string) (QueueStats, error)

	// Get global statistics
	GetGlobalStats(ctx context.Context) (GlobalStats, error)

	// Connect to the statistics backend
	Connect(ctx context.Context) error

	// Close the statistics backend
	Close() error

	// Health check the statistics backend
	Health() error
	Type() string
}

// Registry interface defines what core needs from a worker registry
type Registry interface {
	// Register adds a worker function for a class
	Register(class string, worker WorkerFunc) error

	// Get retrieves a worker function by class
	Get(class string) (WorkerFunc, bool)
}

// Serializer interface defines what core needs from a serializer
type Serializer interface {
	// Serialize converts a job to bytes
	Serialize(job job.Job) ([]byte, error)

	// Deserialize converts bytes to a job
	Deserialize(data []byte, metadata job.Metadata) (job.Job, error)

	// GetFormat returns the serialization format name
	GetFormat() string

	// UseNumber determines if numbers should be decoded as json.Number
	UseNumber() bool

	// SetUseNumber determines if numbers should be decoded as json.Number
	SetUseNumber(useNumber bool)
}

// Supporting types used by the interfaces

// QueueOptions for queue creation
type QueueOptions struct {
	// MaxRetries before moving to dead letter queue
	MaxRetries int
	// MessageTTL is how long a message can remain in queue
	MessageTTL time.Duration
	// VisibilityTimeout for message processing
	VisibilityTimeout time.Duration
	// DeadLetterQueue name for failed messages
	DeadLetterQueue string
}

// WorkerInfo describes a worker
type WorkerInfo struct {
	ID       string
	Hostname string
	Pid      int
	Queues   []string
	Started  time.Time
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

// HealthStatus represents the health of the engine
type HealthStatus struct {
	Healthy       bool
	BrokerHealth  error
	StatsHealth   error
	ActiveWorkers int
	QueuedJobs    map[string]int64
	LastCheck     time.Time
}
