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
	// Queue operations
	Enqueue(ctx context.Context, job job.Job) error
	Dequeue(ctx context.Context, queue string) (job.Job, error)

	// Job lifecycle
	Ack(ctx context.Context, job job.Job) error
	Nack(ctx context.Context, job job.Job, requeue bool) error

	// Queue introspection
	QueueLength(ctx context.Context, name string) (int64, error)

	// Connection management
	Connect(ctx context.Context) error
	Close() error
	Health() error
}

// Statistics interface defines what core needs from a statistics backend
type Statistics interface {
	// Worker lifecycle
	RegisterWorker(ctx context.Context, worker WorkerInfo) error
	UnregisterWorker(ctx context.Context, workerID string) error

	// Job metrics
	RecordJobStarted(ctx context.Context, job job.Job, worker WorkerInfo) error
	RecordJobCompleted(ctx context.Context, job job.Job, worker WorkerInfo, duration time.Duration) error
	RecordJobFailed(ctx context.Context, job job.Job, worker WorkerInfo, err error, duration time.Duration) error

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

// Registry interface defines what core needs from a worker registry
type Registry interface {
	// Register adds a worker function for a class
	Register(class string, worker WorkerFunc) error

	// Get retrieves a worker function by class
	Get(class string) (WorkerFunc, bool)
}

// Poller interface defines what core needs from a job poller/consumer
type Poller interface {
	// Start begins consuming jobs and sending them to the job channel
	Start(ctx context.Context, jobChan chan<- job.Job) error
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
