package interfaces

import (
	"context"
	"time"
)

// Broker interface for queue operations
type Broker interface {
	// Queue operations
	Enqueue(ctx context.Context, job Job) error
	Dequeue(ctx context.Context, queue string) (Job, error)

	// Job lifecycle
	Ack(ctx context.Context, job Job) error
	Nack(ctx context.Context, job Job, requeue bool) error

	// Queue management
	CreateQueue(ctx context.Context, name string, options QueueOptions) error
	DeleteQueue(ctx context.Context, name string) error
	QueueExists(ctx context.Context, name string) (bool, error)
	QueueLength(ctx context.Context, name string) (int64, error)

	// Connection management
	Connect(ctx context.Context) error
	Close() error
	Health() error

	// Broker-specific info
	Type() string
	Capabilities() BrokerCapabilities
}

// BrokerCapabilities describes what features a broker supports
type BrokerCapabilities struct {
	SupportsAck        bool
	SupportsDelay      bool
	SupportsPriority   bool
	SupportsDeadLetter bool
}

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
