package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
)

// MemoryBroker implements the Broker interface using in-memory storage
type MemoryBroker struct {
	mu        sync.RWMutex
	queues    map[string]chan job.Job
	queueSize int
	connected bool
	options   Options
}

// NewBroker creates a new in-memory broker
func NewBroker(options Options) *MemoryBroker {
	return &MemoryBroker{
		queues:    make(map[string]chan job.Job),
		queueSize: options.QueueSize,
		options:   options,
	}
}

// Connect establishes connection (no-op for memory broker)
func (m *MemoryBroker) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connected = true
	return nil
}

// Close closes the broker
func (m *MemoryBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close all queue channels
	for _, ch := range m.queues {
		close(ch)
	}

	m.queues = make(map[string]chan job.Job)
	m.connected = false
	return nil
}

// Health checks the broker health
func (m *MemoryBroker) Health() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.connected {
		return errors.ErrNotConnected
	}
	return nil
}

// Type returns the broker type
func (m *MemoryBroker) Type() string {
	return "memory"
}

// Capabilities returns broker capabilities
func (m *MemoryBroker) Capabilities() core.BrokerCapabilities {
	return core.BrokerCapabilities{
		SupportsAck:        false,
		SupportsDelay:      false,
		SupportsPriority:   false,
		SupportsDeadLetter: false,
	}
}

// Enqueue adds a job to the queue
func (m *MemoryBroker) Enqueue(ctx context.Context, j job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.connected {
		return errors.ErrNotConnected
	}

	queue := j.GetQueue()
	ch, exists := m.queues[queue]
	if !exists {
		// Create queue on demand
		ch = make(chan job.Job, m.queueSize)
		m.queues[queue] = ch
	}

	select {
	case ch <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.NewBrokerError("enqueue", queue, errors.ErrQueueFull)
	}
}

// Dequeue retrieves a job from the queue
func (m *MemoryBroker) Dequeue(ctx context.Context, queue string) (job.Job, error) {
	m.mu.RLock()
	ch, exists := m.queues[queue]
	m.mu.RUnlock()

	if !exists {
		return nil, nil // No jobs in non-existent queue
	}

	select {
	case j, ok := <-ch:
		if !ok {
			return nil, errors.NewBrokerError("dequeue", queue,
				fmt.Errorf("queue is closed"))
		}
		return j, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, nil // No job available
	}
}

// Ack acknowledges job completion (no-op for memory broker)
func (m *MemoryBroker) Ack(ctx context.Context, j job.Job) error {
	return nil
}

// Nack rejects a job and optionally requeues it
func (m *MemoryBroker) Nack(ctx context.Context, j job.Job, requeue bool) error {
	if requeue {
		return m.Enqueue(ctx, j)
	}
	return nil
}

// CreateQueue creates a new queue
func (m *MemoryBroker) CreateQueue(ctx context.Context, name string, options core.QueueOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[name]; !exists {
		m.queues[name] = make(chan job.Job, m.queueSize)
	}

	return nil
}

// DeleteQueue deletes a queue
func (m *MemoryBroker) DeleteQueue(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ch, exists := m.queues[name]; exists {
		close(ch)
		delete(m.queues, name)
	}

	return nil
}

// QueueExists checks if a queue exists
func (m *MemoryBroker) QueueExists(ctx context.Context, name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.queues[name]
	return exists, nil
}

// QueueLength returns the number of jobs in a queue
func (m *MemoryBroker) QueueLength(ctx context.Context, name string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ch, exists := m.queues[name]
	if !exists {
		return 0, nil
	}

	return int64(len(ch)), nil
}
