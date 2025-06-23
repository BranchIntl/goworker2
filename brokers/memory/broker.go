package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/benmanns/goworker/interfaces"
)

// MemoryBroker implements the Broker interface using in-memory storage
type MemoryBroker struct {
	mu        sync.RWMutex
	queues    map[string]chan interfaces.Job
	queueSize int
	connected bool
	options   Options
}

// NewBroker creates a new in-memory broker
func NewBroker(options Options) *MemoryBroker {
	return &MemoryBroker{
		queues:    make(map[string]chan interfaces.Job),
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

	m.queues = make(map[string]chan interfaces.Job)
	m.connected = false
	return nil
}

// Health checks the broker health
func (m *MemoryBroker) Health() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.connected {
		return fmt.Errorf("not connected")
	}
	return nil
}

// Type returns the broker type
func (m *MemoryBroker) Type() string {
	return "memory"
}

// Capabilities returns broker capabilities
func (m *MemoryBroker) Capabilities() interfaces.BrokerCapabilities {
	return interfaces.BrokerCapabilities{
		SupportsAck:        false,
		SupportsDelay:      false,
		SupportsPriority:   false,
		SupportsDeadLetter: false,
	}
}

// Enqueue adds a job to the queue
func (m *MemoryBroker) Enqueue(ctx context.Context, job interfaces.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.connected {
		return fmt.Errorf("broker not connected")
	}

	queue := job.GetQueue()
	ch, exists := m.queues[queue]
	if !exists {
		// Create queue on demand
		ch = make(chan interfaces.Job, m.queueSize)
		m.queues[queue] = ch
	}

	select {
	case ch <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("queue %s is full", queue)
	}
}

// Dequeue retrieves a job from the queue
func (m *MemoryBroker) Dequeue(ctx context.Context, queue string) (interfaces.Job, error) {
	m.mu.RLock()
	ch, exists := m.queues[queue]
	m.mu.RUnlock()

	if !exists {
		return nil, nil // No jobs in non-existent queue
	}

	select {
	case job, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("queue %s is closed", queue)
		}
		return job, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, nil // No job available
	}
}

// Ack acknowledges job completion (no-op for memory broker)
func (m *MemoryBroker) Ack(ctx context.Context, job interfaces.Job) error {
	return nil
}

// Nack rejects a job and optionally requeues it
func (m *MemoryBroker) Nack(ctx context.Context, job interfaces.Job, requeue bool) error {
	if requeue {
		return m.Enqueue(ctx, job)
	}
	return nil
}

// CreateQueue creates a new queue
func (m *MemoryBroker) CreateQueue(ctx context.Context, name string, options interfaces.QueueOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[name]; !exists {
		m.queues[name] = make(chan interfaces.Job, m.queueSize)
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
