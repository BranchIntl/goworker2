package pollers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BranchIntl/goworker2/job"
)

// MockBroker implements the pollers.Broker interface for testing
type MockBroker struct {
	mu                       sync.RWMutex
	dequeueError             error
	nackError                error
	queues                   map[string][]job.Job
	nackedJobs               []job.Job
	shouldReturnNilOnDequeue bool
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		queues:     make(map[string][]job.Job),
		nackedJobs: make([]job.Job, 0),
	}
}

func (m *MockBroker) Dequeue(ctx context.Context, queue string) (job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dequeueError != nil {
		return nil, m.dequeueError
	}

	if m.shouldReturnNilOnDequeue {
		return nil, nil
	}

	jobs, exists := m.queues[queue]
	if !exists || len(jobs) == 0 {
		return nil, nil
	}

	job := jobs[0]
	m.queues[queue] = jobs[1:]
	return job, nil
}

func (m *MockBroker) Nack(ctx context.Context, jobArg job.Job, requeue bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nackError != nil {
		return m.nackError
	}

	m.nackedJobs = append(m.nackedJobs, jobArg)
	if requeue {
		if m.queues[jobArg.GetQueue()] == nil {
			m.queues[jobArg.GetQueue()] = make([]job.Job, 0)
		}
		m.queues[jobArg.GetQueue()] = append(m.queues[jobArg.GetQueue()], jobArg)
	}
	return nil
}

// Test helpers
func (m *MockBroker) SetDequeueError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dequeueError = err
}

func (m *MockBroker) SetShouldReturnNilOnDequeue(shouldReturn bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldReturnNilOnDequeue = shouldReturn
}

func (m *MockBroker) GetNackedJobs() []job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]job.Job(nil), m.nackedJobs...)
}

func (m *MockBroker) AddJobToQueue(queue string, jobArg job.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.queues[queue] == nil {
		m.queues[queue] = make([]job.Job, 0)
	}
	m.queues[queue] = append(m.queues[queue], jobArg)
}

// MockJob implements the job.Job interface for testing
type MockJob struct {
	id         string
	queue      string
	class      string
	args       []interface{}
	metadata   job.Metadata
	enqueuedAt time.Time
	retryCount int
	lastError  string
}

func NewMockJob(class, queue string, args []interface{}) *MockJob {
	return &MockJob{
		id:         fmt.Sprintf("job-%d", time.Now().UnixNano()),
		queue:      queue,
		class:      class,
		args:       args,
		enqueuedAt: time.Now(),
		metadata: job.Metadata{
			Queue:      queue,
			EnqueuedAt: time.Now(),
		},
	}
}

func (m *MockJob) GetID() string {
	return m.id
}

func (m *MockJob) GetQueue() string {
	return m.queue
}

func (m *MockJob) GetClass() string {
	return m.class
}

func (m *MockJob) GetArgs() []interface{} {
	return m.args
}

func (m *MockJob) GetMetadata() job.Metadata {
	return m.metadata
}

func (m *MockJob) GetEnqueuedAt() time.Time {
	return m.enqueuedAt
}

func (m *MockJob) GetRetryCount() int {
	return m.retryCount
}

func (m *MockJob) GetLastError() string {
	return m.lastError
}

func (m *MockJob) SetRetryCount(count int) {
	m.retryCount = count
}

func (m *MockJob) SetLastError(err string) {
	m.lastError = err
}

func (m *MockJob) GetPayload() job.Payload {
	return job.Payload{
		Class: m.class,
		Args:  m.args,
	}
}
