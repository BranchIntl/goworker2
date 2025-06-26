package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BranchIntl/goworker2/job"
)

// Mock implementations for testing

// MockBroker implements the Broker interface for testing
type MockBroker struct {
	mu                       sync.RWMutex
	connected                bool
	connectError             error
	healthError              error
	enqueueError             error
	dequeueError             error
	ackError                 error
	nackError                error
	queueLengthError         error
	queues                   map[string][]job.Job
	queueLengths             map[string]int64
	capabilities             BrokerCapabilities
	enqueuedJobs             []job.Job
	ackedJobs                []job.Job
	nackedJobs               []job.Job
	shouldReturnNilOnDequeue bool
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		queues:       make(map[string][]job.Job),
		queueLengths: make(map[string]int64),
		capabilities: BrokerCapabilities{
			SupportsAck:        true,
			SupportsDelay:      true,
			SupportsPriority:   true,
			SupportsDeadLetter: true,
		},
		enqueuedJobs: make([]job.Job, 0),
		ackedJobs:    make([]job.Job, 0),
		nackedJobs:   make([]job.Job, 0),
	}
}

func (m *MockBroker) Enqueue(ctx context.Context, jobArg job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enqueueError != nil {
		return m.enqueueError
	}

	m.enqueuedJobs = append(m.enqueuedJobs, jobArg)
	if m.queues[jobArg.GetQueue()] == nil {
		m.queues[jobArg.GetQueue()] = make([]job.Job, 0)
	}
	m.queues[jobArg.GetQueue()] = append(m.queues[jobArg.GetQueue()], jobArg)
	m.queueLengths[jobArg.GetQueue()]++
	return nil
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
	m.queueLengths[queue]--
	return job, nil
}

func (m *MockBroker) Ack(ctx context.Context, jobArg job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ackError != nil {
		return m.ackError
	}

	m.ackedJobs = append(m.ackedJobs, jobArg)
	return nil
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
		m.queueLengths[jobArg.GetQueue()]++
	}
	return nil
}

func (m *MockBroker) CreateQueue(ctx context.Context, name string, options QueueOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.queues[name] == nil {
		m.queues[name] = make([]job.Job, 0)
		m.queueLengths[name] = 0
	}
	return nil
}

func (m *MockBroker) DeleteQueue(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.queues, name)
	delete(m.queueLengths, name)
	return nil
}

func (m *MockBroker) QueueExists(ctx context.Context, name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.queues[name]
	return exists, nil
}

func (m *MockBroker) QueueLength(ctx context.Context, name string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.queueLengthError != nil {
		return 0, m.queueLengthError
	}

	return m.queueLengths[name], nil
}

func (m *MockBroker) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.connectError != nil {
		return m.connectError
	}

	m.connected = true
	return nil
}

func (m *MockBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connected = false
	return nil
}

func (m *MockBroker) Health() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.healthError != nil {
		return m.healthError
	}

	if !m.connected {
		return fmt.Errorf("not connected")
	}

	return nil
}

func (m *MockBroker) Type() string {
	return "mock"
}

func (m *MockBroker) Capabilities() BrokerCapabilities {
	return m.capabilities
}

// Test helpers
func (m *MockBroker) SetConnectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectError = err
}

func (m *MockBroker) SetHealthError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthError = err
}

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

func (m *MockBroker) GetEnqueuedJobs() []job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]job.Job(nil), m.enqueuedJobs...)
}

func (m *MockBroker) GetAckedJobs() []job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]job.Job(nil), m.ackedJobs...)
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
	m.queueLengths[queue]++
}

// MockJobCall represents a job call for testing
type MockJobCall struct {
	JobID    string
	Queue    string
	Class    string
	WorkerID string
}

// MockStatistics implements the Statistics interface for testing
type MockStatistics struct {
	mu              sync.RWMutex
	connected       bool
	connectError    error
	healthError     error
	registerError   error
	unregisterError error
	recordError     error
	workers         map[string]WorkerInfo
	jobsStarted     []MockJobCall
	jobsCompleted   []MockJobCall
	jobsFailed      []MockJobCall
}

func NewMockStatistics() *MockStatistics {
	return &MockStatistics{
		workers:       make(map[string]WorkerInfo),
		jobsStarted:   make([]MockJobCall, 0),
		jobsCompleted: make([]MockJobCall, 0),
		jobsFailed:    make([]MockJobCall, 0),
	}
}

func (m *MockStatistics) RegisterWorker(ctx context.Context, worker WorkerInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registerError != nil {
		return m.registerError
	}

	m.workers[worker.ID] = worker
	return nil
}

func (m *MockStatistics) UnregisterWorker(ctx context.Context, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.unregisterError != nil {
		return m.unregisterError
	}

	delete(m.workers, workerID)
	return nil
}

func (m *MockStatistics) RecordJobStarted(ctx context.Context, job job.Job, worker WorkerInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.recordError != nil {
		return m.recordError
	}

	call := MockJobCall{
		JobID:    job.GetID(),
		Queue:    job.GetQueue(),
		Class:    job.GetClass(),
		WorkerID: worker.ID,
	}
	m.jobsStarted = append(m.jobsStarted, call)
	return nil
}

func (m *MockStatistics) RecordJobCompleted(ctx context.Context, job job.Job, worker WorkerInfo, duration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.recordError != nil {
		return m.recordError
	}

	call := MockJobCall{
		JobID:    job.GetID(),
		Queue:    job.GetQueue(),
		Class:    job.GetClass(),
		WorkerID: worker.ID,
	}
	m.jobsCompleted = append(m.jobsCompleted, call)
	return nil
}

func (m *MockStatistics) RecordJobFailed(ctx context.Context, job job.Job, worker WorkerInfo, err error, duration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.recordError != nil {
		return m.recordError
	}

	call := MockJobCall{
		JobID:    job.GetID(),
		Queue:    job.GetQueue(),
		Class:    job.GetClass(),
		WorkerID: worker.ID,
	}
	m.jobsFailed = append(m.jobsFailed, call)
	return nil
}

func (m *MockStatistics) GetWorkerStats(ctx context.Context, workerID string) (WorkerStats, error) {
	return WorkerStats{}, nil
}

func (m *MockStatistics) GetQueueStats(ctx context.Context, queue string) (QueueStats, error) {
	return QueueStats{}, nil
}

func (m *MockStatistics) GetGlobalStats(ctx context.Context) (GlobalStats, error) {
	return GlobalStats{}, nil
}

func (m *MockStatistics) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.connectError != nil {
		return m.connectError
	}

	m.connected = true
	return nil
}

func (m *MockStatistics) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connected = false
	return nil
}

func (m *MockStatistics) Health() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.healthError != nil {
		return m.healthError
	}

	if !m.connected {
		return fmt.Errorf("not connected")
	}

	return nil
}

func (m *MockStatistics) Type() string {
	return "mock"
}

// Test helpers
func (m *MockStatistics) SetConnectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectError = err
}

func (m *MockStatistics) SetHealthError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthError = err
}

func (m *MockStatistics) GetJobsStarted() []MockJobCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]MockJobCall(nil), m.jobsStarted...)
}

func (m *MockStatistics) GetJobsCompleted() []MockJobCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]MockJobCall(nil), m.jobsCompleted...)
}

// MockRegistry implements the Registry interface for testing
type MockRegistry struct {
	mu      sync.RWMutex
	workers map[string]WorkerFunc
}

func NewMockRegistry() *MockRegistry {
	return &MockRegistry{
		workers: make(map[string]WorkerFunc),
	}
}

func (m *MockRegistry) Register(class string, worker WorkerFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[class] = worker
	return nil
}

func (m *MockRegistry) Get(class string) (WorkerFunc, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	worker, ok := m.workers[class]
	return worker, ok
}

func (m *MockRegistry) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	classes := make([]string, 0, len(m.workers))
	for class := range m.workers {
		classes = append(classes, class)
	}
	return classes
}

func (m *MockRegistry) Remove(class string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.workers, class)
	return nil
}

func (m *MockRegistry) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers = make(map[string]WorkerFunc)
}

// MockSerializer implements the Serializer interface for testing
type MockSerializer struct {
	mu             sync.RWMutex
	serializeErr   error
	deserializeErr error
	useNumber      bool
}

func NewMockSerializer() *MockSerializer {
	return &MockSerializer{}
}

func (m *MockSerializer) Serialize(job job.Job) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.serializeErr != nil {
		return nil, m.serializeErr
	}

	return []byte(fmt.Sprintf(`{"class":"%s","args":%v}`, job.GetClass(), job.GetArgs())), nil
}

func (m *MockSerializer) Deserialize(data []byte, metadata job.Metadata) (job.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.deserializeErr != nil {
		return nil, m.deserializeErr
	}

	return NewMockJob("test", "default", []interface{}{"arg1", "arg2"}), nil
}

func (m *MockSerializer) GetFormat() string {
	return "mock"
}

func (m *MockSerializer) UseNumber() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.useNumber
}

func (m *MockSerializer) SetUseNumber(useNumber bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.useNumber = useNumber
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
