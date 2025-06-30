package core

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/job"
)

// TestSetup provides common test dependencies
type TestSetup struct {
	Broker     *MockBroker
	Stats      *MockStatistics
	Registry   *MockRegistry
	Serializer *MockSerializer
}

// NewTestSetup creates a standard test setup with all mocks
func NewTestSetup() *TestSetup {
	// Set up a discard logger for tests to avoid noise
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError, // Only show errors in tests
	}))
	slog.SetDefault(logger)

	return &TestSetup{
		Broker:     NewMockBroker(),
		Stats:      NewMockStatistics(),
		Registry:   NewMockRegistry(),
		Serializer: NewMockSerializer(),
	}
}

// ContextWithTimeout creates a context with standard timeout for tests
func ContextWithTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second)
}

// ContextWithCustomTimeout creates a context with custom timeout
func ContextWithCustomTimeout(t *testing.T, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// JobBuilder helps create test jobs with fluent interface
type JobBuilder struct {
	class string
	queue string
	args  []interface{}
}

// NewJob starts building a test job
func NewJob() *JobBuilder {
	return &JobBuilder{
		class: "TestJob",
		queue: "test-queue",
		args:  []interface{}{},
	}
}

// WithClass sets the job class
func (b *JobBuilder) WithClass(class string) *JobBuilder {
	b.class = class
	return b
}

// WithQueue sets the job queue
func (b *JobBuilder) WithQueue(queue string) *JobBuilder {
	b.queue = queue
	return b
}

// WithArgs sets the job arguments
func (b *JobBuilder) WithArgs(args ...interface{}) *JobBuilder {
	b.args = args
	return b
}

// Build creates the mock job
func (b *JobBuilder) Build() *MockJob {
	return NewMockJob(b.class, b.queue, b.args)
}

// EngineBuilder helps create engines for testing
type EngineBuilder struct {
	setup   *TestSetup
	options []EngineOption
}

// NewEngine starts building a test engine
func (s *TestSetup) NewEngine() *EngineBuilder {
	return &EngineBuilder{
		setup:   s,
		options: []EngineOption{},
	}
}

// WithOptions adds engine options
func (b *EngineBuilder) WithOptions(options ...EngineOption) *EngineBuilder {
	b.options = append(b.options, options...)
	return b
}

// Build creates the engine
func (b *EngineBuilder) Build() *Engine {
	return NewEngine(b.setup.Broker, b.setup.Stats, b.setup.Registry, b.setup.Serializer, b.options...)
}

// WorkerBuilder helps create workers for testing
type WorkerBuilder struct {
	setup *TestSetup
	id    string
}

// NewWorker starts building a test worker
func (s *TestSetup) NewWorker() *WorkerBuilder {
	return &WorkerBuilder{
		setup: s,
		id:    "test-worker",
	}
}

// WithID sets the worker ID
func (b *WorkerBuilder) WithID(id string) *WorkerBuilder {
	b.id = id
	return b
}

// Build creates the worker
func (b *WorkerBuilder) Build() *Worker {
	return NewWorker(b.id, b.setup.Registry, b.setup.Stats, b.setup.Broker)
}

// WorkerPoolBuilder helps create worker pools for testing
type WorkerPoolBuilder struct {
	setup       *TestSetup
	concurrency int
	jobChan     <-chan job.Job
}

// NewWorkerPool starts building a test worker pool
func (s *TestSetup) NewWorkerPool(jobChan <-chan job.Job) *WorkerPoolBuilder {
	return &WorkerPoolBuilder{
		setup:       s,
		concurrency: 2,
		jobChan:     jobChan,
	}
}

// WithConcurrency sets the worker pool concurrency
func (b *WorkerPoolBuilder) WithConcurrency(concurrency int) *WorkerPoolBuilder {
	b.concurrency = concurrency
	return b
}

// Build creates the worker pool
func (b *WorkerPoolBuilder) Build() *WorkerPool {
	return NewWorkerPool(b.setup.Registry, b.setup.Stats, b.setup.Serializer,
		b.concurrency, b.jobChan, b.setup.Broker)
}

// ErrorTestCase represents a common error test scenario
type ErrorTestCase struct {
	Name        string
	SetupError  func(*TestSetup)
	ExpectedErr string
}

// RunConnectionErrorTests runs standard connection error tests
func RunConnectionErrorTests(t *testing.T, testCases []ErrorTestCase, testFunc func(*TestSetup) error) {
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			setup := NewTestSetup()
			tc.SetupError(setup)

			err := testFunc(setup)

			if tc.ExpectedErr != "" {
				if err == nil {
					t.Errorf("Expected error containing '%s', got nil", tc.ExpectedErr)
				} else if !contains(err.Error(), tc.ExpectedErr) {
					t.Errorf("Expected error containing '%s', got '%s'", tc.ExpectedErr, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
			}
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// RegisterTestWorker is a helper to register common test workers
func (s *TestSetup) RegisterTestWorker(class string, fn WorkerFunc) {
	_ = s.Registry.Register(class, fn)
}

// RegisterSimpleWorker registers a worker that just succeeds
func (s *TestSetup) RegisterSimpleWorker(class string) {
	_ = s.Registry.Register(class, func(queue string, args ...interface{}) error {
		return nil
	})
}

// AddJobToQueue is a helper to add jobs to broker queues
func (s *TestSetup) AddJobToQueue(queue string, job job.Job) {
	s.Broker.AddJobToQueue(queue, job)
}

// CreateJobChannel creates a buffered job channel
func CreateJobChannel(size int) chan job.Job {
	return make(chan job.Job, size)
}

// SendJobsAndClose sends jobs to channel and closes it
func SendJobsAndClose(jobChan chan<- job.Job, jobs ...job.Job) {
	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)
}

// WaitForJobs waits for a specified number of jobs to be processed
func WaitForJobs(t *testing.T, resultChan <-chan string, expectedCount int, timeout time.Duration) []string {
	results := make([]string, 0, expectedCount)
	for i := 0; i < expectedCount; i++ {
		select {
		case result := <-resultChan:
			results = append(results, result)
		case <-time.After(timeout):
			t.Fatalf("Timeout waiting for job %d/%d", i+1, expectedCount)
		}
	}
	return results
}
