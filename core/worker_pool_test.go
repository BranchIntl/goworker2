package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/job"
	"github.com/cihub/seelog"
	"github.com/stretchr/testify/assert"
)

func TestWorkerPool_Start_Basic(t *testing.T) {
	setup := NewTestSetup()
	jobChan := CreateJobChannel(10)
	pool := setup.NewWorkerPool(jobChan).WithConcurrency(3).Build()

	ctx, cancel := ContextWithCustomTimeout(t, 100*time.Millisecond)
	defer cancel()

	close(jobChan)

	err := pool.Start(ctx)
	assert.NoError(t, err)
	assert.Len(t, pool.workers, 3)
	assert.Equal(t, 0, pool.ActiveWorkers())
}

func TestWorkerPool_Start_JobProcessing(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 2

	// Register a test worker function
	jobsProcessed := make(chan string, 10)
	registry.Register("TestJob", func(queue string, args ...interface{}) error {
		jobsProcessed <- queue + ":" + args[0].(string)
		return nil
	})

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	// Send some jobs
	job1 := NewMockJob("TestJob", "queue1", []interface{}{"job1"})
	job2 := NewMockJob("TestJob", "queue2", []interface{}{"job2"})
	jobChan <- job1
	jobChan <- job2
	close(jobChan)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := pool.Start(ctx)
	assert.NoError(t, err)

	// Verify jobs were processed
	processed := make([]string, 0)
	for i := 0; i < 2; i++ {
		select {
		case result := <-jobsProcessed:
			processed = append(processed, result)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Job was not processed within timeout")
		}
	}

	assert.Len(t, processed, 2)
	assert.Contains(t, processed, "queue1:job1")
	assert.Contains(t, processed, "queue2:job2")
}

func TestWorkerPool_Start_ConcurrentJobProcessing(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 3

	// Register a worker function that takes some time
	var mu sync.Mutex
	processedJobs := make([]string, 0)
	activeCount := 0
	maxActiveCount := 0

	registry.Register("SlowJob", func(queue string, args ...interface{}) error {
		mu.Lock()
		activeCount++
		if activeCount > maxActiveCount {
			maxActiveCount = activeCount
		}
		mu.Unlock()

		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		processedJobs = append(processedJobs, args[0].(string))
		activeCount--
		mu.Unlock()

		return nil
	})

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	// Send multiple jobs
	numJobs := 5
	for i := 0; i < numJobs; i++ {
		job := NewMockJob("SlowJob", "test-queue", []interface{}{fmt.Sprintf("job-%d", i)})
		jobChan <- job
	}
	close(jobChan)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pool.Start(ctx)
	assert.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	// All jobs should be processed
	assert.Len(t, processedJobs, numJobs)

	// Should have processed jobs concurrently (max active should be <= concurrency)
	assert.LessOrEqual(t, maxActiveCount, concurrency)
	assert.Greater(t, maxActiveCount, 1) // Should have some concurrency
}

func TestWorkerPool_Start_ContextCancellation(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 2

	// Register a long-running worker function
	jobStarted := make(chan bool, 2)
	registry.Register("LongJob", func(queue string, args ...interface{}) error {
		jobStarted <- true
		time.Sleep(time.Second) // Long running
		return nil
	})

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	// Send a job
	job := NewMockJob("LongJob", "test-queue", []interface{}{})
	jobChan <- job

	ctx, cancel := context.WithCancel(context.Background())

	// Start pool in goroutine
	done := make(chan error, 1)
	go func() {
		done <- pool.Start(ctx)
	}()

	// Wait for job to start
	select {
	case <-jobStarted:
		// Job started
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Job did not start within timeout")
	}

	// Cancel context
	cancel()
	close(jobChan)

	// Pool should stop
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Worker pool did not stop within timeout")
	}
}

func TestWorkerPool_ActiveWorkers(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 3

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	// Initially no active workers
	assert.Equal(t, 0, pool.ActiveWorkers())

	// Register a worker that signals when it starts and waits
	workerStarted := make(chan bool, concurrency)
	workerCanContinue := make(chan bool, concurrency)

	registry.Register("TestJob", func(queue string, args ...interface{}) error {
		workerStarted <- true
		<-workerCanContinue
		return nil
	})

	// Send jobs
	for i := 0; i < concurrency; i++ {
		job := NewMockJob("TestJob", "test-queue", []interface{}{})
		jobChan <- job
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start pool in goroutine
	go func() {
		pool.Start(ctx)
	}()

	// Wait for all workers to start
	for i := 0; i < concurrency; i++ {
		select {
		case <-workerStarted:
			// Worker started
		case <-time.After(time.Second):
			t.Fatalf("Worker %d did not start within timeout", i)
		}
	}

	// Should have active workers now
	assert.Equal(t, concurrency, pool.ActiveWorkers())

	// Allow workers to complete
	for i := 0; i < concurrency; i++ {
		workerCanContinue <- true
	}
	close(jobChan)

	// Give time for workers to complete
	time.Sleep(100 * time.Millisecond)
}

func TestWorkerPool_Start_ZeroConcurrency(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 0

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	close(jobChan)

	err := pool.Start(ctx)
	assert.NoError(t, err)

	// No workers should be created
	assert.Len(t, pool.workers, 0)
	assert.Equal(t, 0, pool.ActiveWorkers())
}

func TestWorkerPool_Start_LargeConcurrency(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 100

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	close(jobChan)

	err := pool.Start(ctx)
	assert.NoError(t, err)

	// All workers should be created
	assert.Len(t, pool.workers, concurrency)
}

func TestWorkerPool_Start_MultipleJobTypes(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 2

	// Register multiple job types
	results := make(chan string, 10)
	registry.Register("JobA", func(queue string, args ...interface{}) error {
		results <- "A:" + args[0].(string)
		return nil
	})
	registry.Register("JobB", func(queue string, args ...interface{}) error {
		results <- "B:" + args[0].(string)
		return nil
	})

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	// Send different job types
	jobA1 := NewMockJob("JobA", "queue1", []interface{}{"a1"})
	jobB1 := NewMockJob("JobB", "queue1", []interface{}{"b1"})
	jobA2 := NewMockJob("JobA", "queue2", []interface{}{"a2"})

	jobChan <- jobA1
	jobChan <- jobB1
	jobChan <- jobA2
	close(jobChan)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := pool.Start(ctx)
	assert.NoError(t, err)

	// Collect results
	processed := make([]string, 0)
	for i := 0; i < 3; i++ {
		select {
		case result := <-results:
			processed = append(processed, result)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("Job %d was not processed within timeout", i)
		}
	}

	assert.Len(t, processed, 3)
	assert.Contains(t, processed, "A:a1")
	assert.Contains(t, processed, "B:b1")
	assert.Contains(t, processed, "A:a2")
}

func TestWorkerPool_Start_LongRunningJobs(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	serializer := NewMockSerializer()
	logger := seelog.Disabled
	broker := NewMockBroker()
	jobChan := make(chan job.Job, 10)
	concurrency := 2

	// Register a job that takes time but respects context cancellation
	jobsStarted := make(chan bool, 10)
	jobsCompleted := make(chan bool, 10)

	registry.Register("LongJob", func(queue string, args ...interface{}) error {
		jobsStarted <- true
		time.Sleep(200 * time.Millisecond)
		jobsCompleted <- true
		return nil
	})

	pool := NewWorkerPool(registry, stats, serializer, concurrency, jobChan, logger, broker)

	// Send multiple long jobs
	for i := 0; i < 4; i++ {
		job := NewMockJob("LongJob", "test-queue", []interface{}{})
		jobChan <- job
	}
	close(jobChan)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	err := pool.Start(ctx)
	duration := time.Since(start)

	assert.NoError(t, err)

	// Should have started all jobs
	startedCount := 0
	for len(jobsStarted) > 0 {
		<-jobsStarted
		startedCount++
	}
	assert.Equal(t, 4, startedCount)

	// Should have completed all jobs within reasonable time
	completedCount := 0
	for len(jobsCompleted) > 0 {
		<-jobsCompleted
		completedCount++
	}
	assert.Equal(t, 4, completedCount)

	// With concurrency of 2, should take roughly 2 * 200ms = 400ms
	// But allow some margin for test execution overhead
	assert.Less(t, duration, 600*time.Millisecond)
}
