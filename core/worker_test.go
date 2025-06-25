package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/job"
	"github.com/cihub/seelog"
	"github.com/stretchr/testify/assert"
)

func TestWorker_Work_JobProcessing(t *testing.T) {
	setup := NewTestSetup()

	executed := false
	setup.RegisterTestWorker("TestJob", func(queue string, args ...interface{}) error {
		executed = true
		assert.Equal(t, "test-queue", queue)
		assert.Equal(t, []interface{}{"arg1", "arg2"}, args)
		return nil
	})

	worker := setup.NewWorker().Build()

	jobChan := CreateJobChannel(1)
	testJob := NewJob().WithClass("TestJob").WithArgs("arg1", "arg2").Build()
	SendJobsAndClose(jobChan, testJob)

	ctx, cancel := ContextWithTimeout(t)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)
	assert.True(t, executed)

	assert.Len(t, setup.Broker.GetAckedJobs(), 1)
	assert.Equal(t, testJob.GetID(), setup.Broker.GetAckedJobs()[0].GetID())
	assert.Len(t, setup.Stats.GetJobsStarted(), 1)
	assert.Equal(t, "TestJob", setup.Stats.GetJobsStarted()[0].Class)
	assert.Len(t, setup.Stats.GetJobsCompleted(), 1)
	assert.Equal(t, "TestJob", setup.Stats.GetJobsCompleted()[0].Class)
}

func TestWorker_Work_JobFailure(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	// Register a failing worker function
	testError := errors.New("job failed")
	registry.Register("FailingJob", func(queue string, args ...interface{}) error {
		return testError
	})

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel and send a job
	jobChan := make(chan job.Job, 1)
	testJob := NewMockJob("FailingJob", "test-queue", []interface{}{})
	jobChan <- testJob
	close(jobChan)

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)

	// Verify job was nacked
	assert.Len(t, broker.GetNackedJobs(), 1)

	// Verify failure was recorded in statistics
	jobsStarted := stats.GetJobsStarted()
	assert.Len(t, jobsStarted, 1)
}

func TestWorker_Work_UnknownJob(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel with unknown job type
	jobChan := make(chan job.Job, 1)
	testJob := NewMockJob("UnknownJob", "test-queue", []interface{}{})
	jobChan <- testJob
	close(jobChan)

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)

	// Verify job was nacked due to unknown worker
	assert.Len(t, broker.GetNackedJobs(), 1)
}

func TestWorker_Work_PanicRecovery(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	// Register a panicking worker function
	registry.Register("PanicJob", func(queue string, args ...interface{}) error {
		panic("test panic")
	})

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel and send a job
	jobChan := make(chan job.Job, 1)
	testJob := NewMockJob("PanicJob", "test-queue", []interface{}{})
	jobChan <- testJob
	close(jobChan)

	// Start worker - should not panic
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)

	// Verify job was nacked due to panic
	assert.Len(t, broker.GetNackedJobs(), 1)
}

func TestWorker_Work_ContextCancellation(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel but don't send any jobs
	jobChan := make(chan job.Job)

	// Start worker with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)
}

func TestWorker_Work_ChannelClosed(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel and close it immediately
	jobChan := make(chan job.Job)
	close(jobChan)

	// Start worker
	ctx := context.Background()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)
}

func TestWorker_Work_StatisticsErrors(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	// Set up statistics to return errors
	stats.SetConnectError(errors.New("stats error"))

	registry.Register("TestJob", func(queue string, args ...interface{}) error {
		return nil
	})

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel and send a job
	jobChan := make(chan job.Job, 1)
	testJob := NewMockJob("TestJob", "test-queue", []interface{}{})
	jobChan <- testJob
	close(jobChan)

	// Start worker - should handle statistics errors gracefully
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)

	// Job should still be processed despite statistics errors
	ackedJobs := broker.GetAckedJobs()
	assert.Len(t, ackedJobs, 1)
}

func TestWorker_Work_BrokerErrors(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	// Set up broker to return ack/nack errors
	broker.ackError = errors.New("ack failed")

	registry.Register("TestJob", func(queue string, args ...interface{}) error {
		return nil
	})

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel and send a job
	jobChan := make(chan job.Job, 1)
	testJob := NewMockJob("TestJob", "test-queue", []interface{}{})
	jobChan <- testJob
	close(jobChan)

	// Start worker - should handle broker errors gracefully
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)
}

func TestWorker_ProcessMultipleJobs(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	jobCount := 0
	registry.Register("CounterJob", func(queue string, args ...interface{}) error {
		jobCount++
		return nil
	})

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel with multiple jobs
	jobChan := make(chan job.Job, 3)
	for i := 0; i < 3; i++ {
		testJob := NewMockJob("CounterJob", "test-queue", []interface{}{})
		jobChan <- testJob
	}
	close(jobChan)

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := worker.Work(ctx, jobChan)
	assert.NoError(t, err)
	assert.Equal(t, 3, jobCount)

	// Verify all jobs were acked
	ackedJobs := broker.GetAckedJobs()
	assert.Len(t, ackedJobs, 3)
}

func TestWorker_LongRunningJob(t *testing.T) {
	registry := NewMockRegistry()
	stats := NewMockStatistics()
	logger := seelog.Disabled
	broker := NewMockBroker()

	jobStarted := make(chan bool)
	jobShouldComplete := make(chan bool)

	registry.Register("LongJob", func(queue string, args ...interface{}) error {
		jobStarted <- true
		<-jobShouldComplete
		return nil
	})

	worker := NewWorker("test", registry, stats, logger, broker)

	// Create job channel
	jobChan := make(chan job.Job, 1)
	testJob := NewMockJob("LongJob", "test-queue", []interface{}{})
	jobChan <- testJob

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		worker.Work(ctx, jobChan)
	}()

	// Wait for job to start
	select {
	case <-jobStarted:
		// Job started, now allow it to complete
		close(jobShouldComplete)
		close(jobChan)
	case <-time.After(time.Second):
		t.Fatal("Job did not start within expected time")
	}

	// Give some time for job to complete
	time.Sleep(100 * time.Millisecond)

	// Verify job was acked
	ackedJobs := broker.GetAckedJobs()
	assert.Len(t, ackedJobs, 1)
}
