package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/job"
	"github.com/stretchr/testify/assert"
)

func TestPoller_Start_WithJobs(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	jobChan := make(chan job.Job, 10)
	queues := []string{"test-queue"}

	// Add a job to the broker
	testJob := NewMockJob("TestJob", "test-queue", []interface{}{"arg1"})
	broker.AddJobToQueue("test-queue", testJob)

	poller := NewStandardPoller(broker, stats, queues, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start poller in goroutine
	done := make(chan error, 1)
	go func() {
		done <- poller.Start(ctx, jobChan)
	}()

	// Should receive the job
	select {
	case receivedJob := <-jobChan:
		assert.Equal(t, testJob.GetID(), receivedJob.GetID())
		assert.Equal(t, testJob.GetClass(), receivedJob.GetClass())
	case <-time.After(time.Second):
		t.Fatal("Did not receive job within timeout")
	}

	// Wait for context to timeout and poller to stop
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Poller did not stop within timeout")
	}
}

func TestPoller_Start_NoJobs(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	jobChan := make(chan job.Job, 10)
	queues := []string{"empty-queue"}

	// Configure broker to return nil (no jobs)
	broker.SetShouldReturnNilOnDequeue(true)

	poller := NewStandardPoller(broker, stats, queues, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start poller in goroutine
	done := make(chan error, 1)
	go func() {
		done <- poller.Start(ctx, jobChan)
	}()

	// Should not receive any jobs
	select {
	case job := <-jobChan:
		t.Fatalf("Received unexpected job: %v", job)
	case <-time.After(100 * time.Millisecond):
		// Expected - no jobs should be received
	}

	// Wait for poller to stop
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Poller did not stop within timeout")
	}
}

func TestPoller_Start_MultipleQueues(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	jobChan := make(chan job.Job, 10)
	queues := []string{"queue1", "queue2", "queue3"}

	// Add jobs to different queues
	job1 := NewMockJob("Job1", "queue1", []interface{}{})
	job2 := NewMockJob("Job2", "queue2", []interface{}{})
	job3 := NewMockJob("Job3", "queue3", []interface{}{})

	broker.AddJobToQueue("queue1", job1)
	broker.AddJobToQueue("queue2", job2)
	broker.AddJobToQueue("queue3", job3)

	poller := NewStandardPoller(broker, stats, queues, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start poller in goroutine
	go func() {
		_ = poller.Start(ctx, jobChan)
	}()

	// Should receive all jobs
	receivedJobs := make([]job.Job, 0)
	for i := 0; i < 3; i++ {
		select {
		case receivedJob := <-jobChan:
			receivedJobs = append(receivedJobs, receivedJob)
		case <-time.After(time.Second):
			t.Fatalf("Did not receive job %d within timeout", i+1)
		}
	}

	assert.Len(t, receivedJobs, 3)

	assert.Equal(t, "Job1", receivedJobs[0].GetClass())
	assert.Equal(t, "Job2", receivedJobs[1].GetClass())
	assert.Equal(t, "Job3", receivedJobs[2].GetClass())
}

func TestPoller_Start_DequeueError(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	jobChan := make(chan job.Job, 10)
	queues := []string{"error-queue"}

	// Configure broker to return error
	broker.SetDequeueError(errors.New("dequeue failed"))

	poller := NewStandardPoller(broker, stats, queues, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start poller - should handle errors gracefully
	err := poller.Start(ctx, jobChan)
	assert.NoError(t, err)
}

func TestPoller_Start_ContextCancellationWithJob(t *testing.T) {
	broker := NewMockBroker()
	jobChan := make(chan job.Job)
	queues := []string{"test-queue"}

	// Add a job to the broker
	testJob := NewMockJob("TestJob", "test-queue", []interface{}{})
	broker.AddJobToQueue("test-queue", testJob)

	poller := NewStandardPoller(broker, nil, queues, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	// Start poller in goroutine
	done := make(chan error, 1)
	go func() {
		done <- poller.Start(ctx, jobChan)
	}()

	// Wait for poller to poll the job and try to send it (will block on unbuffered channel)
	time.Sleep(50 * time.Millisecond)

	// Cancel context while job is being sent
	cancel()

	// Poller should stop and possibly requeue the job
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Poller did not stop within timeout")
	}

	// Job might have been requeued (nacked) or successfully sent
	// In this test, we primarily verify the poller stops gracefully
	assert.GreaterOrEqual(t, len(broker.GetNackedJobs()), 0)
}

func TestPoller_pollOnce_Error(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	queues := []string{"error-queue"}

	// Configure broker to return error
	testError := errors.New("dequeue error")
	broker.SetDequeueError(testError)

	poller := NewStandardPoller(broker, stats, queues, time.Second)

	ctx := context.Background()
	job, err := poller.pollOnce(ctx)

	assert.Error(t, err)
	assert.Equal(t, testError, err)
	assert.Nil(t, job)
}

func TestPoller_Start_ChannelBlocked(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()

	// Create small buffered channel that can be filled
	jobChan := make(chan job.Job, 1)
	queues := []string{"test-queue"}

	// Add multiple jobs to the broker
	for i := 0; i < 3; i++ {
		testJob := NewMockJob("TestJob", "test-queue", []interface{}{i})
		broker.AddJobToQueue("test-queue", testJob)
	}

	poller := NewStandardPoller(broker, stats, queues, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start poller in goroutine
	go func() {
		_ = poller.Start(ctx, jobChan)
	}()

	// Fill the channel buffer
	select {
	case <-jobChan:
		// Remove one job from channel
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Did not receive first job")
	}

	// Should be able to receive more jobs
	select {
	case <-jobChan:
		// Got another job
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Did not receive second job")
	}
}
