package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_Start_Success(t *testing.T) {
	setup := NewTestSetup()
	engine := setup.NewEngine().WithOptions(
		WithConcurrency(2),
	).Build()

	ctx, cancel := ContextWithCustomTimeout(t, 200*time.Millisecond)
	defer cancel()

	err := engine.Start(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, engine.workerPool)

	err = engine.Stop()
	assert.NoError(t, err)
}

func TestEngine_ConnectionErrors(t *testing.T) {
	testCases := []ErrorTestCase{
		{
			Name: "broker connection error",
			SetupError: func(s *TestSetup) {
				s.Broker.SetConnectError(errors.New("broker connection failed"))
			},
			ExpectedErr: "failed to connect broker",
		},
		{
			Name: "stats connection error",
			SetupError: func(s *TestSetup) {
				s.Stats.SetConnectError(errors.New("stats connection failed"))
			},
			ExpectedErr: "failed to connect statistics",
		},
	}

	RunConnectionErrorTests(t, testCases, func(setup *TestSetup) error {
		engine := setup.NewEngine().Build()
		return engine.Start(context.Background())
	})
}

func TestEngine_Stop_BeforeStart(t *testing.T) {
	setup := NewTestSetup()
	engine := setup.NewEngine().Build()

	err := engine.Stop()
	assert.NoError(t, err)
}

func TestEngine_Stop_AfterStart(t *testing.T) {
	setup := NewTestSetup()
	engine := setup.NewEngine().WithOptions(
		WithShutdownTimeout(100 * time.Millisecond),
	).Build()

	ctx, cancel := ContextWithCustomTimeout(t, 200*time.Millisecond)
	defer cancel()

	err := engine.Start(ctx)
	require.NoError(t, err)

	err = engine.Stop()
	assert.NoError(t, err)
}

func TestEngine_Health_Healthy(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	// Add some jobs to queues so the broker knows about them
	broker.AddJobToQueue("queue1", NewMockJob("TestJob", "queue1", []interface{}{}))
	broker.AddJobToQueue("queue2", NewMockJob("TestJob", "queue2", []interface{}{}))

	engine := NewEngine(broker, stats, registry)

	ctx := context.Background()
	err := engine.Start(ctx)
	require.NoError(t, err)
	defer func() { _ = engine.Stop() }()

	health := engine.Health()

	assert.True(t, health.Healthy)
	assert.NoError(t, health.BrokerHealth)
	assert.NoError(t, health.StatsHealth)
	assert.Contains(t, health.QueuedJobs, "queue1")
	assert.Contains(t, health.QueuedJobs, "queue2")
	assert.False(t, health.LastCheck.IsZero())
}

func TestEngine_Health_Unhealthy(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	// Configure broker to report unhealthy
	broker.SetHealthError(errors.New("broker is down"))

	engine := NewEngine(broker, stats, registry)

	ctx := context.Background()
	err := engine.Start(ctx)
	require.NoError(t, err)
	defer func() { _ = engine.Stop() }()

	health := engine.Health()

	assert.False(t, health.Healthy)
	assert.Error(t, health.BrokerHealth)
	assert.Contains(t, health.BrokerHealth.Error(), "broker is down")
}

func TestEngine_Register(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry)

	// Test registering a worker
	err := engine.Register("TestJob", func(queue string, args ...interface{}) error {
		return nil
	})
	assert.NoError(t, err)

	// Verify worker was registered
	worker, exists := registry.Get("TestJob")
	assert.True(t, exists)
	assert.NotNil(t, worker)
}

func TestEngine_Run_ContextCancellation(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry,
		WithConcurrency(1),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Start Run in goroutine
	done := make(chan error, 1)
	go func() {
		done <- engine.Run(ctx)
	}()

	// Give it time to start
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Should stop gracefully
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Engine.Run did not stop within timeout")
	}
}

func TestEngine_Integration_JobProcessing(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry,
		WithConcurrency(2),
	)

	// Register a test worker
	processedJobs := make(chan string, 10)
	err := engine.Register("TestJob", func(queue string, args ...interface{}) error {
		processedJobs <- args[0].(string)
		return nil
	})
	require.NoError(t, err)

	// Start engine
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = engine.Start(ctx)
	require.NoError(t, err)

	// Enqueue some jobs
	for i := 0; i < 3; i++ {
		job := NewMockJob("TestJob", "test-queue", []interface{}{fmt.Sprintf("job-%d", i)})
		broker.AddJobToQueue("test-queue", job)
	}

	// Wait for jobs to be processed
	processed := make([]string, 0)
	for i := 0; i < 3; i++ {
		select {
		case jobResult := <-processedJobs:
			processed = append(processed, jobResult)
		case <-time.After(time.Second):
			t.Fatalf("Job %d was not processed within timeout", i)
		}
	}

	assert.Len(t, processed, 3)
	assert.Contains(t, processed, "job-0")
	assert.Contains(t, processed, "job-1")
	assert.Contains(t, processed, "job-2")

	// Stop engine
	err = engine.Stop()
	assert.NoError(t, err)
}

func TestEngine_Integration_FailedJobProcessing(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry,
		WithConcurrency(1),
	)

	// Register a failing worker
	err := engine.Register("FailingJob", func(queue string, args ...interface{}) error {
		return errors.New("job failed")
	})
	require.NoError(t, err)

	// Start engine
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = engine.Start(ctx)
	require.NoError(t, err)

	// Enqueue a failing job
	job := NewMockJob("FailingJob", "test-queue", []interface{}{})
	broker.AddJobToQueue("test-queue", job)

	// Give time for job to be processed
	time.Sleep(200 * time.Millisecond)

	// Stop engine
	err = engine.Stop()
	assert.NoError(t, err)

	// Job should have been nacked multiple times due to retries
	assert.GreaterOrEqual(t, len(broker.GetNackedJobs()), 1)
}

func TestEngine_Integration_MultipleQueues(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry,
		WithConcurrency(2),
	)

	// Register workers
	processedJobs := make(chan string, 10)
	err := engine.Register("TestJob", func(queue string, args ...interface{}) error {
		processedJobs <- queue + ":" + args[0].(string)
		return nil
	})
	require.NoError(t, err)

	// Start engine
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = engine.Start(ctx)
	require.NoError(t, err)

	// Enqueue jobs to different queues
	lowJob := NewMockJob("TestJob", "low", []interface{}{"low-job"})
	highJob := NewMockJob("TestJob", "high", []interface{}{"high-job"})
	mediumJob := NewMockJob("TestJob", "medium", []interface{}{"medium-job"})

	broker.AddJobToQueue("low", lowJob)
	broker.AddJobToQueue("high", highJob)
	broker.AddJobToQueue("medium", mediumJob)

	// Collect processed jobs
	processed := make([]string, 0)
	for i := 0; i < 3; i++ {
		select {
		case jobResult := <-processedJobs:
			processed = append(processed, jobResult)
		case <-time.After(time.Second):
			t.Fatalf("Job %d was not processed within timeout", i)
		}
	}

	// Note: Queue ordering depends on polling timing, so we just verify all jobs were processed
	assert.Len(t, processed, 3)
	assert.Contains(t, processed, "high:high-job")
	assert.Contains(t, processed, "medium:medium-job")
	assert.Contains(t, processed, "low:low-job")

	// Stop engine
	err = engine.Stop()
	assert.NoError(t, err)
}

func TestEngine_Start_WithShutdownTimeout(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry,
		WithConcurrency(1),
		WithShutdownTimeout(50*time.Millisecond), // Very short timeout
	)

	// Register a long-running worker
	err := engine.Register("LongJob", func(queue string, args ...interface{}) error {
		time.Sleep(200 * time.Millisecond) // Longer than shutdown timeout
		return nil
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = engine.Start(ctx)
	require.NoError(t, err)

	// Enqueue a long job
	job := NewMockJob("LongJob", "test-queue", []interface{}{})
	broker.AddJobToQueue("test-queue", job)

	// Give time for job to start
	time.Sleep(50 * time.Millisecond)

	// Stop should timeout due to long-running job
	start := time.Now()
	err = engine.Stop()
	duration := time.Since(start)

	assert.NoError(t, err)
	// Should have waited at least the shutdown timeout
	assert.GreaterOrEqual(t, duration, 50*time.Millisecond)
}

func TestEngine_Health_QueueLengths(t *testing.T) {
	broker := NewMockBroker()
	stats := NewMockStatistics()
	registry := NewMockRegistry()

	engine := NewEngine(broker, stats, registry)

	// Add jobs to queues
	job1 := NewMockJob("TestJob", "queue1", []interface{}{})
	job2 := NewMockJob("TestJob", "queue1", []interface{}{})
	job3 := NewMockJob("TestJob", "queue2", []interface{}{})

	broker.AddJobToQueue("queue1", job1)
	broker.AddJobToQueue("queue1", job2)
	broker.AddJobToQueue("queue2", job3)

	ctx := context.Background()
	err := engine.Start(ctx)
	require.NoError(t, err)
	defer func() { _ = engine.Stop() }()

	health := engine.Health()

	assert.True(t, health.Healthy)
	assert.Equal(t, int64(2), health.QueuedJobs["queue1"])
	assert.Equal(t, int64(1), health.QueuedJobs["queue2"])
}
