package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockJob is a simple job implementation for testing
type mockJob struct {
	id       string
	queue    string
	class    string
	args     []interface{}
	metadata job.Metadata
}

func (m *mockJob) GetID() string             { return m.id }
func (m *mockJob) GetQueue() string          { return m.queue }
func (m *mockJob) GetClass() string          { return m.class }
func (m *mockJob) GetArgs() []interface{}    { return m.args }
func (m *mockJob) GetMetadata() job.Metadata { return m.metadata }
func (m *mockJob) GetEnqueuedAt() time.Time  { return m.metadata.EnqueuedAt }
func (m *mockJob) GetRetryCount() int        { return m.metadata.RetryCount }
func (m *mockJob) GetLastError() string      { return m.metadata.LastError }
func (m *mockJob) SetRetryCount(count int)   { m.metadata.RetryCount = count }
func (m *mockJob) SetLastError(err string)   { m.metadata.LastError = err }
func (m *mockJob) GetPayload() job.Payload {
	return job.Payload{Class: m.class, Args: m.args}
}

func newMockJob(id, queue, class string) *mockJob {
	return &mockJob{
		id:    id,
		queue: queue,
		class: class,
		args:  []interface{}{"arg1", "arg2"},
		metadata: job.Metadata{
			ID:         id,
			Queue:      queue,
			EnqueuedAt: time.Now(),
		},
	}
}

func TestNewBroker(t *testing.T) {
	options := Options{QueueSize: 1000}
	broker := NewBroker(options)

	require.NotNil(t, broker)
	assert.Equal(t, 1000, broker.queueSize)
	assert.NotNil(t, broker.queues)
	assert.False(t, broker.connected)
	assert.Equal(t, options, broker.options)
}

func TestMemoryBroker_Connect(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()

	// Initially not connected
	assert.False(t, broker.connected)

	// Connect should succeed
	err := broker.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, broker.connected)

	// Multiple connects should be fine
	err = broker.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, broker.connected)
}

func TestMemoryBroker_Close(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()

	// Connect first
	require.NoError(t, broker.Connect(ctx))

	// Create some queues
	job1 := newMockJob("1", "test_queue", "TestClass")
	require.NoError(t, broker.Enqueue(ctx, job1))

	// Close should succeed
	err := broker.Close()
	require.NoError(t, err)
	assert.False(t, broker.connected)
	assert.Empty(t, broker.queues)

	// Multiple closes should be fine
	err = broker.Close()
	require.NoError(t, err)
}

func TestMemoryBroker_Health(t *testing.T) {
	broker := NewBroker(DefaultOptions())

	// Not connected - should return error
	err := broker.Health()
	assert.ErrorIs(t, err, errors.ErrNotConnected)

	// Connect and check health
	require.NoError(t, broker.Connect(context.Background()))
	err = broker.Health()
	assert.NoError(t, err)

	// Close and check health
	require.NoError(t, broker.Close())
	err = broker.Health()
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestMemoryBroker_Type(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	assert.Equal(t, "memory", broker.Type())
}

func TestMemoryBroker_Capabilities(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	capabilities := broker.Capabilities()

	expected := core.BrokerCapabilities{
		SupportsAck:        false,
		SupportsDelay:      false,
		SupportsPriority:   false,
		SupportsDeadLetter: false,
	}

	assert.Equal(t, expected, capabilities)
}

func TestMemoryBroker_Enqueue(t *testing.T) {
	broker := NewBroker(Options{QueueSize: 2}) // Small queue for testing full queue
	ctx := context.Background()

	t.Run("not connected", func(t *testing.T) {
		job1 := newMockJob("1", "test_queue", "TestClass")
		err := broker.Enqueue(ctx, job1)
		assert.ErrorIs(t, err, errors.ErrNotConnected)
	})

	// Connect for remaining tests
	require.NoError(t, broker.Connect(ctx))

	t.Run("successful enqueue", func(t *testing.T) {
		job1 := newMockJob("1", "test_queue", "TestClass")
		err := broker.Enqueue(ctx, job1)
		require.NoError(t, err)

		// Queue should be created
		assert.Contains(t, broker.queues, "test_queue")
		assert.Equal(t, 1, len(broker.queues["test_queue"]))
	})

	t.Run("enqueue to existing queue", func(t *testing.T) {
		job2 := newMockJob("2", "test_queue", "TestClass")
		err := broker.Enqueue(ctx, job2)
		require.NoError(t, err)

		// Queue should have 2 jobs now
		assert.Equal(t, 2, len(broker.queues["test_queue"]))
	})

	t.Run("queue full", func(t *testing.T) {
		job3 := newMockJob("3", "test_queue", "TestClass")
		err := broker.Enqueue(ctx, job3)

		var brokerErr *errors.BrokerError
		require.ErrorAs(t, err, &brokerErr)
		assert.Equal(t, "enqueue", brokerErr.Op)
		assert.Equal(t, "test_queue", brokerErr.Queue)
		assert.ErrorIs(t, brokerErr.Err, errors.ErrQueueFull)
	})

}

func TestMemoryBroker_Dequeue(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()
	require.NoError(t, broker.Connect(ctx))

	t.Run("non-existent queue", func(t *testing.T) {
		job, err := broker.Dequeue(ctx, "non_existent")
		require.NoError(t, err)
		assert.Nil(t, job)
	})

	t.Run("empty queue", func(t *testing.T) {
		// Create empty queue
		require.NoError(t, broker.CreateQueue(ctx, "empty_queue", core.QueueOptions{}))

		job, err := broker.Dequeue(ctx, "empty_queue")
		require.NoError(t, err)
		assert.Nil(t, job)
	})

	t.Run("successful dequeue", func(t *testing.T) {
		// Enqueue a job
		originalJob := newMockJob("1", "test_queue", "TestClass")
		require.NoError(t, broker.Enqueue(ctx, originalJob))

		// Dequeue it
		job, err := broker.Dequeue(ctx, "test_queue")
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, originalJob.GetID(), job.GetID())
		assert.Equal(t, originalJob.GetQueue(), job.GetQueue())
	})

	t.Run("closed queue", func(t *testing.T) {
		// Create queue and add a job first
		require.NoError(t, broker.CreateQueue(ctx, "closed_queue", core.QueueOptions{}))
		testJob := newMockJob("closed-job", "closed_queue", "TestClass")
		require.NoError(t, broker.Enqueue(ctx, testJob))

		// Now delete the queue (this closes the channel)
		require.NoError(t, broker.DeleteQueue(ctx, "closed_queue"))

		job, err := broker.Dequeue(ctx, "closed_queue")
		// After deletion, queue doesn't exist anymore, so should return nil, nil
		require.NoError(t, err)
		assert.Nil(t, job)
	})

}

func TestMemoryBroker_Ack(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()
	job1 := newMockJob("1", "test_queue", "TestClass")

	// Ack should always succeed (no-op)
	err := broker.Ack(ctx, job1)
	assert.NoError(t, err)
}

func TestMemoryBroker_Nack(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()
	require.NoError(t, broker.Connect(ctx))

	job1 := newMockJob("1", "test_queue", "TestClass")

	t.Run("nack without requeue", func(t *testing.T) {
		err := broker.Nack(ctx, job1, false)
		assert.NoError(t, err)
	})

	t.Run("nack with requeue", func(t *testing.T) {
		err := broker.Nack(ctx, job1, true)
		require.NoError(t, err)

		// Job should be back in queue
		dequeuedJob, err := broker.Dequeue(ctx, "test_queue")
		require.NoError(t, err)
		require.NotNil(t, dequeuedJob)
		assert.Equal(t, job1.GetID(), dequeuedJob.GetID())
	})
}

func TestMemoryBroker_CreateQueue(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()

	queueName := "test_create_queue"
	queueOptions := core.QueueOptions{MaxRetries: 3}

	err := broker.CreateQueue(ctx, queueName, queueOptions)
	require.NoError(t, err)

	// Queue should exist
	assert.Contains(t, broker.queues, queueName)

	// Creating existing queue should be fine
	err = broker.CreateQueue(ctx, queueName, queueOptions)
	require.NoError(t, err)
}

func TestMemoryBroker_DeleteQueue(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()

	queueName := "test_delete_queue"

	t.Run("delete non-existent queue", func(t *testing.T) {
		err := broker.DeleteQueue(ctx, "non_existent")
		assert.NoError(t, err)
	})

	t.Run("delete existing queue", func(t *testing.T) {
		// Create queue first
		require.NoError(t, broker.CreateQueue(ctx, queueName, core.QueueOptions{}))
		assert.Contains(t, broker.queues, queueName)

		// Delete queue
		err := broker.DeleteQueue(ctx, queueName)
		require.NoError(t, err)
		assert.NotContains(t, broker.queues, queueName)
	})
}

func TestMemoryBroker_QueueExists(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()

	t.Run("non-existent queue", func(t *testing.T) {
		exists, err := broker.QueueExists(ctx, "non_existent")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("existing queue", func(t *testing.T) {
		queueName := "test_exists_queue"
		require.NoError(t, broker.CreateQueue(ctx, queueName, core.QueueOptions{}))

		exists, err := broker.QueueExists(ctx, queueName)
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestMemoryBroker_QueueLength(t *testing.T) {
	broker := NewBroker(DefaultOptions())
	ctx := context.Background()
	require.NoError(t, broker.Connect(ctx))

	t.Run("non-existent queue", func(t *testing.T) {
		length, err := broker.QueueLength(ctx, "non_existent")
		require.NoError(t, err)
		assert.Equal(t, int64(0), length)
	})

	t.Run("empty queue", func(t *testing.T) {
		queueName := "empty_length_queue"
		require.NoError(t, broker.CreateQueue(ctx, queueName, core.QueueOptions{}))

		length, err := broker.QueueLength(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(0), length)
	})

	t.Run("queue with jobs", func(t *testing.T) {
		queueName := "length_test_queue"

		// Enqueue multiple jobs
		for i := 1; i <= 3; i++ {
			job := newMockJob(string(rune(i)), queueName, "TestClass")
			require.NoError(t, broker.Enqueue(ctx, job))
		}

		length, err := broker.QueueLength(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(3), length)

		// Dequeue one and check length
		_, err = broker.Dequeue(ctx, queueName)
		require.NoError(t, err)

		length, err = broker.QueueLength(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(2), length)
	})
}

func TestMemoryBroker_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	broker := NewBroker(DefaultOptions())
	ctx := context.Background()
	require.NoError(t, broker.Connect(ctx))

	const numGoroutines = 10
	const jobsPerGoroutine = 10
	queueName := "concurrent_test_queue"

	// Enqueue jobs concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			for j := 0; j < jobsPerGoroutine; j++ {
				job := newMockJob(
					fmt.Sprintf("worker_%d_job_%d", workerID, j),
					queueName,
					"TestClass",
				)
				broker.Enqueue(ctx, job)
			}
		}(i)
	}

	// Give goroutines time to complete
	time.Sleep(100 * time.Millisecond)

	// Check final queue length
	length, err := broker.QueueLength(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(numGoroutines*jobsPerGoroutine), length)

	// Dequeue all jobs
	dequeueCount := 0
	for {
		job, err := broker.Dequeue(ctx, queueName)
		require.NoError(t, err)
		if job == nil {
			break
		}
		dequeueCount++
	}

	assert.Equal(t, numGoroutines*jobsPerGoroutine, dequeueCount)
}
