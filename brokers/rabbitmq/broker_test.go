package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSerializer is a simple serializer for testing
type mockSerializer struct {
	serializeErr   error
	deserializeErr error
	format         string
}

func (m *mockSerializer) Serialize(j job.Job) ([]byte, error) {
	if m.serializeErr != nil {
		return nil, m.serializeErr
	}
	return []byte(`{"class":"TestClass","args":["arg1","arg2"]}`), nil
}

func (m *mockSerializer) Deserialize(data []byte, metadata job.Metadata) (job.Job, error) {
	if m.deserializeErr != nil {
		return nil, m.deserializeErr
	}
	return &mockJob{
		id:       metadata.ID,
		queue:    metadata.Queue,
		class:    "TestClass",
		args:     []interface{}{"arg1", "arg2"},
		metadata: metadata,
	}, nil
}

func (m *mockSerializer) GetFormat() string { return m.format }

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

func TestNewBroker(t *testing.T) {
	options := DefaultOptions()
	serializer := &mockSerializer{format: "json"}

	broker := NewBroker(options, serializer)

	require.NotNil(t, broker)
	assert.Equal(t, options, broker.options)
	assert.Equal(t, serializer, broker.serializer)
	assert.NotNil(t, broker.declaredQueues)
}

func TestRabbitMQBroker_Connect_InvalidURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "invalid URI format",
			uri:  "://invalid",
		},
		{
			name: "unsupported scheme",
			uri:  "http://localhost:5672",
		},
		{
			name: "unreachable host",
			uri:  "amqp://guest:guest@unreachable-host:5672/",
		},
		{
			name: "invalid port",
			uri:  "amqp://guest:guest@localhost:99999/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := DefaultOptions()
			options.URI = tt.uri

			broker := NewBroker(options, &mockSerializer{})
			ctx := context.Background()

			err := broker.Connect(ctx)
			require.Error(t, err)

			var connErr *errors.ConnectionError
			assert.ErrorAs(t, err, &connErr)
		})
	}
}

func TestRabbitMQBroker_Health_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})

	err := broker.Health()
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRabbitMQBroker_Close_NilConnection(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})

	err := broker.Close()
	assert.NoError(t, err)
}

func TestRabbitMQBroker_Ack_NotRMQJob(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job := &mockJob{id: "test", queue: "test_queue", class: "TestClass"}

	// Ack should succeed for non-RMQJob (no-op)
	err := broker.Ack(ctx, job)
	assert.NoError(t, err)
}

func TestRabbitMQBroker_Nack_NotRMQJob(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job := &mockJob{id: "test", queue: "test_queue", class: "TestClass"}

	// Nack should succeed for non-RMQJob (no-op)
	err := broker.Nack(ctx, job, false)
	assert.NoError(t, err)

	err = broker.Nack(ctx, job, true)
	assert.NoError(t, err)
}

func TestRabbitMQBroker_CreateQueue_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	queueOptions := QueueOptions{
		MaxRetries:      3,
		MessageTTL:      5 * time.Minute,
		DeadLetterQueue: "dlq",
	}

	err := broker.CreateQueue(ctx, "test_queue", queueOptions)
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRabbitMQBroker_DeleteQueue_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	err := broker.DeleteQueue(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRabbitMQBroker_QueueExists_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	exists, err := broker.QueueExists(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
	assert.False(t, exists)
}

func TestRabbitMQBroker_QueueLength_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	length, err := broker.QueueLength(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
	assert.Equal(t, int64(0), length)
}

func TestRabbitMQBroker_Enqueue_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job := &mockJob{
		id:    "test-job-1",
		queue: "test_queue",
		class: "TestClass",
		args:  []interface{}{"arg1", "arg2"},
	}

	err := broker.Enqueue(ctx, job)
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRabbitMQBroker_Start_NotConnected(t *testing.T) {
	options := DefaultOptions()
	options.Queues = []string{"test_queue"}
	broker := NewBroker(options, &mockSerializer{})
	ctx := context.Background()
	jobChan := make(chan job.Job, 10)

	err := broker.Start(ctx, jobChan)
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRabbitMQBroker_Start_EmptyQueues(t *testing.T) {
	options := DefaultOptions()
	options.Queues = []string{}
	broker := NewBroker(options, &mockSerializer{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	jobChan := make(chan job.Job, 10)

	err := broker.Start(ctx, jobChan)
	assert.ErrorIs(t, err, errors.ErrNoQueues)
}

func TestRabbitMQBroker_SerializationError(t *testing.T) {
	serializer := &mockSerializer{
		serializeErr: assert.AnError,
		format:       "json",
	}

	broker := NewBroker(DefaultOptions(), serializer)
	ctx := context.Background()
	job := &mockJob{id: "test", queue: "test_queue", class: "TestClass"}

	// Should fail with not connected error before even reaching serialization
	err := broker.Enqueue(ctx, job)
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}
